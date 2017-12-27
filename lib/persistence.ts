import * as path from "path";
import Datastore from "./datastore";
import * as storage from "./storage";
import * as model from "./model";
import Index from "./indexes";
import { uid } from "./util";

/**
 * Handle every persistence-related task
 * The interface Datastore expects to be implemented is
 * * Persistence.loadDatabase(callback) and callback has signature err
 * * Persistence.persistNewState(newDocs, callback) where newDocs is an array of documents and callback has signature err
 */

var async = require("async");

export interface PersistenceOptions {
  db: Datastore;
  /**
   * specify the name of your NW app if you want options.filename to be relative
   * to the directory where Node Webkit stores application data such as cookies
   * and local storage (the best place to store data in my opinion)
   */
  nodeWebkitAppName?: string;
  corruptAlertThreshold?: number;
  beforeDeserialization?: Function;
  afterSerialization?: Function;
}

/** Create a new Persistence object for database options.db */
export default class Persistence {
  private db: Datastore;
  private inMemoryOnly: boolean;
  private filename: string;
  private corruptAlertThreshold: number;
  private beforeDeserialization: Function;
  private afterSerialization: Function;
  private autocompactionIntervalId: NodeJS.Timer;

  constructor(options: PersistenceOptions) {
    var i, j, randomString;

    this.db = options.db;
    this.inMemoryOnly = this.db.inMemoryOnly;
    this.filename = this.db.filename;
    this.corruptAlertThreshold =
      options.corruptAlertThreshold !== undefined
        ? options.corruptAlertThreshold
        : 0.1;

    if (
      !this.inMemoryOnly &&
      this.filename &&
      this.filename.charAt(this.filename.length - 1) === "~"
    ) {
      throw new Error(
        "The datafile name can't end with a ~, which is reserved for crash safe backup files",
      );
    }

    // After serialization and before deserialization hooks with some basic sanity checks
    if (options.afterSerialization && !options.beforeDeserialization) {
      throw new Error(
        "Serialization hook defined but deserialization hook undefined, cautiously refusing to start NeDB to prevent dataloss",
      );
    }
    if (!options.afterSerialization && options.beforeDeserialization) {
      throw new Error(
        "Serialization hook undefined but deserialization hook defined, cautiously refusing to start NeDB to prevent dataloss",
      );
    }
    this.afterSerialization =
      options.afterSerialization ||
      function(s) {
        return s;
      };
    this.beforeDeserialization =
      options.beforeDeserialization ||
      function(s) {
        return s;
      };
    for (i = 1; i < 30; i += 1) {
      for (j = 0; j < 10; j += 1) {
        randomString = uid(i);
        if (
          this.beforeDeserialization(this.afterSerialization(randomString)) !==
          randomString
        ) {
          throw new Error(
            "beforeDeserialization is not the reverse of afterSerialization, cautiously refusing to start NeDB to prevent dataloss",
          );
        }
      }
    }

    // For NW apps, store data in the same directory where NW stores application data
    if (this.filename && options.nodeWebkitAppName) {
      console.log(
        "==================================================================",
      );
      console.log("WARNING: The nodeWebkitAppName option is deprecated");
      console.log(
        "To get the path to the directory where Node Webkit stores the data",
      );
      console.log("for your app, use the internal nw.gui module like this");
      console.log("require('nw.gui').App.dataPath");
      console.log("See https://github.com/rogerwang/node-webkit/issues/500");
      console.log(
        "==================================================================",
      );
      this.filename = getNWAppFilename(
        options.nodeWebkitAppName,
        this.filename,
      );
    }
  }

  /**
   * Persist cached database
   * This serves as a compaction function since the cache always contains only
   * the number of documents in the collection while the data file is
   * append-only so it may grow larger
   */
  async persistCachedDatabase() {
    let toPersist = "";

    if (this.inMemoryOnly) {
      return;
    }

    this.db.getAllData().forEach(doc => {
      toPersist += this.afterSerialization(model.serialize(doc)) + "\n";
    });
    Object.keys(this.db.indexes).forEach(fieldName => {
      if (fieldName != "_id") {
        // The special _id index is managed by datastore.js, the others need to
        // be persisted
        toPersist +=
          this.afterSerialization(
            model.serialize({
              $$indexCreated: {
                fieldName,
                unique: this.db.indexes[fieldName].unique,
                sparse: this.db.indexes[fieldName].sparse,
              },
            }),
          ) + "\n";
      }
    });

    await storage.crashSafeWriteFile(this.filename, toPersist);
    this.db.emit("compaction.done");
  }

  /**
   * Queue a rewrite of the datafile
   */
  compactDatafile() {
    this.db.executor.push({
      this: this,
      fn: this.persistCachedDatabase,
      arguments: [],
    });
  }

  /**
   * Set automatic compaction every interval ms
   * @param {Number} interval in milliseconds, with an enforced minimum of 5 seconds
   */
  setAutocompactionInterval(interval) {
    const minInterval = 5000,
      realInterval = Math.max(interval || 0, minInterval);

    this.stopAutocompaction();

    this.autocompactionIntervalId = setInterval(
      () => this.compactDatafile(),
      realInterval,
    );
  }

  /**
   * Stop autocompaction (do nothing if autocompaction was not running)
   */
  stopAutocompaction() {
    if (this.autocompactionIntervalId) {
      clearInterval(this.autocompactionIntervalId);
    }
  }

  /**
   * Persist new state for the given newDocs (can be insertion, update or removal)
   * Use an append-only format
   * @param {Array} newDocs Can be empty if no doc was updated/removed
   */
  async persistNewState(newDocs) {
    let toPersist = "";

    // In-memory only datastore
    if (this.inMemoryOnly) {
      return;
    }

    newDocs.forEach(function(doc) {
      toPersist += this.afterSerialization(model.serialize(doc)) + "\n";
    });

    if (toPersist.length === 0) {
      return;
    }

    return storage.appendFile(this.filename, toPersist, "utf8");
  }

  /**
   * From a database's raw data, return the corresponding
   * machine understandable collection
   */
  treatRawData(rawData) {
    var data = rawData.split("\n"),
      dataById = {},
      tdata = [],
      i,
      indexes = {},
      corruptItems = -1; // Last line of every data file is usually blank so not really corrupt

    for (i = 0; i < data.length; i += 1) {
      var doc;

      try {
        doc = model.deserialize(this.beforeDeserialization(data[i]));
        if (doc._id) {
          if (doc.$$deleted === true) {
            delete dataById[doc._id];
          } else {
            dataById[doc._id] = doc;
          }
        } else if (
          doc.$$indexCreated &&
          doc.$$indexCreated.fieldName != undefined
        ) {
          indexes[doc.$$indexCreated.fieldName] = doc.$$indexCreated;
        } else if (typeof doc.$$indexRemoved === "string") {
          delete indexes[doc.$$indexRemoved];
        }
      } catch (e) {
        corruptItems += 1;
      }
    }

    // A bit lenient on corruption
    if (
      data.length > 0 &&
      corruptItems / data.length > this.corruptAlertThreshold
    ) {
      throw new Error(
        "More than " +
          Math.floor(100 * this.corruptAlertThreshold) +
          "% of the data file is corrupt, the wrong beforeDeserialization hook may be used. Cautiously refusing to start NeDB to prevent dataloss",
      );
    }

    Object.keys(dataById).forEach(k => {
      tdata.push(dataById[k]);
    });

    return { data: tdata, indexes: indexes };
  }

  /**
   * Load the database
   * 1) Create all indexes
   * 2) Insert all data
   * 3) Compact the database
   * This means pulling data out of the data file or creating it if it doesn't exist
   * Also, all data is persisted right away, which has the effect of compacting the database file
   * This operation is very quick at startup for a big collection (60ms for ~10k docs)
   */
  async loadDatabase() {
    this.db.resetIndexes();

    // In-memory only datastore
    if (this.inMemoryOnly) {
      return;
    }

    try {
      await ensureDirectoryExists(path.dirname(this.filename));
      await storage.ensureDatafileIntegrity(this.filename);
      const rawData = await storage.readFile(this.filename, "utf8");
      const treatedData = this.treatRawData(rawData);

      // Recreate all indexes in the datafile
      Object.keys(treatedData.indexes).forEach(key => {
        this.db.indexes[key] = new Index(treatedData.indexes[key]);
      });

      // Fill cached database (i.e. all indexes) with data
      try {
        this.db.resetIndexes(treatedData.data);
      } catch (e) {
        this.db.resetIndexes(); // Rollback any index which didn't fail
        throw e;
      }

      await this.db.persistence.persistCachedDatabase();
    } catch (err) {
      this.db.executor.processBuffer();
    }
  }
}

/**
 * Check if a directory exists and create it on the fly if it is not the case
 * cb is optional, signature: err
 */
export function ensureDirectoryExists(dir: string) {
  return storage.mkdir(dir);
}

/**
 * Return the path the datafile if the given filename is relative to the
 * directory where Node Webkit stores data for this application. Probably the
 * best place to store data
 */
export function getNWAppFilename(appName, relativeFilename) {
  let home;

  switch (process.platform) {
    case "win32":
      home = process.env.LOCALAPPDATA || process.env.APPDATA;
      if (!home) {
        throw new Error("Couldn't find the base application data folder");
      }
      home = path.join(home, appName);
      break;
    case "darwin":
      home = process.env.HOME;
      if (!home) {
        throw new Error("Couldn't find the base application data directory");
      }
      home = path.join(home, "Library", "Application Support", appName);
      break;
    case "linux":
      home = process.env.HOME;
      if (!home) {
        throw new Error("Couldn't find the base application data directory");
      }
      home = path.join(home, ".config", appName);
      break;
    default:
      throw new Error(
        "Can't use the Node Webkit relative path for platform " +
          process.platform,
      );
  }

  return path.join(home, "nedb-data", relativeFilename);
}
