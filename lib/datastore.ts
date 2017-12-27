import Executor from "./executor";
import { EventEmitter } from "events";
import Persistence from "./persistence";
import * as model from "./model";
import Index, { IndexOptions } from "./indexes";
import Cursor from "./Cursor";
import { isDate } from "util";

var customUtils = require("./customUtils"),
  async = require("async"),
  _ = require("underscore");

export interface UpdateOptions {
  /** If true, can update multiple documents */
  multi?: boolean;
  /** If true, document is inserted if the query doesn't match anything */
  upsert?: boolean;
  /**
   * if true return as third argument the array of updated matched documents
   * (even if no change actually took place)
   */
  returnUpdatedDocs?: boolean;
}

export interface DataStoreOptions {
  /** Datastore will be in-memory only if not provided */
  filename?: string;
  /**
   * Defaults to false. If set to true, createdAt and updatedAt will
   * be created and populated automatically (if not specified by
   */
  timestampData?: boolean;
  /** Defaults to false */
  inMemoryOnly?: boolean;
  /**
   * Specify the name of your NW app if you want options. filename to
   * be relative to the directory where Node Webkit stores application data such
   * as cookies and local storage (the best place to store data in my opinion)
   */
  nodeWebkitAppName?: string;
  /** Defaults to false */
  autoload?: boolean;
  /**
   * If autoload is used this will be called after the load database
   * with the error object as parameter. If you don't pass it the error will be
   * thrown
   */
  onload?: Function;
  beforeDeserialization?: Function;
  afterSerialization?: Function;
  /** Threshold after which an alert is thrown if too much data is corrupt */
  corruptAlertThreshold?: number;
  /** String comparison function that overrides default for sorting */
  compareStrings?: Function;
}

export interface EnsureIndexOptions {
  fieldName?: string;
  unique?: boolean;
  sparse?: boolean;
  /**
   * If set this index becomes a TTL index (only works on Date fields, not
   * arrays of Date)
   */
  expireAfterSeconds?: number;
}

/**
 * Create a new collection
 *
 * Event Emitter - Events
 * * compaction.done - Fired whenever a compaction operation was finished
 */
export default class Datastore extends EventEmitter {
  public inMemoryOnly: boolean;
  private autoload: boolean;
  private timestampData: boolean;
  public filename: null | string;
  public compareStrings?: Function;
  public executor: Executor;
  public persistence: Persistence;

  // Indexed by field name, dot notation can be used
  // _id is always indexed and since _ids are generated randomly the underlying
  // binary is always well-balanced
  public indexes = {
    _id: new Index({ fieldName: "_id", unique: true }),
  };
  private ttlIndexes = {};

  constructor(options: DataStoreOptions = {}) {
    super();
    var filename;

    filename = options.filename;
    this.inMemoryOnly = options.inMemoryOnly || false;
    this.autoload = options.autoload || false;
    this.timestampData = options.timestampData || false;

    // Determine whether in memory or persistent
    if (!filename || typeof filename !== "string" || filename.length === 0) {
      this.filename = null;
      this.inMemoryOnly = true;
    } else {
      this.filename = filename;
    }

    // String comparison function
    this.compareStrings = options.compareStrings;

    // Persistence handling
    this.persistence = new Persistence({
      db: this,
      nodeWebkitAppName: options.nodeWebkitAppName,
      afterSerialization: options.afterSerialization,
      beforeDeserialization: options.beforeDeserialization,
      corruptAlertThreshold: options.corruptAlertThreshold,
    });

    // This new executor is ready if we don't use persistence
    // If we do, it will only be ready once loadDatabase is called
    this.executor = new Executor();
    if (this.inMemoryOnly) {
      this.executor.ready = true;
    }

    // Queue a load of the database right away and call the onload handler
    // By default (no onload handler), if there is an error there, no operation will be possible so warn the user by throwing an exception
    if (this.autoload) {
      this.loadDatabase();
      if (options.onload) options.onload();
    }
  }

  /**
   * Load the database from the datafile, and trigger the execution of buffered commands if any
   */
  loadDatabase() {
    this.executor.push(
      {
        this: this.persistence,
        fn: this.persistence.loadDatabase,
        arguments,
      },
      true,
    );
  }

  /** Get an array of all the data in the database */
  getAllData() {
    return this.indexes._id.getAll();
  }

  /** Reset all currently defined indexes */
  resetIndexes(newData?: any) {
    Object.keys(this.indexes).forEach(i => {
      this.indexes[i].reset(newData);
    });
  }

  /**
   * Ensure an index is kept for this field. Same parameters as lib/indexes
   * For now this function is synchronous, we need to test how much time it takes
   * We use an async API for consistency with the rest of the code
   */
  async ensureIndex(options: EnsureIndexOptions = {}) {
    if (!options.fieldName) {
      const err = new Error("Cannot create an index without a fieldName");
      (err as any).missingFieldName = true;
      throw err;
    } else if (this.indexes[options.fieldName]) {
      return;
    }
    if (options.expireAfterSeconds !== undefined) {
      this.ttlIndexes[options.fieldName] = options.expireAfterSeconds;
    } // With this implementation index creation is not necessary to ensure TTL but we stick with MongoDB's API here

    delete options.expireAfterSeconds;
    this.indexes[options.fieldName] = new Index(options as IndexOptions);

    try {
      this.indexes[options.fieldName].insert(this.getAllData());
    } catch (e) {
      delete this.indexes[options.fieldName];
      throw e;
    }

    // We may want to force all options to be persisted including defaults, not just the ones passed the index creation function
    return this.persistence.persistNewState([{ $$indexCreated: options }]);
  }

  /** Remove an index */
  async removeIndex(fieldName: string) {
    delete this.indexes[fieldName];
    await this.persistence.persistNewState([{ $$indexRemoved: fieldName }]);
  }

  /**
   * Add one or several document(s) to all indexes
   */
  addToIndexes(doc) {
    var i,
      failingIndex,
      error,
      keys = Object.keys(this.indexes);

    for (i = 0; i < keys.length; i += 1) {
      try {
        this.indexes[keys[i]].insert(doc);
      } catch (e) {
        failingIndex = i;
        error = e;
        break;
      }
    }

    // If an error happened, we need to rollback the insert on all other indexes
    if (error) {
      for (i = 0; i < failingIndex; i += 1) {
        this.indexes[keys[i]].remove(doc);
      }

      throw error;
    }
  }

  /**
   * Remove one or several document(s) from all indexes
   */
  removeFromIndexes(doc) {
    var self = this;

    Object.keys(this.indexes).forEach(function(i) {
      self.indexes[i].remove(doc);
    });
  }

  /**
   * Update one or several documents in all indexes
   * To update multiple documents, oldDoc must be an array of { oldDoc, newDoc } pairs
   * If one update violates a constraint, all changes are rolled back
   */
  updateIndexes(oldDoc, newDoc?: any) {
    var i,
      failingIndex,
      error,
      keys = Object.keys(this.indexes);

    for (i = 0; i < keys.length; i += 1) {
      try {
        this.indexes[keys[i]].update(oldDoc, newDoc);
      } catch (e) {
        failingIndex = i;
        error = e;
        break;
      }
    }

    // If an error happened, we need to rollback the update on all other indexes
    if (error) {
      for (i = 0; i < failingIndex; i += 1) {
        this.indexes[keys[i]].revertUpdate(oldDoc, newDoc);
      }

      throw error;
    }
  }

  /**
   * Return the list of candidates for a given query
   * Crude implementation for now, we return the candidates given by the first usable index if any
   * We try the following query types, in this order: basic match, $in match, comparison match
   * One way to make it better would be to enable the use of multiple indexes if the first usable index
   * returns too much data. I may do it in the future.
   *
   * Returned candidates will be scanned to find and remove all expired documents
   *
   * @param {Query} query
   * @param {Boolean} dontExpireStaleDocs Optional, defaults to false, if true don't remove stale docs. Useful for the remove function which shouldn't be impacted by expirations
   * @param {Function} callback Signature err, candidates
   */
  async getCandidates(query, dontExpireStaleDocs = false) {
    var indexNames = Object.keys(this.indexes),
      usableQueryKeys;

    async.waterfall([
      // STEP 1: get candidates list by checking indexes from most to least frequent usecase
      function(cb) {
        // For a basic match
        usableQueryKeys = [];
        Object.keys(query).forEach(k => {
          if (
            typeof query[k] === "string" ||
            typeof query[k] === "number" ||
            typeof query[k] === "boolean" ||
            isDate(query[k]) ||
            query[k] === null
          ) {
            usableQueryKeys.push(k);
          }
        });
        usableQueryKeys = _.intersection(usableQueryKeys, indexNames);
        if (usableQueryKeys.length > 0) {
          return cb(
            null,
            this.indexes[usableQueryKeys[0]].getMatching(
              query[usableQueryKeys[0]],
            ),
          );
        }

        // For a $in match
        usableQueryKeys = [];
        Object.keys(query).forEach(k => {
          if (query[k] && query[k].hasOwnProperty("$in")) {
            usableQueryKeys.push(k);
          }
        });
        usableQueryKeys = _.intersection(usableQueryKeys, indexNames);
        if (usableQueryKeys.length > 0) {
          return cb(
            null,
            this.indexes[usableQueryKeys[0]].getMatching(
              query[usableQueryKeys[0]].$in,
            ),
          );
        }

        // For a comparison match
        usableQueryKeys = [];
        Object.keys(query).forEach(k => {
          if (
            query[k] &&
            (query[k].hasOwnProperty("$lt") ||
              query[k].hasOwnProperty("$lte") ||
              query[k].hasOwnProperty("$gt") ||
              query[k].hasOwnProperty("$gte"))
          ) {
            usableQueryKeys.push(k);
          }
        });
        usableQueryKeys = _.intersection(usableQueryKeys, indexNames);
        if (usableQueryKeys.length > 0) {
          return cb(
            null,
            this.indexes[usableQueryKeys[0]].getBetweenBounds(
              query[usableQueryKeys[0]],
            ),
          );
        }

        // By default, return all the DB data
        return cb(null, this.getAllData());
      },
      // STEP 2: remove all expired documents
      function(docs) {
        if (dontExpireStaleDocs) {
          return callback(null, docs);
        }

        var expiredDocsIds = [],
          validDocs = [],
          ttlIndexesFieldNames = Object.keys(this.ttlIndexes);

        docs.forEach(doc => {
          var valid = true;
          ttlIndexesFieldNames.forEach(i => {
            if (
              doc[i] !== undefined &&
              isDate(doc[i]) &&
              Date.now() > doc[i].getTime() + this.ttlIndexes[i] * 1000
            ) {
              valid = false;
            }
          });
          if (valid) {
            validDocs.push(doc);
          } else {
            expiredDocsIds.push(doc._id);
          }
        });

        async.eachSeries(
          expiredDocsIds,
          function(_id, cb) {
            this._remove({ _id: _id }, {}, err => {
              if (err) {
                return callback(err);
              }
              return cb();
            });
          },
          function(err) {
            return callback(null, validDocs);
          },
        );
      },
    ]);
  }

  /** Insert a new document */
  private async _insert(newDoc) {
    let preparedDoc;

    preparedDoc = this.prepareDocumentForInsertion(newDoc);
    this._insertInCache(preparedDoc);

    await this.persistence.persistNewState(
      Array.isArray(preparedDoc) ? preparedDoc : [preparedDoc],
    );
    return model.deepCopy(preparedDoc);
  }

  /**
   * Create a new _id that's not already in use
   */
  createNewId() {
    var tentativeId = customUtils.uid(16);
    // Try as many times as needed to get an unused _id. As explained in customUtils, the probability of this ever happening is extremely small, so this is O(1)
    if (this.indexes._id.getMatching(tentativeId).length > 0) {
      tentativeId = this.createNewId();
    }
    return tentativeId;
  }

  /**
   * Prepare a document (or array of documents) to be inserted in a database
   * Meaning adds _id and timestamps if necessary on a copy of newDoc to avoid any side effect on user input
   * @api private
   */
  prepareDocumentForInsertion(newDoc) {
    var preparedDoc;

    if (Array.isArray(newDoc)) {
      preparedDoc = newDoc.map(doc => this.prepareDocumentForInsertion(doc));
    } else {
      preparedDoc = model.deepCopy(newDoc);
      if (preparedDoc._id === undefined) {
        preparedDoc._id = this.createNewId();
      }
      if (this.timestampData && preparedDoc.createdAt === undefined) {
        preparedDoc.createdAt = new Date();
      }
      model.checkObject(preparedDoc);
    }

    return preparedDoc;
  }

  /**
   * If newDoc is an array of documents, this will insert all documents in the cache
   * @api private
   */
  _insertInCache(preparedDoc) {
    if (Array.isArray(preparedDoc)) {
      this._insertMultipleDocsInCache(preparedDoc);
    } else {
      this.addToIndexes(preparedDoc);
    }
  }

  /**
   * If one insertion fails (e.g. because of a unique constraint), roll back all previous
   * inserts and throws the error
   * @api private
   */
  _insertMultipleDocsInCache(preparedDocs) {
    var i, failingI, error;

    for (i = 0; i < preparedDocs.length; i += 1) {
      try {
        this.addToIndexes(preparedDocs[i]);
      } catch (e) {
        error = e;
        failingI = i;
        break;
      }
    }

    if (error) {
      for (i = 0; i < failingI; i += 1) {
        this.removeFromIndexes(preparedDocs[i]);
      }

      throw error;
    }
  }

  insert(...args: any[]) {
    this.executor.push({ this: this, fn: this._insert, arguments: args });
  }

  /**
   * Count all documents matching the query
   * @param {Object} query MongoDB-style query
   */
  async count(query, callback) {
    var cursor = new Cursor(this, query);

    if (typeof callback === "function") {
      const docs = await cursor.exec(callback);
      return docs.length;
    } else {
      return cursor;
    }
  }

  /**
   * Find all documents matching the query
   * @param {Object} query MongoDB-style query
   * @param {Object} projection MongoDB-style projection
   */
  find(query: any, projection = {}) {
    const cursor = new Cursor(this, query);
    cursor.projection(projection);
    return cursor;
  }

  /**
   * Find one document matching the query
   * @param {Object} query MongoDB-style query
   * @param {Object} projection MongoDB-style projection
   */
  findOne(query, projection = {}) {
    const cursor = new Cursor(this, query);
    cursor.projection(projection).limit(1);
    return cursor;
  }

  /**
   * Update all docs matching query
   * @param {Object} query
   * @param {Object} updateQuery
   */
  async _update(query, updateQuery, options: UpdateOptions) {
    await this._tryCreate(query, updateQuery, options.upsert || false);
    return await this._tryUpdate(
      query,
      updateQuery,
      options.multi || false,
      options.returnUpdatedDocs || false,
    );
  }

  private async _tryCreate(query: any, updateQuery: any, upsert: boolean) {
    // If upsert option is set, check whether we need to insert the doc
    if (!upsert) return;

    // Need to use an internal function not tied to the executor to avoid deadlock
    const cursor = new Cursor(this, query);
    const docs = await cursor.limit(1)._exec();
    if (docs.length === 1) {
      return;
    }

    let toBeInserted;

    try {
      model.checkObject(updateQuery);
      // updateQuery is a simple object with no modifier, use it as the document to insert
      toBeInserted = updateQuery;
    } catch (e) {
      // updateQuery contains modifiers, use the find query as the base,
      // strip it from all operators and update it according to updateQuery
      try {
        toBeInserted = model.modify(model.deepCopy(query, true), updateQuery);
      } catch (err) {
        throw err;
      }

      return await this._insert(toBeInserted);
    }
  }

  private async _tryUpdate(
    query: any,
    updateQuery: any,
    multi: boolean,
    returnUpdatedDocs: boolean,
  ) {
    // Perform the update
    var modifiedDoc,
      modifications = [],
      createdAt;

    let numReplaced = 0;

    const candidates = await this.getCandidates(query);

    // Preparing update (if an error is thrown here neither the datafile nor
    // the in-memory indexes are affected)
    for (const item of candidates) {
      if (model.match(item, query) && (multi || numReplaced === 0)) {
        numReplaced += 1;
        if (this.timestampData) {
          createdAt = item.createdAt;
        }
        modifiedDoc = model.modify(item, updateQuery);
        if (this.timestampData) {
          modifiedDoc.createdAt = createdAt;
          modifiedDoc.updatedAt = new Date();
        }
        modifications.push({
          oldDoc: item,
          newDoc: modifiedDoc,
        });
      }
    }

    // Change the docs in memory
    this.updateIndexes(modifications);

    // Update the datafile
    const updatedDocs = _.pluck(modifications, "newDoc");
    await this.persistence.persistNewState(updatedDocs);
    if (!returnUpdatedDocs) {
      return numReplaced;
    } else {
      let updatedDocsDC = updatedDocs.map(doc => model.deepCopy(doc));
      return !multi ? updatedDocsDC[0] : updatedDocsDC;
    }
  }

  update() {
    this.executor.push({ this: this, fn: this._update, arguments });
  }

  /**
   * Remove all docs matching the query
   * For now very naive implementation (similar to update)
   * @param {Object} query
   * @param {Object} options Optional options
   *                 options.multi If true, can update multiple documents (defaults to false)
   */
  async _remove(query, options: { multi?: boolean } = {}) {
    let numRemoved = 0;
    const removedDocs = [];
    const multi = options.multi || false;

    const candidates = await this.getCandidates(query, true);

    candidates.forEach(d => {
      if (model.match(d, query) && (multi || numRemoved === 0)) {
        numRemoved += 1;
        removedDocs.push({ $$deleted: true, _id: d._id });
        this.removeFromIndexes(d);
      }
    });

    await this.persistence.persistNewState(removedDocs);
    return numRemoved;
  }

  remove(...args: any[]) {
    this.executor.push({ this: this, fn: this._remove, arguments: arguments });
  }
}
