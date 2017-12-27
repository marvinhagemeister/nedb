/**
 * Manage access to data, be it to find, update or remove it
 */
import * as model from "./model";
import Datastore from ".";

/** Create a new cursor for this collection */
export default class Cursor {
  private _limit: number;
  private _skip: number;
  private _sort: any;
  private _projection: Record<string, any>;

  constructor(private db: Datastore, private query = {}) {}

  /** Set a limit to the number of results */
  limit(limit: number) {
    this._limit = limit;
    return this;
  }

  /** Skip a the number of results */
  skip(skip: number) {
    this._skip = skip;
    return this;
  }

  /**
   * Sort results of the query
   * @param {SortQuery} sortQuery - SortQuery is { field: order }, field can use the dot-notation, order is 1 for ascending and -1 for descending
   */
  sort(sortQuery) {
    this._sort = sortQuery;
    return this;
  }

  /**
   * Add the use of a projection
   * @param {Object} projection - MongoDB-style projection. {} means take all
   * fields. Then it's { key1: 1, key2: 1 } to take only key1 and key2
   * { key1: 0, key2: 0 } to omit only key1 and key2. Except _id, you can't mix
   * takes and omits
   */
  projection(projection: Record<string, number>) {
    this._projection = projection;
    return this;
  }

  /** Apply the projection */
  project(candidates) {
    const res = [];
    let action: number;
    let keys;

    if (
      this._projection === undefined ||
      Object.keys(this._projection).length === 0
    ) {
      return candidates;
    }

    const keepId = this._projection._id !== 0;
    delete this._projection._id;

    // Check for consistency
    keys = Object.keys(this._projection);
    keys.forEach(k => {
      if (action !== undefined && this._projection[k] !== action) {
        throw new Error("Can't both keep and omit fields except for _id");
      }
      action = this._projection[k];
    });

    // Do the actual projection
    return candidates.map(candidate => {
      let toPush;
      if (action === 1) {
        // pick-type projection
        toPush = { $set: {} };
        keys.forEach(k => {
          toPush.$set[k] = model.getDotValue(candidate, k);
          if (toPush.$set[k] === undefined) {
            delete toPush.$set[k];
          }
        });
        toPush = model.modify({}, toPush);
      } else {
        // omit-type projection
        toPush = { $unset: {} };
        keys.forEach(k => {
          toPush.$unset[k] = true;
        });
        toPush = model.modify(candidate, toPush);
      }

      if (keepId) {
        toPush._id = candidate._id;
      } else {
        delete toPush._id;
      }

      return toPush;
    });
  }

  /**
   * Get all matching elements
   * Will return pointers to matched elements (shallow copies), returning full copies is the role of find or findOne
   * This is an internal function, use exec which uses the executor
   */
  async _exec() {
    var res = [],
      added = 0,
      skipped = 0,
      error = null,
      i,
      keys,
      key;

    const candidates = await this.db.getCandidates(this.query);

    for (i = 0; i < candidates.length; i += 1) {
      if (model.match(candidates[i], this.query)) {
        // If a sort is defined, wait for the results to be sorted before applying limit and skip
        if (!this._sort) {
          if (this._skip && this._skip > skipped) {
            skipped += 1;
          } else {
            res.push(candidates[i]);
            added += 1;
            if (this._limit && this._limit <= added) {
              break;
            }
          }
        } else {
          res.push(candidates[i]);
        }
      }
    }

    // Apply all sorts
    if (this._sort) {
      keys = Object.keys(this._sort);

      // Sorting
      var criteria: any[] = [];
      for (i = 0; i < keys.length; i++) {
        key = keys[i];
        criteria.push({ key: key, direction: this._sort[key] });
      }
      res.sort((a, b) => {
        var criterion, compare, i;
        for (i = 0; i < criteria.length; i++) {
          criterion = criteria[i];
          compare =
            criterion.direction *
            model.compareThings(
              model.getDotValue(a, criterion.key),
              model.getDotValue(b, criterion.key),
              this.db.compareStrings,
            );
          if (compare !== 0) {
            return compare;
          }
        }
        return 0;
      });

      // Applying limit and skip
      var limit = this._limit || res.length,
        skip = this._skip || 0;

      res = res.slice(skip, skip + limit);
    }

    // Apply projection
    return this.project(res);
  }

  async exec(...args: any[]) {
    this.db.executor.push({
      this: this,
      fn: this._exec,
      arguments: args,
    });
  }
}
