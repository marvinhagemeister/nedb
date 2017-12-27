import * as model from "./model";
import { unique } from "./util";
var BinarySearchTree = require("ya").AVLTree;

/**
 * Two indexed pointers are equal iif they point to the same place
 */
function checkValueEquality(a, b) {
  return a === b;
}

/**
 * Type-aware projection
 */
function projectForUnique(elt: any) {
  if (elt === null) {
    return "$null";
  }
  if (typeof elt === "string") {
    return "$string" + elt;
  }
  if (typeof elt === "boolean") {
    return "$boolean" + elt;
  }
  if (typeof elt === "number") {
    return "$number" + elt;
  }
  if (Array.isArray(elt)) {
    return "$date" + elt.getTime();
  }

  return elt; // Arrays and objects, will check for pointer equality
}

export interface IndexOptions {
  fieldName: string;
  unique?: boolean;
  sparse?: boolean;
}

/**
 * Create a new index
 * All methods on an index guarantee that either the whole operation was successful and the index changed
 * or the operation was unsuccessful and an error is thrown while the index is unchanged
 * @param {String} options.fieldName On which field should the index apply (can use dot notation to index on sub fields)
 * @param {Boolean} options.unique Optional, enforce a unique constraint (default: false)
 * @param {Boolean} options.sparse Optional, allow a sparse index (we can have documents for which fieldName is undefined) (default: false)
 */
export default class Index {
  private fieldName: string;
  public unique: boolean;
  public sparse: boolean;
  private tree: BinarySearchTree;

  constructor(options: IndexOptions) {
    this.fieldName = options.fieldName;
    this.unique = options.unique || false;
    this.sparse = options.sparse || false;

    this.treeOptions = {
      unique: this.unique,
      compareKeys: model.compareThings,
      checkValueEquality,
    };

    this.reset(); // No data in the beginning
  }

  /**
   * Reset an index
   * @param {Document or Array of documents} newData Optional, data to initialize the index with
   *                                                 If an error is thrown during insertion, the index is not modified
   */
  reset(newData?: Document | Document[]) {
    this.tree = new BinarySearchTree(this.treeOptions);

    if (newData) {
      this.insert(newData);
    }
  }

  /**
   * Insert a new document in the index
   * If an array is passed, we insert all its elements (if one insertion fails the index is not modified)
   * O(log(n))
   */
  insert(doc) {
    var key,
      self = this,
      keys,
      i,
      failingI,
      error;

    if (Array.isArray(doc)) {
      this.insertMultipleDocs(doc);
      return;
    }

    key = model.getDotValue(doc, this.fieldName);

    // We don't index documents that don't contain the field if the index is sparse
    if (key === undefined && this.sparse) {
      return;
    }

    if (!Array.isArray(key)) {
      this.tree.insert(key, doc);
    } else {
      // If an insert fails due to a unique constraint, roll back all inserts before it
      keys = _.uniq(key, projectForUnique);

      for (i = 0; i < keys.length; i += 1) {
        try {
          this.tree.insert(keys[i], doc);
        } catch (e) {
          error = e;
          failingI = i;
          break;
        }
      }

      if (error) {
        for (i = 0; i < failingI; i += 1) {
          this.tree.delete(keys[i], doc);
        }

        throw error;
      }
    }
  }

  /**
   * Insert an array of documents in the index
   * If a constraint is violated, the changes should be rolled back and an error thrown
   *
   * @API private
   */
  insertMultipleDocs(docs) {
    var i, error, failingI;

    for (i = 0; i < docs.length; i += 1) {
      try {
        this.insert(docs[i]);
      } catch (e) {
        error = e;
        failingI = i;
        break;
      }
    }

    if (error) {
      for (i = 0; i < failingI; i += 1) {
        this.remove(docs[i]);
      }

      throw error;
    }
  }

  /**
   * Remove a document from the index
   * If an array is passed, we remove all its elements
   * The remove operation is safe with regards to the 'unique' constraint
   * O(log(n))
   */
  remove(doc) {
    if (Array.isArray(doc)) {
      doc.forEach(d => this.remove(d));
      return;
    }

    const key = model.getDotValue(doc, this.fieldName);
    if (key === undefined && this.sparse) {
      return;
    }

    if (!Array.isArray(key)) {
      this.tree.delete(key, doc);
    } else {
      _.uniq(key, projectForUnique).forEach(_key => {
        this.tree.delete(_key, doc);
      });
    }
  }

  /**
   * Update a document in the index
   * If a constraint is violated, changes are rolled back and an error thrown
   * Naive implementation, still in O(log(n))
   */
  update(oldDoc: any | any[], newDoc?: any) {
    if (Array.isArray(oldDoc)) {
      this.updateMultipleDocs(oldDoc);
      return;
    }

    this.remove(oldDoc);

    try {
      this.insert(newDoc);
    } catch (e) {
      this.insert(oldDoc);
      throw e;
    }
  }

  /**
   * Update multiple documents in the index
   * If a constraint is violated, the changes need to be rolled back
   * and an error thrown
   * @param {Array of oldDoc, newDoc pairs} pairs
   *
   * @API private
   */
  updateMultipleDocs(pairs) {
    var i, failingI, error;

    for (i = 0; i < pairs.length; i += 1) {
      this.remove(pairs[i].oldDoc);
    }

    for (i = 0; i < pairs.length; i += 1) {
      try {
        this.insert(pairs[i].newDoc);
      } catch (e) {
        error = e;
        failingI = i;
        break;
      }
    }

    // If an error was raised, roll back changes in the inverse order
    if (error) {
      for (i = 0; i < failingI; i += 1) {
        this.remove(pairs[i].newDoc);
      }

      for (i = 0; i < pairs.length; i += 1) {
        this.insert(pairs[i].oldDoc);
      }

      throw error;
    }
  }

  /**
   * Revert an update
   */
  revertUpdate(oldDoc, newDoc) {
    var revert = [];

    if (!Array.isArray(oldDoc)) {
      this.update(newDoc, oldDoc);
    } else {
      oldDoc.forEach(pair => {
        revert.push({ oldDoc: pair.newDoc, newDoc: pair.oldDoc });
      });
      this.update(revert);
    }
  }

  /**
   * Get all documents in index whose key match value (if it is a Thing) or one of the elements of value (if it is an array of Things)
   * @param {Thing} value Value to match the key against
   * @return {Array of documents}
   */
  getMatching(value) {
    if (!Array.isArray(value)) {
      return this.tree.search(value);
    } else {
      const _res = {};

      value.forEach(v => {
        this.getMatching(v).forEach(doc => {
          _res[doc._id] = doc;
        });
      });

      return Object.keys(_res).map(_id => _res[_id]);
    }
  }

  /**
   * Get all documents in index whose key is between bounds are they are defined by query
   * Documents are sorted by key
   * @param {Query} query
   * @return {Array of documents}
   */
  getBetweenBounds(query) {
    return this.tree.betweenBounds(query);
  }

  /**
   * Get all elements in the index
   * @return {Array of documents}
   */
  getAll() {
    let res = [];
    this.tree.executeOnEveryNode(node => {
      res = [...res, node.data];
    });
    return res;
  }
}
