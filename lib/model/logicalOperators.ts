import { match } from "./match";

/**
 * Match any of the subqueries
 * @param {Model} obj
 * @param {Array of Queries} query
 */
export function $or(obj, query: any[]) {
  for (const item of query) {
    if (match(obj, item)) {
      return true;
    }
  }

  return false;
}

/**
 * Match all of the subqueries
 * @param {Model} obj
 * @param {Array of Queries} query
 */
export function $and(obj, query: any[]) {
  for (const item of query) {
    if (!match(obj, item)) {
      return false;
    }
  }

  return true;
}

/**
 * Inverted match of the query
 * @param {Model} obj
 * @param {Query} query
 */
export function $not(obj, query) {
  return !match(obj, query);
}

/**
 * Use a function to match
 * @param {Model} obj
 * @param {Query} query
 */
export function $where<T>(obj: T, fn: (this: T) => boolean): boolean {
  return fn.call(obj);
}
