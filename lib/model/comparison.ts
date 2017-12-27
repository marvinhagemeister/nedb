import { isDate, isRegExp } from "util";
import { match } from "./match";

/**
 * Check whether 'things' are equal
 * Things are defined as any native types (string, number, boolean, null, date) and objects
 * In the case of object, we check deep equality
 * Returns true if they are, false otherwise
 */
export function areThingsEqual(a, b) {
  var aKeys, bKeys, i;

  // Strings, booleans, numbers, null
  if (
    a === null ||
    typeof a === "string" ||
    typeof a === "boolean" ||
    typeof a === "number" ||
    b === null ||
    typeof b === "string" ||
    typeof b === "boolean" ||
    typeof b === "number"
  ) {
    return a === b;
  }

  // Dates
  if (isDate(a) || isDate(b)) {
    return isDate(a) && isDate(b) && a.getTime() === b.getTime();
  }

  // Arrays (no match since arrays are used as a $in)
  // undefined (no match since they mean field doesn't exist and can't be serialized)
  if (
    (!(Array.isArray(a) && Array.isArray(b)) &&
      (Array.isArray(a) || Array.isArray(b))) ||
    a === undefined ||
    b === undefined
  ) {
    return false;
  }

  // General objects (check for deep equality)
  // a and b should be objects at this point
  try {
    aKeys = Object.keys(a);
    bKeys = Object.keys(b);
  } catch (e) {
    return false;
  }

  if (aKeys.length !== bKeys.length) {
    return false;
  }
  for (i = 0; i < aKeys.length; i += 1) {
    if (bKeys.indexOf(aKeys[i]) === -1) {
      return false;
    }
    if (!areThingsEqual(a[aKeys[i]], b[aKeys[i]])) {
      return false;
    }
  }
  return true;
}

/**
 * Check that two values are comparable
 */
function areComparable(a, b) {
  if (
    typeof a !== "string" &&
    typeof a !== "number" &&
    !isDate(a) &&
    typeof b !== "string" &&
    typeof b !== "number" &&
    !isDate(b)
  ) {
    return false;
  }

  if (typeof a !== typeof b) {
    return false;
  }

  return true;
}

/**
 * Arithmetic and comparison operators
 * @param {Native value} a Value in the object
 * @param {Native value} b Value in the query
 */
export function $lt(a, b) {
  return areComparable(a, b) && a < b;
}

export function $lte(a, b) {
  return areComparable(a, b) && a <= b;
}

export function $gt(a, b) {
  return areComparable(a, b) && a > b;
}

export function $gte(a, b) {
  return areComparable(a, b) && a >= b;
}

export function $ne(a, b) {
  if (a === undefined) {
    return true;
  }
  return !areThingsEqual(a, b);
}

export function $in(a, b) {
  var i;

  if (!Array.isArray(b)) {
    throw new Error("$in operator called with a non-array");
  }

  for (i = 0; i < b.length; i += 1) {
    if (areThingsEqual(a, b[i])) {
      return true;
    }
  }

  return false;
}

export function $nin(a, b) {
  if (!Array.isArray(b)) {
    throw new Error("$nin operator called with a non-array");
  }

  return !$in(a, b);
}

export function $regex(a, b) {
  if (!isRegExp(b)) {
    throw new Error("$regex operator called with non regular expression");
  }

  if (typeof a !== "string") {
    return false;
  } else {
    return b.test(a);
  }
}

export function $exists(value, exists) {
  if (exists || exists === "") {
    // This will be true for all values of exists except false, null, undefined and 0
    exists = true; // That's strange behaviour (we should only use true/false) but that's the way Mongo does it...
  } else {
    exists = false;
  }

  if (value === undefined) {
    return !exists;
  } else {
    return exists;
  }
}

// Specific to arrays
export function $size(obj, value) {
  if (!Array.isArray(obj)) {
    return false;
  }
  if (value % 1 !== 0) {
    throw new Error("$size operator called without an integer");
  }

  return obj.length == value;
}

export function $elemMatch(obj, value) {
  if (!Array.isArray(obj)) {
    return false;
  }
  var i = obj.length;
  var result = false; // Initialize result
  while (i--) {
    if (match(obj[i], value)) {
      // If match for array element, return true
      result = true;
      break;
    }
  }
  return result;
}

export const arrayComparisonFunctions = {
  $size: true,
  $elemMatch: true,
};
