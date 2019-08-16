import { isDate, isRegExp } from "util";
import * as logicalOperators from "./logicalOperators";
import * as comparisonFunctions from "./comparison";

/**
 * Tells if an object is a primitive type or a "real" object
 * Arrays are considered primitive
 */
export function isPrimitiveType(obj) {
  return (
    typeof obj === "boolean" ||
    typeof obj === "number" ||
    typeof obj === "string" ||
    obj === null ||
    isDate(obj) ||
    Array.isArray(obj)
  );
}

/**
 * Tell if a given document matches a query
 * @param {Object} obj Document to check
 * @param {Object} query
 */
export function match(obj, query) {
  var queryKeys, queryKey, queryValue, i;

  // Primitive query against a primitive type
  // This is a bit of a hack since we construct an object with an arbitrary key only to dereference it later
  // But I don't have time for a cleaner implementation now
  if (isPrimitiveType(obj) || isPrimitiveType(query)) {
    return matchQueryPart({ needAKey: obj }, "needAKey", query);
  }

  // Normal query
  queryKeys = Object.keys(query);
  for (i = 0; i < queryKeys.length; i += 1) {
    queryKey = queryKeys[i];
    queryValue = query[queryKey];

    if (queryKey[0] === "$") {
      if (!logicalOperators[queryKey]) {
        throw new Error("Unknown logical operator " + queryKey);
      }
      if (!logicalOperators[queryKey](obj, queryValue)) {
        return false;
      }
    } else {
      if (!matchQueryPart(obj, queryKey, queryValue)) {
        return false;
      }
    }
  }

  return true;
}

/**
 * Match an object against a specific { key: value } part of a query
 * if the treatObjAsValue flag is set, don't try to match every part separately, but the array as a whole
 */
function matchQueryPart(
  obj,
  queryKey,
  queryValue,
  treatObjAsValue: boolean = false,
) {
  var objValue = getDotValue(obj, queryKey),
    i,
    keys,
    firstChars,
    dollarFirstChars;

  // Check if the value is an array if we don't force a treatment as value
  if (Array.isArray(objValue) && !treatObjAsValue) {
    // If the queryValue is an array, try to perform an exact match
    if (Array.isArray(queryValue)) {
      return matchQueryPart(obj, queryKey, queryValue, true);
    }

    // Check if we are using an array-specific comparison function
    if (
      queryValue !== null &&
      typeof queryValue === "object" &&
      !isRegExp(queryValue)
    ) {
      keys = Object.keys(queryValue);
      for (i = 0; i < keys.length; i += 1) {
        if (comparisonFunctions.arrayComparisonFunctions[keys[i]]) {
          return matchQueryPart(obj, queryKey, queryValue, true);
        }
      }
    }

    // If not, treat it as an array of { obj, query } where there needs to be at least one match
    for (i = 0; i < objValue.length; i += 1) {
      if (matchQueryPart({ k: objValue[i] }, "k", queryValue)) {
        return true;
      } // k here could be any string
    }
    return false;
  }

  // queryValue is an actual object. Determine whether it contains comparison operators
  // or only normal fields. Mixed objects are not allowed
  if (
    queryValue !== null &&
    typeof queryValue === "object" &&
    !isRegExp(queryValue) &&
    !Array.isArray(queryValue)
  ) {
    keys = Object.keys(queryValue);
    firstChars = keys.map(item => item[0]);
    dollarFirstChars = firstChars.filter(c => c === "$");

    if (
      dollarFirstChars.length !== 0 &&
      dollarFirstChars.length !== firstChars.length
    ) {
      throw new Error("You cannot mix operators and normal fields");
    }

    // queryValue is an object of this form: { $comparisonOperator1: value1, ... }
    if (dollarFirstChars.length > 0) {
      for (i = 0; i < keys.length; i += 1) {
        if (!comparisonFunctions[keys[i]]) {
          throw new Error("Unknown comparison function " + keys[i]);
        }

        if (!comparisonFunctions[keys[i]](objValue, queryValue[keys[i]])) {
          return false;
        }
      }
      return true;
    }
  }

  // Using regular expressions with basic querying
  if (isRegExp(queryValue)) {
    return comparisonFunctions.$regex(objValue, queryValue);
  }

  // queryValue is either a native value or a normal object
  // Basic matching is possible
  if (!comparisonFunctions.areThingsEqual(objValue, queryValue)) {
    return false;
  }

  return true;
}

// ==============================================================
// Finding documents
// ==============================================================

/**
 * Get a value from object with dot notation
 * @param {Object} obj
 * @param {String} field
 */
export function getDotValue(obj, field: string | string[]) {
  var fieldParts = typeof field === "string" ? field.split(".") : field,
    i,
    objs;

  if (!obj) {
    return undefined;
  } // field cannot be empty so that means we should return undefined so that nothing can match

  if (fieldParts.length === 0) {
    return obj;
  }

  if (fieldParts.length === 1) {
    return obj[fieldParts[0]];
  }

  if (Array.isArray(obj[fieldParts[0]])) {
    // If the next field is an integer, return only this item of the array
    i = parseInt(fieldParts[1], 10);
    if (typeof i === "number" && !isNaN(i)) {
      return getDotValue(obj[fieldParts[0]][i], fieldParts.slice(2));
    }

    // Return the array of values
    objs = new Array();
    for (i = 0; i < obj[fieldParts[0]].length; i += 1) {
      objs.push(getDotValue(obj[fieldParts[0]][i], fieldParts.slice(1)));
    }
    return objs;
  } else {
    return getDotValue(obj[fieldParts[0]], fieldParts.slice(1));
  }
}
