/**
 * Handle models (i.e. docs)
 * Serialization/deserialization
 * Copying
 * Querying, update
 */

import * as comparisonFunctions from "./comparison";
import * as lastStepModifierFunctions from "./lastStepModifierFunctions";
import { isDate } from "util";
import { unique } from "../util";
export { match, getDotValue } from "./match";

const modifierFunctions = {};

/**
 * Check a key, throw an error if the key is non valid
 * @param {String} k key
 * @param {Model} v value, needed to treat the Date edge case
 * Non-treatable edge cases here: if part of the object if of the form { $$date: number } or { $$deleted: true }
 * Its serialized-then-deserialized version it will transformed into a Date object
 * But you really need to want it to trigger such behaviour, even when warned not to use '$' at the beginning of the field names...
 */
function checkKey(k: number | string, v: any) {
  if (typeof k === "number") {
    k = k.toString();
  }

  if (
    k[0] === "$" &&
    !(k === "$$date" && typeof v === "number") &&
    !(k === "$$deleted" && v === true) &&
    !(k === "$$indexCreated") &&
    !(k === "$$indexRemoved")
  ) {
    throw new Error("Field names cannot begin with the $ character");
  }

  if (k.indexOf(".") !== -1) {
    throw new Error("Field names cannot contain a .");
  }
}

/**
 * Check a DB object and throw an error if it's not valid
 * Works by applying the above checkKey function to all fields recursively
 */
export function checkObject(obj: any) {
  if (Array.isArray(obj)) {
    obj.forEach(o => checkObject(o));
  }

  if (typeof obj === "object" && obj !== null) {
    Object.keys(obj).forEach(k => {
      checkKey(k, obj[k]);
      checkObject(obj[k]);
    });
  }
}

/**
 * Serialize an object to be persisted to a one-line string
 * For serialization/deserialization, we use the native JSON parser and not eval or Function
 * That gives us less freedom but data entered in the database may come from users
 * so eval and the like are not safe
 * Accepted primitive types: Number, String, Boolean, Date, null
 * Accepted secondary types: Objects, Arrays
 */
export function serialize(obj: any) {
  return JSON.stringify(obj, function(k, v) {
    checkKey(k, v);

    if (v === undefined || v === null) {
      return v;
    }

    // Hackish way of checking if object is Date (this way it works between execution contexts in node-webkit).
    // We can't use value directly because for dates it is already string in this function (date.toJSON was already called), so we use this
    if (typeof this[k].getTime === "function") {
      return { $$date: this[k].getTime() };
    }

    return v;
  });
}

/**
 * From a one-line representation of an object generate by the serialize function
 * Return the object itself
 */
export function deserialize(rawData: string) {
  return JSON.parse(rawData, (k, v) => {
    if (k === "$$date") {
      return new Date(v);
    }
    if (
      typeof v === "string" ||
      typeof v === "number" ||
      typeof v === "boolean" ||
      v === null
    ) {
      return v;
    }
    if (v && v.$$date) {
      return v.$$date;
    }

    return v;
  });
}

/**
 * Deep copy a DB object
 * The optional strictKeys flag (defaulting to false) indicates whether to copy everything or only fields
 * where the keys are valid, i.e. don't begin with $ and don't contain a .
 */
export function deepCopy(obj: any, strictKeys: boolean = false) {
  var res;

  if (
    typeof obj === "boolean" ||
    typeof obj === "number" ||
    typeof obj === "string" ||
    obj === null ||
    isDate(obj)
  ) {
    return obj;
  }

  if (Array.isArray(obj)) {
    return obj.map(o => deepCopy(o, strictKeys));
  }

  if (typeof obj === "object") {
    return Object.keys(obj).reduce<any>((res, k) => {
      if (!strictKeys || (k[0] !== "$" && k.indexOf(".") === -1)) {
        res[k] = deepCopy(obj[k], strictKeys);
      }
      return res;
    }, {});
  }
}

/**
 * Utility functions for comparing things
 * Assumes type checking was already done (a and b already have the same type)
 * compareNSB works for numbers, strings and booleans
 */
function compareNSB<T>(a: T, b: T): number {
  if (a < b) {
    return -1;
  } else if (a > b) {
    return 1;
  }
  return 0;
}

function compareArrays(a: any[], b: any[]): number {
  for (let i = 0; i < Math.min(a.length, b.length); i += 1) {
    const comp = compareThings(a[i], b[i]);

    if (comp !== 0) {
      return comp;
    }
  }

  // Common section was identical, longest one wins
  return compareNSB(a.length, b.length);
}

/**
 * Compare { things U undefined }
 * Things are defined as any native types (string, number, boolean, null, date) and objects
 * We need to compare with undefined as it will be used in indexes
 * In the case of objects and arrays, we deep-compare
 * If two objects dont have the same type, the (arbitrary) type hierarchy is: undefined, null, number, strings, boolean, dates, arrays, objects
 * Return -1 if a < b, 1 if a > b and 0 if a = b (note that equality here is NOT the same as defined in areThingsEqual!)
 *
 * @param {Function} _compareStrings String comparing function, returning -1, 0 or 1, overriding default string comparison (useful for languages with accented letters)
 */
export function compareThings(a: any, b: any, _compareStrings?: any): number {
  var aKeys,
    bKeys,
    comp,
    i,
    compareStrings = _compareStrings || compareNSB;

  // undefined
  if (a === undefined) return b === undefined ? 0 : -1;
  if (b === undefined) return a === undefined ? 0 : 1;

  // null
  if (a === null) return b === null ? 0 : -1;
  if (b === null) return a === null ? 0 : 1;

  // Numbers
  if (typeof a === "number") {
    return typeof b === "number" ? compareNSB(a, b) : -1;
  } else if (typeof b === "number") {
    return typeof a === "number" ? compareNSB(a, b) : 1;
  }

  // Strings
  if (typeof a === "string") {
    return typeof b === "string" ? compareStrings(a, b) : -1;
  } else if (typeof b === "string") {
    return typeof a === "string" ? compareStrings(a, b) : 1;
  }

  // Booleans
  if (typeof a === "boolean") {
    return typeof b === "boolean" ? compareNSB(a, b) : -1;
  } else if (typeof b === "boolean") {
    return typeof a === "boolean" ? compareNSB(a, b) : 1;
  }

  // Dates
  if (isDate(a)) {
    return isDate(b) ? compareNSB(a.getTime(), b.getTime()) : -1;
  } else if (isDate(b)) {
    return isDate(a) ? compareNSB(a.getTime(), b.getTime()) : 1;
  }

  // Arrays (first element is most significant and so on)
  if (Array.isArray(a)) {
    return Array.isArray(b) ? compareArrays(a, b) : -1;
  } else if (Array.isArray(b)) {
    return Array.isArray(a) ? compareArrays(a, b) : 1;
  }

  // Objects
  aKeys = Object.keys(a).sort();
  bKeys = Object.keys(b).sort();

  for (i = 0; i < Math.min(aKeys.length, bKeys.length); i += 1) {
    comp = compareThings(a[aKeys[i]], b[bKeys[i]]);

    if (comp !== 0) {
      return comp;
    }
  }

  return compareNSB(aKeys.length, bKeys.length);
}

// Given its name, create the complete modifier function
function createModifierFunction(modifier) {
  return function(obj, field, value) {
    var fieldParts = typeof field === "string" ? field.split(".") : field;

    if (fieldParts.length === 1) {
      lastStepModifierFunctions[modifier](obj, field, value);
    } else {
      if (obj[fieldParts[0]] === undefined) {
        if (modifier === "$unset") {
          return;
        } // Bad looking specific fix, needs to be generalized modifiers that behave like $unset are implemented
        obj[fieldParts[0]] = {};
      }
      modifierFunctions[modifier](
        obj[fieldParts[0]],
        fieldParts.slice(1),
        value,
      );
    }
  };
}

// Actually create all modifier functions
Object.keys(lastStepModifierFunctions).forEach(function(modifier) {
  modifierFunctions[modifier] = createModifierFunction(modifier);
});

/**
 * Modify a DB object according to an update query
 */
export function modify(obj, updateQuery) {
  const keys = Object.keys(updateQuery);
  const firstChars = keys.map(item => item[0]);
  const dollarFirstChars = firstChars.filter(c => c === "$");

  if (keys.indexOf("_id") !== -1 && updateQuery._id !== obj._id) {
    throw new Error("You cannot change a document's _id");
  }

  if (
    dollarFirstChars.length !== 0 &&
    dollarFirstChars.length !== firstChars.length
  ) {
    throw new Error("You cannot mix modifiers and normal fields");
  }

  let newDoc;
  let modifiers;
  if (dollarFirstChars.length === 0) {
    // Simply replace the object with the update query contents
    newDoc = deepCopy(updateQuery);
    newDoc._id = obj._id;
  } else {
    // Apply modifiers
    modifiers = unique(keys);
    newDoc = deepCopy(obj);
    modifiers.forEach(m => {
      if (!modifierFunctions[m]) {
        throw new Error("Unknown modifier " + m);
      }

      // Can't rely on Object.keys throwing on non objects since ES6
      // Not 100% satisfying as non objects can be interpreted as objects but no false negatives so we can live with it
      if (typeof updateQuery[m] !== "object") {
        throw new Error("Modifier " + m + "'s argument must be an object");
      }

      Object.keys(updateQuery[m]).forEach(k => {
        modifierFunctions[m](newDoc, k, updateQuery[m][k]);
      });
    });
  }

  // Check result is valid and return it
  checkObject(newDoc);

  if (obj._id !== newDoc._id) {
    throw new Error("You can't change a document's _id");
  }
  return newDoc;
}
