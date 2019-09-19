/**
 * Allen Yuan
 * Canvas Analytics Team @ Instructure, Inc.
 * 7/26/2019
 *
 * index.js contains helper functions for reading .avro files and functions to diff .avro files on a
 * key (keyDiff) or as sets (vennDiff).
 */

import {detailedDiff} from 'deep-object-diff';
import avro from 'avsc';
import snappy from 'snappy';
import {inspect} from 'util';
import stableStringify from 'json-stable-stringify';
import 'colors';

import {CONFIG} from './config.js';

/**
 * TODO:
 * filteredSchema: filtering will be read in from a config file (high priority)
 * error messages for corrupt file, file DNE, key not in file (high priority)
 * extractRowsParser: put rows into arr in sorted order (low priority, mild performance increase on keyDiff)
 * add functionality for nested fields: filter by/assign as key nested fields (medium)
 * add functionality to use object as key (medium)
 * improve detailedDiff: deleted fields show value as undefined but I want to show their old value (medium)
 *    note you can get around this by switching the order you pass in oldFile and newFile but I would rather show everything
 *    in one place.
 */

/**
 * Given a schema and fields to KEEP or IGNORE, returns a schema with fields in KEEP
 * and not in IGNORE. Pass in null for KEEP or IGNORE to not filter by that argument.
 * @param {Object} schema - schema for an avro file
 * @param {Set} keepSet only fields in keepSet are not filtered out.
 * @param {Set} ignoreSet all fields in ignoreSet are filtered out.
 * @returns {Object} newFile - returns the schema restricted to fields in P0_FIELDS
 */
const filteredSchema = (schema) => {
  const keepSet = CONFIG.keepFields == null ? null : new Set(CONFIG.keepFields);
  const ignoreSet = CONFIG.ignoreFields == null ? null : new Set(CONFIG.ignoreFields);

  const fieldFilter = (field) => {
    return (ignoreSet == null || !ignoreSet.has(field.name)) && (keepSet == null || keepSet.has(field.name));
  }

  return {
    ...schema,
    fields: schema.fields.filter(fieldFilter),
  };
}

export const printVennDiff = (venn) => {
  // having streamed both files to venn, print venn.
  console.log(inspect({ "added" : venn.added}, { depth: 'Infinity' }).green);
  console.log(inspect({ "removed" : venn.removed}, { depth: 'Infinity' }).red);
  console.log(inspect({ "intersection" : venn.intersection}, { depth: 'Infinity' }).yellow);
  // print some stats about the diff
  console.log(`color code: green for added, red for removed, yellow for intersection`);
  console.log(`${venn['added'] != null ? Object.keys(venn['added']).length : 0} removed`);
  console.log(`${venn['removed'] != null ? Object.keys(venn['removed']).length : 0} added`);
  console.log(`${venn['intersection'] != null ? Object.keys(venn['intersection']).length : 0} in intersection`);
}

export const vennDiff = async (oldFile, newFile) => {
  const oldSchema = await getOriginalSchema(oldFile).then(filteredSchema);
  const newSchema = await getOriginalSchema(newFile).then(filteredSchema);
  const venn = {
    "removed" : {},
    "added" : {},
    "intersection" : {},
  };

  await readAvroFile(makeDecoder(oldFile, {readerSchema: oldSchema}), venn, vennParser(1));
  await readAvroFile(makeDecoder(newFile, {readerSchema: newSchema}), venn, vennParser(2));

  return venn;
}

/**
 * Parser function for vennDiff. Adds row to venn, updating fields 'added'
 * 'removed', 'intersection' accordingly.
 * @param {number} num - Flag for old/new file. Pass in 1 if parsing old file and 2 if parsing new file.
 * @param {Object} venn - object with fields 'removed', 'added', 'intersection' representing a venn diagram
 *                     - of the old and new files.
 * @param {Object} row - Row parsed from avro file.
 */
const vennParser = num => venn => row => {
  // let str be the string representation of row.
  // stableStringify puts all keys of row in sorted order so all equivalent JSON objects are mapped to the same string.
  let str = stableStringify(row);
  // if num === 1 then this is the first file and every row goes in the 'removed' field.
  if (num === 1) {
    // increment count of this row
    venn['removed'][str] = venn['removed'][str] == null ? 1 : venn['removed'][str] + 1;
  }
  // if num !== 1 then this is the second file and we may have to move items to intersection.
  else {
    // if str exists in 'removed' remove one occurence of str in 'removed' and add one occurence
    // of str in 'intersection'
    if(venn['removed'][str]) {
      if(venn['removed'][str] === 1) {
        delete venn['removed'][str];
      } else {
        venn['removed'][str] = venn['removed'][str] - 1;
      }
      // increment count of this row
      venn['intersection'][str] = venn['intersection'][str] == null ? 1 : venn['intersection'][str] + 1;
    }
    // else add one occurence of str to 'added'
    else {
      // increment count of this row
      venn['added'][str] = venn['added'][str] == null ? 1 : venn['added'][str] + 1;
    }
  }
}

/**
 * Returns an object representing a diff of two Avro files.
 * @param {string} oldFile - filepath to old .avro file.
 * @param {string} newFile - filepath to new .avro file.
 * @param {string[]} key  - fields comprising a key to diff oldFile and newFile
 * @returns {Object} Returns an object with schema {added:[Array], removed:[Array], changed:[Array], unchanged[Array]}
 *                    where the even indices of the arrays are strings of the form "<key>: <value>" and the odd indices
 *                    contain the decoded rows of Avro, except in the changed field where the odd indices contain
 *                    JSON diff objects.
 */
export const keyDiff = async (oldFile, newFile, key) => {
  // extract rows to oldData and newData
  const newData = await extractRows(newFile);
  const oldData = await extractRows(oldFile);

  // produce a diff object of oldData and newData
  const diff = await keyDiffHelper(oldData, newData, key);
  return diff;
}

/**
 * Prints an object representing a diff of oldFile and newFile based on
 * the given key to console.
 * @param {Object} diff - an object outputted from keyDiff
 */
export const printKeyDiff = (diff) => {
  // log the diff object
  console.log(inspect({ "added" : diff.added}, { depth: 'Infinity' }).green);
  console.log(inspect({ "removed" : diff.removed}, { depth: 'Infinity' }).red);
  console.log(inspect({ "updated" : diff.changed}, { depth: 'Infinity' }).yellow);
  console.log(inspect({ "unchanged" : diff.unchanged}, { depth: 'Infinity' }).white);
  // print some stats about the diff
  console.log("color code: green for added, red for deleted, yellow for updated, white for unchanged");
  console.log(`${diff['removed'].length} removed, ${diff['added'].length} added`);
  console.log(`${diff['changed'].length} changed, ${diff['unchanged'].length} unchanged`);
}

/**
 * Returns an object representing a diff of oldData and newData based on key.
 * @param {Object[]} oldData - array containing rows of old .avro file.
 * @param {Object[]} newData - array containing rows of new .avro file.
 * @param {string[]} key  - fields comprising a key to diff oldData and newData
 * @returns {Object} Returns an object with schema {added:[Array], removed:[Array], changed:[Array], unchanged[Array]}
 *                    where the elements of each array are objects with schema {id:[Array], data:{Object}}
 */
export const keyDiffHelper = async (oldData, newData, key) => {
  // comparison function to order array based on key.
  // a,b are Objects which represent decoded rows of avro.
  const compare = (a, b) => {
    const arrA = constructKey(a, key);
    const arrB = constructKey(b, key);
    return lexCompare(arrA, arrB);
  }
  // initialize object to print
  const output = {
    'removed': [],
    'added': [],
    'changed': [],
    'unchanged': [],
  };
  // sort oldData and newData according to dictionary order of key
  oldData.sort(compare);
  newData.sort(compare);
  // initialize pointers i j for oldData newData. While i j are not finished
  // iterating through oldData newData, update output.
  for (let i = 0, j = 0; i < oldData.length || j < newData.length;) {
    const key1 = i === oldData.length ? null : constructKey(oldData[i], key);
    const key2 = j === newData.length ? null : constructKey(newData[j], key);
    const order = lexCompare(key1, key2);
    const jsonDiff = {};
    // order < 0 => oldData[i] precedes newData[j] => oldData[i] unique => push data
    if (order < 0) {
      jsonDiff['id'] = key1;
      jsonDiff['data'] = oldData[i];
      output['removed'].push(jsonDiff);
      i++;
    }
    // order > 0 => newData[j] precedes oldData[i] => newData[j] unique => push data
    else if (order > 0) {
      jsonDiff['id'] = key2;
      jsonDiff['data'] = newData[j];
      output['added'].push(jsonDiff);
      j++;
    }
    // else oldData[i] corresponds to newData[j], process both rows
    else {
      // If objects are not equal push the diff to 'changed'.
      const diffObj = detailedDiff(oldData[i], newData[j]);
      if(!diffIsEmpty(diffObj)) {
        jsonDiff['id'] = key2;
        jsonDiff['data'] = diffObj;
        output['changed'].push(jsonDiff);
      }
      // Else the objects are equal, push the ids to 'unchanged'.
      else {
        jsonDiff['id'] = key2;
        jsonDiff['data'] = newData[j];
        output['unchanged'].push(jsonDiff);
      }
      i++;
      j++;
    }
  }
  // at this point we have compared all elements of oldData, newData so we return.
  return output;
}

/**
 * Returns an array containing the rows of the given file.
 * @param {string} file - filepath to .avro file
 * @returns {Promise} - Promise which resolves to an Object[] containing the rows of the given file.
 */
export const extractRows = async (file) => {
  const schema = await getOriginalSchema(file).then(filteredSchema);
  const fileData = await readAvroFile(makeDecoder(file, {readerSchema: schema}), [], extractRowsParser).then(passThrough);
  return fileData;
}

/**
 * Parser function for extractRows. Pushes the given row to the given array.
 * @param {Object[]} arr - array to be filled with rows of a .avro file.
 * @param {Object} row - decoded row from a .avro file.
 */
const extractRowsParser = arr => row => {
  arr.push(row);
}

/* <=== Misc helper functions ===> */

/**
 * Returns true if diffObj represents no differences and false otherwise.
 * @param {Object} diffObj - Object representing a diff using the deep-object-diff library
 * @returns {boolean} - returns true if diffObj is empty or all its fields are empty.
 */
const diffIsEmpty = (diffObj) => {
  return isEmpty(diffObj) || (isEmpty(diffObj['added']) && isEmpty(diffObj['deleted']) && isEmpty(diffObj['updated']));
}

/**
 * Checks if the given object is empty.
 * @param {Object} obj - Object to check for being empty.
 * @returns {boolean} - true if obj is empty or null and false otherwise.
 */
const isEmpty = (obj) => {
  return obj == null || Object.keys(obj).length === 0;
}

/**
 * Constructs a composite key of row with respect to fields.
 * The composite key is an array of row['field'] where field iterates over fields.
 * @param {Object} row - decoded row from avro file.
 * @param {string[]} fields - fields which constitute a composite key for the avro file row was decoded from.
 * @returns {string[]} - returns array of row['field'] where field iterates over fields.
 */
export const constructKey = (row, fields) => {
  if (row == null) return null;
  const result = fields.map(field => {
    return String(row[field]);
  });
  return result;
}

/**
 * Lexicographic order for two arrays of strings.
 * Null/undefined are ahead of the order compared to any non-null object.
 * @param {string[]} id1 - first array to compare
 * @param {string[]} id2 - second array to compare
 * @returns {number}  returns a negative number if id1 < id2,
 *                    a positive number if id1 > id2, 0 otherwise
 */
export const lexCompare = (id1, id2) => {
  // null goes to the end of the ordering to make keyDiffHelper more elegant.
  if (id1 == null && id2 == null) return 0;
  if (id1 == null) return 1;
  if (id2 == null) return -1;
  // iterate len times to avoid null pointer exception
  const len = Math.min(id1.length, id2.length);
  for (let i = 0; i < len; i++) {
      // mismatch => return corresponding output.
      if (id1[i] < id2[i]) {
          return -1;
      }
      if (id1[i] > id2[i]) {
          return 1;
      }
  }
  // no return so far => one array is a prefix of the other.
  // return neg if id1 is prefix, pos if id2 is prefix, 0 if arrays are identical.
  return id1.length - id2.length;
}


// this function is used to get result objects out of a Promise
const passThrough = res => res;

/* <=== End of misc helper functions ===> */

/* <=== Helper functions for reading .avro files ===> */

// snappy codecs to be passed as field in opts into createFileDecoder from avsc.
// See https://github.com/mtth/avsc/wiki/API#class-blockdecoderopts
const codecs = {
  // eslint-disable-next-line promise/prefer-await-to-callbacks
  snappy : function (buf, cb) {
    // Avro appends checksums to compressed blocks, which we skip here.
    // eslint-disable-next-line no-magic-numbers
    return snappy.uncompress(buf.slice(0, buf.length - 4), cb);
  },
};

/**
 * Creates a fileDecoder for the given file and options. Provide opts.readerSchema to decode
 * with a hard-coded schema as opposed to reading in the schema from the file.
 * @param {string} file - filepath to .avro file.
 * @param {Object} opts - Object containing decoding options. See https://github.com/mtth/avsc/wiki/API#class-blockdecoderopts
 * @returns {fileDecoder} fileDecoder for the given file according to the given options.
 */
const makeDecoder = (file, opts = {}) => {
  if (CONFIG.codecs == null || CONFIG.codecs.toLowerCase() !== 'snappy') {
    return avro.createFileDecoder(
      file,
      {
        ...(opts && opts.readerSchema ? { readerSchema: opts.readerSchema } : {}),
      }
    );
  } else {
    return avro.createFileDecoder(
      file,
      {
        codecs,
        ...(opts && opts.readerSchema ? { readerSchema: opts.readerSchema } : {}),
      }
    );
  }
};

/**
 * Returns a Promise resolving to responseObj after parsing decoder with a given parsing function.
 * @param {fileDecoder} decoder - fileDecoder which streams rows of a .avro file.
 * @param {Object} responseObj - Object passed into parser for each row.
 * @param {function} parser - function of function run on each row which is passed responseObj then the current row.
 *                          - See extractRowsParser and vennParser
 * @param {Object} opts - Object containing options for decoder. See https://github.com/mtth/avsc/wiki/API#class-blockdecoderopts
 * @returns {Promise} Promise which resolves to responseObj
 */
const readAvroFile = async (decoder, responseObj = {}, parser = () => {}, opts = {}) => {
  return new Promise((resolve) => {
      decoder.on('data', parser(responseObj, opts));
      decoder.on('end', () => {
      resolve(responseObj);
    });
  });
};

/**
 * Returns a Promise resolving to the schema for the file corresponding to the passed in decoder.
 * @param {fileDecoder} decoder - decoder for a fixed .avro file
 * @returns {Promise} Promise resolving to Object representing schema of file corresponding to decoder.
 */
const getAvroSchema = async (decoder) => {
  return new Promise((resolve) => {
    decoder.on('metadata', (type, codec, header) => {
      let meta = header.meta['avro.schema'];
      let metaString = meta.toString();
      let schema = JSON.parse(metaString);
      resolve(schema);
    });
  });
};

/**
 * Returns a Promise resolving to the schema of the given file.
 * @param {string} file - filepath to .avro file
 * @returns {Promise} - Promise which resolves to the schema of the given file.
 */
const getOriginalSchema = async (file) => {
  if (CONFIG.schema != null) {
    return CONFIG.schema;
  } else {
    let decoder = makeDecoder(file);
    let schema = await getAvroSchema(decoder);
    return schema;
  }
}

/* <=== End of helper functions for reading .avro files ===> */
