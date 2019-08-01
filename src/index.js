/**
 * Allen Yuan
 * Canvas Analytics Team @ Instructure, Inc.
 * 7/26/2019
 *
 * index.js contains helper functions for reading .avro files and functions to diff .avro files on a
 * key (keyDiff) or to make a venn diagram (vennDiff).
 */

import { detailedDiff } from 'deep-object-diff';
import avro from 'avsc';
import snappy from 'snappy';
import { inspect } from 'util';

// P0_FIELDS ("priority zero fields") are the high priority fields we want to decode.
// const P0_FIELDS = ['globalAssignmentId', 'globalStudentId', 'assignmentName', 'weeklyAggregates', 'percentage'];
const P0_FIELDS = ['studentId', 'assignmentId', 'assignmentName', 'submission'];
// const P0_FIELDS = [];

/**
 * Given filepaths to an old and new avro file, prints to console
 * an object representing a venn diagram of the old and new avro files.
 * The object contains 3 fields "removed", "added", and "intersection"
 * each of which contains rows only in old, only in new, and in both
 * mapped to a count of how many copies there are.
 * @param {string} file1 - filepath to old .avro file.
 * @param {string} file2 - filepath to new .avro file.
 */
export const vennDiff = async (file1, file2) => {
  // read in the schema of file1 to a variable called result
  Promise.resolve(getOriginalSchema(file1)).then((result) => {
    // let newSchema be the intersection of result with P0_FIELDS
    const newSchema = {
      ...result,
      fields: result.fields.filter((field) => P0_FIELDS.includes(field.name)),
    };
    // let venn be the object to print
    let venn = {
      "removed" : {},
      "added" : {},
      "intersection" : {},
    };
    // stream the rows of file1 and file2 into venn according to vennParser
    readAvroFile(makeDecoder(file1, {readerSchema: newSchema}), venn, vennParser(1)).then(() => {
      readAvroFile(makeDecoder(file2, {readerSchema: newSchema}), venn, vennParser(2)).then(() => {
        // having streamed both files to venn, print venn.
        console.log(venn);
      })
    });
  });
}

/**
 * Parser function for vennDiff. Adds row to venn, updating fields "added"
 * "removed", "intersection" accordingly.
 * @param {number} num - Flag for old/new file. Pass in 1 if parsing old file and 2 if parsing new file.
 * @param {Object} venn - object with fields "removed", "added", "intersection" representing a venn diagram
 *                     - of the old and new files.
 * @param {Object} row - Row parsed from avro file.
 */
const vennParser = num => venn => row => {
  // let str be the string representation of row.
  // Note: It is more correct to use deep equals rather than string comparisons.
  // However isEqual which I imported is not working so I am using string comparison.
  let str = row.toString();
  // if num === 1 then this is the first file and every row goes in the "removed" field.
  if(num === 1) {
    // increment count of this row
    venn["removed"][str] = venn["removed"][str] == null ? 1 : venn["removed"][str] + 1;
  }
  // if num !== 1 then this is the second file.
  else {
    // if str exists in "removed" remove one occurence of str in "removed" and add one occurence
    // of str in "intersection"
    if(venn["removed"][str]) {
      if(venn["removed"][str] === 1) {
        delete venn["removed"][str];
      } else {
        venn["removed"][str] = venn["removed"][str] - 1;
      }
      // increment count of this row
      venn["intersection"][str] = venn["intersection"][str] == null ? 1 : venn["intersection"][str] + 1;
    }
    // else add one occurence of str to "added"
    else {
      // increment count of this row
      venn["added"][str] = venn["added"][str] == null ? 1 : venn["added"][str] + 1;
    }
  }
}

/**
 * Prints an object representing a diff of file1 and file2 based on
 * the given key to console.
 * @param {string} file1 - filepath to old .avro file.
 * @param {string} file2 - filepath to new .avro file.
 * @param {string} key  - primary key to compare arr1 and arr2 on.
 *                      - for composite key enter the columns as a comma separated list
 *                      - for example "assignmentId,studentId"
 */
export const keyDiff = async (file1, file2, key) => {
  // extract rows from first file to arr1
  extractRows(file1).then(async (arr1) => {
    // extract rows from second file to arr2
    extractRows(file2).then(async (arr2) => {
      // print diff of arr1, arr2 based on key.
      const diff = await keyDiffHelper(arr1, arr2, key);
      console.log(inspect(diff, {depth: "Infinity"}));
      console.log(`${diff["removed"].length} removed, ${diff["added"].length} added`);
      console.log(`${diff["changed"].length} changed, ${diff["unchanged"].length} unchanged`);
    })
  })
}

/**
 * Prints an object representing a diff of file1 and file2 based on
 * the given key to console.
 * @param {Object[]} arr1 - array containing rows of old .avro file.
 * @param {Object[]} arr2 - array containing rows of new .avro file.
 * @param {string} key  - primary key to compare arr1 and arr2 on.
 *                      - for composite key enter the columns as a comma separated list
 *                      - for example "assignmentId,studentId"
 */
export const keyDiffHelper = async (arr1, arr2, key) => {
  // keyArr is the array of columns which comprise a key for the avro files.
  const keyArr = key.split(",");
  // comparison function to order array based on key.
  // a,b are Objects which represent decoded rows of avro.
  const compare = (a, b) => {
    let arrA = constructKey(a, keyArr);
    let arrB = constructKey(b, keyArr);
    return lexCompare(arrA, arrB);
  }
  // initialize object to print
  let output = {
    "removed": [],
    "added": [],
    "changed": [],
    "unchanged": [],
  };
  // sort arr1 and arr2 according to dictionary order of key
  arr1.sort(compare);
  arr2.sort(compare);
  // initialize pointers i j for arr1 arr2. While i j are not finished
  // iterating through arr1 arr2, update output.
  for (let i = 0, j = 0; i < arr1.length || j < arr2.length;) {
    const key1 = i === arr1.length ? undefined : constructKey(arr1[i], keyArr);
    const key2 = j === arr2.length ? undefined : constructKey(arr2[j], keyArr);
    const str1 = key1 == null ? null : key1.join();
    const str2 = key2 == null ? null : key2.join();
    const order = lexCompare(key1, key2);
    // order < 0 => arr1[i] precedes arr2[j] => arr1[i] unique => push data
    if (order < 0) {
      output["removed"].push(key + ": " + str1);
      output["removed"].push(arr1[i]);
      i++;
    }
    // order > 0 => arr2[j] precedes arr1[i] => arr2[j] unique => push data
    else if (order > 0) {
      output["added"].push(key + ": " + str2);
      output["added"].push(arr2[j]);
      j++;
    }
    // else arr1[i] corresponds to arr2[j]
    else {
      // If objects are not equal push the diff to "changed".
      const diffObj = detailedDiff(arr1[i], arr2[j]);
      if(!diffIsEmpty(diffObj)) {
        output["changed"].push(key + ": " + str1);
        output["changed"].push(diffObj);
      }
      // Else the objects are equal, push the ids to "unchanged".
      else {
        output["unchanged"].push(key + ": " + key1);
      }
      // increment both i and j.
      i++;
      j++;
    }
  }
  // at this point we have compared all elements of arr1, arr2 so we return.
  return output;
}

/**
 * Returns an array containing the rows of the given file.
 * @param {string} file - filepath to .avro file
 * @returns {Promise} - Promise which resolves to an Object[] containing the rows of the given file.
 */
export const extractRows = async (file) => {
  var promise = new Promise(function(resolve) {
    // get original schema 'result' of file
    getOriginalSchema(file).then((result) => {
      // let newSchema be the intersection of result and P0_FIELDS
      const newSchema = {
        ...result,
        fields: result.fields.filter((field) => P0_FIELDS.includes(field.name)),
      };
      // read rows according to newSchema into an array and resolve this array.
      readAvroFile(makeDecoder(file, {readerSchema: newSchema}), [], extractRowsParser).then((arr) => {
        resolve(arr);
      });
    })
  });

  return promise;
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
  return isEmpty(diffObj) || (isEmpty(diffObj["added"]) && isEmpty(diffObj["deleted"]) && isEmpty(diffObj["updated"]));
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
 * The composite key is an array of row["field"] where field iterates over fields.
 * @param {Object} row - decoded row from avro file.
 * @param {string[]} fields - fields which constitute a composite key for the avro file row was decoded from.
 * @returns {string[]} - returns array of row["field"] where field iterates over fields.
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
 * @param {string[]} arr1 - first array to compare
 * @param {string[]} arr2 - second array to compare
 * @returns {number}  returns a negative number if arr1 < arr2,
 *                    a positive number if arr1 > arr2, 0 otherwise
 */
export const lexCompare = (arr1, arr2) => {
  // null goes to the end of the ordering to make keyDiffHelper more elegant.
  if (arr1 == null && arr2 == null) return 0;
  if (arr1 == null) return 1;
  if (arr2 == null) return -1;
  // iterate len times to avoid null pointer exception
  const len = Math.min(arr1.length, arr2.length);
  for (let i = 0; i < len; i++) {
    // mismatch => return corresponding output.
    if (arr1[i] < arr2[i]) {
      return -1;
    }
    if (arr1[i] > arr2[i]) {
      return 1;
    }
  }
  // no return so far => one array is a prefix of the other.
  // return neg if arr1 is prefix, pos if arr2 is prefix, 0 if arrays are identical.
  return arr1.length - arr2.length;
}

/* <=== End of misc helper functions ===> */

/* <=== Helper functions for reading .avro files ===> */

// snappy codecs to be passed as field in opts into createFileDecoder from avsc.
// See https://github.com/mtth/avsc/wiki/API#class-blockdecoderopts
const codecs = {
  // eslint-disable-next-line promise/prefer-await-to-callbacks
  snappy (buf, cb) {
    // Avro appends checksums to compressed blocks, which we skip here.
    // eslint-disable-next-line no-magic-numbers
    return snappy.uncompress(buf.slice(0, buf.length - 4), cb);
  }
};

/**
 * Creates a fileDecoder for the given file and options. Provide opts.readerSchema to decode
 * with a hard-coded schema as opposed to reading in the schema from the file.
 * @param {string} file - filepath to .avro file.
 * @param {Object} opts - Object containing decoding options. See https://github.com/mtth/avsc/wiki/API#class-blockdecoderopts
 * @returns {fileDecoder} fileDecoder for the given file according to the given options.
 */
const makeDecoder = (file, opts) => avro.createFileDecoder(
  file,
  {
    codecs,
    ...(opts && opts.readerSchema ? {readerSchema: opts.readerSchema} : {}),
  }
);

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
 * @param {Object} opts - Include field "printSchema" to print the schema.
 * @param {fileDecoder} decoder - decoder for a fixed .avro file
 * @returns {Promise} Promise resolving to Object representing schema of file corresponding to decoder.
 */
const getAvroSchema = (opts) => async (decoder) => {
  return new Promise((resolve) => {
    decoder.on('metadata', (type, codec, header) => {
      var meta = header.meta['avro.schema'];
      var metaString = meta.toString();
      var schema = JSON.parse(metaString);
      // if "printSchema" is set in opts, we print the original schema up to a max depth of 30.
      if (opts && opts.printSchema) {
        console.log(`\n\nThe original schema:\n${inspect(schema.fields, {depth: 30})}\n\n`);
      }
      resolve(schema);
    });
  });
};

/**
 * Returns a Promise resolving to the schema of the given file.
 * @param {string} file - filepath to .avro file
 * @returns {Promise} - Promise which resolves to the schema of the given file.
 */
const getOriginalSchema = async (file) => getAvroSchema()(makeDecoder(file));

/* <=== End of helper functions for reading .avro files ===> */
