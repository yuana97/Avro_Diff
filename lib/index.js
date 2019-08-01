"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.lexCompare = exports.constructKey = exports.extractRows = exports.keyDiffHelper = exports.keyDiff = exports.vennDiff = void 0;

require("core-js/modules/es6.symbol");

require("core-js/modules/web.dom.iterable");

require("core-js/modules/es6.array.sort");

require("core-js/modules/es6.regexp.split");

require("core-js/modules/es6.regexp.to-string");

var _deepObjectDiff = require("deep-object-diff");

var _avsc = _interopRequireDefault(require("avsc"));

var _snappy2 = _interopRequireDefault(require("snappy"));

var _util = require("util");

var _this = void 0;

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { keys.push.apply(keys, Object.getOwnPropertySymbols(object)); } if (enumerableOnly) keys = keys.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(source, true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(source).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

function _newArrowCheck(innerThis, boundThis) { if (innerThis !== boundThis) { throw new TypeError("Cannot instantiate an arrow function"); } }

// P0_FIELDS ("priority zero fields") are the high priority fields we want to decode.
// const P0_FIELDS = ['globalAssignmentId', 'globalStudentId', 'assignmentName', 'weeklyAggregates', 'percentage'];
const P0_FIELDS = ['studentId', 'assignmentId', 'assignmentName', 'submission']; // const P0_FIELDS = [];

/**
 * Given filepaths to an old and new avro file, prints to console
 * an object representing a venn diagram of the old and new avro files.
 * The object contains 3 fields "removed", "added", and "intersection"
 * each of which contains rows only in old, only in new, and in both
 * mapped to a count of how many copies there are.
 * @param {string} file1 - filepath to old .avro file.
 * @param {string} file2 - filepath to new .avro file.
 */

const vennDiff = async function vennDiff(file1, file2) {
  var _this2 = this;

  _newArrowCheck(this, _this);

  // read in the schema of file1 to a variable called result
  Promise.resolve(getOriginalSchema(file1)).then(function (result) {
    var _this3 = this;

    _newArrowCheck(this, _this2);

    // let newSchema be the intersection of result with P0_FIELDS
    const newSchema = _objectSpread({}, result, {
      fields: result.fields.filter(function (field) {
        _newArrowCheck(this, _this3);

        return P0_FIELDS.includes(field.name);
      }.bind(this))
    }); // let venn be the object to print


    let venn = {
      "removed": {},
      "added": {},
      "intersection": {}
    }; // stream the rows of file1 and file2 into venn according to vennParser

    readAvroFile(makeDecoder(file1, {
      readerSchema: newSchema
    }), venn, vennParser(1)).then(function () {
      var _this4 = this;

      _newArrowCheck(this, _this3);

      readAvroFile(makeDecoder(file2, {
        readerSchema: newSchema
      }), venn, vennParser(2)).then(function () {
        _newArrowCheck(this, _this4);

        // having streamed both files to venn, print venn.
        console.log(venn);
      }.bind(this));
    }.bind(this));
  }.bind(this));
}.bind(void 0);
/**
 * Parser function for vennDiff. Adds row to venn, updating fields "added"
 * "removed", "intersection" accordingly.
 * @param {number} num - Flag for old/new file. Pass in 1 if parsing old file and 2 if parsing new file.
 * @param {Object} venn - object with fields "removed", "added", "intersection" representing a venn diagram
 *                     - of the old and new files.
 * @param {Object} row - Row parsed from avro file.
 */


exports.vennDiff = vennDiff;

const vennParser = function vennParser(num) {
  var _this5 = this;

  _newArrowCheck(this, _this);

  return function (venn) {
    var _this6 = this;

    _newArrowCheck(this, _this5);

    return function (row) {
      _newArrowCheck(this, _this6);

      // let str be the string representation of row.
      // Note: It is more correct to use deep equals rather than string comparisons.
      // However isEqual which I imported is not working so I am using string comparison.
      let str = row.toString(); // if num === 1 then this is the first file and every row goes in the "removed" field.

      if (num === 1) {
        // increment count of this row
        venn["removed"][str] = venn["removed"][str] == null ? 1 : venn["removed"][str] + 1;
      } // if num !== 1 then this is the second file.
      else {
          // if str exists in "removed" remove one occurence of str in "removed" and add one occurence
          // of str in "intersection"
          if (venn["removed"][str]) {
            if (venn["removed"][str] === 1) {
              delete venn["removed"][str];
            } else {
              venn["removed"][str] = venn["removed"][str] - 1;
            } // increment count of this row


            venn["intersection"][str] = venn["intersection"][str] == null ? 1 : venn["intersection"][str] + 1;
          } // else add one occurence of str to "added"
          else {
              // increment count of this row
              venn["added"][str] = venn["added"][str] == null ? 1 : venn["added"][str] + 1;
            }
        }
    }.bind(this);
  }.bind(this);
}.bind(void 0);
/**
 * Prints an object representing a diff of file1 and file2 based on
 * the given key to console.
 * @param {string} file1 - filepath to old .avro file.
 * @param {string} file2 - filepath to new .avro file.
 * @param {string} key  - primary key to compare arr1 and arr2 on.
 *                      - for composite key enter the columns as a comma separated list
 *                      - for example "assignmentId,studentId"
 */


const keyDiff = async function keyDiff(file1, file2, key) {
  var _this7 = this;

  _newArrowCheck(this, _this);

  // extract rows from first file to arr1
  extractRows(file1).then(async function (arr1) {
    var _this8 = this;

    _newArrowCheck(this, _this7);

    // extract rows from second file to arr2
    extractRows(file2).then(async function (arr2) {
      _newArrowCheck(this, _this8);

      // print diff of arr1, arr2 based on key.
      const diff = await keyDiffHelper(arr1, arr2, key);
      console.log((0, _util.inspect)(diff, {
        depth: "Infinity"
      }));
      console.log("".concat(diff["removed"].length, " removed, ").concat(diff["added"].length, " added"));
      console.log("".concat(diff["changed"].length, " changed, ").concat(diff["unchanged"].length, " unchanged"));
    }.bind(this));
  }.bind(this));
}.bind(void 0);
/**
 * Prints an object representing a diff of file1 and file2 based on
 * the given key to console.
 * @param {Object[]} arr1 - array containing rows of old .avro file.
 * @param {Object[]} arr2 - array containing rows of new .avro file.
 * @param {string} key  - primary key to compare arr1 and arr2 on.
 *                      - for composite key enter the columns as a comma separated list
 *                      - for example "assignmentId,studentId"
 */


exports.keyDiff = keyDiff;

const keyDiffHelper = async function keyDiffHelper(arr1, arr2, key) {
  var _this9 = this;

  _newArrowCheck(this, _this);

  // keyArr is the array of columns which comprise a key for the avro files.
  const keyArr = key.split(","); // comparison function to order array based on key.
  // a,b are Objects which represent decoded rows of avro.

  const compare = function compare(a, b) {
    _newArrowCheck(this, _this9);

    let arrA = constructKey(a, keyArr);
    let arrB = constructKey(b, keyArr);
    return lexCompare(arrA, arrB);
  }.bind(this); // initialize object to print


  let output = {
    "removed": [],
    "added": [],
    "changed": [],
    "unchanged": []
  }; // sort arr1 and arr2 according to dictionary order of key

  arr1.sort(compare);
  arr2.sort(compare); // initialize pointers i j for arr1 arr2. While i j are not finished
  // iterating through arr1 arr2, update output.

  for (let i = 0, j = 0; i < arr1.length || j < arr2.length;) {
    const key1 = i === arr1.length ? undefined : constructKey(arr1[i], keyArr);
    const key2 = j === arr2.length ? undefined : constructKey(arr2[j], keyArr);
    const str1 = key1 == null ? null : key1.join();
    const str2 = key2 == null ? null : key2.join();
    const order = lexCompare(key1, key2); // order < 0 => arr1[i] precedes arr2[j] => arr1[i] unique => push data

    if (order < 0) {
      output["removed"].push(key + ": " + str1);
      output["removed"].push(arr1[i]);
      i++;
    } // order > 0 => arr2[j] precedes arr1[i] => arr2[j] unique => push data
    else if (order > 0) {
        output["added"].push(key + ": " + str2);
        output["added"].push(arr2[j]);
        j++;
      } // else arr1[i] corresponds to arr2[j]
      else {
          // If objects are not equal push the diff to "changed".
          const diffObj = (0, _deepObjectDiff.detailedDiff)(arr1[i], arr2[j]);

          if (!diffIsEmpty(diffObj)) {
            output["changed"].push(key + ": " + str1);
            output["changed"].push(diffObj);
          } // Else the objects are equal, push the ids to "unchanged".
          else {
              output["unchanged"].push(key + ": " + key1);
            } // increment both i and j.


          i++;
          j++;
        }
  } // at this point we have compared all elements of arr1, arr2 so we return.


  return output;
}.bind(void 0);
/**
 * Returns an array containing the rows of the given file.
 * @param {string} file - filepath to .avro file
 * @returns {Promise} - Promise which resolves to an Object[] containing the rows of the given file.
 */


exports.keyDiffHelper = keyDiffHelper;

const extractRows = async function extractRows(file) {
  _newArrowCheck(this, _this);

  var promise = new Promise(function (resolve) {
    var _this10 = this;

    // get original schema 'result' of file
    getOriginalSchema(file).then(function (result) {
      var _this11 = this;

      _newArrowCheck(this, _this10);

      // let newSchema be the intersection of result and P0_FIELDS
      const newSchema = _objectSpread({}, result, {
        fields: result.fields.filter(function (field) {
          _newArrowCheck(this, _this11);

          return P0_FIELDS.includes(field.name);
        }.bind(this))
      }); // read rows according to newSchema into an array and resolve this array.


      readAvroFile(makeDecoder(file, {
        readerSchema: newSchema
      }), [], extractRowsParser).then(function (arr) {
        _newArrowCheck(this, _this11);

        resolve(arr);
      }.bind(this));
    }.bind(this));
  });
  return promise;
}.bind(void 0);
/**
 * Parser function for extractRows. Pushes the given row to the given array.
 * @param {Object[]} arr - array to be filled with rows of a .avro file.
 * @param {Object} row - decoded row from a .avro file.
 */


exports.extractRows = extractRows;

const extractRowsParser = function extractRowsParser(arr) {
  var _this12 = this;

  _newArrowCheck(this, _this);

  return function (row) {
    _newArrowCheck(this, _this12);

    arr.push(row);
  }.bind(this);
}.bind(void 0);
/* <=== Misc helper functions ===> */

/**
 * Returns true if diffObj represents no differences and false otherwise.
 * @param {Object} diffObj - Object representing a diff using the deep-object-diff library
 * @returns {boolean} - returns true if diffObj is empty or all its fields are empty.
 */


const diffIsEmpty = function diffIsEmpty(diffObj) {
  _newArrowCheck(this, _this);

  return isEmpty(diffObj) || isEmpty(diffObj["added"]) && isEmpty(diffObj["deleted"]) && isEmpty(diffObj["updated"]);
}.bind(void 0);
/**
 * Checks if the given object is empty.
 * @param {Object} obj - Object to check for being empty.
 * @returns {boolean} - true if obj is empty or null and false otherwise.
 */


const isEmpty = function isEmpty(obj) {
  _newArrowCheck(this, _this);

  return obj == null || Object.keys(obj).length === 0;
}.bind(void 0);
/**
 * Constructs a composite key of row with respect to fields.
 * The composite key is an array of row["field"] where field iterates over fields.
 * @param {Object} row - decoded row from avro file.
 * @param {string[]} fields - fields which constitute a composite key for the avro file row was decoded from.
 * @returns {string[]} - returns array of row["field"] where field iterates over fields.
 */


const constructKey = function constructKey(row, fields) {
  var _this13 = this;

  _newArrowCheck(this, _this);

  if (row == null) return null;
  const result = fields.map(function (field) {
    _newArrowCheck(this, _this13);

    return String(row[field]);
  }.bind(this));
  return result;
}.bind(void 0);
/**
 * Lexicographic order for two arrays of strings.
 * Null/undefined are ahead of the order compared to any non-null object.
 * @param {string[]} arr1 - first array to compare
 * @param {string[]} arr2 - second array to compare
 * @returns {number}  returns a negative number if arr1 < arr2,
 *                    a positive number if arr1 > arr2, 0 otherwise
 */


exports.constructKey = constructKey;

const lexCompare = function lexCompare(arr1, arr2) {
  _newArrowCheck(this, _this);

  // null goes to the end of the ordering to make keyDiffHelper more elegant.
  if (arr1 == null && arr2 == null) return 0;
  if (arr1 == null) return 1;
  if (arr2 == null) return -1; // iterate len times to avoid null pointer exception

  const len = Math.min(arr1.length, arr2.length);

  for (let i = 0; i < len; i++) {
    // mismatch => return corresponding output.
    if (arr1[i] < arr2[i]) {
      return -1;
    }

    if (arr1[i] > arr2[i]) {
      return 1;
    }
  } // no return so far => one array is a prefix of the other.
  // return neg if arr1 is prefix, pos if arr2 is prefix, 0 if arrays are identical.


  return arr1.length - arr2.length;
}.bind(void 0);
/* <=== End of misc helper functions ===> */

/* <=== Helper functions for reading .avro files ===> */
// snappy codecs to be passed as field in opts into createFileDecoder from avsc.
// See https://github.com/mtth/avsc/wiki/API#class-blockdecoderopts


exports.lexCompare = lexCompare;
const codecs = {
  // eslint-disable-next-line promise/prefer-await-to-callbacks
  snappy: function snappy(buf, cb) {
    // Avro appends checksums to compressed blocks, which we skip here.
    // eslint-disable-next-line no-magic-numbers
    return _snappy2.default.uncompress(buf.slice(0, buf.length - 4), cb);
  }
};
/**
 * Creates a fileDecoder for the given file and options. Provide opts.readerSchema to decode
 * with a hard-coded schema as opposed to reading in the schema from the file.
 * @param {string} file - filepath to .avro file.
 * @param {Object} opts - Object containing decoding options. See https://github.com/mtth/avsc/wiki/API#class-blockdecoderopts
 * @returns {fileDecoder} fileDecoder for the given file according to the given options.
 */

const makeDecoder = function makeDecoder(file, opts) {
  _newArrowCheck(this, _this);

  return _avsc.default.createFileDecoder(file, _objectSpread({
    codecs: codecs
  }, opts && opts.readerSchema ? {
    readerSchema: opts.readerSchema
  } : {}));
}.bind(void 0);
/**
 * Returns a Promise resolving to responseObj after parsing decoder with a given parsing function.
 * @param {fileDecoder} decoder - fileDecoder which streams rows of a .avro file.
 * @param {Object} responseObj - Object passed into parser for each row.
 * @param {function} parser - function of function run on each row which is passed responseObj then the current row.
 *                          - See extractRowsParser and vennParser
 * @param {Object} opts - Object containing options for decoder. See https://github.com/mtth/avsc/wiki/API#class-blockdecoderopts
 * @returns {Promise} Promise which resolves to responseObj
 */


const readAvroFile = async function readAvroFile(decoder) {
  var _this14 = this;

  let responseObj = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
  let parser = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : function () {
    _newArrowCheck(this, _this14);
  }.bind(this);
  let opts = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : {};

  _newArrowCheck(this, _this);

  return new Promise(function (resolve) {
    var _this15 = this;

    _newArrowCheck(this, _this14);

    decoder.on('data', parser(responseObj, opts));
    decoder.on('end', function () {
      _newArrowCheck(this, _this15);

      resolve(responseObj);
    }.bind(this));
  }.bind(this));
}.bind(void 0);
/**
 * Returns a Promise resolving to the schema for the file corresponding to the passed in decoder.
 * @param {Object} opts - Include field "printSchema" to print the schema.
 * @param {fileDecoder} decoder - decoder for a fixed .avro file
 * @returns {Promise} Promise resolving to Object representing schema of file corresponding to decoder.
 */


const getAvroSchema = function getAvroSchema(opts) {
  var _this16 = this;

  _newArrowCheck(this, _this);

  return async function (decoder) {
    var _this17 = this;

    _newArrowCheck(this, _this16);

    return new Promise(function (resolve) {
      var _this18 = this;

      _newArrowCheck(this, _this17);

      decoder.on('metadata', function (type, codec, header) {
        _newArrowCheck(this, _this18);

        var meta = header.meta['avro.schema'];
        var metaString = meta.toString();
        var schema = JSON.parse(metaString); // if "printSchema" is set in opts, we print the original schema up to a max depth of 30.

        if (opts && opts.printSchema) {
          console.log("\n\nThe original schema:\n".concat((0, _util.inspect)(schema.fields, {
            depth: 30
          }), "\n\n"));
        }

        resolve(schema);
      }.bind(this));
    }.bind(this));
  }.bind(this);
}.bind(void 0);
/**
 * Returns a Promise resolving to the schema of the given file.
 * @param {string} file - filepath to .avro file
 * @returns {Promise} - Promise which resolves to the schema of the given file.
 */


const getOriginalSchema = async function getOriginalSchema(file) {
  _newArrowCheck(this, _this);

  return getAvroSchema()(makeDecoder(file));
}.bind(void 0);
/* <=== End of helper functions for reading .avro files ===> */