# avro_diff

Allen Yuan
7/24/2019
Pandalytics Team @ Instructure Inc

## Setup
1. clone the repo
2. cd to the repo directory
3. run `npm install`
4. set desired configuration options
5. run `yarn build && node bin/key_diff.js <file1> <file2> <key>` or `yarn build && node bin/venn_diff.js <file1> <file2>`
    a. file1 and file2 are paths from current directory to old and new avro files respectively
    a. key is a comma separated list of fields common to file1 and file2 which comprise a key to compare the two files.

## Configuration
1. src/config.js contains an object called CONFIG which sets configuration options for Avro Diff.
    *   schema: Put a schema object in this field. Alternatively set it to null and Avro Diff will read the schema from the file.
    *   codecs: Put 'snappy' here if this avro file is compressed using snappy. Put null in here if the avro file is compressed with
        null codec or deflate codec.
    *   keepFields: Put an array of fields to decode here. Alternatively set it to null and Avro Diff will try to decode all fields.
    *   ignoreFields: Put an array of fields to ignore here. Alternatively set it to null and Avro Diff will try to decode all fields.
2. Manually modify CONFIG for command line usage. For automated data tests pass a config object to the setConfig function.
    * See src/sampleDataTestNull.js, src/sampleDataTestSnappy.js, src/sampleOutput.js for examples.

## Known issues
1. Including large longs in your avro file will cause a precision loss. See https://github.com/mtth/avsc/wiki/Advanced-usage for how to safely use arbitrarily large longs.
