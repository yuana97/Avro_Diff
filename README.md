# avro_diff

Allen Yuan
7/24/2019
Pandalytics Team @ Instructure Inc

## Setup
1. clone the repo
2. cd to the repo directory
3. run `yarn build`
4. run `node bin/key_diff.js <file1> <file2> <key>` or `node bin/venn_diff.js <file1> <file2>`
5. See package.json for some example scripts

## Dev Stuff
* src/index.js contains the main logic & functions which may be useful elsewhere
* avro folder contains test files
* bin contains files governing command line interface
