#! /usr/bin/env node

const commander = require('commander');

const {vennDiff, keyDiff} = require('../lib/index.js');

commander
  .option('-v, --venn <str>', 'Enter comma separated pair <file1>,<file2>')
  .option('-k --key <str>', 'Enter comma separated triple <file1>,<file2>,<key>')
;

commander.parse(process.argv);

console.log(commander.venn);
