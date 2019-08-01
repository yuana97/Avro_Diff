#! /usr/bin/env node

const commander = require('commander');

const {keyDiff} = require('../lib/index.js');

commander
  .arguments('<file1> <file2> <key>')
  .action(function (file1, file2, key) {
    keyDiff(file1, file2, key);
  });

commander.parse(process.argv);