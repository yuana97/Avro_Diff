#! /usr/bin/env node

const commander = require('commander');

const {printVennDiff} = require('../lib/index.js');

commander
  .arguments('<file1> <file2>')
  .action(function (file1, file2) {
    printVennDiff(file1, file2);
  });

commander.parse(process.argv);
