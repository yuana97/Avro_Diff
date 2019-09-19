#! /usr/bin/env node

const commander = require('commander');

const {printVennDiff, vennDiff} = require('../lib/index.js');

commander
  .arguments('<oldFile> <newFile>')
  .action(async function (oldFile, newFile) {
    const diff = await vennDiff(oldFile, newFile);
    printVennDiff(diff);
  });

commander.parse(process.argv);
