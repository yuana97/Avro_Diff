#! /usr/bin/env node

const commander = require('commander');

const {printKeyDiff} = require('../lib/index.js');

commander
  .arguments('<file1> <file2> <key>')
  .action(async function (file1, file2, key) {
    const keyArr = key.split(',');
    await printKeyDiff(file1, file2, keyArr);
  });

commander.parse(process.argv);
