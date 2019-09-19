#! /usr/bin/env node

const commander = require('commander');

const {printKeyDiff, keyDiff} = require('../lib/index.js');

commander
  .arguments('<oldFile> <newFile> <key>')
  .action(async function (oldFile, newFile, key) {
    const keyArr = key.split(',');
    const diff = await keyDiff(oldFile, newFile, keyArr);
    printKeyDiff(diff);
  });

commander.parse(process.argv);
