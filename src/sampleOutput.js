import {keyDiff, printKeyDiff} from './index.js';
import {setConfig} from './config.js'

const f1 = 'avro/sample_avro/userdata1.avro';
const f2 = 'avro/sample_avro/userdata2.avro';
const k = ['id'];

const opts = {
  "schema": null,
  "codecs": 'snappy',
  "keepFields": ['id', 'first_name', 'last_name', 'email'],
  "ignoreFields": null
};

const driver = async () => {
  let diff = await keyDiff(f1, f2, k);
  printKeyDiff(diff);
}

setConfig(opts);
driver();
