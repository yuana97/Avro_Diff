import {keyDiff, printKeyDiff} from './index.js';
import {inspect} from 'util';

export const keyCollisionTest = async (oldFile, newFile, key) => {
  const diff = await keyDiff(oldFile, newFile, key);
  printKeyDiff(diff);

  // set containing ids
  const idSet = new Set();

  // we assume id's don't contain commas then a comma is a sufficient delimiter.
  // replace delimiter with any string that provides a hash for the array
  let delimiter = ',';

  Object.values(diff).forEach((category) => {
    category.forEach((jsonDiff) => {
      const id = jsonDiff.id;
      const idStr = id.join(delimiter);
      if (idSet.has(idStr)) {
        console.log(`Error: ${inspect(id)} is a duplicate key`)
      }
      idSet.add(idStr);
    });
  });
}
