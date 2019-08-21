const {keyDiff} = require('./index.js');

const file1 = 'avro/submissions/studentAssignmentsOld.avro';
const file2 = 'avro/submissions/studentAssignmentsNew.avro';
const key = ['studentId', 'assignmentId'];

test('keys are unique', async () => {
  const diff = await keyDiff(file1, file2, key);

  // set containing ids
  const ids = new Set();

  // we assume id's don't contain commas then a comma is a sufficient delimiter.
  // replace delimiter with any string that provides a hash for the array
  let delimiter = ',';

  for (var category in diff) {
    for (let i = 0; i < diff[category].length || 0; i++) {
      let id = diff[category][i].id;
      let hash = id.join(delimiter);
      expect(ids.has(hash)).toBe(false);
      ids.add(hash);
    }
  }
});
