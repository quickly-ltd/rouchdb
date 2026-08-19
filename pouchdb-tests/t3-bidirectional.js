const {
  memoryDb,
  remoteDb,
  getMaybe,
  runTest,
} = require('./common');

runTest('T3 — Çift yönlü sync', async () => {
  const localA = memoryDb(`t3-a-${Date.now()}`);
  const localB = memoryDb(`t3-b-${Date.now()}`);
  const remote = remoteDb();
  await localA.put({ _id: 't3-a-document', scenario: 'T3', value: 'A' });
  const firstSync = await localA.sync(remote, { batch_size: 100, batches_limit: 10 });
  const secondSync = await localB.sync(remote, { batch_size: 100, batches_limit: 10 });
  const bDocument = await getMaybe(localB, 't3-a-document');
  return {
    pass: bDocument.found && bDocument.doc.value === 'A',
    first_sync: firstSync,
    second_sync: secondSync,
    b_document: bDocument,
  };
});
