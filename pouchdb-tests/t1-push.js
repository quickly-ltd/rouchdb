const {
  memoryDb,
  remoteDb,
  bulkWrite,
  count,
  runTest,
} = require('./common');

runTest('T1 — Tek yönlü push', async () => {
  const local = memoryDb(`t1-local-${Date.now()}`);
  const remote = remoteDb();
  const docs = Array.from({ length: 1000 }, (_, index) => ({
    _id: `t1-${String(index).padStart(4, '0')}`,
    scenario: 'T1',
    index,
  }));
  await bulkWrite(local, docs);
  const replication = await local.replicate.to(remote, {
    batch_size: 100,
    batches_limit: 10,
  });
  const remoteCount = await count(remote);
  return {
    pass: remoteCount === 1000,
    local_count: await count(local),
    remote_count: remoteCount,
    replication,
  };
});
