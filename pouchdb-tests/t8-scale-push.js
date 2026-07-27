const {
  memoryDb,
  remoteDb,
  bulkWrite,
  count,
  runTest,
} = require('./common');

runTest('T8 — 10.000 doküman push', async () => {
  const local = memoryDb(`t8-local-${Date.now()}`);
  const remote = remoteDb();
  const docs = Array.from({ length: 10000 }, (_, index) => ({
    _id: `t8-${String(index).padStart(5, '0')}`,
    scenario: 'T8',
    index,
  }));
  await bulkWrite(local, docs, 500);
  const before = await remote.info();
  const replicationStarted = Date.now();
  const replication = await local.replicate.to(remote, { batch_size: 100, batches_limit: 10 });
  const after = await remote.info();
  return {
    pass: after.doc_count - before.doc_count === 10000,
    local_count: await count(local),
    remote_count_before: before.doc_count,
    remote_count_after: after.doc_count,
    inserted_delta: after.doc_count - before.doc_count,
    replication_duration_ms: Date.now() - replicationStarted,
    replication,
  };
});
