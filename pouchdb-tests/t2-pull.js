const {
  memoryDb,
  remoteDb,
  bulkWrite,
  count,
  runTest,
} = require('./common');

runTest('T2 — Tek yönlü pull', async () => {
  const local = memoryDb(`t2-local-${Date.now()}`);
  const remote = remoteDb();
  const docs = Array.from({ length: 1000 }, (_, index) => ({
    _id: `t2-${String(index).padStart(4, '0')}`,
    scenario: 'T2',
    index,
  }));
  const seedResponses = await bulkWrite(remote, docs);
  const replication = await local.replicate.from(remote, {
    batch_size: 100,
    batches_limit: 10,
  });
  const localCount = await count(local);
  return {
    pass: localCount === 1000,
    seed_ok: seedResponses.filter((item) => item.ok).length,
    local_count: localCount,
    replication,
  };
});
