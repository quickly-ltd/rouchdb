const {
  memoryDb,
  remoteDb,
  bulkWrite,
  count,
  httpJson,
  errorDetails,
  runTest,
} = require('./common');

runTest('T9 — Tombstone ölçekli tam pull', async () => {
  const local = memoryDb(`t9-local-${Date.now()}`);
  const remote = remoteDb();
  const docs = Array.from({ length: 10000 }, (_, index) => ({
    _id: `t9-${String(index).padStart(5, '0')}`,
    scenario: 'T9',
    index,
  }));
  const seedStarted = Date.now();
  await bulkWrite(remote, docs, 500);
  const seeded = Date.now() - seedStarted;
  const all = await remote.allDocs({
    include_docs: true,
    startkey: 't9-',
    endkey: 't9-\uffff',
  });
  const deletionDocs = all.rows.slice(0, 5000).map((row) => ({
    _id: row.id,
    _rev: row.value.rev,
    _deleted: true,
  }));
  const deleteStarted = Date.now();
  const deletionResponses = await bulkWrite(remote, deletionDocs, 500);
  const deletionDuration = Date.now() - deleteStarted;
  const changes = await httpJson('/_changes?style=all_docs&since=0&limit=20000');
  const changesRows = changes.body?.results || [];
  const deletedRows = changesRows.filter((row) => row.deleted === true);
  const pullStarted = Date.now();
  let pull;
  try {
    pull = { ok: true, result: await local.replicate.from(remote, { batch_size: 100, batches_limit: 10 }) };
  } catch (error) {
    pull = { ok: false, error: errorDetails(error) };
  }
  return {
    pass: pull.ok && (await count(local)) === 5000 && deletedRows.length === 5000,
    seed_duration_ms: seeded,
    deletion_duration_ms: deletionDuration,
    seed_ok: docs.length,
    deletion_ok: deletionResponses.filter((item) => item.ok).length,
    changes_http_status: changes.status,
    changes_row_count: changesRows.length,
    changes_deleted_count: deletedRows.length,
    local_count_after_pull: await count(local),
    pull_duration_ms: Date.now() - pullStarted,
    pull,
  };
});
