const {
  memoryDb,
  remoteDb,
  getMaybe,
  errorDetails,
  runTest,
} = require('./common');

runTest('T6 — Conflict davranışı', async () => {
  const local = memoryDb(`t6-local-${Date.now()}`);
  const remote = remoteDb();
  const id = 't6-conflict';
  await remote.put({ _id: id, scenario: 'T6', value: 'base' });
  const base = await remote.get(id, { revs: true });
  await local.bulkDocs([base], { new_edits: false });
  const localBranch = await local.put({ _id: id, _rev: base._rev, scenario: 'T6', value: 'local-branch' });
  const remoteBranch = await remote.put({ _id: id, _rev: base._rev, scenario: 'T6', value: 'remote-branch' });
  let sync;
  try {
    sync = { ok: true, result: await local.sync(remote, { batch_size: 100, batches_limit: 10 }) };
  } catch (error) {
    sync = { ok: false, error: errorDetails(error) };
  }
  const localAfter = await getMaybe(local, id, { conflicts: true });
  const remoteAfter = await getMaybe(remote, id, { conflicts: true });
  const localConflicts = localAfter.found ? (localAfter.doc._conflicts || []) : [];
  const remoteConflicts = remoteAfter.found ? (remoteAfter.doc._conflicts || []) : [];
  return {
    pass: sync.ok && localAfter.found && remoteAfter.found && localConflicts.length > 0 && remoteConflicts.length > 0 && localAfter.doc._rev === remoteAfter.doc._rev,
    sync,
    local_branch: localBranch,
    remote_branch: remoteBranch,
    local_after: localAfter,
    remote_after: remoteAfter,
    local_conflicts: localConflicts,
    remote_conflicts: remoteConflicts,
    winning_revisions_equal: localAfter.found && remoteAfter.found && localAfter.doc._rev === remoteAfter.doc._rev,
  };
});
