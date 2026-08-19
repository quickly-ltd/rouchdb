const {
  memoryDb,
  remoteDb,
  getMaybe,
  httpJson,
  errorDetails,
  runTest,
} = require('./common');

async function seedBoth(local, remote, id, value) {
  await remote.put({ _id: id, scenario: 'T5', value });
  const document = await remote.get(id, { revs: true });
  await local.bulkDocs([document], { new_edits: false });
}

async function attemptSync(local, remote) {
  try {
    return { ok: true, result: await local.sync(remote, { batch_size: 100, batches_limit: 10 }) };
  } catch (error) {
    return { ok: false, error: errorDetails(error) };
  }
}

runTest('T5 — Tombstone yayılımı', async () => {
  const localToRemote = memoryDb(`t5-local-to-remote-${Date.now()}`);
  const remoteToLocal = memoryDb(`t5-remote-to-local-${Date.now()}`);
  const remote = remoteDb();
  await seedBoth(localToRemote, remote, 't5-local-delete', 'local-delete');
  await localToRemote.remove(await localToRemote.get('t5-local-delete'));
  const pushDeletion = await attemptSync(localToRemote, remote);
  const remoteAfterLocalDeletion = await getMaybe(remote, 't5-local-delete');
  await seedBoth(remoteToLocal, remote, 't5-remote-delete', 'remote-delete');
  const remoteDeleteResponse = await remote.remove(await remote.get('t5-remote-delete'));
  const pullDeletion = await attemptSync(remoteToLocal, remote);
  const localAfterRemoteDeletion = await getMaybe(remoteToLocal, 't5-remote-delete');
  const changes = await httpJson('/_changes?style=all_docs&since=0&limit=100');
  const deletedRows = changes.body?.results?.filter((row) => row.deleted === true) || [];
  return {
    pass: !remoteAfterLocalDeletion.found && !localAfterRemoteDeletion.found && deletedRows.length >= 2,
    push_deletion: pushDeletion,
    pull_deletion: pullDeletion,
    remote_after_local_deletion: remoteAfterLocalDeletion,
    local_after_remote_deletion: localAfterRemoteDeletion,
    remote_delete_response: remoteDeleteResponse,
    changes_http_status: changes.status,
    changes_deleted_rows: deletedRows,
    changes_body: changes.body,
  };
});
