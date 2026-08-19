const {
  memoryDb,
  remoteDb,
  errorDetails,
  httpJson,
  runTest,
} = require('./common');

async function attempt(replication) {
  try {
    return { ok: true, result: await replication };
  } catch (error) {
    return { ok: false, error: errorDetails(error) };
  }
}

runTest('T7 — Checkpoint ve artımlı devam', async () => {
  const local = memoryDb(`t7-local-${Date.now()}`);
  const remote = remoteDb();
  await local.put({ _id: 't7-checkpoint-document', scenario: 'T7', value: 1 });
  const first = await attempt(local.replicate.to(remote, { batch_size: 100, batches_limit: 10 }));
  const localCheckpointRowsAfterFirst = await local.allDocs({
    include_docs: true,
    startkey: '_local/',
    endkey: '_local/\uffff',
  });
  const remoteCheckpointProbe = await httpJson('/_local/t7-checkpoint-probe');
  const second = await attempt(local.replicate.to(remote, { batch_size: 100, batches_limit: 10 }));
  const localCheckpointRowsAfterSecond = await local.allDocs({
    include_docs: true,
    startkey: '_local/',
    endkey: '_local/\uffff',
  });
  return {
    pass: first.ok && second.ok && second.result?.docs_read === 0 && remoteCheckpointProbe.status === 404,
    first_replication: first,
    second_replication: second,
    local_checkpoints_after_first: localCheckpointRowsAfterFirst.rows,
    local_checkpoints_after_second: localCheckpointRowsAfterSecond.rows,
    remote_checkpoint_probe: remoteCheckpointProbe,
  };
});
