const {
  memoryDb,
  remoteDb,
  sleep,
  errorDetails,
  runTest,
} = require('./common');

runTest('T4 — CANLI sync', async () => {
  const local = memoryDb(`t4-local-${Date.now()}`);
  const remote = remoteDb();
  const startedAt = Date.now();
  const errors = [];
  const writes = [];
  const arrivals = new Map();
  const completedAt = [];
  let sync;
  try {
    sync = local.sync(remote, {
      live: true,
      retry: true,
      heartbeat: 10000,
      timeout: false,
      batch_size: 100,
      batches_limit: 10,
    });
    sync.on('error', (error) => errors.push({ at_ms: Date.now() - startedAt, error: errorDetails(error) }));
    sync.on('complete', () => completedAt.push(Date.now() - startedAt));
    let polling = false;
    const poll = setInterval(async () => {
      if (polling) return;
      polling = true;
      try {
        for (const write of writes) {
          if (arrivals.has(write.id)) continue;
          try {
            await local.get(write.id);
            arrivals.set(write.id, Date.now());
          } catch (error) {
            if (error.status !== 404) errors.push({ at_ms: Date.now() - startedAt, error: errorDetails(error) });
          }
        }
      } finally {
        polling = false;
      }
    }, 250);
    for (let index = 0; index < 8; index += 1) {
      await sleep(10000);
      const id = `t4-${String(index).padStart(2, '0')}`;
      const writeAt = Date.now();
      try {
        const response = await remote.put({ _id: id, scenario: 'T4', index });
        writes.push({ id, index, write_at_ms: writeAt - startedAt, rev: response.rev });
      } catch (error) {
        writes.push({ id, index, write_at_ms: writeAt - startedAt, error: errorDetails(error) });
      }
    }
    await sleep(10000);
    clearInterval(poll);
  } finally {
    if (sync) sync.cancel();
  }
  const measurements = writes.map((write) => {
    const arrival = arrivals.get(write.id);
    return {
      ...write,
      arrived: Boolean(arrival),
      latency_ms: arrival ? arrival - startedAt - write.write_at_ms : null,
    };
  });
  const successfulWrites = measurements.filter((item) => item.rev);
  const withinLimit = successfulWrites.every((item) => item.arrived && item.latency_ms < 15000);
  return {
    pass: errors.length === 0 && completedAt.length === 0 && successfulWrites.length === 8 && withinLimit,
    duration_ms_observed: Date.now() - startedAt,
    error_count: errors.length,
    errors,
    writes: measurements,
    completed_before_cancel_ms: completedAt,
    sync_stayed_live: completedAt.length === 0,
    acceptance: {
      ninety_seconds_without_error: errors.length === 0,
      every_document_under_15_seconds: withinLimit,
      eight_writes_succeeded: successfulWrites.length === 8,
    },
  };
});
