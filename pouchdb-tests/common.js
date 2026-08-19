const PouchDB = require('pouchdb');
const memoryAdapter = require('pouchdb-adapter-memory');

PouchDB.plugin(memoryAdapter);

const REMOTE_URL = process.env.REMOTE_URL || 'http://127.0.0.1:15986/appserver';

function memoryDb(name) {
  return new PouchDB(name, { adapter: 'memory' });
}

function remoteDb() {
  return new PouchDB(REMOTE_URL);
}

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function errorDetails(error) {
  return {
    name: error?.name,
    message: error?.message || String(error),
    status: error?.status,
    error: error?.error,
    reason: error?.reason,
    result: error?.result,
    stack: error?.stack,
  };
}

async function runTest(test, body) {
  const started = Date.now();
  try {
    const details = await body();
    const passed = details.pass !== false;
    const output = {
      test,
      status: passed ? 'GEÇTİ' : 'KALDI',
      duration_ms: Date.now() - started,
      ...details,
    };
    delete output.pass;
    process.stdout.write(`${JSON.stringify(output)}\n`);
    if (!passed) process.exitCode = 1;
  } catch (error) {
    process.stdout.write(`${JSON.stringify({
      test,
      status: 'KALDI',
      duration_ms: Date.now() - started,
      error: errorDetails(error),
    })}\n`);
    process.exitCode = 1;
  }
}

async function bulkWrite(db, docs, batchSize = 100) {
  const responses = [];
  for (let index = 0; index < docs.length; index += batchSize) {
    responses.push(...await db.bulkDocs(docs.slice(index, index + batchSize)));
  }
  return responses;
}

async function count(db) {
  return (await db.info()).doc_count;
}

async function getMaybe(db, id, options = {}) {
  try {
    return { found: true, doc: await db.get(id, options) };
  } catch (error) {
    if (error.status === 404 || error.name === 'not_found') return { found: false, error: errorDetails(error) };
    throw error;
  }
}

async function httpJson(path, options = {}) {
  const response = await fetch(`${REMOTE_URL}${path}`, options);
  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = text;
  }
  return { status: response.status, body, text };
}

function replicationSummary(replication) {
  return new Promise((resolve, reject) => {
    const changes = [];
    const errors = [];
    replication.on('change', (change) => changes.push(change));
    replication.on('error', (error) => errors.push(errorDetails(error)));
    replication.on('complete', (info) => resolve({ changes, errors, complete: info }));
    replication.on('error', (error) => reject(Object.assign(new Error(error.message || String(error)), {
      replicationErrors: errors,
      replicationChanges: changes,
      original: errorDetails(error),
    })));
  });
}

module.exports = {
  REMOTE_URL,
  PouchDB,
  memoryDb,
  remoteDb,
  sleep,
  errorDetails,
  runTest,
  bulkWrite,
  count,
  getMaybe,
  httpJson,
  replicationSummary,
};
