# HTTP Server & PouchDB Replication Guide

`rouchdb-server` provides a CouchDB 3.x-compatible REST HTTP server built on top of Axum. It enables PouchDB 9 clients, Fauxton web UI, and `curl` to connect directly to RouchDB backends.

## Starting the Server

```bash
# Compile and run
cargo run --release --bin rouchdb-server -- mydatabase.redb --port 5984 --host 127.0.0.1 --db-name appserver
```

## Fauxton Setup

To use the Fauxton Web UI:

```bash
# Download Fauxton assets into static folder
bash scripts/download-fauxton.sh

# Open Fauxton dashboard
open http://127.0.0.1:5984/_utils/
```

## Endpoint Inventory

| HTTP Method | Route | Description |
|-------------|-------|-------------|
| `GET` | `/` | Server welcome banner, CouchDB 3.3.3 emulation version |
| `GET`, `PUT`, `POST`, `DELETE` | `/{db}`, `/{db}/` | Database info, creation, doc creation, database deletion |
| `POST` | `/{db}/_revs_diff` | Revision diffing for replication missing revision checks |
| `POST` | `/{db}/_bulk_get` | Bulk fetching documents by ID and revision |
| `GET`, `PUT`, `DELETE` | `/{db}/_local/{id}` | Non-replicated local checkpoint documents |
| `GET`, `POST` | `/{db}/_all_docs` | Query all documents with startkey, endkey, keys, and conflict filtering |
| `POST` | `/{db}/_bulk_docs` | Bulk document inserts/updates with per-document results and `new_edits` support |
| `GET`, `POST` | `/{db}/_changes` | Real-time changes feed supporting `normal`, `longpoll`, and `continuous` feeds |
| `GET`, `PUT`, `DELETE` | `/{db}/{docid}` | Document CRUD operations |
| `GET`, `PUT`, `DELETE` | `/{db}/{docid}/{attname}` | Attachment storage and retrieval |

## PouchDB Client Replication Example

```javascript
const PouchDB = require('pouchdb');

// Connect local memory/indexeddb PouchDB to remote rouchdb-server
const local = new PouchDB('local_db');
const remote = new PouchDB('http://127.0.0.1:5984/appserver');

// One-shot replication
local.replicate.to(remote).then((info) => {
  console.log('Replication complete', info);
});

// Live two-way synchronization
const sync = PouchDB.sync(local, remote, {
  live: true,
  retry: true
}).on('change', (info) => {
  console.log('Synchronized change', info);
}).on('error', (err) => {
  console.error('Sync error', err);
});
```

## Live Feed Semantics

- **`normal`**: Returns immediately with existing changes since `since`.
- **`longpoll`**: Blocks until new changes occur or `timeout` (default 30,000ms) elapses. When a document is written, it returns the single batch immediately.
- **`continuous`**: Keeps an open HTTP stream yielding line-delimited (`\n`) JSON change events as they happen, emitting `\n` keep-alive signals according to `heartbeat`.

## Embedded Replication Task Tracking

`build_router(db, &config)` uses an empty task registry. Embedders starting live
replication should use `build_router_with_tasks(db, &config, tasks.clone())` and
start each direction with the same `tasks.start_live_replication(...)` registry.
`GET /_active_tasks` then reports real `docs_read`, `docs_written`,
`doc_write_failures`, status and error information. The router keeps the same
authentication, Host and CORS checks as the default router.

This replaces the fork's 0.4 three-argument `build_router` call. The registry's
`sync_status_summary(remote_label)` remains available to embedders serving a
legacy sync-status response.

`ReplicationEvent::Change` counters are cumulative within one replication pass;
`Active` starts a new pass. `ActiveTasks` accumulates differences within each
pass, so live polling and multiple batches do not inflate lifetime totals.
Keep the returned replication handle alive; cancellation removes the task from
the registry when the replication driver finishes.

## Existing Databases and Network Access

Files written by 0.4 require the explicit, backed-up upgrade described in
[Upgrading 0.4 to 0.5](../upgrading/0.4-to-0.5.md). Starting this server does not
silently migrate them. Test the upgrade on a copy before using an existing file.

The default bind is loopback, with CORS disabled. Binding to a non-loopback
address requires admin credentials unless `--allow-unauthenticated` is explicitly
set. See [Adapters](adapters.md) for the HTTP client options.

## Local PouchDB Test Suite

Build `rouchdb-server` and run `bash run-tests.sh`. The script resolves the
repository relative to itself, uses temporary databases and ephemeral ports,
and exits non-zero if any scenario fails. `ROUCHDB_SERVER_BIN` selects another
server binary and `ROUCHDB_TEST_OUTPUT_DIR` selects the results directory.
