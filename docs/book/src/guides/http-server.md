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
| `POST` | `/{db}/_bulk_docs` | Atomic bulk document inserts/updates with `new_edits` support |
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
