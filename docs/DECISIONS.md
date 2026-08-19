# Architectural Decisions Log

## ADR 001: HTTP Server PouchDB 9 Live Replication Compatibility

- **Status:** Accepted
- **Date:** 2026-07-27
- **Context:** `rouchdb-server` needed to support full PouchDB 9 client replication protocol surface over HTTP. PouchDB 9 initial probes and live streaming tests (T1-T9) were blocked by missing endpoints (`/{db}/` trailing slash, `/{db}/_revs_diff`, `/{db}/_bulk_get`, `/{db}/_local/{id}`), as well as missing longpoll/continuous live feed implementations in `/{db}/_changes`.

### Decision

1. **Trailing-Slash Database Route:** Registered `/{db}/` alongside `/{db}` in `rouchdb-server` routes matching CouchDB 3.x conventions.
2. **Revisions Diffing (`_revs_diff`):** Wired `POST /{db}/_revs_diff` directly to `Adapter::revs_diff` returning missing revision sets.
3. **Bulk Fetching (`_bulk_get`):** Wired `POST /{db}/_bulk_get` to `Adapter::bulk_get` returning CouchDB 3.x bulk document responses.
4. **Local Checkpoint CRUD (`_local`):** Mounted `GET`, `PUT`, `DELETE /{db}/_local/{*id}` ahead of 3-segment attachment routes. Automatically handle `_id` prefix and rev generation (`0-1`, `0-2`, ...).
5. **Live Changes Feeds (`_changes`):** Implemented longpoll mode (blocking loop with fast 100ms interval check and configurable timeout) and continuous streaming mode (`axum::body::Body::from_stream` over `live_changes_events` with line-delimited JSON and heartbeat signals).
6. **Conflict Surfacing (`_conflicts`):** Ensured non-winning leaves are returned under `_conflicts` in `GET /{db}/{docid}?conflicts=true` and `_all_docs?conflicts=true`.

### Consequences

- All 9 PouchDB 9 client integration tests (T1–T9) pass 100% cleanly.
- `rouchdb-server` maintains zero breaking changes to existing core `Adapter` trait interfaces.
