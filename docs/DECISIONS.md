# Architectural Decisions Log

## ADR 001: HTTP Server PouchDB 9 Live Replication Compatibility

- **Status:** Accepted
- **Date:** 2026-07-27
- **Context:** `rouchdb-server` needed to support full PouchDB 9 client replication protocol surface over HTTP. PouchDB 9 initial probes and live streaming tests (T1-T9) were blocked by missing endpoints (`/{db}/` trailing slash, `/{db}/_revs_diff`, `/{db}/_bulk_get`, `/{db}/_local/{id}`), as well as missing longpoll/continuous live feed implementations in `/{db}/_changes`.

### Decision

1. **Trailing-Slash Database Route:** Registered `/{db}/` alongside `/{db}` in `rouchdb-server` routes matching CouchDB 3.x conventions.
2. **Revisions Diffing (`_revs_diff`):** Wired `POST /{db}/_revs_diff` directly to `Adapter::revs_diff` returning missing revision sets.
3. **Bulk Fetching (`_bulk_get`):** Wired `POST /{db}/_bulk_get` to `Adapter::bulk_get` returning CouchDB 3.x bulk document responses.
4. **Local Checkpoint CRUD (`_local`):** Mount `GET`, `PUT`, `DELETE /{db}/_local/{id}` ahead of attachment routes. Handle the `_id` prefix and local revision counters (`0-1`, `0-2`, ...).
5. **Live Changes Feeds (`_changes`):** Use upstream 0.5.1's request-owned streaming implementation, with write notifications, timeouts and heartbeat handling. Longpoll returns a batch when a matching change arrives; continuous feeds retain their stream state until disconnection or completion.
6. **Conflict Surfacing (`_conflicts`):** Ensured non-winning leaves are returned under `_conflicts` in `GET /{db}/{docid}?conflicts=true` and `_all_docs?conflicts=true`.

### Consequences

- The nine PouchDB client scenarios (T1–T9) exercise the HTTP replication surface; Rust tests cover the storage and replication contracts separately.
- The core and facade APIs follow upstream 0.5.1. See the book's 0.4-to-0.5 upgrade guide for API and on-disk format changes.

## ADR 002: Preserve Local Features on the Upstream 0.5.1 Base

- **Status:** Accepted
- **Date:** 2026-11-14
- **Decision:** Keep upstream's storage, authentication, checkpoint and changes-feed implementations, replacing the fork's overlapping handlers. Retain the local `ActiveTasks` registry, legacy `sync_status_summary`, PouchDB scenarios, secret-scanning configuration and GitNexus instructions.
- **Router API:** Keep upstream's `build_router(db, config)` and expose `build_router_with_tasks(db, config, tasks)` for embedders sharing a live-replication registry.
- **Progress API:** `ReplicationEvent::Change` includes read, successful-write and write-failure counters, cumulative within a pass. The registry accumulates per-pass differences to preserve lifetime totals without double counting.
- **Data:** Git integration does not upgrade existing redb files. On-disk upgrades remain a separate explicit operation with a verified backup.
