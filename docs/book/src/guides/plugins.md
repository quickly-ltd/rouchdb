# Plugins

RouchDB has a plugin system that lets you hook into the document lifecycle. Plugins can modify documents before they are written, react to writes after they happen, and perform cleanup when a database is destroyed.

## The Plugin Trait

```rust
use rouchdb::{Plugin, Document, DocResult, OldDoc, Result, WriteContext};
use async_trait::async_trait;

#[async_trait]
pub trait Plugin: Send + Sync {
    /// The plugin name (used for identification).
    fn name(&self) -> &str;

    /// Called before documents are written. Can modify or reject documents.
    async fn before_write(&self, docs: &mut Vec<Document>) -> Result<()> {
        Ok(()) // default: no-op
    }

    /// Called for each document about to be written: CouchDB's
    /// `validate_doc_update(newDoc, oldDoc, userCtx)`. Rejects that document only.
    async fn validate_doc_update(
        &self,
        new_doc: &Document,
        old_doc: &OldDoc<'_>,
        ctx: &WriteContext,
    ) -> Result<()> {
        Ok(()) // default: no-op
    }

    /// Called after documents are written with the results.
    async fn after_write(&self, results: &[DocResult]) -> Result<()> {
        Ok(()) // default: no-op
    }

    /// Called when the database is destroyed.
    async fn on_destroy(&self) -> Result<()> {
        Ok(()) // default: no-op
    }
}
```

## Adding Plugins

Use `with_plugin()` to register a plugin on a database:

```rust
use rouchdb::Database;
use std::sync::Arc;

let my_plugin = Arc::new(TimestampPlugin);
let db = Database::memory("mydb").with_plugin(my_plugin);
```

Note: `with_plugin()` consumes `self` and returns a new `Database`, so use the builder pattern.

Multiple plugins can be added. They execute in registration order.

## Example: Automatic Timestamps

```rust
use rouchdb::{Plugin, Document, Result};
use async_trait::async_trait;

struct TimestampPlugin;

#[async_trait]
impl Plugin for TimestampPlugin {
    fn name(&self) -> &str { "timestamp" }

    async fn before_write(&self, docs: &mut Vec<Document>) -> Result<()> {
        for doc in docs.iter_mut() {
            if let Some(obj) = doc.data.as_object_mut() {
                obj.insert(
                    "updated_at".into(),
                    serde_json::json!(chrono::Utc::now().to_rfc3339()),
                );
            }
        }
        Ok(())
    }
}
```

## Example: Validation Plugin

Return an error from `before_write` to reject the entire batch:

```rust
use rouchdb::{Plugin, Document, Result, RouchError};
use async_trait::async_trait;

struct RequireTypeField;

#[async_trait]
impl Plugin for RequireTypeField {
    fn name(&self) -> &str { "require-type" }

    async fn before_write(&self, docs: &mut Vec<Document>) -> Result<()> {
        for doc in docs {
            if doc.data.get("type").is_none() && !doc.deleted {
                return Err(RouchError::Forbidden(
                    "All documents must have a 'type' field".into(),
                ));
            }
        }
        Ok(())
    }
}
```

## Example: `validate_doc_update`

`validate_doc_update` is CouchDB's validation function. It is called once per
document, after `before_write`, with:

- `ctx.user_ctx`: who writes, CouchDB's `userCtx` (`name` and `roles`);
- `ctx.new_edits`: `false` for a replicated revision;
- `old_doc`: the revision the write replaces, read only if you call
  `old_doc.get().await` (`None` for a new document or a new branch).

Returning `Forbidden`, `Unauthorized` or `BadRequest` rejects this document
only: `bulk_docs` and replication report it as `forbidden` (or
`unauthorized`) with your reason and write the others, and `put` returns the
error. Every document of a batch is validated before any is written.

```rust
use rouchdb::{Document, OldDoc, Plugin, Result, RouchError, WriteContext};
use async_trait::async_trait;

/// Documents of type "invoice" can be created but never changed, except by
/// a server admin.
struct ImmutableInvoices;

#[async_trait]
impl Plugin for ImmutableInvoices {
    fn name(&self) -> &str { "immutable-invoices" }

    async fn validate_doc_update(
        &self,
        new_doc: &Document,
        old_doc: &OldDoc<'_>,
        ctx: &WriteContext,
    ) -> Result<()> {
        if ctx.user_ctx.is_admin() || new_doc.data["type"] != "invoice" {
            return Ok(());
        }
        match old_doc.get().await? {
            Some(old) if !old.deleted => Err(RouchError::Forbidden("invoices are immutable".into())),
            _ => Ok(()),
        }
    }
}
```

### Who writes

A `Database` writes as `UserCtx::admin()` (`{name: null, roles: ["_admin"]}`,
the context of CouchDB's internal writes) unless you ask for another user:

```rust
use rouchdb::{Database, UserCtx};

let db = Database::memory("mydb");
let cashier = db.as_user(UserCtx::new(Some("cashier".into()), vec!["staff".into()]));
cashier.put("doc", serde_json::json!({})).await?; // validated as "cashier"
```

`as_user` returns a handle to the same database (storage, plugins and
indexes are shared). Replication into a database writes with that handle's
context, so `remote.replicate_to(&db.as_user(ctx))` (or
`db.as_user(ctx).replicate_from(&remote)`) validates every replicated
document as `ctx`, like CouchDB's replicator runs as the `user_ctx` of its
replication document.

`rouchdb-server` sets the context of each request from authentication: the
configured admin (`{name, roles: ["_admin"]}`), or the admin party
(`{name: null, roles: ["_admin"]}`) when no admin is configured. An embedder
that authenticates requests itself puts a `UserCtx` in the request's
extensions from a layer around the router; the server keeps it.

## Plugin Hooks

| Hook | When | Can Modify? | Can Reject? |
|------|------|-------------|-------------|
| `before_write` | Before `bulk_docs` writes | Yes (mutable `&mut Vec<Document>`) | Yes (return `Err`) |
| `validate_doc_update` | For each document, after `before_write` | No | Yes, that document only |
| `after_write` | After successful writes | No (read-only `&[DocResult]`) | Yes (return `Err`) |
| `on_destroy` | When `db.destroy()` is called | N/A | Yes (return `Err`) |

Plugins are called for all write paths: `put()`, `update()`, `remove()`, `post()`, `bulk_docs()`,
`put_attachment()`, `remove_attachment()` and documents replicated into the database.
For attachment writes and replicated documents `before_write` acts like CouchDB's
`validate_doc_update`: it sees the revision being written (for an attachment write, the
parent's body with the attachment added or removed) and can reject it, but its changes are
not stored.
