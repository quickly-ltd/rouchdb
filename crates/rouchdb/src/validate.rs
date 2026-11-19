//! Write validation context: who writes a document, and the revision it
//! replaces, as CouchDB passes them to `validate_doc_update(newDoc, oldDoc,
//! userCtx, secObj)`.

use rouchdb_core::adapter::Adapter;
use rouchdb_core::document::{AllDocsOptions, Document, GetOptions};
use rouchdb_core::error::{Result, RouchError};

/// The role CouchDB gives server admins.
pub const ADMIN_ROLE: &str = "_admin";

/// Who is writing: CouchDB's `userCtx` (`{name, roles}`).
///
/// A [`Database`](crate::Database) writes with the context it was created
/// with ([`UserCtx::admin`] by default, like CouchDB's internal writes) or
/// the one given to [`Database::as_user`](crate::Database::as_user).
/// Replication into a database writes with that database's context, so a
/// replication started from `db.as_user(ctx)` carries `ctx`. `rouchdb-server`
/// sets it per request from authentication.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct UserCtx {
    /// The user name; `None` for an anonymous user or an internal write.
    pub name: Option<String>,
    /// The user's roles; server admins have [`ADMIN_ROLE`].
    pub roles: Vec<String>,
}

impl UserCtx {
    /// A user with a name (or none) and roles.
    pub fn new(name: Option<String>, roles: Vec<String>) -> Self {
        Self { name, roles }
    }

    /// CouchDB's internal admin context, `{name: null, roles: ["_admin"]}`:
    /// the default of a [`Database`](crate::Database), and what CouchDB's
    /// "admin party" (no admin configured) gives every request.
    pub fn admin() -> Self {
        Self::new(None, vec![ADMIN_ROLE.to_string()])
    }

    /// An anonymous user, `{name: null, roles: []}`.
    pub fn anonymous() -> Self {
        Self::default()
    }

    /// Whether the user has `role`.
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }

    /// Whether the user is a server admin (has [`ADMIN_ROLE`]).
    pub fn is_admin(&self) -> bool {
        self.has_role(ADMIN_ROLE)
    }

    /// The context as CouchDB passes it to validation functions:
    /// `{"db": db, "name": name, "roles": roles}`.
    pub fn to_json(&self, db: &str) -> serde_json::Value {
        serde_json::json!({ "db": db, "name": self.name, "roles": self.roles })
    }
}

/// What a [`Plugin::validate_doc_update`](crate::Plugin::validate_doc_update)
/// call is validating a document for.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct WriteContext {
    /// Who is writing.
    pub user_ctx: UserCtx,
    /// `false` for a replicated write (`new_edits: false`), whose revision
    /// comes from the source; `true` for an edit.
    pub new_edits: bool,
}

impl WriteContext {
    pub fn new(user_ctx: UserCtx, new_edits: bool) -> Self {
        Self {
            user_ctx,
            new_edits,
        }
    }
}

/// The revision a written document replaces (CouchDB's `oldDoc`), read from
/// the database only when a plugin asks for it, and at most once.
///
/// It is read before any document of the write is stored, so every document
/// of a batch is validated against the database as it was before the batch.
pub struct OldDoc<'a> {
    adapter: &'a dyn Adapter,
    new_doc: &'a Document,
    new_edits: bool,
    loaded: tokio::sync::OnceCell<Option<Document>>,
}

impl<'a> OldDoc<'a> {
    pub(crate) fn new(adapter: &'a dyn Adapter, new_doc: &'a Document, new_edits: bool) -> Self {
        Self {
            adapter,
            new_doc,
            new_edits,
            loaded: tokio::sync::OnceCell::new(),
        }
    }

    /// The replaced revision, `None` when the write creates the document
    /// (or a new branch of it). As in CouchDB:
    ///
    /// - an edit with a `_rev` replaces that revision (`None` if it is not
    ///   in the database: the write will then conflict);
    /// - an edit without `_rev` replaces the winning revision, including a
    ///   deleted one (a document with `deleted: true`);
    /// - a replicated revision replaces the newest revision of its
    ///   `_revisions` history that the database has.
    ///
    /// A revision whose body was removed by compaction counts as missing.
    pub async fn get(&self) -> Result<Option<&Document>> {
        self.loaded
            .get_or_try_init(|| load_old_doc(self.adapter, self.new_doc, self.new_edits))
            .await
            .map(Option::as_ref)
    }
}

/// A revision of `id`, or `None` if the database does not have it.
async fn revision(adapter: &dyn Adapter, id: &str, rev: &str) -> Result<Option<Document>> {
    let opts = GetOptions {
        rev: Some(rev.to_string()),
        ..Default::default()
    };
    match adapter.get(id, opts).await {
        Ok(doc) => Ok(Some(doc)),
        Err(RouchError::NotFound(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

async fn load_old_doc(
    adapter: &dyn Adapter,
    doc: &Document,
    new_edits: bool,
) -> Result<Option<Document>> {
    let id = doc.id.as_str();
    if id.is_empty() {
        return Ok(None);
    }
    if !new_edits {
        // Newest first, skipping the revision being written.
        for rev in replicated_ancestors(doc).into_iter().skip(1) {
            if let Some(old) = revision(adapter, id, &rev).await? {
                return Ok(Some(old));
            }
        }
        return Ok(None);
    }
    if let Some(rev) = &doc.rev {
        return revision(adapter, id, &rev.to_string()).await;
    }
    match adapter.get(id, GetOptions::default()).await {
        Ok(doc) => Ok(Some(doc)),
        Err(RouchError::NotFound(_)) => {
            // A deleted document is not found by a plain read: find its
            // winning revision, then read that.
            let rows = adapter
                .all_docs(AllDocsOptions {
                    keys: Some(vec![id.to_string()]),
                    ..AllDocsOptions::new()
                })
                .await?;
            let deleted_rev = rows
                .rows
                .into_iter()
                .find_map(|row| row.value.filter(|v| v.deleted == Some(true)).map(|v| v.rev));
            match deleted_rev {
                Some(rev) => revision(adapter, id, &rev).await,
                None => Ok(None),
            }
        }
        Err(e) => Err(e),
    }
}

/// The revision ids of a replicated document's history, newest (its own)
/// first: from `_revisions`, or just its `_rev` without one.
fn replicated_ancestors(doc: &Document) -> Vec<String> {
    let history = doc.data.get("_revisions");
    let start = history
        .and_then(|h| h.get("start"))
        .and_then(|s| s.as_u64());
    let ids = history
        .and_then(|h| h.get("ids"))
        .and_then(|ids| ids.as_array());
    match (start, ids) {
        (Some(start), Some(ids)) => ids
            .iter()
            .filter_map(|id| id.as_str())
            .enumerate()
            .take_while(|(i, _)| (*i as u64) < start)
            .map(|(i, id)| format!("{}-{}", start - i as u64, id))
            .collect(),
        _ => doc.rev.iter().map(|r| r.to_string()).collect(),
    }
}
