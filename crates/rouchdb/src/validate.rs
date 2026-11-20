//! Write validation, as CouchDB does it (`couch_db:validate_doc_update` and
//! `prep_and_validate_update` / `prep_and_validate_replicated_updates`):
//! who writes a document, the revision it replaces, and the database's
//! security object, given to `validate_doc_update(newDoc, oldDoc, userCtx,
//! secObj)`.

use std::collections::HashMap;

use rouchdb_core::adapter::Adapter;
use rouchdb_core::document::{Document, GetOptions, SecurityDocument, normalize_rev_hash};
use rouchdb_core::error::{Result, RouchError};

/// The role CouchDB gives server admins.
pub const ADMIN_ROLE: &str = "_admin";

/// CouchDB's reason for a design document written by a non-admin.
pub(crate) const NOT_ADMIN_REASON: &str = "You are not a db or server admin.";

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

    /// Whether the user is an admin of a database with this security
    /// object: a server admin, or listed in its `admins` by name or role
    /// (CouchDB's `check_is_admin`).
    pub fn is_db_admin(&self, security: &SecurityDocument) -> bool {
        self.is_admin()
            || self
                .name
                .as_ref()
                .is_some_and(|name| security.admins.names.contains(name))
            || self
                .roles
                .iter()
                .any(|role| security.admins.roles.contains(role))
    }

    /// The context as CouchDB passes it to validation functions:
    /// `{"db": db, "name": name, "roles": roles}`.
    pub fn to_json(&self, db: &str) -> serde_json::Value {
        serde_json::json!({ "db": db, "name": self.name, "roles": self.roles })
    }
}

/// What a [`Plugin::validate_doc_update`](crate::Plugin::validate_doc_update)
/// call validates a document for: who writes, whether the revision is
/// replicated, and the database it is written to (its name and security
/// object, read once per write when asked for).
#[non_exhaustive]
pub struct WriteContext<'a> {
    /// Who is writing.
    pub user_ctx: UserCtx,
    /// `false` for a replicated write (`new_edits: false`), whose revision
    /// comes from the source; `true` for an edit.
    pub new_edits: bool,
    adapter: &'a dyn Adapter,
    security: tokio::sync::OnceCell<SecurityDocument>,
    db_name: tokio::sync::OnceCell<String>,
}

impl<'a> WriteContext<'a> {
    pub(crate) fn new(adapter: &'a dyn Adapter, user_ctx: UserCtx, new_edits: bool) -> Self {
        Self {
            user_ctx,
            new_edits,
            adapter,
            security: tokio::sync::OnceCell::new(),
            db_name: tokio::sync::OnceCell::new(),
        }
    }

    /// The database's security object (CouchDB's `secObj`).
    pub async fn security(&self) -> Result<&SecurityDocument> {
        self.security
            .get_or_try_init(|| self.adapter.get_security())
            .await
    }

    /// The database's name (`userCtx.db` in CouchDB).
    pub async fn db_name(&self) -> Result<&str> {
        self.db_name
            .get_or_try_init(|| async { Ok(self.adapter.info().await?.db_name) })
            .await
            .map(String::as_str)
    }

    /// The `userCtx` object CouchDB passes to validation functions:
    /// `{"db", "name", "roles"}`.
    pub async fn user_ctx_json(&self) -> Result<serde_json::Value> {
        Ok(self.user_ctx.to_json(self.db_name().await?))
    }
}

/// The revision a written document replaces (CouchDB's `oldDoc`), as CouchDB
/// determines it before validating:
///
/// - an edit with a `_rev` replaces that leaf revision (a deleted leaf is a
///   document with `deleted: true`);
/// - an edit without `_rev` of a document that does not exist, or is
///   deleted, has none (it creates the document again);
/// - a replicated revision replaces the newest revision of its `_revisions`
///   history that the database has, if any (none for a new branch).
///
/// It is read before any document of the write is stored, so every document
/// of a batch is validated against the database as it was before the batch.
/// A revision whose body was removed by compaction counts as missing.
pub struct OldDoc<'a> {
    doc: Option<Document>,
    _adapter: std::marker::PhantomData<&'a ()>,
}

impl OldDoc<'_> {
    /// The replaced revision, `None` when the write creates the document
    /// (or a new branch of it).
    pub async fn get(&self) -> Result<Option<&Document>> {
        Ok(self.doc.as_ref())
    }
}

/// What CouchDB does with a document before its validation functions run.
pub(crate) enum Prior<'a> {
    /// Validate it, replacing this revision.
    Validate(OldDoc<'a>),
    /// Write it without validating: the write is a conflict (an edit of a
    /// live document without `_rev`, or from a revision that is not a leaf),
    /// or the replicated revision is already stored.
    Skip,
}

/// A revision of `id`, or `None` if the database does not have it.
async fn revision(
    adapter: &dyn Adapter,
    id: &str,
    rev: &str,
    latest: bool,
) -> Result<Option<Document>> {
    let opts = GetOptions {
        rev: Some(rev.to_string()),
        latest,
        ..Default::default()
    };
    match adapter.get(id, opts).await {
        Ok(doc) => Ok(Some(doc)),
        Err(RouchError::NotFound(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

pub(crate) async fn prior<'a>(
    adapter: &dyn Adapter,
    doc: &Document,
    new_edits: bool,
) -> Result<Prior<'a>> {
    let validate = |doc| {
        Prior::Validate(OldDoc {
            doc,
            _adapter: std::marker::PhantomData,
        })
    };
    let id = doc.id.as_str();
    if id.is_empty() {
        return Ok(validate(None));
    }
    if !new_edits {
        let history = replicated_ancestors(doc);
        let Some(own) = history.first() else {
            return Ok(validate(None));
        };
        let missing = adapter
            .revs_diff(HashMap::from([(id.to_string(), history.clone())]))
            .await?
            .results
            .remove(id)
            .map(|diff| diff.missing)
            .unwrap_or_default();
        if !missing.contains(own) {
            return Ok(Prior::Skip);
        }
        for rev in history.iter().skip(1).filter(|rev| !missing.contains(rev)) {
            if let Some(old) = revision(adapter, id, rev, false).await? {
                return Ok(validate(Some(old)));
            }
        }
        return Ok(validate(None));
    }
    match &doc.rev {
        Some(rev) => {
            let rev = rev.clone().normalized().to_string();
            match revision(adapter, id, &rev, true).await? {
                // `latest` stays on the revision only when it is a leaf.
                Some(old) if old.rev.as_ref().is_some_and(|r| r.to_string() == rev) => {
                    Ok(validate(Some(old)))
                }
                _ => Ok(Prior::Skip),
            }
        }
        None => match adapter.get(id, GetOptions::default()).await {
            Ok(_) => Ok(Prior::Skip),
            Err(RouchError::NotFound(_)) => Ok(validate(None)),
            Err(e) => Err(e),
        },
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
            .map(|(i, id)| format!("{}-{}", start - i as u64, normalize_rev_hash(id)))
            .collect(),
        _ => doc
            .rev
            .iter()
            .map(|r| r.clone().normalized().to_string())
            .collect(),
    }
}
