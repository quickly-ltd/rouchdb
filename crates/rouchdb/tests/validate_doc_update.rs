//! `Plugin::validate_doc_update`: every write path validates each document
//! with the writer's `UserCtx` and the revision it replaces, like CouchDB's
//! `validate_doc_update(newDoc, oldDoc, userCtx)`, and a rejection denies
//! that document only.

use std::sync::{Arc, Mutex};

use rouchdb::*;
use serde_json::json;

/// One `validate_doc_update` call, as the plugin saw it.
#[derive(Debug, Clone, PartialEq)]
struct Seen {
    id: String,
    new_rev: Option<String>,
    deleted: bool,
    /// `(rev, deleted, body)` of the old document.
    old: Option<(String, bool, serde_json::Value)>,
    user: UserCtx,
    new_edits: bool,
}

/// Records every call; rejects a document whose body says so:
/// `{"reject": reason}` is `Forbidden`, `{"unauth": true}` `Unauthorized`,
/// `{"boom": true}` an error that is not a rejection.
#[derive(Default)]
struct Recorder {
    seen: Mutex<Vec<Seen>>,
}

impl Recorder {
    fn take(&self) -> Vec<Seen> {
        std::mem::take(&mut self.seen.lock().unwrap())
    }
}

#[async_trait::async_trait]
impl Plugin for Recorder {
    fn name(&self) -> &str {
        "recorder"
    }

    async fn validate_doc_update(
        &self,
        new_doc: &Document,
        old_doc: &OldDoc<'_>,
        ctx: &WriteContext<'_>,
    ) -> Result<()> {
        let old = old_doc.get().await?.map(|d| {
            (
                d.rev.as_ref().map(|r| r.to_string()).unwrap_or_default(),
                d.deleted,
                d.data.clone(),
            )
        });
        self.seen.lock().unwrap().push(Seen {
            id: new_doc.id.clone(),
            new_rev: new_doc.rev.as_ref().map(|r| r.to_string()),
            deleted: new_doc.deleted,
            old,
            user: ctx.user_ctx.clone(),
            new_edits: ctx.new_edits,
        });
        if let Some(reason) = new_doc.data.get("reject").and_then(|r| r.as_str()) {
            return Err(RouchError::Forbidden(reason.to_string()));
        }
        if new_doc.data.get("unauth").is_some() {
            return Err(RouchError::Unauthorized);
        }
        if new_doc.data.get("boom").is_some() {
            return Err(RouchError::DatabaseError("boom".into()));
        }
        Ok(())
    }
}

fn cashier() -> UserCtx {
    UserCtx::new(Some("cashier".into()), vec!["staff".into()])
}

#[derive(Clone, Copy, Debug)]
enum Kind {
    Memory,
    Redb,
}

const KINDS: [Kind; 2] = [Kind::Memory, Kind::Redb];

/// A database of `kind` with a `Recorder`, and the dir that holds it.
fn open(kind: Kind, name: &str) -> (Database, Arc<Recorder>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = match kind {
        Kind::Memory => Database::memory(name),
        Kind::Redb => Database::open(dir.path().join(format!("{name}.redb")), name).unwrap(),
    };
    let recorder = Arc::new(Recorder::default());
    (db.with_plugin(recorder.clone()), recorder, dir)
}

fn doc(id: &str, data: serde_json::Value) -> Document {
    Document {
        id: id.into(),
        rev: None,
        deleted: false,
        data,
        attachments: Default::default(),
    }
}

#[tokio::test]
async fn local_writes_see_the_admin_context_and_the_replaced_revision() {
    for kind in KINDS {
        let (db, rec, _dir) = open(kind, "local");

        let created = db.put("a", json!({"v": 1})).await.unwrap();
        let seen = rec.take();
        assert_eq!(seen.len(), 1, "{kind:?}");
        assert_eq!(seen[0].old, None, "{kind:?}: a new doc has no old doc");
        assert_eq!(seen[0].user, UserCtx::admin(), "{kind:?}");
        assert!(seen[0].user.is_admin());
        assert!(seen[0].new_edits, "{kind:?}");

        let r1 = created.rev.unwrap();
        let updated = db.update("a", &r1, json!({"v": 2})).await.unwrap();
        let seen = rec.take();
        assert_eq!(
            seen[0].old,
            Some((r1.clone(), false, json!({"v": 1}))),
            "{kind:?}: an update replaces the revision it names"
        );

        let r2 = updated.rev.unwrap();
        db.remove("a", &r2).await.unwrap();
        let seen = rec.take();
        assert!(seen[0].deleted, "{kind:?}");
        assert_eq!(seen[0].old.as_ref().map(|o| &o.0), Some(&r2), "{kind:?}");

        // A write without `_rev` over a deleted document creates it again:
        // like CouchDB (`prep_and_validate_update`), it has no old doc.
        db.put("a", json!({"v": 3})).await.unwrap();
        let seen = rec.take();
        assert_eq!(seen[0].old, None, "{kind:?}: re-created over a deletion");

        // `post` goes through it too.
        db.post(json!({"p": true})).await.unwrap();
        assert_eq!(rec.take().len(), 1, "{kind:?}: post");
    }
}

#[tokio::test]
async fn local_and_design_documents_are_not_validated_by_plugins() {
    // CouchDB (`couch_db:validate_doc_update`) never validates `_local/`
    // documents, and only checks that a `_design/` writer is an admin.
    for kind in KINDS {
        let (db, rec, _dir) = open(kind, "special");
        db.put("_local/x", json!({"reject": "never"}))
            .await
            .unwrap();
        db.put_design(
            DesignDocument::from_json(json!({"_id": "_design/d", "reject": "no"})).unwrap(),
        )
        .await
        .unwrap();
        assert!(rec.take().is_empty(), "{kind:?}");

        let err = db
            .as_user(cashier())
            .put_design(DesignDocument::from_json(json!({"_id": "_design/e"})).unwrap())
            .await
            .unwrap_err();
        assert!(matches!(err, RouchError::Unauthorized), "{kind:?}: {err:?}");
        let results = db
            .as_user(UserCtx::anonymous())
            .bulk_docs(
                vec![doc("_design/f", json!({})), doc("ok", json!({}))],
                BulkDocsOptions::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            results[0].error.as_deref(),
            Some("unauthorized"),
            "{results:?}"
        );
        assert_eq!(
            results[0].reason.as_deref(),
            Some("You are not a db or server admin.")
        );
        assert!(results[1].ok, "{kind:?}: {results:?}");

        // A database admin (security document) may write design documents.
        let mut security = SecurityDocument::default();
        security.admins.names.push("cashier".into());
        db.put_security(security).await.unwrap();
        db.as_user(cashier())
            .put_design(DesignDocument::from_json(json!({"_id": "_design/e"})).unwrap())
            .await
            .unwrap();
        // Design documents are still not passed to the plugins.
        assert_eq!(rec.take().len(), 1, "{kind:?}: only `ok`");
    }
}

#[tokio::test]
async fn conflicting_edits_are_conflicts_before_any_validation() {
    // CouchDB `prep_and_validate_update`: an edit of a live document without
    // `_rev`, or from a revision that is not a leaf, is a conflict and the
    // validation functions do not run.
    for kind in KINDS {
        let (db, rec, _dir) = open(kind, "conflicts");
        let r1 = db.put("a", json!({"v": 1})).await.unwrap().rev.unwrap();
        db.update("a", &r1, json!({"v": 2})).await.unwrap();
        rec.take();

        let err = db.put("a", json!({"reject": "x"})).await.unwrap_err();
        assert!(matches!(err, RouchError::Conflict), "{kind:?}: {err:?}");
        let err = db
            .update("a", &r1, json!({"reject": "x"}))
            .await
            .unwrap_err();
        assert!(matches!(err, RouchError::Conflict), "{kind:?}: {err:?}");
        let err = db
            .update(
                "a",
                "1-ffffffffffffffffffffffffffffffff",
                json!({"reject": "x"}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, RouchError::Conflict), "{kind:?}: {err:?}");
        assert!(rec.take().is_empty(), "{kind:?}: not validated");
    }
}

#[tokio::test]
async fn replicated_revisions_already_stored_are_not_validated_again() {
    // CouchDB (`prep_and_validate_replicated_updates`) counts a revision it
    // already has as written, without validating it.
    for kind in KINDS {
        let (db, rec, _dir) = open(kind, "dupes");
        let replicated = Document::from_json(json!({
            "_id": "a",
            "_rev": "1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "_revisions": {"start": 1, "ids": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]},
            "v": 1,
        }))
        .unwrap();
        db.bulk_docs(vec![replicated.clone()], BulkDocsOptions::replication())
            .await
            .unwrap();
        rec.take();
        let mut again = replicated;
        again.data["reject"] = json!("seen before");
        let results = db
            .bulk_docs(vec![again], BulkDocsOptions::replication())
            .await
            .unwrap();
        assert!(results.iter().all(|r| r.ok), "{kind:?}: {results:?}");
        assert!(rec.take().is_empty(), "{kind:?}");
    }
}

/// Records what `WriteContext` says about the database.
#[derive(Default)]
struct DbProbe(Mutex<Vec<serde_json::Value>>);

#[async_trait::async_trait]
impl Plugin for DbProbe {
    fn name(&self) -> &str {
        "probe"
    }
    async fn validate_doc_update(
        &self,
        _new_doc: &Document,
        _old_doc: &OldDoc<'_>,
        ctx: &WriteContext<'_>,
    ) -> Result<()> {
        let sec = ctx.security().await?;
        let user = ctx.user_ctx_json().await?;
        self.0
            .lock()
            .unwrap()
            .push(json!({"user": user, "members": sec.members.names}));
        Ok(())
    }
}

#[tokio::test]
async fn the_context_gives_the_database_name_and_security_object() {
    let probe = Arc::new(DbProbe::default());
    let db = Database::memory("shop").with_plugin(probe.clone());
    let mut security = SecurityDocument::default();
    security.members.names.push("cashier".into());
    db.put_security(security).await.unwrap();
    db.as_user(cashier()).put("a", json!({})).await.unwrap();
    assert_eq!(
        probe.0.lock().unwrap()[0],
        json!({
            "user": {"db": "shop", "name": "cashier", "roles": ["staff"]},
            "members": ["cashier"],
        })
    );
}

#[tokio::test]
async fn as_user_writes_with_its_own_context_and_shares_the_data() {
    for kind in KINDS {
        let (db, rec, _dir) = open(kind, "scoped");
        let staff = db.as_user(cashier());
        assert_eq!(staff.user_ctx(), &cashier());
        assert_eq!(
            db.user_ctx(),
            &UserCtx::admin(),
            "the original is unchanged"
        );

        staff.put("a", json!({"v": 1})).await.unwrap();
        assert_eq!(rec.take()[0].user, cashier(), "{kind:?}");
        // Same storage, same plugins.
        assert_eq!(db.get("a").await.unwrap().data["v"], 1, "{kind:?}");
        db.put("b", json!({})).await.unwrap();
        assert_eq!(rec.take()[0].user, UserCtx::admin(), "{kind:?}");
    }
}

#[tokio::test]
async fn bulk_docs_denies_rejected_docs_one_by_one_in_order() {
    for kind in KINDS {
        let (db, rec, _dir) = open(kind, "bulk");
        let results = db
            .bulk_docs(
                vec![
                    doc("ok1", json!({})),
                    doc("no", json!({"reject": "not yours"})),
                    doc("ok2", json!({})),
                    doc("anon", json!({"unauth": true})),
                ],
                BulkDocsOptions::new(),
            )
            .await
            .unwrap();
        let ids: Vec<&str> = results.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, ["ok1", "no", "ok2", "anon"], "{kind:?}: request order");
        assert!(results[0].ok && results[2].ok, "{kind:?}: {results:?}");
        assert_eq!(results[1].error.as_deref(), Some("forbidden"));
        assert_eq!(results[1].reason.as_deref(), Some("not yours"));
        assert_eq!(results[3].error.as_deref(), Some("unauthorized"));
        assert!(db.get("ok2").await.is_ok());
        assert!(matches!(db.get("no").await, Err(RouchError::NotFound(_))));
        assert_eq!(rec.take().len(), 4);

        // A single-document write returns the rejection as an error.
        let err = db.put("x", json!({"reject": "nope"})).await.unwrap_err();
        assert!(
            matches!(&err, RouchError::Forbidden(r) if r == "nope"),
            "{err:?}"
        );
        let err = db.put("y", json!({"unauth": true})).await.unwrap_err();
        assert!(matches!(err, RouchError::Unauthorized), "{err:?}");

        // Any other error fails the whole write, and nothing is written.
        rec.take();
        let err = db
            .bulk_docs(
                vec![doc("first", json!({})), doc("bad", json!({"boom": true}))],
                BulkDocsOptions::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, RouchError::DatabaseError(_)), "{err:?}");
        assert!(matches!(
            db.get("first").await,
            Err(RouchError::NotFound(_))
        ));
    }
}

#[tokio::test]
async fn a_batch_is_validated_against_the_database_before_it() {
    for kind in KINDS {
        let (db, rec, _dir) = open(kind, "before");
        let results = db
            .bulk_docs(
                vec![doc("a", json!({"n": 1})), doc("a", json!({"n": 2}))],
                BulkDocsOptions::new(),
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 2, "{kind:?}");
        let seen = rec.take();
        assert!(
            seen.iter().all(|s| s.old.is_none()),
            "{kind:?}: neither sees the other: {seen:?}"
        );
    }
}

#[tokio::test]
async fn replicated_docs_carry_the_target_context_and_their_parent() {
    for kind in KINDS {
        let source = Database::memory("source");
        let a1 = source.put("a", json!({"v": 1})).await.unwrap();
        source.put("b", json!({"reject": "foreign"})).await.unwrap();

        let (target, rec, _dir) = open(kind, "target");
        let trusted = UserCtx::new(Some("cloud".into()), vec!["_replicator".into()]);
        let result = target
            .as_user(trusted.clone())
            .replicate_from(&source)
            .await
            .unwrap();
        assert_eq!(result.docs_written, 1, "{kind:?}: {result:?}");
        assert!(matches!(
            target.get("b").await,
            Err(RouchError::NotFound(_))
        ));

        let seen = rec.take();
        assert_eq!(seen.len(), 2, "{kind:?}: {seen:?}");
        for s in &seen {
            assert_eq!(s.user, trusted, "{kind:?}");
            assert!(!s.new_edits, "{kind:?}");
            assert_eq!(s.old, None, "{kind:?}: first copy has no parent here");
        }

        // The next revision replaces the one the target has.
        let r1 = a1.rev.unwrap();
        source.update("a", &r1, json!({"v": 2})).await.unwrap();
        target
            .as_user(trusted)
            .replicate_from(&source)
            .await
            .unwrap();
        let seen = rec.take();
        let a = seen.iter().find(|s| s.id == "a").expect("a replicated");
        assert_eq!(a.old, Some((r1, false, json!({"v": 1}))), "{kind:?}");
        assert!(a.new_rev.as_deref().is_some_and(|r| r.starts_with("2-")));

        // Without `as_user`, replication writes as the target's own context.
        let other = Database::memory("other");
        other.put("c", json!({})).await.unwrap();
        other.replicate_to(&target).await.unwrap();
        assert_eq!(rec.take()[0].user, UserCtx::admin(), "{kind:?}");
    }
}

#[tokio::test]
async fn replicated_deletions_and_conflict_branches_have_the_expected_parent() {
    let source = Database::memory("source");
    let (target, rec, _dir) = open(Kind::Memory, "target");
    let r1 = source.put("a", json!({"v": 1})).await.unwrap().rev.unwrap();
    source.replicate_to(&target).await.unwrap();
    rec.take();

    // A conflicting first revision written on the target: its branch
    // shares no ancestor with the source's, so it has no parent.
    target
        .bulk_docs(
            vec![
                Document::from_json(json!({
                    "_id": "a",
                    "_rev": "1-ffffffffffffffffffffffffffffffff",
                    "v": "branch",
                    "_revisions": {"start": 1, "ids": ["ffffffffffffffffffffffffffffffff"]},
                }))
                .unwrap(),
            ],
            BulkDocsOptions::replication(),
        )
        .await
        .unwrap();
    let seen = rec.take();
    assert_eq!(seen[0].old, None, "{seen:?}");

    source.remove("a", &r1).await.unwrap();
    source.replicate_to(&target).await.unwrap();
    let seen = rec.take();
    assert!(seen[0].deleted, "{seen:?}");
    assert_eq!(
        seen[0].old.as_ref().map(|o| o.0.as_str()),
        Some(r1.as_str())
    );
}

#[tokio::test]
async fn live_replication_into_a_scoped_target_validates_as_its_user() {
    let source = Database::memory("source");
    source.put("a", json!({})).await.unwrap();
    let (target, rec, _dir) = open(Kind::Memory, "target");
    let (mut events, handle) = source.replicate_to_live(
        &target.as_user(cashier()),
        ReplicationOptions {
            live: true,
            ..Default::default()
        },
    );
    while let Some(event) = events.recv().await {
        if matches!(
            event,
            ReplicationEvent::Paused | ReplicationEvent::CaughtUp { .. }
        ) {
            break;
        }
    }
    handle.cancel();
    let seen = rec.take();
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert_eq!(seen[0].user, cashier());
    assert!(!seen[0].new_edits);
}

#[tokio::test]
async fn attachment_writes_are_validated_with_the_edited_revision() {
    for kind in KINDS {
        let (db, rec, _dir) = open(kind, "att");
        let staff = db.as_user(cashier());
        let r1 = staff.put("a", json!({"v": 1})).await.unwrap().rev.unwrap();
        rec.take();

        let r2 = staff
            .put_attachment("a", "f.txt", &r1, b"hi".to_vec(), "text/plain")
            .await
            .unwrap()
            .rev
            .unwrap();
        let seen = rec.take();
        assert_eq!(seen.len(), 1, "{kind:?}");
        assert_eq!(seen[0].user, cashier(), "{kind:?}");
        assert_eq!(seen[0].old.as_ref().map(|o| &o.0), Some(&r1), "{kind:?}");

        staff.remove_attachment("a", "f.txt", &r2).await.unwrap();
        assert_eq!(rec.take()[0].old.as_ref().map(|o| &o.0), Some(&r2));
    }
}

/// Rejects a document whose body has `"conflict"` with `RouchError::Conflict`.
struct ConflictPlugin;

#[async_trait::async_trait]
impl Plugin for ConflictPlugin {
    fn name(&self) -> &str {
        "conflict"
    }
    async fn validate_doc_update(
        &self,
        new_doc: &Document,
        _old_doc: &OldDoc<'_>,
        _ctx: &WriteContext<'_>,
    ) -> Result<()> {
        match new_doc.data.get("conflict") {
            Some(_) => Err(RouchError::Conflict),
            None => Ok(()),
        }
    }
}

#[tokio::test]
async fn a_plugin_conflict_denies_that_document_only() {
    let db = Database::memory("c").with_plugin(Arc::new(ConflictPlugin));
    let results = db
        .bulk_docs(
            vec![doc("a", json!({"conflict": true})), doc("b", json!({}))],
            BulkDocsOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(results[0].error.as_deref(), Some("conflict"), "{results:?}");
    assert!(results[1].ok, "{results:?}");
    let err = db.put("x", json!({"conflict": true})).await.unwrap_err();
    assert!(matches!(err, RouchError::Conflict), "{err:?}");
}

/// Changes every document it sees in `before_write`.
struct Stamper;

#[async_trait::async_trait]
impl Plugin for Stamper {
    fn name(&self) -> &str {
        "stamper"
    }
    async fn before_write(&self, docs: &mut Vec<Document>) -> Result<()> {
        for doc in docs {
            doc.data["stamped"] = json!(true);
        }
        Ok(())
    }
}

#[tokio::test]
async fn replicated_writes_keep_the_source_body_on_every_path() {
    // `before_write` changes are applied to edits only: a replicated revision
    // is stored as the source sent it, through `bulk_docs` as through
    // replication.
    let db = Database::memory("t").with_plugin(Arc::new(Stamper));
    let replicated = Document::from_json(json!({
        "_id": "r",
        "_rev": "1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "_revisions": {"start": 1, "ids": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]},
        "v": 1,
    }))
    .unwrap();
    db.bulk_docs(vec![replicated], BulkDocsOptions::replication())
        .await
        .unwrap();
    assert!(db.get("r").await.unwrap().data.get("stamped").is_none());
    let source = Database::memory("s");
    source.put("p", json!({})).await.unwrap();
    source.replicate_to(&db).await.unwrap();
    assert!(db.get("p").await.unwrap().data.get("stamped").is_none());
    db.put("e", json!({})).await.unwrap();
    assert_eq!(db.get("e").await.unwrap().data["stamped"], true);
}

#[tokio::test]
async fn replicated_uppercase_hex_revs_still_validate_and_reject() {
    for kind in KINDS {
        let (db, rec, _dir) = open(kind, "upper_rev");

        // Replicated write with uppercase 32-hex `_rev` and `_revisions.ids`.
        let upper_rev = Document {
            id: "u1".into(),
            rev: Some(Revision::new(1, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into())),
            deleted: false,
            data: json!({
                "_revisions": {
                    "start": 1,
                    "ids": ["AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"]
                },
                "reject": "uppercase denied",
            }),
            attachments: Default::default(),
        };
        let results = db
            .bulk_docs(vec![upper_rev], BulkDocsOptions::replication())
            .await
            .unwrap();
        assert_eq!(results.len(), 1, "{kind:?}: {results:?}");
        assert!(!results[0].ok, "{kind:?}: {results:?}");
        assert_eq!(
            results[0].error.as_deref(),
            Some("forbidden"),
            "{kind:?}: {results:?}"
        );
        assert_eq!(
            results[0].reason.as_deref(),
            Some("uppercase denied"),
            "{kind:?}: {results:?}"
        );
        assert!(
            matches!(db.get("u1").await, Err(RouchError::NotFound(_))),
            "{kind:?}: forbidden doc must not be stored"
        );
        let seen = rec.take();
        assert_eq!(seen.len(), 1, "{kind:?}: {seen:?}");
        assert_eq!(seen[0].id, "u1", "{kind:?}");

        // Fallback branch without `_revisions`, using uppercase `Document.rev`.
        let upper_fallback = Document {
            id: "u2".into(),
            rev: Some(Revision::new(1, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB".into())),
            deleted: false,
            data: json!({"reject": "uppercase fallback"}),
            attachments: Default::default(),
        };
        let results = db
            .bulk_docs(vec![upper_fallback], BulkDocsOptions::replication())
            .await
            .unwrap();
        assert_eq!(
            results[0].error.as_deref(),
            Some("forbidden"),
            "{kind:?}: {results:?}"
        );
        assert!(matches!(db.get("u2").await, Err(RouchError::NotFound(_))));
        assert_eq!(rec.take().len(), 1, "{kind:?}");

        // Existing lowercase ancestor is still resolved as `old_doc` when the
        // replicated update sends uppercase 32-hex ids in `_revisions`.
        let base = Document::from_json(json!({
            "_id": "u3",
            "_rev": "1-cccccccccccccccccccccccccccccccc",
            "_revisions": {"start": 1, "ids": ["cccccccccccccccccccccccccccccccc"]},
            "v": 1,
        }))
        .unwrap();
        db.bulk_docs(vec![base], BulkDocsOptions::replication())
            .await
            .unwrap();
        rec.take();

        let child = Document {
            id: "u3".into(),
            rev: Some(Revision::new(2, "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD".into())),
            deleted: false,
            data: json!({
                "_revisions": {
                    "start": 2,
                    "ids": [
                        "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD",
                        "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
                    ]
                },
                "reject": "no child",
            }),
            attachments: Default::default(),
        };
        let results = db
            .bulk_docs(vec![child], BulkDocsOptions::replication())
            .await
            .unwrap();
        assert_eq!(
            results[0].error.as_deref(),
            Some("forbidden"),
            "{kind:?}: {results:?}"
        );
        let seen = rec.take();
        assert_eq!(seen.len(), 1, "{kind:?}: {seen:?}");
        assert_eq!(
            seen[0].old,
            Some((
                "1-cccccccccccccccccccccccccccccccc".into(),
                false,
                json!({"v": 1})
            )),
            "{kind:?}: uppercase ancestor must match stored lowercase rev"
        );
    }
}

#[tokio::test]
async fn bulk_docs_new_edits_with_rev_and_deleted_in_data_validates_with_old_doc() {
    for kind in KINDS {
        let (db, rec, _dir) = open(kind, "data_rev");
        let rev1 = db.put("doc1", json!({"v": 1})).await.unwrap().rev.unwrap();
        rec.take();

        // Forbidden update carrying `_rev` inside `doc.data`.
        let results = db
            .bulk_docs(
                vec![Document::new(
                    "doc1",
                    json!({"_rev": rev1, "v": 2, "reject": "forbidden update"}),
                )],
                BulkDocsOptions::new(),
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 1, "{kind:?}: {results:?}");
        assert!(!results[0].ok, "{kind:?}: {results:?}");
        assert_eq!(
            results[0].error.as_deref(),
            Some("forbidden"),
            "{kind:?}: {results:?}"
        );
        assert_eq!(
            results[0].reason.as_deref(),
            Some("forbidden update"),
            "{kind:?}: {results:?}"
        );
        let seen = rec.take();
        assert_eq!(seen.len(), 1, "{kind:?}: {seen:?}");
        assert_eq!(
            seen[0].old,
            Some((rev1.clone(), false, json!({"v": 1}))),
            "{kind:?}: old_doc must be passed when _rev is in data"
        );
        assert_eq!(db.get("doc1").await.unwrap().data["v"], 1, "{kind:?}");

        // Forbidden deletion carrying `_rev` and `_deleted` inside `doc.data`.
        let results = db
            .bulk_docs(
                vec![Document::new(
                    "doc1",
                    json!({"_rev": rev1, "_deleted": true, "reject": "no delete"}),
                )],
                BulkDocsOptions::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            results[0].error.as_deref(),
            Some("forbidden"),
            "{kind:?}: {results:?}"
        );
        let seen = rec.take();
        assert_eq!(seen.len(), 1, "{kind:?}: {seen:?}");
        assert!(seen[0].deleted, "{kind:?}: _deleted must be lifted");
        assert_eq!(
            seen[0].old,
            Some((rev1, false, json!({"v": 1}))),
            "{kind:?}: old_doc must be passed for _deleted in data"
        );
        assert_eq!(db.get("doc1").await.unwrap().data["v"], 1, "{kind:?}");
    }
}

#[tokio::test]
async fn bulk_docs_with_plugin_reports_per_doc_bad_request_for_invalid_doc() {
    for kind in KINDS {
        let (db, rec, _dir) = open(kind, "bad_in_batch");
        let results = db
            .bulk_docs(
                vec![
                    Document::new("ok1", json!({"v": 1})),
                    Document::new("bad", json!({"_unknown": 1})),
                    Document::new("ok2", json!({"v": 2})),
                ],
                BulkDocsOptions::new(),
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 3, "{kind:?}: {results:?}");
        assert!(results[0].ok, "{kind:?}: {results:?}");
        assert_eq!(results[0].id, "ok1", "{kind:?}");
        assert!(!results[1].ok, "{kind:?}: {results:?}");
        assert_eq!(results[1].id, "bad", "{kind:?}");
        assert_eq!(
            results[1].error.as_deref(),
            Some("bad_request"),
            "{kind:?}: {results:?}"
        );
        assert_eq!(
            results[1].reason.as_deref(),
            Some("Bad special document member: _unknown"),
            "{kind:?}: bare reason without 'bad request: ' prefix"
        );
        assert!(results[2].ok, "{kind:?}: {results:?}");
        assert_eq!(results[2].id, "ok2", "{kind:?}");

        assert_eq!(db.get("ok1").await.unwrap().data["v"], 1, "{kind:?}");
        assert_eq!(db.get("ok2").await.unwrap().data["v"], 2, "{kind:?}");
        assert!(
            matches!(db.get("bad").await, Err(RouchError::NotFound(_))),
            "{kind:?}"
        );
        let seen_ids: Vec<String> = rec.take().into_iter().map(|s| s.id).collect();
        assert_eq!(seen_ids, vec!["ok1", "ok2"], "{kind:?}");
    }
}
