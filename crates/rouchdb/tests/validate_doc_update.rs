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
        ctx: &WriteContext,
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

        // A write without `_rev` over a deleted document replaces the
        // deletion, as in CouchDB.
        db.put("a", json!({"v": 3})).await.unwrap();
        let seen = rec.take();
        let (_, old_deleted, _) = seen[0].old.clone().expect("deleted winner");
        assert!(old_deleted, "{kind:?}: the old doc is the deletion");

        // `post` and design documents go through it too.
        db.post(json!({"p": true})).await.unwrap();
        assert_eq!(rec.take().len(), 1, "{kind:?}: post");
        db.put_design(DesignDocument::from_json(json!({"_id": "_design/d"})).unwrap())
            .await
            .unwrap();
        let seen = rec.take();
        assert_eq!(seen[0].id, "_design/d", "{kind:?}: put_design");
    }
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
