//! Basic CRUD operations via the HTTP adapter against CouchDB.

mod common;

use common::fresh_remote_db;
use rouchdb::{AllDocsOptions, ChangesOptions, Database};

#[tokio::test]
#[ignore = "requires CouchDB"]
async fn http_put_and_get() {
    let url = fresh_remote_db("http_crud").await;
    let db = Database::http(&url);

    let result = db
        .put("doc1", serde_json::json!({"name": "Alice"}))
        .await
        .unwrap();
    assert!(result.ok);

    let doc = db.get("doc1").await.unwrap();
    assert_eq!(doc.data["name"], "Alice");
}

#[tokio::test]
#[ignore = "requires CouchDB"]
async fn http_update_document() {
    let url = fresh_remote_db("http_update").await;
    let db = Database::http(&url);

    let r1 = db.put("doc1", serde_json::json!({"v": 1})).await.unwrap();
    let rev = r1.rev.unwrap();

    let r2 = db
        .update("doc1", &rev, serde_json::json!({"v": 2}))
        .await
        .unwrap();
    assert!(r2.ok);

    let doc = db.get("doc1").await.unwrap();
    assert_eq!(doc.data["v"], 2);
}

#[tokio::test]
#[ignore = "requires CouchDB"]
async fn http_delete_document() {
    let url = fresh_remote_db("http_delete").await;
    let db = Database::http(&url);

    let r1 = db.put("doc1", serde_json::json!({"v": 1})).await.unwrap();
    let rev = r1.rev.unwrap();

    let r2 = db.remove("doc1", &rev).await.unwrap();
    assert!(r2.ok);

    let err = db.get("doc1").await;
    assert!(err.is_err());
}

#[tokio::test]
#[ignore = "requires CouchDB"]
async fn http_all_docs() {
    let url = fresh_remote_db("http_alldocs").await;
    let db = Database::http(&url);

    db.put("alice", serde_json::json!({"name": "Alice"}))
        .await
        .unwrap();
    db.put("bob", serde_json::json!({"name": "Bob"}))
        .await
        .unwrap();
    db.put("charlie", serde_json::json!({"name": "Charlie"}))
        .await
        .unwrap();

    let result = db.all_docs(AllDocsOptions::new()).await.unwrap();
    assert_eq!(result.total_rows, 3);
}

#[tokio::test]
#[ignore = "requires CouchDB"]
async fn http_changes_feed() {
    let url = fresh_remote_db("http_changes").await;
    let db = Database::http(&url);

    db.put("doc1", serde_json::json!({"v": 1})).await.unwrap();
    db.put("doc2", serde_json::json!({"v": 2})).await.unwrap();

    let changes = db.changes(ChangesOptions::default()).await.unwrap();
    assert_eq!(changes.results.len(), 2);
}

/// F08: the HTTP adapter reports CouchDB's purge state (an opaque string on
/// a clustered CouchDB), which changes with every purge request.
#[tokio::test]
#[ignore = "requires CouchDB"]
async fn http_info_reports_purge_seq() {
    let url = fresh_remote_db("http_purge_seq").await;
    let db = Database::http(&url);
    let before = db.info().await.unwrap().purge_seq;
    let rev = db
        .put("d", serde_json::json!({}))
        .await
        .unwrap()
        .rev
        .unwrap();
    db.purge("d", vec![rev]).await.unwrap();
    let after = db.info().await.unwrap().purge_seq;
    assert!(matches!(after, rouchdb::Seq::Str(_)), "{after:?}");
    assert_ne!(before, after);
}
