//! `DELETE /{db}` removes the database until `PUT /{db}` creates it again,
//! as in CouchDB 3.5.1 (404 "Database does not exist." in between).
mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::http::{Method, StatusCode};
use common::*;
use rouchdb::Database;
use serde_json::json;

#[track_caller]
fn assert_error(resp: &Resp, status: StatusCode, error: &str, reason: &str) {
    assert_eq!(
        resp.status,
        status,
        "{:?}",
        String::from_utf8_lossy(&resp.body)
    );
    let body = resp.json();
    assert_eq!(body["error"], error, "{body}");
    assert_eq!(body["reason"], reason, "{body}");
}

#[tokio::test]
async fn deleted_database_is_gone_until_recreated() {
    let db = Arc::new(Database::memory(DB));
    db.put("doc", json!({"v": 1})).await.unwrap();
    let app = app_with(db.clone(), &config());

    let resp = delete(&app, "/db").await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.json(), json!({"ok": true}));

    let gone = |resp: &Resp| {
        assert_error(
            resp,
            StatusCode::NOT_FOUND,
            "not_found",
            "Database does not exist.",
        )
    };
    gone(&get(&app, "/db").await);
    gone(&get(&app, "/db/doc").await);
    gone(&put(&app, "/db/doc", json!({"v": 2})).await);
    gone(&post(&app, "/db", json!({"v": 2})).await);
    gone(&get(&app, "/db/_all_docs").await);
    gone(&get(&app, "/db/_changes").await);
    gone(&post(&app, "/db/_find", json!({"selector": {}})).await);
    gone(&post(&app, "/db/_bulk_docs", json!({"docs": [{"_id": "x"}]})).await);
    gone(&put(&app, "/db/_local/l", json!({})).await);
    gone(&get(&app, "/db/_security").await);
    gone(&delete(&app, "/db").await);
    assert_eq!(get(&app, "/_all_dbs").await.json(), json!([]));
    // Nothing was written behind the 404s.
    assert_eq!(db.info().await.unwrap().doc_count, 0);

    // PUT /{db} creates it again, empty.
    let resp = call(&app, Method::PUT, "/db", None).await;
    assert_eq!(resp.status, StatusCode::CREATED);
    assert_eq!(resp.json(), json!({"ok": true}));
    let info = get(&app, "/db").await;
    assert_eq!(info.status, StatusCode::OK);
    assert_eq!(info.json()["doc_count"], 0);
    assert_eq!(get(&app, "/_all_dbs").await.json(), json!([DB]));
    assert_eq!(
        put(&app, "/db/doc", json!({"v": 3})).await.status,
        StatusCode::CREATED
    );

    // Creating an existing database is a 412 with CouchDB's reason.
    assert_error(
        &call(&app, Method::PUT, "/db", None).await,
        StatusCode::PRECONDITION_FAILED,
        "file_exists",
        "The database could not be created, the file already exists.",
    );
}

#[tokio::test]
async fn deleted_database_ends_waiting_changes_feeds() {
    for (feed, check) in [
        ("longpoll", "longpoll answers with no results"),
        ("continuous", "continuous ends with its last_seq line"),
    ] {
        let app = app();
        let waiting = {
            let app = app.clone();
            let uri = format!("/db/_changes?feed={feed}&since=now");
            tokio::spawn(async move { get(&app, &uri).await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(delete(&app, "/db").await.status, StatusCode::OK);
        let resp = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .unwrap_or_else(|_| panic!("the {feed} feed must end when the database is deleted"))
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let text = String::from_utf8_lossy(&resp.body).to_string();
        let last: serde_json::Value =
            serde_json::from_str(text.trim().lines().last().unwrap_or_default())
                .unwrap_or_else(|e| panic!("{check}: {e}: {text:?}"));
        if feed == "longpoll" {
            assert_eq!(last["results"], json!([]), "{check}: {text}");
        } else {
            assert!(last.get("last_seq").is_some(), "{check}: {text}");
            assert!(last.get("results").is_none(), "{check}: {text}");
        }
    }
}

struct PausedDestroy {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl rouchdb::Plugin for PausedDestroy {
    fn name(&self) -> &str {
        "paused-destroy"
    }
    fn on_destroy<'a, 'f>(
        &'a self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = rouchdb::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        Self: 'f,
    {
        Box::pin(async move {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(())
        })
    }
}

#[tokio::test]
async fn audit_regression_recreation_waits_until_destroy_finishes() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let db = Arc::new(Database::memory(DB).with_plugin(Arc::new(PausedDestroy {
        entered: entered.clone(),
        release: release.clone(),
    })));
    let app = app_with(db, &config());
    let deletion = tokio::spawn({
        let app = app.clone();
        async move { delete(&app, "/db").await }
    });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let mut recreate = Box::pin(call(&app, Method::PUT, "/db", None));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut recreate)
            .await
            .is_err(),
        "recreation must not acknowledge a new database while its storage is being destroyed"
    );
    release.notify_one();
    assert_eq!(deletion.await.unwrap().status, StatusCode::OK);
    assert_eq!(recreate.await.status, StatusCode::CREATED);
    assert_eq!(
        put(&app, "/db/survivor", json!({"v": 1})).await.status,
        StatusCode::CREATED
    );
    assert_eq!(get(&app, "/db/survivor").await.json()["v"], 1);
}

/// F08: `GET /{db}` reports the purge state instead of a constant 0; it
/// changes with every `_purge` request and survives a restart.
#[tokio::test]
async fn db_info_purge_seq_follows_purges() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.redb");
    {
        let app = app_with(Arc::new(Database::open(&path, DB).unwrap()), &config());
        assert_eq!(get(&app, "/db").await.json()["purge_seq"], 0);
        let rev = put(&app, "/db/d", json!({})).await.json()["rev"].clone();
        let resp = post(&app, "/db/_purge", json!({ "d": [rev] })).await;
        assert_eq!(resp.status, StatusCode::CREATED, "{:?}", resp.body);
        assert_eq!(get(&app, "/db").await.json()["purge_seq"], 1);
        post(&app, "/db/_purge", json!({"missing": ["1-abc"]})).await;
        assert_eq!(get(&app, "/db").await.json()["purge_seq"], 2);
    }
    let app = app_with(Arc::new(Database::open(&path, DB).unwrap()), &config());
    assert_eq!(get(&app, "/db").await.json()["purge_seq"], 2);
}
