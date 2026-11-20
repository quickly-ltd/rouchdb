//! F05: counters at the edge of u64 are answered with a JSON 400, never a
//! panic, and the database keeps working. CouchDB 3.5.1 has no limit (it
//! uses Erlang integers); RouchDB's revision generations, local revisions
//! and `_find` offsets are u64.
mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use base64::Engine;
use common::*;
use rouchdb::Database;
use serde_json::json;

const MAX: u64 = u64::MAX;

fn bookmark(skip: u64) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("{{\"skip\":{skip}}}"))
}

fn assert_bad_request(resp: &Resp, what: &str) {
    assert_eq!(
        resp.status,
        StatusCode::BAD_REQUEST,
        "{what}: {:?}",
        resp.body
    );
    assert_eq!(resp.json()["error"], "bad_request", "{what}");
}

#[tokio::test]
async fn accepted_divergence_find_offset_stops_at_u64_max() {
    let app = app();
    put(&app, "/db/a", json!({"n": 1})).await;
    let find = |bookmark: Option<u64>, skip: u64| {
        let mut body = json!({"selector": {}, "skip": skip});
        if let Some(b) = bookmark {
            body["bookmark"] = json!(self::bookmark(b));
        }
        post(&app, "/db/_find", body)
    };

    let resp = find(Some(0), 0).await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.json()["docs"].as_array().unwrap().len(), 1);
    assert_eq!(resp.json()["bookmark"], bookmark(1));

    for (b, skip) in [(MAX - 1, 1), (MAX, 0), (0, MAX)] {
        let resp = find(Some(b), skip).await;
        assert_eq!(resp.status, StatusCode::OK, "bookmark {b} + skip {skip}");
        assert_eq!(resp.json()["docs"], json!([]));
        assert_eq!(resp.json()["bookmark"], bookmark(MAX));
    }
    for (b, skip) in [(MAX, 1), (1, MAX), (MAX, MAX)] {
        assert_bad_request(&find(Some(b), skip).await, "bookmark + skip past u64");
    }
    // The database still answers.
    assert_eq!(find(None, 0).await.status, StatusCode::OK);
}

#[tokio::test]
async fn accepted_divergence_local_revision_stops_at_u64_max() {
    let app = app();
    for (sent, answered) in [
        (Some("0-0".to_string()), "0-1".to_string()),
        (None, "0-1".to_string()),
        (Some(format!("0-{}", MAX - 1)), format!("0-{MAX}")),
    ] {
        let mut body = json!({"v": 1});
        if let Some(rev) = &sent {
            body["_rev"] = json!(rev);
        }
        let resp = put(&app, "/db/_local/x", body).await;
        assert_eq!(resp.status, StatusCode::CREATED, "{sent:?}");
        assert_eq!(resp.json()["rev"], answered, "{sent:?}");
    }
    let resp = put(
        &app,
        "/db/_local/x",
        json!({"_rev": format!("0-{MAX}"), "v": 2}),
    )
    .await;
    assert_bad_request(&resp, "local rev MAX");
    // Not written, and later writes work.
    assert_eq!(
        get(&app, "/db/_local/x").await.json()["_rev"],
        format!("0-{MAX}")
    );
    let resp = put(&app, "/db/_local/x?rev=0-5", json!({"v": 3})).await;
    assert_eq!(resp.json()["rev"], "0-6");
}

/// Seed a replicated revision at generation `pos` and edit it.
async fn edit_at(app: &axum::Router, id: &str, pos: u64) -> Resp {
    let hash = "a".repeat(32);
    let rev = format!("{pos}-{hash}");
    let resp = post(
        app,
        "/db/_bulk_docs",
        json!({"new_edits": false, "docs": [{
            "_id": id, "_rev": rev, "v": 1,
            "_revisions": {"start": pos, "ids": [hash]},
        }]}),
    )
    .await;
    assert_eq!(resp.status, StatusCode::CREATED, "{:?}", resp.body);
    put(app, &format!("/db/{id}"), json!({"_rev": rev, "v": 2})).await
}

async fn check_generation_edits(app: axum::Router) {
    let resp = edit_at(&app, "below", MAX - 1).await;
    assert_eq!(resp.status, StatusCode::CREATED, "{:?}", resp.body);
    assert!(
        resp.json()["rev"]
            .as_str()
            .unwrap()
            .starts_with(&format!("{MAX}-"))
    );

    let resp = edit_at(&app, "at", MAX).await;
    assert_bad_request(&resp, "edit of generation MAX");
    let doc = get(&app, "/db/at").await.json();
    assert_eq!(doc["v"], 1);

    // The same refusal through _bulk_docs, as a row error.
    let resp = post(
        &app,
        "/db/_bulk_docs",
        json!({"docs": [{"_id": "at", "_rev": format!("{MAX}-{}", "a".repeat(32)), "v": 3}]}),
    )
    .await;
    assert_eq!(resp.status, StatusCode::CREATED);
    assert_eq!(resp.json()[0]["error"], "bad_request");

    // Deleting it would need the next generation too.
    let resp = call(
        &app,
        axum::http::Method::DELETE,
        &format!("/db/at?rev={MAX}-{}", "a".repeat(32)),
        None,
    )
    .await;
    assert_bad_request(&resp, "delete of generation MAX");

    let resp = put(&app, "/db/fresh", json!({"v": 1})).await;
    assert_eq!(resp.status, StatusCode::CREATED);
}

#[tokio::test]
async fn accepted_divergence_generation_stops_at_u64_max_memory() {
    check_generation_edits(app()).await;
}

#[tokio::test]
async fn accepted_divergence_generation_stops_at_u64_max_redb() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(dir.path().join("db.redb"), DB).unwrap());
    check_generation_edits(app_with(db, &config())).await;
}
