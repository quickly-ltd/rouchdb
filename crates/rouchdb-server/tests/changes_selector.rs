//! `_changes?filter=_selector`: `limit` counts matching changes only, the
//! selector may come from a GET query parameter, and the optional in-memory
//! projection (`ServerConfig::selector_index_fields`) returns exactly what a
//! full scan returns. Every scenario runs with and without the projection.
mod common;

use std::sync::Arc;

use axum::Router;
use axum::http::StatusCode;
use common::*;
use rouchdb::Database;
use serde_json::{Value, json};

/// Without the projection, and with one over `kind` and `day`.
const VARIANTS: [&[&str]; 2] = [&[], &["kind", "day"]];

/// Two products among twenty events of a closed day.
async fn mixed(fields: &[&str]) -> (Arc<Database>, Router) {
    let db = Arc::new(Database::memory(DB));
    for i in 0..10 {
        db.put(
            &format!("event:{i:04}"),
            json!({"kind": "events", "day": "day-1"}),
        )
        .await
        .unwrap();
    }
    db.put("product-1", json!({"kind": "products", "name": "Tea"}))
        .await
        .unwrap();
    for i in 10..20 {
        db.put(
            &format!("event:{i:04}"),
            json!({"kind": "events", "day": "day-1"}),
        )
        .await
        .unwrap();
    }
    db.put("product-2", json!({"kind": "products", "name": "Coffee"}))
        .await
        .unwrap();
    let mut config = config();
    config.selector_index_fields = fields.iter().map(|f| f.to_string()).collect();
    let app = app_with(db.clone(), &config);
    (db, app)
}

fn ids(body: &Value) -> Vec<String> {
    body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect()
}

fn since_of(body: &Value) -> String {
    match &body["last_seq"] {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn products() -> Value {
    json!({"selector": {"kind": "products"}})
}

#[tokio::test]
async fn post_filter_selector_returns_only_matching_changes() {
    for fields in VARIANTS {
        let (_db, app) = mixed(fields).await;
        let resp = post(
            &app,
            "/db/_changes?filter=_selector&include_docs=true",
            products(),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        let body = resp.json();
        assert_eq!(ids(&body), ["product-1", "product-2"], "{fields:?}");
        assert_eq!(body["results"][0]["doc"]["name"], "Tea");
    }
}

/// PouchDB treats an empty batch as "caught up": `limit` must count matching
/// changes only, and `last_seq` must not skip a match not yet returned.
#[tokio::test]
async fn limit_counts_only_matching_changes_and_last_seq_resumes() {
    for fields in VARIANTS {
        let (_db, app) = mixed(fields).await;
        let first = post(&app, "/db/_changes?filter=_selector&limit=1", products())
            .await
            .json();
        assert_eq!(ids(&first), ["product-1"], "{fields:?}");
        assert!(first["results"][0].get("doc").is_none());

        let uri = format!(
            "/db/_changes?filter=_selector&limit=1&since={}",
            since_of(&first)
        );
        let second = post(&app, &uri, products()).await.json();
        assert_eq!(ids(&second), ["product-2"], "{fields:?}");

        let uri = format!(
            "/db/_changes?filter=_selector&limit=1&since={}",
            since_of(&second)
        );
        let done = post(&app, &uri, products()).await.json();
        assert!(ids(&done).is_empty(), "{fields:?}");
    }
}

#[tokio::test]
async fn longpoll_with_selector_returns_matches_without_waiting() {
    for fields in VARIANTS {
        let (_db, app) = mixed(fields).await;
        let resp = post(
            &app,
            "/db/_changes?filter=_selector&feed=longpoll&timeout=200",
            products(),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(ids(&resp.json()), ["product-1", "product-2"], "{fields:?}");
    }
}

#[tokio::test]
async fn longpoll_with_selector_times_out_at_the_last_scanned_seq() {
    for fields in VARIANTS {
        let (_db, app) = mixed(fields).await;
        let resp = post(
            &app,
            "/db/_changes?filter=_selector&feed=longpoll&timeout=150",
            json!({"selector": {"kind": "tables"}}),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        let body = resp.json();
        assert!(ids(&body).is_empty());
        // The next request does not rescan what was already scanned.
        assert_eq!(since_of(&body), "22", "{fields:?}");
    }
}

#[tokio::test]
async fn get_filter_selector_reads_selector_from_query() {
    for fields in VARIANTS {
        let (_db, app) = mixed(fields).await;
        let selector = q(json!({"kind": "products", "name": "Coffee"}));
        let resp = get(
            &app,
            &format!("/db/_changes?filter=_selector&selector={selector}"),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(ids(&resp.json()), ["product-2"], "{fields:?}");

        // The POST body wins over the query parameter.
        let resp = post(
            &app,
            &format!("/db/_changes?filter=_selector&selector={selector}"),
            products(),
        )
        .await;
        assert_eq!(ids(&resp.json()), ["product-1", "product-2"]);

        let resp = get(&app, "/db/_changes?filter=_selector&selector=%5B1%5D").await;
        assert_eq!(resp.status, StatusCode::BAD_REQUEST);
        assert_eq!(resp.json()["reason"], "`selector` must be a JSON object");
    }
}

#[tokio::test]
async fn invalid_selector_is_bad_request() {
    let mut bodies = Vec::new();
    for fields in VARIANTS {
        let (_db, app) = mixed(fields).await;
        let resp = post(
            &app,
            "/db/_changes?filter=_selector",
            json!({"selector": {"kind": {"$bogus": 1}}}),
        )
        .await;
        assert_eq!(resp.status, StatusCode::BAD_REQUEST, "{fields:?}");
        bodies.push(resp.json());
    }
    assert_eq!(bodies[0]["error"], "invalid_operator");
    assert_eq!(bodies[0], bodies[1], "same answer with the projection");
}

#[tokio::test]
async fn field_less_tombstone_matches_on_its_deleted_body() {
    for fields in VARIANTS {
        let (db, app) = mixed(fields).await;
        let rev = db.get("product-2").await.unwrap().rev.unwrap().to_string();
        db.remove("product-2", &rev).await.unwrap();
        let body = post(
            &app,
            "/db/_changes?filter=_selector&include_docs=true",
            json!({"selector": {"kind": {"$exists": false}}}),
        )
        .await
        .json();
        assert_eq!(ids(&body), ["product-2"], "{fields:?}");
        assert_eq!(body["results"][0]["deleted"], true);
        assert_eq!(body["results"][0]["doc"]["_deleted"], true);
    }
}

#[tokio::test]
async fn projection_follows_new_and_updated_documents() {
    for fields in VARIANTS {
        let (db, app) = mixed(fields).await;
        let uri = "/db/_changes?filter=_selector&include_docs=true";
        let first = post(&app, uri, products()).await.json();
        assert_eq!(ids(&first), ["product-1", "product-2"]);

        // A product written later, a product moved to another kind, an event.
        db.put("product-3", json!({"kind": "products", "name": "Water"}))
            .await
            .unwrap();
        let rev = db.get("product-1").await.unwrap().rev.unwrap().to_string();
        db.update("product-1", &rev, json!({"kind": "tables"}))
            .await
            .unwrap();
        db.put("event:9999", json!({"kind": "events"}))
            .await
            .unwrap();

        let from_start = post(&app, uri, products()).await.json();
        assert_eq!(ids(&from_start), ["product-2", "product-3"], "{fields:?}");
        let uri = format!("/db/_changes?filter=_selector&since={}", since_of(&first));
        let incremental = post(&app, &uri, products()).await.json();
        assert_eq!(ids(&incremental), ["product-3"], "{fields:?}");
    }
}

/// `DELETE /{db}` restarts the sequences: a projection left at the old
/// position would never see the documents written afterwards.
#[tokio::test]
async fn selector_feed_sees_documents_written_after_database_delete() {
    for fields in VARIANTS {
        let (db, app) = mixed(fields).await;
        let before = post(&app, "/db/_changes?filter=_selector", products())
            .await
            .json();
        assert_eq!(ids(&before), ["product-1", "product-2"]);

        assert_eq!(delete(&app, "/db").await.status, StatusCode::OK);
        assert_eq!(
            put(&app, "/db", json!({})).await.status,
            StatusCode::CREATED
        );
        db.put("product-3", json!({"kind": "products", "name": "Water"}))
            .await
            .unwrap();

        let after = post(&app, "/db/_changes?filter=_selector", products())
            .await
            .json();
        assert_eq!(ids(&after), ["product-3"], "{fields:?}");
    }
}

/// A database destroyed behind the server's back (through the shared
/// `Database`) is detected by its changed identity.
#[tokio::test]
async fn projection_starts_over_when_the_database_is_destroyed_directly() {
    let (db, app) = mixed(VARIANTS[1]).await;
    let before = post(&app, "/db/_changes?filter=_selector", products())
        .await
        .json();
    assert_eq!(ids(&before), ["product-1", "product-2"]);

    db.destroy().await.unwrap();
    db.put("product-3", json!({"kind": "products"}))
        .await
        .unwrap();
    let after = post(&app, "/db/_changes?filter=_selector", products())
        .await
        .json();
    assert_eq!(ids(&after), ["product-3"]);
}

#[tokio::test]
async fn descending_and_doc_ids_feeds_are_unchanged() {
    for fields in VARIANTS {
        let (_db, app) = mixed(fields).await;
        let body = post(
            &app,
            "/db/_changes?filter=_selector&descending=true",
            products(),
        )
        .await
        .json();
        assert_eq!(ids(&body), ["product-2", "product-1"], "{fields:?}");

        let body = post(
            &app,
            "/db/_changes?filter=_doc_ids",
            json!({"doc_ids": ["product-2", "event:0003"]}),
        )
        .await
        .json();
        assert_eq!(ids(&body), ["event:0003", "product-2"], "{fields:?}");

        let body = get(&app, "/db/_changes").await.json();
        assert_eq!(ids(&body).len(), 22);
    }
}
