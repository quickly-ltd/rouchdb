//! F49: `_find` pages with a real bookmark and defaults to limit 25.
//! F63: Mango indexes are persisted as `language: "query"` design documents.
mod common;

use std::collections::HashSet;
use std::sync::Arc;

use axum::http::StatusCode;
use common::*;
use rouchdb::Database;
use serde_json::json;

async fn thirty_docs() -> (Arc<Database>, axum::Router) {
    let db = Arc::new(Database::memory(DB));
    for i in 0..30 {
        db.put(&format!("d{i:02}"), json!({"n": i})).await.unwrap();
    }
    let app = app_with(db.clone(), &config());
    (db, app)
}

fn doc_ids(resp: &Resp) -> Vec<String> {
    resp.json()["docs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["_id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn find_defaults_to_25_results() {
    let (_db, app) = thirty_docs().await;
    let resp = post(&app, "/db/_find", json!({"selector": {"n": {"$gte": 0}}})).await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(doc_ids(&resp).len(), 25);

    let resp = post(
        &app,
        "/db/_find",
        json!({"selector": {"n": {"$gte": 0}}, "limit": 100}),
    )
    .await;
    assert_eq!(doc_ids(&resp).len(), 30);
}

#[tokio::test]
async fn find_bookmark_pages_through_all_results() {
    let (_db, app) = thirty_docs().await;
    let mut seen = HashSet::new();
    let mut bookmark: Option<String> = None;
    let mut pages = 0;
    loop {
        let mut body = json!({"selector": {"n": {"$gte": 0}}, "limit": 7});
        if let Some(b) = &bookmark {
            body["bookmark"] = json!(b);
        }
        let resp = post(&app, "/db/_find", body).await;
        assert_eq!(resp.status, StatusCode::OK);
        let ids = doc_ids(&resp);
        let next = resp.json()["bookmark"].as_str().unwrap().to_string();
        if ids.is_empty() {
            break;
        }
        for id in ids {
            assert!(seen.insert(id.clone()), "{id} returned twice");
        }
        assert_ne!(next, "nil");
        bookmark = Some(next);
        pages += 1;
        assert!(pages < 10, "pagination does not terminate");
    }
    assert_eq!(seen.len(), 30);
    assert_eq!(pages, 5);
}

#[tokio::test]
async fn find_bookmark_combines_with_skip() {
    let (_db, app) = thirty_docs().await;
    let selector = json!({"n": {"$gte": 0}});
    let sort = json!([{"n": "asc"}]);
    let first = post(
        &app,
        "/db/_find",
        json!({"selector": selector, "sort": sort, "limit": 3}),
    )
    .await;
    assert_eq!(doc_ids(&first), ["d00", "d01", "d02"]);
    let bookmark = first.json()["bookmark"].clone();

    let resp = post(
        &app,
        "/db/_find",
        json!({"selector": selector, "sort": sort, "limit": 2, "skip": 2, "bookmark": bookmark}),
    )
    .await;
    assert_eq!(doc_ids(&resp), ["d05", "d06"]);
}

#[tokio::test]
async fn find_rejects_invalid_bookmarks() {
    let (_db, app) = thirty_docs().await;
    for bookmark in ["garbage", "nil"] {
        let resp = post(
            &app,
            "/db/_find",
            json!({"selector": {}, "bookmark": bookmark}),
        )
        .await;
        assert_eq!(resp.status, StatusCode::BAD_REQUEST, "{bookmark}");
        assert_eq!(resp.json()["error"], "invalid_bookmark");
    }
}

#[tokio::test]
async fn find_without_matches_returns_nil_bookmark() {
    let (_db, app) = thirty_docs().await;
    let resp = post(&app, "/db/_find", json!({"selector": {"n": {"$gt": 1000}}})).await;
    assert_eq!(resp.json(), json!({"docs": [], "bookmark": "nil"}));
}

// ─── F63: persisted indexes ─────────────────────────────────────────────────

fn index_names(resp: &Resp) -> Vec<(serde_json::Value, String)> {
    resp.json()["indexes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| (i["ddoc"].clone(), i["name"].as_str().unwrap().to_string()))
        .collect()
}

#[tokio::test]
async fn create_index_writes_a_query_design_doc() {
    let db = Arc::new(Database::memory(DB));
    let app = app_with(db.clone(), &config());

    let body = json!({"index": {"fields": ["n"]}, "name": "by-n", "ddoc": "my-idx"});
    let resp = post(&app, "/db/_index", body.clone()).await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(
        resp.json(),
        json!({"result": "created", "id": "_design/my-idx", "name": "by-n"})
    );
    let resp = post(&app, "/db/_index", body).await;
    assert_eq!(
        resp.json(),
        json!({"result": "exists", "id": "_design/my-idx", "name": "by-n"})
    );
    let resp = post(
        &app,
        "/db/_index",
        json!({"index": {"fields": [{"t": "desc"}]}, "name": "by-t", "ddoc": "_design/my-idx"}),
    )
    .await;
    assert_eq!(resp.json()["result"], "created");

    let ddoc = db.get("_design/my-idx").await.unwrap().to_json();
    assert_eq!(ddoc["language"], "query");
    assert_eq!(ddoc["views"]["by-n"]["map"]["fields"], json!({"n": "asc"}));
    assert_eq!(
        ddoc["views"]["by-n"]["options"]["def"]["fields"],
        json!(["n"])
    );
    assert_eq!(ddoc["views"]["by-t"]["map"]["fields"], json!({"t": "desc"}));

    let resp = get(&app, "/db/_index").await;
    assert_eq!(
        index_names(&resp),
        [
            (json!(null), "_all_docs".to_string()),
            (json!("_design/my-idx"), "by-n".to_string()),
            (json!("_design/my-idx"), "by-t".to_string()),
        ]
    );
}

#[tokio::test]
async fn unnamed_index_gets_a_design_doc() {
    let db = Arc::new(Database::memory(DB));
    let app = app_with(db.clone(), &config());
    let resp = post(&app, "/db/_index", json!({"index": {"fields": ["n"]}})).await;
    let body = resp.json();
    assert_eq!(body["result"], "created");
    let id = body["id"].as_str().unwrap();
    assert!(id.starts_with("_design/"), "{body}");
    let name = body["name"].as_str().unwrap();
    assert!(db.get(id).await.unwrap().data["views"][name].is_object());
}

#[tokio::test]
async fn indexes_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.redb");
    {
        let db = Arc::new(Database::open(&path, DB).unwrap());
        let app = app_with(db, &config());
        let resp = post(
            &app,
            "/db/_index",
            json!({"index": {"fields": ["n"]}, "name": "by-n", "ddoc": "my-idx"}),
        )
        .await;
        assert_eq!(resp.json()["result"], "created");
    }

    let db = Arc::new(Database::open(&path, DB).unwrap());
    rouchdb_server::restore_indexes(&db).await.unwrap();
    let names: Vec<String> = db.get_indexes().await.into_iter().map(|i| i.name).collect();
    assert_eq!(names, ["by-n"]);

    let app = app_with(db, &config());
    let resp = get(&app, "/db/_index").await;
    assert!(index_names(&resp).contains(&(json!("_design/my-idx"), "by-n".to_string())));
}

#[tokio::test]
async fn delete_index_requires_matching_ddoc_and_type() {
    let db = Arc::new(Database::memory(DB));
    let app = app_with(db.clone(), &config());
    for (name, field) in [("by-n", "n"), ("by-t", "t")] {
        let body = json!({"index": {"fields": [field]}, "name": name, "ddoc": "my-idx"});
        assert_eq!(
            post(&app, "/db/_index", body).await.json()["result"],
            "created"
        );
    }

    for uri in [
        "/db/_index/other/json/by-n",
        "/db/_index/my-idx/text/by-n",
        "/db/_index/my-idx/json/nope",
    ] {
        let resp = delete(&app, uri).await;
        assert_eq!(resp.status, StatusCode::NOT_FOUND, "{uri}");
        assert_eq!(
            resp.json(),
            json!({"error": "not_found", "reason": "missing"}),
            "{uri}"
        );
    }
    assert_eq!(db.get_indexes().await.len(), 2);

    let resp = delete(&app, "/db/_index/my-idx/json/by-n").await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.json(), json!({"ok": true}));
    let ddoc = db.get("_design/my-idx").await.unwrap().to_json();
    assert!(ddoc["views"].get("by-n").is_none());
    assert!(ddoc["views"]["by-t"].is_object());

    // The last index of a design document removes the document.
    let resp = delete(&app, "/db/_index/_design/my-idx/json/by-t").await;
    assert_eq!(resp.status, StatusCode::OK);
    assert!(db.get("_design/my-idx").await.is_err());
    assert!(db.get_indexes().await.is_empty());
}

#[tokio::test]
async fn bulk_delete_removes_index_design_docs() {
    let db = Arc::new(Database::memory(DB));
    let app = app_with(db.clone(), &config());
    let body = json!({"index": {"fields": ["n"]}, "name": "by-n", "ddoc": "my-idx"});
    post(&app, "/db/_index", body).await;

    let resp = post(
        &app,
        "/db/_index/_bulk_delete",
        json!({"docids": ["_design/my-idx", "_design/nope"]}),
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(
        resp.json(),
        json!({
            "success": [{"id": "_design/my-idx", "ok": true}],
            "fail": [{"id": "_design/nope", "error": "not_found"}],
        })
    );
    assert!(db.get("_design/my-idx").await.is_err());
    assert!(db.get_indexes().await.is_empty());
}

// ─── F04: an index is identified by (ddoc, name) ────────────────────────────

/// `_explain`'s index as `(ddoc, name, fields)`.
async fn explained(app: &axum::Router, selector: serde_json::Value) -> serde_json::Value {
    let index = post(app, "/db/_explain", json!({ "selector": selector }))
        .await
        .json()["index"]
        .clone();
    // Fields as `{field: direction}`, as `_index` lists them.
    let fields: Vec<_> = index["def"]["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| match f.as_str() {
            Some(name) => json!({ name: "asc" }),
            None => f.clone(),
        })
        .collect();
    json!([index["ddoc"], index["name"], fields])
}

async fn index_defs(app: &axum::Router) -> Vec<serde_json::Value> {
    get(app, "/db/_index").await.json()["indexes"]
        .as_array()
        .unwrap()
        .iter()
        .skip(1)
        .map(|i| json!([i["ddoc"], i["name"], i["def"]["fields"]]))
        .collect()
}

async fn x_y_docs(db: &Database) {
    db.put("a", json!({"x": 1, "y": 2})).await.unwrap();
    db.put("b", json!({"x": 2, "y": 1})).await.unwrap();
}

/// Same checks as CouchDB 3.5.1: the same name in two design documents is
/// two indexes, and re-creating `(ddoc, name)` with other fields replaces it.
#[tokio::test]
async fn same_index_name_in_two_ddocs_and_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.redb");
    {
        let db = Arc::new(Database::open(&path, DB).unwrap());
        x_y_docs(&db).await;
        let app = app_with(db.clone(), &config());
        let create = |fields: serde_json::Value, ddoc: &str| json!({"index": {"fields": fields}, "name": "same", "ddoc": ddoc});

        let resp = post(&app, "/db/_index", create(json!(["x"]), "first")).await;
        assert_eq!(resp.json()["result"], "created");
        let resp = post(&app, "/db/_index", create(json!(["y"]), "second")).await;
        assert_eq!(
            resp.json(),
            json!({"result": "created", "id": "_design/second", "name": "same"})
        );
        let resp = post(&app, "/db/_index", create(json!(["y"]), "second")).await;
        assert_eq!(resp.json()["result"], "exists");
        assert_eq!(
            explained(&app, json!({"y": {"$gt": 0}})).await,
            json!(["_design/second", "same", [{"y": "asc"}]])
        );
        assert_eq!(
            explained(&app, json!({"x": {"$gt": 0}})).await,
            json!(["_design/first", "same", [{"x": "asc"}]])
        );

        // first/same now indexes y: the stored definition, the list and the
        // plan all show it; a query on x is a full scan.
        let resp = post(&app, "/db/_index", create(json!(["y"]), "first")).await;
        assert_eq!(
            resp.json(),
            json!({"result": "created", "id": "_design/first", "name": "same"})
        );
        let ddoc = db.get("_design/first").await.unwrap().to_json();
        assert_eq!(ddoc["views"]["same"]["map"]["fields"], json!({"y": "asc"}));
        assert_eq!(
            index_defs(&app).await,
            [
                json!(["_design/first", "same", [{"y": "asc"}]]),
                json!(["_design/second", "same", [{"y": "asc"}]]),
            ]
        );
        assert_eq!(
            explained(&app, json!({"x": {"$gt": 0}})).await,
            json!([null, "_all_docs", []])
        );
        let resp = post(&app, "/db/_find", json!({"selector": {"x": {"$gt": 0}}})).await;
        assert_eq!(doc_ids(&resp), ["a", "b"]);
        let resp = post(&app, "/db/_find", json!({"selector": {"y": {"$gt": 1}}})).await;
        assert_eq!(doc_ids(&resp), ["a"]);

        // Deleting second/same keeps first/same.
        let resp = delete(&app, "/db/_index/second/json/same").await;
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(
            index_defs(&app).await,
            [json!(["_design/first", "same", [{"y": "asc"}]])]
        );
        assert_eq!(
            explained(&app, json!({"y": {"$gt": 0}})).await,
            json!(["_design/first", "same", [{"y": "asc"}]])
        );
    }

    // After a restart the indexes are rebuilt from the stored definitions.
    let db = Arc::new(Database::open(&path, DB).unwrap());
    rouchdb_server::restore_indexes(&db).await.unwrap();
    let app = app_with(db, &config());
    assert_eq!(
        index_defs(&app).await,
        [json!(["_design/first", "same", [{"y": "asc"}]])]
    );
    assert_eq!(
        explained(&app, json!({"y": {"$gt": 0}})).await,
        json!(["_design/first", "same", [{"y": "asc"}]])
    );
    assert_eq!(
        explained(&app, json!({"x": {"$gt": 0}})).await,
        json!([null, "_all_docs", []])
    );
    let resp = post(&app, "/db/_find", json!({"selector": {"y": {"$gt": 1}}})).await;
    assert_eq!(doc_ids(&resp), ["a"]);
}

/// Bulk-deleting a design document drops its indexes only, not the
/// same-named index of another design document.
#[tokio::test]
async fn bulk_delete_keeps_same_name_in_other_ddoc() {
    let db = Arc::new(Database::memory(DB));
    x_y_docs(&db).await;
    let app = app_with(db.clone(), &config());
    for (field, ddoc) in [("x", "first"), ("y", "second")] {
        let body = json!({"index": {"fields": [field]}, "name": "same", "ddoc": ddoc});
        post(&app, "/db/_index", body).await;
    }
    let resp = post(
        &app,
        "/db/_index/_bulk_delete",
        json!({"docids": ["_design/first"]}),
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);
    let left: Vec<_> = db
        .get_indexes()
        .await
        .into_iter()
        .map(|i| (i.ddoc, i.name))
        .collect();
    assert_eq!(
        left,
        [(Some("_design/second".to_string()), "same".to_string())]
    );
    assert_eq!(
        explained(&app, json!({"y": 1})).await,
        json!(["_design/second", "same", [{"y": "asc"}]])
    );
}
