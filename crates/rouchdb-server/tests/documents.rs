//! F109: ETag / If-None-Match / If-Match and coherent revision sources.
mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use common::*;
use rouchdb::Database;
use serde_json::json;

async fn with_doc() -> (Arc<Database>, axum::Router, String) {
    let db = Arc::new(Database::memory(DB));
    let rev = db.put("a", json!({"v": 1})).await.unwrap().rev.unwrap();
    let app = app_with(db.clone(), &config());
    (db, app, rev)
}

fn req(
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
    body: Option<serde_json::Value>,
) -> Request<Body> {
    let mut r = Request::builder().method(method).uri(uri);
    for (k, v) in headers {
        r = r.header(*k, *v);
    }
    match body {
        Some(b) => r
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(b.to_string()))
            .unwrap(),
        None => r.body(Body::empty()).unwrap(),
    }
}

#[tokio::test]
async fn get_sends_etag_and_honors_if_none_match() {
    let (_db, app, rev) = with_doc().await;
    let etag = format!("\"{rev}\"");

    let resp = get(&app, "/db/a").await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.header("etag"), Some(etag.as_str()));

    let resp = send(
        &app,
        req(Method::GET, "/db/a", &[("if-none-match", &etag)], None),
    )
    .await;
    assert_eq!(resp.status, StatusCode::NOT_MODIFIED);
    assert!(resp.body.is_empty());
    assert_eq!(resp.header("etag"), Some(etag.as_str()));

    let resp = send(
        &app,
        req(
            Method::GET,
            "/db/a",
            &[("if-none-match", "\"1-other\"")],
            None,
        ),
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn query_and_body_revs_must_agree() {
    let (db, app, rev) = with_doc().await;
    let resp = put(
        &app,
        &format!("/db/a?rev={rev}"),
        json!({"_rev": "1-other", "v": 2}),
    )
    .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        resp.json()["reason"],
        "Document rev from request body and query string have different values"
    );
    assert_eq!(db.get("a").await.unwrap().data["v"], 1);

    let resp = put(
        &app,
        &format!("/db/a?rev={rev}"),
        json!({"_rev": rev, "v": 2}),
    )
    .await;
    assert_eq!(resp.status, StatusCode::CREATED);
}

#[tokio::test]
async fn if_match_supplies_the_revision() {
    let (db, app, rev) = with_doc().await;
    let etag = format!("\"{rev}\"");

    let resp = send(
        &app,
        req(
            Method::PUT,
            "/db/a",
            &[("if-match", &etag)],
            Some(json!({"v": 2})),
        ),
    )
    .await;
    assert_eq!(resp.status, StatusCode::CREATED);
    let rev2 = resp.json()["rev"].as_str().unwrap().to_string();
    assert!(rev2.starts_with("2-"), "{rev2}");
    assert_eq!(resp.json(), json!({"ok": true, "id": "a", "rev": rev2}));
    assert_eq!(resp.header("etag"), Some(format!("\"{rev2}\"").as_str()));
    assert_eq!(db.get("a").await.unwrap().data, json!({"v": 2}));

    let resp = send(
        &app,
        req(
            Method::DELETE,
            "/db/a",
            &[("if-match", &format!("\"{rev2}\""))],
            None,
        ),
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);
    let rev3 = resp.json()["rev"].as_str().unwrap().to_string();
    assert!(rev3.starts_with("3-"), "{rev3}");
    assert_eq!(resp.json(), json!({"ok": true, "id": "a", "rev": rev3}));
    assert!(db.get("a").await.is_err());
}

#[tokio::test]
async fn if_match_must_agree_with_other_revisions() {
    let (db, app, rev) = with_doc().await;
    let resp = send(
        &app,
        req(
            Method::DELETE,
            &format!("/db/a?rev={rev}"),
            &[("if-match", "\"1-b\"")],
            None,
        ),
    )
    .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        resp.json()["reason"],
        "Document rev and etag have different values"
    );

    let resp = send(
        &app,
        req(
            Method::PUT,
            "/db/a",
            &[("if-match", "\"1-b\"")],
            Some(json!({"_rev": rev})),
        ),
    )
    .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    assert!(db.get("a").await.is_ok());
}

#[tokio::test]
async fn delete_without_any_revision_is_a_conflict() {
    let (_db, app, _rev) = with_doc().await;
    let resp = delete(&app, "/db/a").await;
    assert_eq!(resp.status, StatusCode::CONFLICT);
    assert_eq!(resp.json()["error"], "conflict");
}

#[tokio::test]
async fn design_docs_use_the_same_revision_rules() {
    let app = app();
    let resp = put(&app, "/db/_design/d", json!({"views": {}})).await;
    assert_eq!(resp.status, StatusCode::CREATED);
    let rev = resp.json()["rev"].as_str().unwrap().to_string();
    assert!(rev.starts_with("1-"), "{rev}");
    assert_eq!(
        resp.json(),
        json!({"ok": true, "id": "_design/d", "rev": rev})
    );

    let resp = put(
        &app,
        &format!("/db/_design/d?rev={rev}"),
        json!({"_rev": "1-x"}),
    )
    .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);

    let resp = put(
        &app,
        &format!("/db/_design/d?rev={rev}"),
        json!({"views": {}}),
    )
    .await;
    assert_eq!(resp.status, StatusCode::CREATED);
    let rev2 = resp.json()["rev"].as_str().unwrap().to_string();
    assert!(rev2.starts_with("2-"), "{rev2}");

    let resp = delete(&app, "/db/_design/d").await;
    assert_eq!(resp.status, StatusCode::CONFLICT);
    assert_eq!(resp.json()["error"], "conflict");
    let resp = send(
        &app,
        req(
            Method::DELETE,
            "/db/_design/d",
            &[("if-match", &format!("\"{rev2}\""))],
            None,
        ),
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);
    let rev3 = resp.json()["rev"].as_str().unwrap().to_string();
    assert!(rev3.starts_with("3-"), "{rev3}");
    assert_eq!(
        resp.json(),
        json!({"ok": true, "id": "_design/d", "rev": rev3})
    );
    assert_eq!(
        get(&app, "/db/_design/d").await.status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn design_docs_round_trip_every_member() {
    // F55: what Fauxton (or any client) reads from GET /_design/x and PUTs
    // back is stored unchanged: views.lib, Mango index views, options,
    // object-valued functions, custom fields and attachment stubs.
    let app = app();
    let ddoc = json!({
        "language": "javascript",
        "views": {
            "lib": {"util": "exports.twice = function(x){ return 2*x; };"},
            "by_type": {"map": "function (doc) {\n  emit(doc.type, 1);\n}",
                "reduce": "_count", "options": {"collation": "raw"}},
            "mango": {"map": {"fields": {"t": "asc"}, "partial_filter_selector": {}},
                "reduce": "_count", "options": {"def": {"fields": ["t"]}}}
        },
        "filters": {"erl": {"src": "x"}},
        "options": {"partitioned": false, "local_seq": true},
        "autoupdate": false,
        "rewrites": [{"from": "/a", "to": "/b"}],
        "custom": {"nested": [1, 2.5, "x", null, true]},
        "_attachments": {"index.html": {"content_type": "text/html", "data": "PGgxPmhpPC9oMT4="}}
    });
    let resp = put(&app, "/db/_design/app", ddoc.clone()).await;
    assert_eq!(resp.status, StatusCode::CREATED);
    let rev1 = resp.json()["rev"].as_str().unwrap().to_string();

    let got = get(&app, "/db/_design/app").await;
    assert_eq!(got.status, StatusCode::OK);
    assert_eq!(got.header("etag"), Some(format!("\"{rev1}\"").as_str()));
    let mut body = got.json();
    let stub = body["_attachments"]["index.html"].clone();
    assert_eq!(stub["stub"], true);
    assert_eq!(stub["length"], 11);
    let mut expected = ddoc.clone();
    expected["_id"] = json!("_design/app");
    expected["_rev"] = json!(rev1);
    expected["_attachments"] = json!({"index.html": stub});
    assert_eq!(body, expected);

    // PUT back what was read (with an edit): only the edit changes.
    body["custom"] = json!("edited");
    let resp = put(&app, "/db/_design/app", body.clone()).await;
    assert_eq!(resp.status, StatusCode::CREATED);
    let rev2 = resp.json()["rev"].as_str().unwrap().to_string();
    let mut again = get(&app, "/db/_design/app").await.json();
    assert_eq!(again["_rev"], json!(rev2));
    again["_rev"] = json!(rev1);
    assert_eq!(again, body);

    // GET takes the document query options, like any document.
    let old = get(&app, &format!("/db/_design/app?rev={rev1}"))
        .await
        .json();
    assert_eq!(old["custom"], ddoc["custom"]);
    let with_revs = get(&app, "/db/_design/app?revs=true").await.json();
    assert_eq!(with_revs["_revisions"]["start"], 2);
}

#[tokio::test]
async fn big_numbers_are_served_as_stored() {
    // With rouchdb's `arbitrary-precision` feature (serde_json's
    // `arbitrary_precision`), numbers keep their exact text through the
    // server; without it, an integer beyond u64 becomes a float.
    let big: serde_json::Value = serde_json::from_str("18446744073709551616").unwrap();
    let exact = serde_json::to_string(&big).unwrap() == "18446744073709551616";
    let app = app();
    let resp = send(
        &app,
        req(
            Method::PUT,
            "/db/n",
            &[],
            Some(serde_json::from_str(r#"{"big":18446744073709551616,"dec":1.50}"#).unwrap()),
        ),
    )
    .await;
    assert_eq!(resp.status, StatusCode::CREATED);
    let body = String::from_utf8(get(&app, "/db/n").await.body.to_vec()).unwrap();
    assert_eq!(
        body.contains(r#""big":18446744073709551616"#),
        exact,
        "{body}"
    );
    assert_eq!(body.contains(r#""dec":1.50"#), exact, "{body}");
}

/// A `PUT` with `_deleted: true` stores the rest of its body in the
/// tombstone, as CouchDB does; a plain `DELETE` still writes an empty one.
/// Both answer 200 OK, like CouchDB 3.5.1.
#[tokio::test]
async fn delete_via_put_keeps_the_tombstone_body() {
    let (db, app, rev) = with_doc().await;
    let rev = db
        .put_attachment("a", "note.txt", &rev, b"hi".to_vec(), "text/plain")
        .await
        .unwrap()
        .rev
        .unwrap();
    let resp = put(
        &app,
        "/db/a",
        json!({"_rev": rev, "_deleted": true, "kind": "order", "deleted_at": "2026-11-14"}),
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);
    let tomb_rev = resp.json()["rev"].as_str().unwrap().to_string();

    let doc = get(&app, &format!("/db/a?rev={tomb_rev}")).await.json();
    assert_eq!(doc["_deleted"], true);
    assert_eq!(doc["kind"], "order");
    assert_eq!(doc["deleted_at"], "2026-11-14");
    // An attachment the body does not name is not inherited by the
    // tombstone (rouchdb drops attachments on delete; see `write.rs`).
    assert!(doc.get("_attachments").is_none());

    let changes = get(&app, "/db/_changes?include_docs=true").await.json();
    let row = &changes["results"][0];
    assert_eq!(row["deleted"], true);
    assert_eq!(row["doc"]["kind"], "order");

    // Deleting the tombstone again is a conflict, as in CouchDB.
    let again = put(&app, "/db/a", json!({"_rev": tomb_rev, "_deleted": true})).await;
    assert_eq!(again.status, StatusCode::CONFLICT);

    let rev_b = db.put("b", json!({"v": 1})).await.unwrap().rev.unwrap();
    let resp = delete(&app, &format!("/db/b?rev={rev_b}")).await;
    assert_eq!(resp.status, StatusCode::OK);
    let rev_b = resp.json()["rev"].as_str().unwrap().to_string();
    let doc = get(&app, &format!("/db/b?rev={rev_b}")).await.json();
    assert_eq!(doc.as_object().unwrap().len(), 3, "{doc}");
}

/// Without a revision, CouchDB 3.5.1 creates a tombstone (with its body)
/// for a new id and answers 409 for a live or deleted document.
#[tokio::test]
async fn delete_via_put_without_a_revision() {
    let (db, app, _rev) = with_doc().await;
    let resp = put(&app, "/db/n", json!({"_deleted": true, "kind": "x"})).await;
    assert_eq!(resp.status, StatusCode::OK);
    let rev = resp.json()["rev"].as_str().unwrap().to_string();
    assert!(rev.starts_with("1-"), "{rev}");
    let doc = get(&app, &format!("/db/n?rev={rev}")).await.json();
    assert_eq!(doc["_deleted"], true);
    assert_eq!(doc["kind"], "x");

    let live = put(&app, "/db/a", json!({"_deleted": true})).await;
    assert_eq!(live.status, StatusCode::CONFLICT);
    assert!(db.get("a").await.is_ok());

    let tomb = put(&app, "/db/n", json!({"_deleted": true, "k": 1})).await;
    assert_eq!(tomb.status, StatusCode::CONFLICT);
}

/// An edit of a deleted document's tombstone is a conflict, as in CouchDB
/// 3.5.1 (`_rev` in the body, `?rev=` and `_bulk_docs`); without a
/// revision the document is re-created.
#[tokio::test]
async fn edit_of_a_tombstone_is_a_conflict() {
    let app = app();
    let rev = put(&app, "/db/a", json!({"v": 1})).await.json()["rev"]
        .as_str()
        .unwrap()
        .to_string();
    let tomb = delete(&app, &format!("/db/a?rev={rev}")).await.json()["rev"]
        .as_str()
        .unwrap()
        .to_string();

    let conflict = json!({"error": "conflict", "reason": "Document update conflict."});
    let resp = put(&app, "/db/a", json!({"_rev": tomb, "v": 2})).await;
    assert_eq!(
        (resp.status, resp.json()),
        (StatusCode::CONFLICT, conflict.clone())
    );
    let resp = put(&app, &format!("/db/a?rev={tomb}"), json!({"v": 2})).await;
    assert_eq!(
        (resp.status, resp.json()),
        (StatusCode::CONFLICT, conflict.clone())
    );
    let resp = post(
        &app,
        "/db/_bulk_docs",
        json!({"docs": [{"_id": "a", "_rev": tomb, "v": 3}]}),
    )
    .await;
    assert_eq!(resp.json()[0]["error"], "conflict");
    assert_eq!(get(&app, "/db/a").await.status, StatusCode::NOT_FOUND);

    let resp = put(&app, "/db/a", json!({"v": 9})).await;
    assert_eq!(resp.status, StatusCode::CREATED);
    assert!(resp.json()["rev"].as_str().unwrap().starts_with("3-"));
}

/// A `_local` `PUT` with `_deleted: true` deletes the document, as in
/// CouchDB 3.5.1: `200` with `rev: "0-0"`, whatever the revision sent (it
/// must still be well formed) and also when the document does not exist.
async fn check_local_delete_via_put(app: axum::Router) {
    let resp = put(&app, "/db/_local/x", json!({"v": 1})).await;
    assert_eq!(resp.status, StatusCode::CREATED);

    let resp = put(
        &app,
        "/db/_local/x",
        json!({"_rev": "0-1", "_deleted": true, "k": 1}),
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK, "{:?}", resp.body);
    assert_eq!(
        resp.json(),
        json!({"ok": true, "id": "_local/x", "rev": "0-0"})
    );
    let resp = get(&app, "/db/_local/x").await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
    assert_eq!(
        resp.json(),
        json!({"error": "not_found", "reason": "missing"})
    );

    // Already gone, without or with any revision: still 200.
    for uri in ["/db/_local/x", "/db/_local/x?rev=0-18446744073709551615"] {
        let resp = put(&app, uri, json!({"_deleted": true})).await;
        assert_eq!(resp.status, StatusCode::OK, "{uri}");
        assert_eq!(resp.json()["rev"], "0-0");
    }
    let resp = put(
        &app,
        "/db/_local/x",
        json!({"_deleted": true, "_rev": "abc"}),
    )
    .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);

    // `_deleted: false` writes the document, without the member.
    let resp = put(&app, "/db/_local/w", json!({"_deleted": false, "v": 1})).await;
    assert_eq!(resp.status, StatusCode::CREATED);
    assert_eq!(
        get(&app, "/db/_local/w").await.json(),
        json!({"_id": "_local/w", "_rev": "0-1", "v": 1})
    );
}

#[tokio::test]
async fn local_delete_via_put_memory() {
    check_local_delete_via_put(app()).await;
}

#[tokio::test]
async fn local_delete_via_put_redb() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(dir.path().join("db.redb"), DB).unwrap());
    check_local_delete_via_put(app_with(db, &config())).await;
}
