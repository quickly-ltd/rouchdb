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

/// Documents with conflicts, a deleted conflict branch, attachments and a
/// tombstone, written through the HTTP API so both variants hold the same
/// revisions.
async fn tricky(fields: &[&str]) -> Router {
    let (db, app) = mixed(fields).await;
    let resp = post(
        &app,
        "/db/_bulk_docs",
        json!({"new_edits": false, "docs": [
            {"_id": "c", "_rev": "1-aaa", "kind": "products", "name": "Base"},
            {"_id": "c", "_revisions": {"start": 2, "ids": ["bbb", "aaa"]},
             "kind": "products", "name": "B"},
            {"_id": "c", "_revisions": {"start": 2, "ids": ["ccc", "aaa"]},
             "kind": "products", "name": "C"},
            {"_id": "c", "_revisions": {"start": 2, "ids": ["ddd", "aaa"]},
             "_deleted": true},
        ]}),
    )
    .await;
    assert_eq!(resp.status, StatusCode::CREATED);
    let rev = db
        .put("a", json!({"kind": "products", "v": 1}))
        .await
        .unwrap()
        .rev
        .unwrap();
    db.put_attachment("a", "note.txt", &rev, b"hello".to_vec(), "text/plain")
        .await
        .unwrap();
    let rev = db
        .put("gone", json!({"kind": "products"}))
        .await
        .unwrap()
        .rev
        .unwrap();
    db.remove("gone", &rev).await.unwrap();
    app
}

/// The projection is an optimization only: every combination of `style`,
/// `include_docs` and `conflicts`, and selectors on `_attachments` or
/// `_deleted`, answer byte for byte what the full scan answers.
#[tokio::test]
async fn projection_answers_exactly_like_the_full_scan() {
    let selectors = [
        json!({"kind": "products"}),
        json!({"_attachments": {"$exists": true}}),
        json!({"_attachments": {"$exists": false}, "kind": "products"}),
        json!({"_deleted": true}),
        json!({"kind": {"$exists": false}}),
        json!({"name": {"$in": ["B", "C"]}}),
    ];
    let queries = [
        "",
        "&include_docs=true",
        "&style=all_docs",
        "&style=all_docs&include_docs=true&conflicts=true",
        "&conflicts=true",
        "&limit=1",
        "&limit=2&style=all_docs&include_docs=true",
    ];
    let plain = tricky(VARIANTS[0]).await;
    let projected = tricky(VARIANTS[1]).await;
    for selector in &selectors {
        for query in queries {
            let uri = format!("/db/_changes?filter=_selector{query}");
            let body = json!({"selector": selector});
            let expected = post(&plain, &uri, body.clone()).await.json();
            let actual = post(&projected, &uri, body).await.json();
            assert_eq!(actual, expected, "{uri} {selector}");
        }
    }
}

/// What [`Racy`] does right before the first candidate read of a projected
/// selector feed (a one-change page), i.e. after the projection was
/// refreshed for the request.
enum Race {
    /// Update `product-1`: it moves to a newer seq.
    Update,
    /// Destroy the database and write a new generation whose seq 11 is
    /// another document.
    Recreate,
    /// Destroy the database and write a new generation shorter than the
    /// first candidate's seq (four changes, a product at seq 4).
    RecreateShort,
}

/// A memory database that runs a [`Race`] once during a request.
struct Racy {
    inner: Database,
    race: std::sync::Mutex<Option<Race>>,
}

impl Racy {
    async fn run(&self, race: Race) {
        let db = &self.inner;
        match race {
            Race::Update => {
                let rev = db.get("product-1").await.unwrap().rev.unwrap().to_string();
                db.update(
                    "product-1",
                    &rev,
                    json!({"kind": "products", "name": "Tea 2"}),
                )
                .await
                .unwrap();
            }
            Race::RecreateShort => {
                db.destroy().await.unwrap();
                for i in 0..3 {
                    db.put(&format!("x{i}"), json!({"kind": "events"}))
                        .await
                        .unwrap();
                }
                db.put("intruder", json!({"kind": "products"}))
                    .await
                    .unwrap();
            }
            Race::Recreate => {
                db.destroy().await.unwrap();
                for i in 0..10 {
                    db.put(&format!("x{i}"), json!({"kind": "events"}))
                        .await
                        .unwrap();
                }
                db.put("intruder", json!({"kind": "products"}))
                    .await
                    .unwrap();
                db.put("another", json!({"kind": "products"}))
                    .await
                    .unwrap();
            }
        }
    }
}

#[async_trait::async_trait]
impl rouchdb::Adapter for Racy {
    async fn info(&self) -> rouchdb::Result<rouchdb::DbInfo> {
        self.inner.adapter().info().await
    }
    async fn id(&self) -> rouchdb::Result<String> {
        self.inner.adapter().id().await
    }
    async fn get(&self, id: &str, opts: rouchdb::GetOptions) -> rouchdb::Result<rouchdb::Document> {
        self.inner.adapter().get(id, opts).await
    }
    async fn bulk_docs(
        &self,
        docs: Vec<rouchdb::Document>,
        opts: rouchdb::BulkDocsOptions,
    ) -> rouchdb::Result<Vec<rouchdb::DocResult>> {
        self.inner.adapter().bulk_docs(docs, opts).await
    }
    async fn all_docs(
        &self,
        opts: rouchdb::AllDocsOptions,
    ) -> rouchdb::Result<rouchdb::AllDocsResponse> {
        self.inner.adapter().all_docs(opts).await
    }
    async fn changes(
        &self,
        opts: rouchdb::ChangesOptions,
    ) -> rouchdb::Result<rouchdb::ChangesResponse> {
        if opts.limit == Some(1) {
            let race = self.race.lock().unwrap().take();
            if let Some(race) = race {
                self.run(race).await;
            }
        }
        self.inner.adapter().changes(opts).await
    }
    async fn revs_diff(
        &self,
        revs: std::collections::HashMap<String, Vec<String>>,
    ) -> rouchdb::Result<rouchdb::RevsDiffResponse> {
        self.inner.adapter().revs_diff(revs).await
    }
    async fn bulk_get(
        &self,
        docs: Vec<rouchdb::BulkGetItem>,
    ) -> rouchdb::Result<rouchdb::BulkGetResponse> {
        self.inner.adapter().bulk_get(docs).await
    }
    async fn put_attachment(
        &self,
        doc_id: &str,
        att_id: &str,
        rev: &str,
        data: Vec<u8>,
        content_type: &str,
    ) -> rouchdb::Result<rouchdb::DocResult> {
        self.inner
            .adapter()
            .put_attachment(doc_id, att_id, rev, data, content_type)
            .await
    }
    async fn get_attachment(
        &self,
        doc_id: &str,
        att_id: &str,
        opts: rouchdb::GetAttachmentOptions,
    ) -> rouchdb::Result<Vec<u8>> {
        self.inner
            .adapter()
            .get_attachment(doc_id, att_id, opts)
            .await
    }
    async fn remove_attachment(
        &self,
        doc_id: &str,
        att_id: &str,
        rev: &str,
    ) -> rouchdb::Result<rouchdb::DocResult> {
        self.inner
            .adapter()
            .remove_attachment(doc_id, att_id, rev)
            .await
    }
    async fn get_local(&self, id: &str) -> rouchdb::Result<Value> {
        self.inner.adapter().get_local(id).await
    }
    async fn put_local(&self, id: &str, doc: Value) -> rouchdb::Result<()> {
        self.inner.adapter().put_local(id, doc).await
    }
    async fn remove_local(&self, id: &str) -> rouchdb::Result<()> {
        self.inner.adapter().remove_local(id).await
    }
    async fn compact(&self) -> rouchdb::Result<()> {
        self.inner.adapter().compact().await
    }
    async fn destroy(&self) -> rouchdb::Result<()> {
        self.inner.adapter().destroy().await
    }
}

/// A projected selector feed with a [`Race`] during its candidate reads,
/// and the plain feed's answer right after it.
async fn raced(race: Race) -> (Value, Value) {
    let selector = products();
    let (inner, _) = mixed(&[]).await;
    let inner = Arc::try_unwrap(inner).ok().unwrap();
    let racy = Arc::new(Racy {
        inner,
        race: std::sync::Mutex::new(Some(race)),
    });
    let db = Arc::new(Database::from_adapter(racy));
    let mut config = config();
    config.selector_index_fields = vec!["kind".into()];
    let projected = app_with(db.clone(), &config);
    let uri = "/db/_changes?filter=_selector&include_docs=true";
    let answer = post(&projected, uri, selector.clone()).await.json();
    let plain = app_with(db, &common::config());
    let expected = post(&plain, uri, selector).await.json();
    (answer, expected)
}

/// A document written while the request reads its candidates moved to a
/// newer seq: it is returned at that seq, as the full scan returns it.
#[tokio::test]
async fn document_moved_during_the_request_is_returned_at_its_new_seq() {
    let (answer, expected) = raced(Race::Update).await;
    assert_eq!(ids(&expected), ["product-2", "product-1"]);
    assert_eq!(answer, expected);
}

/// The database recreated while the request reads its candidates: a seq of
/// the old generation is not taken for the new document at that seq.
#[tokio::test]
async fn database_recreated_during_the_request_is_read_anew() {
    let (answer, expected) = raced(Race::Recreate).await;
    assert_eq!(ids(&expected), ["intruder", "another"]);
    assert_eq!(answer, expected);
}

/// The database recreated shorter than the first candidate's seq: the
/// empty candidate page is not taken for the end of the scan.
#[tokio::test]
async fn database_recreated_shorter_during_the_request_is_read_anew() {
    let (answer, expected) = raced(Race::RecreateShort).await;
    assert_eq!(ids(&expected), ["intruder"]);
    assert_eq!(answer, expected);
}
