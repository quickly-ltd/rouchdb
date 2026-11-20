//! Every route that writes documents validates them with the request's
//! `UserCtx`: from authentication, from CouchDB's admin party without
//! configured credentials, or from a layer of the embedder that
//! authenticates requests itself.
mod common;

use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use base64::Engine;
use common::*;
use rouchdb::{Database, Document, OldDoc, Plugin, RouchError, UserCtx, WriteContext};
use rouchdb_server::{AdminCredentials, ServerConfig};
use serde_json::json;

/// Records `(doc id, user, new_edits)` per validated document and rejects
/// a document whose body has `"reject": reason`.
#[derive(Default)]
struct Recorder(Mutex<Vec<(String, UserCtx, bool)>>);

impl Recorder {
    fn take(&self) -> Vec<(String, UserCtx, bool)> {
        std::mem::take(&mut self.0.lock().unwrap())
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
        _old_doc: &OldDoc<'_>,
        ctx: &WriteContext<'_>,
    ) -> rouchdb::Result<()> {
        self.0
            .lock()
            .unwrap()
            .push((new_doc.id.clone(), ctx.user_ctx.clone(), ctx.new_edits));
        match new_doc.data.get("reject").and_then(|r| r.as_str()) {
            Some(reason) => Err(RouchError::Forbidden(reason.into())),
            None => Ok(()),
        }
    }
}

fn app_recording(config: &ServerConfig) -> (Router, Arc<Recorder>) {
    let recorder = Arc::new(Recorder::default());
    let db = Database::memory(DB).with_plugin(recorder.clone());
    (app_with(Arc::new(db), config), recorder)
}

fn satellite() -> UserCtx {
    UserCtx::new(Some("terminal-7".into()), vec!["satellite".into()])
}

/// The embedder's own authentication: a layer around the router that gives
/// every request its user.
fn behind_embedder_auth(router: Router) -> Router {
    router.layer(axum::middleware::from_fn(
        |mut req: axum::extract::Request, next: axum::middleware::Next| async move {
            req.extensions_mut().insert(satellite());
            next.run(req).await
        },
    ))
}

#[tokio::test]
async fn without_credentials_requests_write_as_admin_party() {
    let (app, rec) = app_recording(&config());
    assert_eq!(
        put(&app, "/db/a", json!({})).await.status,
        StatusCode::CREATED
    );
    assert_eq!(rec.take(), [("a".to_string(), UserCtx::admin(), true)]);
}

#[tokio::test]
async fn authenticated_admin_writes_with_its_name() {
    const USER: &str = "admin";
    let config = ServerConfig {
        admin: Some(AdminCredentials::parse("admin:s3cret").unwrap()),
        ..config()
    };
    let (app, rec) = app_recording(&config);
    let token = base64::engine::general_purpose::STANDARD.encode("admin:s3cret");
    let req = Request::builder()
        .method(Method::PUT)
        .uri("/db/a")
        .header(header::AUTHORIZATION, format!("Basic {token}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{}"))
        .unwrap();
    assert_eq!(send(&app, req).await.status, StatusCode::CREATED);
    let seen = rec.take();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].1.name.as_deref(), Some(USER));
    assert!(seen[0].1.is_admin());
}

#[tokio::test]
async fn every_write_route_passes_the_embedders_user_on() {
    let (app, rec) = app_recording(&config());
    // The embedder's users are database admins, so they may write design
    // documents (which plugins do not validate).
    let security = json!({"admins": {"names": [], "roles": ["satellite"]}});
    assert_eq!(
        put(&app, "/db/_security", security).await.status,
        StatusCode::OK
    );
    let app = behind_embedder_auth(app);

    let posted = post(&app, "/db", json!({"_id": "p"})).await;
    assert_eq!(posted.status, StatusCode::CREATED);
    let a = put(&app, "/db/a", json!({})).await.json();
    let a_rev = a["rev"].as_str().unwrap().to_string();
    let att = send(
        &app,
        Request::builder()
            .method(Method::PUT)
            .uri(format!("/db/a/f.txt?rev={a_rev}"))
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from("hi"))
            .unwrap(),
    )
    .await;
    assert_eq!(att.status, StatusCode::CREATED, "{:?}", att.body);
    let att_rev = att.json()["rev"].as_str().unwrap().to_string();
    let deleted_att = delete(&app, &format!("/db/a/f.txt?rev={att_rev}")).await;
    assert_eq!(deleted_att.status, StatusCode::OK);
    let rev = deleted_att.json()["rev"].as_str().unwrap().to_string();
    assert_eq!(
        delete(&app, &format!("/db/a?rev={rev}")).await.status,
        StatusCode::OK
    );

    let ddoc = put(&app, "/db/_design/d", json!({})).await.json();
    let ddoc_rev = ddoc["rev"].as_str().unwrap();
    let removed = delete(&app, &format!("/db/_design/d?rev={ddoc_rev}")).await;
    assert_eq!(removed.status, StatusCode::OK);

    let index = post(
        &app,
        "/db/_index",
        json!({"index": {"fields": ["n"]}, "name": "n"}),
    )
    .await;
    assert_eq!(index.status, StatusCode::OK, "{:?}", index.body);
    let ddoc = index.json()["id"].as_str().unwrap().to_string();
    let ddoc_name = ddoc.trim_start_matches("_design/");
    let gone = delete(&app, &format!("/db/_index/{ddoc_name}/json/n")).await;
    assert_eq!(gone.status, StatusCode::OK, "{:?}", gone.body);

    let bulk = post(&app, "/db/_bulk_docs", json!({"docs": [{"_id": "b"}]})).await;
    assert_eq!(bulk.status, StatusCode::CREATED);

    let seen = rec.take();
    let ids: Vec<&str> = seen.iter().map(|(id, _, _)| id.as_str()).collect();
    assert_eq!(ids, ["p", "a", "a", "a", "a", "b"], "{seen:?}");
    assert!(
        seen.iter().all(|(_, user, _)| *user == satellite()),
        "{seen:?}"
    );
}

#[tokio::test]
async fn rejected_documents_are_denied_one_by_one() {
    let (app, rec) = app_recording(&config());
    let app = behind_embedder_auth(app);

    // A single write is a 403 with the plugin's reason.
    let resp = put(&app, "/db/x", json!({"reject": "foreign tenant"})).await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN);
    assert_eq!(
        resp.json(),
        json!({"error": "forbidden", "reason": "foreign tenant"})
    );

    // `_bulk_docs` reports it among the others, in order.
    let resp = post(
        &app,
        "/db/_bulk_docs",
        json!({"docs": [{"_id": "1"}, {"_id": "2", "reject": "no"}, {"_id": "3"}]}),
    )
    .await;
    let body = resp.json();
    assert_eq!(body[0]["ok"], true);
    assert_eq!(
        body[1],
        json!({"id": "2", "error": "forbidden", "reason": "no"})
    );
    assert_eq!(body[2]["ok"], true);

    // Replicated (`new_edits: false`) writes answer with the failures only.
    let resp = post(
        &app,
        "/db/_bulk_docs",
        json!({"new_edits": false, "docs": [
            {"_id": "r1", "_rev": "1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
            {"_id": "r2", "_rev": "1-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "reject": "closed day"},
        ]}),
    )
    .await;
    assert_eq!(resp.status, StatusCode::CREATED);
    assert_eq!(
        resp.json(),
        json!([{
            "id": "r2",
            "rev": "1-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "error": "forbidden",
            "reason": "closed day",
        }])
    );
    assert_eq!(get(&app, "/db/r1").await.status, StatusCode::OK);
    assert_eq!(get(&app, "/db/r2").await.status, StatusCode::NOT_FOUND);

    let replicated: Vec<bool> = rec
        .take()
        .into_iter()
        .filter(|(id, _, _)| id.starts_with('r'))
        .map(|(_, _, new_edits)| new_edits)
        .collect();
    assert_eq!(replicated, [false, false]);
}

#[tokio::test]
async fn design_documents_need_an_admin_and_skip_plugins() {
    let (app, rec) = app_recording(&config());
    // The embedder's user is not an admin.
    let lan = behind_embedder_auth(app.clone());
    let resp = put(&lan, "/db/_design/d", json!({"views": {}})).await;
    assert_eq!(resp.status, StatusCode::UNAUTHORIZED, "{:?}", resp.body);
    // A single write keeps the error kind; `RouchError::Unauthorized` has no
    // reason, so the server's own message replaces CouchDB's.
    assert_eq!(resp.json()["error"], "unauthorized");
    let bulk = post(
        &lan,
        "/db/_bulk_docs",
        json!({"docs": [{"_id": "_design/e"}]}),
    )
    .await;
    assert_eq!(
        bulk.json(),
        json!([{"id": "_design/e", "error": "unauthorized", "reason": "You are not a db or server admin."}])
    );
    // The admin party may, and plugins do not see design or local docs.
    assert_eq!(
        put(&app, "/db/_design/d", json!({"views": {}}))
            .await
            .status,
        StatusCode::CREATED
    );
    assert_eq!(
        put(&lan, "/db/_local/x", json!({"reject": "no"}))
            .await
            .status,
        StatusCode::CREATED
    );
    assert!(rec.take().is_empty());
}

#[tokio::test]
async fn routes_without_require_auth_default_to_the_same_user() {
    use rouchdb_server::routes::build_routes;
    use rouchdb_server::state::AppState;
    let recorder = Arc::new(Recorder::default());
    let db = Arc::new(Database::memory(DB).with_plugin(recorder.clone()));
    let routes = build_routes(AppState::new(db, DB.to_string(), None));
    assert_eq!(
        put(&routes, "/db/a", json!({})).await.status,
        StatusCode::CREATED
    );
    assert_eq!(recorder.take()[0].1, UserCtx::admin());
}

#[tokio::test]
async fn admin_endpoints_enforce_server_and_db_admin_roles() {
    let (app, _rec) = app_recording(&config());
    let non_admin = behind_embedder_auth(app.clone());

    // Non-admin embedder user gets 401 on admin endpoints and cannot escalate
    // privileges via `_security`.
    let resp = put(
        &non_admin,
        "/db/_security",
        json!({"admins": {"names": [], "roles": ["satellite"]}}),
    )
    .await;
    assert_eq!(resp.status, StatusCode::UNAUTHORIZED, "{:?}", resp.body);
    assert_eq!(resp.json()["error"], "unauthorized");

    let resp = send(
        &non_admin,
        Request::builder()
            .method(Method::POST)
            .uri("/db/_compact")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status, StatusCode::UNAUTHORIZED, "{:?}", resp.body);
    assert_eq!(resp.json()["error"], "unauthorized");

    let resp = post(&non_admin, "/db/_purge", json!({})).await;
    assert_eq!(resp.status, StatusCode::UNAUTHORIZED, "{:?}", resp.body);
    assert_eq!(resp.json()["error"], "unauthorized");

    let resp = put(&non_admin, "/db", json!({})).await;
    assert_eq!(resp.status, StatusCode::UNAUTHORIZED, "{:?}", resp.body);
    assert_eq!(resp.json()["error"], "unauthorized");

    let resp = delete(&non_admin, "/db").await;
    assert_eq!(resp.status, StatusCode::UNAUTHORIZED, "{:?}", resp.body);
    assert_eq!(resp.json()["error"], "unauthorized");

    // Promote `satellite` to a database admin via server admin (admin party).
    assert_eq!(
        put(
            &app,
            "/db/_security",
            json!({"admins": {"names": [], "roles": ["satellite"]}}),
        )
        .await
        .status,
        StatusCode::OK
    );

    // DB admin succeeds on `PUT /db/_security` and `POST /db/_compact`,
    // while still getting 401 on `DELETE /db` and `PUT /db`.
    assert_eq!(
        put(
            &non_admin,
            "/db/_security",
            json!({"admins": {"names": [], "roles": ["satellite"]}}),
        )
        .await
        .status,
        StatusCode::OK
    );
    let compact = send(
        &non_admin,
        Request::builder()
            .method(Method::POST)
            .uri("/db/_compact")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(compact.status, StatusCode::ACCEPTED, "{:?}", compact.body);
    let purged = post(&non_admin, "/db/_purge", json!({})).await;
    assert_eq!(purged.status, StatusCode::CREATED, "{:?}", purged.body);

    assert_eq!(
        delete(&non_admin, "/db").await.status,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        put(&non_admin, "/db", json!({})).await.status,
        StatusCode::UNAUTHORIZED
    );

    // Server admin (`_admin` role) succeeds on `DELETE /db` and `PUT /db`.
    let server_admin = app.clone().layer(axum::middleware::from_fn(
        |mut req: axum::extract::Request, next: axum::middleware::Next| async move {
            req.extensions_mut().insert(UserCtx::admin());
            next.run(req).await
        },
    ));
    assert_eq!(delete(&server_admin, "/db").await.status, StatusCode::OK);
    let recreated = send(
        &server_admin,
        Request::builder()
            .method(Method::PUT)
            .uri("/db")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        recreated.status,
        StatusCode::CREATED,
        "{:?}",
        recreated.body
    );
}

#[tokio::test]
async fn revless_attachment_upload_preserves_forbidden_and_unauthorized() {
    struct RejectById;

    #[async_trait::async_trait]
    impl Plugin for RejectById {
        fn name(&self) -> &str {
            "reject-by-id"
        }

        async fn validate_doc_update(
            &self,
            new_doc: &Document,
            _old_doc: &OldDoc<'_>,
            _ctx: &WriteContext<'_>,
        ) -> rouchdb::Result<()> {
            match new_doc.id.as_str() {
                "forbid" => Err(RouchError::Forbidden("no attachments".into())),
                "unauth" => Err(RouchError::Unauthorized),
                _ => Ok(()),
            }
        }
    }

    let db = Database::memory(DB).with_plugin(Arc::new(RejectById));
    let app = behind_embedder_auth(app_with(Arc::new(db), &config()));

    let resp = send(
        &app,
        Request::builder()
            .method(Method::PUT)
            .uri("/db/forbid/file.txt")
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from("hi"))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN, "{:?}", resp.body);
    assert_eq!(
        resp.json(),
        json!({"error": "forbidden", "reason": "no attachments"})
    );

    let resp = send(
        &app,
        Request::builder()
            .method(Method::PUT)
            .uri("/db/unauth/file.txt")
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from("hi"))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status, StatusCode::UNAUTHORIZED, "{:?}", resp.body);
    assert_eq!(resp.json()["error"], "unauthorized");
}
