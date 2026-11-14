mod common;

use axum::http::{Method, StatusCode};
use serde_json::json;

#[tokio::test]
async fn trailing_slash_database_url_keeps_the_same_lifecycle() {
    let app = common::app();
    assert_eq!(common::get(&app, "/db/").await.status, StatusCode::OK);
    assert_eq!(
        common::call(&app, Method::HEAD, "/db/", None).await.status,
        StatusCode::OK
    );
    assert_eq!(
        common::put(&app, "/db/", json!({})).await.status,
        StatusCode::PRECONDITION_FAILED
    );
    let created = common::post(&app, "/db/", json!({"value": 1})).await;
    assert_eq!(created.status, StatusCode::CREATED);
    let id = created.json()["id"].as_str().unwrap().to_string();
    assert_eq!(
        common::get(&app, &format!("/db/{id}")).await.json()["value"],
        1
    );
    assert_eq!(common::delete(&app, "/db/").await.status, StatusCode::OK);
    assert_eq!(
        common::get(&app, "/db/").await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        common::put(&app, "/db/", json!({})).await.status,
        StatusCode::CREATED
    );
    assert_eq!(common::get(&app, "/db/").await.json()["doc_count"], 0);
}
