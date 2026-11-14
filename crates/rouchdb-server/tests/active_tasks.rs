mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::{Json, Router, routing::get, routing::post};
use rouchdb::{Database, ReplicationEvent, ReplicationOptions};
use rouchdb_server::{ActiveTasks, AdminCredentials, build_router_with_tasks};
use serde_json::json;
use tokio::time::{sleep, timeout};

async fn wait_for_idle(tasks: &ActiveTasks, written: u64) {
    timeout(Duration::from_secs(5), async {
        loop {
            if tasks
                .snapshot()
                .first()
                .is_some_and(|task| task.status == "idle" && task.docs_written == written)
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("replication should catch up");
}

#[tokio::test]
async fn registry_counts_batches_and_live_passes_without_double_counting() {
    let source = Database::memory("source");
    let target = Arc::new(Database::memory(common::DB));
    for i in 0..3 {
        source.put(&format!("d{i}"), json!({"i": i})).await.unwrap();
    }
    let tasks = ActiveTasks::new();
    let app = build_router_with_tasks(target.clone(), &common::config(), tasks.clone());
    let handle = tasks.start_live_replication(
        &source,
        &target,
        "remote",
        "local",
        ReplicationOptions {
            batch_size: 2,
            poll_interval: Duration::from_millis(20),
            ..Default::default()
        },
    );

    wait_for_idle(&tasks, 3).await;
    let first = common::get(&app, "/_active_tasks").await.json();
    assert_eq!(first[0]["docs_read"], 3);
    assert_eq!(first[0]["docs_written"], 3);
    assert_eq!(first[0]["doc_write_failures"], 0);

    for i in 3..5 {
        source.put(&format!("d{i}"), json!({"i": i})).await.unwrap();
    }
    wait_for_idle(&tasks, 5).await;
    let next = common::get(&app, "/_active_tasks").await.json();
    assert_eq!(next[0]["docs_read"], 5);
    assert_eq!(next[0]["docs_written"], 5);
    assert_eq!(target.info().await.unwrap().doc_count, 5);
    let summary = tasks.sync_status_summary("remote");
    assert_eq!(summary.state, "idle");
    assert_eq!(summary.docs_pulled, 5);

    handle.cancel();
    timeout(Duration::from_secs(5), async {
        while !tasks.snapshot().is_empty() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cancelled task should leave the registry");
    assert_eq!(common::get(&app, "/_active_tasks").await.json(), json!([]));
}

#[tokio::test]
async fn router_with_tasks_keeps_upstream_authentication() {
    let config = rouchdb_server::ServerConfig {
        admin: Some(AdminCredentials::parse("admin:secret").unwrap()),
        ..common::config()
    };
    let app = build_router_with_tasks(
        Arc::new(Database::memory(common::DB)),
        &config,
        ActiveTasks::new(),
    );
    assert_eq!(
        common::get(&app, "/_active_tasks").await.status,
        axum::http::StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn progress_reports_rejected_writes_without_claiming_success() {
    let app = Router::new()
        .route(
            "/",
            get(|| async { Json(json!({"uuid": "remote-server"})) }),
        )
        .route(
            "/remote",
            get(|| async { Json(json!({"db_name": "remote", "doc_count": 0, "update_seq": 0})) }),
        )
        .route(
            "/remote/_revs_diff",
            post(
                |Json(revs): Json<HashMap<String, Vec<String>>>| async move {
                    Json(
                        revs.into_iter()
                            .map(|(id, revs)| (id, json!({"missing": revs})))
                            .collect::<HashMap<_, _>>(),
                    )
                },
            ),
        )
        .route(
            "/remote/_bulk_docs",
            post(|| async {
                Json(json!([{
                    "id": "rejected", "error": "forbidden", "reason": "validation rejected"
                }]))
            }),
        )
        .route(
            "/remote/_local/{id}",
            get(|| async {
                (
                    axum::http::StatusCode::NOT_FOUND,
                    Json(json!({"error": "not_found", "reason": "missing"})),
                )
            })
            .put(|| async { Json(json!({"ok": true})) }),
        );
    let addr = common::serve(app).await;
    let source = Database::memory("source");
    source.put("rejected", json!({"value": 1})).await.unwrap();
    let target = Database::http(&format!("http://{addr}/remote"));
    let (result, mut events) = source
        .replicate_to_with_events(&target, ReplicationOptions::default())
        .await
        .unwrap();
    assert!(!result.ok);
    assert_eq!(result.docs_written, 0);
    let mut progress = None;
    while let Some(event) = events.recv().await {
        if let ReplicationEvent::Change {
            docs_read,
            docs_written,
            doc_write_failures,
            ..
        } = event
        {
            progress = Some((docs_read, docs_written, doc_write_failures));
        }
    }
    assert_eq!(progress, Some((1, 0, 1)));
}
