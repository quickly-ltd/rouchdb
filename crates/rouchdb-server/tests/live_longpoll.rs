//! A live replication from a remote source (which cannot announce its
//! changes) waits for them on the source's longpoll feed instead of polling
//! `_changes` every `poll_interval`, and still picks a change up at once.
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::*;
use rouchdb::{Database, ReplicationEvent, ReplicationOptions};
use serde_json::json;

#[tokio::test]
async fn idle_live_replication_from_http_waits_on_longpoll() {
    let server_db = Arc::new(Database::memory(DB));
    let app = app_with(server_db.clone(), &config());
    // Count the `_changes` requests the replication sends.
    let changes_requests = Arc::new(AtomicUsize::new(0));
    let counted = {
        let count = changes_requests.clone();
        app.clone().layer(axum::middleware::from_fn(
            move |req: axum::extract::Request, next: axum::middleware::Next| {
                if req.uri().path().ends_with("/_changes") {
                    count.fetch_add(1, Ordering::SeqCst);
                }
                next.run(req)
            },
        ))
    };
    let addr = serve(counted).await;
    put(&app, "/db/first", json!({})).await;

    let source = Database::http(&format!("http://{addr}/{DB}"));
    let target = Database::memory("target");
    let (mut events, handle) = source.replicate_to_live(
        &target,
        ReplicationOptions {
            live: true,
            poll_interval: Duration::from_millis(50),
            ..Default::default()
        },
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(event) = events.recv().await {
            if matches!(event, ReplicationEvent::Paused) {
                return;
            }
        }
    })
    .await
    .expect("the first pass ends");
    assert!(target.get("first").await.is_ok());

    // Idle for a second: a poll every 50 ms would be ~20 requests.
    changes_requests.store(0, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let idle = changes_requests.load(Ordering::SeqCst);
    assert!(idle <= 2, "{idle} _changes requests while idle");

    // A change wakes the wait up at once.
    put(&app, "/db/second", json!({})).await;
    let arrived = tokio::time::timeout(Duration::from_secs(5), async {
        while target.get("second").await.is_err() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    handle.cancel();
    assert!(arrived.is_ok(), "the change was not replicated");
}

#[tokio::test]
async fn a_busy_http_source_is_read_at_most_once_per_poll_interval() {
    let server_db = Arc::new(Database::memory(DB));
    let app = app_with(server_db.clone(), &config());
    let changes_requests = Arc::new(AtomicUsize::new(0));
    let counted = {
        let count = changes_requests.clone();
        app.clone().layer(axum::middleware::from_fn(
            move |req: axum::extract::Request, next: axum::middleware::Next| {
                if req.uri().path().ends_with("/_changes") {
                    count.fetch_add(1, Ordering::SeqCst);
                }
                next.run(req)
            },
        ))
    };
    let addr = serve(counted).await;
    put(&app, "/db/first", json!({})).await;

    let source = Database::http(&format!("http://{addr}/{DB}"));
    let target = Database::memory("target");
    let (mut events, handle) = source.replicate_to_live(
        &target,
        ReplicationOptions {
            live: true,
            poll_interval: Duration::from_millis(200),
            ..Default::default()
        },
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(event) = events.recv().await {
            if matches!(event, ReplicationEvent::Paused) {
                return;
            }
        }
    })
    .await
    .expect("the first pass ends");

    // A write every 10 ms for a second.
    changes_requests.store(0, Ordering::SeqCst);
    let writer = {
        let app = app.clone();
        tokio::spawn(async move {
            for i in 0..100 {
                put(&app, &format!("/db/busy-{i}"), json!({})).await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };
    let mut paused = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while let Ok(Some(event)) = tokio::time::timeout_at(deadline, events.recv()).await {
        paused += usize::from(matches!(event, ReplicationEvent::Paused));
    }
    let requests = changes_requests.load(Ordering::SeqCst);
    writer.await.unwrap();
    handle.cancel();
    // At most one pass per 200 ms (each reads the feed and waits once).
    assert!(requests <= 15, "{requests} _changes requests in 1 s");
    assert!(paused <= 6, "{paused} Paused events in 1 s");
}
