//! Live replication integration test against a remote CouchDB instance.
//!
//! Requires a running CouchDB instance (uses `COUCHDB_URL` or defaults to
//! `http://admin:password@localhost:15984` via `common`):
//!   docker compose up -d
//!   cargo test -p rouchdb --test live_sync_test -- --ignored

mod common;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{delete_remote_db, fresh_remote_db};
use rouchdb::{AuthClient, Database, ReplicationEvent, ReplicationOptions};
use serde_json::json;
use tokio::time::sleep;

#[tokio::test]
#[ignore]
async fn test_live_sync_remote() {
    let raw_db_url = fresh_remote_db("live_sync").await;

    let mut parsed = reqwest::Url::parse(&raw_db_url).expect("valid COUCHDB_URL");
    let username = parsed.username().to_string();
    let password = parsed.password().unwrap_or_default().to_string();
    let _ = parsed.set_username("");
    let _ = parsed.set_password(None);

    let db_url = parsed.to_string();
    let mut server_parsed = parsed;
    if let Ok(mut segments) = server_parsed.path_segments_mut() {
        segments.pop_if_empty().pop();
    }
    let host_url = server_parsed.as_str().trim_end_matches('/').to_string();

    let remote = if !username.is_empty() {
        let auth = AuthClient::new(&host_url);
        auth.login(&username, &password)
            .await
            .expect("Failed to login");
        Arc::new(Database::http_with_auth(&db_url, &auth))
    } else {
        Arc::new(Database::http(&db_url))
    };
    let local = Arc::new(Database::memory("live_sync_local"));

    let doc_id = format!(
        "live_sync_test_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Start live replication from remote to local
    let options = ReplicationOptions {
        live: true,
        retry: true,
        poll_interval: Duration::from_millis(200),
        ..Default::default()
    };

    let (mut rx, handle) = remote.replicate_to_live(&local, options);

    // Monitor events in background
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            match event {
                ReplicationEvent::Change { docs_read, .. } => {
                    println!("Live Sync: read {docs_read} docs");
                }
                ReplicationEvent::Paused => println!("Live Sync: Paused (Up to date)"),
                ReplicationEvent::Error(err) => println!("Live Sync Error: {err}"),
                _ => {}
            }
        }
    });

    // Wait briefly for live sync to initialize
    sleep(Duration::from_millis(500)).await;

    // Write a document to the remote database
    remote
        .put(
            &doc_id,
            json!({"live": true, "message": "hello from remote"}),
        )
        .await
        .expect("Failed to put on remote");

    // Wait for live sync to pull the document into the local database
    let mut found = false;
    for _ in 0..25 {
        sleep(Duration::from_millis(200)).await;
        if let Ok(doc) = local.get(&doc_id).await {
            assert_eq!(doc.data["live"], true);
            found = true;
            break;
        }
    }

    handle.cancel();
    delete_remote_db(&raw_db_url).await;

    assert!(found, "Live sync failed to replicate the document in time");
}
