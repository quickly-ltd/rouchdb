use rouchdb::{Database, AuthClient, ReplicationOptions, ReplicationEvent, FindOptions};
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

#[tokio::test]
#[ignore]
async fn test_live_sync_remote() {
    let db_url = "https://couchdb.quickly.host/rouchdb-stress-test".to_string();
    let host_url = "https://couchdb.quickly.host";
    
    let auth = AuthClient::new(host_url);
    auth.login("admin", "123321Co").await.expect("Failed to login");

    let remote = Arc::new(Database::http_with_auth(&db_url, &auth));
    let local = Arc::new(Database::memory("live_sync_local"));

    // Ensure remote has a doc to start with, or we just write a fresh one later.
    let doc_id = format!("live_sync_test_{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis());

    // Start live replication from remote to local
    let mut options = ReplicationOptions::default();
    options.live = true;
    options.retry = true;
    options.poll_interval = Duration::from_secs(1);

    let (mut rx, mut handle) = remote.replicate_to_live(&local, options);

    // Monitor events in background
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            match event {
                ReplicationEvent::Change { docs_read, .. } => println!("Live Sync: read {} docs", docs_read),
                ReplicationEvent::Paused => println!("Live Sync: Paused (Up to date)"),
                ReplicationEvent::Error(err) => println!("Live Sync Error: {}", err),
                _ => {}
            }
        }
    });

    // Wait a bit for live sync to initialize and go into Paused state
    sleep(Duration::from_secs(3)).await;

    // Now, write a document to the REMOTE database
    println!("Writing document to remote: {}", doc_id);
    remote.put(&doc_id, json!({"live": true, "message": "hello from remote"})).await.expect("Failed to put on remote");

    // Wait for the live sync to pick it up and write to LOCAL
    println!("Waiting for live sync to pull the document...");
    let mut found = false;
    for _ in 0..10 {
        sleep(Duration::from_secs(1)).await;
        if let Ok(doc) = local.get(&doc_id).await {
            println!("Success! Found document in local DB: {:?}", doc.data);
            found = true;
            break;
        }
    }

    handle.cancel();

    assert!(found, "Live sync failed to replicate the document in time");
    println!("Live sync test completed successfully.");
}
