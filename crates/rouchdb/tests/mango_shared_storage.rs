//! Mango indexes of a handle whose storage another handle reset.
//!
//! Every `Database` keeps its own index cache. When a second handle on the
//! same adapter calls `destroy()` and new documents are written, the first
//! handle's indexed `find` must still answer exactly like a full scan,
//! whether the new database's sequence ends below, at or above the one the
//! index had reached.

use std::sync::Arc;

use rouchdb::{Adapter, Database, FindOptions, IndexDefinition, MemoryAdapter, SortField};
use rouchdb_adapter_redb::RedbAdapter;
use serde_json::json;

fn by_x() -> IndexDefinition {
    IndexDefinition {
        name: "by_x".into(),
        fields: vec![SortField::Simple("x".into())],
        ddoc: None,
    }
}

async fn ids(db: &Database, selector: serde_json::Value) -> Vec<String> {
    let opts = FindOptions {
        selector,
        ..Default::default()
    };
    let mut ids: Vec<String> = db
        .find(opts)
        .await
        .unwrap()
        .docs
        .iter()
        .map(|d| d["_id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    ids
}

/// Index on `a`, reset and re-fill through `b`, and compare `a`'s indexed
/// result with `b`'s full scan (`b` has no index).
async fn check(adapter: Arc<dyn Adapter>, before: usize, after: usize) {
    let a = Database::from_adapter(adapter.clone());
    let b = Database::from_adapter(adapter);
    for i in 0..before {
        a.put(&format!("old{i}"), json!({"x": 1})).await.unwrap();
    }
    // `a`'s index reaches the old database's last sequence.
    a.create_index(by_x()).await.unwrap();
    assert_eq!(ids(&a, json!({"x": 1})).await.len(), before);

    b.destroy().await.unwrap();
    for i in 0..after {
        // The same id as before, with other content, must not keep its old entry.
        let x = if i == 0 { 2 } else { 1 };
        b.put(&format!("old{i}"), json!({"x": x})).await.unwrap();
    }

    for selector in [json!({"x": 1}), json!({"x": 2}), json!({"x": {"$gt": 0}})] {
        let expected = ids(&b, selector.clone()).await;
        assert_eq!(
            ids(&a, selector.clone()).await,
            expected,
            "before {before}, after {after}, selector {selector}"
        );
    }
}

#[tokio::test]
async fn a_reset_through_another_handle_rebuilds_the_index() {
    for (before, after) in [(3, 1), (2, 2), (1, 3)] {
        check(Arc::new(MemoryAdapter::new("shared")), before, after).await;

        let dir = tempfile::tempdir().unwrap();
        let redb = RedbAdapter::open(dir.path().join("shared.redb"), "shared").unwrap();
        check(Arc::new(redb), before, after).await;
    }
}
