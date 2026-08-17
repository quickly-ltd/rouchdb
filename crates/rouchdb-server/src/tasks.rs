//! Live replication task tracking, backing `GET /_active_tasks`.
//!
//! CouchDB clients (PouchDB, Fauxton, ad-hoc `curl`/monitoring scripts) all
//! observe sync progress the same standard way: polling `_active_tasks` for
//! entries with `"type": "replication"`
//! (<https://docs.couchdb.org/en/stable/api/server/common.html#active-tasks>).
//! `ActiveTasks` is the shared registry that makes RouchDB's `_active_tasks`
//! endpoint reflect real replication state instead of an empty stub — any
//! caller that starts a live replication through [`ActiveTasks::start_live_replication`]
//! gets this for free, on every platform that embeds `rouchdb-server`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use rouchdb::{Database, ReplicationEvent, ReplicationHandle, ReplicationOptions};

/// One running replication task, shaped to match CouchDB's `_active_tasks`
/// schema for `"type": "replication"` entries, plus two extra fields
/// (`status`, `last_error`) that aren't part of that schema but are harmless
/// to any standard CouchDB client — they give a client-side sync-status UI
/// the same coarse states PouchDB's `.sync()` events provide, without it
/// having to infer them from raw doc counts.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ActiveTaskInfo {
    #[serde(rename = "type")]
    pub task_type: &'static str,
    pub pid: String,
    pub source: String,
    pub target: String,
    pub continuous: bool,
    pub docs_read: u64,
    pub docs_written: u64,
    pub doc_write_failures: u64,
    pub started_on: u64,
    pub updated_on: u64,
    /// "connecting" (no event yet) | "syncing" (active transfer) |
    /// "idle" (caught up, waiting) | "error" (last event was an error).
    pub status: &'static str,
    pub last_error: Option<String>,
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);

/// Shared registry of running replication tasks.
///
/// Cheap to clone (internally `Arc`'d) — hand one clone to
/// [`crate::build_router`] (so `GET /_active_tasks` can read it) and keep
/// another wherever replication is started, so both sides observe the same
/// tasks.
#[derive(Clone, Default)]
pub struct ActiveTasks {
    inner: Arc<RwLock<HashMap<u64, ActiveTaskInfo>>>,
}

impl ActiveTasks {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of all currently-running tasks, in `_active_tasks` order
    /// (insertion order is not guaranteed; CouchDB doesn't guarantee an
    /// order either).
    pub fn snapshot(&self) -> Vec<ActiveTaskInfo> {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }

    /// Starts a live (continuous) replication and keeps this registry
    /// updated with its progress for as long as it runs. The task is
    /// removed from the registry when replication ends (cancelled, or
    /// stops retrying after an error) — matching CouchDB, where a finished
    /// task simply disappears from `_active_tasks`.
    ///
    /// `source_name`/`target_name` are opaque labels for the `source`/
    /// `target` fields (e.g. a local db name or a redacted remote host) —
    /// they are not used for anything but display.
    pub fn start_live_replication(
        &self,
        source: &Database,
        target: &Database,
        source_name: impl Into<String>,
        target_name: impl Into<String>,
        opts: ReplicationOptions,
    ) -> ReplicationHandle {
        let (mut rx, handle) = source.replicate_to_live(target, opts);

        let task_id = NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed);
        let started = now_unix();
        let info = ActiveTaskInfo {
            task_type: "replication",
            pid: format!("rouchdb-{task_id}"),
            source: source_name.into(),
            target: target_name.into(),
            continuous: true,
            docs_read: 0,
            docs_written: 0,
            doc_write_failures: 0,
            started_on: started,
            updated_on: started,
            status: "connecting",
            last_error: None,
        };

        // Register synchronously (std::sync::RwLock, no .await needed) so
        // the task is visible even if a poll of _active_tasks lands before
        // the forwarding loop below processes its first event.
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(task_id, info);

        let registry = self.inner.clone();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let mut tasks = registry.write().unwrap_or_else(|e| e.into_inner());
                let Some(entry) = tasks.get_mut(&task_id) else {
                    break;
                };
                match event {
                    ReplicationEvent::Change {
                        docs_read,
                        docs_written,
                        doc_write_failures,
                    } => {
                        entry.docs_read += docs_read;
                        entry.docs_written += docs_written;
                        entry.doc_write_failures += doc_write_failures;
                        entry.status = "syncing";
                        entry.updated_on = now_unix();
                    }
                    ReplicationEvent::Active => {
                        entry.status = "syncing";
                        entry.updated_on = now_unix();
                    }
                    ReplicationEvent::Paused => {
                        entry.status = "idle";
                        entry.last_error = None;
                        entry.updated_on = now_unix();
                    }
                    ReplicationEvent::Error(message) => {
                        entry.status = "error";
                        entry.last_error = Some(message);
                        entry.updated_on = now_unix();
                    }
                    ReplicationEvent::Complete(_) => {
                        tasks.remove(&task_id);
                        break;
                    }
                }
            }
            registry
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&task_id);
        });

        handle
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, sleep, timeout};

    /// A task started via `start_live_replication` should show up in
    /// `_active_tasks`-shaped snapshots with real, growing counts as docs
    /// flow, and disappear once the replication is cancelled — this is the
    /// exact behavior the frontend's sync-status UI depends on.
    #[tokio::test]
    async fn tracks_live_replication_progress_and_cleans_up_on_cancel() {
        let source = Database::memory("src");
        let target = Database::memory("tgt");
        source
            .post(serde_json::json!({"hello": "world"}))
            .await
            .unwrap();

        let active_tasks = ActiveTasks::new();
        let handle = active_tasks.start_live_replication(
            &source,
            &target,
            "src",
            "tgt",
            ReplicationOptions {
                poll_interval: Duration::from_millis(20),
                ..Default::default()
            },
        );

        // The task must be visible immediately (synchronous registration),
        // before any replication event has actually arrived.
        let immediate = active_tasks.snapshot();
        assert_eq!(immediate.len(), 1);
        assert_eq!(immediate[0].task_type, "replication");
        assert_eq!(immediate[0].source, "src");
        assert_eq!(immediate[0].target, "tgt");
        assert!(immediate[0].continuous);

        // Wait for the initial doc to actually replicate.
        let synced = timeout(Duration::from_secs(5), async {
            loop {
                let snap = active_tasks.snapshot();
                if snap.first().is_some_and(|t| t.docs_written > 0) {
                    break snap;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("replication should progress within 5s");
        assert_eq!(synced[0].docs_read, 1);
        assert_eq!(synced[0].docs_written, 1);
        assert_eq!(synced[0].doc_write_failures, 0);
        assert_eq!(synced[0].status, "syncing");
        assert!(synced[0].updated_on >= synced[0].started_on);

        let all = target.all_docs(Default::default()).await.unwrap();
        assert_eq!(all.rows.len(), 1);

        // Once caught up (no more changes), the next poll pass should
        // report "idle" — mirroring PouchDB's `.on('paused')` with no error.
        timeout(Duration::from_secs(5), async {
            loop {
                if active_tasks
                    .snapshot()
                    .first()
                    .is_some_and(|t| t.status == "idle")
                {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("task should settle into idle once caught up");

        handle.cancel();
        timeout(Duration::from_secs(5), async {
            loop {
                if active_tasks.snapshot().is_empty() {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("task should be removed from the registry after cancel");
    }
}
