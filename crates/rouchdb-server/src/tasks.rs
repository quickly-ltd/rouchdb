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

/// Legacy `/sync-status`-shaped summary derived from [`ActiveTasks`] — see
/// [`ActiveTasks::sync_status_summary`].
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncStatusSummary {
    pub state: &'static str,
    pub remote_configured: bool,
    pub last_error: Option<String>,
    pub docs_pushed: u64,
    pub docs_pulled: u64,
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
/// [`crate::build_router_with_tasks`] (so `GET /_active_tasks` can read it) and keep
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

    /// Summarizes replication tasks into the shape a legacy express-pouchdb
    /// sidecar's bespoke `/sync-status` endpoint used to return, so an
    /// embedder that needs to serve that contract (e.g. for clients that
    /// haven't switched to reading `_active_tasks` directly) can derive it
    /// here instead of re-implementing the mapping client-side.
    ///
    /// `remote_label` is the opaque source/target name marking the far side
    /// of the sync pair — whatever label the caller passed as `source_name`/
    /// `target_name` to [`ActiveTasks::start_live_replication`].
    pub fn sync_status_summary(&self, remote_label: &str) -> SyncStatusSummary {
        let tasks: Vec<ActiveTaskInfo> = self
            .snapshot()
            .into_iter()
            .filter(|t| t.task_type == "replication")
            .collect();

        if tasks.is_empty() {
            return SyncStatusSummary {
                state: "offline",
                remote_configured: false,
                last_error: None,
                docs_pushed: 0,
                docs_pulled: 0,
            };
        }

        let docs_pushed = tasks
            .iter()
            .find(|t| t.target == remote_label)
            .map(|t| t.docs_written)
            .unwrap_or(0);
        let docs_pulled = tasks
            .iter()
            .find(|t| t.source == remote_label)
            .map(|t| t.docs_written)
            .unwrap_or(0);

        // Worst-first: an error or an unconnected task anywhere in the pair
        // outweighs another task quietly being idle.
        fn priority(status: &str) -> u8 {
            match status {
                "error" => 0,
                "connecting" => 1,
                "syncing" => 2,
                _ => 3, // "idle"
            }
        }

        let mut state = "idle";
        let mut last_error = None;
        for t in &tasks {
            if priority(t.status) < priority(state) {
                state = t.status;
            }
            if let Some(ref e) = t.last_error {
                last_error = Some(e.clone());
            }
        }

        SyncStatusSummary {
            state,
            remote_configured: true,
            last_error,
            docs_pushed,
            docs_pulled,
        }
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
            let mut previous = (0u64, 0u64, 0u64);
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
                        ..
                    } => {
                        entry.docs_read += docs_read.saturating_sub(previous.0);
                        entry.docs_written += docs_written.saturating_sub(previous.1);
                        entry.doc_write_failures += doc_write_failures.saturating_sub(previous.2);
                        previous = (docs_read, docs_written, doc_write_failures);
                        entry.status = "syncing";
                        entry.updated_on = now_unix();
                    }
                    ReplicationEvent::Active => {
                        previous = (0, 0, 0);
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
                    _ => {}
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

    fn insert_task(active_tasks: &ActiveTasks, id: u64, info: ActiveTaskInfo) {
        active_tasks
            .inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, info);
    }

    fn task(source: &str, target: &str, status: &'static str, docs_written: u64) -> ActiveTaskInfo {
        ActiveTaskInfo {
            task_type: "replication",
            pid: "rouchdb-test".into(),
            source: source.into(),
            target: target.into(),
            continuous: true,
            docs_read: docs_written,
            docs_written,
            doc_write_failures: 0,
            started_on: 0,
            updated_on: 0,
            status,
            last_error: None,
        }
    }

    #[test]
    fn sync_status_summary_offline_when_no_tasks() {
        let active_tasks = ActiveTasks::new();
        let summary = active_tasks.sync_status_summary("remote");
        assert_eq!(
            summary,
            SyncStatusSummary {
                state: "offline",
                remote_configured: false,
                last_error: None,
                docs_pushed: 0,
                docs_pulled: 0,
            }
        );
    }

    /// Mirrors `mapActiveTasksToSyncStatus` in the frontend's `http-upstream.ts`:
    /// push is the task whose `target` is the remote label, pull is the task
    /// whose `source` is the remote label, and the overall state is the
    /// *worst* one across the pair (error > connecting > syncing > idle).
    #[test]
    fn sync_status_summary_maps_push_pull_and_worst_state() {
        let active_tasks = ActiveTasks::new();
        insert_task(&active_tasks, 1, task("alldata", "remote", "idle", 5));
        insert_task(&active_tasks, 2, task("remote", "alldata", "syncing", 3));

        let summary = active_tasks.sync_status_summary("remote");
        assert_eq!(summary.state, "syncing");
        assert!(summary.remote_configured);
        assert_eq!(summary.docs_pushed, 5);
        assert_eq!(summary.docs_pulled, 3);
        assert_eq!(summary.last_error, None);
    }

    #[test]
    fn sync_status_summary_surfaces_last_error() {
        let active_tasks = ActiveTasks::new();
        let mut errored = task("alldata", "remote", "error", 0);
        errored.last_error = Some("bağlantı koptu".into());
        insert_task(&active_tasks, 1, errored);
        insert_task(&active_tasks, 2, task("remote", "alldata", "idle", 2));

        let summary = active_tasks.sync_status_summary("remote");
        assert_eq!(summary.state, "error");
        assert_eq!(summary.last_error, Some("bağlantı koptu".to_string()));
        assert_eq!(summary.docs_pulled, 2);
    }
}
