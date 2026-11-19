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
    /// `push_failures + pull_failures` of the running tasks.
    pub doc_write_failures: u64,
    /// Documents the far side refused to write in the running push task.
    pub push_failures: u64,
    /// Documents the local side failed to write in the running pull task.
    pub pull_failures: u64,
    /// Documents the far side denied (push direction), including those of
    /// earlier push tasks to the same label that have since ended, so the
    /// count survives an embedder tearing down and restarting replication
    /// (e.g. on every credential refresh). A denial advances the checkpoint,
    /// so the document is not retried on its own; the count is kept in
    /// memory for the life of the process only. Pull failures are local
    /// write errors and are not counted here.
    pub docs_denied: u64,
    /// When a push last wrote at least one document to the far side (UTC,
    /// ISO 8601 with milliseconds, as JavaScript's `toISOString()`); `None`
    /// if nothing was pushed yet. Kept across replication restarts for the
    /// life of the process.
    pub last_push_at: Option<String>,
    /// Local changes not yet pushed: the source's `update_seq` minus the
    /// source sequence the push had processed when it last caught up
    /// ([`ReplicationEvent::CaughtUp`]), so a write landing while the push
    /// catches up still counts as pending. Only
    /// [`ActiveTasks::sync_status_with_pending`] fills it, and only once a
    /// push has caught up at least once.
    pub pending_changes: Option<u64>,
}

/// Push progress kept per target label for the life of the process, so it
/// survives a replication restart.
#[derive(Debug, Clone, Default)]
struct PushProgress {
    last_push_ms: Option<u64>,
    /// The source sequence the push had processed when it last caught up
    /// ([`ReplicationEvent::CaughtUp`]).
    caught_up_seq: Option<u64>,
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Epoch milliseconds as `YYYY-MM-DDTHH:MM:SS.mmmZ` (UTC), the format of
/// JavaScript's `Date.prototype.toISOString()`.
pub fn iso_utc_ms(ms: u64) -> String {
    let secs = ms / 1000;
    let (days, rem) = (secs / 86_400, secs % 86_400);
    // Howard Hinnant's civil_from_days (days since 1970-01-01).
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60,
        ms % 1000
    )
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
#[derive(Default)]
struct Denied {
    /// Running tasks: task id → (target label, count).
    live: HashMap<u64, (String, u64)>,
    /// Tasks that have ended, by target label, so
    /// [`SyncStatusSummary::docs_denied`] survives a replication restart.
    retired: HashMap<String, u64>,
}

impl Denied {
    fn total_for(&self, remote_label: &str) -> u64 {
        let live: u64 = self
            .live
            .values()
            .filter(|(target, _)| target == remote_label)
            .map(|(_, denied)| denied)
            .sum();
        live + self.retired.get(remote_label).copied().unwrap_or(0)
    }

    /// Moves an ended task's count to its target label.
    fn retire(&mut self, task_id: u64) {
        if let Some((target, denied)) = self.live.remove(&task_id) {
            *self.retired.entry(target).or_insert(0) += denied;
        }
    }
}

#[derive(Clone, Default)]
pub struct ActiveTasks {
    inner: Arc<RwLock<HashMap<u64, ActiveTaskInfo>>>,
    /// Denied documents, under one lock so a task's count moving from
    /// `live` to `retired` is never seen half-way.
    denied: Arc<RwLock<Denied>>,
    /// Push progress by target label, kept after the task ends.
    push_progress: Arc<RwLock<HashMap<String, PushProgress>>>,
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
            .filter(|t| {
                t.task_type == "replication"
                    && (t.source == remote_label || t.target == remote_label)
            })
            .collect();

        if tasks.is_empty() {
            return SyncStatusSummary {
                state: "offline",
                remote_configured: false,
                last_error: None,
                docs_pushed: 0,
                docs_pulled: 0,
                doc_write_failures: 0,
                push_failures: 0,
                pull_failures: 0,
                docs_denied: self.denied_for(remote_label),
                last_push_at: self.last_push_at_for(remote_label),
                pending_changes: None,
            };
        }

        let push_task = tasks.iter().find(|t| t.target == remote_label);
        let pull_task = tasks.iter().find(|t| t.source == remote_label);
        let docs_pushed = push_task.map(|t| t.docs_written).unwrap_or(0);
        let docs_pulled = pull_task.map(|t| t.docs_written).unwrap_or(0);
        let push_failures = push_task.map(|t| t.doc_write_failures).unwrap_or(0);
        let pull_failures = pull_task.map(|t| t.doc_write_failures).unwrap_or(0);

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
            doc_write_failures: push_failures + pull_failures,
            push_failures,
            pull_failures,
            docs_denied: self.denied_for(remote_label),
            last_push_at: self.last_push_at_for(remote_label),
            pending_changes: None,
        }
    }

    /// [`Self::sync_status_summary`] plus
    /// [`SyncStatusSummary::pending_changes`], measured against `local`, the
    /// source of the push to `remote_label`. `pending_changes` stays `None`
    /// if that push never caught up or `local`'s info cannot be read.
    pub async fn sync_status_with_pending(
        &self,
        remote_label: &str,
        local: &Database,
    ) -> SyncStatusSummary {
        let mut summary = self.sync_status_summary(remote_label);
        let caught_up = self
            .push_progress
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(remote_label)
            .and_then(|p| p.caught_up_seq);
        if let (Some(caught_up), Ok(info)) = (caught_up, local.info().await) {
            summary.pending_changes = Some(info.update_seq.as_num().saturating_sub(caught_up));
        }
        summary
    }

    fn last_push_at_for(&self, remote_label: &str) -> Option<String> {
        self.push_progress
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(remote_label)
            .and_then(|p| p.last_push_ms)
            .map(iso_utc_ms)
    }

    fn denied_for(&self, remote_label: &str) -> u64 {
        self.denied
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .total_for(remote_label)
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
        let denied_docs = self.denied.clone();
        let push_progress = self.push_progress.clone();
        tokio::spawn(async move {
            let mut previous = (0u64, 0u64, 0u64, 0u64);
            while let Some(event) = rx.recv().await {
                {
                    let mut tasks = registry.write().unwrap_or_else(|e| e.into_inner());
                    let Some(entry) = tasks.get_mut(&task_id) else {
                        break;
                    };
                    match event {
                        ReplicationEvent::Change {
                            docs_read,
                            docs_written,
                            doc_write_failures,
                            docs_denied,
                            ..
                        } => {
                            let written = docs_written.saturating_sub(previous.1);
                            entry.docs_read += docs_read.saturating_sub(previous.0);
                            entry.docs_written += written;
                            entry.doc_write_failures +=
                                doc_write_failures.saturating_sub(previous.2);
                            let denied = docs_denied.saturating_sub(previous.3);
                            previous = (docs_read, docs_written, doc_write_failures, docs_denied);
                            if denied > 0 {
                                denied_docs
                                    .write()
                                    .unwrap_or_else(|e| e.into_inner())
                                    .live
                                    .entry(task_id)
                                    .or_insert_with(|| (entry.target.clone(), 0))
                                    .1 += denied;
                            }
                            entry.status = "syncing";
                            entry.updated_on = now_unix();
                            if written > 0 {
                                push_progress
                                    .write()
                                    .unwrap_or_else(|e| e.into_inner())
                                    .entry(entry.target.clone())
                                    .or_default()
                                    .last_push_ms = Some(now_unix_ms());
                            }
                        }
                        ReplicationEvent::Active => {
                            previous = (0, 0, 0, 0);
                            entry.status = "syncing";
                            entry.updated_on = now_unix();
                        }
                        // Every source change up to `last_seq` was
                        // processed (written, denied or filtered out).
                        ReplicationEvent::CaughtUp { last_seq, .. } => {
                            push_progress
                                .write()
                                .unwrap_or_else(|e| e.into_inner())
                                .entry(entry.target.clone())
                                .or_default()
                                .caught_up_seq = Some(last_seq.as_num());
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
                        // The entry is removed once, below, so its failures
                        // are retired exactly once.
                        ReplicationEvent::Complete(_) => break,
                        _ => {}
                    }
                }
            }
            registry
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&task_id);
            denied_docs
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .retire(task_id);
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
                doc_write_failures: 0,
                push_failures: 0,
                pull_failures: 0,
                docs_denied: 0,
                last_push_at: None,
                pending_changes: None,
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
        let mut push_task = task("alldata", "remote", "idle", 5);
        push_task.doc_write_failures = 2;
        insert_task(&active_tasks, 1, push_task);
        let mut pull_task = task("remote", "alldata", "syncing", 3);
        pull_task.doc_write_failures = 1;
        insert_task(&active_tasks, 2, pull_task);
        let mut unrelated_task = task("alldata", "other-remote", "idle", 99);
        unrelated_task.doc_write_failures = 42;
        insert_task(&active_tasks, 3, unrelated_task);

        let summary = active_tasks.sync_status_summary("remote");
        assert_eq!(summary.state, "syncing");
        assert!(summary.remote_configured);
        assert_eq!(summary.docs_pushed, 5);
        assert_eq!(summary.docs_pulled, 3);
        assert_eq!(summary.push_failures, 2);
        assert_eq!(summary.pull_failures, 1);
        assert_eq!(summary.doc_write_failures, 3);
        // Write failures are not denials: only a `forbidden`/`unauthorized`
        // reply of the far side is (see the live-replication tests below).
        assert_eq!(summary.docs_denied, 0);
        assert_eq!(summary.last_error, None);
    }

    /// Denies replicated documents carrying `"bad": true`, like a CouchDB
    /// `validate_doc_update` throwing `forbidden`.
    struct DenyBad;

    #[async_trait::async_trait]
    impl rouchdb::Plugin for DenyBad {
        fn name(&self) -> &str {
            "deny-bad"
        }
        async fn before_write(&self, docs: &mut Vec<rouchdb::Document>) -> rouchdb::Result<()> {
            if docs
                .iter()
                .any(|d| d.data["bad"] == serde_json::json!(true))
            {
                return Err(rouchdb::RouchError::Forbidden("no bad docs".into()));
            }
            Ok(())
        }
    }

    fn fast_opts() -> ReplicationOptions {
        ReplicationOptions {
            poll_interval: Duration::from_millis(20),
            ..Default::default()
        }
    }

    async fn wait_for_summary(
        active_tasks: &ActiveTasks,
        what: &str,
        done: impl Fn(&SyncStatusSummary) -> bool,
    ) -> SyncStatusSummary {
        timeout(Duration::from_secs(5), async {
            loop {
                let s = active_tasks.sync_status_summary("remote");
                if done(&s) {
                    break s;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what}"))
    }

    /// A document the far side denies during a real live replication shows
    /// up as `docs_denied` (JSON `docsDenied`) and stays counted once the
    /// task is idle again, while the valid document is written.
    #[tokio::test]
    async fn sync_status_summary_reports_denied_docs_from_real_replication() {
        let local = Database::memory("alldata");
        let remote = Database::memory("remote-db").with_plugin(Arc::new(DenyBad));
        local
            .put("denied", serde_json::json!({"bad": true}))
            .await
            .unwrap();
        local.put("ok", serde_json::json!({"v": 1})).await.unwrap();

        let active_tasks = ActiveTasks::new();
        let handle =
            active_tasks.start_live_replication(&local, &remote, "alldata", "remote", fast_opts());

        let summary = wait_for_summary(&active_tasks, "denial should be reported", |s| {
            s.docs_denied > 0 && s.state == "idle"
        })
        .await;
        assert_eq!(summary.docs_denied, 1);
        assert_eq!(summary.docs_pushed, 1, "the valid doc is written");
        assert!(remote.get("ok").await.is_ok());
        assert!(remote.get("denied").await.is_err());
        let json = serde_json::to_value(&summary).unwrap();
        assert_eq!(json["docsDenied"], 1);

        handle.cancel();
    }

    /// Restarting the replication (the old task ends, a new one starts for
    /// the same pair) must not reset `docs_denied`: the denied document is
    /// not retried, so the new task alone would report zero.
    #[tokio::test]
    async fn docs_denied_survives_replication_restart() {
        let local = Database::memory("alldata");
        let remote = Database::memory("remote-db").with_plugin(Arc::new(DenyBad));
        local
            .put("denied", serde_json::json!({"bad": true}))
            .await
            .unwrap();

        let active_tasks = ActiveTasks::new();
        let first =
            active_tasks.start_live_replication(&local, &remote, "alldata", "remote", fast_opts());
        wait_for_summary(&active_tasks, "denial should be reported", |s| {
            s.docs_denied > 0
        })
        .await;

        first.cancel();
        timeout(Duration::from_secs(5), async {
            while !active_tasks.snapshot().is_empty() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the old task should be removed");
        assert_eq!(active_tasks.sync_status_summary("remote").docs_denied, 1);

        let second =
            active_tasks.start_live_replication(&local, &remote, "alldata", "remote", fast_opts());
        let summary = wait_for_summary(&active_tasks, "the new task should settle", |s| {
            s.state == "idle"
        })
        .await;
        assert_eq!(summary.docs_denied, 1, "kept across the restart");
        assert_eq!(summary.push_failures, 0, "the denied doc is not retried");
        second.cancel();
    }

    /// Retiring a task moves its count in one step: the total for its
    /// label is the same before and after, and other labels are untouched.
    #[test]
    fn retiring_a_task_keeps_the_denied_total() {
        let mut denied = Denied::default();
        denied.live.insert(1, ("remote".into(), 2));
        denied.live.insert(2, ("remote".into(), 1));
        denied.live.insert(3, ("other".into(), 5));
        denied.retired.insert("remote".into(), 4);
        assert_eq!(denied.total_for("remote"), 7);
        denied.retire(1);
        assert_eq!(denied.total_for("remote"), 7);
        assert_eq!(denied.retired["remote"], 6);
        denied.retire(1);
        assert_eq!(denied.total_for("remote"), 7, "retired once");
        assert_eq!(denied.total_for("other"), 5);
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
    #[test]
    fn audit_regression_summary_isolates_remote_connections() {
        let tasks = ActiveTasks::new();
        insert_task(&tasks, 1, task("local", "remote-a", "idle", 5));
        let mut unrelated = task("remote-b", "local", "error", 9);
        unrelated.last_error = Some("remote-b unavailable".into());
        insert_task(&tasks, 2, unrelated);
        insert_task(&tasks, 3, task("remote-a", "local", "idle", 3));
        let summary = tasks.sync_status_summary("remote-a");
        assert_eq!(summary.state, "idle");
        assert_eq!(summary.last_error, None);
        assert_eq!((summary.docs_pushed, summary.docs_pulled), (5, 3));
        assert!(summary.remote_configured);
        assert_eq!(
            tasks.sync_status_summary("missing"),
            SyncStatusSummary {
                state: "offline",
                remote_configured: false,
                last_error: None,
                docs_pushed: 0,
                docs_pulled: 0,
                doc_write_failures: 0,
                push_failures: 0,
                pull_failures: 0,
                docs_denied: 0,
                last_push_at: None,
                pending_changes: None,
            }
        );
    }

    #[test]
    fn iso_utc_ms_matches_javascript_to_iso_string() {
        assert_eq!(iso_utc_ms(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso_utc_ms(1_791_763_200_123), "2026-10-12T00:00:00.123Z");
        assert_eq!(iso_utc_ms(1_709_211_661_005), "2024-02-29T13:01:01.005Z");
    }

    /// A real replication reports the last push and the pending changes;
    /// once it stops (far side unreachable) new local writes count as
    /// pending and the last push time is kept.
    #[tokio::test]
    async fn sync_status_reports_last_push_and_pending_changes_from_real_replication() {
        let local = Database::memory("alldata");
        let remote = Database::memory("remote-db");
        local.put("a", serde_json::json!({"v": 1})).await.unwrap();
        local.put("b", serde_json::json!({"v": 2})).await.unwrap();

        let active_tasks = ActiveTasks::new();
        let before = active_tasks
            .sync_status_with_pending("remote", &local)
            .await;
        assert_eq!(before.last_push_at, None);
        assert_eq!(before.pending_changes, None, "unknown until caught up");

        let handle =
            active_tasks.start_live_replication(&local, &remote, "alldata", "remote", fast_opts());
        let caught_up = timeout(Duration::from_secs(5), async {
            loop {
                let s = active_tasks
                    .sync_status_with_pending("remote", &local)
                    .await;
                if s.state == "idle" && s.pending_changes == Some(0) && s.last_push_at.is_some() {
                    break s;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the push should catch up");
        assert_eq!(caught_up.docs_pushed, 2);
        let json = serde_json::to_value(&caught_up).unwrap();
        assert_eq!(json["pendingChanges"], 0);
        assert!(json["lastPushAt"].as_str().unwrap().ends_with('Z'));

        handle.cancel();
        timeout(Duration::from_secs(5), async {
            while !active_tasks.snapshot().is_empty() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the task should be removed");
        for id in ["c", "d", "e"] {
            local.put(id, serde_json::json!({"v": 3})).await.unwrap();
        }
        let offline = active_tasks
            .sync_status_with_pending("remote", &local)
            .await;
        assert_eq!(offline.state, "offline");
        assert_eq!(offline.pending_changes, Some(3));
        assert_eq!(
            offline.last_push_at, caught_up.last_push_at,
            "the last push time is kept"
        );
    }
}
