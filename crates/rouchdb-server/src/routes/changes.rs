use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::response::Response;
use bytes::Bytes;
use futures_util::stream::unfold;
use serde::Deserialize;

use rouchdb::{ChangesEvent, ChangesStreamOptions, live_changes_events};
use rouchdb_core::document::{ChangesOptions, ChangesStyle, Seq};

use crate::error::AppError;
use crate::state::AppState;

#[derive(Deserialize, Default, Clone)]
#[serde(default)]
pub struct ChangesQuery {
    pub since: Option<String>,
    pub limit: Option<u64>,
    pub descending: Option<bool>,
    pub include_docs: Option<bool>,
    pub style: Option<String>,
    pub conflicts: Option<bool>,
    pub doc_ids: Option<String>,
    pub filter: Option<String>,
    pub feed: Option<String>,
    pub timeout: Option<u64>,
    pub heartbeat: Option<u64>,
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

fn validate_db(db: &str, state: &AppState) -> Result<(), AppError> {
    if db != state.db_name {
        return Err(AppError(rouchdb_core::error::RouchError::NotFound(
            format!("Database does not exist: {db}"),
        )));
    }
    Ok(())
}

async fn resolve_since(state: &AppState, since: Option<String>) -> Result<Seq, AppError> {
    match since {
        None => Ok(Seq::from(0u64)),
        Some(s) => {
            if s == "now" {
                Ok(state.db.info().await?.update_seq)
            } else if let Ok(n) = s.parse::<u64>() {
                Ok(Seq::from(n))
            } else {
                Ok(Seq::Str(s))
            }
        }
    }
}

async fn process_changes_request(
    state: AppState,
    db: String,
    query: ChangesQuery,
    body_doc_ids: Option<Vec<String>>,
    body_selector: Option<serde_json::Value>,
) -> Result<Response, AppError> {
    validate_db(&db, &state)?;

    let feed = query.feed.as_deref().unwrap_or("normal");
    let style = match query.style.as_deref() {
        Some("all_docs") => ChangesStyle::AllDocs,
        _ => ChangesStyle::MainOnly,
    };

    let since_seq = resolve_since(&state, query.since.clone()).await?;

    let doc_ids = body_doc_ids.or_else(|| {
        query.doc_ids.as_ref().map(|s| {
            s.split(',')
                .map(|id| id.trim().to_string())
                .filter(|id| !id.is_empty())
                .collect()
        })
    });

    let opts = ChangesOptions {
        since: since_seq.clone(),
        limit: query.limit,
        descending: query.descending.unwrap_or(false),
        include_docs: query.include_docs.unwrap_or(false),
        live: false,
        doc_ids: doc_ids.clone(),
        selector: body_selector.clone(),
        conflicts: query.conflicts.unwrap_or(false),
        style: style.clone(),
    };

    match feed {
        "longpoll" => {
            let timeout_ms = query.timeout.unwrap_or(30000);
            let timeout_dur = Duration::from_millis(timeout_ms);
            let start = tokio::time::Instant::now();

            loop {
                let response = state.db.adapter().changes(opts.clone()).await?;
                if !response.results.is_empty() || start.elapsed() >= timeout_dur {
                    let json = serde_json::json!({
                        "results": response.results,
                        "last_seq": response.last_seq,
                        "pending": 0,
                    });
                    return Ok(Response::builder()
                        .header("Content-Type", "application/json")
                        .body(axum::body::Body::from(serde_json::to_string(&json).unwrap()))
                        .unwrap());
                }

                let remaining = timeout_dur.saturating_sub(start.elapsed());
                if remaining.is_zero() {
                    let json = serde_json::json!({
                        "results": response.results,
                        "last_seq": response.last_seq,
                        "pending": 0,
                    });
                    return Ok(Response::builder()
                        .header("Content-Type", "application/json")
                        .body(axum::body::Body::from(serde_json::to_string(&json).unwrap()))
                        .unwrap());
                }

                let sleep_dur = std::cmp::min(remaining, Duration::from_millis(100));
                tokio::time::sleep(sleep_dur).await;
            }
        }
        "continuous" => {
            let stream_opts = ChangesStreamOptions {
                since: since_seq,
                live: true,
                include_docs: query.include_docs.unwrap_or(false),
                doc_ids,
                selector: body_selector,
                limit: query.limit,
                conflicts: query.conflicts.unwrap_or(false),
                style,
                poll_interval: Duration::from_millis(100),
                timeout: query.timeout.map(Duration::from_millis),
                heartbeat: query.heartbeat.map(Duration::from_millis),
                ..Default::default()
            };

            let (rx, _handle) = live_changes_events(state.db.adapter_arc(), stream_opts);

            let stream = unfold(rx, |mut rx| async move {
                match rx.recv().await {
                    Some(event) => match event {
                        ChangesEvent::Change(c) => {
                            if let Ok(json) = serde_json::to_string(&c) {
                                let mut line = json;
                                line.push('\n');
                                Some((Ok::<_, std::convert::Infallible>(Bytes::from(line)), rx))
                            } else {
                                Some((Ok(Bytes::from("\n")), rx))
                            }
                        }
                        ChangesEvent::Heartbeat => Some((Ok(Bytes::from("\n")), rx)),
                        ChangesEvent::Complete { last_seq } => {
                            let line = format!(
                                "{{\"last_seq\":{}}}\n",
                                serde_json::to_string(&last_seq).unwrap_or_default()
                            );
                            Some((Ok(Bytes::from(line)), rx))
                        }
                        _ => Some((Ok(Bytes::new()), rx)),
                    },
                    None => None,
                }
            });

            Ok(Response::builder()
                .header("Content-Type", "application/json")
                .header("Cache-Control", "no-cache")
                .header("Connection", "keep-alive")
                .body(axum::body::Body::from_stream(stream))
                .unwrap())
        }
        _ => {
            let response = state.db.adapter().changes(opts).await?;
            let json = serde_json::json!({
                "results": response.results,
                "last_seq": response.last_seq,
                "pending": 0,
            });
            Ok(Response::builder()
                .header("Content-Type", "application/json")
                .body(axum::body::Body::from(serde_json::to_string(&json).unwrap()))
                .unwrap())
        }
    }
}

/// GET /{db}/_changes — get the changes feed.
pub async fn get_changes(
    State(state): State<AppState>,
    Path(db): Path<String>,
    Query(query): Query<ChangesQuery>,
) -> Result<Response, AppError> {
    process_changes_request(state, db, query, None, None).await
}

/// POST /{db}/_changes — get the changes feed with body params.
pub async fn post_changes(
    State(state): State<AppState>,
    Path(db): Path<String>,
    Query(query): Query<ChangesQuery>,
    axum::extract::Json(body): axum::extract::Json<serde_json::Value>,
) -> Result<Response, AppError> {
    let doc_ids = body.get("doc_ids").and_then(|v| v.as_array()).map(|arr| {
        arr.iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect()
    });
    let selector = body.get("selector").cloned();

    // Body params take precedence for since/limit/conflicts/style/feed if specified
    let mut q = query;
    if let Some(s) = body.get("since").and_then(|v| v.as_str()) {
        q.since = Some(s.to_string());
    } else if let Some(n) = body.get("since").and_then(|v| v.as_u64()) {
        q.since = Some(n.to_string());
    }
    if let Some(l) = body.get("limit").and_then(|v| v.as_u64()) {
        q.limit = Some(l);
    }
    if let Some(f) = body.get("feed").and_then(|v| v.as_str()) {
        q.feed = Some(f.to_string());
    }
    if let Some(t) = body.get("timeout").and_then(|v| v.as_u64()) {
        q.timeout = Some(t);
    }
    if let Some(h) = body.get("heartbeat").and_then(|v| v.as_u64()) {
        q.heartbeat = Some(h);
    }

    process_changes_request(state, db, q, doc_ids, selector).await
}
