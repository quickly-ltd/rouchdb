use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::Deserialize;

use crate::error::AppError;
use crate::state::AppState;

#[derive(Deserialize, Default)]
pub struct LocalQuery {
    pub rev: Option<String>,
}

fn validate_db(db: &str, state: &AppState) -> Result<(), AppError> {
    if db != state.db_name {
        return Err(AppError(rouchdb_core::error::RouchError::NotFound(
            format!("Database does not exist: {db}"),
        )));
    }
    Ok(())
}

fn normalize_id(id: &str) -> (&str, String) {
    let clean = id.strip_prefix("_local/").unwrap_or(id);
    let full = format!("_local/{}", clean);
    (clean, full)
}

/// GET /{db}/_local or /{db}/_local/ — list all local documents.
pub async fn get_all_local(
    State(state): State<AppState>,
    Path(db): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    validate_db(&db, &state)?;
    let opts = rouchdb_core::document::AllDocsOptions {
        include_docs: true,
        start_key: Some("_local/".to_string()),
        end_key: Some("_local/\u{ffff}".to_string()),
        ..Default::default()
    };
    let response = state.db.adapter().all_docs(opts).await?;
    let total_rows = response.rows.len() as u64;
    Ok(Json(serde_json::json!({
        "offset": 0,
        "rows": response.rows,
        "total_rows": total_rows,
    })))
}

/// GET /{db}/_local/{*id} — get a local document (checkpoint).
pub async fn get_local(
    State(state): State<AppState>,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, AppError> {
    validate_db(&db, &state)?;
    let (clean_id, full_id) = normalize_id(&id);

    match state.db.adapter().get_local(clean_id).await {
        Ok(mut doc) => {
            if let serde_json::Value::Object(ref mut map) = doc {
                map.insert("_id".into(), serde_json::Value::String(full_id));
            }
            Ok(Json(doc))
        }
        Err(rouchdb_core::error::RouchError::NotFound(_)) => {
            Err(AppError(rouchdb_core::error::RouchError::NotFound(
                "missing".to_string(),
            )))
        }
        Err(err) => Err(AppError(err)),
    }
}

/// PUT /{db}/_local/{*id} — create or update a local document (checkpoint).
pub async fn put_local(
    State(state): State<AppState>,
    Path((db, id)): Path<(String, String)>,
    Query(query): Query<LocalQuery>,
    Json(mut body): Json<serde_json::Value>,
) -> Result<(StatusCode, Json<serde_json::Value>), AppError> {
    validate_db(&db, &state)?;
    let (clean_id, full_id) = normalize_id(&id);

    let current_rev = query.rev.or_else(|| {
        body.as_object()
            .and_then(|o| o.get("_rev"))
            .and_then(|v| v.as_str())
            .map(String::from)
    });

    let next_rev = match current_rev {
        Some(ref r) => {
            if let Some((num_str, _)) = r.split_once('-') {
                if let Ok(num) = num_str.parse::<u64>() {
                    format!("{}-{}", num + 1, uuid::Uuid::new_v4().simple())
                } else {
                    format!("0-{}", uuid::Uuid::new_v4().simple())
                }
            } else {
                format!("0-{}", uuid::Uuid::new_v4().simple())
            }
        }
        None => "0-1".to_string(),
    };

    if let serde_json::Value::Object(ref mut map) = body {
        map.insert("_id".into(), serde_json::Value::String(full_id.clone()));
        map.insert("_rev".into(), serde_json::Value::String(next_rev.clone()));
    }

    state.db.adapter().put_local(clean_id, body).await?;

    let response = serde_json::json!({
        "ok": true,
        "id": full_id,
        "rev": next_rev,
    });

    Ok((StatusCode::CREATED, Json(response)))
}

/// DELETE /{db}/_local/{*id} — delete a local document (checkpoint).
pub async fn delete_local(
    State(state): State<AppState>,
    Path((db, id)): Path<(String, String)>,
    Query(query): Query<LocalQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    validate_db(&db, &state)?;
    let (clean_id, full_id) = normalize_id(&id);

    let rev = query.rev.unwrap_or_else(|| "0-0".to_string());
    state.db.adapter().remove_local(clean_id).await?;

    let response = serde_json::json!({
        "ok": true,
        "id": full_id,
        "rev": rev,
    });

    Ok(Json(response))
}
