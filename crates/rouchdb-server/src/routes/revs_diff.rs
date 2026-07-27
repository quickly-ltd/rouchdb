use std::collections::HashMap;

use axum::Json;
use axum::extract::{Path, State};

use crate::error::AppError;
use crate::state::AppState;

fn validate_db(db: &str, state: &AppState) -> Result<(), AppError> {
    if db != state.db_name {
        return Err(AppError(rouchdb_core::error::RouchError::NotFound(
            format!("Database does not exist: {db}"),
        )));
    }
    Ok(())
}

/// POST /{db}/_revs_diff — determine missing revisions.
pub async fn post_revs_diff(
    State(state): State<AppState>,
    Path(db): Path<String>,
    Json(body): Json<HashMap<String, Vec<String>>>,
) -> Result<Json<rouchdb_core::document::RevsDiffResponse>, AppError> {
    validate_db(&db, &state)?;
    let res = state.db.adapter().revs_diff(body).await?;
    Ok(Json(res))
}
