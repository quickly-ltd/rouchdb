use axum::Json;
use axum::extract::{Path, State};
use serde::Deserialize;

use rouchdb_core::document::{BulkGetItem, BulkGetResponse};

use crate::error::AppError;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct BulkGetRequestBody {
    pub docs: Vec<BulkGetItem>,
    #[serde(default)]
    pub revs: bool,
    #[serde(default)]
    pub latest: bool,
}

fn validate_db(db: &str, state: &AppState) -> Result<(), AppError> {
    if db != state.db_name {
        return Err(AppError(rouchdb_core::error::RouchError::NotFound(
            format!("Database does not exist: {db}"),
        )));
    }
    Ok(())
}

/// POST /{db}/_bulk_get — fetch multiple documents by ID and revision.
pub async fn post_bulk_get(
    State(state): State<AppState>,
    Path(db): Path<String>,
    Json(body): Json<BulkGetRequestBody>,
) -> Result<Json<BulkGetResponse>, AppError> {
    validate_db(&db, &state)?;
    let res = state.db.adapter().bulk_get(body.docs).await?;
    Ok(Json(res))
}
