use axum::Json;
use axum::extract::State;

use crate::state::AppState;

/// GET /_active_tasks — list running tasks (currently: replications),
/// matching CouchDB's schema so any standard client can poll sync progress
/// the normal CouchDB way instead of needing a bespoke endpoint.
pub async fn get_active_tasks(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!(state.active_tasks.snapshot()))
}
