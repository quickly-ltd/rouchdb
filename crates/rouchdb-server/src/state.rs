use std::sync::Arc;

use rouchdb::Database;

use crate::tasks::ActiveTasks;

/// Shared application state for all route handlers.
#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Database>,
    pub db_name: String,
    pub active_tasks: ActiveTasks,
}
