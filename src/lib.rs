pub mod arangodb;
pub mod arangodb_handler;
pub mod cli;
pub mod handler;
pub mod settings;
use chrono::{DateTime, Utc};

pub const LOADER_WORKERS: usize = 16;

#[derive(Debug, Clone)]
pub enum Mode {
    Historical(DateTime<Utc>, DateTime<Utc>),
    // TODO: other modes (current)
}
