use chrono::Duration;
use config::{Config, Environment, File};
use file_store::Settings as FSettings;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrackerSettings {
    /// Tick interval (secs). Default = 60s.
    #[serde(default = "default_interval")]
    pub interval: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ArangoDBSettings {
    #[serde(default = "default_arangodb_endpoint")]
    pub endpoint: String,
    #[serde(default = "default_arangodb_user")]
    pub user: String,
    #[serde(default = "default_arangodb_password")]
    pub password: String,
    #[serde(default = "default_arangodb_database")]
    pub database: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Settings {
    // Configure logging level = debug
    #[serde(default = "default_log")]
    pub log: String,
    // Configure ingest file store settings
    pub ingest: FSettings,
    // Configure arangodb settings
    pub arangodb: ArangoDBSettings,
    // Configure current tracker settings
    pub tracker: TrackerSettings,
}

pub fn default_interval() -> i64 {
    60
}

pub fn default_log() -> String {
    "arango_etl=debug".to_string()
}

pub fn default_arangodb_endpoint() -> String {
    "http://localhost:8925".to_string()
}

pub fn default_arangodb_user() -> String {
    "root".to_string()
}

pub fn default_arangodb_password() -> String {
    "arangodb".to_string()
}

pub fn default_arangodb_database() -> String {
    "iot".to_string()
}

impl Settings {
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self, config::ConfigError> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        builder
            .add_source(Environment::with_prefix("ARANGO_ETL").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
    }

    pub fn interval(&self) -> Duration {
        Duration::seconds(self.tracker.interval)
    }
}
