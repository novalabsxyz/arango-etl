use chrono::{DateTime, Duration, NaiveDateTime, TimeZone, Utc};
use config::{Config, Environment, File};
use file_store::Settings as FSettings;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// We are doing this here instead of CLI args to make
/// it easier to use with systemd unit files.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CurrentSettings {
    /// After timestamp to start from
    #[serde(default = "default_after_ts")]
    pub after: NaiveDateTime,
}

impl CurrentSettings {
    pub fn after_utc(&self) -> DateTime<Utc> {
        Utc.from_utc_datetime(&self.after)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RedisSettings {
    /// redis endpoint, default: redis://localhost:6739
    #[serde(default = "default_redis_endpoint")]
    pub endpoint: String,
    /// redis connection pool size, default: 16
    #[serde(default = "default_redis_pool_size")]
    pub pool_size: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrackerSettings {
    /// Tick interval (secs). Default = 10s.
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
    // Configure max_concurrent_files when sourcing iot-poc files from S3 bucket
    #[serde(default = "default_max_concurrent_files")]
    pub max_concurrent_files: usize,
    // Configure file_chunk_size (number of pocs to ingest at a time)
    #[serde(default = "default_file_chunk_size")]
    pub file_chunk_size: usize,
    // Configure max_processing_capacity (limit number of concurrent tasks)
    #[serde(default = "default_max_processing_capacity")]
    pub max_processing_capacity: usize,
    // Configure max_retries for one poc file
    #[serde(default = "default_max_retries")]
    pub max_retries: u8,
    // Configure ingest file store settings
    pub ingest: FSettings,
    // Configure arangodb settings
    pub arangodb: ArangoDBSettings,
    // Configure current tracker settings
    pub tracker: TrackerSettings,
    // Configure current mode settings
    pub current: CurrentSettings,
    // Configure redis settings
    pub redis: Option<RedisSettings>,
}

pub fn default_after_ts() -> NaiveDateTime {
    // NOTE: The default timestamp (picked randomly): 2023-06-27T17:48:50.980Z
    // Just crash if this doesn't work.
    NaiveDateTime::from_timestamp_millis(1687888130980).unwrap()
}

pub fn default_max_retries() -> u8 {
    3
}

pub fn default_max_concurrent_files() -> usize {
    16
}

pub fn default_file_chunk_size() -> usize {
    600
}

pub fn default_max_processing_capacity() -> usize {
    32
}

pub fn default_interval() -> i64 {
    10
}

pub fn default_log() -> String {
    "arango_etl=debug".to_string()
}

pub fn default_redis_pool_size() -> usize {
    16
}

pub fn default_redis_endpoint() -> String {
    "redis://localhost:6739".to_string()
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
