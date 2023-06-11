use chrono::{DateTime, Utc};
use file_store::FileInfo;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IotPocFile {
    pub _key: String,
    pub timestamp: DateTime<Utc>,
    pub unix_ts: i64,
    pub size: usize,
    pub done: bool,
    pub retries: u8,
}

impl From<&FileInfo> for IotPocFile {
    fn from(fi: &FileInfo) -> Self {
        Self {
            _key: fi.key.clone(),
            size: fi.size,
            timestamp: fi.timestamp,
            unix_ts: fi.timestamp.timestamp_millis(),
            done: false,
            retries: 0,
        }
    }
}
