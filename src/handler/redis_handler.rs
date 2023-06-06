use crate::settings::RedisSettings;
use anyhow::{Error, Result};
use deadpool_redis::{redis::AsyncCommands, Config, Pool, Runtime};

pub struct RedisHandler {
    pool: Pool,
}

impl RedisHandler {
    pub fn from_settings(settings: &RedisSettings) -> Result<Self> {
        let pool = Config::from_url(&settings.endpoint)
            .builder()?
            .max_size(settings.pool_size)
            .runtime(Runtime::Tokio1)
            .build()?;
        Ok(Self { pool })
    }

    pub async fn xadd(&self, stream_name: &str, poc_id: &str) -> Result<String> {
        let mut conn = self.pool.get().await?;
        conn.xadd(stream_name, "*", &[(&poc_id, "done".to_string())])
            .await
            .map_err(Error::from)
    }
}
