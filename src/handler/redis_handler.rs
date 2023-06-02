use crate::settings::Settings;
use anyhow::Result;
use deadpool_redis::{redis::AsyncCommands, Config, Pool, Runtime};

pub struct RedisHandler {
    pool: Pool,
}

impl RedisHandler {
    pub async fn from_settings(settings: &Settings) -> Result<Self> {
        let pool = Config::from_url(&settings.redis.endpoint)
            .builder()?
            .max_size(settings.redis.pool_size)
            .runtime(Runtime::Tokio1)
            .build()?;
        Ok(Self { pool })
    }

    pub async fn xadd(&self, stream_name: &str, poc_id: &str) -> Result<()> {
        let mut conn = self.pool.get().await?;
        if let Err(e) = conn
            .xadd::<_, _, _, String, String>(stream_name, "*", &[(&poc_id, "done".to_string())])
            .await
        {
            tracing::error!(
                "failed to store poc_id {:?} in redis, error: {:?}",
                poc_id,
                e
            );
            return Err(e.into());
        }
        Ok(())
    }
}
