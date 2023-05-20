use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait Handler {
    async fn process(&self) -> Result<()>;
}
