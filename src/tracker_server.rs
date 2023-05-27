use crate::{arangodb_handler::ArangodbHandler, settings::Settings};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use tokio::time;

#[derive(Debug)]
pub struct TrackerServer {
    after_utc: DateTime<Utc>,
    interval_duration: Duration,
    arangodb_handler: ArangodbHandler,
}

impl TrackerServer {
    pub async fn new(settings: &Settings, after_utc: DateTime<Utc>) -> Result<Self> {
        let arangodb_handler = ArangodbHandler::new(settings).await?;
        Ok(Self {
            interval_duration: settings.interval(),
            after_utc,
            arangodb_handler,
        })
    }

    pub async fn run(&mut self, shutdown: &triggered::Listener) -> Result<()> {
        tracing::info!("starting current tracker @ {:?}", self.after_utc);
        self.arangodb_handler.handle_current(self.after_utc).await?;
        tracing::info!("done processing initial tick @ {:?}", self.after_utc);

        let mut trigger = time::interval(self.interval_duration.to_std()?);

        loop {
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = trigger.tick() => {
                    let previous_utc = self.after_utc;
                    self.after_utc = previous_utc.checked_add_signed(self.interval_duration).unwrap_or(previous_utc);
                    self.arangodb_handler.handle_current(self.after_utc).await?;
                    tracing::info!("done processing next tick @ {:?}", self.after_utc);
                }
            }
        }
        tracing::info!("stopping current tracker @ {:?}", self.after_utc);
        Ok(())
    }
}
