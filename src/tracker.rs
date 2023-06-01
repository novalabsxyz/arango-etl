use crate::{arangodb_handler::ArangodbHandler, settings::Settings};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use tokio::time;
use tokio_graceful_shutdown::SubsystemHandle;

pub struct Tracker {
    after_utc: DateTime<Utc>,
    interval_duration: Duration,
    arangodb_handler: ArangodbHandler,
}

impl Tracker {
    pub async fn new(settings: &Settings, after_utc: DateTime<Utc>) -> Result<Self> {
        let arangodb_handler = ArangodbHandler::new(settings).await?;
        Ok(Self {
            interval_duration: settings.interval(),
            after_utc,
            arangodb_handler,
        })
    }
}

pub async fn run(mut tracker: Tracker, subsys: SubsystemHandle) -> Result<()> {
    let mut trigger = time::interval(tracker.interval_duration.to_std()?);

    loop {
        tokio::select! {
            _ = subsys.on_shutdown_requested() => {
                subsys.request_shutdown();
                break;
            }
            _ = trigger.tick() => {
                let max_ts = tracker.arangodb_handler.process(tracker.after_utc, None).await?;
                let next_utc = tracker.after_utc.checked_add_signed(tracker.interval_duration).context("failed to add interval")?;
                tracing::info!("start processing next tick @ {:?}", next_utc);
                tracker.after_utc = max_ts;
                tracing::info!("scheduling next tick @ {:?} for ts: {:?}", next_utc, max_ts);
            }
        }
    }
    tracing::info!("stopping current tracker for {:?}", tracker.after_utc);
    Ok(())
}
