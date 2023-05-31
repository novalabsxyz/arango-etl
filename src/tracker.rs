use crate::{arangodb_handler::ArangodbHandler, settings::Settings};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use tokio::time;
use tokio_graceful_shutdown::SubsystemHandle;

#[derive(Debug)]
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
                let previous_utc = tracker.after_utc;
                let mut next_utc = previous_utc.checked_add_signed(tracker.interval_duration).context("failed to get next_utc")?;
                tracing::info!("start processing next tick for {:?}", next_utc);
                if let Ok(max_ts) = tracker.arangodb_handler.process(next_utc, None).await {
                    tracing::info!("max_ts {:?}", max_ts);
                    if max_ts >= next_utc {
                        next_utc = max_ts;
                    }
                }

                tracker.after_utc = next_utc;
                tracing::info!("done processing for {:?}", next_utc);
                tracing::info!("scheduling next tick for {:?}", next_utc.checked_add_signed(tracker.interval_duration));
            }
        }
    }
    tracing::info!("stopping current tracker for {:?}", tracker.after_utc);
    Ok(())
}
