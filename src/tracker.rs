use crate::{arangodb_handler::ArangodbHandler, settings::Settings};
use anyhow::Result;
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
    tracing::info!("start current tracker @ {:?}", tracker.after_utc);
    tracker
        .arangodb_handler
        .handle_current(tracker.after_utc)
        .await?;
    tracing::info!("done processing initial tick @ {:?}", tracker.after_utc);

    let mut trigger = time::interval(tracker.interval_duration.to_std()?);

    loop {
        tokio::select! {
            _ = subsys.on_shutdown_requested() => {
                subsys.request_shutdown();
                break;
            }
            _ = trigger.tick() => {
                let previous_utc = tracker.after_utc;
                let next_utc = previous_utc.checked_add_signed(tracker.interval_duration).unwrap_or(previous_utc);
                tracker.after_utc = next_utc;
                tracing::info!("start processing next tick @ {:?}", next_utc);
                tracker.arangodb_handler.handle_current(next_utc).await?;
                tracing::info!("done processing next tick @ {:?}", next_utc);
            }
        }
    }
    tracing::info!("stopping current tracker @ {:?}", tracker.after_utc);
    Ok(())
}
