use crate::{settings::Settings, tracker};
use anyhow::Result;
use chrono::{NaiveDateTime, TimeZone, Utc};
use tokio::time::Duration;
use tokio_graceful_shutdown::{SubsystemHandle, Toplevel};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Args)]
pub struct Server {
    /// Required start time to begin from (inclusive)
    #[clap(long)]
    after: NaiveDateTime,
}

impl Server {
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(&settings.log))
            .with(tracing_subscriber::fmt::layer())
            .init();

        let after_utc = Utc.from_utc_datetime(&self.after);
        let tracker = tracker::Tracker::new(settings, after_utc).await?;
        let subsystem = |subsys: SubsystemHandle| async { tracker::run(tracker, subsys).await };

        match Toplevel::new()
            .start("tracker", subsystem)
            .catch_signals()
            .handle_shutdown_requests(Duration::from_millis(500))
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::error!("error: {:?}", e);
                Err(e.into())
            }
        }
    }
}
