use crate::{settings::Settings, tracker_server::TrackerServer};
use anyhow::{Error, Result};
use chrono::{NaiveDateTime, TimeZone, Utc};
use futures_util::TryFutureExt;
use tokio::{self, signal};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Args)]
pub struct Server {
    /// Required start time to look for (inclusive)
    #[clap(long)]
    after: NaiveDateTime,
}

impl Server {
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(&settings.log))
            .with(tracing_subscriber::fmt::layer())
            .init();

        // configure shutdown trigger
        let (shutdown_trigger, shutdown) = triggered::trigger();

        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        tokio::spawn(async move {
            tokio::select! {
                _ = sigterm.recv() => shutdown_trigger.trigger(),
                _ = signal::ctrl_c() => shutdown_trigger.trigger(),
            }
        });

        let after_utc = Utc.from_utc_datetime(&self.after);
        let mut tracker = TrackerServer::new(settings, after_utc).await?;

        tokio::try_join!(tracker.run(&shutdown).map_err(Error::from),).map(|_| ())
    }
}
