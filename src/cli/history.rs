use crate::{arangodb_handler::ArangodbHandler, settings::Settings};
use anyhow::Result;
use chrono::{NaiveDateTime, TimeZone, Utc};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Required start time to look for (inclusive)
    #[clap(long)]
    after: NaiveDateTime,
    /// Required before time to look for (inclusive)
    #[clap(long)]
    before: NaiveDateTime,
}

impl Cmd {
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(&settings.log))
            .with(tracing_subscriber::fmt::layer())
            .init();

        let after_utc = Utc.from_utc_datetime(&self.after);
        let before_utc = Utc.from_utc_datetime(&self.before);

        let handler = ArangodbHandler::new(settings).await?;
        handler.process(after_utc, Some(before_utc)).await?;
        Ok(())
    }
}
