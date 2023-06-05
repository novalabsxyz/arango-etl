use crate::{handler::ArangodbHandler, settings::Settings};
use anyhow::{Context, Result};
use chrono::{Days, NaiveDate, TimeZone, Utc};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Required date to rehydrate
    #[clap(long)]
    date: NaiveDate,
}

impl Cmd {
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(&settings.log))
            .with(tracing_subscriber::fmt::layer())
            .init();

        let after = self
            .date
            .and_hms_opt(00, 00, 00)
            .context("unable to get after date")?;
        let before = self
            .date
            .checked_add_days(Days::new(1))
            .context("unable to add 1 day")?
            .and_hms_opt(00, 00, 00)
            .context("unable to get before date")?;
        let after_utc = Utc.from_utc_datetime(&after);
        let before_utc = Utc.from_utc_datetime(&before);

        tracing::info!("after_utc: {:?}", after_utc);
        tracing::info!("before_utc: {:?}", before_utc);

        let handler = ArangodbHandler::new(settings).await?;
        handler.process(after_utc, Some(before_utc)).await?;
        Ok(())
    }
}
