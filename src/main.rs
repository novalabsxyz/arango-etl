use anyhow::Result;
use arango_etl::{cli::history, settings::Settings};
use clap::Parser;
use std::path;

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    /// Run in historical data gathering mode
    History(history::Cmd),
}

impl Cmd {
    pub async fn run(self, settings: Settings) -> Result<()> {
        match self {
            Self::History(cmd) => cmd.run(&settings).await,
        }
    }
}

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "ArangoDB ETL")]
pub struct Cli {
    #[clap(short = 'c')]
    config: Option<path::PathBuf>,

    #[clap(subcommand)]
    cmd: Cmd,
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        let settings = Settings::new(self.config)?;
        tracing::info!("settings: {:#?}", settings);
        self.cmd.run(settings).await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
