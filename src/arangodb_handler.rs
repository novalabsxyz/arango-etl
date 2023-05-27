use crate::{arangodb::DB, settings::Settings, LOADER_WORKERS};
use anyhow::Result;
use chrono::{DateTime, Utc};
use file_store::{FileStore, FileType};
use futures::StreamExt;

#[derive(Debug)]
pub struct ArangodbHandler {
    pub store: FileStore,
    pub db: DB,
}

impl ArangodbHandler {
    pub async fn new(settings: &Settings) -> Result<Self> {
        let store = FileStore::from_settings(&settings.ingest).await?;
        let db = DB::from_settings(&settings.arangodb).await?;
        Ok(Self { db, store })
    }

    pub async fn handle_history(
        &self,
        before_ts: DateTime<Utc>,
        after_ts: DateTime<Utc>,
    ) -> Result<()> {
        tracing::debug!("before_ts: {:?}", before_ts);
        tracing::debug!("after_ts: {:?}", after_ts);

        let file_list = self
            .store
            .list(FileType::IotPoc, after_ts, before_ts)
            .boxed();

        let mut file_infos = self.store.source_unordered(LOADER_WORKERS, file_list);

        while let Some(msg) = file_infos.next().await {
            // NOTE: Doing a match statement to just log and keep going in case of errors
            match msg {
                Ok(m) => match self.db.populate_collections(&m).await {
                    Ok(_) => tracing::debug!("populated successfully!"),
                    Err(e) => tracing::error!("failed to populate_collections: {:?}", e),
                },
                Err(e) => tracing::error!("failed to get next msg: {:?}", e),
            }
        }

        Ok(())
    }

    pub async fn handle_current(&self, after_ts: DateTime<Utc>) -> Result<()> {
        tracing::debug!("finding files after_ts: {:?}", after_ts);
        let file_list = self.store.list(FileType::IotPoc, after_ts, None).boxed();
        let mut file_infos = self.store.source_unordered(LOADER_WORKERS, file_list);

        while let Some(msg) = file_infos.next().await {
            // NOTE: Doing a match statement to just log and keep going in case of errors
            match msg {
                Ok(m) => match self.db.populate_collections(&m).await {
                    Ok(_) => tracing::debug!("populated successfully!"),
                    Err(e) => tracing::error!("failed to populate_collections: {:?}", e),
                },
                Err(e) => tracing::error!("failed to get next msg: {:?}", e),
            }
        }

        Ok(())
    }
}
