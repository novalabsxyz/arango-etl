use crate::{arangodb::DB, settings::Settings};
use anyhow::{Error, Result};
use chrono::{DateTime, Utc};
use file_store::{FileInfo, FileStore, FileType};
use futures::stream::TryStreamExt;
use helium_proto::{services::poc_lora::LoraPocV1, Message};

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

        self.store
            .list(FileType::IotPoc, after_ts, before_ts)
            .map_err(Error::from)
            .and_then(|file| self.process_file(file))
            .try_fold((), |_, r| async move { Ok(r) })
            .await?;

        Ok(())
    }

    pub async fn handle_current(&self, after_ts: DateTime<Utc>) -> Result<()> {
        tracing::debug!("finding files after_ts: {:?}", after_ts);

        self.store
            .list(FileType::IotPoc, after_ts, None)
            .map_err(Error::from)
            .and_then(|file| self.process_file(file))
            .try_fold((), |_, r| async move { Ok(r) })
            .await?;

        Ok(())
    }

    async fn process_file(&self, file: FileInfo) -> Result<()> {
        self.store
            .stream_file(file)
            .await?
            .map_err(Error::from)
            .and_then(|buf| async { LoraPocV1::decode(buf).map_err(Error::from) })
            .and_then(|dec_msg| async move { self.db.populate_collections(dec_msg).await })
            .try_fold((), |_, r| async move { Ok(r) })
            .await?;
        Ok(())
    }
}
