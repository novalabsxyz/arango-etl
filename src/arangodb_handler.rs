use crate::{arangodb::DB, settings::Settings};
use anyhow::Result;
use chrono::{DateTime, Utc};
use file_store::{FileStore, FileType};
use futures::stream::{self, StreamExt};
use helium_proto::{services::poc_lora::LoraPocV1, Message};
use std::sync::Arc;
use tokio::task::JoinSet;

#[derive(Debug)]
pub struct ArangodbHandler {
    pub store: FileStore,
    pub db: Arc<DB>,
    max_ingest: usize,
    num_loaders: usize,
}

impl ArangodbHandler {
    pub async fn new(settings: &Settings) -> Result<Self> {
        let store = FileStore::from_settings(&settings.ingest).await?;
        let max_ingest = settings.max_ingest;
        let num_loaders = settings.num_loaders;
        let db = Arc::new(DB::from_settings(&settings.arangodb).await?);
        Ok(Self {
            db,
            store,
            max_ingest,
            num_loaders,
        })
    }

    pub async fn process(
        &self,
        after_ts: DateTime<Utc>,
        before_ts: Option<DateTime<Utc>>,
    ) -> Result<DateTime<Utc>> {
        tracing::debug!("before_ts: {:?}", before_ts);
        tracing::debug!("after_ts: {:?}", after_ts);

        let ft = FileType::IotPoc;
        let file_list = self.store.list_all(ft, after_ts, before_ts).await?;
        tracing::info!("# files: {:#?}", file_list);

        if file_list.is_empty() {
            tracing::info!("no available ingest files of type {ft}");
            return Ok(after_ts);
        }

        // Set max_ts to the file with the highest timestamp
        let mut max_ts = after_ts;
        for file_info in file_list.iter() {
            if file_info.timestamp >= max_ts {
                max_ts = file_info.timestamp
            }
        }

        let mut stream = self
            .store
            .source_unordered(self.num_loaders, stream::iter(file_list).map(Ok).boxed());

        let mut set = JoinSet::new();

        while let Some(msg) = stream.next().await {
            match msg {
                Err(err) => tracing::warn!("skipping entry in stream: {err:?}"),
                Ok(buf) => {
                    let db = Arc::clone(&self.db);
                    set.spawn(async move {
                        match LoraPocV1::decode(buf) {
                            Ok(dec_msg) => {
                                if let Err(e) = db.populate_collections(dec_msg).await {
                                    tracing::error!("Error populating collections: {:?}", e);
                                }
                            }
                            Err(e) => {
                                tracing::error!("Error decoding message: {:?}", e);
                            }
                        }
                    });

                    if set.len() > self.max_ingest {
                        tracing::debug!("Processing {} submissions", { self.max_ingest });
                        set.join_next().await;
                    }
                }
            }
        }

        // Make sure the tasks are finished to completion even when we run out of stream items
        while !set.is_empty() {
            set.join_next().await;
        }

        Ok(max_ts)
    }
}
