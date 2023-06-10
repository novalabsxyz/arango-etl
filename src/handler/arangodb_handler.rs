use crate::{
    handler::{arangodb::DB, RedisHandler},
    settings::Settings,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use file_store::{FileInfo, FileStore, FileType};
use futures::stream::{self, StreamExt};
use helium_proto::{services::poc_lora::LoraPocV1, Message};
use std::sync::Arc;
use tokio::sync::Semaphore;

pub struct ArangodbHandler {
    store: FileStore,
    db: Arc<DB>,
    redis_handler: Arc<Option<RedisHandler>>,
    file_chunk_size: usize,
    max_concurrent_files: usize,
    max_processing_capacity: usize,
}

impl ArangodbHandler {
    pub async fn new(settings: &Settings) -> Result<Self> {
        let store = FileStore::from_settings(&settings.ingest).await?;

        let redis_handler = if let Some(rh) = &settings.redis {
            Arc::new(Some(RedisHandler::from_settings(rh)?))
        } else {
            Arc::new(None)
        };

        let db = Arc::new(DB::from_settings(&settings.arangodb).await?);
        Ok(Self {
            db,
            store,
            redis_handler,
            file_chunk_size: settings.file_chunk_size,
            max_concurrent_files: settings.max_concurrent_files,
            max_processing_capacity: settings.max_processing_capacity,
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
        let mut file_infos = self.store.list_all(ft, after_ts, before_ts).await?;

        // return early if no files to process
        if file_infos.is_empty() {
            tracing::info!("no available ingest files of type {ft}");
            return Ok(after_ts);
        }

        self.exclude_done_files(&mut file_infos).await?;

        // return early if all files are already processed
        if file_infos.is_empty() {
            tracing::info!("all {ft} files processed!");
            return Ok(after_ts);
        }

        // Set max_ts to the file with the highest timestamp
        let mut max_ts = after_ts;
        for file_info in file_infos.iter() {
            if file_info.timestamp >= max_ts {
                max_ts = file_info.timestamp
            }
        }

        self.process_files(file_infos).await?;

        Ok(max_ts)
    }

    /// Exclude already done files
    async fn exclude_done_files(&self, file_infos: &mut Vec<FileInfo>) -> Result<()> {
        let before_len = file_infos.len();
        tracing::info!("# all files: {:#?}", before_len);

        match self.db.get_done_file_keys().await {
            Ok(done_file_keys) if !done_file_keys.is_empty() => {
                tracing::info!("# done files: {:#?}", done_file_keys.len());
                file_infos.retain(|fi| !done_file_keys.contains(&fi.key));
                let after_len = file_infos.len();
                tracing::info!("# not done files: {:#?}", after_len);
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!("ignoring error: {:?}", e);
            }
        }
        Ok(())
    }

    /// Process necessary files
    async fn process_files(&self, file_infos: Vec<FileInfo>) -> Result<()> {
        if file_infos.is_empty() {
            return Ok(());
        }

        let semaphore = Arc::new(Semaphore::new(self.max_processing_capacity));

        stream::iter(file_infos)
            .for_each_concurrent(self.max_concurrent_files, |file_info| {
                let semaphore = semaphore.clone();

                async move {
                    match semaphore.acquire().await {
                        Ok(_permit) => {
                            match self.process_file(file_info.clone()).await {
                                Ok(()) => match self.db.complete_file(&file_info.key).await {
                                    Ok(()) => {
                                        tracing::info!("completed file ts: {}", file_info.timestamp)
                                    }
                                    Err(err) => {
                                        tracing::warn!(
                                            "error completing file ts: {}, {err:?}",
                                            file_info.timestamp
                                        )
                                    }
                                },
                                Err(err) => tracing::warn!(
                                    "error while processing file ts: {}, err: {err:?}",
                                    file_info.timestamp
                                ),
                            };
                        }
                        Err(e) => {
                            tracing::error!("Failed to acquire semaphore: {}", e);
                        }
                    }
                }
            })
            .await;
        Ok(())
    }

    /// Process individual file
    async fn process_file(&self, file_info: FileInfo) -> Result<()> {
        self.db.init_file(&file_info).await?;
        self.store
            .stream_file(file_info)
            .await?
            .chunks(self.file_chunk_size)
            .for_each_concurrent(self.max_concurrent_files, |msgs| async move {
                for msg in msgs {
                    match msg {
                        Err(err) => {
                            tracing::warn!("skipping report of due to error {err:?}")
                        }
                        Ok(buf) => {
                            let db = self.db.clone();
                            let rh = self.redis_handler.clone();
                            match LoraPocV1::decode(buf) {
                                Ok(dec_msg) => match (db.populate_collections(dec_msg).await, &*rh)
                                {
                                    (Err(e), _) => {
                                        tracing::error!("error populating collections: {:?}", e)
                                    }
                                    (Ok(Some(poc_id)), Some(rh)) => {
                                        tracing::debug!("storing poc_id: {:?} in redis", poc_id);
                                        if let Err(e) = rh.xadd("poc_id", &poc_id).await {
                                            tracing::error!(
                                                "failed to store poc_id {:?} in redis, error: {:?}",
                                                poc_id,
                                                e
                                            );
                                        }
                                    }
                                    _ => (),
                                },
                                Err(e) => {
                                    tracing::error!("error decoding message: {:?}", e);
                                }
                            }
                        }
                    }
                }
            })
            .await;
        Ok(())
    }
}
