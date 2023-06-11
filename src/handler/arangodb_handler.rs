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
use tokio::sync::{Mutex, Semaphore};

pub struct ArangodbHandler {
    store: FileStore,
    db: Arc<DB>,
    redis_handler: Arc<Option<RedisHandler>>,
    file_chunk_size: usize,
    max_concurrent_files: usize,
    max_processing_capacity: usize,
    max_retries: u8,
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
            max_retries: settings.max_retries,
        })
    }

    /// Processes a set of files within a specified timestamp range.
    ///
    /// This function performs the following steps:
    /// 1. Lists all `IotPoc` files that have a timestamp between `after_ts` and `before_ts`.
    /// 2. Excludes any files that have already been processed.
    /// 3. Processes the remaining files concurrently.
    ///
    /// If all files are processed successfully, the function returns the timestamp of the latest file processed.
    /// If there are files that failed during processing, it returns the timestamp of the earliest failed file,
    /// enabling the next run to start processing from that file.
    /// If an error occurs while processing the files, it returns the timestamp from which it started processing,
    /// effectively enabling the next run to retry processing the same set of files.
    ///
    /// # Arguments
    ///
    /// * `after_ts` - The start of the timestamp range for processing.
    /// * `before_ts` - The optional end of the timestamp range for processing.
    ///
    /// # Returns
    ///
    /// A Result containing the timestamp from which the next set of files should start processing.
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
        let max_ts = self.get_max_ts(&file_infos).unwrap_or(after_ts);

        match self.process_files(file_infos).await {
            Ok(None) => Ok(max_ts),
            Ok(Some(failed_files)) => {
                let failed_files = self
                    .filter_retry_exceeded_failed_files(failed_files)
                    .await?;
                // If there are failed files, return the minimum timestamp of those files
                // Subsequent duplicate files which are already processed will be ignored
                // Files which failed will be reprocessed
                let min_ts = self.get_min_ts(&failed_files).unwrap_or(max_ts);
                Ok(min_ts)
            }
            Err(err) => {
                // If there is an error, return the initial after_ts
                tracing::error!("error processing files: {:?}", err);
                Ok(after_ts)
            }
        }
    }

    // Filter failed files which have reached the max number of retries
    // by querying the db for the number of retries for that file's key
    async fn filter_retry_exceeded_failed_files(
        &self,
        failed_files: Vec<FileInfo>,
    ) -> Result<Vec<FileInfo>> {
        let failed_files = stream::iter(failed_files)
            .filter_map(|fi| {
                let db = self.db.clone();
                async move {
                    match db.get_file_retries(&fi.key).await {
                        Ok(retries) => {
                            if retries <= self.max_retries {
                                Some(fi)
                            } else {
                                None
                            }
                        }
                        Err(err) => {
                            tracing::error!("error getting file retries: {:?}", err);
                            None
                        }
                    }
                }
            })
            .collect()
            .await;
        Ok(failed_files)
    }

    fn get_max_ts(&self, file_infos: &[FileInfo]) -> Option<DateTime<Utc>> {
        file_infos
            .iter()
            .max_by_key(|fi| fi.timestamp)
            .map(|fi| fi.timestamp)
    }

    fn get_min_ts(&self, file_infos: &[FileInfo]) -> Option<DateTime<Utc>> {
        file_infos
            .iter()
            .min_by_key(|fi| fi.timestamp)
            .map(|fi| fi.timestamp)
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

    /// Processes a list of files concurrently.
    ///
    /// This function concurrently processes each file in the `file_infos` list.
    /// A semaphore is used to limit the number of concurrently processed files.
    /// Each file is processed using the `process_file` method. If processing a file
    /// fails or marking it as complete fails, the file is added to a list of failed files.
    ///
    /// After all files have been processed, the function returns either None,
    /// if all files have been processed successfully, or a list of the files that failed to process.
    ///
    /// # Arguments
    ///
    /// * `file_infos` - A vector of `FileInfo` objects, each representing a file to be processed.
    ///
    /// # Returns
    ///
    /// A Result containing either None if all files have been processed successfully,
    /// or a list of files that failed to process.
    async fn process_files(&self, file_infos: Vec<FileInfo>) -> Result<Option<Vec<FileInfo>>> {
        if file_infos.is_empty() {
            return Ok(None);
        }

        let semaphore = Arc::new(Semaphore::new(self.max_processing_capacity));
        let failed_files: Arc<Mutex<Vec<FileInfo>>> = Arc::new(Mutex::new(vec![]));

        stream::iter(file_infos)
            .for_each_concurrent(self.max_concurrent_files, |file_info| {
                let semaphore = semaphore.clone();
                let failed_files = failed_files.clone();

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
                                        );
                                        failed_files.lock().await.push(file_info.clone());
                                        if let Err(e) =
                                            self.db.increment_file_retry(&file_info.key).await
                                        {
                                            tracing::error!(
                                                "error incrementing file retry: {:?}",
                                                e
                                            );
                                        }
                                    }
                                },
                                Err(err) => {
                                    tracing::warn!(
                                        "error while processing file ts: {}, err: {err:?}",
                                        file_info.timestamp
                                    );
                                    if let Err(e) =
                                        self.db.increment_file_retry(&file_info.key).await
                                    {
                                        tracing::error!("error incrementing file retry: {:?}", e);
                                    }
                                    failed_files.lock().await.push(file_info.clone());
                                }
                            };
                        }
                        Err(e) => {
                            tracing::error!("Failed to acquire semaphore: {}", e);
                        }
                    }
                }
            })
            .await;

        let failed_files = failed_files.lock().await.clone();
        if failed_files.is_empty() {
            Ok(None)
        } else {
            tracing::warn!("# failed_files {:?}", failed_files.len());
            Ok(Some(failed_files))
        }
    }

    /// Processes an individual file.
    ///
    /// This function performs the following steps:
    /// 1. Initializes the file in the database.
    /// 2. Streams the file from the file store.
    /// 3. For each message in the file, it decodes the message and populates the database with the decoded data.
    /// 4. If a Redis handler is available, it sends the encoded `poc_id` to Redis.
    ///
    /// # Arguments
    ///
    /// * `file_info` - A `FileInfo` object representing the file to be processed.
    ///
    /// # Returns
    ///
    /// A Result indicating whether the file has been processed successfully.
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
