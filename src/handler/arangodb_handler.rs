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
use tokio::task::JoinSet;

pub struct ArangodbHandler {
    store: FileStore,
    db: Arc<DB>,
    redis_handler: Arc<RedisHandler>,
    max_ingest: usize,
    num_loaders: usize,
}

impl ArangodbHandler {
    pub async fn new(settings: &Settings) -> Result<Self> {
        let store = FileStore::from_settings(&settings.ingest).await?;
        let redis_handler = Arc::new(RedisHandler::from_settings(settings).await?);
        let max_ingest = settings.max_ingest;
        let num_loaders = settings.num_loaders;
        let db = Arc::new(DB::from_settings(&settings.arangodb).await?);
        Ok(Self {
            db,
            store,
            redis_handler,
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
        let all_files = file_list.len();
        tracing::info!("all files: {:#?}", all_files);

        if file_list.is_empty() {
            tracing::info!("no available ingest files of type {ft}");
            return Ok(after_ts);
        }

        let file_list = self.remove_done_files(file_list).await?;
        let considered_files = file_list.len();
        tracing::info!("not done files: {:#?}", considered_files);
        tracing::info!("ignored files: {:#?}", all_files - considered_files);

        // Set max_ts to the file with the highest timestamp
        let mut max_ts = after_ts;
        for file_info in file_list.iter() {
            if file_info.timestamp >= max_ts {
                max_ts = file_info.timestamp
            }
        }

        let keys: Vec<String> = file_list.iter().map(|fi| fi.key.clone()).collect();

        self.init_files(&file_list).await?;
        self.process_files(file_list).await?;
        self.complete_files(&keys).await?;

        Ok(max_ts)
    }

    /// Filter out "done" files
    async fn remove_done_files(&self, file_list: Vec<FileInfo>) -> Result<Vec<FileInfo>> {
        let fl = stream::iter(file_list)
            .filter_map(|file_info| {
                let key = file_info.key.clone();
                let db = Arc::clone(&self.db);

                async move {
                    match db.get_file(&key).await {
                        Ok(Some(doc))
                            if doc.get("done").and_then(|v| v.as_bool()).unwrap_or(true) =>
                        {
                            tracing::info!("ignore already processed file: {:?}", key);
                            None
                        }
                        Ok(_) => Some(file_info),
                        Err(e) => {
                            tracing::error!("error fetching file info from DB: {:?}", e);
                            None
                        }
                    }
                }
            })
            .collect::<Vec<_>>()
            .await;
        Ok(fl)
    }

    /// Initialize files being processed, done: false
    async fn init_files(&self, files: &Vec<FileInfo>) -> Result<()> {
        self.db.init_files(files).await?;
        Ok(())
    }

    /// Mark files done processing, done: true
    async fn complete_files(&self, keys: &[String]) -> Result<()> {
        self.db.complete_files(keys).await?;
        Ok(())
    }

    /// Process necessary files
    async fn process_files(&self, file_list: Vec<FileInfo>) -> Result<()> {
        let mut stream = self
            .store
            .source_unordered(self.num_loaders, stream::iter(file_list).map(Ok).boxed());

        let mut set = JoinSet::new();

        while let Some(msg) = stream.next().await {
            match msg {
                Err(err) => tracing::warn!("skipping entry in stream: {err:?}"),
                Ok(buf) => {
                    let db = Arc::clone(&self.db);
                    let rh = Arc::clone(&self.redis_handler);
                    set.spawn(async move {
                        match LoraPocV1::decode(buf) {
                            Ok(dec_msg) => match db.populate_collections(dec_msg).await {
                                Err(e) => tracing::error!("error populating collections: {:?}", e),
                                Ok(None) => (),
                                Ok(Some(poc_id)) => {
                                    tracing::debug!("storing poc_id: {:?} in redis", poc_id);
                                    if let Err(e) = rh.xadd("poc_id", &poc_id).await {
                                        tracing::error!(
                                            "failed to store poc_id {:?} in redis, error: {:?}",
                                            poc_id,
                                            e
                                        );
                                    }
                                }
                            },
                            Err(e) => {
                                tracing::error!("error decoding message: {:?}", e);
                            }
                        }
                    });

                    if set.len() > self.max_ingest {
                        tracing::debug!("processing {} submissions", { self.max_ingest });
                        set.join_next().await;
                    }
                }
            }
        }

        // Make sure the tasks are finished to completion even when we run out of stream items
        while !set.is_empty() {
            set.join_next().await;
        }
        Ok(())
    }
}
