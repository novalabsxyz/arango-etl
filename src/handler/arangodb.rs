use crate::{
    document::{
        iot_poc_file::IotPocFile, Beacon, Edge, Hotspot, BEACON_COLLECTION, FILES_COLLECTION,
        HOTSPOT_COLLECTION, WITNESS_EDGE_COLLECTION,
    },
    settings::ArangoDBSettings,
};
use anyhow::Result;
use arangors::{
    document::options::InsertOptions,
    index::{Index, IndexSettings},
    uclient::reqwest::ReqwestClient,
    AqlQuery, ClientError, Collection, Connection, Database,
};
use file_store::{iot_valid_poc::IotPoc, FileInfo};
use helium_proto::services::poc_lora::LoraPocV1;
use serde_json::Value;

type ArangoCollection = Collection<ReqwestClient>;
type ArangoDatabase = Database<ReqwestClient>;

#[derive(Debug)]
pub struct DB {
    pub conn: Connection,
    pub inner: ArangoDatabase,
    pub collections: Collections,
}

#[derive(thiserror::Error, Debug)]
pub enum DBError {
    #[error("serde error")]
    SerdeError(#[from] serde_json::Error),
    #[error("arango client error")]
    ArangoClientError(#[from] arangors::ClientError),
    #[error("other error")]
    Other(#[from] anyhow::Error),
}

#[derive(Debug)]
pub struct Collections {
    // store beacon json (including a list of witnesses)
    pub beacons: ArangoCollection,
    // store all the hotspots (beacon + witness)
    pub hotspots: ArangoCollection,
    // edge collection to store beacon -> witness information
    pub witnesses: ArangoCollection,
    // store names of all processed (and in-process) iot-poc files
    pub files: ArangoCollection,
}

#[derive(Debug)]
enum HotspotType {
    Beacon,
    Witness,
}

impl DB {
    pub async fn from_settings(settings: &ArangoDBSettings) -> Result<Self> {
        let conn = Connection::establish_basic_auth(
            &settings.endpoint,
            &settings.user,
            &settings.password,
        )
        .await?;

        let existing_databases = conn.accessible_databases().await?;

        let (inner, collections) = if !existing_databases.contains_key(&settings.database) {
            let inner = conn.create_database(&settings.database).await?;
            let cols = create_new_db_and_collections(&inner).await?;
            (inner, cols)
        } else {
            let inner = conn.db(&settings.database).await?;
            let cols = use_existing_db_and_collections(&inner).await?;
            (inner, cols)
        };

        Ok(Self {
            conn,
            inner,
            collections,
        })
    }

    pub async fn init_file(&self, file: &FileInfo) -> Result<(), DBError> {
        tracing::info!("init file: {:?}", file.key);
        let iot_poc_file = IotPocFile::from(file);
        let doc = serde_json::to_value(iot_poc_file)?;

        if !self.file_exists(&file.key).await? {
            self.insert_document(
                &self.collections.files,
                doc,
                "file",
                InsertOptions::builder().build(),
            )
            .await
        } else {
            Ok(())
        }
    }

    pub async fn complete_file(&self, key: &str) -> Result<(), DBError> {
        let query = r#"UPDATE @key WITH { done: @done } IN @@collection"#;
        let aql = AqlQuery::builder()
            .query(query)
            .bind_var("@collection", FILES_COLLECTION)
            .bind_var("key", key)
            .bind_var("done", true)
            .build();

        self.inner
            .aql_query::<Vec<Value>>(aql)
            .await
            .map(|_| ())
            .map_err(DBError::from)
    }

    pub async fn get_done_file_keys(&self) -> Result<Vec<String>, DBError> {
        let query = r#"FOR f IN @@collection FILTER f.done == @done RETURN f._key"#;
        let aql = AqlQuery::builder()
            .query(query)
            .bind_var("@collection", FILES_COLLECTION)
            .bind_var("done", true)
            .build();

        let keys: Vec<String> = self.inner.aql_query(aql).await?;
        Ok(keys)
    }

    pub async fn get_file_retries(&self, key: &str) -> Result<u8, DBError> {
        let query = r#"FOR f in @@collection FILTER f._key == @key RETURN f.retries"#;
        let aql = AqlQuery::builder()
            .query(query)
            .bind_var("@collection", FILES_COLLECTION)
            .bind_var("key", key)
            .build();

        let retries: Vec<u8> = self.inner.aql_query(aql).await?;
        if retries.is_empty() {
            Ok(0)
        } else {
            Ok(retries[0])
        }
    }

    pub async fn file_exists(&self, key: &str) -> Result<bool, DBError> {
        let query = r#"FOR f IN @@collection FILTER f._key == @key RETURN f._key"#;
        let aql = AqlQuery::builder()
            .query(query)
            .bind_var("@collection", FILES_COLLECTION)
            .bind_var("key", key)
            .build();

        let keys: Vec<Option<String>> = self.inner.aql_query(aql).await?;
        Ok(!keys.is_empty())
    }

    pub async fn beacon_exists(&self, poc_id: &str) -> Result<bool, DBError> {
        let query = r#"FOR b IN @@collection FILTER b._key == @poc_id RETURN b.poc_id"#;
        let aql = AqlQuery::builder()
            .query(query)
            .bind_var("@collection", BEACON_COLLECTION)
            .bind_var("poc_id", poc_id)
            .build();

        let keys: Vec<Option<String>> = self.inner.aql_query(aql).await?;
        Ok(!keys.is_empty())
    }

    pub async fn increment_file_retry(&self, key: &str) -> Result<(), DBError> {
        let query = r#"UPDATE @key WITH { retries: OLD.retries + 1 } IN @@collection"#;
        let aql = AqlQuery::builder()
            .query(query)
            .bind_var("@collection", FILES_COLLECTION)
            .bind_var("key", key)
            .build();

        self.inner
            .aql_query::<Vec<Value>>(aql)
            .await
            .map(|_| ())
            .map_err(DBError::from)
    }

    async fn insert_document(
        &self,
        collection: &ArangoCollection,
        doc: serde_json::Value,
        doc_name: &str,
        options: InsertOptions,
    ) -> Result<(), DBError> {
        match collection.create_document(doc, options).await {
            Ok(_) => {
                tracing::debug!("successfully inserted {:?} document", doc_name);
                Ok(())
            }
            Err(ClientError::Arango(ae)) if [1210, 1200].contains(&ae.error_num()) => {
                tracing::debug!(
                    "error, doc: {:?}, {:?}: {:?}",
                    doc_name,
                    ae.error_num(),
                    ae.message()
                );
                Ok(())
            }
            Err(err) => Err(DBError::ArangoClientError(err)),
        }
    }

    async fn populate_hotspot(
        &self,
        hotspot_type: HotspotType,
        hotspot: Hotspot,
    ) -> Result<(), DBError> {
        let (query, poc_id) = match hotspot_type {
            HotspotType::Beacon => (
                unindent(
                    r#"
                UPSERT { _key: @pub_key }
                INSERT @hotspot
                UPDATE { poc_ids: UNION_DISTINCT(OLD.poc_ids, [@poc_id]),
                         last_updated_at: MAX([OLD.last_updated_at, DATE_NOW()]),
                         gain: @gain,
                         elevation: @elevation}
                IN @@collection"#,
                ),
                // NOTE: we only have a single poc_id for a beacon
                // The query takes care of adding it to the list of poc_ids
                Some(hotspot.poc_ids[0].clone()),
            ),
            HotspotType::Witness => (
                unindent(
                    r#"
                UPSERT { _key: @pub_key }
                INSERT @hotspot
                UPDATE { last_updated_at: MAX([OLD.last_updated_at, DATE_NOW()]), gain: @gain, elevation: @elevation }
                IN @@collection"#,
                ),
                None,
            ),
        };

        let mut aql_builder = AqlQuery::builder()
            .query(&query)
            .bind_var("@collection", HOTSPOT_COLLECTION)
            .bind_var("hotspot", serde_json::to_value(&hotspot)?)
            .bind_var("pub_key", hotspot._key.to_string())
            .bind_var("gain", hotspot.gain)
            .bind_var("elevation", hotspot.elevation);

        if let Some(poc_id) = poc_id {
            aql_builder = aql_builder.bind_var("poc_id", poc_id);
        }

        let aql = aql_builder.build();

        match self.inner.aql_query::<Vec<Value>>(aql).await {
            Ok(_) => {
                tracing::debug!("successfully populated {:?} hotspot", hotspot_type);
                Ok(())
            }
            Err(ClientError::Arango(ae)) if [1210, 1200].contains(&ae.error_num()) => {
                tracing::debug!(
                    "warning, collection: {:?}, hotspot_type: {:?}, {:?}: {:?}",
                    HOTSPOT_COLLECTION,
                    hotspot_type,
                    ae.error_num(),
                    ae.message()
                );
                Ok(())
            }
            Err(err) => Err(DBError::ArangoClientError(err)),
        }
    }

    async fn populate_beacon(&self, beacon: Beacon) -> Result<(), DBError> {
        if !self.beacon_exists(&beacon._key).await? {
            self.insert_document(
                &self.collections.beacons,
                serde_json::to_value(beacon)?,
                "beacon",
                InsertOptions::builder().build(),
            )
            .await
        } else {
            Ok(())
        }
    }

    async fn populate_edge(&self, edge: Edge) -> Result<(), DBError> {
        let witness_edge_key = edge._key;
        let distance = edge.distance;
        let beacon_pub_key = edge.beacon_pub_key;
        let witness_pub_key = edge.witness_pub_key;
        let witness_snr = edge.witness_snr;
        let witness_signal = edge.witness_signal;
        let ingest_latency = edge.ingest_latency;

        let query = unindent(
            r#"
             UPSERT { _key: @witness_edge_key }
             INSERT {
                 _key: @witness_edge_key,
                 _from: CONCAT_SEPARATOR("/", "hotspots", @beacon_pub_key),
                 _to: CONCAT_SEPARATOR("/", "hotspots", @witness_pub_key),
                 count: 1,
                 distance: @distance,
                 snr_hist: {@witness_snr: 1},
                 signal_hist: {@witness_signal: 1},
                 ingest_latency_hist: {@ingest_latency: 1},
                 last_updated_at: DATE_NOW()
             }
             UPDATE {
                 count: OLD.count + 1,
                 snr_hist: MERGE(OLD.snr_hist, {@witness_snr: OLD.snr_hist[@witness_snr] ? OLD.snr_hist[@witness_snr] + 1 : 1}),
                 signal_hist: MERGE(OLD.signal_hist, {@witness_signal: OLD.signal_hist[@witness_signal] ? OLD.signal_hist[@witness_signal] + 1 : 1}),
                 ingest_latency_hist: MERGE(OLD.ingest_latency_hist, {@ingest_latency: OLD.ingest_latency_hist[@ingest_latency] ? OLD.ingest_latency_hist[@ingest_latency] + 1 : 1}),
                 last_updated_at: MAX([OLD.last_updated_at, DATE_NOW()])
             }
             IN @@witness_edge_collection
             "#,
        );

        let aql = AqlQuery::builder()
            .query(&query)
            .bind_var("@witness_edge_collection", WITNESS_EDGE_COLLECTION)
            .bind_var("witness_edge_key", witness_edge_key)
            .bind_var("beacon_pub_key", beacon_pub_key.to_string())
            .bind_var("witness_pub_key", witness_pub_key.to_string())
            .bind_var("distance", distance)
            .bind_var("witness_snr", witness_snr)
            .bind_var("witness_signal", witness_signal)
            .bind_var("ingest_latency", ingest_latency)
            .build();

        tracing::debug!("upserting edge");
        self.inner
            .aql_query::<Vec<Value>>(aql)
            .await
            .map(|_| ())
            .map_err(DBError::from)
    }

    pub async fn populate_collections(&self, dec_msg: LoraPocV1) -> Result<Option<String>> {
        let iot_poc = IotPoc::try_from(dec_msg)?;

        // return early if no witnesses
        if iot_poc.selected_witnesses.is_empty() {
            tracing::debug!("ignored, no witnesses");
            return Ok(None);
        }

        let beacon = Beacon::try_from(&iot_poc)?;

        // insert beacon hotspot
        let poc_id = beacon.poc_id.clone();
        let beacon_hotspot = Hotspot::try_from(&beacon)?;
        self.populate_hotspot(HotspotType::Beacon, beacon_hotspot)
            .await?;

        for witness in beacon.witnesses.iter() {
            // insert witness hotspot
            let witness_hotspot = Hotspot::try_from(witness)?;
            self.populate_hotspot(HotspotType::Witness, witness_hotspot)
                .await?;
            // insert beacon -> witness edge
            let edge = Edge::new(&beacon, witness)?;
            self.populate_edge(edge).await?;
        }

        // insert beacon itself
        self.populate_beacon(beacon).await?;

        Ok(Some(poc_id))
    }
}

// Helper functions

async fn create_new_db_and_collections(inner: &ArangoDatabase) -> Result<Collections> {
    let collections = Collections {
        beacons: inner.create_collection(BEACON_COLLECTION).await?,
        hotspots: inner.create_collection(HOTSPOT_COLLECTION).await?,
        files: inner.create_collection(FILES_COLLECTION).await?,
        witnesses: inner
            .create_edge_collection(WITNESS_EDGE_COLLECTION)
            .await?,
    };

    create_indices(inner).await?;

    Ok(collections)
}

async fn use_existing_db_and_collections(inner: &ArangoDatabase) -> Result<Collections> {
    Ok(Collections {
        beacons: inner.collection(BEACON_COLLECTION).await?,
        hotspots: inner.collection(HOTSPOT_COLLECTION).await?,
        files: inner.collection(FILES_COLLECTION).await?,
        witnesses: inner.collection(WITNESS_EDGE_COLLECTION).await?,
    })
}

async fn create_indices(inner: &ArangoDatabase) -> Result<()> {
    create_beacon_indices(inner).await?;
    create_file_indices(inner).await?;
    create_witnes_indices(inner).await?;
    create_hotspot_indices(inner).await?;
    Ok(())
}

async fn create_file_indices(inner: &ArangoDatabase) -> Result<()> {
    let file_ts_skiplist_index = Index::builder()
        .name("file_ts")
        .fields(vec!["unix_ts".to_string()])
        .settings(IndexSettings::Skiplist {
            unique: false,
            sparse: true,
            deduplicate: false,
        })
        .build();
    let file_size_skiplist_index = Index::builder()
        .name("file_size")
        .fields(vec!["size".to_string()])
        .settings(IndexSettings::Skiplist {
            unique: false,
            sparse: true,
            deduplicate: false,
        })
        .build();
    inner
        .create_index(FILES_COLLECTION, &file_ts_skiplist_index)
        .await?;
    inner
        .create_index(FILES_COLLECTION, &file_size_skiplist_index)
        .await?;
    Ok(())
}

async fn create_beacon_indices(inner: &ArangoDatabase) -> Result<()> {
    let beacon_pub_key_hash_index = Index::builder()
        .name("beacon_pub_key")
        .fields(vec!["pub_key".to_string()])
        .settings(IndexSettings::Persistent {
            unique: false,
            sparse: false,
            deduplicate: false,
        })
        .build();
    let beacon_ingest_skiplist_index = Index::builder()
        .name("beacon_ingest_time")
        .fields(vec!["ingest_time_unix".to_string()])
        .settings(IndexSettings::Skiplist {
            unique: false,
            sparse: true,
            deduplicate: false,
        })
        .build();
    let beacon_geo_index = Index::builder()
        .name("beacon_geo_index")
        .fields(vec!["geo".to_string()])
        .settings(IndexSettings::Geo { geo_json: true })
        .build();
    inner
        .create_index(BEACON_COLLECTION, &beacon_pub_key_hash_index)
        .await?;
    inner
        .create_index(BEACON_COLLECTION, &beacon_ingest_skiplist_index)
        .await?;
    inner
        .create_index(BEACON_COLLECTION, &beacon_geo_index)
        .await?;
    Ok(())
}

async fn create_witnes_indices(inner: &ArangoDatabase) -> Result<()> {
    let witness_count_index = Index::builder()
        .name("witness_count")
        .fields(vec!["count".to_string()])
        .settings(IndexSettings::Persistent {
            unique: false,
            sparse: false,
            deduplicate: false,
        })
        .build();
    let beacon_witness_distance_index = Index::builder()
        .name("beacon_witness_distance")
        .fields(vec!["distance".to_string()])
        .settings(IndexSettings::Persistent {
            unique: false,
            sparse: false,
            deduplicate: false,
        })
        .build();
    inner
        .create_index(WITNESS_EDGE_COLLECTION, &witness_count_index)
        .await?;
    inner
        .create_index(WITNESS_EDGE_COLLECTION, &beacon_witness_distance_index)
        .await?;
    Ok(())
}

async fn create_hotspot_indices(inner: &ArangoDatabase) -> Result<()> {
    let hotspot_geo_index = Index::builder()
        .name("hotspot_geo_index")
        .fields(vec!["geo".to_string()])
        .settings(IndexSettings::Geo { geo_json: true })
        .build();
    inner
        .create_index(HOTSPOT_COLLECTION, &hotspot_geo_index)
        .await?;

    let hotspot_parent_geo_index = Index::builder()
        .name("hotspot_parent_geo_index")
        .fields(vec!["parent_geo".to_string()])
        .settings(IndexSettings::Geo { geo_json: true })
        .build();
    inner
        .create_index(HOTSPOT_COLLECTION, &hotspot_parent_geo_index)
        .await?;
    Ok(())
}

fn unindent(s: &str) -> String {
    s.lines()
        .map(|line| line.trim_start())
        .collect::<Vec<_>>()
        .join("\n")
}
