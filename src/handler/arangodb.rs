use crate::{
    document::{Beacon, Edge, Hotspot},
    settings::ArangoDBSettings,
};
use anyhow::Result;
use arangors::{
    document::options::{InsertOptions, UpdateOptions},
    index::{Index, IndexSettings},
    uclient::reqwest::ReqwestClient,
    ClientError, Collection, Connection, Database, Document,
};
use file_store::{iot_valid_poc::IotPoc, FileInfo};
use helium_proto::services::poc_lora::LoraPocV1;
use serde_json::json;

type ArangoCollection = Collection<ReqwestClient>;
type ArangoDatabase = Database<ReqwestClient>;

const BEACON_COLLECTION: &str = "beacons";
const HOTSPOT_COLLECTION: &str = "hotspots";
const WITNESS_EDGE_COLLECTION: &str = "witnesses";
const FILES_COLLECTION: &str = "files";

#[derive(Debug)]
pub struct DB {
    pub conn: Connection,
    pub inner: ArangoDatabase,
    // This collection will store beacon json (including a list of witnesses)
    pub beacons: ArangoCollection,
    // This collection will just store all the pubkeys
    pub hotspots: ArangoCollection,
    // This collection will store all beacon-witness edges
    pub witnesses: ArangoCollection,
    // This collection will store names of all processed iot-poc files
    pub files: ArangoCollection,
}

impl DB {
    pub async fn from_settings(settings: &ArangoDBSettings) -> Result<Self> {
        let conn =
            Connection::establish_jwt(&settings.endpoint, &settings.user, &settings.password)
                .await?;

        let existing_databases = conn.accessible_databases().await?;
        let db = if !existing_databases.contains_key(&settings.database) {
            let inner = conn.create_database(&settings.database).await?;
            let beacons = inner.create_collection(BEACON_COLLECTION).await?;
            let hotspots = inner.create_collection(HOTSPOT_COLLECTION).await?;
            let files = inner.create_collection(FILES_COLLECTION).await?;
            let witnesses = inner
                .create_edge_collection(WITNESS_EDGE_COLLECTION)
                .await?;

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
                .create_index(BEACON_COLLECTION, &beacon_pub_key_hash_index)
                .await?;
            inner
                .create_index(BEACON_COLLECTION, &beacon_ingest_skiplist_index)
                .await?;
            inner
                .create_index(WITNESS_EDGE_COLLECTION, &witness_count_index)
                .await?;
            inner
                .create_index(WITNESS_EDGE_COLLECTION, &beacon_witness_distance_index)
                .await?;

            Self {
                conn,
                inner,
                beacons,
                hotspots,
                witnesses,
                files,
            }
        } else {
            let inner = conn.db(&settings.database).await?;
            let beacons = inner.collection(BEACON_COLLECTION).await?;
            let hotspots = inner.collection(HOTSPOT_COLLECTION).await?;
            let files = inner.collection(FILES_COLLECTION).await?;
            let witnesses = inner.collection(WITNESS_EDGE_COLLECTION).await?;
            Self {
                conn,
                inner,
                beacons,
                hotspots,
                witnesses,
                files,
            }
        };
        Ok(db)
    }

    pub async fn init_files(&self, files: &Vec<FileInfo>) -> Result<()> {
        for file in files {
            let doc =
                json!({"_key": file.key, "size": file.size, "ts": file.timestamp, "done": false });
            self.insert_document(&self.files, doc, "file", InsertOptions::builder().build())
                .await?;
            tracing::info!("init file: {:?}", file.key);
        }

        Ok(())
    }

    pub async fn complete_files(&self, keys: &[String]) -> Result<()> {
        for key in keys {
            let update_doc = json!({"done": true});
            self.files
                .update_document(
                    key,
                    update_doc,
                    UpdateOptions::builder().merge_objects(true).build(),
                )
                .await?;
            tracing::info!("completed file: {:?}", key);
        }

        Ok(())
    }

    pub async fn get_file(&self, key: &str) -> Result<Option<Document<serde_json::Value>>> {
        match self.files.document(key).await {
            Ok(doc) => Ok(Some(doc)),
            Err(err) => match err {
                ClientError::Arango(ae) if ae.error_num() == 1202 => Ok(None),
                _ => Err(err.into()),
            },
        }
    }

    async fn insert_document(
        &self,
        collection: &ArangoCollection,
        doc: serde_json::Value,
        doc_name: &str,
        options: InsertOptions,
    ) -> Result<()> {
        match collection.create_document(doc, options).await {
            Ok(_) => {
                tracing::debug!("successfully inserted {:?} document", doc_name);
                Ok(())
            }
            Err(err) => match err {
                ClientError::Arango(ae) if ae.error_num() == 1210 => {
                    tracing::debug!("skipping already inserted {:?} doc", doc_name);
                    Ok(())
                }
                _ => Err(err.into()),
            },
        }
    }

    async fn populate_hotspot(&self, hotspot: Hotspot) -> Result<()> {
        self.insert_document(
            &self.hotspots,
            serde_json::to_value(hotspot)?,
            "hotspot",
            InsertOptions::builder().build(),
        )
        .await?;
        Ok(())
    }

    async fn populate_beacon(&self, beacon: Beacon) -> Result<()> {
        self.insert_document(
            &self.beacons,
            serde_json::to_value(beacon)?,
            "beacon",
            InsertOptions::builder().build(),
        )
        .await?;
        Ok(())
    }

    async fn populate_edge(&self, edge: Edge) -> Result<()> {
        let witness_edge_key = edge._key;
        let distance = edge.distance;
        let beacon_pub_key = edge.beacon_pub_key;
        let witness_pub_key = edge.witness_pub_key;
        let witness_snr = edge.witness_snr;
        let witness_signal = edge.witness_signal;
        let ingest_latency = edge.ingest_latency;
        let query = unindent(format!(
            r#"
             UPSERT {{ _key: "{witness_edge_key}" }}
             INSERT {{
                 _key: "{witness_edge_key}",
                 _from: "{HOTSPOT_COLLECTION}/{beacon_pub_key}",
                 _to: "{HOTSPOT_COLLECTION}/{witness_pub_key}",
                 count: 1,
                 distance: {distance},
                 snr_hist: {{"{witness_snr}": 1}},
                 signal_hist: {{"{witness_signal}": 1}},
                 ingest_latency_hist: {{"{ingest_latency}": 1}}
             }}
             UPDATE {{
                 count: OLD.count + 1,
                 snr_hist: MERGE(OLD.snr_hist, {{"{witness_snr}": OLD.snr_hist["{witness_snr}"] ? OLD.snr_hist["{witness_snr}"] + 1 : 1}}),
                 signal_hist: MERGE(OLD.signal_hist, {{"{witness_signal}": OLD.signal_hist["{witness_signal}"] ? OLD.signal_hist["{witness_signal}"] + 1 : 1}}),
                 ingest_latency_hist: MERGE(OLD.ingest_latency_hist, {{"{ingest_latency}": OLD.ingest_latency_hist["{ingest_latency}"] ? OLD.ingest_latency_hist["{ingest_latency}"] + 1 : 1}})
             }}
             IN {WITNESS_EDGE_COLLECTION}
             "#
        ));

        match self.inner.aql_str::<Vec<serde_json::Value>>(&query).await {
            Ok(_) => tracing::debug!("successfully upserted edge"),
            Err(e) => tracing::error!("error: {:?}", e),
        }
        Ok(())
    }

    pub async fn populate_collections(&self, dec_msg: LoraPocV1) -> Result<Option<String>> {
        let iot_poc = IotPoc::try_from(dec_msg)?;

        // return early if no witnesses
        if iot_poc.selected_witnesses.is_empty() {
            tracing::debug!("ignored, no witnesses");
            return Ok(None);
        }

        let beacon = Beacon::try_from(&iot_poc)?;
        let poc_id = beacon.poc_id.clone();
        let beacon_hotspot = Hotspot::from(&beacon);
        // insert beacon hotspot
        self.populate_hotspot(beacon_hotspot).await?;

        for witness in beacon.witnesses.iter() {
            // insert witness hotspot
            let witness_hotspot = Hotspot::try_from(witness)?;
            self.populate_hotspot(witness_hotspot).await?;
            // insert beacon -> witness edge
            let edge = Edge::new(&beacon, witness)?;
            self.populate_edge(edge).await?;
        }

        // insert beacon itself
        self.populate_beacon(beacon).await?;

        Ok(Some(poc_id))
    }
}

fn unindent(s: String) -> String {
    s.lines()
        .map(|line| line.trim_start())
        .collect::<Vec<_>>()
        .join("\n")
}