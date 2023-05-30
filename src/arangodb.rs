use crate::settings::ArangoDBSettings;
use anyhow::Result;
use arangors::{
    document::options::InsertOptions,
    index::{Index, IndexSettings},
    uclient::reqwest::ReqwestClient,
    ClientError, Collection, Connection, Database,
};
use base64::{engine::general_purpose, Engine as _};
use file_store::iot_valid_poc::{IotPoc, IotValidBeaconReport, IotVerifiedWitnessReport};
use h3ron::{FromH3Index, H3Cell, ToCoordinate};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_lora::LoraPocV1;
use serde_json::json;
use vincenty_core::distance_from_points;

type ArangoCollection = Collection<ReqwestClient>;
type ArangoDatabase = Database<ReqwestClient>;

const BEACON_COLLECTION: &str = "beacons";
const HOTSPOT_COLLECTION: &str = "hotspots";
const WITNESS_EDGE_COLLECTION: &str = "witnesses";
const PROCESSED_FILES_COLLECTION: &str = "processed_files";

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
    pub processed_files: ArangoCollection,
}

impl DB {
    pub async fn from_settings(settings: &ArangoDBSettings) -> Result<Self> {
        let conn =
            Connection::establish_jwt(&settings.endpoint, &settings.user, &settings.password)
                .await?;
        tracing::debug!("databases: {:?}", conn.accessible_databases().await?);

        let existing_databases = conn.accessible_databases().await?;
        let db = if !existing_databases.contains_key(&settings.database) {
            let inner = conn.create_database(&settings.database).await?;
            let beacons = inner.create_collection(BEACON_COLLECTION).await?;
            let hotspots = inner.create_collection(HOTSPOT_COLLECTION).await?;
            let processed_files = inner.create_collection(PROCESSED_FILES_COLLECTION).await?;
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
                processed_files,
            }
        } else {
            tracing::debug!("reusing existing database {:?}", &settings.database);
            let inner = conn.db(&settings.database).await?;
            let beacons = inner.collection(BEACON_COLLECTION).await?;
            tracing::debug!("reusing beacons collection from {:?}", &settings.database);
            let hotspots = inner.collection(HOTSPOT_COLLECTION).await?;
            tracing::debug!("reusing hotspots collection from {:?}", &settings.database);
            let processed_files = inner.collection(PROCESSED_FILES_COLLECTION).await?;
            tracing::debug!("reusing hotspots collection from {:?}", &settings.database);
            let witnesses = inner.collection(WITNESS_EDGE_COLLECTION).await?;
            tracing::debug!(
                "reusing witnesses edge collection from {:?}",
                &settings.database
            );
            Self {
                conn,
                inner,
                beacons,
                hotspots,
                witnesses,
                processed_files,
            }
        };
        Ok(db)
    }

    async fn populate_hotspot(
        &self,
        hotspot_pub_key: PublicKeyBinary,
        location: Option<u64>,
    ) -> Result<()> {
        let (lat, lng) = lat_lng_from_h3_index(location)?;

        let hotspot_json = json!({
            "_key": hotspot_pub_key,
            "pub_key": hotspot_pub_key,
            "location": location,
            "latitude": lat,
            "longitude": lng,
        });
        self.insert_document(&self.hotspots, hotspot_json, "hotspot")
            .await?;
        Ok(())
    }

    async fn populate_beacon(&self, beacon_json: serde_json::Value) -> Result<()> {
        self.insert_document(&self.beacons, beacon_json, "beacon")
            .await?;
        Ok(())
    }

    async fn insert_document(
        &self,
        collection: &ArangoCollection,
        doc: serde_json::Value,
        doc_name: &str,
    ) -> Result<()> {
        match collection
            .create_document(doc, InsertOptions::builder().build())
            .await
        {
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

    async fn populate_witness(
        &self,
        beacon_report: IotValidBeaconReport,
        witness: IotVerifiedWitnessReport,
        selected: bool,
    ) -> Result<serde_json::Value> {
        let received_ts = witness.received_timestamp;
        let witness_pub_key = witness.report.pub_key;
        let witness_loc = witness.location;
        let (witness_lat, witness_lng) = lat_lng_from_h3_index(witness_loc)?;
        let witness_snr = witness.report.snr;
        let witness_signal = witness.report.signal;
        let witness_ingest_unix = received_ts.timestamp_millis();
        let witness_json = json!({
            "ingest_time": received_ts,
            "ingest_time_unix": witness_ingest_unix,
            "location": witness_loc,
            "latitude": witness_lat,
            "longitude": witness_lng,
            "hex_scale": witness.hex_scale,
            "reward_unit": witness.reward_unit,
            "verification_status": witness.status,
            "invalid_reason": witness.invalid_reason,
            "participant_side": witness.participant_side,
            "pub_key": witness_pub_key,
            "timestamp": witness.report.timestamp,
            "tmst": witness.report.tmst,
            "frequency": witness.report.frequency,
            "signal": witness_signal,
            "snr": witness_snr,
            "gain": witness.gain,
            "elevation": witness.elevation,
            "selected": selected
        });

        self.populate_hotspot(witness_pub_key.clone(), witness_loc)
            .await?;

        let beacon_ts = beacon_report.received_timestamp;
        let beacon_ingest_unix = beacon_ts.timestamp_millis();
        let beacon_loc = beacon_report.location;
        let (beacon_lat, beacon_lng) = lat_lng_from_h3_index(beacon_loc)?;
        let beacon_pub_key = beacon_report.report.pub_key;

        let distance =
            calc_distance(beacon_lat, beacon_lng, witness_lat, witness_lng)?.unwrap_or_default();

        let witness_edge_key = witness_edge_key(beacon_loc, witness_loc);
        let ingest_latency = witness_ingest_unix
            .checked_sub(beacon_ingest_unix)
            .unwrap_or_default();

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
        Ok(witness_json)
    }

    pub async fn populate_collections(&self, dec_msg: LoraPocV1) -> Result<()> {
        let iot_poc = IotPoc::try_from(dec_msg)?;
        let enc_poc_id = general_purpose::URL_SAFE_NO_PAD.encode(iot_poc.poc_id);
        let beacon_loc = iot_poc.beacon_report.location;

        if iot_poc.selected_witnesses.is_empty() {
            return Ok(());
        }

        // populate the beaconer in hotspots collection
        self.populate_hotspot(iot_poc.beacon_report.report.pub_key.clone(), beacon_loc)
            .await?;

        // gather all witnesses
        let mut witnesses = vec![];
        for witness in iot_poc.selected_witnesses {
            let selected_witness_json = self
                .populate_witness(iot_poc.beacon_report.clone(), witness, true)
                .await?;
            witnesses.push(selected_witness_json);
        }

        for witness in iot_poc.unselected_witnesses {
            let unselected_witness_json = self
                .populate_witness(iot_poc.beacon_report.clone(), witness, false)
                .await?;
            witnesses.push(unselected_witness_json);
        }

        let beacon_ts = iot_poc.beacon_report.received_timestamp;
        let beacon_ingest_unix = beacon_ts.timestamp_millis();
        let beacon_loc = iot_poc.beacon_report.location;
        let (beacon_lat, beacon_lng) = lat_lng_from_h3_index(beacon_loc)?;
        // populate the beacons collection
        let beacon_json = json!({
            "_key": enc_poc_id,
            "poc_id": enc_poc_id,
            "ingest_time": beacon_ts,
            "ingest_time_unix": beacon_ingest_unix,
            "location": beacon_loc,
            "latitude": beacon_lat,
            "longitude": beacon_lng,
            "hex_scale": iot_poc.beacon_report.hex_scale,
            "reward_unit": iot_poc.beacon_report.reward_unit,
            "pub_key": iot_poc.beacon_report.report.pub_key.clone(),
            "frequency": iot_poc.beacon_report.report.frequency,
            "channel": iot_poc.beacon_report.report.channel,
            "tx_power": iot_poc.beacon_report.report.tx_power,
            "timestamp": iot_poc.beacon_report.report.timestamp,
            "tmst": iot_poc.beacon_report.report.tmst,
            "gain": iot_poc.beacon_report.gain,
            "elevation": iot_poc.beacon_report.elevation,
            "witnesses": witnesses
        });
        self.populate_beacon(beacon_json).await?;

        tracing::debug!(
            "stored beacon, poc_id: {:?}, # witnesses: {:?}",
            enc_poc_id,
            witnesses.len()
        );

        Ok(())
    }
}

fn unindent(s: String) -> String {
    s.lines()
        .map(|line| line.trim_start())
        .collect::<Vec<_>>()
        .join("\n")
}

fn lat_lng_from_h3_index(location: Option<u64>) -> Result<(Option<f64>, Option<f64>)> {
    match location {
        Some(h3index) => {
            let cell = H3Cell::from_h3index(h3index);
            let coords = cell.to_coordinate()?;
            let (lat, lng) = coords.x_y();
            Ok((Some(lat), Some(lng)))
        }
        None => Ok((None, None)),
    }
}

fn calc_distance(
    beacon_lat: Option<f64>,
    beacon_lng: Option<f64>,
    witness_lat: Option<f64>,
    witness_lng: Option<f64>,
) -> Result<Option<f64>> {
    match (beacon_lat, beacon_lng, witness_lat, witness_lng) {
        (Some(x1), Some(y1), Some(x2), Some(y2)) => Ok(Some(distance_from_points(x1, y1, x2, y2)?)),
        _ => Ok(None),
    }
}

fn witness_edge_key(beacon_loc: Option<u64>, witness_loc: Option<u64>) -> String {
    match (beacon_loc, witness_loc) {
        (Some(b_loc), Some(w_loc)) => format!("beacon_{:?}_witness_{:?}", b_loc, w_loc),
        (Some(b_loc), None) => format!("beacon_{:?}_witness_unknown", b_loc),
        (None, Some(w_loc)) => format!("beacon_unknown_witness_{:?}", w_loc),
        (None, None) => "beacon_unknown_witness_unknown".to_string(),
    }
}
