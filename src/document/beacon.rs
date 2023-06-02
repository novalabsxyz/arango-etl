use crate::document::{lat_lng_from_h3_index, Witnesses};
use anyhow::Result;
use base64::{engine::general_purpose, Engine as _};
use chrono::{DateTime, Utc};
use file_store::iot_valid_poc::IotPoc;
use helium_crypto::PublicKeyBinary;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Beacon {
    pub _key: String,
    pub poc_id: String,
    pub ingest_time: DateTime<Utc>,
    pub ingest_time_unix: i64,
    pub location: Option<u64>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub gain: i32,
    pub elevation: i32,
    pub hex_scale: Option<f64>,
    pub reward_unit: Option<f64>,
    pub pub_key: PublicKeyBinary,
    pub frequency: u64,
    pub channel: i32,
    pub tx_power: i32,
    pub timestamp: DateTime<Utc>,
    pub tmst: u32,
    pub witnesses: Witnesses,
}

impl TryFrom<&IotPoc> for Beacon {
    type Error = anyhow::Error;

    fn try_from(iot_poc: &IotPoc) -> Result<Self> {
        let beacon_report = &iot_poc.beacon_report;
        let enc_poc_id = general_purpose::URL_SAFE_NO_PAD.encode(iot_poc.poc_id.clone());
        let beacon_loc = beacon_report.location;
        let beacon_ts = beacon_report.received_timestamp;
        let beacon_ingest_unix = beacon_ts.timestamp_millis();
        let (beacon_lat, beacon_lng) = lat_lng_from_h3_index(beacon_loc)?;

        Ok(Self {
            _key: enc_poc_id.clone(),
            poc_id: enc_poc_id,
            ingest_time: beacon_ts,
            ingest_time_unix: beacon_ingest_unix,
            location: beacon_loc,
            latitude: beacon_lat,
            longitude: beacon_lng,
            hex_scale: beacon_report.hex_scale.to_f64(),
            reward_unit: beacon_report.reward_unit.to_f64(),
            pub_key: beacon_report.report.pub_key.clone(),
            frequency: beacon_report.report.frequency,
            channel: beacon_report.report.channel,
            tx_power: beacon_report.report.tx_power,
            timestamp: beacon_report.report.timestamp,
            tmst: beacon_report.report.tmst,
            gain: beacon_report.gain,
            elevation: beacon_report.elevation,
            witnesses: Witnesses::try_from(iot_poc)?,
        })
    }
}
