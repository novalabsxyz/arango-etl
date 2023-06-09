use crate::document::{
    get_name,
    loc_data::{LocData, ParentLocData},
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use file_store::iot_valid_poc::{IotPoc, IotVerifiedWitnessReport};
use geojson::Geometry;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_lora::{InvalidParticipantSide, InvalidReason, VerificationStatus};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Witness {
    pub ingest_time: DateTime<Utc>,
    pub ingest_time_unix: i64,
    pub str_location: Option<String>,
    pub location: Option<u64>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub geo: Option<Geometry>,
    pub parent_str_location: Option<String>,
    pub parent_location: Option<u64>,
    pub parent_latitude: Option<f64>,
    pub parent_longitude: Option<f64>,
    pub parent_geo: Option<Geometry>,
    pub gain: i32,
    pub elevation: i32,
    pub hex_scale: Option<f64>,
    pub reward_unit: Option<f64>,
    pub invalid_reason: InvalidReason,
    pub verification_status: VerificationStatus,
    pub participant_side: InvalidParticipantSide,
    pub pub_key: PublicKeyBinary,
    pub name: String,
    pub timestamp: DateTime<Utc>,
    pub tmst: u32,
    pub signal: i32,
    pub snr: i32,
    pub frequency: u64,
    pub selected: bool,
    pub distance: f64,
}

impl TryFrom<&IotVerifiedWitnessReport> for Witness {
    type Error = anyhow::Error;

    fn try_from(witness_report: &IotVerifiedWitnessReport) -> Result<Self> {
        let location = witness_report.location;
        let witness_ts = witness_report.received_timestamp;
        let witness_ingest_unix = witness_ts.timestamp_millis();
        let loc_data = LocData::from_h3(location)?;
        let parent_loc_data = ParentLocData::from_h3(location)?;
        let name = get_name(&witness_report.report.pub_key)?;

        Ok(Self {
            ingest_time: witness_ts,
            ingest_time_unix: witness_ingest_unix,
            location,
            str_location: loc_data.str_loc,
            latitude: loc_data.lat,
            longitude: loc_data.lng,
            geo: loc_data.geo,
            parent_str_location: parent_loc_data.str_loc,
            parent_location: parent_loc_data.loc,
            parent_latitude: parent_loc_data.lat,
            parent_longitude: parent_loc_data.lng,
            parent_geo: parent_loc_data.geo,
            name,
            hex_scale: witness_report.hex_scale.to_f64(),
            reward_unit: witness_report.reward_unit.to_f64(),
            pub_key: witness_report.report.pub_key.clone(),
            frequency: witness_report.report.frequency,
            timestamp: witness_report.report.timestamp,
            tmst: witness_report.report.tmst,
            gain: witness_report.gain,
            elevation: witness_report.elevation,
            verification_status: witness_report.status,
            invalid_reason: witness_report.invalid_reason,
            participant_side: witness_report.participant_side,
            signal: witness_report.report.signal,
            snr: witness_report.report.snr,
            selected: false, // default on init
            distance: 0.0,   // default on init
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Witnesses(Vec<Witness>);

impl Deref for Witnesses {
    type Target = Vec<Witness>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Witnesses {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TryFrom<&IotPoc> for Witnesses {
    type Error = anyhow::Error;

    fn try_from(iot_poc: &IotPoc) -> Result<Self> {
        // gather all witnesses
        let mut witnesses = vec![];
        for witness_report in iot_poc.selected_witnesses.iter() {
            let mut witness = Witness::try_from(witness_report)?;
            witness.selected = true;
            witnesses.push(witness);
        }

        for witness_report in iot_poc.unselected_witnesses.iter() {
            let mut witness = Witness::try_from(witness_report)?;
            witness.selected = false;
            witnesses.push(witness);
        }
        Ok(Self(witnesses))
    }
}
