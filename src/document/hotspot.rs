use crate::document::{get_name, Beacon, Witness};
use anyhow::{Error, Result};
use chrono::Utc;
use geojson::Geometry;
use helium_crypto::PublicKeyBinary;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Hotspot {
    pub _key: PublicKeyBinary,
    pub poc_ids: Vec<String>,
    str_location: Option<String>,
    location: Option<u64>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    geo: Option<Geometry>,
    parent_str_location: Option<String>,
    parent_location: Option<u64>,
    parent_latitude: Option<f64>,
    parent_longitude: Option<f64>,
    parent_geo: Option<Geometry>,
    name: String,
    last_updated_at: Option<u64>,
    pub gain: Option<i32>,
    pub elevation: Option<i32>,
}

impl TryFrom<&Beacon> for Hotspot {
    type Error = Error;

    fn try_from(beacon: &Beacon) -> Result<Self> {
        let name = get_name(&beacon.pub_key)?;
        Ok(Self {
            _key: beacon.pub_key.clone(),
            str_location: beacon.str_location.clone(),
            location: beacon.location,
            latitude: beacon.latitude,
            longitude: beacon.longitude,
            geo: beacon.geo.clone(),
            parent_str_location: beacon.parent_str_location.clone(),
            parent_location: beacon.parent_location,
            parent_latitude: beacon.parent_latitude,
            parent_longitude: beacon.parent_longitude,
            parent_geo: beacon.parent_geo.clone(),
            name,
            poc_ids: vec![beacon.poc_id.clone()],
            last_updated_at: Some(Utc::now().timestamp_millis() as u64),
            gain: Some(beacon.gain),
            elevation: Some(beacon.elevation),
        })
    }
}

impl TryFrom<&Witness> for Hotspot {
    type Error = Error;

    fn try_from(witness: &Witness) -> Result<Self> {
        let name = get_name(&witness.pub_key)?;
        Ok(Self {
            _key: witness.pub_key.clone(),
            str_location: witness.str_location.clone(),
            location: witness.location,
            latitude: witness.latitude,
            longitude: witness.longitude,
            geo: witness.geo.clone(),
            parent_str_location: witness.parent_str_location.clone(),
            parent_location: witness.parent_location,
            parent_latitude: witness.parent_latitude,
            parent_longitude: witness.parent_longitude,
            parent_geo: witness.parent_geo.clone(),
            name,
            poc_ids: vec![],
            last_updated_at: Some(Utc::now().timestamp_millis() as u64),
            gain: Some(witness.gain),
            elevation: Some(witness.elevation),
        })
    }
}
