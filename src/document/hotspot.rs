use crate::document::{get_name, Beacon, Witness};
use anyhow::{Error, Result};
use geojson::Geometry;
use helium_crypto::PublicKeyBinary;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Hotspot {
    pub _key: PublicKeyBinary,
    pub poc_ids: Vec<String>,
    location: Option<u64>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    geo: Option<Geometry>,
    parent_location: Option<u64>,
    parent_latitude: Option<f64>,
    parent_longitude: Option<f64>,
    parent_geo: Option<Geometry>,
    name: String,
}

impl TryFrom<&Beacon> for Hotspot {
    type Error = Error;

    fn try_from(beacon: &Beacon) -> Result<Self> {
        let name = get_name(&beacon.pub_key)?;
        Ok(Self {
            _key: beacon.pub_key.clone(),
            location: beacon.location,
            latitude: beacon.latitude,
            longitude: beacon.longitude,
            geo: beacon.geo.clone(),
            parent_location: beacon.parent_location,
            parent_latitude: beacon.parent_latitude,
            parent_longitude: beacon.parent_longitude,
            parent_geo: beacon.parent_geo.clone(),
            name,
            poc_ids: vec![beacon.poc_id.clone()],
        })
    }
}

impl TryFrom<&Witness> for Hotspot {
    type Error = Error;

    fn try_from(witness: &Witness) -> Result<Self> {
        let name = get_name(&witness.pub_key)?;
        Ok(Self {
            _key: witness.pub_key.clone(),
            location: witness.location,
            latitude: witness.latitude,
            longitude: witness.longitude,
            geo: witness.geo.clone(),
            parent_location: witness.parent_location,
            parent_latitude: witness.parent_latitude,
            parent_longitude: witness.parent_longitude,
            parent_geo: witness.parent_geo.clone(),
            name,
            poc_ids: vec![],
        })
    }
}
