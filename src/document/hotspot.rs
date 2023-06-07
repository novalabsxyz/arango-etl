use crate::document::{Beacon, Witness};
use angry_purple_tiger::AnimalName;
use anyhow::{Error, Result};
use geojson::Geometry;
use helium_crypto::PublicKeyBinary;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Hotspot {
    _key: PublicKeyBinary,
    location: Option<u64>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    geo: Option<Geometry>,
    name: String,
}

impl TryFrom<&Beacon> for Hotspot {
    type Error = Error;

    fn try_from(beacon: &Beacon) -> Result<Self> {
        let name = beacon
            .pub_key
            .to_string()
            .parse::<AnimalName>()?
            .to_string();
        Ok(Self {
            _key: beacon.pub_key.clone(),
            location: beacon.location,
            latitude: beacon.latitude,
            longitude: beacon.longitude,
            geo: beacon.geo.clone(),
            name,
        })
    }
}

impl TryFrom<&Witness> for Hotspot {
    type Error = Error;

    fn try_from(witness: &Witness) -> Result<Self> {
        let name = witness
            .pub_key
            .to_string()
            .parse::<AnimalName>()?
            .to_string();
        Ok(Self {
            _key: witness.pub_key.clone(),
            location: witness.location,
            latitude: witness.latitude,
            longitude: witness.longitude,
            geo: witness.geo.clone(),
            name,
        })
    }
}
