use crate::document::{Beacon, Witness};
use geojson::Geometry;
use helium_crypto::PublicKeyBinary;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Hotspot {
    _key: PublicKeyBinary,
    pub_key: PublicKeyBinary,
    location: Option<u64>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    geo: Option<Geometry>,
}

impl From<&Beacon> for Hotspot {
    fn from(beacon: &Beacon) -> Self {
        Self {
            _key: beacon.pub_key.clone(),
            pub_key: beacon.pub_key.clone(),
            location: beacon.location,
            latitude: beacon.latitude,
            longitude: beacon.longitude,
            geo: beacon.geo.clone(),
        }
    }
}

impl From<&Witness> for Hotspot {
    fn from(witness: &Witness) -> Self {
        Self {
            _key: witness.pub_key.clone(),
            pub_key: witness.pub_key.clone(),
            location: witness.location,
            latitude: witness.latitude,
            longitude: witness.longitude,
            geo: witness.geo.clone(),
        }
    }
}
