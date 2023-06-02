use crate::document::{Beacon, Witness};
use anyhow::Result;
use helium_crypto::PublicKeyBinary;
use serde::{Deserialize, Serialize};
use vincenty_core::distance_from_points;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Edge {
    pub _key: String,
    pub beacon_pub_key: PublicKeyBinary,
    pub witness_pub_key: PublicKeyBinary,
    pub distance: f64,
    pub witness_snr: i32,
    pub witness_signal: i32,
    pub ingest_latency: i64,
}

impl Edge {
    pub fn new(beacon: &Beacon, witness: &Witness) -> Result<Self> {
        let _key = witness_edge_key(beacon.location, witness.location);
        let distance = calc_distance(
            beacon.latitude,
            beacon.longitude,
            witness.latitude,
            witness.longitude,
        )?
        .unwrap_or_default();
        let ingest_latency = witness
            .ingest_time_unix
            .checked_sub(beacon.ingest_time_unix)
            .unwrap_or_default();
        Ok(Self {
            _key,
            beacon_pub_key: beacon.pub_key.clone(),
            witness_pub_key: witness.pub_key.clone(),
            witness_snr: witness.snr,
            witness_signal: witness.signal,
            distance,
            ingest_latency,
        })
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
