pub mod beacon;
pub mod edge;
pub mod hotspot;
pub mod iot_poc_file;
pub mod loc_data;
pub mod witness;

pub use beacon::Beacon;
pub use edge::Edge;
pub use hotspot::Hotspot;
pub use witness::{Witness, Witnesses};

use angry_purple_tiger::AnimalName;
use anyhow::Result;
use helium_crypto::PublicKeyBinary;

pub const BEACON_COLLECTION: &str = "beacons";
pub const HOTSPOT_COLLECTION: &str = "hotspots";
pub const WITNESS_EDGE_COLLECTION: &str = "witnesses";
pub const FILES_COLLECTION: &str = "files";

pub fn get_name(pub_key: &PublicKeyBinary) -> Result<String> {
    Ok(pub_key.to_string().parse::<AnimalName>()?.to_string())
}
