pub mod beacon;
pub mod edge;
pub mod hotspot;
pub mod witness;

pub use beacon::Beacon;
pub use edge::Edge;
pub use hotspot::Hotspot;
pub use witness::{Witness, Witnesses};

use anyhow::Result;
use h3ron::{FromH3Index, H3Cell, ToCoordinate};
pub fn lat_lng_from_h3_index(location: Option<u64>) -> Result<(Option<f64>, Option<f64>)> {
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
