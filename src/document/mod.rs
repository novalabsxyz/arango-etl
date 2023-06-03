pub mod beacon;
pub mod edge;
pub mod hotspot;
pub mod witness;

pub use beacon::Beacon;
pub use edge::Edge;
pub use hotspot::Hotspot;
pub use witness::{Witness, Witnesses};

use anyhow::Result;
use h3o::{CellIndex, LatLng};

pub fn lat_lng_from_h3_index(location: Option<u64>) -> Result<(Option<f64>, Option<f64>)> {
    match location {
        Some(h3index) => {
            let cell = CellIndex::try_from(h3index)?;
            let latlng = LatLng::from(cell);
            Ok((Some(latlng.lat()), Some(latlng.lng())))
        }
        None => Ok((None, None)),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use h3o::Resolution;

    #[test]
    fn known() {
        let coord = LatLng::new(37.769377, -122.388903).unwrap();
        println!("coord: {:?}", coord);
        let cell_from_coord = coord.to_cell(Resolution::Twelve);
        println!("cell_from_coord: {:?}", cell_from_coord);
        let cell_u64 = u64::from(cell_from_coord);
        println!("cell_u64: {:?}", cell_u64);
        let cell_str = cell_from_coord.to_string();
        println!("cell_str: {:?}", cell_str);

        let cell_from_u64 = CellIndex::try_from(cell_u64).unwrap();
        println!("cell_from_u64: {:?}", cell_from_u64);
        let cell_coords = LatLng::from(cell_from_u64);
        println!("cell_coords: {:?}", cell_coords);
        let cell_from_u64_str = cell_from_u64.to_string();
        println!("cell_from_u64_str: {:?}", cell_from_u64_str);

        assert_eq!(cell_from_coord, cell_from_u64);
        assert_eq!(cell_str, cell_from_u64_str);
        // The latlng match upto 4 decimals (not a huge deal as long as the above asserts work)
        // assert_eq!(coord, cell_coords);
    }
}
