use anyhow::Result;
use geojson::Geometry;
use h3o::{geom::ToGeo, CellIndex, LatLng, Resolution};
use serde::{Deserialize, Serialize};

const PARENT_RESOLUTION: u8 = 5;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LocData {
    pub lat: Option<f64>,
    pub lng: Option<f64>,
    pub geo: Option<Geometry>,
}

impl LocData {
    fn new(lat: Option<f64>, lng: Option<f64>, geo: Option<Geometry>) -> Self {
        Self { lat, lng, geo }
    }

    pub fn from_h3(location: Option<u64>) -> Result<Self> {
        match location {
            Some(h3index) => {
                let cell = CellIndex::try_from(h3index)?;
                let latlng = LatLng::from(cell);
                let geom = cell.to_geojson()?;
                Ok(LocData::new(
                    Some(latlng.lat()),
                    Some(latlng.lng()),
                    Some(geom),
                ))
            }
            None => Ok(LocData::default()),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ParentLocData {
    pub loc: Option<u64>,
    pub lat: Option<f64>,
    pub lng: Option<f64>,
    pub geo: Option<Geometry>,
}

impl ParentLocData {
    fn new(loc: Option<u64>, lat: Option<f64>, lng: Option<f64>, geo: Option<Geometry>) -> Self {
        Self { loc, lat, lng, geo }
    }

    pub fn from_h3(location: Option<u64>) -> Result<ParentLocData> {
        match location {
            Some(h3index) => {
                let cell = CellIndex::try_from(h3index)?;
                match cell.parent(Resolution::try_from(PARENT_RESOLUTION)?) {
                    Some(parent) => {
                        let latlng = LatLng::from(parent);
                        let geom = parent.to_geojson()?;
                        Ok(ParentLocData::new(
                            Some(u64::from(parent)),
                            Some(latlng.lat()),
                            Some(latlng.lng()),
                            Some(geom),
                        ))
                    }
                    None => Ok(ParentLocData::default()),
                }
            }
            None => Ok(ParentLocData::default()),
        }
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
