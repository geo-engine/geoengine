//! We needto run this in a separate process since it changes the global state of the GDAL driver

use gdal::{Dataset, DriverManager};
use geoengine_datatypes::test_data;
use geoengine_operators::util::gdal::register_gdal_drivers_from_list;
use std::collections::HashSet;

#[test]
fn test_gdal_driver_restriction() {
    register_gdal_drivers_from_list(HashSet::new());

    let dataset_path = test_data!("raster/geotiff_from_stream_compressed.tiff").to_path_buf();

    assert!(Dataset::open(&dataset_path).is_err());

    DriverManager::register_all();

    register_gdal_drivers_from_list(HashSet::from([
        "GTiff".to_string(),
        "CSV".to_string(),
        "GPKG".to_string(),
    ]));

    assert!(Dataset::open(&dataset_path).is_ok());

    // reset for other tests

    DriverManager::register_all();

    assert!(Dataset::open(&dataset_path).is_ok());
}
