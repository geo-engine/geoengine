#![feature(bench_black_box)]

use gdal::config::set_thread_local_config_option;
use gdal::{Dataset, DatasetOptions, GdalOpenFlags};
use geoengine_datatypes::test_data;
use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::{path::Path, time::Instant};

fn main() {
    let iterations = 100;
    let path = test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF").to_path_buf();

    for _ in 0..10 {
        let mut time = Instant::now();

        load_data_reopen_dataset(&path, iterations);

        println!("load_data_reopen_dataset: {:?}", time.elapsed());

        time = Instant::now();

        load_data_reuse_dataset(&path, iterations);

        println!("load_data_reuse_dataset: {:?}", time.elapsed());
    }
}

fn load_data_reuse_dataset(path: &Path, iterations: usize) {
    let dataset = Arc::new(Mutex::new(gdal_open(path)));

    for _ in 0..iterations {
        let dataset = dataset.clone();

        let _ = black_box(gdal_read(&dataset.lock().unwrap()));
    }
}

fn load_data_reopen_dataset(path: &Path, iterations: usize) {
    for _ in 0..iterations {
        let dataset = gdal_open(path);

        let _ = black_box(gdal_read(&dataset));
    }
}

fn gdal_open(path: &Path) -> Dataset {
    set_thread_local_config_option("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR").unwrap();

    Dataset::open_ex(
        path,
        DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_RASTER | GdalOpenFlags::GDAL_OF_READONLY,
            allowed_drivers: Some(&["GTiff"]),
            open_options: Some(&[]),
            sibling_files: None,
        },
    )
    .unwrap()
}

fn gdal_read(dataset: &Dataset) -> Vec<u8> {
    set_thread_local_config_option("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR").unwrap();

    let buffer = dataset
        .rasterband(1)
        .unwrap()
        .read_as::<u8>((0, 0), (512, 512), (512, 512), None)
        .unwrap();
    buffer.data
}
