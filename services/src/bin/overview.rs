use gdal::{raster::GDALDataType, Dataset, Driver, GeoTransform};

fn main() {
    let ds = Dataset::open("/home/michael/rust-projects/geoengine/upload/1e9edaa9-6623-4db3-bc48-5ec0920b575f/raster.tiff").unwrap();

    let band = ds.rasterband(1).unwrap();

    let overviews = band.overview_count().unwrap() as isize;

    let geo_transform = ds.geo_transform().unwrap();

    dbg!(&geo_transform);

    for i in 0..overviews {
        let ov = band.overview(i).unwrap();

        let driver = Driver::get("GTiff").unwrap();
        // TODO: generify band type
        let mut out_ds = driver
            .create_with_band_type::<f32, _>(
                format!("overview_{}.tiff", i),
                ov.x_size() as isize,
                ov.y_size() as isize,
                1,
            )
            .unwrap();

        out_ds.set_spatial_ref(&ds.spatial_ref().unwrap()).unwrap();

        let mut overview_geo_transform = geo_transform;
        overview_geo_transform[1] = geo_transform[1] * 2_i32.pow((i + 1) as u32) as f64;
        overview_geo_transform[5] = geo_transform[5] * 2_i32.pow((i + 1) as u32) as f64;

        dbg!(&overview_geo_transform);

        out_ds.set_geo_transform(&overview_geo_transform).unwrap();

        // TODO: generify band type
        out_ds
            .rasterband(1)
            .unwrap()
            .write::<f32>(
                (0, 0),
                (ov.x_size(), ov.y_size()),
                &ov.read_band_as().unwrap(),
            )
            .unwrap();
    }
}
