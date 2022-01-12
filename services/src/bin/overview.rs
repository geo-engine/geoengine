use gdal::{Dataset, Driver};

fn main() {
    let ds = Dataset::open("upload/758a6ea4-2578-4aae-ae69-9f181542b3bc/raster.tiff").unwrap();

    let band = ds.rasterband(1).unwrap();

    let overviews = band.overview_count().unwrap() as isize;

    for i in 0..overviews {
        let ov = band.overview(i).unwrap();

        let driver = Driver::get("GTiff").unwrap();
        let out_ds = driver
            .create(
                format!("raster_{}.tiff", i),
                ov.x_size() as isize,
                ov.y_size() as isize,
                1,
            )
            .unwrap();

        out_ds
            .rasterband(1)
            .unwrap()
            .write::<u8>(
                (0, 0),
                (ov.x_size(), ov.y_size()),
                &ov.read_band_as().unwrap(),
            )
            .unwrap();
    }
}
