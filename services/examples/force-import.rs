use gdal::vector::{Defn, Feature, FieldDefn, LayerOptions, OGRFieldType};
use gdal::{Dataset as GdalDataset, DriverManager};
use std::path::Path;

/// Creates a tile index for the given datasets and writes it to a GeoJSON file.
/// uses the SRS from the first dataset
fn gdaltindex(datasets: &[&Path], gti_file: &Path, tile_index_file: &Path) {
    let spatial_ref = {
        let first_dataset = GdalDataset::open(datasets.get(0).expect("datasets must not be empty"))
            .expect("Failed to open dataset");
        first_dataset
            .spatial_ref()
            .expect("Failed to get spatial reference")
    };

    let driver =
        DriverManager::get_driver_by_name("GeoJSON").expect("Failed to get GeoJSON driver");
    let mut vector_ds = driver
        .create_vector_only(tile_index_file)
        .expect("Failed to create vector dataset");

    let layer = vector_ds
        .create_layer(LayerOptions {
            name: "tile_index",
            srs: Some(&spatial_ref),
            ty: gdal::vector::OGRwkbGeometryType::wkbPolygon,
            options: None,
        })
        .expect("Failed to create layer");

    let field_defn =
        FieldDefn::new("location", OGRFieldType::OFTString).expect("Failed to create field defn");
    // field_defn.set_width(80);
    field_defn
        .add_to_layer(&layer)
        .expect("Failed to add field to layer");

    let defn = Defn::from_layer(&layer);

    let location_idx = defn
        .field_index("location")
        .expect("Failed to get field index");

    for dataset_path in datasets {
        let dataset = GdalDataset::open(dataset_path).expect("Failed to open dataset");
        let geo_transform = dataset
            .geo_transform()
            .expect("Failed to get geo-transform");
        let raster_size = dataset.raster_size();

        // TODO: get bbox from gdal?
        let min_x = geo_transform[0];
        let max_y = geo_transform[3];
        let max_x = min_x + geo_transform[1] * raster_size.0 as f64;
        let min_y = max_y + geo_transform[5] * raster_size.1 as f64;

        let mut ring =
            gdal::vector::Geometry::empty(gdal::vector::OGRwkbGeometryType::wkbLinearRing)
                .expect("Failed to create ring");

        // TODO: reproject bbox to output srs(?)
        ring.add_point_2d((min_x, max_y));
        ring.add_point_2d((max_x, max_y));
        ring.add_point_2d((max_x, min_y));
        ring.add_point_2d((min_x, min_y));
        ring.add_point_2d((min_x, max_y));

        let mut polygon =
            gdal::vector::Geometry::empty(gdal::vector::OGRwkbGeometryType::wkbPolygon)
                .expect("Failed to create polygon");
        polygon
            .add_geometry(ring)
            .expect("Failed to add ring to polygon");

        let mut feature = Feature::new(&defn).expect("Failed to create feature");
        feature
            .set_geometry(polygon)
            .expect("Failed to set geometry");
        feature
            .set_field_string(
                location_idx,
                dataset_path
                    .to_str()
                    .expect("Failed to convert path to string"),
            )
            .expect("Failed to set field");

        feature.create(&layer).expect("Failed to create feature");
    }

    let gti_xml = format!(
        r"<GDALTileIndexDataset>
    <IndexDataset>{index_dataset}</IndexDataset>
    <IndexLayer>tile_index</IndexLayer>
    <LocationField>location</LocationField>
</GDALTileIndexDataset>",
        index_dataset = tile_index_file
            .to_str()
            .expect("Failed to convert path to string"),
    );

    std::fs::write(gti_file, gti_xml).expect("Failed to write GTI XML to file");
}

fn main() {
    let datasets: [&Path; 2] = [
        Path::new(
            "/mnt/data_raid/geo_data/force/gti/data/X0059_Y0049/20000124_LEVEL2_LND07_BOA.tif",
        ),
        Path::new(
            "/mnt/data_raid/geo_data/force/gti/data/X0059_Y0049/20000124_LEVEL2_LND07_BOA.tif",
        ),
    ];

    let gti_file = Path::new("./foo.gti");
    let tile_index_file = Path::new("./foo.tile_index.geojson");

    gdaltindex(&datasets, gti_file, tile_index_file);

    let gti_dataset = GdalDataset::open(gti_file).expect("Failed to open GTI dataset");

    let raster_band = gti_dataset.rasterband(1).unwrap();
    let shape = raster_band.size();

    let mut data = raster_band
        .read_as::<u8>((0, 0), shape, shape, None)
        .unwrap();

    let driver = DriverManager::get_driver_by_name("GTiff").unwrap();
    let mut new_ds = driver
        .create_with_band_type::<u8, _>(
            "output.tif",
            shape.0,
            shape.1,
            1, // Number of bands
        )
        .unwrap();
    new_ds
        .set_spatial_ref(&gti_dataset.spatial_ref().unwrap())
        .unwrap();

    // Write the data to the new dataset
    let mut new_band = new_ds.rasterband(1).unwrap();
    new_band.write((0, 0), shape, &mut data).unwrap();
}
