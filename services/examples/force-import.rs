use chrono::{NaiveDate, TimeZone};
use gdal::vector::{Defn, Feature, FieldDefn, LayerOptions, OGRFieldType};
use gdal::{Dataset as GdalDataset, DriverManager, Metadata};
use geoengine_datatypes::primitives::{CacheTtlSeconds, DateTime, TimeInstance, TimeInterval};
use geoengine_datatypes::raster::GdalGeoTransform;
use geoengine_operators::source::{
    FileNotFoundHandling, GdalDatasetParameters, GdalLoadingInfoTemporalSlice, GdalMetaDataList,
};
use geoengine_operators::util::gdal::raster_descriptor_from_dataset;
use geoengine_services::datasets::storage::{DatasetDefinition, MetaDataDefinition};
use geoengine_services::datasets::{AddDataset, DatasetName};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;

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

#[allow(dead_code)]
fn test() {
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

    let raster_band = gti_dataset
        .rasterband(1)
        .expect("Failed to get raster band");
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

fn naive_date_to_time_instance(date: NaiveDate) -> TimeInstance {
    let time: chrono::DateTime<chrono::Utc> = chrono::Utc.from_utc_datetime(
        &date
            .and_hms_opt(0, 0, 0)
            .expect("Failed to create datetime"),
    );
    let time: DateTime = time.into();
    time.into()
}

fn main() {
    const FORCE_DATA_DIR: &str = "/home/michael/geodata/force/marburg";
    const GTI_OUTPUT_DIR: &str = "/home/michael/geodata/force/marburg/gti";
    const DATASET_OUTPUT_DIR: &str = "/home/michael/geodata/force/marburg/geoengine";

    let mut tile_dirs = Vec::new();

    let entries = fs::read_dir(&FORCE_DATA_DIR).expect("Failed to read FORCE_DATA_DIR");

    for entry in entries {
        let entry = entry.expect("Failed to read directory entry");
        if entry.file_type().expect("Failed to get file type").is_dir() {
            let folder_name = entry.file_name();
            // TODO: parse pattern X0059_Y0049
            if folder_name.to_string_lossy().starts_with('X') {
                tile_dirs.push(entry.path());
            }
        }
    }

    println!("Found tiles: {:?}", tile_dirs);

    let mut tif_files = Vec::new();

    for tile_dir in tile_dirs {
        let entries = fs::read_dir(&tile_dir).expect("Failed to read tile directory");
        for entry in entries {
            let entry = entry.expect("Failed to read directory entry");
            if entry
                .file_type()
                .expect("Failed to get file type")
                .is_file()
            {
                if let Some(extension) = entry.path().extension() {
                    if extension == "tif" {
                        tif_files.push(entry.path());
                    }
                }
            }
        }
    }

    println!("Found {:?} tif files", tif_files.len());

    let mut product_dataset_timesteps: HashMap<(String, String), HashMap<NaiveDate, Vec<PathBuf>>> =
        HashMap::new();

    for tif in &tif_files {
        if let Some(filename) = tif.file_name().and_then(|f| f.to_str()) {
            let parts: Vec<&str> = filename.split('_').collect();
            if parts.len() >= 4 {
                if let Ok(date_obj) = NaiveDate::parse_from_str(parts[0], "%Y%m%d") {
                    let product = parts[2].to_string();
                    let band = parts[3].split('.').next().unwrap_or("").to_string();
                    let key = (product, band);

                    product_dataset_timesteps
                        .entry(key)
                        .or_insert_with(HashMap::new)
                        .entry(date_obj)
                        .or_insert_with(Vec::new)
                        .push(tif.clone());
                }
            }
        }
    }

    println!("Found {} products:", product_dataset_timesteps.len());
    for ((product, dataset), timesteps) in &product_dataset_timesteps {
        println!(
            "  ({}, {}), #timesteps: {}",
            product,
            dataset,
            timesteps.len()
        );
    }

    let mut product_dataset_bands: HashMap<(String, String), Vec<String>> = HashMap::new();

    for ((product, dataset), timesteps) in &product_dataset_timesteps {
        if let Some(first_file) = timesteps.values().flat_map(|v| v).next() {
            let gdal_dataset = GdalDataset::open(first_file).expect("Failed to open dataset");
            let band_count = gdal_dataset.raster_count();

            print!("Available bands for {product} {dataset}: ",);

            let mut bands = Vec::new();

            for band_index in 1..=band_count {
                let band = gdal_dataset
                    .rasterband(band_index)
                    .expect("Failed to get raster band");

                let band_desc = band.description().unwrap_or("No description".to_string());

                bands.push(band_desc.clone());

                print!("{band_desc}");

                if band_index < band_count {
                    print!(", ");
                }
            }

            product_dataset_bands.insert((product.clone(), dataset.clone()), bands);
            println!();
        }
    }

    let product_datasets = product_dataset_timesteps.keys().collect::<Vec<_>>();

    let mut product_dataset_gtis: HashMap<(String, String), Vec<(NaiveDate, String)>> =
        HashMap::new();

    for (product, dataset) in product_datasets {
        let mut timestep_gtis = Vec::new();

        for (timestep, tiles) in product_dataset_timesteps
            .get(&(product.clone(), dataset.clone()))
            .expect("Failed to get timesteps")
        {
            let gti_file = format!(
                "{}/{}_{}_{}.gti",
                GTI_OUTPUT_DIR, product, dataset, timestep
            );
            let tile_index_file = format!(
                "{}/{}_{}_{}.tile_index.geojson",
                GTI_OUTPUT_DIR, product, dataset, timestep
            );

            let tile_refs: Vec<&Path> = tiles.iter().map(|tile| tile.as_path()).collect();
            gdaltindex(&tile_refs, gti_file.as_ref(), tile_index_file.as_ref());

            timestep_gtis.push((timestep.clone(), gti_file));
        }

        // Sort the vector by NaiveDate
        timestep_gtis.sort_by_key(|(timestep, _)| *timestep);

        println!(
            "Created {} GTIs for {product} {dataset}",
            timestep_gtis.len()
        );

        product_dataset_gtis.insert((product.clone(), dataset.clone()), timestep_gtis);
    }

    for ((product, dataset), timesteps) in &product_dataset_gtis {
        for (band_idx, band_name) in product_dataset_bands
            .get(&(product.clone(), dataset.clone()))
            .expect("Failed to get bands")
            .iter()
            .enumerate()
            .map(|(i, band)| (i + 1, band))
        {
            let gdal_dataset = GdalDataset::open(Path::new(
                &timesteps
                    .iter()
                    .next()
                    .expect("Failed to get first timestep")
                    .1,
            ))
            .expect("Failed to open dataset");

            let geo_transform: GdalGeoTransform = gdal_dataset
                .geo_transform()
                .expect("Failed to get geo-transform")
                .into();
            let geo_transform: geoengine_operators::source::GdalDatasetGeoTransform =
                geo_transform.into();

            let result_descriptor = raster_descriptor_from_dataset(&gdal_dataset, band_idx)
                .expect("Could not get raster descriptor");

            let mut slices = Vec::new();

            for (i, (timestep, file)) in timesteps.iter().enumerate() {
                let start_time = naive_date_to_time_instance(*timestep);

                let end_time: TimeInstance = if let Some((next_timestep, _)) = timesteps.get(i + 1)
                {
                    naive_date_to_time_instance(*next_timestep)
                } else {
                    TimeInstance::MAX
                };

                let time_interval: TimeInterval = TimeInterval::new(start_time, end_time)
                    .expect("Failed to create time interval");

                let rasterband = gdal_dataset
                    .rasterband(band_idx)
                    .expect("Failed to get raster band");

                slices.push(GdalLoadingInfoTemporalSlice {
                    time: time_interval,
                    params: Some(GdalDatasetParameters {
                        file_path: file.into(),
                        rasterband_channel: band_idx as usize,
                        geo_transform: geo_transform.try_into().unwrap(),
                        width: rasterband.size().0,
                        height: rasterband.size().1,
                        file_not_found_handling: FileNotFoundHandling::Error,
                        no_data_value: None, // TODO
                        properties_mapping: None,
                        gdal_open_options: None,
                        gdal_config_options: None,
                        allow_alphaband_as_mask: false,
                        retry: None,
                    }),
                    cache_ttl: CacheTtlSeconds::new(0),
                });
            }

            let dataset_def = DatasetDefinition {
                properties: AddDataset {
                    name: Some(
                        DatasetName::from_str(&format!("{product}_{dataset}_{band_idx}"))
                            .expect("Failed to create dataset name"),
                    ),
                    display_name: format!("{product} {dataset} {band_name}"),
                    description: format!("{} {} {}", product, dataset, band_name),
                    source_operator: "GdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: None,
                },
                meta_data: MetaDataDefinition::GdalMetaDataList(GdalMetaDataList {
                    result_descriptor: result_descriptor.into(),
                    params: slices,
                }),
            };

            let output_file = format!(
                "{}/{}_{}_{}_{}.json",
                DATASET_OUTPUT_DIR, product, dataset, band_name, band_idx
            );

            let json = serde_json::to_string_pretty(&dataset_def)
                .expect("Failed to serialize dataset definition to JSON");

            std::fs::write(&output_file, json).expect("Failed to write dataset definition to file");

            println!("Saved dataset definition to {}", output_file);
        }
    }
}
