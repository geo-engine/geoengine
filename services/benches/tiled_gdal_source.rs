#![allow(clippy::unwrap_used, clippy::print_stdout, clippy::print_stderr)] // okay in benchmarks

use futures::StreamExt;
use geoengine_datatypes::{
    primitives::{BandSelection, RasterQueryRectangle, TimeInstance, TimeInterval},
    raster::{GridBoundingBox2D, RasterTile2D},
};
use geoengine_operators::{
    engine::{RasterOperator, WorkflowOperatorPath},
    source::{
        GdalSource, GdalSourceParameters, MultiBandGdalSource, MultiBandGdalSourceParameters,
    },
    util::{gdal::create_ndvi_result_descriptor, number_statistics::NumberStatistics},
};
use geoengine_services::{
    api::{
        handlers::datasets::AddDatasetTile,
        model::{
            datatypes::{Coordinate2D, SpatialPartition2D},
            operators::{
                FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalMultiBand,
            },
            services::{AddDataset, DatasetDefinition, MetaDataDefinition, Provenance},
        },
    },
    contexts::{ApplicationContext, PostgresContext, SessionContext},
    datasets::{DatasetName, storage::DatasetStore},
    permissions::{Permission, PermissionDb, Role},
    test_data,
    users::{UserAuth, UserSession},
    util::tests::{add_ndvi_to_datasets2, with_temp_context},
};
use std::env;
use std::str::FromStr;
use tokio_postgres::NoTls;
use uuid::Uuid;

const RUNS: usize = 10;

#[tokio::main]
async fn main() {
    bench_gdal_source().await;
    bench_multi_band_gdal_source().await;
}

async fn bench_gdal_source() {
    with_temp_context(|app_ctx, _| async move {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let exe_ctx = ctx.execution_context().unwrap();
        let query_ctx = ctx.query_context(Uuid::new_v4(), Uuid::new_v4()).unwrap();

        let (_, dataset) = add_ndvi_to_datasets2(&app_ctx, true, true).await;

        let operator = GdalSource {
            params: GdalSourceParameters::new(dataset),
        }
        .boxed();

        let processor = operator
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let qrect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap(),
            TimeInterval::new(
                TimeInstance::from_str("2014-01-01T00:00:00Z").unwrap(),
                TimeInstance::from_str("2014-07-01T00:00:00Z").unwrap(),
            )
            .unwrap(),
            BandSelection::first(),
        );

        let mut times = NumberStatistics::default();

        for _ in 0..RUNS {
            let (time, result) = time_it(|| async {
                let native_query = processor
                    .raster_query(qrect.clone(), &query_ctx)
                    .await
                    .unwrap();

                native_query.map(Result::unwrap).collect::<Vec<_>>().await
            })
            .await;

            times.add(time);

            std::hint::black_box(result);
        }

        println!(
            "GdalSource: {} runs, mean: {:.3} s, std_dev: {:.3} s",
            RUNS,
            times.mean(),
            times.std_dev()
        );
    })
    .await;
}

async fn bench_multi_band_gdal_source() {
    with_temp_context(|app_ctx, _| async move {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let exe_ctx = ctx.execution_context().unwrap();
        let query_ctx = ctx.query_context(Uuid::new_v4(), Uuid::new_v4()).unwrap();

        let dataset = add_ndvi_multi_tile_dataset(&app_ctx).await;

        let operator = MultiBandGdalSource {
            params: MultiBandGdalSourceParameters::new(dataset.into()),
        }
        .boxed();

        let processor = operator
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let qrect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap(),
            TimeInterval::new(
                TimeInstance::from_str("2014-01-01T00:00:00Z").unwrap(),
                TimeInstance::from_str("2014-07-01T00:00:00Z").unwrap(),
            )
            .unwrap(),
            BandSelection::first(),
        );

        let mut times = NumberStatistics::default();

        for _ in 0..RUNS {
            let (time, result) = time_it(|| async {
                let native_query = processor
                    .raster_query(qrect.clone(), &query_ctx)
                    .await
                    .unwrap();

                native_query.map(Result::unwrap).collect::<Vec<_>>().await
            })
            .await;

            times.add(time);

            std::hint::black_box(result);
        }

        println!(
            "MultiBandGdalSource: {} runs, mean: {:.3} s, std_dev: {:.3} s",
            RUNS,
            times.mean(),
            times.std_dev()
        );
    })
    .await;
}

async fn time_it<F, Fut>(f: F) -> (f64, Vec<RasterTile2D<u8>>)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Vec<RasterTile2D<u8>>>,
{
    let start = std::time::Instant::now();
    let result = f().await;
    let end = start.elapsed();
    let secs = end.as_secs() as f64 + f64::from(end.subsec_nanos()) / 1_000_000_000.0;

    (secs, result)
}

async fn add_ndvi_multi_tile_dataset(app_ctx: &PostgresContext<NoTls>) -> DatasetName {
    let dataset_name = DatasetName {
        namespace: None,
        name: "NDVI_multi_tile".to_string(),
    };

    let ndvi = DatasetDefinition {
        properties: AddDataset {
            name: Some(dataset_name.clone()),
            display_name: "NDVI multi tile".to_string(),
            description: "NDVI data from MODIS".to_string(),
            source_operator: "MultiBandGdalSource".to_string(),
            symbology: None,
            provenance: Some(vec![Provenance {
                citation: "Sample Citation".to_owned(),
                license: "Sample License".to_owned(),
                uri: "http://example.org/".to_owned(),
            }]),
            tags: Some(vec!["raster".to_owned(), "test".to_owned()]),
        },
        meta_data: MetaDataDefinition::GdalMultiBand(GdalMultiBand {
            result_descriptor: create_ndvi_result_descriptor(true).into(),
            r#type: geoengine_services::api::model::operators::GdalMultiBandTypeTag::GdalMultiBandTypeTag,
        }),
    };

    let system_session = UserSession::admin_session();

    let db = app_ctx.session_context(system_session).db();

    let dataset_id = db
        .add_dataset(ndvi.properties.into(), ndvi.meta_data.into(), None)
        .await
        .expect("dataset db access")
        .id;

    db.add_permission(Role::anonymous_role_id(), dataset_id, Permission::Read)
        .await
        .unwrap();

    let time_steps = [
        ("2014-01-01T00:00:00Z", "2014-02-01T00:00:00Z"),
        ("2014-02-01T00:00:00Z", "2014-03-01T00:00:00Z"),
        ("2014-03-01T00:00:00Z", "2014-04-01T00:00:00Z"),
        ("2014-04-01T00:00:00Z", "2014-05-01T00:00:00Z"),
        ("2014-05-01T00:00:00Z", "2014-06-01T00:00:00Z"),
        ("2014-06-01T00:00:00Z", "2014-07-01T00:00:00Z"),
    ];

    let tiles: Vec<AddDatasetTile> = time_steps
        .iter()
        .map(|(start, end)| AddDatasetTile {
            time: TimeInterval::new(
                TimeInstance::from_str(start).unwrap(),
                TimeInstance::from_str(end).unwrap(),
            )
            .unwrap()
            .into(),
            spatial_partition: SpatialPartition2D {
                upper_left_coordinate: Coordinate2D { x: -180., y: 90. },
                lower_right_coordinate: Coordinate2D { x: 180., y: -90. },
            },
            band: 0,
            z_index: 0,
            params: GdalDatasetParameters {
                file_path: test_data!(&format!(
                    "raster/modis_ndvi/MOD13A2_M_NDVI_{}.TIFF",
                    start.split('T').next().unwrap()
                ))
                .into(),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: Coordinate2D { x: -180., y: 90. },
                    x_pixel_size: 0.1,
                    y_pixel_size: -0.1,
                },
                width: 3600,
                height: 1800,
                file_not_found_handling: FileNotFoundHandling::Error,
                no_data_value: Some(0.0),
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: true,
            },
        })
        .collect();

    db.add_dataset_tiles(dataset_id, tiles).await.unwrap();

    dataset_name
}
