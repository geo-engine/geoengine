use std::collections::HashMap;

use futures::StreamExt;
use geoengine_datatypes::{
    primitives::{RasterQueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval},
    raster::RasterTile2D,
    util::test::TestDefault,
};
use geoengine_operators::{
    engine::{MockExecutionContext, MockQueryContext, RasterOperator, WorkflowOperatorPath},
    pro::cache::cache_tiles::{CompressedRasterTile2D, CompressedRasterTileExt},
    source::{GdalSource, GdalSourceParameters},
    util::{gdal::add_ndvi_dataset, number_statistics::NumberStatistics, Result},
};
use serde::Serialize;

#[derive(Serialize)]
struct OutputRow {
    mode: String,
    time_mean: f64,
    time_std_dev: f64,
    time_min: f64,
    time_max: f64,
    tile_size_mean: f64,
    tile_size_std_dev: f64,
    tile_size_min: f64,
    tile_size_max: f64,
    tile_count: usize,
    empty_tile_count: usize,
}

fn ndvi_source(execution_context: &mut MockExecutionContext) -> Box<dyn RasterOperator> {
    let ndvi_id = add_ndvi_dataset(execution_context);

    let gdal_operator = GdalSource {
        params: GdalSourceParameters { data: ndvi_id },
    };

    gdal_operator.boxed()
}

#[tokio::main]
async fn main() {
    const COMPRESS: [&str; 2] = ["native", "lz4_flex"];

    let mut execution_context = MockExecutionContext::test_default();
    let query_context = MockQueryContext::test_default();

    let ndvi_source = ndvi_source(&mut execution_context);

    let source_processor = ndvi_source
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .get_u8()
        .unwrap();

    // World in 36000x18000 pixels",
    let qrect = RasterQueryRectangle {
        spatial_bounds: SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap(),
        time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        spatial_resolution: SpatialResolution::new(1., 1.).unwrap(),
    };

    let mut mode_times = HashMap::<String, NumberStatistics>::new();
    let mut mode_tile_sizes = HashMap::<String, NumberStatistics>::new();

    let native_query = source_processor
        .raster_query(qrect, &query_context)
        .await
        .unwrap();

    let result: Vec<RasterTile2D<u8>> = native_query.map(Result::unwrap).collect().await;

    for compression_mode in COMPRESS {
        let mut times = NumberStatistics::default();
        let mut tile_sizes = NumberStatistics::default();

        for tile in result.iter() {
            if tile.is_empty() {
                tile_sizes.add_no_data();
                continue;
            }

            let t = tile.clone();

            let time_start = std::time::Instant::now();
            let tile = match compression_mode {
                "native" => {
                    CompressedRasterTile2D::compress_tile(t, |x: &[u8]| x.to_vec()).unwrap()
                }
                "lz4_flex" => CompressedRasterTile2D::compress_tile(t, |x: &[u8]| {
                    lz4_flex::compress_prepend_size(x)
                })
                .unwrap(),
                _ => unreachable!(),
            };
            let time_end = std::time::Instant::now();
            times.add((time_end - time_start).as_micros());
            tile_sizes.add(tile.compressed_data_len());
        }

        mode_times.insert(compression_mode.to_string(), times);
        mode_tile_sizes.insert(compression_mode.to_string(), tile_sizes);
    }

    let mut csv = csv::WriterBuilder::new()
        .delimiter(b';')
        .has_headers(true)
        .from_writer(std::io::stdout());

    for compression_mode in COMPRESS {
        let times = mode_times.get(compression_mode).unwrap();
        let tile_sizes = mode_tile_sizes.get(compression_mode).unwrap();

        csv.serialize(OutputRow {
            mode: compression_mode.to_string(),
            time_mean: times.mean(),
            time_std_dev: times.std_dev(),
            time_min: times.min(),
            time_max: times.max(),
            tile_size_mean: tile_sizes.mean(),
            tile_size_std_dev: tile_sizes.std_dev(),
            tile_size_min: tile_sizes.min(),
            tile_size_max: tile_sizes.max(),
            tile_count: tile_sizes.count(),
            empty_tile_count: tile_sizes.nan_count(),
        })
        .unwrap();
    }
}
