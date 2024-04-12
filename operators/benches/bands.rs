#![allow(clippy::unwrap_used, clippy::print_stdout, clippy::print_stderr)] // okay in benchmarks

use futures::{Future, StreamExt};
use geoengine_datatypes::{
    primitives::{BandSelection, RasterQueryRectangle, TimeInterval, TimeStep},
    raster::{GridBoundingBox2D, RasterDataType, RasterTile2D, RenameBands},
    util::test::TestDefault,
};
use geoengine_operators::{
    engine::{
        MockExecutionContext, MockQueryContext, MultipleRasterSources, RasterOperator,
        SingleRasterSource, WorkflowOperatorPath,
    },
    processing::{
        Aggregation, RasterStacker, RasterStackerParams, TemporalRasterAggregation,
        TemporalRasterAggregationParameters,
    },
    source::{GdalSource, GdalSourceParameters},
    util::{gdal::add_ndvi_dataset, number_statistics::NumberStatistics, Result},
};
use serde::Serialize;

#[derive(Serialize)]
struct OutputRow {
    mean: f64,
    std_dev: f64,
}

fn aggregation_on_source(raster: Box<dyn RasterOperator>) -> Box<dyn RasterOperator> {
    TemporalRasterAggregation {
        params: TemporalRasterAggregationParameters {
            aggregation: Aggregation::Sum {
                ignore_no_data: true,
            },
            window: TimeStep {
                granularity: geoengine_datatypes::primitives::TimeGranularity::Months,
                step: 6,
            },
            window_reference: None,
            output_type: Some(RasterDataType::U64),
        },
        sources: SingleRasterSource { raster },
    }
    .boxed()
}

fn ndvi_source(execution_context: &mut MockExecutionContext) -> Box<dyn RasterOperator> {
    let ndvi_id = add_ndvi_dataset(execution_context);

    let gdal_operator = GdalSource {
        params: GdalSourceParameters::new(ndvi_id),
    };

    gdal_operator.boxed()
}

async fn one_band_at_a_time(runs: usize, bands: u32) {
    let mut execution_context = MockExecutionContext::test_default();
    let query_context = MockQueryContext::test_default();

    let ndvi_source = ndvi_source(&mut execution_context);

    let aggregation = aggregation_on_source(ndvi_source);

    let eprocessor = aggregation
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .get_u64()
        .unwrap();

    let qrect = RasterQueryRectangle::new_with_grid_bounds(
        GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap(),
        TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        BandSelection::first(),
    );

    let mut times = NumberStatistics::default();

    for _ in 0..runs {
        let (time, result) = time_it(|| async {
            let mut last_res = None;
            for _ in 0..bands {
                let native_query = eprocessor
                    .raster_query(qrect.clone(), &query_context)
                    .await
                    .unwrap();

                last_res = Some(native_query.map(Result::unwrap).collect::<Vec<_>>().await);
            }
            last_res.unwrap()
        })
        .await;

        times.add(time);

        std::hint::black_box(result);
    }

    let mut csv = csv::WriterBuilder::new()
        .delimiter(b';')
        .has_headers(true)
        .from_writer(std::io::stdout());

    csv.serialize(OutputRow {
        mean: times.mean(),
        std_dev: times.std_dev(),
    })
    .unwrap();
}

async fn all_bands_at_once(runs: usize, bands: u32) {
    let mut execution_context = MockExecutionContext::test_default();
    let query_context = MockQueryContext::test_default();

    let stacker = RasterStacker {
        params: RasterStackerParams {
            rename_bands: RenameBands::Default,
        },
        sources: MultipleRasterSources {
            rasters: (0..bands)
                .map(|_| ndvi_source(&mut execution_context))
                .collect(),
        },
    }
    .boxed();

    let aggregation = aggregation_on_source(stacker);

    let processor = aggregation
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .get_u64()
        .unwrap();

    let qrect = RasterQueryRectangle::new_with_grid_bounds(
        GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap(),
        TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        (0..bands).collect::<Vec<_>>().try_into().unwrap(),
    );

    let mut times = NumberStatistics::default();

    for _ in 0..runs {
        let (time, result) = time_it(|| async {
            let native_query = processor
                .raster_query(qrect.clone(), &query_context)
                .await
                .unwrap();

            native_query.map(Result::unwrap).collect::<Vec<_>>().await
        })
        .await;

        times.add(time);

        std::hint::black_box(result);
    }

    let mut csv = csv::WriterBuilder::new()
        .delimiter(b';')
        .has_headers(true)
        .from_writer(std::io::stdout());

    csv.serialize(OutputRow {
        mean: times.mean(),
        std_dev: times.std_dev(),
    })
    .unwrap();
}

#[tokio::main]
async fn main() {
    const RUNS: usize = 5;
    const BANDS: u32 = 8;

    println!("one band at a time");

    one_band_at_a_time(RUNS, BANDS).await;

    println!("all bands at once");

    all_bands_at_once(RUNS, BANDS).await;
}

async fn time_it<F, Fut>(f: F) -> (f64, Vec<RasterTile2D<u64>>)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Vec<RasterTile2D<u64>>>,
{
    let start = std::time::Instant::now();
    let result = f().await;
    let end = start.elapsed();
    let secs = end.as_secs() as f64 + f64::from(end.subsec_nanos()) / 1_000_000_000.0;

    (secs, result)
}
