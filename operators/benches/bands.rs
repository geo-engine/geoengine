use futures::{Future, StreamExt};
use geoengine_datatypes::{
    primitives::{
        RasterQueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval, TimeStep,
    },
    raster::{RasterDataType, RasterTile2D},
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
        params: GdalSourceParameters { data: ndvi_id },
    };

    gdal_operator.boxed()
}

async fn one_band_at_a_time(runs: usize, bands: usize, resolution: SpatialResolution) {
    let mut execution_context = MockExecutionContext::test_default();
    let query_context = MockQueryContext::test_default();

    let ndvi_source = ndvi_source(&mut execution_context);

    let aggregation = aggregation_on_source(ndvi_source);

    let expression_processor = aggregation
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .get_u64()
        .unwrap();

    // World in 36000x18000 pixels",
    let qrect = RasterQueryRectangle {
        spatial_bounds: SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap(),
        time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        spatial_resolution: resolution,
        selection: Default::default(),
    };

    let mut times = NumberStatistics::default();

    for _ in 0..runs {
        let (time, result) = time_it(|| async {
            let mut last_res = None;
            for _ in 0..bands {
                let native_query = expression_processor
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

async fn all_bands_at_once(runs: usize, bands: usize, resolution: SpatialResolution) {
    let mut execution_context = MockExecutionContext::test_default();
    let query_context = MockQueryContext::test_default();

    let stacker = RasterStacker {
        params: RasterStackerParams {},
        sources: MultipleRasterSources {
            rasters: (0..bands)
                .map(|_| ndvi_source(&mut execution_context))
                .collect(),
        },
    }
    .boxed();

    let aggregation = aggregation_on_source(stacker);

    let expression_processor = aggregation
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .get_u64()
        .unwrap();

    // World in 36000x18000 pixels",
    let qrect = RasterQueryRectangle {
        spatial_bounds: SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap(),
        time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        spatial_resolution: resolution,
        selection: (0..bands).collect::<Vec<_>>().into(),
    };

    let mut times = NumberStatistics::default();

    for _ in 0..runs {
        let (time, result) = time_it(|| async {
            let native_query = expression_processor
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
    const BANDS: usize = 8;
    const RESOLUTION: f64 = 0.1;

    println!("one band at a time");

    one_band_at_a_time(
        RUNS,
        BANDS,
        SpatialResolution::new(RESOLUTION, RESOLUTION).unwrap(),
    )
    .await;

    println!("all bands at once");

    all_bands_at_once(
        RUNS,
        BANDS,
        SpatialResolution::new(RESOLUTION, RESOLUTION).unwrap(),
    )
    .await;
}

async fn time_it<F, Fut>(f: F) -> (f64, Vec<RasterTile2D<u64>>)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Vec<RasterTile2D<u64>>>,
{
    let start = std::time::Instant::now();
    let result = f().await;
    let end = start.elapsed();
    let secs = end.as_secs() as f64 + end.subsec_nanos() as f64 / 1_000_000_000.0;

    // println!("{} took {} seconds", name, secs);

    (secs, result)
}
