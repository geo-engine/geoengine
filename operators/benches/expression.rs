#![allow(clippy::unwrap_used, clippy::print_stdout, clippy::print_stderr)] // okay in benchmarks

use futures::{Future, StreamExt};
use geoengine_datatypes::{
    primitives::{
        BandSelection, RasterQueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval,
    },
    raster::{RasterDataType, RasterTile2D, RenameBands},
    util::test::TestDefault,
};
use geoengine_operators::{
    engine::{
        MockExecutionContext, MockQueryContext, MultipleRasterSources, RasterOperator,
        SingleRasterSource, WorkflowOperatorPath,
    },
    processing::{Expression, ExpressionParams, RasterStacker, RasterStackerParams},
    source::{GdalSource, GdalSourceParameters},
    util::{Result, gdal::add_ndvi_dataset, number_statistics::NumberStatistics},
};
use serde::Serialize;

#[derive(Serialize)]
struct OutputRow {
    expression_mean: f64,
    expression_std_dev: f64,
}

fn expression_on_sources(
    a: Box<dyn RasterOperator>,
    b: Box<dyn RasterOperator>,
) -> Box<dyn RasterOperator> {
    Expression {
        params: ExpressionParams {
            expression: "(A - B) / (A + B)".to_string(),
            output_type: RasterDataType::F64,
            output_band: None,
            map_no_data: false,
        },
        sources: SingleRasterSource {
            raster: RasterStacker {
                params: RasterStackerParams {
                    rename_bands: RenameBands::Default,
                },
                sources: MultipleRasterSources {
                    rasters: vec![a, b],
                },
            }
            .boxed(),
        },
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

#[tokio::main]
async fn main() {
    const RUNS: usize = 5;

    let mut execution_context = MockExecutionContext::test_default();
    let query_context = MockQueryContext::test_default();

    let ndvi_source = ndvi_source(&mut execution_context);

    let expression = expression_on_sources(ndvi_source.clone(), ndvi_source);

    let expression_processor = expression
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .get_f64()
        .unwrap();

    // World in 36000x18000 pixels",
    let qrect = RasterQueryRectangle {
        spatial_bounds: SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap(),
        time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
        spatial_resolution: SpatialResolution::new(0.01, 0.01).unwrap(),
        attributes: BandSelection::first(),
    };

    let mut times = NumberStatistics::default();

    for _ in 0..RUNS {
        let (time, result) = time_it(|| async {
            let native_query = expression_processor
                .raster_query(qrect.clone(), &query_context)
                .await
                .unwrap();

            native_query.map(Result::unwrap).collect().await
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
        expression_mean: times.mean(),
        expression_std_dev: times.std_dev(),
    })
    .unwrap();
}

async fn time_it<F, Fut>(f: F) -> (f64, Vec<RasterTile2D<f64>>)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Vec<RasterTile2D<f64>>>,
{
    let start = std::time::Instant::now();
    let result = f().await;
    let end = start.elapsed();
    let secs = end.as_secs() as f64 + f64::from(end.subsec_nanos()) / 1_000_000_000.0;

    // println!("{} took {} seconds", name, secs);

    (secs, result)
}
