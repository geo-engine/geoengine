use futures::{Future, StreamExt};
use geoengine_datatypes::{
    primitives::{
        Measurement, RasterQueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval,
    },
    raster::{RasterDataType, RasterTile2D},
    util::test::TestDefault,
};
use geoengine_operators::{
    engine::{MockExecutionContext, MockQueryContext, RasterOperator},
    processing::{
        Expression, ExpressionParams, ExpressionSources, NewExpression, NewExpressionParams,
        NewExpressionSources,
    },
    source::{GdalSource, GdalSourceParameters},
    util::{gdal::add_ndvi_dataset, number_statistics::NumberStatistics, Result},
};
use serde::Serialize;

#[derive(Serialize)]
struct OutputRow {
    opencl_mean: f64,
    opencl_std_dev: f64,
    native_mean: f64,
    native_std_dev: f64,
}

fn opencl_source(
    a: Box<dyn RasterOperator>,
    b: Box<dyn RasterOperator>,
) -> Box<dyn RasterOperator> {
    Expression {
        params: ExpressionParams {
            expression: "(A - B) / (A + B)".to_string(),
            output_type: RasterDataType::F64,
            output_no_data_value: 0.,
            output_measurement: Some(Measurement::Unitless),
        },
        sources: ExpressionSources::new_a_b(a, b),
    }
    .boxed()
}

fn native_source(
    a: Box<dyn RasterOperator>,
    b: Box<dyn RasterOperator>,
) -> Box<dyn RasterOperator> {
    NewExpression {
        params: NewExpressionParams {
            expression: "(A - B) / (A + B)".to_string(),
            output_type: RasterDataType::F64,
            output_no_data_value: 0.,
            output_measurement: Some(Measurement::Unitless),
            map_no_data: false,
        },
        sources: NewExpressionSources::new_a_b(a, b),
    }
    .boxed()
}

fn ndvi_source(execution_context: &mut MockExecutionContext) -> Box<dyn RasterOperator> {
    let ndvi_id = add_ndvi_dataset(execution_context);

    let gdal_operator = GdalSource {
        params: GdalSourceParameters { dataset: ndvi_id },
    };

    gdal_operator.boxed()
}

#[tokio::main]
async fn main() {
    const RUNS: usize = 5;

    let mut execution_context = MockExecutionContext::test_default();
    let query_context = MockQueryContext::test_default();

    let ndvi_source = ndvi_source(&mut execution_context);

    let opencl_source = opencl_source(ndvi_source.clone(), ndvi_source.clone());
    let native_source = native_source(ndvi_source.clone(), ndvi_source);

    let opencl_processor = opencl_source
        .initialize(&execution_context)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .get_f64()
        .unwrap();

    let native_processor = native_source
        .initialize(&execution_context)
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
    };

    let mut opencl_times = NumberStatistics::default();
    let mut native_times = NumberStatistics::default();

    for _ in 0..RUNS {
        let (opencl_time, native_result) = time_it(|| async {
            let opencl_query = opencl_processor
                .raster_query(qrect, &query_context)
                .await
                .unwrap();

            opencl_query.map(Result::unwrap).collect().await
        })
        .await;

        let (native_time, opencl_result) = time_it(|| async {
            let native_query = native_processor
                .raster_query(qrect, &query_context)
                .await
                .unwrap();

            native_query.map(Result::unwrap).collect().await
        })
        .await;

        assert_eq!(opencl_result, native_result); // validate results

        opencl_times.add(opencl_time);
        native_times.add(native_time);
    }

    let mut csv = csv::WriterBuilder::new()
        .delimiter(b';')
        .has_headers(true)
        .from_writer(std::io::stdout());

    csv.serialize(OutputRow {
        opencl_mean: opencl_times.mean(),
        opencl_std_dev: opencl_times.std_dev(),
        native_mean: native_times.mean(),
        native_std_dev: native_times.std_dev(),
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
    let secs = end.as_secs() as f64 + end.subsec_nanos() as f64 / 1_000_000_000.0;

    // println!("{} took {} seconds", name, secs);

    (secs, result)
}
