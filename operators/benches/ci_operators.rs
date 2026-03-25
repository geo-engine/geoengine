#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "okay in benchmarks"
)]

use criterion::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use geoengine_datatypes::{
    collections::MultiPointCollection,
    primitives::{BoundingBox2D, ColumnSelection, DateTime, VectorQueryRectangle},
};
use geoengine_datatypes::{primitives::TimeInterval, util::test::TestDefault};
use geoengine_operators::{
    engine::{
        ExecutionContext, MockExecutionContext, QueryContext, SingleVectorMultipleRasterSources,
        VectorOperator, WorkflowOperatorPath,
    },
    processing::{
        ColumnNames, FeatureAggregationMethod, RasterVectorJoin, RasterVectorJoinParams,
        TemporalAggregationMethod,
    },
    source::{GdalSource, GdalSourceParameters, OgrSource, OgrSourceParameters},
    util::{
        Result,
        gdal::{add_ndvi_dataset, add_ports_dataset},
    },
};
use std::hint::black_box;

async fn raster_vector_join(
    exec_ctx: &dyn ExecutionContext,
    query_ctx: &dyn QueryContext,
    operator: Box<dyn VectorOperator>,
) -> Vec<MultiPointCollection> {
    let processor = operator
        .initialize(WorkflowOperatorPath::initialize_root(), exec_ctx)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .multi_point()
        .unwrap();

    let qrect = VectorQueryRectangle::new(
        BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
        TimeInterval::new_instant(DateTime::new_utc(2014, 6, 1, 12, 0, 0)).unwrap(),
        ColumnSelection::all(),
    );

    let result = processor
        .vector_query(qrect, query_ctx)
        .await
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await;

    black_box(result)
}

fn raster_vector_join_benchmark(c: &mut Criterion) {
    let mut execution_context = MockExecutionContext::test_default();
    let query_context = execution_context.mock_query_context_test_default();

    let ndvi_dataset = add_ndvi_dataset(&mut execution_context);
    let ports_dataset = add_ports_dataset(&mut execution_context);

    let operator = RasterVectorJoin {
        params: RasterVectorJoinParams {
            names: ColumnNames::Names(vec!["ndvi".to_string()]),
            feature_aggregation: FeatureAggregationMethod::Mean,
            feature_aggregation_ignore_no_data: true,
            temporal_aggregation: TemporalAggregationMethod::Mean,
            temporal_aggregation_ignore_no_data: true,
        },
        sources: SingleVectorMultipleRasterSources {
            vector: Box::new(OgrSource {
                params: OgrSourceParameters {
                    data: ports_dataset,
                    attribute_projection: None,
                    attribute_filters: None,
                },
            }),
            rasters: vec![Box::new(GdalSource {
                params: GdalSourceParameters::new(ndvi_dataset),
            })],
        },
    }
    .boxed();

    c.bench_function("raster_vector_join", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| raster_vector_join(&execution_context, &query_context, operator.clone()));
    });
}

criterion_group!(operators, raster_vector_join_benchmark);
criterion_main!(operators);
