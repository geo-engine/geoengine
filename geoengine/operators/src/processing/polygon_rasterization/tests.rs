use super::*;
use crate::engine::{
    MockExecutionContext, MultipleRasterSources, SingleRasterSource, StaticMetaData,
    TypedVectorOperator, VectorOperator, VectorResultDescriptor,
};
use crate::mock::MockFeatureCollectionSource;
use crate::processing::{Expression, ExpressionParams, RasterStacker, RasterStackerParams};
use crate::source::{
    GdalSource, GdalSourceParameters, OgrSource, OgrSourceColumnSpec, OgrSourceDataset,
    OgrSourceDatasetTimeType, OgrSourceErrorSpec, OgrSourceParameters,
};
use crate::util::gdal::add_ndvi_dataset;
use crate::util::raster_stream_to_geotiff::{
    GdalCompressionNumThreads, GdalGeoTiffDatasetMetadata, GdalGeoTiffOptions,
    single_timestep_raster_stream_to_geotiff_bytes,
};
use crate::util::raster_stream_to_png::raster_stream_to_png_bytes;
use crate::util::test::AsciiRasterTilesExt;
use geoengine_datatypes::collections::MultiPointCollection;
use geoengine_datatypes::dataset::{DataId, DatasetId, NamedData};
use geoengine_datatypes::operations::image::{Colorizer, RgbaColor};
use geoengine_datatypes::primitives::{
    ClassificationMeasurement, DateTime, FeatureData, MultiPoint, MultiPolygon, TimeGranularity,
};
use geoengine_datatypes::raster::{RenameBands, TileSize, TilingSpecification};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_datatypes::test_data;
use geoengine_datatypes::util::test::{
    AsciiRasterTiles, TestDefault, assert_raster_tiles_eq_ascii,
};
use geoengine_datatypes::util::{
    Identifier, ImageFormat, assert_image_equals, assert_image_equals_with_format,
};
use indoc::indoc;
use std::collections::HashMap;

/// An axis-aligned rectangle as a multi polygon.
fn rectangle(x_min: f64, y_min: f64, x_max: f64, y_max: f64) -> MultiPolygon {
    MultiPolygon::new(vec![vec![vec![
        (x_min, y_min).into(),
        (x_max, y_min).into(),
        (x_max, y_max).into(),
        (x_min, y_max).into(),
        (x_min, y_min).into(),
    ]]])
    .unwrap()
}

fn polygon_source(
    polygons: Vec<MultiPolygon>,
    times: Vec<TimeInterval>,
    data: HashMap<String, FeatureData>,
) -> TypedVectorOperator {
    MockFeatureCollectionSource::single(
        MultiPolygonCollection::from_data(polygons, times, data, CacheHint::no_cache()).unwrap(),
    )
    .typed()
}

/// Rasterization with unit pixels aligned to the origin, i.e., the grid of [`AsciiRasterTiles`].
fn params(value: BurnValue, time: RasterizationTime) -> PolygonRasterizationParams {
    PolygonRasterizationParams {
        spatial_resolution: SpatialResolution::new(1., 1.).unwrap(),
        origin_coordinate: Coordinate2D::new(0., 0.),
        value,
        output_type: None,
        output_measurement: None,
        time,
    }
}

fn exe_ctx_4x4() -> MockExecutionContext {
    MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(TileSize::new_unchecked(
        4, 4,
    )))
}

async fn initialize(
    params: PolygonRasterizationParams,
    source: TypedVectorOperator,
    exe_ctx: &MockExecutionContext,
    time_scan_batch_size: usize,
) -> Result<InitializedPolygonRasterization> {
    Box::new(PolygonRasterization {
        params,
        sources: SingleVectorSource { vector: source },
    })
    .initialize_with_time_scan_batch_size(
        ProcessingGraphOperatorPath::initialize_root(),
        exe_ctx,
        time_scan_batch_size,
    )
    .await
}

async fn query_tiles<T: Pixel>(
    processor: BoxRasterQueryProcessor<T>,
    exe_ctx: &MockExecutionContext,
    bounds: GridBoundingBox2D,
    time: TimeInterval,
) -> Vec<RasterTile2D<T>> {
    let query_ctx = exe_ctx.mock_query_context_test_default();
    processor
        .query(
            RasterQueryRectangle::new(bounds, time, BandSelection::first()),
            &query_ctx,
        )
        .await
        .unwrap()
        .map(Result::unwrap)
        .collect()
        .await
}

/// The pixels of the tile in the upper right quadrant of the origin, i.e., x and y in `[0, 4]`.
fn first_tile_bounds() -> GridBoundingBox2D {
    GridBoundingBox2D::new([-4, 0], [-1, 3]).unwrap()
}

#[tokio::test]
async fn it_rasterizes_constant() {
    let exe_ctx = exe_ctx_4x4();
    let source = polygon_source(
        vec![rectangle(1., 1., 6., 3.)],
        vec![TimeInterval::default()],
        HashMap::new(),
    );
    let rasterization = initialize(
        params(BurnValue::default(), RasterizationTime::FeatureTimes),
        source,
        &exe_ctx,
        TIME_SCAN_BATCH_SIZE,
    )
    .await
    .unwrap();

    assert_eq!(
        rasterization.result_descriptor().data_type,
        RasterDataType::U8
    );

    let tiles = query_tiles(
        rasterization.query_processor().unwrap().get_u8().unwrap(),
        &exe_ctx,
        GridBoundingBox2D::new([-4, 0], [-1, 11]).unwrap(),
        TimeInterval::new_unchecked(0, 10),
    )
    .await;

    assert_raster_tiles_eq_ascii(
        &tiles,
        &AsciiRasterTiles::new(indoc! {"
            [-inf, inf)
            +-------------+-------------+-------------+
            |  _  _  _  _ |  _  _  _  _ |  _  _  _  _ |
            |  _  1  1  1 |  1  1  _  _ |  _  _  _  _ |
            |  _  1  1  1 |  1  1  _  _ |  _  _  _  _ |
            |  _  _  _  _ |  _  _  _  _ |  _  _  _  _ |
            +-------------+-------------+-------------+
            "})
        .tile_origin([-1, 0]),
    );
    assert!(tiles[2].grid_array.is_empty(), "uncovered tile is empty");
}

#[tokio::test]
async fn it_respects_holes() {
    let exe_ctx = exe_ctx_4x4();
    let with_hole = MultiPolygon::new(vec![vec![
        vec![
            (0., 0.).into(),
            (4., 0.).into(),
            (4., 4.).into(),
            (0., 4.).into(),
            (0., 0.).into(),
        ],
        vec![
            (1., 1.).into(),
            (1., 3.).into(),
            (3., 3.).into(),
            (3., 1.).into(),
            (1., 1.).into(),
        ],
    ]])
    .unwrap();
    let source = polygon_source(
        vec![with_hole],
        vec![TimeInterval::default()],
        HashMap::new(),
    );

    let rasterization = initialize(
        params(BurnValue::default(), RasterizationTime::Ignore),
        source,
        &exe_ctx,
        TIME_SCAN_BATCH_SIZE,
    )
    .await
    .unwrap();

    let tiles = query_tiles(
        rasterization.query_processor().unwrap().get_u8().unwrap(),
        &exe_ctx,
        first_tile_bounds(),
        TimeInterval::new_unchecked(0, 10),
    )
    .await;

    assert_raster_tiles_eq_ascii(
        &tiles,
        &AsciiRasterTiles::new(indoc! {"
            [-inf, inf)
            +-------------+
            |  1  1  1  1 |
            |  1  _  _  1 |
            |  1  _  _  1 |
            |  1  1  1  1 |
            +-------------+
            "})
        .tile_origin([-1, 0]),
    );
}

#[tokio::test]
async fn it_counts_overlaps() {
    let exe_ctx = exe_ctx_4x4();
    let source = polygon_source(
        vec![rectangle(0., 0., 3., 4.), rectangle(1., 0., 4., 4.)],
        vec![TimeInterval::default(); 2],
        HashMap::new(),
    );

    let rasterization = initialize(
        params(BurnValue::Count, RasterizationTime::Ignore),
        source,
        &exe_ctx,
        TIME_SCAN_BATCH_SIZE,
    )
    .await
    .unwrap();

    let tiles = query_tiles(
        rasterization.query_processor().unwrap().get_u32().unwrap(),
        &exe_ctx,
        first_tile_bounds(),
        TimeInterval::new_unchecked(0, 10),
    )
    .await;

    assert_raster_tiles_eq_ascii(
        &tiles,
        &AsciiRasterTiles::new(indoc! {"
            [-inf, inf)
            +-------------+
            |  1  2  2  1 |
            |  1  2  2  1 |
            |  1  2  2  1 |
            |  1  2  2  1 |
            +-------------+
            "})
        .tile_origin([-1, 0]),
    );
}

/// Four features: the whole tile with value 4, the left half with 1, the upper half with 7,
/// and the whole tile with null, which is skipped.
fn attribute_source() -> TypedVectorOperator {
    polygon_source(
        vec![
            rectangle(0., 0., 4., 4.),
            rectangle(0., 0., 2., 4.),
            rectangle(0., 2., 4., 4.),
            rectangle(0., 0., 4., 4.),
        ],
        vec![TimeInterval::default(); 4],
        [(
            "class".to_string(),
            FeatureData::NullableFloat(vec![Some(4.), Some(1.), Some(7.), None]),
        )]
        .into_iter()
        .collect(),
    )
}

#[tokio::test]
async fn it_burns_attribute_with_aggregations() {
    // the values of the upper left, upper right, lower left and lower right quadrants
    let cases = [
        (OverlapAggregation::First, ["4", "4", "4", "4"]),
        (OverlapAggregation::Last, ["7", "7", "1", "4"]),
        (OverlapAggregation::Min, ["1", "4", "1", "4"]),
        (OverlapAggregation::Max, ["7", "7", "4", "4"]),
        (OverlapAggregation::Sum, ["12", "11", "5", "4"]),
        (OverlapAggregation::Mean, ["4", "5.5", "2.5", "4"]),
        (OverlapAggregation::MedianEstimate, ["4", "5.5", "2.5", "4"]),
    ];

    for (aggregation, [ul, ur, ll, lr]) in cases {
        let exe_ctx = exe_ctx_4x4();
        let rasterization = initialize(
            params(
                BurnValue::Attribute {
                    column: "class".to_string(),
                    aggregation,
                    ignore_no_data: true,
                },
                RasterizationTime::Ignore,
            ),
            attribute_source(),
            &exe_ctx,
            TIME_SCAN_BATCH_SIZE,
        )
        .await
        .unwrap();

        assert_eq!(
            rasterization.result_descriptor().data_type,
            RasterDataType::F64
        );

        let tiles = query_tiles(
            rasterization.query_processor().unwrap().get_f64().unwrap(),
            &exe_ctx,
            first_tile_bounds(),
            TimeInterval::new_unchecked(0, 10),
        )
        .await;

        let expected = format!(
            "[-inf, inf)
            | {ul} {ul} {ur} {ur} |
            | {ul} {ul} {ur} {ur} |
            | {ll} {ll} {lr} {lr} |
            | {ll} {ll} {lr} {lr} |
            "
        );
        assert_raster_tiles_eq_ascii(
            &tiles,
            &AsciiRasterTiles::new(&expected).tile_origin([-1, 0]),
        );
    }
}

#[tokio::test]
async fn it_propagates_no_data_of_attributes() {
    // the whole tile with value 4 and its right half with null
    let source = || {
        polygon_source(
            vec![rectangle(0., 0., 4., 4.), rectangle(2., 0., 4., 4.)],
            vec![TimeInterval::default(); 2],
            [(
                "value".to_string(),
                FeatureData::NullableInt(vec![Some(4), None]),
            )]
            .into_iter()
            .collect(),
        )
    };

    for (ignore_no_data, right) in [(true, "4"), (false, "_")] {
        let exe_ctx = exe_ctx_4x4();
        let rasterization = initialize(
            params(
                BurnValue::Attribute {
                    column: "value".to_string(),
                    aggregation: OverlapAggregation::Sum,
                    ignore_no_data,
                },
                RasterizationTime::Ignore,
            ),
            source(),
            &exe_ctx,
            TIME_SCAN_BATCH_SIZE,
        )
        .await
        .unwrap();

        let tiles = query_tiles(
            rasterization.query_processor().unwrap().get_f64().unwrap(),
            &exe_ctx,
            first_tile_bounds(),
            TimeInterval::new_unchecked(0, 10),
        )
        .await;

        let expected = format!(
            "[-inf, inf)
            | 4 4 {right} {right} |
            | 4 4 {right} {right} |
            | 4 4 {right} {right} |
            | 4 4 {right} {right} |
            "
        );
        assert_raster_tiles_eq_ascii(
            &tiles,
            &AsciiRasterTiles::new(&expected).tile_origin([-1, 0]),
        );
    }
}

#[tokio::test]
async fn it_estimates_the_median_of_many_overlaps() {
    let exe_ctx = exe_ctx_4x4();
    let source = polygon_source(
        vec![rectangle(0., 0., 4., 4.); 7],
        vec![TimeInterval::default(); 7],
        [(
            "value".to_string(),
            FeatureData::Float(vec![7., 1., 6., 2., 5., 3., 4.]),
        )]
        .into_iter()
        .collect(),
    );

    let rasterization = initialize(
        params(
            BurnValue::Attribute {
                column: "value".to_string(),
                aggregation: OverlapAggregation::MedianEstimate,
                ignore_no_data: true,
            },
            RasterizationTime::Ignore,
        ),
        source,
        &exe_ctx,
        TIME_SCAN_BATCH_SIZE,
    )
    .await
    .unwrap();

    let tiles = query_tiles(
        rasterization.query_processor().unwrap().get_f64().unwrap(),
        &exe_ctx,
        first_tile_bounds(),
        TimeInterval::new_unchecked(0, 10),
    )
    .await;

    let values = tiles[0]
        .grid_array
        .clone()
        .into_materialized_masked_grid()
        .inner_grid
        .data;
    for value in values {
        assert!((value - 4.).abs() <= 0.5, "median estimate {value}");
    }
}

/// A feature for the whole tile at `[0, 10)` and one for its left half at `[5, 20)`.
fn timed_source() -> TypedVectorOperator {
    polygon_source(
        vec![rectangle(0., 0., 4., 4.), rectangle(0., 0., 2., 4.)],
        vec![
            TimeInterval::new_unchecked(0, 10),
            TimeInterval::new_unchecked(5, 20),
        ],
        HashMap::new(),
    )
}

#[tokio::test]
async fn it_creates_feature_time_slices() {
    // a batch size of 1 and 2 needs several scans of the input for the boundaries 0, 5, 10 and 20
    for time_scan_batch_size in [1, 2, 3, TIME_SCAN_BATCH_SIZE] {
        let exe_ctx = exe_ctx_4x4();
        let rasterization = initialize(
            params(BurnValue::Count, RasterizationTime::FeatureTimes),
            timed_source(),
            &exe_ctx,
            time_scan_batch_size,
        )
        .await
        .unwrap();
        let processor = rasterization.query_processor().unwrap().get_u32().unwrap();

        let tiles = query_tiles(
            processor,
            &exe_ctx,
            first_tile_bounds(),
            TimeInterval::new_unchecked(-5, 25),
        )
        .await;

        assert_raster_tiles_eq_ascii(
            &tiles,
            &AsciiRasterTiles::new(indoc! {"
                [-inf, 0)
                |  _  _  _  _ |
                |  _  _  _  _ |
                |  _  _  _  _ |
                |  _  _  _  _ |

                [0, 5)
                |  1  1  1  1 |
                |  1  1  1  1 |
                |  1  1  1  1 |
                |  1  1  1  1 |

                [5, 10)
                |  2  2  1  1 |
                |  2  2  1  1 |
                |  2  2  1  1 |
                |  2  2  1  1 |

                [10, 20)
                |  1  1  _  _ |
                |  1  1  _  _ |
                |  1  1  _  _ |
                |  1  1  _  _ |

                [20, inf)
                |  _  _  _  _ |
                |  _  _  _  _ |
                |  _  _  _  _ |
                |  _  _  _  _ |
                "})
            .tile_origin([-1, 0]),
        );
    }
}

#[tokio::test]
async fn it_returns_the_feature_time_slices_that_intersect_the_query() {
    let cases = [
        // not aligned to the boundaries
        (
            TimeInterval::new_unchecked(2, 12),
            vec![(0, 5), (5, 10), (10, 20)],
        ),
        // an instant on a boundary belongs to the slice that starts there
        (TimeInterval::new_unchecked(5, 5), vec![(5, 10)]),
        // a query that ends at a boundary does not include the next slice
        (TimeInterval::new_unchecked(0, 10), vec![(0, 5), (5, 10)]),
        // a query before all features
        (
            TimeInterval::new_unchecked(-10, -5),
            vec![(TimeInstance::MIN.inner(), 0)],
        ),
    ];

    for time_scan_batch_size in [1, 2, TIME_SCAN_BATCH_SIZE] {
        for (query, expected) in &cases {
            let exe_ctx = exe_ctx_4x4();
            let rasterization = initialize(
                params(BurnValue::Count, RasterizationTime::FeatureTimes),
                timed_source(),
                &exe_ctx,
                time_scan_batch_size,
            )
            .await
            .unwrap();
            let processor = rasterization.query_processor().unwrap().get_u32().unwrap();
            let query_ctx = exe_ctx.mock_query_context_test_default();

            let slices: Vec<TimeInterval> = processor
                .time_query(*query, &query_ctx)
                .await
                .unwrap()
                .map(Result::unwrap)
                .collect()
                .await;

            let expected: Vec<TimeInterval> = expected
                .iter()
                .map(|&(start, end)| TimeInterval::new_unchecked(start, end))
                .collect();
            assert_eq!(
                slices, expected,
                "query {query} with batch size {time_scan_batch_size}"
            );
        }
    }
}

#[tokio::test]
async fn it_creates_regular_time_slices() {
    let exe_ctx = exe_ctx_4x4();
    let source = polygon_source(
        vec![rectangle(0., 0., 4., 4.), rectangle(0., 0., 2., 4.)],
        vec![
            TimeInterval::new_unchecked(5, 25),
            TimeInterval::new_unchecked(12, 14),
        ],
        HashMap::new(),
    );
    let rasterization = initialize(
        params(
            BurnValue::Count,
            RasterizationTime::Regular {
                step: TimeStep {
                    granularity: TimeGranularity::Millis,
                    step: 10,
                },
                reference: None,
            },
        ),
        source,
        &exe_ctx,
        TIME_SCAN_BATCH_SIZE,
    )
    .await
    .unwrap();

    assert!(
        rasterization
            .result_descriptor()
            .time
            .dimension
            .is_regular()
    );

    let tiles = query_tiles(
        rasterization.query_processor().unwrap().get_u32().unwrap(),
        &exe_ctx,
        first_tile_bounds(),
        TimeInterval::new_unchecked(0, 30),
    )
    .await;

    assert_raster_tiles_eq_ascii(
        &tiles,
        &AsciiRasterTiles::new(indoc! {"
            [0, 10)
            |  1  1  1  1 |
            |  1  1  1  1 |
            |  1  1  1  1 |
            |  1  1  1  1 |

            [10, 20)
            |  2  2  1  1 |
            |  2  2  1  1 |
            |  2  2  1  1 |
            |  2  2  1  1 |

            [20, 30)
            |  1  1  1  1 |
            |  1  1  1  1 |
            |  1  1  1  1 |
            |  1  1  1  1 |
            "})
        .tile_origin([-1, 0]),
    );
}

#[tokio::test]
async fn it_ignores_time() {
    let exe_ctx = exe_ctx_4x4();
    let rasterization = initialize(
        params(BurnValue::Count, RasterizationTime::Ignore),
        timed_source(),
        &exe_ctx,
        TIME_SCAN_BATCH_SIZE,
    )
    .await
    .unwrap();

    // the query time does not intersect any feature
    let tiles = query_tiles(
        rasterization.query_processor().unwrap().get_u32().unwrap(),
        &exe_ctx,
        first_tile_bounds(),
        TimeInterval::new_unchecked(100, 200),
    )
    .await;

    assert_raster_tiles_eq_ascii(
        &tiles,
        &AsciiRasterTiles::new(indoc! {"
            [-inf, inf)
            |  2  2  1  1 |
            |  2  2  1  1 |
            |  2  2  1  1 |
            |  2  2  1  1 |
            "})
        .tile_origin([-1, 0]),
    );
}

#[tokio::test]
async fn it_derives_measurements() {
    let classes = Measurement::Classification(ClassificationMeasurement {
        measurement: "land cover".to_string(),
        classes: [(1, "forest".to_string()), (2, "water".to_string())]
            .into_iter()
            .collect(),
    });
    let height = Measurement::continuous("height".to_string(), Some("m".to_string()));

    let source = || {
        MockFeatureCollectionSource::with_collections_and_measurements(
            vec![
                MultiPolygonCollection::from_data(
                    vec![rectangle(0., 0., 4., 4.)],
                    vec![TimeInterval::default()],
                    [
                        ("class".to_string(), FeatureData::Category(vec![1])),
                        ("height".to_string(), FeatureData::Float(vec![1.])),
                        ("name".to_string(), FeatureData::Text(vec!["a".into()])),
                    ]
                    .into_iter()
                    .collect(),
                    CacheHint::no_cache(),
                )
                .unwrap(),
            ],
            [
                ("class".to_string(), classes.clone()),
                ("height".to_string(), height.clone()),
            ]
            .into_iter()
            .collect(),
        )
        .typed()
    };
    let attribute = |column: &str, aggregation| BurnValue::Attribute {
        column: column.to_string(),
        aggregation,
        ignore_no_data: true,
    };

    let cases = [
        (BurnValue::default(), None, "band", Measurement::Unitless),
        (
            BurnValue::Count,
            None,
            "count",
            Measurement::continuous("count".to_string(), None),
        ),
        (
            attribute("class", OverlapAggregation::First),
            None,
            "class",
            classes.clone(),
        ),
        (
            attribute("class", OverlapAggregation::Max),
            None,
            "class",
            classes.clone(),
        ),
        (
            attribute("class", OverlapAggregation::Mean),
            None,
            "class",
            Measurement::Unitless,
        ),
        (
            attribute("class", OverlapAggregation::MedianEstimate),
            None,
            "class",
            Measurement::Unitless,
        ),
        (
            attribute("height", OverlapAggregation::Sum),
            None,
            "height",
            height.clone(),
        ),
        (
            BurnValue::default(),
            Some(classes.clone()),
            "band",
            classes.clone(),
        ),
    ];

    let exe_ctx = exe_ctx_4x4();
    for (value, output_measurement, name, measurement) in cases {
        let rasterization = initialize(
            PolygonRasterizationParams {
                output_measurement,
                ..params(value.clone(), RasterizationTime::Ignore)
            },
            source(),
            &exe_ctx,
            TIME_SCAN_BATCH_SIZE,
        )
        .await
        .unwrap();

        let band = &rasterization.result_descriptor().bands[0];
        assert_eq!(band.name, name, "{value:?}");
        assert_eq!(band.measurement, measurement, "{value:?}");
    }
}

#[tokio::test]
async fn it_rejects_invalid_columns() {
    let exe_ctx = exe_ctx_4x4();
    let source = || {
        polygon_source(
            vec![rectangle(0., 0., 4., 4.)],
            vec![TimeInterval::default()],
            [("name".to_string(), FeatureData::Text(vec!["a".into()]))]
                .into_iter()
                .collect(),
        )
    };
    let attribute = |column: &str, aggregation| BurnValue::Attribute {
        column: column.to_string(),
        aggregation,
        ignore_no_data: true,
    };

    let text_column = initialize(
        params(
            attribute("name", OverlapAggregation::First),
            RasterizationTime::Ignore,
        ),
        source(),
        &exe_ctx,
        TIME_SCAN_BATCH_SIZE,
    )
    .await;
    assert!(
        matches!(text_column, Err(Error::InvalidType { .. })),
        "text column is rejected"
    );

    let missing_column = initialize(
        params(
            attribute("missing", OverlapAggregation::First),
            RasterizationTime::Ignore,
        ),
        source(),
        &exe_ctx,
        TIME_SCAN_BATCH_SIZE,
    )
    .await;
    assert!(
        matches!(missing_column, Err(Error::ColumnDoesNotExist { .. })),
        "missing column is rejected"
    );
}

#[tokio::test]
async fn it_rejects_invalid_inputs() {
    let exe_ctx = exe_ctx_4x4();

    let points = MockFeatureCollectionSource::single(
        MultiPointCollection::from_data(
            vec![MultiPoint::new(vec![(1., 1.).into()]).unwrap()],
            vec![TimeInterval::default()],
            HashMap::new(),
            CacheHint::no_cache(),
        )
        .unwrap(),
    )
    .typed();
    let result = initialize(
        params(BurnValue::default(), RasterizationTime::Ignore),
        points,
        &exe_ctx,
        TIME_SCAN_BATCH_SIZE,
    )
    .await;
    assert!(
        matches!(result, Err(Error::InvalidType { .. })),
        "points are rejected"
    );

    let result = initialize(
        params(
            BurnValue::Constant {
                value: FiniteF64::new_unchecked(300.),
            },
            RasterizationTime::Ignore,
        ),
        timed_source(),
        &exe_ctx,
        TIME_SCAN_BATCH_SIZE,
    )
    .await;
    assert!(
        matches!(result, Err(Error::InvalidOperatorSpec { .. })),
        "300 does not fit into the default type u8"
    );
}

#[test]
fn it_deserializes_params_with_defaults() {
    let params: PolygonRasterizationParams = serde_json::from_value(serde_json::json!({
        "spatialResolution": { "x": 0.1, "y": 0.1 },
    }))
    .unwrap();

    assert_eq!(
        params,
        PolygonRasterizationParams {
            spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
            origin_coordinate: Coordinate2D::new(0., 0.),
            value: BurnValue::Constant {
                value: FiniteF64::new_unchecked(1.)
            },
            output_type: None,
            output_measurement: None,
            time: RasterizationTime::FeatureTimes,
        }
    );

    let params: PolygonRasterizationParams = serde_json::from_value(serde_json::json!({
        "spatialResolution": { "x": 1, "y": 1 },
        "value": { "type": "attribute", "column": "class", "aggregation": "medianEstimate" },
        "time": { "type": "regular", "step": { "granularity": "months", "step": 1 } },
    }))
    .unwrap();
    assert_eq!(
        params.value,
        BurnValue::Attribute {
            column: "class".to_string(),
            aggregation: OverlapAggregation::MedianEstimate,
            ignore_no_data: true,
        }
    );
    let strict: BurnValue = serde_json::from_value(serde_json::json!({
        "type": "attribute", "column": "class", "aggregation": "min", "ignoreNoData": false
    }))
    .unwrap();
    assert_eq!(
        strict,
        BurnValue::Attribute {
            column: "class".to_string(),
            aggregation: OverlapAggregation::Min,
            ignore_no_data: false,
        }
    );
    assert_eq!(
        params.time,
        RasterizationTime::Regular {
            step: TimeStep {
                granularity: TimeGranularity::Months,
                step: 1,
            },
            reference: None,
        }
    );

    // JSON has no infinity, so a number out of range is the closest to it
    assert!(
        serde_json::from_str::<BurnValue>(r#"{ "type": "constant", "value": 1e999 }"#).is_err()
    );
}

#[tokio::test]
async fn it_clips_raster_with_polygon() {
    let raster = AsciiRasterTiles::new(indoc! {"
        [0, 10)
        +-------------+
        |  1  2  3  4 |
        |  5  6  7  8 |
        |  9 10 11 12 |
        | 13 14 15 16 |
        +-------------+
        "})
    .tile_origin([-1, 0]);

    let mask = PolygonRasterization {
        params: params(
            BurnValue::default(),
            RasterizationTime::Regular {
                step: TimeStep {
                    granularity: TimeGranularity::Millis,
                    step: 10,
                },
                reference: None,
            },
        ),
        sources: SingleVectorSource {
            vector: polygon_source(
                vec![rectangle(1., 1., 3., 3.)],
                vec![TimeInterval::default()],
                HashMap::new(),
            ),
        },
    }
    .typed();

    let clipped = clip(raster.mock_raster_source::<u8>().typed(), mask);

    let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
    let processor = clipped
        .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .get_u8()
        .unwrap();

    let tiles = query_tiles(
        processor,
        &exe_ctx,
        first_tile_bounds(),
        TimeInterval::new_unchecked(0, 10),
    )
    .await;

    assert_raster_tiles_eq_ascii(
        &tiles,
        &AsciiRasterTiles::new(indoc! {"
            [0, 10)
            +-------------+
            |  _  _  _  _ |
            |  _  6  7  _ |
            |  _ 10 11  _ |
            |  _  _  _  _ |
            +-------------+
            "})
        .tile_origin([-1, 0]),
    );
}

/// Masks `raster` with the no-data pixels of `mask`.
fn clip(raster: TypedRasterOperator, mask: TypedRasterOperator) -> TypedRasterOperator {
    Expression {
        params: ExpressionParams {
            expression: "if B IS NODATA { NODATA } else { A }".to_string(),
            output_type: RasterDataType::U8,
            output_band: None,
            map_no_data: true,
        },
        sources: SingleRasterSource {
            raster: RasterStacker {
                params: RasterStackerParams {
                    rename_bands: RenameBands::Default,
                    output_origin: None,
                },
                sources: MultipleRasterSources {
                    rasters: vec![raster, mask],
                },
            }
            .typed(),
        },
    }
    .typed()
}

/// An OGR source for the outline of Germany, which has no time.
fn germany(exe_ctx: &mut MockExecutionContext) -> TypedVectorOperator {
    let id: DataId = DatasetId::new().into();
    let name = NamedData::with_system_name("germany");
    exe_ctx.add_meta_data::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>(
        id,
        name.clone(),
        Box::new(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: test_data!("vector/data/germany_polygon.gpkg").into(),
                layer_name: "test_germany".to_owned(),
                data_type: Some(VectorDataType::MultiPolygon),
                time: OgrSourceDatasetTimeType::None,
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: None,
                    x: String::new(),
                    y: None,
                    int: vec![],
                    float: vec![],
                    text: vec![],
                    bool: vec![],
                    datetime: vec![],
                    rename: None,
                }),
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Abort,
                sql_query: None,
                attribute_query: None,
                cache_ttl: None,
            },
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPolygon,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: HashMap::new(),
                time: None,
                bbox: None,
            },
            phantom: PhantomData,
        }),
    );

    OgrSource {
        params: OgrSourceParameters {
            data: name,
            attribute_projection: None,
            attribute_filters: None,
        },
    }
    .typed()
}

/// The pixels of Germany at 0.1° resolution aligned to `(0, 0)`, i.e., 5.8°–15.1° E and 47.2°–55.1° N.
fn germany_bounds() -> GridBoundingBox2D {
    GridBoundingBox2D::new([-551, 58], [-473, 150]).unwrap()
}

fn germany_rasterization(
    exe_ctx: &mut MockExecutionContext,
    time: RasterizationTime,
) -> PolygonRasterization {
    PolygonRasterization {
        params: PolygonRasterizationParams {
            spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
            ..params(BurnValue::default(), time)
        },
        sources: SingleVectorSource {
            vector: germany(exe_ctx),
        },
    }
}

#[tokio::test]
async fn it_rasterizes_germany() {
    let mut exe_ctx = MockExecutionContext::test_default();
    let rasterization = germany_rasterization(&mut exe_ctx, RasterizationTime::Ignore);

    let processor = rasterization
        .typed()
        .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .get_u8()
        .unwrap();

    let bytes = single_timestep_raster_stream_to_geotiff_bytes(
        processor,
        RasterQueryRectangle::new(
            germany_bounds(),
            TimeInterval::new_unchecked(0, 1),
            BandSelection::first(),
        ),
        exe_ctx.mock_query_context_test_default(),
        GdalGeoTiffDatasetMetadata {
            no_data_value: Some(0.),
            spatial_reference: SpatialReference::epsg_4326(),
        },
        GdalGeoTiffOptions {
            as_cog: false,
            compression_num_threads: GdalCompressionNumThreads::NumThreads(2),
            force_big_tiff: false,
        },
        None,
        Box::pin(futures::future::pending()),
        (),
    )
    .await
    .unwrap();

    // The raster equals the output of `gdal_rasterize` on the same grid.
    // It has no-data pixels inside Germany where the polygon cuts out the Elbe up to Hamburg
    // (9.75° E, 53.55° N) and where the Czech Aš salient reaches into Germany (12.15° E, 50.25° N).

    // Delete the file to regenerate it, and check it, e.g., by overlaying it with the polygon in QGIS
    geoengine_datatypes::util::test::save_test_bytes_if_not_exists(
        &bytes,
        test_data!("raster/polygon_rasterization/germany_0_1deg.tif"),
    );

    assert_image_equals_with_format(
        test_data!("raster/polygon_rasterization/germany_0_1deg.tif"),
        &bytes,
        ImageFormat::Tiff,
    );
}

#[tokio::test]
async fn it_clips_ndvi_with_germany() {
    let mut exe_ctx = MockExecutionContext::test_default();
    let ndvi = GdalSource {
        params: GdalSourceParameters::new(add_ndvi_dataset(&mut exe_ctx)),
    }
    .typed();

    // the NDVI has monthly time steps, so the mask uses the same regular time
    let mask = germany_rasterization(
        &mut exe_ctx,
        RasterizationTime::Regular {
            step: TimeStep {
                granularity: TimeGranularity::Months,
                step: 1,
            },
            reference: None,
        },
    )
    .typed();

    let processor = clip(ndvi, mask)
        .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .get_u8()
        .unwrap();

    let colorizer = Colorizer::linear_gradient(
        vec![
            (0.0, RgbaColor::white()).try_into().unwrap(),
            (255.0, RgbaColor::black()).try_into().unwrap(),
        ],
        RgbaColor::transparent(),
        RgbaColor::white(),
        RgbaColor::black(),
    )
    .unwrap()
    .into();

    let (bytes, _) = raster_stream_to_png_bytes(
        processor,
        RasterQueryRectangle::new(
            germany_bounds(),
            TimeInstance::from(DateTime::new_utc(2014, 4, 1, 0, 0, 0)).into(),
            BandSelection::first(),
        ),
        exe_ctx.mock_query_context_test_default(),
        93,
        79,
        None,
        Some(colorizer),
        Box::pin(futures::future::pending()),
    )
    .await
    .unwrap();

    // Delete the file to regenerate it
    geoengine_datatypes::util::test::save_test_bytes_if_not_exists(
        &bytes,
        test_data!("wms/polygon_rasterization_germany_ndvi_clip.png"),
    );

    assert_image_equals(
        test_data!("wms/polygon_rasterization_germany_ndvi_clip.png"),
        &bytes,
    );
}

#[tokio::test]
async fn it_optimizes_to_a_coarser_resolution() {
    let exe_ctx = exe_ctx_4x4();
    let rasterization = initialize(
        params(BurnValue::Count, RasterizationTime::Ignore),
        timed_source(),
        &exe_ctx,
        TIME_SCAN_BATCH_SIZE,
    )
    .await
    .unwrap();

    let optimized = rasterization
        .optimize(SpatialResolution::new(4., 4.).unwrap())
        .unwrap();

    let TypedRasterOperator::PolygonRasterization(optimized) = optimized else {
        panic!("optimized operator should be a polygon rasterization");
    };
    assert_eq!(
        optimized.params,
        PolygonRasterizationParams {
            spatial_resolution: SpatialResolution::new(4., 4.).unwrap(),
            ..params(BurnValue::Count, RasterizationTime::Ignore)
        }
    );

    let unchanged = rasterization
        .optimize(SpatialResolution::new(1., 1.).unwrap())
        .unwrap();
    let TypedRasterOperator::PolygonRasterization(unchanged) = unchanged else {
        panic!("optimized operator should be a polygon rasterization");
    };
    assert_eq!(
        unchanged.params.spatial_resolution,
        SpatialResolution::new(1., 1.).unwrap()
    );

    assert!(
        rasterization
            .optimize(SpatialResolution::new(0.5, 0.5).unwrap())
            .is_err(),
        "a finer resolution is rejected"
    );
}
