//! Shared helpers of the footprint tests of the operators.
#![allow(clippy::unwrap_used)]

use crate::engine::{
    Footprint, FootprintBounds, FootprintPrecision, MockExecutionContext, MultipleRasterSources,
    ProcessingGraphOperatorPath, RasterBandDescriptor, RasterBandDescriptors, RasterOperator,
    RasterQueryProcessor as _, RasterResultDescriptor, SpatialGridDescriptor, TimeDescriptor,
    TypedRasterOperator,
};
use crate::mock::{MockRasterSource, MockRasterSourceParams};
use crate::processing::{RasterStacker, RasterStackerParams};
use futures::TryStreamExt;
use geoengine_datatypes::{
    primitives::{BandSelection, CacheHint, RasterQueryRectangle, TimeInterval, TimeStep},
    raster::{
        EmptyGrid2D, GeoTransform, Grid2D, GridBoundingBox2D, GridShape2D, GridSize,
        RasterDataType, RasterTile2D, RenameBands,
    },
    spatial_reference::SpatialReference,
    util::test::TestDefault,
};

pub(crate) fn grid_box(
    y_min: isize,
    y_max: isize,
    x_min: isize,
    x_max: isize,
) -> GridBoundingBox2D {
    GridBoundingBox2D::new_min_max(y_min, y_max, x_min, x_max).unwrap()
}

pub(crate) fn boxes(bounds: &FootprintBounds) -> &[GridBoundingBox2D] {
    match bounds {
        FootprintBounds::Unknown => panic!("expected boxes"),
        FootprintBounds::Boxes(boxes) => boxes,
    }
}

/// The tile at x position `tile_x` of the test grid: 2×2 pixels, in one row of tiles, for the
/// time step of 5 that starts at `start`.
pub(crate) fn tile(start: i64, tile_x: isize, band: u32, has_data: bool) -> RasterTile2D<u8> {
    tile_in(
        TimeInterval::new_unchecked(start, start + 5),
        tile_x,
        band,
        has_data,
    )
}

/// Like [`tile`], for any time step.
pub(crate) fn tile_in(
    time: TimeInterval,
    tile_x: isize,
    band: u32,
    has_data: bool,
) -> RasterTile2D<u8> {
    let shape = GridShape2D::new_2d(2, 2);
    RasterTile2D {
        time,
        tile_position: [-1, tile_x].into(),
        band,
        global_geo_transform: TestDefault::test_default(),
        grid_array: if has_data {
            Grid2D::new(shape, vec![1, 2, 3, 4]).unwrap().into()
        } else {
            EmptyGrid2D::new(shape).into()
        },
        properties: Default::default(),
        cache_hint: CacheHint::no_cache(),
    }
}

pub(crate) fn tile_box(tile_x: isize) -> GridBoundingBox2D {
    grid_box(-2, -1, tile_x * 2, tile_x * 2 + 1)
}

pub(crate) fn regular_time() -> TimeDescriptor {
    TimeDescriptor::new_regular_with_epoch(
        Some(TimeInterval::new_unchecked(0, 10)),
        TimeStep::millis(5).unwrap(),
    )
}

/// A source with `band_count` bands. Time steps without tiles need empty tiles to exist.
pub(crate) fn source(
    data: Vec<RasterTile2D<u8>>,
    band_count: u32,
    time: TimeDescriptor,
) -> TypedRasterOperator {
    source_of_tiles(data, band_count, time, 2)
}

/// Four tiles in a row. Only tile 0 has data, and only in the first of the two time steps.
pub(crate) fn one_of_four_tiles_source() -> TypedRasterOperator {
    let tiles = [0, 5]
        .into_iter()
        .flat_map(|start| (0..4).map(move |x| tile(start, x, 0, start == 0 && x == 0)))
        .collect();
    source_of_tiles(tiles, 1, regular_time(), 4)
}

fn source_of_tiles(
    data: Vec<RasterTile2D<u8>>,
    band_count: u32,
    time: TimeDescriptor,
    tiles_in_row: isize,
) -> TypedRasterOperator {
    let bands = (0..band_count)
        .map(|band| RasterBandDescriptor::new_unitless(format!("band{band}")))
        .collect();

    MockRasterSource {
        params: MockRasterSourceParams {
            data,
            result_descriptor: RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::epsg_4326().into(),
                time,
                spatial_grid: SpatialGridDescriptor::source_from_parts(
                    GeoTransform::test_default(),
                    grid_box(-2, -1, 0, tiles_in_row * 2 - 1),
                ),
                bands: RasterBandDescriptors::new(bands).unwrap(),
            },
        },
    }
    .typed()
}

/// Band 0 has data in tile 0 at the first step, band 1 in tile 1 at both steps.
pub(crate) fn two_band_source() -> TypedRasterOperator {
    source(
        vec![
            tile(0, 0, 0, true),
            tile(0, 0, 1, false),
            tile(0, 1, 0, false),
            tile(0, 1, 1, true),
            tile(5, 0, 0, false),
            tile(5, 0, 1, false),
            tile(5, 1, 0, false),
            tile(5, 1, 1, true),
        ],
        2,
        regular_time(),
    )
}

/// Runs the footprint query and the real query, and checks that the footprints have the time
/// steps of `time_query` and that no valid tile lies outside of its footprint, because a
/// footprint that is too tight would silently hide data.
pub(crate) async fn checked_footprints(
    operator: TypedRasterOperator,
    bands: &[u32],
) -> Vec<Footprint> {
    checked_footprints_and_bounds(operator, bands).await.0
}

/// For an operator over [`one_of_four_tiles_source`] that keeps the time steps: checks the footprints
/// like [`checked_footprints`], and that they tell where the data is, i.e. the first step has boxes
/// that leave out the end of the row, and the second step is empty.
pub(crate) async fn assert_footprints_follow_the_data(operator: TypedRasterOperator) {
    let (footprints, bounds) = checked_footprints_and_bounds(operator, &[0]).await;
    assert_eq!(footprints.len(), 2);

    let with_data = boxes(&footprints[0].bounds);
    assert_ne!(with_data, []);
    let end_of_row = grid_box(
        bounds.y_min(),
        bounds.y_max(),
        bounds.x_max(),
        bounds.x_max(),
    );
    assert!(
        !footprints[0].bounds.intersects(&end_of_row),
        "{with_data:?} covers the end of the row of {bounds:?}"
    );

    assert!(footprints[1].bounds.is_empty());
}

/// [`checked_footprints`], with the pixel bounds of the operator that were queried.
async fn checked_footprints_and_bounds(
    operator: TypedRasterOperator,
    bands: &[u32],
) -> (Vec<Footprint>, GridBoundingBox2D) {
    let mut exe_ctx = MockExecutionContext::test_default();
    exe_ctx.tiling_specification.tile_size_in_pixels = GridShape2D::new_2d(2, 2);
    let query_ctx = exe_ctx.mock_query_context_test_default();

    let processor = operator
        .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .get_u8()
        .unwrap();

    // all of the operator's data, on whole tiles
    let bounds = processor
        .result_descriptor()
        .spatial_grid_descriptor()
        .tiling_grid_definition(exe_ctx.tiling_specification)
        .tiling_grid_bounds();
    let query = RasterQueryRectangle::new(
        bounds,
        TimeInterval::new_unchecked(0, 10),
        BandSelection::new(bands.to_vec()).unwrap(),
    );

    let footprints: Vec<Footprint> = processor
        .footprint_query(query.clone(), FootprintPrecision::Coverage, &query_ctx)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    // a footprint stream has exactly the time steps of the time stream
    let times: Vec<TimeInterval> = processor
        .time_query(query.time_interval(), &query_ctx)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        footprints.iter().map(|step| step.time).collect::<Vec<_>>(),
        times
    );

    let tiles: Vec<RasterTile2D<u8>> = processor
        .raster_query(query, &query_ctx)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_ne!(tiles.len(), 0);

    assert_data_within_footprints(&tiles, &footprints);

    (footprints, bounds)
}

/// Checks that every valid pixel of `tiles` lies in the footprint of its time step: a footprint that is
/// too tight would silently hide data, also by less than a tile.
pub(crate) fn assert_data_within_footprints(tiles: &[RasterTile2D<u8>], footprints: &[Footprint]) {
    for tile in tiles.iter().filter(|tile| !tile.is_empty()) {
        let footprint = footprints
            .iter()
            .find(|footprint| footprint.time == tile.time)
            .expect("every time step of the data has a footprint");
        let upper_left = tile.tile_information().global_upper_left_pixel_idx();
        let [y_min, x_min] = upper_left.inner();
        let width = tile.tile_information().tile_size_in_pixels().axis_size_x();
        let materialized = tile.clone().into_materialized_tile();
        for (index, _) in materialized
            .grid_array
            .validity_mask
            .data
            .iter()
            .enumerate()
            .filter(|(_, valid)| **valid)
        {
            let pixel = [
                y_min + (index / width) as isize,
                x_min + (index % width) as isize,
            ];
            assert!(
                footprint
                    .bounds
                    .intersects(&GridBoundingBox2D::new_unchecked(pixel, pixel)),
                "pixel {pixel:?} at {:?} is outside of {:?}",
                tile.time,
                footprint.bounds
            );
        }
    }
}

/// All tiles of the operator, with footprint skipping off or on.
pub(crate) async fn tiles_with_skipping(
    operator: TypedRasterOperator,
    footprint_skipping: bool,
) -> Vec<RasterTile2D<u8>> {
    let mut exe_ctx = MockExecutionContext::test_default();
    exe_ctx.tiling_specification.tile_size_in_pixels = GridShape2D::new_2d(2, 2);
    let mut query_ctx = exe_ctx.mock_query_context_test_default();
    query_ctx.footprint_skipping = footprint_skipping;

    let processor = operator
        .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .get_u8()
        .unwrap();
    let bounds = processor
        .result_descriptor()
        .spatial_grid_descriptor()
        .tiling_grid_definition(exe_ctx.tiling_specification)
        .tiling_grid_bounds();

    processor
        .raster_query(
            RasterQueryRectangle::new(
                bounds,
                TimeInterval::new_unchecked(0, 10),
                BandSelection::first(),
            ),
            &query_ctx,
        )
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

pub(crate) fn step_boxes(footprints: &[Footprint]) -> Vec<&[GridBoundingBox2D]> {
    footprints.iter().map(|step| boxes(&step.bounds)).collect()
}

pub(crate) fn stack(rasters: Vec<TypedRasterOperator>) -> TypedRasterOperator {
    RasterStacker {
        params: RasterStackerParams {
            rename_bands: RenameBands::Default,
        },
        sources: MultipleRasterSources { rasters },
    }
    .typed()
}

/// Two sources: tile 0 at the first step and tile 1 at the first step.
pub(crate) fn stack_inputs() -> Vec<TypedRasterOperator> {
    vec![
        source(
            vec![
                tile(0, 0, 0, true),
                tile(0, 1, 0, false),
                tile(5, 0, 0, false),
                tile(5, 1, 0, false),
            ],
            1,
            regular_time(),
        ),
        source(
            vec![
                tile(0, 0, 0, false),
                tile(0, 1, 0, true),
                tile(5, 0, 0, false),
                tile(5, 1, 0, false),
            ],
            1,
            regular_time(),
        ),
    ]
}
