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
        EmptyGrid2D, GeoTransform, Grid2D, GridBoundingBox2D, GridShape2D, RasterDataType,
        RasterTile2D, RenameBands,
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

/// The tile at x position `tile_x` of the test grid: 2×2 pixels, in one row of tiles.
pub(crate) fn tile(start: i64, tile_x: isize, band: u32, has_data: bool) -> RasterTile2D<u8> {
    let shape = GridShape2D::new_2d(2, 2);
    RasterTile2D {
        time: TimeInterval::new_unchecked(start, start + 5),
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
                    grid_box(-2, -1, 0, 3),
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

/// Runs the footprint query and the real query, and checks that no valid tile lies outside of
/// its footprint, because a footprint that is too tight would silently hide data.
pub(crate) async fn checked_footprints(
    operator: TypedRasterOperator,
    bands: &[u32],
) -> Vec<Footprint> {
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

    let query = RasterQueryRectangle::new(
        grid_box(-2, -1, 0, 3),
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

    for tile in tiles.iter().filter(|tile| !tile.is_empty()) {
        let footprint = footprints
            .iter()
            .find(|footprint| footprint.time == tile.time)
            .expect("every time step of the data has a footprint");
        assert!(
            footprint
                .bounds
                .intersects(&tile.tile_information().global_pixel_bounds()),
            "tile {:?} at {:?} is outside of {:?}",
            tile.tile_position,
            tile.time,
            footprint.bounds
        );
    }

    footprints
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
