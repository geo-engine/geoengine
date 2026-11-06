use crate::adapters::next_merged_interval;
use crate::util::Result;
use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use geoengine_datatypes::{
    primitives::TimeInterval,
    raster::{GridBoundingBox2D, GridBoundingBoxExt, GridContains, GridIntersection},
};
use std::sync::Arc;

/// More boxes per time step than this are merged, so a footprint stays small and cheap to test.
pub const MAX_FOOTPRINT_BOXES: usize = 16;

/// Where a raster may contain data in one time step of a query, in the raster's pixel space.
///
/// This is a superset of the valid pixels: a box can hold no-data, but outside of the boxes there
/// are no valid pixels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FootprintBounds {
    /// The processor cannot tell, so any pixel of the query may hold data.
    Unknown,
    /// Data can only exist inside of these boxes. No boxes means the time step has no data.
    Boxes(Arc<[GridBoundingBox2D]>),
}

impl FootprintBounds {
    pub fn empty() -> Self {
        Self::Boxes(Arc::new([]))
    }

    /// Drops boxes covered by others and merges all boxes into their hull if too many remain.
    pub fn from_boxes(boxes: impl IntoIterator<Item = GridBoundingBox2D>) -> Self {
        let mut kept: Vec<GridBoundingBox2D> = Vec::new();

        for grid_box in boxes {
            if kept.iter().any(|other| other.contains(&grid_box)) {
                continue;
            }
            kept.retain(|other| !grid_box.contains(other));
            kept.push(grid_box);

            if kept.len() > MAX_FOOTPRINT_BOXES {
                // shortcut: one hull is coarse for scattered scenes, merge the closest boxes if the overlay needs more detail
                kept = kept
                    .into_iter()
                    .reduce(|hull, other| hull.extended(&other))
                    .into_iter()
                    .collect();
            }
        }

        Self::Boxes(kept.into())
    }

    /// `true` if the time step certainly has no data.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Unknown => false,
            Self::Boxes(boxes) => boxes.is_empty(),
        }
    }

    pub fn intersects(&self, grid_box: &GridBoundingBox2D) -> bool {
        match self {
            Self::Unknown => true,
            Self::Boxes(boxes) => boxes.iter().any(|other| other.intersects(grid_box)),
        }
    }

    /// The footprint where data may exist in both. Where only one of them can have data, there
    /// is none. A footprint that cannot tell does not limit the other.
    #[must_use]
    pub fn intersection(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Unknown, known) | (known, Self::Unknown) => known.clone(),
            (Self::Boxes(a), Self::Boxes(b)) => Self::from_boxes(
                a.iter()
                    .flat_map(|a| b.iter().filter_map(|b| a.intersection(b))),
            ),
        }
    }

    /// The combined footprint of two rasters: data may exist where either may have data.
    #[must_use]
    pub fn union(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Boxes(a), Self::Boxes(b)) if Arc::ptr_eq(a, b) => self.clone(),
            (Self::Boxes(a), Self::Boxes(b)) => Self::from_boxes(a.iter().chain(b.iter()).copied()),
            _ => Self::Unknown,
        }
    }
}

/// The footprint of one time step of a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Footprint {
    pub time: TimeInterval,
    pub bounds: FootprintBounds,
}

/// How the footprints of several inputs make the footprint of one step of their output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FootprintCombine {
    /// The output has data where any input may have data, as a stack of bands does.
    Union,
    /// The output has data only where all inputs may have data, as an expression does for which
    /// a no-data input gives a no-data output.
    Intersection,
}

fn combine_bounds(
    combine: FootprintCombine,
    inputs: &[Option<&FootprintBounds>],
) -> FootprintBounds {
    // an input without a step here has no data here
    match combine {
        FootprintCombine::Union => inputs
            .iter()
            .flatten()
            .fold(FootprintBounds::empty(), |all, bounds| all.union(bounds)),
        FootprintCombine::Intersection => inputs
            .iter()
            .copied()
            .try_fold(None::<FootprintBounds>, |all, bounds| {
                bounds.map(|bounds| {
                    Some(all.map_or_else(|| bounds.clone(), |all| all.intersection(bounds)))
                })
            })
            .flatten()
            .unwrap_or_else(FootprintBounds::empty),
    }
}

/// Zips the footprint streams of several inputs, one step at a time. The steps are those of the
/// merged time streams of the inputs (the same as `TimeIntervalStreamMerge` gives the data path),
/// so inputs with different time steps are split at every start and end. Each input is read
/// only as far as the step needs, so long ranges stay bounded.
pub fn zip_footprints(
    streams: Vec<BoxStream<'_, Result<Footprint>>>,
    combine: FootprintCombine,
) -> BoxStream<'_, Result<Footprint>> {
    struct Zip<'a> {
        streams: Vec<BoxStream<'a, Result<Footprint>>>,
        heads: Vec<Option<Footprint>>,
        done: Vec<bool>,
        current: Option<TimeInterval>,
    }

    let inputs = streams.len();
    let zip = Zip {
        streams,
        heads: vec![None; inputs],
        done: vec![false; inputs],
        current: None,
    };

    stream::try_unfold(zip, move |mut zip| async move {
        for i in 0..zip.streams.len() {
            if zip.heads[i].is_none() && !zip.done[i] {
                match zip.streams[i].next().await {
                    Some(footprint) => zip.heads[i] = Some(footprint?),
                    None => zip.done[i] = true,
                }
            }
        }

        let active = zip
            .heads
            .iter()
            .flatten()
            .map(|head| head.time)
            .collect::<Vec<_>>();
        let Some(time) = next_merged_interval(zip.current, &active) else {
            return Ok(None);
        };

        let bounds = {
            let covering = zip
                .heads
                .iter()
                .map(|head| {
                    head.as_ref()
                        .filter(|head| {
                            head.time.start() <= time.start() && time.end() <= head.time.end()
                        })
                        .map(|head| &head.bounds)
                })
                .collect::<Vec<_>>();
            combine_bounds(combine, &covering)
        };

        for head in &mut zip.heads {
            if head
                .as_ref()
                .is_some_and(|head| head.time.end() <= time.end())
            {
                *head = None;
            }
        }
        zip.current = Some(time);

        Ok::<_, crate::error::Error>(Some((Footprint { time, bounds }, zip)))
    })
    .boxed()
}

/// How exact the boxes of a footprint stream have to be. A source may answer more exactly than asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FootprintPrecision {
    /// Only whether a step has data. A step with data may carry the whole query bounds as its
    /// box. This is enough to skip empty steps, and it is much cheaper than boxes.
    Availability,
    /// The boxes in which data may exist, enough to skip empty tiles.
    Boxes,
}

/// The answer of a processor that knows its time steps but not where their data is.
pub fn unknown_footprints(
    time_steps: BoxStream<'_, Result<TimeInterval>>,
) -> BoxStream<'_, Result<Footprint>> {
    time_steps
        .map(|time| {
            time.map(|time| Footprint {
                time,
                bounds: FootprintBounds::Unknown,
            })
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        engine::{
            MockExecutionContext, MultipleRasterSources, ProcessingGraphOperatorPath,
            RasterBandDescriptor, RasterBandDescriptors, RasterOperator, RasterQueryProcessor as _,
            RasterResultDescriptor, SingleRasterOrVectorSource, SingleRasterSource,
            SpatialGridDescriptor, TimeDescriptor, TypedRasterOperator,
        },
        mock::{MockRasterSource, MockRasterSourceParams},
        processing::{
            BandFilter, BandFilterParams, BandsByNameOrIndex, Expression, ExpressionParams,
            RasterStacker, RasterStackerParams, TimeShift, TimeShiftParams,
        },
    };
    use futures::{TryStreamExt, stream};
    use geoengine_datatypes::{
        primitives::{BandSelection, CacheHint, RasterQueryRectangle, TimeGranularity, TimeStep},
        raster::{
            EmptyGrid2D, GeoTransform, Grid2D, GridShape2D, RasterDataType, RasterTile2D,
            RenameBands,
        },
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    fn grid_box(y_min: isize, y_max: isize, x_min: isize, x_max: isize) -> GridBoundingBox2D {
        GridBoundingBox2D::new_min_max(y_min, y_max, x_min, x_max).unwrap()
    }

    fn boxes(bounds: &FootprintBounds) -> &[GridBoundingBox2D] {
        match bounds {
            FootprintBounds::Unknown => panic!("expected boxes"),
            FootprintBounds::Boxes(boxes) => boxes,
        }
    }

    #[test]
    fn it_drops_covered_boxes_in_any_order() {
        let outer = grid_box(0, 9, 0, 9);
        let inner = grid_box(2, 3, 2, 3);

        for input in [[outer, inner], [inner, outer]] {
            assert_eq!(boxes(&FootprintBounds::from_boxes(input)), [outer]);
        }
    }

    #[test]
    fn it_merges_too_many_boxes_into_a_superset() {
        let input = (0..=MAX_FOOTPRINT_BOXES as isize)
            .map(|i| grid_box(0, 1, i * 10, i * 10 + 1))
            .collect::<Vec<_>>();

        let bounds = FootprintBounds::from_boxes(input.clone());

        assert_eq!(boxes(&bounds).len(), 1);
        assert!(
            input
                .iter()
                .all(|grid_box| boxes(&bounds)[0].contains(grid_box))
        );
    }

    #[test]
    fn it_keeps_a_superset_after_merging() {
        let input = (0..MAX_FOOTPRINT_BOXES as isize * 3)
            .map(|i| grid_box(0, 1, i * 10, i * 10 + 1))
            .collect::<Vec<_>>();

        let bounds = FootprintBounds::from_boxes(input.clone());

        assert!(boxes(&bounds).len() <= MAX_FOOTPRINT_BOXES);
        assert!(input.iter().all(|grid_box| bounds.intersects(grid_box)));
    }

    #[test]
    fn it_tells_empty_from_unknown() {
        assert!(FootprintBounds::empty().is_empty());
        assert!(FootprintBounds::from_boxes([]).is_empty());
        assert!(!FootprintBounds::Unknown.is_empty());

        let any_box = grid_box(0, 1, 0, 1);
        assert!(FootprintBounds::Unknown.intersects(&any_box));
        assert!(!FootprintBounds::empty().intersects(&any_box));
    }

    #[test]
    fn it_treats_unknown_as_absorbing_in_a_union() {
        let known = FootprintBounds::from_boxes([grid_box(0, 1, 0, 1)]);

        assert_eq!(
            known.union(&FootprintBounds::Unknown),
            FootprintBounds::Unknown
        );
        assert_eq!(
            FootprintBounds::Unknown.union(&known),
            FootprintBounds::Unknown
        );
    }

    #[test]
    fn it_unions_boxes() {
        let a = grid_box(0, 1, 0, 1);
        let b = grid_box(5, 6, 5, 6);

        let union = FootprintBounds::from_boxes([a]).union(&FootprintBounds::from_boxes([b]));

        assert_eq!(boxes(&union), [a, b]);
        assert!(!union.intersects(&grid_box(3, 3, 3, 3)));
    }

    /// The tile at x position `tile_x` of the test grid: 2×2 pixels, in one row of tiles.
    fn tile(start: i64, tile_x: isize, band: u32, has_data: bool) -> RasterTile2D<u8> {
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

    fn tile_box(tile_x: isize) -> GridBoundingBox2D {
        grid_box(-2, -1, tile_x * 2, tile_x * 2 + 1)
    }

    fn regular_time() -> TimeDescriptor {
        TimeDescriptor::new_regular_with_epoch(
            Some(TimeInterval::new_unchecked(0, 10)),
            TimeStep::millis(5).unwrap(),
        )
    }

    /// A source with `band_count` bands. Time steps without tiles need empty tiles to exist.
    fn source(
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
    fn two_band_source() -> TypedRasterOperator {
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
    async fn checked_footprints(operator: TypedRasterOperator, bands: &[u32]) -> Vec<Footprint> {
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
            .footprint_query(query.clone(), FootprintPrecision::Boxes, &query_ctx)
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

    fn step_boxes(footprints: &[Footprint]) -> Vec<&[GridBoundingBox2D]> {
        footprints.iter().map(|step| boxes(&step.bounds)).collect()
    }

    #[tokio::test]
    async fn band_filter_answers_for_the_selected_input_band() {
        let band_filter = BandFilter {
            params: BandFilterParams {
                bands: BandsByNameOrIndex::Name(vec!["band1".to_string()]),
            },
            sources: SingleRasterSource {
                raster: two_band_source(),
            },
        }
        .typed();

        let footprints = checked_footprints(band_filter, &[0]).await;

        assert_eq!(
            step_boxes(&footprints),
            [[tile_box(1)].as_slice(), [tile_box(1)].as_slice()]
        );
    }

    fn stack(rasters: Vec<TypedRasterOperator>) -> TypedRasterOperator {
        RasterStacker {
            params: RasterStackerParams {
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources { rasters },
        }
        .typed()
    }

    /// Two sources: tile 0 at the first step and tile 1 at the first step.
    fn stack_inputs() -> Vec<TypedRasterOperator> {
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

    #[tokio::test]
    async fn raster_stacker_unions_the_sources_of_the_selected_bands() {
        let both = checked_footprints(stack(stack_inputs()), &[0, 1]).await;
        assert_eq!(
            step_boxes(&both),
            [[tile_box(0), tile_box(1)].as_slice(), [].as_slice()]
        );

        let second = checked_footprints(stack(stack_inputs()), &[1]).await;
        assert_eq!(
            step_boxes(&second),
            [[tile_box(1)].as_slice(), [].as_slice()]
        );
    }

    #[tokio::test]
    async fn raster_stacker_zips_sources_with_other_time_steps() {
        // the first source has one step of ten, the second two of five: the stack splits at 5
        let slow = source(
            vec![tile(0, 0, 0, true), tile(0, 1, 0, false)],
            1,
            TimeDescriptor::new_irregular(None),
        );
        let fast = source(
            vec![
                tile(0, 0, 0, false),
                tile(0, 1, 0, true),
                tile(5, 0, 0, false),
                tile(5, 1, 0, true),
            ],
            1,
            TimeDescriptor::new_irregular(None),
        );

        let footprints = checked_footprints(stack(vec![slow, fast]), &[0, 1]).await;

        // every step has data in one of the sources, and none is unknown
        assert!(footprints.len() >= 2);
        assert!(
            footprints
                .iter()
                .all(|step| step.bounds != FootprintBounds::Unknown)
        );
    }

    fn expression(map_no_data: bool) -> TypedRasterOperator {
        Expression {
            params: ExpressionParams {
                expression: "A + B".to_string(),
                output_type: RasterDataType::U8,
                output_band: None,
                map_no_data,
            },
            sources: SingleRasterSource {
                raster: stack(stack_inputs()),
            },
        }
        .typed()
    }

    #[tokio::test]
    async fn expression_has_data_only_where_all_inputs_have() {
        // the inputs have data in different tiles, so no pixel has data in both
        let footprints = checked_footprints(expression(false), &[0]).await;
        assert_eq!(step_boxes(&footprints), [[].as_slice(), [].as_slice()]);

        // here both have data in tile 1, and only the first in tile 0
        let overlapping = Expression {
            params: ExpressionParams {
                expression: "A + B".to_string(),
                output_type: RasterDataType::U8,
                output_band: None,
                map_no_data: false,
            },
            sources: SingleRasterSource {
                raster: stack(vec![
                    source(
                        vec![
                            tile(0, 0, 0, true),
                            tile(0, 1, 0, true),
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
                ]),
            },
        }
        .typed();
        let footprints = checked_footprints(overlapping, &[0]).await;
        assert_eq!(
            step_boxes(&footprints),
            [[tile_box(1)].as_slice(), [].as_slice()]
        );
    }

    #[tokio::test]
    async fn expression_cannot_tell_if_it_maps_no_data() {
        let footprints = checked_footprints(expression(true), &[0]).await;

        assert_eq!(footprints.len(), 2);
        assert!(
            footprints
                .iter()
                .all(|step| step.bounds == FootprintBounds::Unknown)
        );
    }

    fn step(start: i64, end: i64, boxes: &[GridBoundingBox2D]) -> Footprint {
        Footprint {
            time: TimeInterval::new_unchecked(start, end),
            bounds: FootprintBounds::from_boxes(boxes.iter().copied()),
        }
    }

    async fn zipped(
        a: Vec<Footprint>,
        b: Vec<Footprint>,
        combine: FootprintCombine,
    ) -> Vec<Footprint> {
        zip_footprints(
            vec![
                stream::iter(a.into_iter().map(Ok)).boxed(),
                stream::iter(b.into_iter().map(Ok)).boxed(),
            ],
            combine,
        )
        .try_collect()
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn zip_makes_the_steps_of_the_time_merge() {
        let (box_a, box_b) = (grid_box(0, 1, 0, 1), grid_box(5, 6, 5, 6));
        let a = vec![step(0, 10, &[box_a]), step(10, 20, &[])];
        let b = vec![step(0, 5, &[box_b]), step(5, 20, &[box_b])];

        // the same steps as the merge of the time streams gives the data path
        let times = |steps: &[Footprint]| {
            stream::iter(
                steps
                    .iter()
                    .map(|step| Ok::<_, crate::error::Error>(step.time))
                    .collect::<Vec<_>>(),
            )
            .boxed()
        };
        let expected: Vec<TimeInterval> =
            crate::adapters::TimeIntervalStreamMerge::new(vec![times(&a), times(&b)])
                .try_collect()
                .await
                .unwrap();

        let union = zipped(a.clone(), b.clone(), FootprintCombine::Union).await;
        assert_eq!(
            union.iter().map(|step| step.time).collect::<Vec<_>>(),
            expected
        );
        // data may exist in a step where any input has data, and in none where all have none
        assert!(union.iter().all(|step| !step.bounds.is_empty()));

        let intersection = zipped(a, b, FootprintCombine::Intersection).await;
        assert_eq!(intersection.len(), expected.len());
        assert!(intersection.iter().all(|step| step.bounds.is_empty()));
    }

    #[tokio::test]
    async fn zip_intersects_the_boxes_of_a_step() {
        let a = vec![step(0, 10, &[grid_box(0, 9, 0, 9)])];
        let b = vec![step(0, 10, &[grid_box(5, 14, 5, 14)])];

        let zipped = zipped(a, b, FootprintCombine::Intersection).await;

        assert_eq!(boxes(&zipped[0].bounds), [grid_box(5, 9, 5, 9)]);
    }

    #[tokio::test]
    async fn zip_treats_an_input_without_a_step_as_empty_and_passes_errors() {
        let some_box = grid_box(0, 1, 0, 1);
        // the second input ends early
        let a = vec![step(0, 10, &[some_box]), step(10, 20, &[some_box])];
        let b = vec![step(0, 10, &[some_box])];

        let union = zipped(a.clone(), b.clone(), FootprintCombine::Union).await;
        assert_eq!(union.len(), 2);
        let intersection = zipped(a, b, FootprintCombine::Intersection).await;
        assert_eq!(boxes(&intersection[0].bounds), [some_box]);
        assert!(intersection[1].bounds.is_empty());

        let failing = vec![
            Ok(step(0, 10, &[some_box])),
            Err(crate::error::Error::MustNotHappen {
                message: "x".into(),
            }),
        ];
        let result = zip_footprints(vec![stream::iter(failing).boxed()], FootprintCombine::Union)
            .try_collect::<Vec<_>>()
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn unknown_does_not_limit_an_intersection() {
        let known = FootprintBounds::from_boxes([grid_box(0, 1, 0, 1)]);

        assert_eq!(known.intersection(&FootprintBounds::Unknown), known);
        assert_eq!(FootprintBounds::Unknown.intersection(&known), known);
        assert_eq!(
            FootprintBounds::Unknown.intersection(&FootprintBounds::Unknown),
            FootprintBounds::Unknown
        );
    }

    #[tokio::test]
    async fn time_shift_answers_with_the_output_times() {
        let shifted = TimeShift {
            params: TimeShiftParams::Relative {
                granularity: TimeGranularity::Millis,
                value: 5,
            },
            sources: SingleRasterOrVectorSource {
                source: stack_inputs().remove(0).into(),
            },
        }
        .typed();

        // shifted by one step, the query of [0, 5) reads the empty input step [5, 10)
        let footprints = checked_footprints(shifted, &[0]).await;

        assert_eq!(footprints[0].time, TimeInterval::new_unchecked(0, 5));
        assert!(footprints[0].bounds.is_empty());
    }
}
