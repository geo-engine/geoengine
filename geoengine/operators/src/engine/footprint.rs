use crate::adapters::next_merged_interval;
use crate::util::Result;
use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use geoengine_datatypes::{
    primitives::TimeInterval,
    raster::{GridBoundingBox2D, GridBoundingBoxExt, GridContains, GridIntersection, GridSize},
};
use std::sync::Arc;

/// The most boxes per time step if nothing else is configured, see `footprint_boxes_per_step_limit` of the
/// query context. Footprints with more boxes are merged, so that intersecting and combining them
/// stays cheap: it takes the product of the numbers of boxes.
pub const DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT: usize = 64;

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

    /// Drops boxes covered by others and merges boxes if more than `max_boxes` remain.
    pub fn from_boxes(
        boxes: impl IntoIterator<Item = GridBoundingBox2D>,
        max_boxes: usize,
    ) -> Self {
        let mut kept = Vec::new();
        for grid_box in boxes {
            add_box(&mut kept, grid_box);
            if kept.len() > max_boxes {
                merge_cheapest_pair(&mut kept);
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
    pub fn intersection(&self, other: &Self, max_boxes: usize) -> Self {
        match (self, other) {
            (Self::Unknown, known) | (known, Self::Unknown) => known.clone(),
            (Self::Boxes(a), Self::Boxes(b)) => Self::from_boxes(
                a.iter()
                    .flat_map(|a| b.iter().filter_map(|b| a.intersection(b))),
                max_boxes,
            ),
        }
    }

    /// The combined footprint of two rasters: data may exist where either may have data.
    #[must_use]
    pub fn union(&self, other: &Self, max_boxes: usize) -> Self {
        match (self, other) {
            (Self::Boxes(a), Self::Boxes(b)) if Arc::ptr_eq(a, b) => self.clone(),
            (Self::Boxes(a), Self::Boxes(b)) => {
                Self::from_boxes(a.iter().chain(b.iter()).copied(), max_boxes)
            }
            _ => Self::Unknown,
        }
    }
}

/// The cost of merging two boxes into their hull: first the area the hull adds, then how far the
/// hull is from a square.
///
/// One hull of all boxes would claim the space between scattered scenes, which is wrong for a
/// view from Italy to Denmark. Boxes that touch or overlap merge first, so a block of scenes
/// stays a block. Among pairs that add the same area, such as the neighbors in a row, the pair
/// with the squarest hull merges, so blocks grow in both directions and not into long strips.
/// The added area is the waste measure of Guttman's quadratic R-tree split ("R-Trees: A Dynamic
/// Index Structure for Spatial Searching", 1984), minimized here instead of maximized.
#[derive(Debug, Clone, Copy)]
struct MergeCost {
    added_area: u128,
    long_side: u128,
    short_side: u128,
}

impl MergeCost {
    fn of(a: &GridBoundingBox2D, b: &GridBoundingBox2D) -> Self {
        let area = |grid_box: &GridBoundingBox2D| {
            let [height, width] = grid_box.axis_size();
            height as u128 * width as u128
        };
        let hull = a.extended(b);
        // an overlap of the two boxes is covered once, not twice
        let overlap = a.intersection(b).map_or(0, |overlap| area(&overlap));
        let [height, width] = hull.axis_size();
        Self {
            added_area: area(&hull) - (area(a) + area(b) - overlap),
            long_side: height.max(width) as u128,
            short_side: height.min(width) as u128,
        }
    }

    fn is_below(&self, other: &Self) -> bool {
        // compares the ratios of the sides without floats
        self.added_area < other.added_area
            || (self.added_area == other.added_area
                && self.long_side * other.short_side < other.long_side * self.short_side)
    }
}

/// Adds a box unless another covers it, and drops the boxes it covers.
fn add_box(kept: &mut Vec<GridBoundingBox2D>, grid_box: GridBoundingBox2D) {
    if !kept.iter().any(|other| other.contains(&grid_box)) {
        kept.retain(|other| !grid_box.contains(other));
        kept.push(grid_box);
    }
}

/// Merges the pair of boxes with the cheapest hull (greedy agglomerative clustering). Called
/// after each added box, so it tries the pairs of at most `max_boxes + 1` boxes.
fn merge_cheapest_pair(kept: &mut Vec<GridBoundingBox2D>) {
    let pairs = (0..kept.len()).flat_map(|i| ((i + 1)..kept.len()).map(move |j| (i, j)));
    let Some((i, j, _)) = pairs
        .map(|(i, j)| (i, j, MergeCost::of(&kept[i], &kept[j])))
        .reduce(|best, pair| if pair.2.is_below(&best.2) { pair } else { best })
    else {
        return;
    };
    let hull = kept[i].extended(&kept[j]);
    kept.remove(j);
    kept.remove(i);
    add_box(kept, hull);
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
    max_boxes: usize,
) -> FootprintBounds {
    // an input without a step here has no data here
    match combine {
        FootprintCombine::Union => inputs
            .iter()
            .flatten()
            .fold(FootprintBounds::empty(), |all, bounds| {
                all.union(bounds, max_boxes)
            }),
        FootprintCombine::Intersection => {
            let Some(inputs) = inputs.iter().copied().collect::<Option<Vec<_>>>() else {
                return FootprintBounds::empty();
            };
            inputs
                .into_iter()
                .cloned()
                .reduce(|all, bounds| all.intersection(&bounds, max_boxes))
                .unwrap_or_else(FootprintBounds::empty)
        }
    }
}

/// Zips the footprint streams of several inputs, one step at a time. The steps are those of the
/// merged time streams of the inputs (the same as `TimeIntervalStreamMerge` gives the data path),
/// so inputs with different time steps are split at every start and end. Each input is read
/// only as far as the step needs, so long ranges stay bounded. A step has at most `max_boxes` boxes.
pub fn zip_footprints(
    streams: Vec<BoxStream<'_, Result<Footprint>>>,
    combine: FootprintCombine,
    max_boxes: usize,
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
            combine_bounds(combine, &covering, max_boxes)
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

/// How exact the boxes of a footprint stream have to be: `Availability` answers *when* there is
/// data, `Coverage` also *where*. Both are one query, because every operator combines them by the
/// same rule. A source may answer more exactly than asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FootprintPrecision {
    /// Only whether a step has data. A step with data may carry the whole query bounds as its
    /// box. This is enough to skip empty steps, and it is much cheaper than boxes.
    Availability,
    /// The areas in which data may exist, as exact as the source can tell. This is enough to skip
    /// empty tiles.
    Coverage,
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
pub(crate) mod test_util;

#[cfg(test)]
mod tests {
    use super::test_util::{boxes, grid_box};
    use super::*;
    use futures::{TryStreamExt, stream};

    #[tokio::test]
    async fn it_ends_the_footprint_stream_with_an_error_when_the_query_is_aborted() {
        use crate::engine::{
            MockExecutionContext, ProcessingGraphOperatorPath, QueryContext as _,
            RasterQueryProcessor as _,
        };
        use geoengine_datatypes::primitives::{BandSelection, RasterQueryRectangle};
        use geoengine_datatypes::util::test::TestDefault;

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels =
            geoengine_datatypes::raster::GridShape2D::new_2d(2, 2);
        let mut query_ctx = exe_ctx.mock_query_context_test_default();
        let abort = query_ctx.abort_trigger().unwrap();
        let processor = super::test_util::two_band_source()
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
            BandSelection::first(),
        );

        let mut footprints = processor
            .footprint_query(query, FootprintPrecision::Coverage, &query_ctx)
            .await
            .unwrap();
        abort.abort();

        assert!(matches!(
            footprints.next().await,
            Some(Err(crate::error::Error::QueryCanceled))
        ));
        assert!(footprints.next().await.is_none());
    }

    #[test]
    fn it_drops_covered_boxes_in_any_order() {
        let outer = grid_box(0, 9, 0, 9);
        let inner = grid_box(2, 3, 2, 3);

        for input in [[outer, inner], [inner, outer]] {
            assert_eq!(
                boxes(&FootprintBounds::from_boxes(
                    input,
                    DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT
                )),
                [outer]
            );
        }
    }

    #[test]
    fn it_merges_too_many_boxes_into_a_superset() {
        let input = (0..=DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT as isize)
            .map(|i| grid_box(0, 1, i * 10, i * 10 + 1))
            .collect::<Vec<_>>();

        let bounds =
            FootprintBounds::from_boxes(input.clone(), DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT);

        assert!(boxes(&bounds).len() <= DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT);
        assert!(input.iter().all(|grid_box| bounds.intersects(grid_box)));
    }

    #[test]
    fn it_merges_into_squares_before_strips() {
        // both pairs of neighbors add no empty area, but the strips would make a hull of 1 x 8
        // and the blocks one of 5 x 4
        let (strip_a, strip_b) = (grid_box(0, 0, 5, 8), grid_box(0, 0, 9, 12));
        let (block, below) = (grid_box(0, 3, 0, 3), grid_box(4, 4, 0, 3));
        let bounds = FootprintBounds::from_boxes([strip_a, strip_b, block, below], 3);
        let boxes = boxes(&bounds);

        assert_eq!(boxes.len(), 3);
        assert!(boxes.contains(&grid_box(0, 4, 0, 3)));
        assert!(boxes.contains(&strip_a) && boxes.contains(&strip_b));
    }

    #[test]
    fn it_counts_the_overlap_of_two_boxes_once() {
        // the overlapping pair adds 50 pixels (hull 225, boxes 175 together), the other pair 30
        let (overlapping_a, overlapping_b) = (grid_box(0, 9, 0, 9), grid_box(5, 14, 5, 14));
        let (apart_a, apart_b) = (grid_box(100, 109, 0, 9), grid_box(100, 109, 13, 22));
        let bounds =
            FootprintBounds::from_boxes([overlapping_a, overlapping_b, apart_a, apart_b], 3);
        let boxes = boxes(&bounds);

        assert_eq!(boxes.len(), 3);
        assert!(boxes.contains(&grid_box(100, 109, 0, 22)));
        assert!(boxes.contains(&overlapping_a) && boxes.contains(&overlapping_b));
    }

    #[test]
    fn it_does_not_claim_the_space_between_scattered_scenes() {
        // two blocks of scenes far apart, together more boxes than the cap
        let block = |x0: isize| {
            (0..=DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT as isize / 2)
                .map(move |i| grid_box(0, 9, x0 + i * 10, x0 + i * 10 + 9))
        };
        let input = block(0).chain(block(100_000)).collect::<Vec<_>>();

        let bounds =
            FootprintBounds::from_boxes(input.clone(), DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT);

        assert!(boxes(&bounds).len() <= DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT);
        assert!(input.iter().all(|grid_box| bounds.intersects(grid_box)));
        assert!(
            !bounds.intersects(&grid_box(0, 9, 50_000, 50_009)),
            "the gap must stay free"
        );
    }

    #[test]
    fn it_keeps_a_superset_after_merging() {
        let input = (0..DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT as isize * 3)
            .map(|i| grid_box(0, 1, i * 10, i * 10 + 1))
            .collect::<Vec<_>>();

        let bounds =
            FootprintBounds::from_boxes(input.clone(), DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT);

        assert!(boxes(&bounds).len() <= DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT);
        assert!(input.iter().all(|grid_box| bounds.intersects(grid_box)));
    }

    #[test]
    fn it_tells_empty_from_unknown() {
        assert!(FootprintBounds::empty().is_empty());
        assert!(FootprintBounds::from_boxes([], DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT).is_empty());
        assert!(!FootprintBounds::Unknown.is_empty());

        let any_box = grid_box(0, 1, 0, 1);
        assert!(FootprintBounds::Unknown.intersects(&any_box));
        assert!(!FootprintBounds::empty().intersects(&any_box));
    }

    #[test]
    fn it_treats_unknown_as_absorbing_in_a_union() {
        let known = FootprintBounds::from_boxes(
            [grid_box(0, 1, 0, 1)],
            DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT,
        );

        assert_eq!(
            known.union(
                &FootprintBounds::Unknown,
                DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT
            ),
            FootprintBounds::Unknown
        );
        assert_eq!(
            FootprintBounds::Unknown.union(&known, DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT),
            FootprintBounds::Unknown
        );
    }

    #[test]
    fn it_unions_boxes() {
        let a = grid_box(0, 1, 0, 1);
        let b = grid_box(5, 6, 5, 6);

        let union = FootprintBounds::from_boxes([a], DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT).union(
            &FootprintBounds::from_boxes([b], DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT),
            DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT,
        );

        assert_eq!(boxes(&union), [a, b]);
        assert!(!union.intersects(&grid_box(3, 3, 3, 3)));
    }

    fn step(start: i64, end: i64, boxes: &[GridBoundingBox2D]) -> Footprint {
        Footprint {
            time: TimeInterval::new_unchecked(start, end),
            bounds: FootprintBounds::from_boxes(
                boxes.iter().copied(),
                DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT,
            ),
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
            DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT,
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
        let result = zip_footprints(
            vec![stream::iter(failing).boxed()],
            FootprintCombine::Union,
            DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT,
        )
        .try_collect::<Vec<_>>()
        .await;
        assert!(result.is_err());
    }

    #[test]
    fn unknown_does_not_limit_an_intersection() {
        let known = FootprintBounds::from_boxes(
            [grid_box(0, 1, 0, 1)],
            DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT,
        );

        assert_eq!(
            known.intersection(
                &FootprintBounds::Unknown,
                DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT
            ),
            known
        );
        assert_eq!(
            FootprintBounds::Unknown.intersection(&known, DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT),
            known
        );
        assert_eq!(
            FootprintBounds::Unknown.intersection(
                &FootprintBounds::Unknown,
                DEFAULT_FOOTPRINT_BOXES_PER_STEP_LIMIT
            ),
            FootprintBounds::Unknown
        );
    }
}
