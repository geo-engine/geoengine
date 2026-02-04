use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::adapters::RasterStreamExt;
use crate::engine::{
    BoxRasterQueryProcessor, CanonicOperatorName, ExecutionContext, InitializedRasterOperator,
    InitializedSources, Operator, OperatorName, QueryContext, QueryProcessor, RasterOperator,
    RasterQueryProcessor, RasterResultDescriptor, ResultDescriptor, SingleRasterSource,
    TypedRasterQueryProcessor, WorkflowOperatorPath,
};

use crate::optimization::OptimizationError;
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{FutureExt, Stream};
use geoengine_datatypes::primitives::{BandSelection, RasterQueryRectangle, SpatialResolution};
use geoengine_datatypes::raster::{
    GridBoundingBox2D, GridIdx2D, GridIndexAccess, MapElements, MapIndexedElements, RasterDataType,
    RasterTile2D,
};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use tokio::task::JoinHandle;

const MAX_WINDOW_SIZE: u32 = 8;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BandNeighborhoodAggregateParams {
    pub aggregate: NeighborhoodAggregate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum NeighborhoodAggregate {
    // approximate the first derivative using the central difference method
    #[serde(rename_all = "camelCase")]
    FirstDerivative { band_distance: BandDistance },
    // TODO: SecondDerivative,
    #[serde(rename_all = "camelCase")]
    Average { window_size: u32 },
    // TODO: Savitzky-Golay Filter
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum BandDistance {
    EquallySpaced { distance: f64 },
    // TODO: give `RasterBandDescriptor` a dimensions and give the user the (default) option to use it here
}

/// This `QueryProcessor` performs a pixel-wise aggregate over surrounding bands.
// TODO: different name like BandMovingWindowAggregate?
pub type BandNeighborhoodAggregate = Operator<BandNeighborhoodAggregateParams, SingleRasterSource>;

impl OperatorName for BandNeighborhoodAggregate {
    const TYPE_NAME: &'static str = "BandNeighborhoodAggregate";
}

#[derive(Debug, Snafu)]
#[snafu(
    visibility(pub(crate)),
    context(suffix(false)), // disables default `Snafu` suffix
    module(error))
]
pub enum BandNeighborhoodAggregateError {
    #[snafu(display(
        "First derivative needs at least two input bands. Input raster has only one band.",
    ))]
    FirstDerivativeNeedsAtLeastTwoBands,

    #[snafu(display(
        "The distance of the bands for computing the first derivative must be positive, found {distance}."
    ))]
    FirstDerivativeDistanceMustBePositive { distance: f64 },

    #[snafu(display("The window size for the average must be odd, found {window_size}."))]
    AverageWindowSizeMustBeOdd { window_size: u32 },

    #[snafu(display(
        "The window size for is too large (max. {MAX_WINDOW_SIZE}), found {window_size}."
    ))]
    WindowSizeTooLarge { window_size: u32 },
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for BandNeighborhoodAggregate {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let name = CanonicOperatorName::from(&self);

        let source = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?
            .raster;

        let in_descriptor = source.result_descriptor();

        match &self.params.aggregate {
            NeighborhoodAggregate::FirstDerivative { band_distance } => {
                if in_descriptor.bands.count() <= 1 {
                    return Err(
                        BandNeighborhoodAggregateError::FirstDerivativeNeedsAtLeastTwoBands.into(),
                    );
                }

                match band_distance {
                    BandDistance::EquallySpaced { distance } => {
                        if *distance <= 0.0 {
                            return Err(
                        BandNeighborhoodAggregateError::FirstDerivativeDistanceMustBePositive{distance:*distance}
                            .into(),
                    );
                        }
                    }
                }
            }
            NeighborhoodAggregate::Average { window_size } => {
                if window_size % 2 == 0 {
                    return Err(BandNeighborhoodAggregateError::AverageWindowSizeMustBeOdd {
                        window_size: *window_size,
                    }
                    .into());
                }

                if *window_size > MAX_WINDOW_SIZE {
                    return Err(BandNeighborhoodAggregateError::WindowSizeTooLarge {
                        window_size: *window_size,
                    }
                    .into());
                }
            }
        }

        let result_descriptor = in_descriptor.map_data_type(|_| RasterDataType::F64);

        Ok(Box::new(InitializedBandNeighborhoodAggregate {
            name,
            path,
            result_descriptor,
            source,
            aggregate: self.params.aggregate,
        }))
    }

    span_fn!(BandNeighborhoodAggregate);
}

pub struct InitializedBandNeighborhoodAggregate {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
    aggregate: NeighborhoodAggregate,
}

impl InitializedRasterOperator for InitializedBandNeighborhoodAggregate {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        Ok(TypedRasterQueryProcessor::F64(
            BandNeighborhoodAggregateProcessor::new(
                self.source.query_processor()?.into_f64(),
                self.result_descriptor.clone(),
                self.aggregate.clone(),
            )
            .boxed(),
        ))
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        BandNeighborhoodAggregate::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }

    fn optimize(
        &self,
        target_resolution: SpatialResolution,
    ) -> Result<Box<dyn RasterOperator>, OptimizationError> {
        Ok(BandNeighborhoodAggregate {
            params: BandNeighborhoodAggregateParams {
                aggregate: self.aggregate.clone(),
            },
            sources: SingleRasterSource {
                raster: self.source.optimize(target_resolution)?,
            },
        }
        .boxed())
    }
}

pub(crate) struct BandNeighborhoodAggregateProcessor {
    source: BoxRasterQueryProcessor<f64>,
    result_descriptor: RasterResultDescriptor,
    aggregate: NeighborhoodAggregate,
}

impl BandNeighborhoodAggregateProcessor {
    pub fn new(
        source: BoxRasterQueryProcessor<f64>,
        result_descriptor: RasterResultDescriptor,
        aggregate: NeighborhoodAggregate,
    ) -> Self {
        Self {
            source,
            result_descriptor,
            aggregate,
        }
    }
}

#[async_trait]
impl QueryProcessor for BandNeighborhoodAggregateProcessor {
    type Output = RasterTile2D<f64>;
    type ResultDescription = RasterResultDescriptor;
    type Selection = BandSelection;
    type SpatialBounds = GridBoundingBox2D;

    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<f64>>>> {
        // query the source with all bands, to compute the aggregate
        // then, select only the queried bands
        // TODO: avoid computing the aggregate for bands that are not queried
        let source_result_descriptor = self.source.raster_result_descriptor();
        let source_query = RasterQueryRectangle::new(
            query.spatial_bounds(),
            query.time_interval(),
            (&source_result_descriptor.bands).into(),
        );

        let must_extract_bands = query.attributes() != source_query.attributes();

        let aggregate = match &self.aggregate {
            NeighborhoodAggregate::FirstDerivative { band_distance } => {
                let band_distance = band_distance.clone();
                Box::pin(
                    BandNeighborhoodAggregateStream::<_, FirstDerivativeAccu, _>::new(
                        self.source.raster_query(source_query, ctx).await?,
                        self.result_descriptor.bands.count(),
                        move || {
                            FirstDerivativeAccu::new(
                                self.result_descriptor.bands.count(),
                                &band_distance,
                            )
                        },
                    ),
                ) as BoxStream<'a, Result<RasterTile2D<f64>>>
            }
            NeighborhoodAggregate::Average { window_size } => Box::pin(
                BandNeighborhoodAggregateStream::<_, MovingAverageAccu, _>::new(
                    self.source.raster_query(source_query, ctx).await?,
                    self.result_descriptor.bands.count(),
                    move || {
                        MovingAverageAccu::new(self.result_descriptor.bands.count(), *window_size)
                    },
                ),
            )
                as BoxStream<'a, Result<RasterTile2D<f64>>>,
        };

        if must_extract_bands {
            Ok(Box::pin(aggregate.extract_bands(
                query.attributes().as_vec(),
                source_result_descriptor.bands.count(),
            )))
        } else {
            Ok(aggregate)
        }
    }

    fn result_descriptor(&self) -> &Self::ResultDescription {
        &self.result_descriptor
    }
}

#[async_trait]
impl RasterQueryProcessor for BandNeighborhoodAggregateProcessor {
    type RasterType = f64;

    async fn _time_query<'a>(
        &'a self,
        query: geoengine_datatypes::primitives::TimeInterval,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<geoengine_datatypes::primitives::TimeInterval>>> {
        self.source.time_query(query, ctx).await
    }
}

#[pin_project(project = BandNeighborhoodAggregateStreamProjection)]
struct BandNeighborhoodAggregateStream<S, A, AF> {
    #[pin]
    input_stream: S,
    current_input_band_idx: u32,
    current_output_band_idx: u32,
    num_bands: u32,
    state: State<A>,
    new_accu_fn: AF,
}

enum State<A> {
    Invalid,                // invalid state, used only during transition
    ComputeNextBandTile(A), // initial state, where we query the accu for the next output band
    AwaitNextBandTile(JoinHandle<(Option<RasterTile2D<f64>>, A)>), // the accu is queried for the next output band and the result is awaited
    ConsumeNextSourceBandTile(A),                                  // consume the next input band
    AwaitAccumulate(JoinHandle<(Result<()>, A)>), // accumulate the next input band into the accu
    Finished,                                     // the stream is finished
}

impl<S, A, AF> BandNeighborhoodAggregateStream<S, A, AF>
where
    S: Stream<Item = Result<RasterTile2D<f64>>> + Unpin,
    A: Accu,
    AF: Fn() -> A,
{
    pub fn new(input_stream: S, num_bands: u32, new_accu_fn: AF) -> Self {
        Self {
            input_stream,
            current_input_band_idx: 0,
            current_output_band_idx: 0,
            num_bands,
            state: State::ConsumeNextSourceBandTile(new_accu_fn()),
            new_accu_fn,
        }
    }
}

trait Accu {
    fn add_tile(&mut self, tile: RasterTile2D<f64>) -> Result<()>;
    // try to produce the next band, returns None when the accu is not yet ready (needs more bands)
    // this method also removes all bands that are not longer needed for future bands
    fn next_band_tile(&mut self) -> Option<RasterTile2D<f64>>;
}

// this stream consumes an input stream, collects bands of a tile and aggregates them
// it outputs the aggregated tile bands as soon as they are fully processed
impl<S, A, AF> Stream for BandNeighborhoodAggregateStream<S, A, AF>
where
    S: Stream<Item = Result<RasterTile2D<f64>>> + Unpin,
    A: Accu + Send + 'static,
    AF: Fn() -> A,
{
    type Item = Result<RasterTile2D<f64>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let BandNeighborhoodAggregateStreamProjection {
            mut input_stream,
            current_input_band_idx,
            current_output_band_idx,
            num_bands,
            state,
            new_accu_fn,
        } = self.as_mut().project();

        // process in a loop to step through the state machine until a `Poll` is returned
        loop {
            let (new_state, poll) = match std::mem::replace(state, State::Invalid) {
                State::ComputeNextBandTile(accu) => (
                    compute_next_band_tile(
                        current_output_band_idx,
                        num_bands,
                        current_input_band_idx,
                        accu,
                        new_accu_fn,
                    ),
                    None,
                ),
                State::AwaitNextBandTile(fut) => {
                    await_next_band_tile(fut, cx, current_output_band_idx)
                }
                State::ConsumeNextSourceBandTile(accu) => {
                    let (new_state, poll, new_input_band_index) = consume_next_source_band_tile(
                        input_stream.as_mut(),
                        cx,
                        accu,
                        *current_input_band_idx,
                        *num_bands,
                    );
                    *current_input_band_idx = new_input_band_index;

                    (new_state, poll)
                }
                State::AwaitAccumulate(mut fut) => {
                    // await the `add_tile` future
                    match fut.poll_unpin(cx) {
                        Poll::Ready(Ok((Ok(()), accu))) => (State::ComputeNextBandTile(accu), None),
                        Poll::Ready(Ok((Err(e), _))) => {
                            (State::Finished, Some(Poll::Ready(Some(Err(e)))))
                        }
                        Poll::Ready(Err(e)) => {
                            (State::Finished, Some(Poll::Ready(Some(Err(e.into())))))
                        }
                        Poll::Pending => (State::AwaitAccumulate(fut), Some(Poll::Pending)),
                    }
                }
                State::Finished => {
                    *state = State::Finished;
                    return Poll::Ready(None);
                }
                State::Invalid => {
                    debug_assert!(false, "Invalid state reached");
                    *state = State::Finished;
                    return Poll::Ready(Some(Err(crate::error::Error::MustNotHappen {
                        message: "Invalid state reached".to_string(),
                    })));
                }
            };

            *state = new_state;
            if let Some(poll) = poll {
                return poll;
            }
        }
    }
}

type PollResult = Poll<Option<Result<RasterTile2D<f64>>>>;

fn await_next_band_tile<A>(
    mut fut: JoinHandle<(Option<RasterTile2D<f64>>, A)>,
    cx: &mut Context,
    current_output_band_idx: &mut u32,
) -> (State<A>, Option<PollResult>)
where
    A: Accu,
{
    let next = match fut.poll_unpin(cx) {
        Poll::Ready(next) => next,
        Poll::Pending => {
            return (State::AwaitNextBandTile(fut), Some(Poll::Pending));
        }
    };

    let (next_band_tile, accu) = match next {
        Ok(next) => next,
        Err(e) => {
            return (State::Finished, Some(Poll::Ready(Some(Err(e.into())))));
        }
    };

    if let Some(next_band_tile) = next_band_tile {
        // the accu has a tile to output
        debug_assert!(
            next_band_tile.band == *current_output_band_idx,
            "unexpected output band index"
        );
        *current_output_band_idx += 1;
        return (
            State::ComputeNextBandTile(accu),
            Some(Poll::Ready(Some(Ok(next_band_tile)))),
        );
    }

    // nothing in the accu, we either need more inputs or are done with all bands of the tile
    (State::ConsumeNextSourceBandTile(accu), None)
}

fn compute_next_band_tile<A, AF>(
    current_output_band_idx: &mut u32,
    num_bands: &mut u32,
    current_input_band_idx: &mut u32,
    mut accu: A,
    new_accu_fn: &mut AF,
) -> State<A>
where
    A: Accu + Send + 'static,
    AF: Fn() -> A,
{
    // only check accu if there are still more bands to produce for the current tile, otherwise go to consume next
    if *current_output_band_idx >= *num_bands {
        // all output bands were produced, reset the accu for the next tile
        debug_assert!(
            *current_input_band_idx == *num_bands,
            "not all bands were consumed before finishing"
        );

        accu = new_accu_fn();
        *current_input_band_idx = 0;
        *current_output_band_idx = 0;

        State::ConsumeNextSourceBandTile(accu)
    } else {
        // query the accc for a tile
        let fut = crate::util::spawn_blocking(move || (accu.next_band_tile(), accu));

        State::AwaitNextBandTile(fut)
    }
}

fn consume_next_source_band_tile<S, A>(
    input_stream: Pin<&mut S>,
    cx: &mut Context,
    mut accu: A,
    current_input_band_idx: u32,
    num_bands: u32,
) -> (State<A>, Option<PollResult>, u32)
where
    S: Stream<Item = Result<RasterTile2D<f64>>> + Unpin,
    A: Accu + Send + 'static,
{
    // await the next input tile
    let tile = match input_stream.poll_next(cx) {
        Poll::Ready(tile) => tile,
        Poll::Pending => {
            return (
                State::ConsumeNextSourceBandTile(accu),
                Some(Poll::Pending),
                current_input_band_idx,
            );
        }
    };

    let tile = match tile {
        Some(Ok(tile)) => tile,
        Some(Err(e)) => {
            return (
                State::Finished,
                Some(Poll::Ready(Some(Err(e)))),
                current_input_band_idx,
            );
        }
        None => {
            if current_input_band_idx > 0 && current_input_band_idx < num_bands - 1 {
                // stream must not end in the middle of a tile
                return (
                    State::Finished,
                    Some(Poll::Ready(Some(Err(crate::error::Error::MustNotHappen {
                        message: "Unexpected end of stream".to_string(),
                    })))),
                    current_input_band_idx,
                );
            }

            // stream ended properly
            return (
                State::Finished,
                Some(Poll::Ready(None)),
                current_input_band_idx,
            );
        }
    };

    debug_assert!(
        tile.band == current_input_band_idx,
        "unexpected input band index: expected {} found {}",
        current_input_band_idx,
        tile.band
    );

    // accumulate the input tile asynchronously
    let fut = crate::util::spawn_blocking(move || (accu.add_tile(tile), accu));

    (
        State::AwaitAccumulate(fut),
        None,
        current_input_band_idx + 1,
    )
}

/// Approximate the first derivative using the central difference method
/// `f′(x_i​) ≈ (​y_{i+1} ​ − y_{i−1​​}) / (x_{i+1}​ − x_{i−1})`
/// and forward/backward difference for the endpoints
/// `f′(x_1​) ≈ (y_2 - y_1) / (x_2 - x_1)`
/// `f′(x_n​) ≈ (y_n - y_{n-1}) / (x_n - x_{n-1})`
#[derive(Clone)]
pub struct FirstDerivativeAccu {
    /// hold the last three input bands to compute the first derivative
    input_band_tiles: VecDeque<(u32, RasterTile2D<f64>)>,
    output_band_idx: u32,
    num_bands: u32,
    band_distances: Vec<f64>,
}

impl FirstDerivativeAccu {
    pub fn new(num_bands: u32, band_distances: &BandDistance) -> Self {
        let band_distances = match band_distances {
            BandDistance::EquallySpaced { distance } => vec![*distance; (num_bands - 1) as usize],
        };

        debug_assert!(
            band_distances.len() == (num_bands - 1) as usize,
            "unexpected number of band distances"
        );

        Self {
            input_band_tiles: VecDeque::new(),
            output_band_idx: 0,
            num_bands,
            band_distances,
        }
    }
}

impl Accu for FirstDerivativeAccu {
    fn add_tile(&mut self, tile: RasterTile2D<f64>) -> Result<()> {
        let next_idx = self.input_band_tiles.back().map_or(0, |t| t.0 + 1);
        self.input_band_tiles.push_back((next_idx, tile));
        Ok(())
    }

    fn next_band_tile(&mut self) -> Option<RasterTile2D<f64>> {
        if self.output_band_idx >= self.num_bands {
            debug_assert!(false, "no more bands to produce");
            return None;
        }

        let prev = self.input_band_tiles.front()?;
        let next = if self.output_band_idx == 0 || self.output_band_idx == self.num_bands - 1 {
            // special case because there is no predecessor for the first band and no successor for the last band
            self.input_band_tiles.get(1)?
        } else {
            self.input_band_tiles.get(2)?
        };

        // TODO: the divisor should be the difference of the bands on a spectrum. In order to compute this, bands first need a dimension.
        let divisor = if self.output_band_idx == 0 {
            self.band_distances[0]
        } else if self.output_band_idx == self.num_bands - 1 {
            // special case because there is no predecessor or successor for the first and last band
            self.band_distances[self.band_distances.len() - 1]
        } else {
            self.band_distances[self.output_band_idx as usize - 1]
                + self.band_distances[self.output_band_idx as usize]
        };

        let mut out = prev
            .1
            .clone()
            .map_indexed_elements(|idx: GridIdx2D, prev_value| {
                let next_value = next.1.get_at_grid_index(idx).unwrap_or(None);

                match (prev_value, next_value) {
                    (Some(prev), Some(next)) => Some((next - prev) / divisor),
                    _ => None,
                }
            });
        out.band = self.output_band_idx;

        if self.output_band_idx != 0 {
            // unless we are at the first output band, we no longer need the first input band
            self.input_band_tiles.pop_front();
        }

        self.output_band_idx += 1;

        Some(out)
    }
}

/// Compute the moving average of the bands with a given window size
/// For the borders, the window is reduced to the available bands
pub struct MovingAverageAccu {
    input_band_tiles: VecDeque<(u32, RasterTile2D<f64>)>,
    output_band_idx: u32,
    num_source_bands: u32,
    window_size: u32,
}

impl MovingAverageAccu {
    pub fn new(num_source_bands: u32, window_size: u32) -> Self {
        debug_assert!(window_size % 2 == 1, "window size must be odd");
        Self {
            input_band_tiles: VecDeque::new(),
            output_band_idx: 0,
            num_source_bands,
            window_size,
        }
    }
}

impl Accu for MovingAverageAccu {
    fn add_tile(&mut self, tile: RasterTile2D<f64>) -> Result<()> {
        let next_idx = self.input_band_tiles.back().map_or(0, |t| t.0 + 1);
        self.input_band_tiles.push_back((next_idx, tile));
        Ok(())
    }

    fn next_band_tile(&mut self) -> Option<RasterTile2D<f64>> {
        if self.output_band_idx >= self.num_source_bands {
            debug_assert!(false, "no more bands to produce");
            return None;
        }

        // compute bands required for the window
        let window_radius = self.window_size / 2;
        let first_band = self.output_band_idx.saturating_sub(window_radius);

        debug_assert!(
            self.input_band_tiles
                .front()
                .is_some_and(|t| t.0 == first_band),
            "unexpected first band in queue"
        );

        let last_band = if self.output_band_idx + window_radius >= self.num_source_bands {
            self.num_source_bands - 1
        } else {
            self.output_band_idx + window_radius
        };

        if self.input_band_tiles.back().is_none_or(|t| t.0 < last_band) {
            // not enough bands for the window
            return None;
        }

        let window_len = self
            .input_band_tiles
            .iter()
            .take_while(|t| t.0 <= last_band)
            .count();

        // sum up all the the tiles and divide by the window length afterwards.
        // this is safe because we operate on f64 with a fixed (small) number of bands
        let out = self
            .input_band_tiles
            .iter()
            .take_while(|t| t.0 <= last_band)
            .fold(None, |accu, (_, tile)| {
                let Some(accu) = accu else {
                    // first tile becomes the accumulator
                    return Some(tile.clone());
                };

                let accu = accu.map_indexed_elements(|idx: GridIdx2D, accu_value| {
                    let tile_value = tile.get_at_grid_index(idx).unwrap_or(None);

                    match (accu_value, tile_value) {
                        (Some(accu), Some(tile)) => Some(accu + tile),
                        _ => None,
                    }
                });

                Some(accu)
            });

        let Some(mut out) = out else {
            // must not happen
            debug_assert!(false, "no output tile produced");
            return None;
        };

        out.band = self.output_band_idx;

        // remove tile if we no longer need it
        if self.output_band_idx >= self.window_size / 2 {
            // unless we are at the first output bands, we no longer need the first input band
            self.input_band_tiles.pop_front();
        }

        let out =
            out.map_elements(|value: Option<f64>| value.map(|value| value / window_len as f64));

        self.output_band_idx += 1;

        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use geoengine_datatypes::{
        primitives::{BandSelection, CacheHint, TimeInterval, TimeStep},
        raster::{Grid, GridBoundingBox2D, GridShape, RasterDataType, TilesEqualIgnoringCacheHint},
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    use crate::{
        engine::{
            MockExecutionContext, RasterBandDescriptors, SpatialGridDescriptor, TimeDescriptor,
        },
        mock::{MockRasterSource, MockRasterSourceParams},
    };

    use super::*;

    #[test]
    fn it_computes_first_derivative() {
        let mut data: Vec<RasterTile2D<f64>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0., 1., 2., 3.])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![2., 3., 4., 5.])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4., 5., 6., 7.])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let mut accu = FirstDerivativeAccu::new(3, &BandDistance::EquallySpaced { distance: 1. });

        accu.add_tile(data.remove(0)).unwrap();
        assert!(accu.next_band_tile().is_none());

        accu.add_tile(data.remove(0)).unwrap();
        assert!(
            accu.next_band_tile()
                .unwrap()
                .tiles_equal_ignoring_cache_hint(&RasterTile2D {
                    time: TimeInterval::new_unchecked(0, 5),
                    tile_position: [-1, 0].into(),
                    band: 0,
                    global_geo_transform: TestDefault::test_default(),
                    grid_array: Grid::new([2, 2].into(), vec![2., 2., 2., 2.])
                        .unwrap()
                        .into(),
                    properties: Default::default(),
                    cache_hint: CacheHint::default(),
                })
        );

        accu.add_tile(data.remove(0)).unwrap();
        assert!(
            accu.next_band_tile()
                .unwrap()
                .tiles_equal_ignoring_cache_hint(&RasterTile2D {
                    time: TimeInterval::new_unchecked(0, 5),
                    tile_position: [-1, 0].into(),
                    band: 1,
                    global_geo_transform: TestDefault::test_default(),
                    grid_array: Grid::new([2, 2].into(), vec![2., 2., 2., 2.])
                        .unwrap()
                        .into(),
                    properties: Default::default(),
                    cache_hint: CacheHint::default(),
                }),
        );
        assert!(
            accu.next_band_tile()
                .unwrap()
                .tiles_equal_ignoring_cache_hint(&RasterTile2D {
                    time: TimeInterval::new_unchecked(0, 5),
                    tile_position: [-1, 0].into(),
                    band: 2,
                    global_geo_transform: TestDefault::test_default(),
                    grid_array: Grid::new([2, 2].into(), vec![2., 2., 2., 2.])
                        .unwrap()
                        .into(),
                    properties: Default::default(),
                    cache_hint: CacheHint::default(),
                })
        );

        assert!(std::panic::catch_unwind(move || accu.next_band_tile()).is_err());
    }

    #[test]
    fn it_computes_first_derivative_distances() {
        let mut data: Vec<RasterTile2D<f64>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0., 1., 2., 3.])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![2., 3., 4., 5.])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4., 5., 6., 7.])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let mut accu = FirstDerivativeAccu::new(3, &BandDistance::EquallySpaced { distance: 3. });

        accu.add_tile(data.remove(0)).unwrap();
        assert!(accu.next_band_tile().is_none());

        accu.add_tile(data.remove(0)).unwrap();
        assert!(
            accu.next_band_tile()
                .unwrap()
                .tiles_equal_ignoring_cache_hint(&RasterTile2D {
                    time: TimeInterval::new_unchecked(0, 5),
                    tile_position: [-1, 0].into(),
                    band: 0,
                    global_geo_transform: TestDefault::test_default(),
                    grid_array: Grid::new([2, 2].into(), vec![2. / 3., 2. / 3., 2. / 3., 2. / 3.])
                        .unwrap()
                        .into(),
                    properties: Default::default(),
                    cache_hint: CacheHint::default(),
                })
        );

        accu.add_tile(data.remove(0)).unwrap();
        assert!(
            accu.next_band_tile()
                .unwrap()
                .tiles_equal_ignoring_cache_hint(&RasterTile2D {
                    time: TimeInterval::new_unchecked(0, 5),
                    tile_position: [-1, 0].into(),
                    band: 1,
                    global_geo_transform: TestDefault::test_default(),
                    grid_array: Grid::new([2, 2].into(), vec![2. / 3., 2. / 3., 2. / 3., 2. / 3.])
                        .unwrap()
                        .into(),
                    properties: Default::default(),
                    cache_hint: CacheHint::default(),
                }),
        );
        assert!(
            accu.next_band_tile()
                .unwrap()
                .tiles_equal_ignoring_cache_hint(&RasterTile2D {
                    time: TimeInterval::new_unchecked(0, 5),
                    tile_position: [-1, 0].into(),
                    band: 2,
                    global_geo_transform: TestDefault::test_default(),
                    grid_array: Grid::new([2, 2].into(), vec![2. / 3., 2. / 3., 2. / 3., 2. / 3.])
                        .unwrap()
                        .into(),
                    properties: Default::default(),
                    cache_hint: CacheHint::default(),
                })
        );

        assert!(std::panic::catch_unwind(move || accu.next_band_tile()).is_err());
    }

    #[test]
    fn it_computes_moving_average() {
        let mut data: Vec<RasterTile2D<f64>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0., 1., 2., 3.])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![2., 3., 4., 5.])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4., 5., 6., 7.])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let mut accu = MovingAverageAccu::new(3, 3);

        accu.add_tile(data.remove(0)).unwrap();
        assert!(accu.next_band_tile().is_none());

        accu.add_tile(data.remove(0)).unwrap();
        assert!(
            accu.next_band_tile()
                .unwrap()
                .tiles_equal_ignoring_cache_hint(&RasterTile2D {
                    time: TimeInterval::new_unchecked(0, 5),
                    tile_position: [-1, 0].into(),
                    band: 0,
                    global_geo_transform: TestDefault::test_default(),
                    grid_array: Grid::new([2, 2].into(), vec![1., 2., 3., 4.])
                        .unwrap()
                        .into(),
                    properties: Default::default(),
                    cache_hint: CacheHint::default(),
                })
        );

        accu.add_tile(data.remove(0)).unwrap();
        assert!(
            accu.next_band_tile()
                .unwrap()
                .tiles_equal_ignoring_cache_hint(&RasterTile2D {
                    time: TimeInterval::new_unchecked(0, 5),
                    tile_position: [-1, 0].into(),
                    band: 1,
                    global_geo_transform: TestDefault::test_default(),
                    grid_array: Grid::new([2, 2].into(), vec![2., 3., 4., 5.])
                        .unwrap()
                        .into(),
                    properties: Default::default(),
                    cache_hint: CacheHint::default(),
                })
        );
        assert!(
            accu.next_band_tile()
                .unwrap()
                .tiles_equal_ignoring_cache_hint(&RasterTile2D {
                    time: TimeInterval::new_unchecked(0, 5),
                    tile_position: [-1, 0].into(),
                    band: 2,
                    global_geo_transform: TestDefault::test_default(),
                    grid_array: Grid::new([2, 2].into(), vec![3., 4., 5., 6.])
                        .unwrap()
                        .into(),
                    properties: Default::default(),
                    cache_hint: CacheHint::default(),
                })
        );

        assert!(std::panic::catch_unwind(move || accu.next_band_tile()).is_err());
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_computes_first_derivative_on_stream() {
        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![2, 3, 4, 5]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let expected: Vec<RasterTile2D<f64>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![2., 2., 2., 2.])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![2., 2., 2., 2.])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![2., 2., 2., 2.])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4., 4., 4., 4.])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4., 4., 4., 4.])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4., 4., 4., 4.])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: TimeDescriptor::new_regular_with_epoch(
                        Some(
                            TimeInterval::new(
                                data.first().unwrap().time.start(),
                                data.last().unwrap().time.end(),
                            )
                            .unwrap(),
                        ),
                        TimeStep::millis(5).unwrap(),
                    ),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        TestDefault::test_default(),
                        GridBoundingBox2D::new_min_max(-2, -1, 0, 3).unwrap(),
                    ),
                    bands: RasterBandDescriptors::new_multiple_bands(3),
                },
            },
        }
        .boxed();

        let band_neighborhood_aggregate: Box<dyn RasterOperator> = BandNeighborhoodAggregate {
            params: BandNeighborhoodAggregateParams {
                aggregate: NeighborhoodAggregate::FirstDerivative {
                    band_distance: BandDistance::EquallySpaced { distance: 1.0 },
                },
            },
            sources: SingleRasterSource { raster: mrs1 },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new_min_max(-2, -1, 0, 3).unwrap(),
            TimeInterval::new_unchecked(0, 5),
            BandSelection::new_unchecked(vec![0, 1, 2]),
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let op = band_neighborhood_aggregate
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let qp = op.query_processor().unwrap().get_f64().unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert!(result.tiles_equal_ignoring_cache_hint(&expected));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_selects_bands_from_result() {
        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![2, 3, 4, 5]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let expected: Vec<RasterTile2D<f64>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![2., 2., 2., 2.])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4., 4., 4., 4.])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: TimeDescriptor::new_regular_with_epoch(
                        Some(
                            TimeInterval::new(
                                data.first().unwrap().time.start(),
                                data.last().unwrap().time.end(),
                            )
                            .unwrap(),
                        ),
                        TimeStep::millis(5).unwrap(),
                    ),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        TestDefault::test_default(),
                        GridBoundingBox2D::new_min_max(-2, -1, 0, 3).unwrap(),
                    ),
                    bands: RasterBandDescriptors::new_multiple_bands(3),
                },
            },
        }
        .boxed();

        let band_neighborhood_aggregate: Box<dyn RasterOperator> = BandNeighborhoodAggregate {
            params: BandNeighborhoodAggregateParams {
                aggregate: NeighborhoodAggregate::FirstDerivative {
                    band_distance: BandDistance::EquallySpaced { distance: 1.0 },
                },
            },
            sources: SingleRasterSource { raster: mrs1 },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new_min_max(-2, -1, 0, 3).unwrap(),
            TimeInterval::new_unchecked(0, 5),
            BandSelection::new_unchecked(vec![0]), // only get first band
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let op = band_neighborhood_aggregate
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let qp = op.query_processor().unwrap().get_f64().unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert!(result.tiles_equal_ignoring_cache_hint(&expected));
    }
}
