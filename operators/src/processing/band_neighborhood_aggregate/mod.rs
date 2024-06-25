use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedRasterOperator, InitializedSources, Operator,
    OperatorName, QueryContext, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
    ResultDescriptor, SingleRasterSource, TypedRasterQueryProcessor, WorkflowOperatorPath,
};

use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{ready, Stream};
use geoengine_datatypes::primitives::RasterQueryRectangle;
use geoengine_datatypes::raster::{
    GridIdx2D, GridIndexAccess, MapIndexedElements, RasterDataType, RasterTile2D,
};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BandNeighborhoodAggregateParams {
    pub aggregate: NeighborHoodAggregate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NeighborHoodAggregate {
    FirstDerivative,
    // TODO: SecondDerivative,
    // TODO: Average { window_size: usize },
    // TODO: Savitzky-Golay Filter
}

/// This `QueryProcessor` performs a pixel-wise aggregate over surrounding bands.
// TODO: different name like BandMovingWindowAggregate?
pub type BandNeighborhoodAggregate = Operator<BandNeighborhoodAggregateParams, SingleRasterSource>;

impl OperatorName for BandNeighborhoodAggregate {
    const TYPE_NAME: &'static str = "BandNeighborhoodAggregate";
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

        let source = self.sources.initialize_sources(path, context).await?.raster;

        let in_descriptor = source.result_descriptor();

        // TODO: ensure min. amound of bands

        let result_descriptor = in_descriptor.map_data_type(|_| RasterDataType::F64);

        Ok(Box::new(InitializedBandNeighborhoodAggregate {
            name,
            result_descriptor,
            source,
        }))
    }

    span_fn!(BandNeighborhoodAggregate);
}

pub struct InitializedBandNeighborhoodAggregate {
    name: CanonicOperatorName,
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
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
            )
            .boxed(),
        ))
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }
}

pub(crate) struct BandNeighborhoodAggregateProcessor {
    source: Box<dyn RasterQueryProcessor<RasterType = f64>>,
    result_descriptor: RasterResultDescriptor,
}

impl BandNeighborhoodAggregateProcessor {
    pub fn new(
        source: Box<dyn RasterQueryProcessor<RasterType = f64>>,
        result_descriptor: RasterResultDescriptor,
    ) -> Self {
        Self {
            source,
            result_descriptor,
        }
    }
}

#[async_trait]
impl RasterQueryProcessor for BandNeighborhoodAggregateProcessor {
    type RasterType = f64;

    async fn raster_query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<f64>>>> {
        // TODO: ensure min amount of bands in query
        Ok(Box::pin(BandNeighborhoodAggregateStream::<
            _,
            FirstDerivativeAccu, // TODO: make interchangable
        >::new(
            self.source.raster_query(query, ctx).await?,
            self.result_descriptor.bands.count(),
        )))
    }

    fn raster_result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

#[pin_project(project = BandNeighborhoodAggregateStreamProjection)]
struct BandNeighborhoodAggregateStream<S, A> {
    #[pin]
    input_stream: S,
    accu: A,
    current_input_band_idx: u32,
    current_output_band_idx: u32,
    num_bands: u32,
    finished: bool,
}

impl<S, A> BandNeighborhoodAggregateStream<S, A>
where
    S: Stream<Item = Result<RasterTile2D<f64>>> + Unpin,
    A: Accu,
{
    pub fn new(input_stream: S, num_bands: u32) -> Self {
        Self {
            input_stream,
            accu: A::new(num_bands),
            current_input_band_idx: 0,
            current_output_band_idx: 0,
            num_bands,
            finished: false,
        }
    }
}

trait Accu {
    fn new(num_bands: u32) -> Self;
    fn reset(&mut self);
    fn add_tile(&mut self, tile: RasterTile2D<f64>) -> Result<()>;
    // try to produce the next band, returns None when the accu is not yet ready (needs more bands)
    // this method also removes all bands that are not longer needed for future bands
    fn next_band_tile(&mut self) -> Option<RasterTile2D<f64>>;
}

// this stream consumes an input stream, collects bands of a tile and aggregates them
// it outputs the aggregated tile bands as soon as they are fully processed
impl<S, A> Stream for BandNeighborhoodAggregateStream<S, A>
where
    S: Stream<Item = Result<RasterTile2D<f64>>> + Unpin,
    A: Accu,
{
    type Item = Result<RasterTile2D<f64>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        let BandNeighborhoodAggregateStreamProjection {
            mut input_stream,
            accu,
            current_input_band_idx,
            current_output_band_idx,
            num_bands,
            finished,
        } = self.as_mut().project();

        // process in a loop, as it may be necessary to consume multiple input bands to produce a single output band
        loop {
            if *current_input_band_idx > *num_bands - 1 {
                // all bands of the current tile have been consumed

                if *current_output_band_idx < *num_bands - 1 {
                    // there are still bands left to produce for the current tile, so consume the accu
                    // TODO: compute next band asynchronously
                    let Some(next_band_tile) = accu.next_band_tile() else {
                        *finished = true;
                        return Poll::Ready(Some(Err(crate::error::Error::MustNotHappen {
                            message: "Unexpected end of accu".to_string(), // if the accu does not produce a band, there is a bug in the accu
                        })));
                    };

                    *current_output_band_idx += 1;
                    return Poll::Ready(Some(Ok(next_band_tile)));
                }

                // all bands of the current tile have been produced
                *current_input_band_idx = 0;
                *current_output_band_idx = 0;
                accu.reset();
            }

            // TODO: first try to consume accu before getting the next band from the input stream?

            // get next band from input stream
            let tile = ready!(input_stream.as_mut().poll_next(cx));

            let tile = match tile {
                Some(Ok(tile)) => tile,
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => {
                    if *current_input_band_idx > 0 {
                        // stream must not end in the middle of a tile
                        return Poll::Ready(Some(Err(crate::error::Error::MustNotHappen {
                            message: "Unexpected end of stream".to_string(),
                        })));
                    }

                    // stream ended properly
                    *finished = true;
                    return Poll::Ready(None);
                }
            };

            *current_input_band_idx += 1;

            // TODO: add asynchronously
            accu.add_tile(tile);

            // TODO: compute next band asynchrously
            if let Some(next_band_tile) = accu.next_band_tile() {
                // if the accu already generates a new band, return it
                *current_output_band_idx += 1;
                return Poll::Ready(Some(Ok(next_band_tile)));
            }

            // the accu may need more input bands to produce the next output band, so continue the loop
        }
    }
}

#[derive(Clone)]
pub struct FirstDerivativeAccu {
    input_band_tiles: VecDeque<(u32, RasterTile2D<f64>)>,
    output_band_idx: u32,
    num_bands: u32,
}

impl Accu for FirstDerivativeAccu {
    fn new(num_bands: u32) -> Self {
        Self {
            input_band_tiles: VecDeque::new(),
            output_band_idx: 0,
            num_bands,
        }
    }

    fn add_tile(&mut self, tile: RasterTile2D<f64>) -> Result<()> {
        let next_idx = self.input_band_tiles.back().map_or(0, |t| t.0 + 1);
        self.input_band_tiles.push_back((next_idx, tile));
        Ok(())
    }

    fn next_band_tile(&mut self) -> Option<RasterTile2D<f64>> {
        if self.output_band_idx >= self.num_bands {
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
        let divisor = if self.output_band_idx == 0 || self.output_band_idx == self.num_bands - 1 {
            // special case because there is no predecessor or successor for the first and last band
            1.0
        } else {
            2.0
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
            self.input_band_tiles.pop_front();
        }

        self.output_band_idx += 1;

        Some(out)
    }

    fn reset(&mut self) {
        self.input_band_tiles.clear();
        self.output_band_idx = 0;
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use geoengine_datatypes::{
        primitives::{CacheHint, SpatialPartition2D, SpatialResolution, TimeInterval},
        raster::{Grid, GridShape, RasterDataType, TilesEqualIgnoringCacheHint},
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    use crate::{
        engine::{MockExecutionContext, MockQueryContext, RasterBandDescriptors},
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

        let mut accu = FirstDerivativeAccu::new(3);

        accu.add_tile(data.remove(0)).unwrap();
        assert!(accu.next_band_tile().is_none());

        accu.add_tile(data.remove(0)).unwrap();
        assert!(accu
            .next_band_tile()
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
            }));

        accu.add_tile(data.remove(0)).unwrap();
        assert!(accu
            .next_band_tile()
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
            }),);
        assert!(accu
            .next_band_tile()
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
            }));

        assert!(accu.next_band_tile().is_none());
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
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_multiple_bands(3),
                },
            },
        }
        .boxed();

        let band_neighborhood_aggregate: Box<dyn RasterOperator> = BandNeighborhoodAggregate {
            params: BandNeighborhoodAggregateParams {
                aggregate: NeighborHoodAggregate::FirstDerivative,
            },
            sources: SingleRasterSource { raster: mrs1 },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 1.).into(), (3., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 5),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::new_unchecked(vec![0, 1, 2]),
        };

        let query_ctx = MockQueryContext::test_default();

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

        dbg!(&result);

        assert!(result.tiles_equal_ignoring_cache_hint(&expected));
    }
}
