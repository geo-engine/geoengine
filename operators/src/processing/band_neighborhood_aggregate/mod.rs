use std::sync::Arc;

use crate::adapters::{FoldTileAccu, RasterSubQueryAdapter, SubQueryTileAggregator};
use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedRasterOperator, InitializedSources, Operator,
    OperatorName, QueryContext, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
    ResultDescriptor, SingleRasterSource, TypedRasterQueryProcessor, WorkflowOperatorPath,
};

use crate::util::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use geoengine_datatypes::primitives::{
    BandSelection, RasterQueryRectangle, SpatialPartitioned, TimeInstance, TimeInterval,
};
use geoengine_datatypes::raster::{
    GridIdx2D, GridIndexAccess, MapIndexedElements, RasterDataType, RasterTile2D, TileInformation,
    TilingSpecification,
};
use rayon::ThreadPool;
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

        let result_descriptor = in_descriptor.map_data_type(|_| RasterDataType::F64);

        let tiling_specification = context.tiling_specification();

        Ok(Box::new(InitializedBandNeighborhoodAggregate {
            name,
            result_descriptor,
            source,
            tiling_specification,
        }))
    }

    span_fn!(BandNeighborhoodAggregate);
}

pub struct InitializedBandNeighborhoodAggregate {
    name: CanonicOperatorName,
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
    tiling_specification: TilingSpecification,
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
                self.tiling_specification,
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
    tiling_specification: TilingSpecification,
}

impl BandNeighborhoodAggregateProcessor {
    pub fn new(
        source: Box<dyn RasterQueryProcessor<RasterType = f64>>,
        result_descriptor: RasterResultDescriptor,
        tiling_specification: TilingSpecification,
    ) -> Self {
        Self {
            source,
            result_descriptor,
            tiling_specification,
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
        Ok(RasterSubQueryAdapter::<'a, f64, _, _>::new(
            &self.source,
            query,
            self.tiling_specification,
            ctx,
            FirstDerivativeSubQuery {}, // TODO: distinguish between different aggregate functions
        )
        .filter_and_fill(
            crate::adapters::FillerTileCacheExpirationStrategy::DerivedFromSurroundingTiles,
        ))
    }

    fn raster_result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

#[derive(Debug, Clone)]
pub struct FirstDerivativeSubQuery {}

#[derive(Clone)]
pub struct FirstDerivativeAccu {
    pub pool: Arc<ThreadPool>,
    pub state: FirstDerivativeAccuState,
}

#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum FirstDerivativeAccuState {
    Initial,
    Previous(RasterTile2D<f64>),
    PreviousAndNext(RasterTile2D<f64>, RasterTile2D<f64>),
}

impl FirstDerivativeAccu {
    pub fn new(pool: Arc<ThreadPool>) -> Self {
        Self {
            pool,
            state: FirstDerivativeAccuState::Initial,
        }
    }
}

#[async_trait]
impl FoldTileAccu for FirstDerivativeAccu {
    type RasterType = f64;

    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>> {
        let output_tile: RasterTile2D<f64> =
            crate::util::spawn_blocking_with_thread_pool(self.pool, move || {
                let FirstDerivativeAccuState::PreviousAndNext(prev, next) = self.state else {
                    return Err(crate::error::Error::MustNotHappen {
                        message: "Cannot compute first derivative without previous and next tile"
                            .to_string(),
                    });
                };

                let out = prev.map_indexed_elements(|idx: GridIdx2D, prev_value| {
                    let next_value = next.get_at_grid_index(idx).unwrap_or(None);

                    // TODO: the divisor should be the difference of the bands on a spectrum. In order to compute this, bands first need a dimension.
                    let divisor = 2.0;

                    match (prev_value, next_value) {
                        (Some(prev), Some(next)) => Some((next - prev) / divisor),
                        _ => None,
                    }
                });

                Ok(out)
            })
            .await??;

        Ok(output_tile)
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

type FoldFuture = BoxFuture<'static, Result<FirstDerivativeAccu>>;

impl<'a> SubQueryTileAggregator<'a, f64> for FirstDerivativeSubQuery {
    type FoldFuture = FoldFuture;

    type FoldMethod = fn(FirstDerivativeAccu, RasterTile2D<f64>) -> Self::FoldFuture;

    type TileAccu = FirstDerivativeAccu;
    type TileAccuFuture = BoxFuture<'a, Result<Self::TileAccu>>;

    fn new_fold_accu(
        &self,
        _tile_info: TileInformation,
        _query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture {
        Box::pin(futures::future::ready(Ok(FirstDerivativeAccu::new(
            pool.clone(),
        ))))
    }

    /// Set the query rectangle to include the neighbor bands
    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
        band_idx: u32,
    ) -> Result<Option<RasterQueryRectangle>> {
        let spatial_bounds = tile_info.spatial_partition();

        let bands = query_rect.attributes.as_slice();

        // compute derivative by difference between predecessor and successor
        let predecessor_idx = if band_idx == 0 {
            band_idx // if no predecessor, use current band
        } else {
            band_idx - 1
        };

        let successor_idx = if band_idx as usize == bands.len() - 1 {
            band_idx // if no successor, use current band
        } else {
            band_idx + 1
        };

        let attributes = BandSelection::new_unchecked(vec![
            bands[predecessor_idx as usize],
            bands[successor_idx as usize],
        ]);

        Ok(Some(RasterQueryRectangle {
            spatial_bounds,
            time_interval: TimeInterval::new_instant(start_time)?, // TODO: check
            spatial_resolution: query_rect.spatial_resolution,
            attributes,
        }))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        |accu, tile| {
            let state = match accu.state {
                FirstDerivativeAccuState::Initial => Ok(FirstDerivativeAccuState::Previous(tile)),
                FirstDerivativeAccuState::Previous(predecessor) => {
                    Ok(FirstDerivativeAccuState::PreviousAndNext(predecessor, tile))
                }
                FirstDerivativeAccuState::PreviousAndNext(_, _) => {
                    log::warn!("Unexpected state in fold method: got another tile after successor and predecessor");
                    Err(crate::error::Error::MustNotHappen {
                        message: "Unexpected state in fold method: got another tile after successor and predecessor".to_string(),
                    })
                }
            };

            let accu = state.map(|state| FirstDerivativeAccu {
                pool: accu.pool,
                state,
            });

            Box::pin(futures::future::ready(accu))
        }
    }
}

#[cfg(test)]
mod tests {}
