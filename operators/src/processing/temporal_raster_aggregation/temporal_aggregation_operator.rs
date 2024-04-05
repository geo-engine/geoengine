use super::aggregators::{
    CountPixelAggregator, CountPixelAggregatorIngoringNoData, FirstPixelAggregatorIngoringNoData,
    GlobalStateTemporalRasterPixelAggregator, LastPixelAggregatorIngoringNoData,
    MaxPixelAggregator, MaxPixelAggregatorIngoringNoData, MeanPixelAggregator, MinPixelAggregator,
    MinPixelAggregatorIngoringNoData, SumPixelAggregator, SumPixelAggregatorIngoringNoData,
    TemporalRasterPixelAggregator,
};
use super::first_last_subquery::{
    first_tile_fold_future, last_tile_fold_future, TemporalRasterAggregationSubQueryNoDataOnly,
};
use super::subquery::GlobalStateTemporalRasterAggregationSubQuery;
use crate::adapters::stack_individual_aligned_raster_bands;
use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedSources, Operator, QueryProcessor,
    RasterOperator, SingleRasterSource, WorkflowOperatorPath,
};
use crate::processing::temporal_raster_aggregation::aggregators::PercentileEstimateAggregator;
use crate::{
    adapters::SubQueryTileAggregator,
    engine::{
        InitializedRasterOperator, OperatorName, RasterQueryProcessor, RasterResultDescriptor,
        TypedRasterQueryProcessor,
    },
    error,
    util::Result,
};
use async_trait::async_trait;
use geoengine_datatypes::primitives::{
    BandSelection, RasterQueryRectangle, SpatialPartition2D, TimeInstance,
};
use geoengine_datatypes::raster::{Pixel, RasterDataType, RasterTile2D};
use geoengine_datatypes::{primitives::TimeStep, raster::TilingSpecification};
use log::debug;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::marker::PhantomData;
use std::sync::Arc;

use typetag;

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TemporalRasterAggregationParameters {
    pub aggregation: Aggregation,
    pub window: TimeStep,
    /// Define an anchor point for `window`
    /// If `None`, the anchor point is `1970-01-01T00:00:00Z` by default
    pub window_reference: Option<TimeInstance>,
    /// If specified, this will be the output type.
    /// If not, the output type will be the same as the input type.
    pub output_type: Option<RasterDataType>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type")]
pub enum Aggregation {
    #[serde(rename_all = "camelCase")]
    Min { ignore_no_data: bool },
    #[serde(rename_all = "camelCase")]
    Max { ignore_no_data: bool },
    #[serde(rename_all = "camelCase")]
    First { ignore_no_data: bool },
    #[serde(rename_all = "camelCase")]
    Last { ignore_no_data: bool },
    #[serde(rename_all = "camelCase")]
    Mean { ignore_no_data: bool },
    #[serde(rename_all = "camelCase")]
    Sum { ignore_no_data: bool },
    #[serde(rename_all = "camelCase")]
    Count { ignore_no_data: bool },
    #[serde(rename_all = "camelCase")]
    PercentileEstimate {
        ignore_no_data: bool,
        /// Must in in range [0, 1]
        percentile: f64,
    },
}

pub type TemporalRasterAggregation =
    Operator<TemporalRasterAggregationParameters, SingleRasterSource>;

impl OperatorName for TemporalRasterAggregation {
    const TYPE_NAME: &'static str = "TemporalRasterAggregation";
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for TemporalRasterAggregation {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        ensure!(self.params.window.step > 0, error::WindowSizeMustNotBeZero);

        let name = CanonicOperatorName::from(&self);

        let initialized_source = self.sources.initialize_sources(path, context).await?;
        let source = initialized_source.raster;

        debug!(
            "Initializing TemporalRasterAggregation with {:?}.",
            &self.params
        );

        let mut out_result_descriptor = source.result_descriptor().clone();

        if let Some(output_type) = self.params.output_type {
            out_result_descriptor.data_type = output_type;
        };

        let initialized_operator = InitializedTemporalRasterAggregation {
            name,
            aggregation_type: self.params.aggregation,
            window: self.params.window,
            window_reference: self
                .params
                .window_reference
                .unwrap_or(TimeInstance::EPOCH_START),
            result_descriptor: out_result_descriptor,
            source,
            tiling_specification: context.tiling_specification(),
            output_type: self.params.output_type,
        };

        Ok(initialized_operator.boxed())
    }

    span_fn!(TemporalRasterAggregation);
}

pub struct InitializedTemporalRasterAggregation {
    name: CanonicOperatorName,
    aggregation_type: Aggregation,
    window: TimeStep,
    window_reference: TimeInstance,
    source: Box<dyn InitializedRasterOperator>,
    result_descriptor: RasterResultDescriptor,
    tiling_specification: TilingSpecification,
    output_type: Option<RasterDataType>,
}

impl InitializedRasterOperator for InitializedTemporalRasterAggregation {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source_processor = self.source.query_processor()?;

        let source_processor: TypedRasterQueryProcessor = match self.output_type {
            Some(RasterDataType::U8) => source_processor.into_u8().into(),
            Some(RasterDataType::U16) => source_processor.into_u16().into(),
            Some(RasterDataType::U32) => source_processor.into_u32().into(),
            Some(RasterDataType::U64) => source_processor.into_u64().into(),
            Some(RasterDataType::I8) => source_processor.into_i8().into(),
            Some(RasterDataType::I16) => source_processor.into_i16().into(),
            Some(RasterDataType::I32) => source_processor.into_i32().into(),
            Some(RasterDataType::I64) => source_processor.into_i64().into(),
            Some(RasterDataType::F32) => source_processor.into_f32().into(),
            Some(RasterDataType::F64) => source_processor.into_f64().into(),
            // use the same output type as the input type
            None => source_processor,
        };

        let res = call_on_generic_raster_processor!(
            source_processor, p =>
            TemporalRasterAggregationProcessor::new(
                self.result_descriptor.clone(),
                self.aggregation_type,
                self.window,
                self.window_reference,
                p,
                self.tiling_specification,
            ).boxed()
            .into()
        );

        Ok(res)
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }
}

pub struct TemporalRasterAggregationProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    result_descriptor: RasterResultDescriptor,
    aggregation_type: Aggregation,
    window: TimeStep,
    window_reference: TimeInstance,
    source: Q,
    tiling_specification: TilingSpecification,
}

impl<Q, P> TemporalRasterAggregationProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>
        + QueryProcessor<
            Output = RasterTile2D<P>,
            SpatialBounds = SpatialPartition2D,
            Selection = BandSelection,
            ResultDescription = RasterResultDescriptor,
        >,
    P: Pixel,
{
    fn new(
        result_descriptor: RasterResultDescriptor,
        aggregation_type: Aggregation,
        window: TimeStep,
        window_reference: TimeInstance,
        source: Q,
        tiling_specification: TilingSpecification,
    ) -> Self {
        Self {
            result_descriptor,
            aggregation_type,
            window,
            window_reference,
            source,
            tiling_specification,
        }
    }

    fn create_subquery<F: TemporalRasterPixelAggregator<P> + 'static, FoldFn>(
        &self,
        fold_fn: FoldFn,
    ) -> super::subquery::TemporalRasterAggregationSubQuery<FoldFn, P, F> {
        super::subquery::TemporalRasterAggregationSubQuery {
            fold_fn,
            step: self.window,
            step_reference: self.window_reference,
            _phantom_pixel_type: PhantomData,
        }
    }

    fn create_global_state_subquery<
        F: GlobalStateTemporalRasterPixelAggregator<P> + 'static,
        FoldFn,
    >(
        &self,
        aggregator: F,
        fold_fn: FoldFn,
    ) -> GlobalStateTemporalRasterAggregationSubQuery<FoldFn, P, F> {
        GlobalStateTemporalRasterAggregationSubQuery {
            aggregator: Arc::new(aggregator),
            fold_fn,
            step: self.window,
            step_reference: self.window_reference,
            _phantom_pixel_type: PhantomData,
        }
    }

    fn create_subquery_first<F>(
        &self,
        fold_fn: F,
    ) -> TemporalRasterAggregationSubQueryNoDataOnly<F, P> {
        TemporalRasterAggregationSubQueryNoDataOnly {
            fold_fn,
            step: self.window,
            step_reference: self.window_reference,
            _phantom_pixel_type: PhantomData,
        }
    }

    fn create_subquery_last<F>(
        &self,
        fold_fn: F,
    ) -> TemporalRasterAggregationSubQueryNoDataOnly<F, P> {
        TemporalRasterAggregationSubQueryNoDataOnly {
            fold_fn,
            step: self.window,
            step_reference: self.window_reference,
            _phantom_pixel_type: PhantomData,
        }
    }

    #[allow(clippy::too_many_lines)]
    fn create_subquery_adapter_stream_for_single_band<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<futures::stream::BoxStream<'a, Result<RasterTile2D<P>>>> {
        ensure!(
            query.attributes.count() == 1,
            error::InvalidBandCount {
                expected: 1u32,
                found: query.attributes.count()
            }
        );

        Ok(match self.aggregation_type {
            Aggregation::Min {
                ignore_no_data: true,
            } => self
                .create_subquery(
                    super::subquery::subquery_all_tiles_fold_fn::<
                        P,
                        MinPixelAggregatorIngoringNoData,
                    >,
                )
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::Min"),
            Aggregation::Min {
                ignore_no_data: false,
            } => self
                .create_subquery(
                    super::subquery::subquery_all_tiles_fold_fn::<P, MinPixelAggregator>,
                )
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::Min"),
            Aggregation::Max {
                ignore_no_data: true,
            } => self
                .create_subquery(
                    super::subquery::subquery_all_tiles_fold_fn::<
                        P,
                        MaxPixelAggregatorIngoringNoData,
                    >,
                )
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::Max"),
            Aggregation::Max {
                ignore_no_data: false,
            } => self
                .create_subquery(
                    super::subquery::subquery_all_tiles_fold_fn::<P, MaxPixelAggregator>,
                )
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::Max"),

            Aggregation::First {
                ignore_no_data: true,
            } => self
                .create_subquery(
                    super::subquery::subquery_all_tiles_fold_fn::<
                        P,
                        FirstPixelAggregatorIngoringNoData,
                    >,
                )
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::First"),
            Aggregation::First {
                ignore_no_data: false,
            } => self
                .create_subquery_first(first_tile_fold_future::<P>)
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::First"),
            Aggregation::Last {
                ignore_no_data: true,
            } => self
                .create_subquery(
                    super::subquery::subquery_all_tiles_fold_fn::<
                        P,
                        LastPixelAggregatorIngoringNoData,
                    >,
                )
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::Last"),

            Aggregation::Last {
                ignore_no_data: false,
            } => self
                .create_subquery_last(last_tile_fold_future::<P>)
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::Last"),

            Aggregation::Mean {
                ignore_no_data: true,
            } => self
                .create_subquery(
                    super::subquery::subquery_all_tiles_fold_fn::<P, MeanPixelAggregator<true>>,
                )
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::Mean"),

            Aggregation::Mean {
                ignore_no_data: false,
            } => self
                .create_subquery(
                    super::subquery::subquery_all_tiles_fold_fn::<P, MeanPixelAggregator<false>>,
                )
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::Mean"),

            Aggregation::Sum {
                ignore_no_data: true,
            } => self
                .create_subquery(
                    super::subquery::subquery_all_tiles_fold_fn::<
                        P,
                        SumPixelAggregatorIngoringNoData,
                    >,
                )
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::Sum"),

            Aggregation::Sum {
                ignore_no_data: false,
            } => self
                .create_subquery(
                    super::subquery::subquery_all_tiles_fold_fn::<P, SumPixelAggregator>,
                )
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::Sum"),

            Aggregation::Count {
                ignore_no_data: true,
            } => self
                .create_subquery(
                    super::subquery::subquery_all_tiles_fold_fn::<
                        P,
                        CountPixelAggregatorIngoringNoData,
                    >,
                )
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::Sum"),

            Aggregation::Count {
                ignore_no_data: false,
            } => self
                .create_subquery(
                    super::subquery::subquery_all_tiles_fold_fn::<P, CountPixelAggregator>,
                )
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::Sum"),
            Aggregation::PercentileEstimate {
                ignore_no_data: true,
                percentile,
            } => self
                .create_global_state_subquery(
                    PercentileEstimateAggregator::<true>::new(percentile),
                    super::subquery::subquery_all_tiles_global_state_fold_fn,
                )
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::PercentileEstimate"),
            Aggregation::PercentileEstimate {
                ignore_no_data: false,
                percentile,
            } => self
                .create_global_state_subquery(
                    PercentileEstimateAggregator::<false>::new(percentile),
                    super::subquery::subquery_all_tiles_global_state_fold_fn,
                )
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::PercentileEstimate"),
        })
    }
}

#[async_trait]
impl<Q, P> QueryProcessor for TemporalRasterAggregationProcessor<Q, P>
where
    Q: QueryProcessor<
        Output = RasterTile2D<P>,
        SpatialBounds = SpatialPartition2D,
        Selection = BandSelection,
        ResultDescription = RasterResultDescriptor,
    >,
    P: Pixel,
{
    type Output = RasterTile2D<P>;
    type SpatialBounds = SpatialPartition2D;
    type Selection = BandSelection;
    type ResultDescription = RasterResultDescriptor;

    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<futures::stream::BoxStream<'a, Result<Self::Output>>> {
        stack_individual_aligned_raster_bands(&query, ctx, |query, ctx| async {
            self.create_subquery_adapter_stream_for_single_band(query, ctx)
        })
        .await
    }

    fn result_descriptor(&self) -> &Self::ResultDescription {
        &self.result_descriptor
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;
    use geoengine_datatypes::{
        primitives::{CacheHint, Measurement, SpatialResolution, TimeInterval},
        raster::{
            EmptyGrid, EmptyGrid2D, Grid2D, GridOrEmpty, MaskedGrid2D, RasterDataType,
            TileInformation, TilesEqualIgnoringCacheHint,
        },
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    use crate::{
        engine::{
            MockExecutionContext, MockQueryContext, MultipleRasterSources, RasterBandDescriptors,
        },
        mock::{MockRasterSource, MockRasterSourceParams},
        processing::{
            raster_stacker::{RasterStacker, RasterStackerParams},
            Expression, ExpressionParams,
        },
    };

    use super::*;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_min() {
        let raster_tiles = make_raster();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Min {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                window_reference: None,
                output_type: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 40),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp = agg
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 4);

        assert!(result[0].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap()),
                CacheHint::default()
            )
        ));

        assert!(result[1].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1]).unwrap()),
                CacheHint::default()
            )
        ));

        assert!(result[2].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap()),
                CacheHint::default()
            )
        ));

        assert!(result[3].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1]).unwrap()),
                CacheHint::default()
            )
        ));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_max() {
        let raster_tiles = make_raster();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Max {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                window_reference: None,
                output_type: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 40),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp = agg
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 4);

        assert!(result[0].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7]).unwrap()),
                CacheHint::default()
            )
        ));

        assert!(result[1].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12]).unwrap()),
                CacheHint::default()
            )
        ));

        assert!(result[2].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7]).unwrap()),
                CacheHint::default()
            )
        ));

        assert!(result[3].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12]).unwrap()),
                CacheHint::default()
            )
        ));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_max_with_no_data() {
        let raster_tiles = make_raster(); // TODO: switch to make_raster_with_no_data?

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Max {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                window_reference: None,
                output_type: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 40),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp = agg
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 4);

        assert!(result[0].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7],).unwrap()),
                CacheHint::default()
            )
        ));

        assert!(result[1].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12]).unwrap()),
                CacheHint::default()
            )
        ));

        assert!(result[2].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7]).unwrap()),
                CacheHint::default()
            )
        ));

        assert!(result[3].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12]).unwrap()),
                CacheHint::default()
            )
        ));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_max_with_no_data_but_ignoring_it() {
        let raster_tiles = make_raster(); // TODO: switch to make_raster_with_no_data?

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Max {
                    ignore_no_data: true,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                window_reference: None,
                output_type: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 40),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp = agg
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 4);

        assert!(result[0].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7]).unwrap()),
                CacheHint::default()
            )
        ));

        assert!(result[1].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12]).unwrap()),
                CacheHint::default()
            )
        ));

        assert!(result[2].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7]).unwrap()),
                CacheHint::default()
            )
        ));

        assert!(result[3].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12]).unwrap()),
                CacheHint::default()
            )
        ));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_only_no_data() {
        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![RasterTile2D::new_with_tile_info(
                    TimeInterval::new_unchecked(0, 20),
                    TileInformation {
                        global_tile_position: [-1, 0].into(),
                        tile_size_in_pixels: [3, 2].into(),
                        global_geo_transform: TestDefault::test_default(),
                    },
                    0,
                    GridOrEmpty::from(EmptyGrid2D::<u8>::new([3, 2].into())),
                    CacheHint::default(),
                )],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Max {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                window_reference: None,
                output_type: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (2., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 20),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp = agg
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 1);

        assert!(result[0].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::Empty(EmptyGrid::new([3, 2].into())),
                CacheHint::default()
            )
        ));
    }

    #[tokio::test]
    async fn test_first_with_no_data() {
        let raster_tiles = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::First {
                    ignore_no_data: true,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
                window_reference: None,
                output_type: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp = agg
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 2);

        assert!(result[0].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 16, 11, 12]).unwrap()),
                CacheHint::default()
            )
        ));

        assert!(result[1].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(
                    MaskedGrid2D::new(
                        Grid2D::new([3, 2].into(), vec![1, 2, 3, 0, 5, 6]).unwrap(),
                        Grid2D::new([3, 2].into(), vec![true, true, true, false, true, true])
                            .unwrap()
                    )
                    .unwrap()
                ),
                CacheHint::default()
            )
        ));
    }

    #[tokio::test]
    async fn test_last_with_no_data() {
        let raster_tiles = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Last {
                    ignore_no_data: true,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
                window_reference: None,
                output_type: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp = agg
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 2);

        assert!(result[0].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![13, 8, 15, 16, 17, 18]).unwrap()),
                CacheHint::default()
            )
        ));

        assert!(result[1].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(
                    MaskedGrid2D::new(
                        Grid2D::new([3, 2].into(), vec![1, 2, 3, 0, 5, 6]).unwrap(),
                        Grid2D::new([3, 2].into(), vec![true, true, true, false, true, true])
                            .unwrap()
                    )
                    .unwrap()
                ),
                CacheHint::default()
            )
        ));
    }

    #[tokio::test]
    async fn test_last() {
        let raster_tiles = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Last {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
                window_reference: None,
                output_type: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp = agg
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 2);

        assert!(result[0].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(
                    MaskedGrid2D::new(
                        Grid2D::new([3, 2].into(), vec![13, 42, 15, 16, 17, 18]).unwrap(),
                        Grid2D::new([3, 2].into(), vec![true, false, true, true, true, true])
                            .unwrap()
                    )
                    .unwrap()
                ),
                CacheHint::default()
            )
        ));

        assert!(result[1].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::Empty(EmptyGrid2D::new([3, 2].into())),
                CacheHint::default()
            )
        ));
    }

    #[tokio::test]
    async fn test_first() {
        let raster_tiles = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::First {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
                window_reference: None,
                output_type: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp = agg
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 2);

        assert!(result[0].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(EmptyGrid2D::new([3, 2].into())),
                CacheHint::default()
            )
        ));

        assert!(result[1].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(
                    MaskedGrid2D::new(
                        Grid2D::new([3, 2].into(), vec![1, 2, 3, 42, 5, 6]).unwrap(),
                        Grid2D::new([3, 2].into(), vec![true, true, true, false, true, true])
                            .unwrap()
                    )
                    .unwrap()
                ),
                CacheHint::default()
            )
        ));
    }

    #[tokio::test]
    async fn test_mean_nodata() {
        let raster_tiles = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Mean {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
                window_reference: None,
                output_type: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp = agg
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 2);

        assert!(result[0].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(EmptyGrid2D::new([3, 2].into())),
                CacheHint::default()
            )
        ));

        assert!(result[1].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(
                    MaskedGrid2D::new(
                        Grid2D::new([3, 2].into(), vec![1, 2, 3, 0, 5, 6]).unwrap(),
                        Grid2D::new([3, 2].into(), vec![true, true, true, false, true, true])
                            .unwrap()
                    )
                    .unwrap()
                ),
                CacheHint::default()
            )
        ));
    }

    #[tokio::test]
    async fn test_mean_ignore_no_data() {
        let raster_tiles = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Mean {
                    ignore_no_data: true,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
                window_reference: None,
                output_type: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp = agg
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 2);

        assert!(result[0].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![10, 8, 12, 16, 14, 15]).unwrap()),
                CacheHint::default()
            )
        ));

        assert!(result[1].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(
                    MaskedGrid2D::new(
                        Grid2D::new([3, 2].into(), vec![1, 2, 3, 0, 5, 6]).unwrap(),
                        Grid2D::new([3, 2].into(), vec![true, true, true, false, true, true])
                            .unwrap()
                    )
                    .unwrap()
                ),
                CacheHint::default()
            )
        ));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_sum_without_nodata() {
        let operator = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Sum {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                window_reference: Some(TimeInstance::from_millis(0).unwrap()),
                output_type: None,
            },
            sources: SingleRasterSource {
                raster: MockRasterSource {
                    params: MockRasterSourceParams {
                        data: make_raster(),
                        result_descriptor: RasterResultDescriptor {
                            data_type: RasterDataType::U8,
                            spatial_reference: SpatialReference::epsg_4326().into(),
                            time: None,
                            bbox: None,
                            resolution: None,
                            bands: RasterBandDescriptors::new_single_band(),
                        },
                    },
                }
                .boxed(),
            },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let query_processor = operator
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = query_processor
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .await;

        assert!(result.tiles_equal_ignoring_cache_hint(&[
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                Grid2D::new([3, 2].into(), vec![13, 13, 13, 13, 13, 13])
                    .unwrap()
                    .into(),
                CacheHint::default()
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                Grid2D::new([3, 2].into(), vec![13, 13, 13, 13, 13, 13])
                    .unwrap()
                    .into(),
                CacheHint::default()
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                Grid2D::new([3, 2].into(), vec![13, 13, 13, 13, 13, 13])
                    .unwrap()
                    .into(),
                CacheHint::default()
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                Grid2D::new([3, 2].into(), vec![13, 13, 13, 13, 13, 13])
                    .unwrap()
                    .into(),
                CacheHint::default()
            )
        ]));
    }

    #[tokio::test]
    async fn test_sum_nodata() {
        let raster_tiles = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Sum {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
                window_reference: None,
                output_type: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp = agg
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 2);

        assert!(result[0].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(EmptyGrid2D::new([3, 2].into())),
                CacheHint::default()
            )
        ));

        assert!(result[1].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(
                    MaskedGrid2D::new(
                        Grid2D::new([3, 2].into(), vec![1, 2, 3, 0, 5, 6]).unwrap(),
                        Grid2D::new([3, 2].into(), vec![true, true, true, false, true, true])
                            .unwrap()
                    )
                    .unwrap(),
                ),
                CacheHint::default()
            )
        ));
    }

    #[tokio::test]
    async fn test_sum_ignore_no_data() {
        let raster_tiles = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Sum {
                    ignore_no_data: true,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
                window_reference: None,
                output_type: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp = agg
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 2);

        assert!(result[0].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![20, 8, 24, 16, 28, 30]).unwrap()),
                CacheHint::default()
            )
        ));

        assert!(result[1].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(
                    MaskedGrid2D::new(
                        Grid2D::new([3, 2].into(), vec![1, 2, 3, 0, 5, 6]).unwrap(),
                        Grid2D::new([3, 2].into(), vec![true, true, true, false, true, true])
                            .unwrap()
                    )
                    .unwrap()
                ),
                CacheHint::default()
            )
        ));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_sum_with_larger_data_type() {
        let operator = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Sum {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                window_reference: Some(TimeInstance::from_millis(0).unwrap()),
                output_type: Some(RasterDataType::U16),
            },
            sources: SingleRasterSource {
                raster: Expression {
                    params: ExpressionParams {
                        expression: "20 * A".to_string(),
                        output_type: RasterDataType::U8,
                        output_measurement: Some(Measurement::Unitless),
                        map_no_data: true,
                    },
                    sources: SingleRasterSource {
                        raster: MockRasterSource {
                            params: MockRasterSourceParams {
                                data: make_raster(),
                                result_descriptor: RasterResultDescriptor {
                                    data_type: RasterDataType::U8,
                                    spatial_reference: SpatialReference::epsg_4326().into(),
                                    time: None,
                                    bbox: None,
                                    resolution: None,
                                    bands: RasterBandDescriptors::new_single_band(),
                                },
                            },
                        }
                        .boxed(),
                    },
                }
                .boxed(),
            },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let query_processor = operator
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u16()
            .unwrap();

        let result = query_processor
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .await;

        assert!(result.tiles_equal_ignoring_cache_hint(&[
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                Grid2D::new(
                    [3, 2].into(),
                    vec![13 * 20, 13 * 20, 13 * 20, 13 * 20, 13 * 20, 13 * 20]
                )
                .unwrap()
                .into(),
                CacheHint::default()
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                Grid2D::new(
                    [3, 2].into(),
                    vec![13 * 20, 13 * 20, 13 * 20, 13 * 20, 13 * 20, 13 * 20]
                )
                .unwrap()
                .into(),
                CacheHint::default()
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                Grid2D::new(
                    [3, 2].into(),
                    vec![13 * 20, 13 * 20, 13 * 20, 13 * 20, 13 * 20, 13 * 20]
                )
                .unwrap()
                .into(),
                CacheHint::default()
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                Grid2D::new(
                    [3, 2].into(),
                    vec![13 * 20, 13 * 20, 13 * 20, 13 * 20, 13 * 20, 13 * 20]
                )
                .unwrap()
                .into(),
                CacheHint::default()
            )
        ]),);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_count_without_nodata() {
        let operator = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Count {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                window_reference: Some(TimeInstance::from_millis(0).unwrap()),
                output_type: None,
            },
            sources: SingleRasterSource {
                raster: MockRasterSource {
                    params: MockRasterSourceParams {
                        data: make_raster(),
                        result_descriptor: RasterResultDescriptor {
                            data_type: RasterDataType::U8,
                            spatial_reference: SpatialReference::epsg_4326().into(),
                            time: None,
                            bbox: None,
                            resolution: None,
                            bands: RasterBandDescriptors::new_single_band(),
                        },
                    },
                }
                .boxed(),
            },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let query_processor = operator
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = query_processor
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .await;

        assert!(result.tiles_equal_ignoring_cache_hint(&[
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                Grid2D::new([3, 2].into(), vec![2, 2, 2, 2, 2, 2])
                    .unwrap()
                    .into(),
                CacheHint::default()
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                Grid2D::new([3, 2].into(), vec![2, 2, 2, 2, 2, 2])
                    .unwrap()
                    .into(),
                CacheHint::default()
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                Grid2D::new([3, 2].into(), vec![2, 2, 2, 2, 2, 2])
                    .unwrap()
                    .into(),
                CacheHint::default()
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                Grid2D::new([3, 2].into(), vec![2, 2, 2, 2, 2, 2])
                    .unwrap()
                    .into(),
                CacheHint::default()
            )
        ]));
    }

    #[tokio::test]
    async fn test_count_nodata() {
        let raster_tiles = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Count {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
                window_reference: None,
                output_type: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp = agg
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 2);

        assert!(result[0].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(EmptyGrid2D::new([3, 2].into())),
                CacheHint::default()
            )
        ));

        assert!(result[1].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(
                    MaskedGrid2D::new(
                        Grid2D::new([3, 2].into(), vec![1, 1, 1, 0, 1, 1]).unwrap(),
                        Grid2D::new([3, 2].into(), vec![true, true, true, false, true, true])
                            .unwrap()
                    )
                    .unwrap()
                ),
                CacheHint::default()
            )
        ));
    }

    #[tokio::test]
    async fn test_count_ignore_no_data() {
        let raster_tiles = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Count {
                    ignore_no_data: true,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
                window_reference: None,
                output_type: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp = agg
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 2);

        assert!(result[0].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![2, 1, 2, 1, 2, 2]).unwrap()),
                CacheHint::default()
            )
        ));

        assert!(result[1].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(
                    MaskedGrid2D::new(
                        Grid2D::new([3, 2].into(), vec![1, 1, 1, 0, 1, 1]).unwrap(),
                        Grid2D::new([3, 2].into(), vec![true, true, true, false, true, true])
                            .unwrap()
                    )
                    .unwrap()
                ),
                CacheHint::default()
            )
        ));
    }

    #[tokio::test]
    async fn test_query_not_aligned_with_window_reference() {
        let raster_tiles = make_raster();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Last {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
                window_reference: Some(TimeInstance::EPOCH_START),
                output_type: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(5, 5),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp = agg
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 2);
        assert!(result[0].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap()),
                CacheHint::default()
            )
        ));

        assert!(result[1].as_ref().unwrap().tiles_equal_ignoring_cache_hint(
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12]).unwrap()),
                CacheHint::default()
            )
        ));
    }

    fn make_raster() -> Vec<geoengine_datatypes::raster::RasterTile2D<u8>> {
        let raster_tiles = vec![
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap()),
                CacheHint::default(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12]).unwrap()),
                CacheHint::default(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7]).unwrap()),
                CacheHint::default(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1]).unwrap()),
                CacheHint::default(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap()),
                CacheHint::default(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12]).unwrap()),
                CacheHint::default(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(30, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7]).unwrap()),
                CacheHint::default(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(30, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1]).unwrap()),
                CacheHint::default(),
            ),
        ];
        raster_tiles
    }

    fn make_raster_with_no_data() -> Vec<geoengine_datatypes::raster::RasterTile2D<u8>> {
        let raster_tiles = vec![
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(EmptyGrid2D::new([3, 2].into())),
                CacheHint::default(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(
                    MaskedGrid2D::new(
                        Grid2D::new([3, 2].into(), vec![1, 2, 3, 42, 5, 6]).unwrap(),
                        Grid2D::new([3, 2].into(), vec![true, true, true, false, true, true])
                            .unwrap(),
                    )
                    .unwrap(),
                ),
                CacheHint::default(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(
                    MaskedGrid2D::new(
                        Grid2D::new([3, 2].into(), vec![7, 8, 9, 42, 11, 12]).unwrap(),
                        Grid2D::new([3, 2].into(), vec![true, true, true, false, true, true])
                            .unwrap(),
                    )
                    .unwrap(),
                ),
                CacheHint::default(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(EmptyGrid2D::new([3, 2].into())),
                CacheHint::default(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(
                    MaskedGrid2D::new(
                        Grid2D::new([3, 2].into(), vec![13, 42, 15, 16, 17, 18]).unwrap(),
                        Grid2D::new([3, 2].into(), vec![true, false, true, true, true, true])
                            .unwrap(),
                    )
                    .unwrap(),
                ),
                CacheHint::default(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(EmptyGrid2D::new([3, 2].into())),
                CacheHint::default(),
            ),
        ];
        raster_tiles
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_sums_multiple_bands() {
        let operator = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Sum {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                window_reference: Some(TimeInstance::from_millis(0).unwrap()),
                output_type: None,
            },
            sources: SingleRasterSource {
                raster: RasterStacker {
                    params: RasterStackerParams {},
                    sources: MultipleRasterSources {
                        rasters: vec![
                            MockRasterSource {
                                params: MockRasterSourceParams {
                                    data: make_raster(),
                                    result_descriptor: RasterResultDescriptor {
                                        data_type: RasterDataType::U8,
                                        spatial_reference: SpatialReference::epsg_4326().into(),
                                        time: None,
                                        bbox: None,
                                        resolution: None,
                                        bands: RasterBandDescriptors::new_single_band(),
                                    },
                                },
                            }
                            .boxed(),
                            MockRasterSource {
                                params: MockRasterSourceParams {
                                    data: make_raster(),
                                    result_descriptor: RasterResultDescriptor {
                                        data_type: RasterDataType::U8,
                                        spatial_reference: SpatialReference::epsg_4326().into(),
                                        time: None,
                                        bbox: None,
                                        resolution: None,
                                        bands: RasterBandDescriptors::new_single_band(),
                                    },
                                },
                            }
                            .boxed(),
                        ],
                    },
                }
                .boxed(),
            },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
            attributes: [0, 1].try_into().unwrap(),
        };
        let query_ctx = MockQueryContext::test_default();

        let query_processor = operator
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = query_processor
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .await;

        assert!(result.tiles_equal_ignoring_cache_hint(&[
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                Grid2D::new([3, 2].into(), vec![13, 13, 13, 13, 13, 13])
                    .unwrap()
                    .into(),
                CacheHint::default()
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                1,
                Grid2D::new([3, 2].into(), vec![13, 13, 13, 13, 13, 13])
                    .unwrap()
                    .into(),
                CacheHint::default()
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                Grid2D::new([3, 2].into(), vec![13, 13, 13, 13, 13, 13])
                    .unwrap()
                    .into(),
                CacheHint::default()
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                1,
                Grid2D::new([3, 2].into(), vec![13, 13, 13, 13, 13, 13])
                    .unwrap()
                    .into(),
                CacheHint::default()
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                Grid2D::new([3, 2].into(), vec![13, 13, 13, 13, 13, 13])
                    .unwrap()
                    .into(),
                CacheHint::default()
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                1,
                Grid2D::new([3, 2].into(), vec![13, 13, 13, 13, 13, 13])
                    .unwrap()
                    .into(),
                CacheHint::default()
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                Grid2D::new([3, 2].into(), vec![13, 13, 13, 13, 13, 13])
                    .unwrap()
                    .into(),
                CacheHint::default()
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                1,
                Grid2D::new([3, 2].into(), vec![13, 13, 13, 13, 13, 13])
                    .unwrap()
                    .into(),
                CacheHint::default()
            )
        ]));
    }

    #[tokio::test]
    async fn it_estimates_a_median() {
        let raster_tiles = make_raster();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::PercentileEstimate {
                    percentile: 0.5,
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 40,
                },
                window_reference: None,
                output_type: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 40),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp = agg
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 2);

        assert_eq!(
            result[0].grid_array,
            GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![6, 6, 6, 6, 6, 6]).unwrap())
        );
    }
}
