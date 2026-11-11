use super::first_last_subquery::{
    TemporalRasterAggregationSubQueryNoDataOnly, first_tile_fold_future, last_tile_fold_future,
};
use super::subquery::{TemporalRasterAggregationSubQuery, subquery_all_tiles_fold_fn};
use crate::adapters::SimpleRasterStackerAdapter;
use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedSources, Operator,
    ProcessingGraphOperatorPath, QueryProcessor, RasterOperator, SingleRasterSource,
    TimeDescriptor, TypedRasterOperator,
};
use crate::optimization::OptimizationError;
use crate::util::pixel_aggregation::{
    CountAggregator, FirstAggregator, LastAggregator, MaxAggregator, MeanAggregator, MinAggregator,
    PercentileEstimateAggregator, PixelAggregator, PropagateNoData, SumAggregator,
};
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
use futures::{Stream, StreamExt};
use geoengine_datatypes::primitives::{
    BandSelection, RasterQueryRectangle, RegularTimeDimension, SpatialResolution, TimeFilledItem,
    TimeInstance,
};
use geoengine_datatypes::raster::{GridBoundingBox2D, Pixel, RasterDataType, RasterTile2D};
use geoengine_datatypes::{primitives::TimeStep, raster::TilingSpecification};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::marker::PhantomData;
use std::sync::Arc;
use tracing::debug;

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

#[async_trait]
impl RasterOperator for TemporalRasterAggregation {
    async fn _initialize(
        self: Box<Self>,
        path: ProcessingGraphOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        ensure!(self.params.window.step > 0, error::WindowSizeMustNotBeZero);

        let name = CanonicOperatorName::from(&self);

        let initialized_source = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?;
        let source = initialized_source.raster;

        debug!(
            "Initializing TemporalRasterAggregation with {:?}.",
            &self.params
        );

        let mut out_result_descriptor = source.result_descriptor().clone();

        out_result_descriptor.time = TimeDescriptor::new(
            source.result_descriptor().time.bounds,
            geoengine_datatypes::primitives::TimeDimension::Regular(RegularTimeDimension {
                step: self.params.window,
                origin: self
                    .params
                    .window_reference
                    .unwrap_or(TimeInstance::from_millis_unchecked(0)),
            }),
        );

        if let Some(output_type) = self.params.output_type {
            out_result_descriptor.data_type = output_type;
        }

        let initialized_operator = InitializedTemporalRasterAggregation {
            name,
            path,
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
    path: ProcessingGraphOperatorPath,
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

    fn name(&self) -> &'static str {
        TemporalRasterAggregation::TYPE_NAME
    }

    fn path(&self) -> ProcessingGraphOperatorPath {
        self.path.clone()
    }

    fn optimize(
        &self,
        target_resolution: SpatialResolution,
    ) -> Result<TypedRasterOperator, OptimizationError> {
        Ok(TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: self.aggregation_type,
                window: self.window,
                window_reference: Some(self.window_reference),
                output_type: self.output_type,
            },
            sources: SingleRasterSource {
                raster: self.source.optimize(target_resolution)?,
            },
        }
        .typed())
    }
}

pub struct TemporalRasterAggregationProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    result_descriptor: RasterResultDescriptor,
    aggregation_type: Aggregation,
    source: Q,
    tiling_specification: TilingSpecification,
}

impl<Q, P> TemporalRasterAggregationProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>
        + QueryProcessor<
            Output = RasterTile2D<P>,
            SpatialBounds = GridBoundingBox2D,
            Selection = BandSelection,
            ResultDescription = RasterResultDescriptor,
        >,
    P: Pixel,
{
    fn new(
        result_descriptor: RasterResultDescriptor,
        aggregation_type: Aggregation,
        source: Q,
        tiling_specification: TilingSpecification,
    ) -> Self {
        Self {
            result_descriptor,
            aggregation_type,
            source,
            tiling_specification,
        }
    }

    fn create_subquery<A: PixelAggregator<P> + 'static, FoldFn>(
        aggregator: A,
        fold_fn: FoldFn,
    ) -> TemporalRasterAggregationSubQuery<FoldFn, P, A> {
        TemporalRasterAggregationSubQuery {
            aggregator: Arc::new(aggregator),
            fold_fn,
            _phantom_pixel_type: PhantomData,
        }
    }

    fn create_subquery_first<F>(fold_fn: F) -> TemporalRasterAggregationSubQueryNoDataOnly<F, P> {
        TemporalRasterAggregationSubQueryNoDataOnly {
            fold_fn,

            _phantom_pixel_type: PhantomData,
        }
    }

    fn create_subquery_last<F>(fold_fn: F) -> TemporalRasterAggregationSubQueryNoDataOnly<F, P> {
        TemporalRasterAggregationSubQueryNoDataOnly {
            fold_fn,

            _phantom_pixel_type: PhantomData,
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn create_subquery_adapter_stream_for_single_band<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<futures::stream::BoxStream<'a, Result<RasterTile2D<P>>>> {
        ensure!(
            query.attributes().count() == 1,
            error::InvalidBandCount {
                expected: 1u32,
                found: query.attributes().count()
            }
        );

        let grid_desc = self.result_descriptor.spatial_grid_descriptor();
        let tiling_strategy = grid_desc
            .tiling_grid_definition(self.tiling_specification)
            .generate_data_tiling_strategy();

        let time_stream: std::pin::Pin<
            Box<
                dyn Stream<
                        Item = std::result::Result<
                            geoengine_datatypes::primitives::TimeInterval,
                            error::Error,
                        >,
                    > + Send,
            >,
        > = self.time_query(query.time_interval(), ctx).await?;

        // aggregates all tiles with the aggregator, which ignores or propagates no-data
        macro_rules! aggregate {
            ($aggregator:expr) => {
                Self::create_subquery($aggregator, subquery_all_tiles_fold_fn)
                    .into_raster_subquery_adapter(
                        &self.source,
                        query,
                        ctx,
                        tiling_strategy,
                        time_stream,
                    )
                    .box_pin()
            };
            ($aggregator:expr, ignore_no_data: $ignore_no_data:expr) => {
                if $ignore_no_data {
                    aggregate!($aggregator)
                } else {
                    aggregate!(PropagateNoData($aggregator))
                }
            };
        }

        Ok(match self.aggregation_type {
            Aggregation::Min { ignore_no_data } => {
                aggregate!(MinAggregator::default(), ignore_no_data: ignore_no_data)
            }
            Aggregation::Max { ignore_no_data } => {
                aggregate!(MaxAggregator::default(), ignore_no_data: ignore_no_data)
            }
            Aggregation::First {
                ignore_no_data: true,
            } => aggregate!(FirstAggregator::default()),
            Aggregation::First {
                ignore_no_data: false,
            } => Self::create_subquery_first(first_tile_fold_future::<P>)
                .into_raster_subquery_adapter(
                    &self.source,
                    query,
                    ctx,
                    tiling_strategy,
                    time_stream,
                )
                .box_pin(),
            Aggregation::Last {
                ignore_no_data: true,
            } => aggregate!(LastAggregator::default()),
            Aggregation::Last {
                ignore_no_data: false,
            } => Self::create_subquery_last(last_tile_fold_future::<P>)
                .into_raster_subquery_adapter(
                    &self.source,
                    query,
                    ctx,
                    tiling_strategy,
                    time_stream,
                )
                .box_pin(),
            Aggregation::Mean { ignore_no_data } => {
                aggregate!(MeanAggregator, ignore_no_data: ignore_no_data)
            }
            Aggregation::Sum { ignore_no_data } => {
                aggregate!(SumAggregator::default(), ignore_no_data: ignore_no_data)
            }
            Aggregation::Count { ignore_no_data } => {
                aggregate!(CountAggregator::default(), ignore_no_data: ignore_no_data)
            }
            Aggregation::PercentileEstimate {
                ignore_no_data,
                percentile,
            } => aggregate!(
                PercentileEstimateAggregator::new(percentile),
                ignore_no_data: ignore_no_data
            ),
        })
    }
}

#[async_trait]
impl<Q, P> QueryProcessor for TemporalRasterAggregationProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P> + Send + Sync,
    P: Pixel,
{
    type Output = RasterTile2D<P>;
    type SpatialBounds = GridBoundingBox2D;
    type Selection = BandSelection;
    type ResultDescription = RasterResultDescriptor;

    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<futures::stream::BoxStream<'a, Result<Self::Output>>> {
        SimpleRasterStackerAdapter::stack_individual_aligned_raster_bands(
            &query,
            ctx,
            |query, ctx| async {
                self.create_subquery_adapter_stream_for_single_band(query, ctx)
                    .await
            },
        )
        .await
        .map_err(|e| crate::error::Error::SimpleRasterStacker { source: e })
        .map(StreamExt::boxed)
    }

    fn result_descriptor(&self) -> &Self::ResultDescription {
        &self.result_descriptor
    }
}

#[async_trait]
impl<Q, P> RasterQueryProcessor for TemporalRasterAggregationProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    type RasterType = P;

    async fn _time_query<'a>(
        &'a self,
        query: geoengine_datatypes::primitives::TimeInterval,
        _ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<futures::stream::BoxStream<'a, Result<geoengine_datatypes::primitives::TimeInterval>>>
    {
        let ti_iter = self
            .result_descriptor
            .time
            .dimension
            .unwrap_regular()
            .expect("must be regular as this operator only creates regular time dimensions")
            .intersecting_intervals(query.time())?
            .map(Ok);
        Ok(futures::stream::iter(ti_iter).boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        engine::{MockExecutionContext, MultipleRasterSources, QueryContext},
        processing::{
            Expression, ExpressionParams,
            raster_stacker::{RasterStacker, RasterStackerParams},
        },
        util::test::AsciiRasterTilesExt,
    };
    use futures::stream::StreamExt;
    use geoengine_datatypes::{
        primitives::TimeInterval,
        raster::{Grid2D, GridBoundingBox2D, GridOrEmpty, RasterDataType, RenameBands},
        util::test::{AsciiRasterTiles, assert_raster_tiles_eq_ascii},
    };
    use indoc::indoc;

    #[tokio::test]
    async fn test_min() {
        let raster = test_raster();

        let mrs = raster.mock_raster_source::<u8>().typed();

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
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, 0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 40),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let qp = agg
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 20)
                +-----+-----+
                | 1 2 | 6 5 |
                | 3 4 | 4 3 |
                | 5 6 | 2 1 |
                +-----+-----+
                [20, 40)
                +-----+-----+
                | 1 2 | 6 5 |
                | 3 4 | 4 3 |
                | 5 6 | 2 1 |
                +-----+-----+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn test_max() {
        let raster = test_raster();

        let mrs = raster.mock_raster_source::<u8>().typed();

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
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 40),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let qp = agg
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 20)
                +-------+-------+
                | 12 11 |  7  8 |
                | 10  9 |  9 10 |
                |  8  7 | 11 12 |
                +-------+-------+
                [20, 40)
                +-------+-------+
                | 12 11 |  7  8 |
                | 10  9 |  9 10 |
                |  8  7 | 11 12 |
                +-------+-------+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn test_max_with_no_data() {
        let raster = test_raster(); // TODO: switch to test_raster_with_no_data?

        let mrs = raster.mock_raster_source::<u8>().typed();

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
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 40),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let qp = agg
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 20)
                +-------+-------+
                | 12 11 |  7  8 |
                | 10  9 |  9 10 |
                |  8  7 | 11 12 |
                +-------+-------+
                [20, 40)
                +-------+-------+
                | 12 11 |  7  8 |
                | 10  9 |  9 10 |
                |  8  7 | 11 12 |
                +-------+-------+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn test_max_with_no_data_but_ignoring_it() {
        let raster = test_raster(); // TODO: switch to test_raster_with_no_data?

        let mrs = raster.mock_raster_source::<u8>().typed();

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
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 40),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let qp = agg
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 20)
                +-------+-------+
                | 12 11 |  7  8 |
                | 10  9 |  9 10 |
                |  8  7 | 11 12 |
                +-------+-------+
                [20, 40)
                +-------+-------+
                | 12 11 |  7  8 |
                | 10  9 |  9 10 |
                |  8  7 | 11 12 |
                +-------+-------+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn test_only_no_data() {
        let raster = AsciiRasterTiles::new(indoc! {"
            [0, 20)
            +-----+
            | _ _ |
            | _ _ |
            | _ _ |
            +-----+
            "})
        .tile_origin([-1, 0]);

        let mrs = raster.mock_raster_source::<u8>().typed();

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
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new_min_max(-3, -1, 0, 1).unwrap(),
            TimeInterval::new_unchecked(0, 20),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let qp = agg
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 20)
                +-----+
                | _ _ |
                | _ _ |
                | _ _ |
                +-----+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn test_first_with_no_data() {
        let raster = test_raster_with_no_data();

        let mrs = raster.mock_raster_source::<u8>().typed();

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
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 30),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let qp = agg
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 30)
                +-------+-------+
                |  7  8 |  1  2 |
                |  9 16 |  3  _ |
                | 11 12 |  5  6 |
                +-------+-------+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn test_last_with_no_data() {
        let raster = test_raster_with_no_data();

        let mrs = raster.mock_raster_source::<u8>().typed();
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
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 30),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let qp = agg
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 30)
                +-------+-------+
                | 13  8 |  1  2 |
                | 15 16 |  3  _ |
                | 17 18 |  5  6 |
                +-------+-------+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn test_last() {
        let raster = test_raster_with_no_data();

        let mrs = raster.mock_raster_source::<u8>().typed();

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
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, 0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 30),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let qp = agg
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 30)
                +-------+-------+
                | 13  _ |  _  _ |
                | 15 16 |  _  _ |
                | 17 18 |  _  _ |
                +-------+-------+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn test_first() {
        let raster = test_raster_with_no_data();

        let mrs = raster.mock_raster_source::<u8>().typed();

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
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 30),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let qp = agg
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 30)
                +-----+-----+
                | _ _ | 1 2 |
                | _ _ | 3 _ |
                | _ _ | 5 6 |
                +-----+-----+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn test_mean_nodata() {
        let raster = test_raster_with_no_data();

        let mrs = raster.mock_raster_source::<u8>().typed();

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
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 30),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let qp = agg
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 30)
                +-----+-----+
                | _ _ | 1 2 |
                | _ _ | 3 _ |
                | _ _ | 5 6 |
                +-----+-----+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn test_mean_ignore_no_data() {
        let raster = test_raster_with_no_data();

        let mrs = raster.mock_raster_source::<u8>().typed();

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
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 30),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let qp = agg
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 30)
                +-------+-------+
                | 10  8 |  1  2 |
                | 12 16 |  3  _ |
                | 14 15 |  5  6 |
                +-------+-------+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn test_sum_without_nodata() {
        let raster = test_raster();

        let mrs = raster.mock_raster_source::<u8>().typed();

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
            sources: SingleRasterSource { raster: mrs },
        }
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 30),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let query_processor = operator
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 20)
                +-------+-------+
                | 13 13 | 13 13 |
                | 13 13 | 13 13 |
                | 13 13 | 13 13 |
                +-------+-------+
                [20, 40)
                +-------+-------+
                | 13 13 | 13 13 |
                | 13 13 | 13 13 |
                | 13 13 | 13 13 |
                +-------+-------+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn test_sum_nodata() {
        let raster = test_raster_with_no_data();

        let mrs = raster.mock_raster_source::<u8>().typed();

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
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 30),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let qp = agg
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 30)
                +-----+-----+
                | _ _ | 1 2 |
                | _ _ | 3 _ |
                | _ _ | 5 6 |
                +-----+-----+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn test_sum_ignore_no_data() {
        let raster = test_raster_with_no_data();

        let mrs = raster.mock_raster_source::<u8>().typed();

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
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 30),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let qp = agg
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 30)
                +-------+-------+
                | 20  8 |  1  2 |
                | 24 16 |  3  _ |
                | 28 30 |  5  6 |
                +-------+-------+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn test_sum_with_larger_data_type() {
        let raster = test_raster();

        let mrs = raster.mock_raster_source::<u8>().typed();

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
                        output_band: None,
                        map_no_data: true,
                    },
                    sources: SingleRasterSource { raster: mrs },
                }
                .typed(),
            },
        }
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 30),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let query_processor = operator
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 20)
                +---------+---------+
                | 260 260 | 260 260 |
                | 260 260 | 260 260 |
                | 260 260 | 260 260 |
                +---------+---------+
                [20, 40)
                +---------+---------+
                | 260 260 | 260 260 |
                | 260 260 | 260 260 |
                | 260 260 | 260 260 |
                +---------+---------+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn test_count_without_nodata() {
        let raster = test_raster();

        let mrs = raster.mock_raster_source::<u8>().typed();

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
            sources: SingleRasterSource { raster: mrs },
        }
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 30),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let query_processor = operator
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 20)
                +-----+-----+
                | 2 2 | 2 2 |
                | 2 2 | 2 2 |
                | 2 2 | 2 2 |
                +-----+-----+
                [20, 40)
                +-----+-----+
                | 2 2 | 2 2 |
                | 2 2 | 2 2 |
                | 2 2 | 2 2 |
                +-----+-----+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn test_count_nodata() {
        let raster = test_raster_with_no_data();

        let mrs = raster.mock_raster_source::<u8>().typed();

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
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 30),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let qp = agg
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 30)
                +-----+-----+
                | _ _ | 1 1 |
                | _ _ | 1 _ |
                | _ _ | 1 1 |
                +-----+-----+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn test_count_ignore_no_data() {
        let raster = test_raster_with_no_data();

        let mrs = raster.mock_raster_source::<u8>().typed();

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
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 30),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let qp = agg
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 30)
                +-----+-----+
                | 2 1 | 1 1 |
                | 2 1 | 1 _ |
                | 2 2 | 1 1 |
                +-----+-----+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn test_query_not_aligned_with_window_reference() {
        let raster = test_raster();

        let mrs = raster.mock_raster_source::<u8>().typed();

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
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(5, 5),
            BandSelection::first(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let qp = agg
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 30)
                +-------+-------+
                |  1  2 |  7  8 |
                |  3  4 |  9 10 |
                |  5  6 | 11 12 |
                +-------+-------+
                "})
            .tile_origin([-1, 0]),
        );
    }

    /// Four time steps of two 3×2 tiles; the steps 0 and 2 and the steps 1 and 3 are equal.
    fn test_raster() -> AsciiRasterTiles<'static> {
        AsciiRasterTiles::new(indoc! {"
            [0, 10)
            +-------+-------+
            |  1  2 |  7  8 |
            |  3  4 |  9 10 |
            |  5  6 | 11 12 |
            +-------+-------+
            [10, 20)
            +-------+-------+
            | 12 11 |  6  5 |
            | 10  9 |  4  3 |
            |  8  7 |  2  1 |
            +-------+-------+
            [20, 30)
            +-------+-------+
            |  1  2 |  7  8 |
            |  3  4 |  9 10 |
            |  5  6 | 11 12 |
            +-------+-------+
            [30, 40)
            +-------+-------+
            | 12 11 |  6  5 |
            | 10  9 |  4  3 |
            |  8  7 |  2  1 |
            +-------+-------+
            "})
        .tile_origin([-1, 0])
    }

    /// Three time steps of two 3×2 tiles with no-data and empty tiles.
    fn test_raster_with_no_data() -> AsciiRasterTiles<'static> {
        AsciiRasterTiles::new(indoc! {"
            [0, 10)
            +-------+-------+
            |  _  _ |  1  2 |
            |  _  _ |  3  _ |
            |  _  _ |  5  6 |
            +-------+-------+
            [10, 20)
            +-------+-------+
            |  7  8 |  _  _ |
            |  9  _ |  _  _ |
            | 11 12 |  _  _ |
            +-------+-------+
            [20, 30)
            +-------+-------+
            | 13  _ |  _  _ |
            | 15 16 |  _  _ |
            | 17 18 |  _  _ |
            +-------+-------+
            "})
        .tile_origin([-1, 0])
    }

    #[tokio::test]
    async fn it_sums_multiple_bands() {
        let raster = test_raster();

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
                    params: RasterStackerParams {
                        rename_bands: RenameBands::Default,
                    },
                    sources: MultipleRasterSources {
                        rasters: vec![
                            raster.mock_raster_source::<u8>().typed(),
                            raster.mock_raster_source::<u8>().typed(),
                        ],
                    },
                }
                .typed(),
            },
        }
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 30),
            [0, 1].try_into().unwrap(),
        );
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let query_processor = operator
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 20)
                +-------+-------+
                | 13 13 | 13 13 |
                | 13 13 | 13 13 |
                | 13 13 | 13 13 |
                +-------+-------+
                [0, 20) band 1
                +-------+-------+
                | 13 13 | 13 13 |
                | 13 13 | 13 13 |
                | 13 13 | 13 13 |
                +-------+-------+
                [20, 40)
                +-------+-------+
                | 13 13 | 13 13 |
                | 13 13 | 13 13 |
                | 13 13 | 13 13 |
                +-------+-------+
                [20, 40) band 1
                +-------+-------+
                | 13 13 | 13 13 |
                | 13 13 | 13 13 |
                | 13 13 | 13 13 |
                +-------+-------+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn it_estimates_a_median() {
        let raster = test_raster();

        let mrs = raster.mock_raster_source::<u8>().typed();

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
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 40),
            BandSelection::first(),
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let qp = agg
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
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

    #[tokio::test]
    async fn it_propagates_query_cancellation_as_error() {
        let raster = test_raster();

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
                raster: raster.mock_raster_source::<u8>().typed(),
            },
        }
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster.tiling_specification());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-3, -0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 30),
            BandSelection::first(),
        );
        let mut query_ctx = exe_ctx.mock_query_context_test_default();
        let trigger = query_ctx.abort_trigger().unwrap();

        let query_processor = operator
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let mut result = Box::pin(
            query_processor
                .raster_query(query_rect, &query_ctx)
                .await
                .unwrap(),
        );

        assert!(matches!(result.next().await, Some(Ok(_))));

        trigger.abort();

        match result.next().await {
            Some(Err(e)) => assert!(matches!(e, error::Error::QueryCanceled)),
            other => panic!("expected QueryCanceled error after abort, got {other:?}"),
        }
        assert!(result.next().await.is_none());
    }
}
