use std::sync::Arc;

use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedRasterOperator, InitializedSources, Operator,
    OperatorName, QueryContext, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
    ResultDescriptor, SingleRasterSource, TypedRasterQueryProcessor, WorkflowOperatorPath,
};

use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use geoengine_datatypes::primitives::RasterQueryRectangle;
use geoengine_datatypes::raster::{
    GridOrEmpty2D, MapElementsParallel, Pixel, RasterDataType, RasterTile2D,
};
use geoengine_expression::{
    DataType, ExpressionAst, ExpressionParser, LinkedExpression, Parameter,
};
use serde::{Deserialize, Serialize};

use super::RasterExpressionError;
use super::expression::get_expression_dependencies;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BandwiseExpressionParams {
    pub expression: String,
    pub output_type: RasterDataType,
    pub map_no_data: bool,
    // TODO: new unit for each band?
}

/// This `QueryProcessor` performs a unary expression on all bands of its input raster series.
pub type BandwiseExpression = Operator<BandwiseExpressionParams, SingleRasterSource>;

impl OperatorName for BandwiseExpression {
    const TYPE_NAME: &'static str = "BandwiseExpression";
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for BandwiseExpression {
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

        // TODO: ensure all bands have same measurement unit?

        let result_descriptor = in_descriptor.map_data_type(|_| self.params.output_type);

        let parameters = vec![Parameter::Number("x".into())];

        let expression = ExpressionParser::new(&parameters, DataType::Number)
            .map_err(RasterExpressionError::from)?
            .parse(
                "expression", // TODO: what is the name used for?
                &self.params.expression,
            )
            .map_err(RasterExpressionError::from)?;

        Ok(Box::new(InitializedBandwiseExpression {
            name,
            path,
            result_descriptor,
            source,
            expression,
            map_no_data: self.params.map_no_data,
        }))
    }

    span_fn!(BandwiseExpression);
}

pub struct InitializedBandwiseExpression {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
    expression: ExpressionAst,
    map_no_data: bool,
}

impl InitializedRasterOperator for InitializedBandwiseExpression {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let typed_raster_processor = self.source.query_processor()?.into_f64();

        let output_type = self.result_descriptor().data_type;

        // TODO: spawn a blocking task for the compilation process
        let expression_dependencies = get_expression_dependencies()
            .map_err(|source| RasterExpressionError::Dependencies { source })?;

        let expression = LinkedExpression::new(
            self.expression.name(),
            &self.expression.code(),
            expression_dependencies,
        )
        .map_err(RasterExpressionError::from)?;

        Ok(call_generic_raster_processor!(
            output_type,
            BandwiseExpressionProcessor::new(
                typed_raster_processor,
                self.result_descriptor.clone(),
                expression,
                self.map_no_data
            )
            .boxed()
        ))
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        BandwiseExpression::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }
}

pub(crate) struct BandwiseExpressionProcessor<TO> {
    source: Box<dyn RasterQueryProcessor<RasterType = f64>>,
    result_descriptor: RasterResultDescriptor,
    expression: Arc<LinkedExpression>,
    map_no_data: bool,
    phantom: std::marker::PhantomData<TO>,
}

impl<TO> BandwiseExpressionProcessor<TO>
where
    TO: Pixel,
{
    pub fn new(
        source: Box<dyn RasterQueryProcessor<RasterType = f64>>,
        result_descriptor: RasterResultDescriptor,
        expression: LinkedExpression,
        map_no_data: bool,
    ) -> Self {
        Self {
            source,
            result_descriptor,
            expression: Arc::new(expression),
            map_no_data,
            phantom: Default::default(),
        }
    }

    #[inline]
    fn compute_expression(
        raster: RasterTile2D<f64>,
        expression: &LinkedExpression,
        map_no_data: bool,
    ) -> Result<GridOrEmpty2D<TO>> {
        let expression = unsafe {
            // we have to "trust" that the function has the signature we expect
            expression
                .function_1::<Option<f64>>()
                .map_err(RasterExpressionError::from)?
        };

        let map_fn = |in_value: Option<f64>| {
            // TODO: could be a |in_value: T1| if map no data is false!
            if !map_no_data && in_value.is_none() {
                return None;
            }

            let result = expression(in_value);

            result.map(TO::from_)
        };

        let res = raster.grid_array.map_elements_parallel(map_fn);

        Result::Ok(res)
    }
}

#[async_trait]
impl<TO> RasterQueryProcessor for BandwiseExpressionProcessor<TO>
where
    TO: Pixel,
{
    type RasterType = TO;

    async fn raster_query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<TO>>>> {
        let stream = self
            .source
            .raster_query(query, ctx)
            .await?
            .and_then(move |tile| async move {
                let expression = self.expression.clone();
                let map_no_data = self.map_no_data;

                let time = tile.time;
                let tile_position = tile.tile_position;
                let band = tile.band;
                let global_geo_transform = tile.global_geo_transform;
                let cache_hint = tile.cache_hint;

                let out = crate::util::spawn_blocking_with_thread_pool(
                    ctx.thread_pool().clone(),
                    move || Self::compute_expression(tile, &expression, map_no_data),
                )
                .await??;

                Ok(RasterTile2D::new(
                    time,
                    tile_position,
                    band,
                    global_geo_transform,
                    out,
                    cache_hint,
                ))
            });

        Ok(stream.boxed())
    }

    fn raster_result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{CacheHint, TimeInterval},
        raster::{
            Grid, GridBoundingBox2D, GridShape, MapElements, RenameBands,
            TilesEqualIgnoringCacheHint,
        },
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    use crate::{
        engine::{
            MockExecutionContext, MultipleRasterSources, RasterBandDescriptors,
            SpatialGridDescriptor,
        },
        mock::{MockRasterSource, MockRasterSourceParams},
        processing::{RasterStacker, RasterStackerParams},
    };

    use super::*;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_computes_bandwise_expression() {
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
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let data2: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
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
                grid_array: Grid::new([2, 2].into(), vec![20, 21, 22, 23])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: None,
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                TestDefault::test_default(),
                GridBoundingBox2D::new_min_max(-2, -1, 0, 3).unwrap(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: result_descriptor.clone(),
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor,
            },
        }
        .boxed();

        let stacker = RasterStacker {
            params: RasterStackerParams {
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![mrs1, mrs2],
            },
        }
        .boxed();

        let expression = BandwiseExpression {
            params: BandwiseExpressionParams {
                expression: "x + 1".to_string(),
                output_type: RasterDataType::U8,
                map_no_data: false,
            },
            sources: SingleRasterSource { raster: stacker },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new_min_max(-2, -1, 0, 3).unwrap(),
            TimeInterval::new_unchecked(0, 10),
            [0, 1].try_into().unwrap(),
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let op = expression
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        let expected: Vec<RasterTile2D<u8>> = data
            .into_iter()
            .zip(data2.into_iter().map(|mut tile| {
                tile.band = 1;
                tile
            }))
            .flat_map(|(a, b)| vec![a.clone(), b.clone()])
            .map(|mut tile| {
                tile.grid_array = tile.grid_array.map_elements(|in_value: u8| in_value + 1);
                tile
            })
            .collect();

        assert!(expected.tiles_equal_ignoring_cache_hint(&result));
    }
}
