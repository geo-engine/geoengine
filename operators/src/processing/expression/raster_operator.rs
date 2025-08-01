use super::{
    RasterExpressionError, get_expression_dependencies,
    raster_query_processor::{ExpressionInput, ExpressionQueryProcessor},
};
use crate::{
    engine::{
        CanonicOperatorName, InitializedRasterOperator, InitializedSources, Operator, OperatorName,
        RasterBandDescriptor, RasterBandDescriptors, RasterOperator, RasterQueryProcessor,
        RasterResultDescriptor, SingleRasterSource, TypedRasterQueryProcessor,
        WorkflowOperatorPath,
    },
    error::InvalidNumberOfExpressionInputBands,
    processing::expression::canonicalize_name,
    util::Result,
};
use async_trait::async_trait;
use geoengine_datatypes::raster::RasterDataType;
use geoengine_expression::{
    DataType, ExpressionAst, ExpressionParser, LinkedExpression, Parameter,
};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::borrow::Cow;

/// Parameters for the `Expression` operator.
/// * The `expression` must only contain simple arithmetic
///   calculations.
/// * `output_type` is the data type of the produced raster tiles.
/// * `output_no_data_value` is the no data value of the output raster
/// * `output_measurement` is the measurement description of the output
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExpressionParams {
    pub expression: String,
    pub output_type: RasterDataType,
    pub output_band: Option<RasterBandDescriptor>,
    pub map_no_data: bool,
}
/// The `Expression` operator calculates an expression for all pixels of the input rasters bands and
/// produces raster tiles of a given output type
pub type Expression = Operator<ExpressionParams, SingleRasterSource>;

/// Create a parameter name from an index.
/// Starts with `A`.
///
/// ## Note
///
/// This function only makes sense for indices between 0 and 25.
///
fn index_to_parameter(index: usize) -> String {
    let index = index as u32;
    let start_index = 'A' as u32;

    let parameter = char::from_u32(start_index + index).unwrap_or_default();

    parameter.to_string()
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for Expression {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let name = CanonicOperatorName::from(&self);

        let source = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?
            .raster;

        let in_descriptor = source.result_descriptor();

        ensure!(
            !in_descriptor.bands.is_empty() && in_descriptor.bands.len() <= 8,
            InvalidNumberOfExpressionInputBands {
                found: in_descriptor.bands.len()
            }
        );

        // we refer to raster bands by A, B, C, …
        let parameters = (0..in_descriptor.bands.len())
            .map(|i| {
                let parameter = index_to_parameter(i);
                Parameter::Number(parameter.into())
            })
            .collect::<Vec<_>>();

        let expression = ExpressionParser::new(&parameters, DataType::Number)
            .map_err(RasterExpressionError::from)?
            .parse(
                self.params
                    .output_band
                    .as_ref()
                    .map_or(Cow::Borrowed("expression"), |b| {
                        // allow only idents
                        Cow::Owned(canonicalize_name(&b.name))
                    })
                    .as_ref(),
                &self.params.expression,
            )
            .map_err(RasterExpressionError::from)?;

        let result_descriptor = RasterResultDescriptor {
            data_type: self.params.output_type,
            spatial_reference: in_descriptor.spatial_reference,
            time: in_descriptor.time,
            spatial_grid: in_descriptor.spatial_grid,
            bands: RasterBandDescriptors::new(vec![
                self.params
                    .output_band
                    .unwrap_or(RasterBandDescriptor::new_unitless("expression".into())),
            ])?,
        };

        let initialized_operator = InitializedExpression {
            name,
            path,
            result_descriptor,
            source,
            expression,
            map_no_data: self.params.map_no_data,
        };

        Ok(initialized_operator.boxed())
    }

    span_fn!(Expression);
}

impl OperatorName for Expression {
    const TYPE_NAME: &'static str = "Expression";
}

pub struct InitializedExpression {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
    expression: ExpressionAst,
    map_no_data: bool,
}

/// Macro for generating the match cases for number of bands to the `ExpressionInput` struct.
macro_rules! generate_match_cases {
    ($num_bands:expr, $output_type:expr, $expression:expr, $source_processor:expr, $result_descriptor:expr, $map_no_data:expr, $($x:expr),*) => {
        match $num_bands {
            $(
                $x => call_generic_raster_processor!(
                    $output_type,
                    ExpressionQueryProcessor::new(
                        $expression,
                        ExpressionInput::<$x> {
                            raster: $source_processor,
                        },
                        $result_descriptor,
                        $map_no_data.clone(),
                    )
                    .boxed()
                ),
            )*
            _ => unreachable!("number of bands was checked to be between 1 and 8"),
        }
    };
}

impl InitializedRasterOperator for InitializedExpression {
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
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

        let source_processor = self.source.query_processor()?.into_f64();

        Ok(generate_match_cases!(
            self.source.result_descriptor().bands.len(),
            output_type,
            expression,
            source_processor,
            self.result_descriptor.clone(),
            self.map_no_data,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8
        ))
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        Expression::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        MockExecutionContext, MultipleRasterSources, QueryProcessor, SpatialGridDescriptor,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use crate::processing::{RasterStacker, RasterStackerParams};
    use futures::StreamExt;
    use geoengine_datatypes::primitives::{BandSelection, CacheHint, CacheTtlSeconds, Measurement};
    use geoengine_datatypes::primitives::{RasterQueryRectangle, TimeInterval};
    use geoengine_datatypes::raster::{
        Grid2D, GridBoundingBox2D, GridOrEmpty, MapElements, MaskedGrid2D, RasterTile2D,
        RenameBands, TileInformation, TilingSpecification,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;

    #[test]
    fn deserialize_params() {
        let s =
            r#"{"expression":"1*A","outputType":"F64","outputMeasurement":null,"mapNoData":false}"#;

        assert_eq!(
            serde_json::from_str::<ExpressionParams>(s).unwrap(),
            ExpressionParams {
                expression: "1*A".to_owned(),
                output_type: RasterDataType::F64,
                output_band: None,
                map_no_data: false,
            }
        );
    }

    #[test]
    fn serialize_params() {
        let s = r#"{"expression":"1*A","outputType":"F64","outputBand":null,"mapNoData":false}"#;

        assert_eq!(
            s,
            serde_json::to_string(&ExpressionParams {
                expression: "1*A".to_owned(),
                output_type: RasterDataType::F64,
                output_band: None,
                map_no_data: false,
            })
            .unwrap()
        );
    }

    #[test]
    fn serialize_params_no_data() {
        let s = r#"{"expression":"1*A","outputType":"F64","outputBand":null,"mapNoData":false}"#;

        assert_eq!(
            s,
            serde_json::to_string(&ExpressionParams {
                expression: "1*A".to_owned(),
                output_type: RasterDataType::F64,
                output_band: None,
                map_no_data: false,
            })
            .unwrap()
        );
    }

    #[tokio::test]
    async fn basic_unary() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            tile_size_in_pixels,
        };

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let raster_a = make_raster(Some(3));

        let o = Expression {
            params: ExpressionParams {
                expression: "2 * A".to_string(),
                output_type: RasterDataType::I8,
                output_band: None,
                map_no_data: false,
            },
            sources: SingleRasterSource { raster: raster_a },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &ctx)
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = ctx.mock_query_context(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle::new(
                    GridBoundingBox2D::new([-3, 0], [-1, 1]).unwrap(),
                    Default::default(),
                    BandSelection::first(),
                ),
                &ctx,
            )
            .await
            .unwrap();

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0].as_ref().unwrap().grid_array,
            GridOrEmpty::from(
                MaskedGrid2D::new(
                    Grid2D::new([3, 2].into(), vec![2, 4, 0, 8, 10, 12],).unwrap(),
                    Grid2D::new([3, 2].into(), vec![true, true, false, true, true, true],).unwrap()
                )
                .unwrap()
            )
        );
    }

    #[tokio::test]
    async fn unary_map_no_data() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            tile_size_in_pixels,
        };

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let raster_a = make_raster(Some(3));

        let o = Expression {
            params: ExpressionParams {
                expression: "2 * A".to_string(),
                output_type: RasterDataType::I8,
                output_band: None,
                map_no_data: true,
            },
            sources: SingleRasterSource { raster: raster_a },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &ctx)
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = ctx.mock_query_context(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle::new(
                    GridBoundingBox2D::new([-3, 0], [-1, 1]).unwrap(),
                    Default::default(),
                    BandSelection::first(),
                ),
                &ctx,
            )
            .await
            .unwrap();

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0].as_ref().unwrap().grid_array,
            GridOrEmpty::from(
                MaskedGrid2D::new(
                    Grid2D::new([3, 2].into(), vec![2, 4, 0, 8, 10, 12],).unwrap(), // pixels with no data are turned to Default::default() wich is 0. And 0 is the out_no_data value.
                    Grid2D::new([3, 2].into(), vec![true, true, false, true, true, true],).unwrap()
                )
                .unwrap()
            )
        );
    }

    #[tokio::test]
    async fn basic_binary() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            tile_size_in_pixels,
        };

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let raster_a = make_raster(None);
        let raster_b = make_raster(None);

        let o = Expression {
            params: ExpressionParams {
                expression: "A+B".to_string(),
                output_type: RasterDataType::I8,
                output_band: None,
                map_no_data: false,
            },
            sources: SingleRasterSource {
                raster: RasterStacker {
                    params: RasterStackerParams {
                        rename_bands: RenameBands::Default,
                    },
                    sources: MultipleRasterSources {
                        rasters: vec![raster_a, raster_b],
                    },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &ctx)
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = ctx.mock_query_context(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle::new(
                    GridBoundingBox2D::new([-3, 0], [-1, 1]).unwrap(),
                    Default::default(),
                    BandSelection::first(),
                ),
                &ctx,
            )
            .await
            .unwrap();

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0].as_ref().unwrap().grid_array,
            Grid2D::new([3, 2].into(), vec![2, 4, 6, 8, 10, 12],)
                .unwrap()
                .into()
        );
    }

    #[tokio::test]
    async fn basic_coalesce() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            tile_size_in_pixels,
        };

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let raster_a = make_raster(Some(3));
        let raster_b = make_raster(None);

        let o = Expression {
            params: ExpressionParams {
                expression: "if A IS NODATA {
                       B * 2
                   } else if A == 6 {
                       NODATA
                   } else {
                       A
                   }"
                .to_string(),
                output_type: RasterDataType::I8,
                output_band: None,
                map_no_data: true,
            },
            sources: SingleRasterSource {
                raster: RasterStacker {
                    params: RasterStackerParams {
                        rename_bands: RenameBands::Default,
                    },
                    sources: MultipleRasterSources {
                        rasters: vec![raster_a, raster_b],
                    },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &ctx)
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = ctx.mock_query_context(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle::new(
                    GridBoundingBox2D::new([-3, 0], [-1, 1]).unwrap(),
                    Default::default(),
                    BandSelection::first(),
                ),
                &ctx,
            )
            .await
            .unwrap();

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0].as_ref().unwrap().grid_array,
            GridOrEmpty::from(
                MaskedGrid2D::new(
                    Grid2D::new([3, 2].into(), vec![1, 2, 6, 4, 5, 0],).unwrap(),
                    Grid2D::new([3, 2].into(), vec![true, true, true, true, true, false],).unwrap()
                )
                .unwrap()
            )
        );
    }

    #[tokio::test]
    async fn basic_ternary() {
        let no_data_value = 3;
        let no_data_value_option = Some(no_data_value);

        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            tile_size_in_pixels,
        };

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let raster_a = make_raster(no_data_value_option);
        let raster_b = make_raster(no_data_value_option);
        let raster_c = make_raster(no_data_value_option);

        let o = Expression {
            params: ExpressionParams {
                expression: "A+B+C".to_string(),
                output_type: RasterDataType::I8,
                output_band: None,
                map_no_data: false,
            },
            sources: SingleRasterSource {
                raster: RasterStacker {
                    params: RasterStackerParams {
                        rename_bands: RenameBands::Default,
                    },
                    sources: MultipleRasterSources {
                        rasters: vec![raster_a, raster_b, raster_c],
                    },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &ctx)
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = ctx.mock_query_context(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle::new(
                    GridBoundingBox2D::new([-3, 0], [-1, 1]).unwrap(),
                    Default::default(),
                    BandSelection::first(),
                ),
                &ctx,
            )
            .await
            .unwrap();

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;

        assert_eq!(result.len(), 1);

        let first_result = result[0].as_ref().unwrap();

        assert!(!first_result.is_empty());

        let grid = match &first_result.grid_array {
            GridOrEmpty::Grid(g) => g,
            GridOrEmpty::Empty(_) => panic!(),
        };

        let res: Vec<Option<i8>> = grid.masked_element_deref_iterator().collect();

        assert_eq!(
            res,
            [Some(3), Some(6), None, Some(12), Some(15), Some(18)] // third is None is because all inputs are masked because 3 == no_data_value
        );
    }

    #[tokio::test]
    async fn octave_inputs() {
        let no_data_value = 0;
        let no_data_value_option = Some(no_data_value);

        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            tile_size_in_pixels,
        };

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let raster_a = make_raster(no_data_value_option);
        let raster_b = make_raster(no_data_value_option);
        let raster_c = make_raster(no_data_value_option);
        let raster_d = make_raster(no_data_value_option);
        let raster_e = make_raster(no_data_value_option);
        let raster_f = make_raster(no_data_value_option);
        let raster_g = make_raster(no_data_value_option);
        let raster_h = make_raster(no_data_value_option);

        let o = Expression {
            params: ExpressionParams {
                expression: "A+B+C+D+E+F+G+H".to_string(),
                output_type: RasterDataType::I8,
                output_band: None,
                map_no_data: false,
            },
            sources: SingleRasterSource {
                raster: RasterStacker {
                    params: RasterStackerParams {
                        rename_bands: RenameBands::Default,
                    },
                    sources: MultipleRasterSources {
                        rasters: vec![
                            raster_a, raster_b, raster_c, raster_d, raster_e, raster_f, raster_g,
                            raster_h,
                        ],
                    },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &ctx)
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = ctx.mock_query_context(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle::new(
                    GridBoundingBox2D::new([-3, 0], [-1, 1]).unwrap(),
                    Default::default(),
                    BandSelection::first(),
                ),
                &ctx,
            )
            .await
            .unwrap();

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0].as_ref().unwrap().grid_array,
            Grid2D::new([3, 2].into(), vec![8, 16, 24, 32, 40, 48],)
                .unwrap()
                .into()
        );
    }

    #[tokio::test]
    async fn it_classifies() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            tile_size_in_pixels,
        };

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let operator = Expression {
            params: ExpressionParams {
                expression: "if A <= 1 { 0 } else if A <= 3 { 1 } else { 2 }".to_string(),
                output_type: RasterDataType::U8,
                output_band: Some(RasterBandDescriptor::new(
                    "a class".into(),
                    Measurement::classification(
                        "classes".into(),
                        [
                            (0, "Class A".into()),
                            (1, "Class B".into()),
                            (2, "Class C".into()),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                )),
                map_no_data: false,
            },
            sources: SingleRasterSource {
                raster: RasterStacker {
                    params: RasterStackerParams {
                        rename_bands: RenameBands::Default,
                    },
                    sources: MultipleRasterSources {
                        rasters: vec![make_raster(None)],
                    },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &ctx)
        .await
        .unwrap();

        let processor = operator.query_processor().unwrap().get_u8().unwrap();

        let ctx = ctx.mock_query_context(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle::new(
                    GridBoundingBox2D::new_min_max(-3, -1, 0, 1).unwrap(),
                    Default::default(),
                    BandSelection::first(),
                ),
                &ctx,
            )
            .await
            .unwrap();

        let result: Vec<Result<RasterTile2D<u8>>> = result_stream.collect().await;

        assert_eq!(result.len(), 1);

        let first_result = result[0].as_ref().unwrap();

        assert!(!first_result.is_empty());

        let grid = match &first_result.grid_array {
            GridOrEmpty::Grid(g) => g,
            GridOrEmpty::Empty(_) => panic!(),
        };

        let res: Vec<Option<u8>> = grid.masked_element_deref_iterator().collect();

        assert_eq!(res, [Some(0), Some(1), Some(1), Some(2), Some(2), Some(2)]);
    }

    #[tokio::test]
    async fn test_functions() {
        let no_data_value = 0;
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            tile_size_in_pixels,
        };

        let ectx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let raster_a = make_raster(Some(no_data_value));

        let o = Expression {
            params: ExpressionParams {
                expression: "min(A * pi(), 10)".to_string(),
                output_type: RasterDataType::I8,
                output_band: None,
                map_no_data: false,
            },
            sources: SingleRasterSource {
                raster: RasterStacker {
                    params: RasterStackerParams {
                        rename_bands: RenameBands::Default,
                    },
                    sources: MultipleRasterSources {
                        rasters: vec![raster_a],
                    },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &ectx)
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = ectx.mock_query_context(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle::new(
                    GridBoundingBox2D::new([-3, 0], [-1, 1]).unwrap(),
                    Default::default(),
                    BandSelection::first(),
                ),
                &ctx,
            )
            .await
            .unwrap();

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0].as_ref().unwrap().grid_array,
            Grid2D::new([3, 2].into(), vec![3, 6, 9, 10, 10, 10],)
                .unwrap()
                .into()
        );
    }

    fn make_raster(no_data_value: Option<i8>) -> Box<dyn RasterOperator> {
        make_raster_with_cache_hint(no_data_value, CacheHint::no_cache())
    }

    fn make_raster_with_cache_hint(
        no_data_value: Option<i8>,
        cache_hint: CacheHint,
    ) -> Box<dyn RasterOperator> {
        let raster = Grid2D::<i8>::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap();

        let real_raster = if let Some(no_data_value) = no_data_value {
            MaskedGrid2D::from(raster)
                .map_elements(|e: Option<i8>| e.filter(|v| *v != no_data_value))
                .into()
        } else {
            GridOrEmpty::from(raster)
        };

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_tile_position: [-1, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
                global_geo_transform: TestDefault::test_default(),
            },
            0,
            real_raster,
            cache_hint,
        );

        MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::I8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        TestDefault::test_default(),
                        GridBoundingBox2D::new([-3, 0], [-1, 1]).unwrap(),
                    ),
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed()
    }

    #[tokio::test]
    async fn it_attaches_cache_hint_1() {
        let no_data_value = 0;
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            tile_size_in_pixels,
        };

        let ectx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let cache_hint = CacheHint::seconds(1234);
        let raster_a = make_raster_with_cache_hint(Some(no_data_value), cache_hint);

        let o = Expression {
            params: ExpressionParams {
                expression: "min(A * pi(), 10)".to_string(),
                output_type: RasterDataType::I8,
                output_band: None,
                map_no_data: false,
            },
            sources: SingleRasterSource {
                raster: RasterStacker {
                    params: RasterStackerParams {
                        rename_bands: RenameBands::Default,
                    },
                    sources: MultipleRasterSources {
                        rasters: vec![raster_a],
                    },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &ectx)
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = ectx.mock_query_context(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle::new(
                    GridBoundingBox2D::new([-3, 0], [-1, 1]).unwrap(),
                    Default::default(),
                    BandSelection::first(),
                ),
                &ctx,
            )
            .await
            .unwrap();

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;

        assert_eq!(result.len(), 1);

        assert!(
            result[0].as_ref().unwrap().cache_hint.total_ttl_seconds() > CacheTtlSeconds::new(0)
                && result[0].as_ref().unwrap().cache_hint.total_ttl_seconds()
                    <= cache_hint.total_ttl_seconds()
        );
    }

    #[tokio::test]
    async fn it_attaches_cache_hint_2() {
        let no_data_value = 0;
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            tile_size_in_pixels,
        };

        let ectx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let cache_hint_a = CacheHint::seconds(1234);
        let raster_a = make_raster_with_cache_hint(Some(no_data_value), cache_hint_a);

        let cache_hint_b = CacheHint::seconds(4567);
        let raster_b = make_raster_with_cache_hint(Some(no_data_value), cache_hint_b);

        let o = Expression {
            params: ExpressionParams {
                expression: "A + B".to_string(),
                output_type: RasterDataType::I8,
                output_band: None,
                map_no_data: false,
            },
            sources: SingleRasterSource {
                raster: RasterStacker {
                    params: RasterStackerParams {
                        rename_bands: RenameBands::Default,
                    },
                    sources: MultipleRasterSources {
                        rasters: vec![raster_a, raster_b],
                    },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &ectx)
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = ectx.mock_query_context(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle::new(
                    GridBoundingBox2D::new([-3, 0], [-1, 1]).unwrap(),
                    Default::default(),
                    BandSelection::first(),
                ),
                &ctx,
            )
            .await
            .unwrap();

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0].as_ref().unwrap().cache_hint.expires(),
            cache_hint_a.merged(&cache_hint_b).expires()
        );
    }

    #[tokio::test]
    async fn it_attaches_cache_hint_3() {
        let no_data_value = 0;
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            tile_size_in_pixels,
        };

        let ectx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let cache_hint_a = CacheHint::seconds(1234);
        let raster_a = make_raster_with_cache_hint(Some(no_data_value), cache_hint_a);

        let cache_hint_b = CacheHint::seconds(4567);
        let raster_b = make_raster_with_cache_hint(Some(no_data_value), cache_hint_b);

        let cache_hint_c = CacheHint::seconds(7891);
        let raster_c = make_raster_with_cache_hint(Some(no_data_value), cache_hint_c);

        let o = Expression {
            params: ExpressionParams {
                expression: "A + B".to_string(),
                output_type: RasterDataType::I8,
                output_band: None,
                map_no_data: false,
            },
            sources: SingleRasterSource {
                raster: RasterStacker {
                    params: RasterStackerParams {
                        rename_bands: RenameBands::Default,
                    },
                    sources: MultipleRasterSources {
                        rasters: vec![raster_a, raster_b, raster_c],
                    },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &ectx)
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = ectx.mock_query_context(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle::new(
                    GridBoundingBox2D::new([-3, 0], [-1, 1]).unwrap(),
                    Default::default(),
                    BandSelection::first(),
                ),
                &ctx,
            )
            .await
            .unwrap();

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0].as_ref().unwrap().cache_hint.expires(),
            cache_hint_a
                .merged(&cache_hint_b)
                .merged(&cache_hint_c)
                .expires()
        );
    }
}
