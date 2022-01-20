use self::{codegen::ExpressionAst, compiled::LinkedExpression, parser::ExpressionParser};
use crate::{
    engine::{
        ExecutionContext, InitializedRasterOperator, Operator, OperatorDatasets, RasterOperator,
        RasterQueryProcessor, RasterResultDescriptor, TypedRasterQueryProcessor,
    },
    processing::new_expression::{codegen::Parameter, query_processor::ExpressionQueryProcessor},
    util::{input::float_with_nan, Result},
};
use async_trait::async_trait;
use geoengine_datatypes::{dataset::DatasetId, primitives::Measurement, raster::RasterDataType};
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};
use snafu::ensure;

pub use self::error::ExpressionError;

mod codegen;
mod compiled;
mod error;
mod parser;
mod query_processor;

/// Parameters for the `Expression` operator.
/// * The `expression` must only contain simple arithmetic
///     calculations.
/// * `output_type` is the data type of the produced raster tiles.
/// * `output_no_data_value` is the no data value of the output raster
/// * `output_measurement` is the measurement description of the output
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExpressionParams {
    pub expression: String,
    pub output_type: RasterDataType,
    #[serde(with = "float_with_nan")]
    pub output_no_data_value: f64, // TODO: check value is valid for given output type during deserialization
    pub output_measurement: Option<Measurement>,
    pub map_no_data: bool,
}

// TODO: rename to `Expression`
/// The `Expression` operator calculates an expression for all pixels of the input rasters and
/// produces raster tiles of a given output type
pub type NewExpression = Operator<ExpressionParams, ExpressionSources>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpressionSources {
    a: Box<dyn RasterOperator>,
    b: Option<Box<dyn RasterOperator>>,
    c: Option<Box<dyn RasterOperator>>,
}

impl OperatorDatasets for ExpressionSources {
    fn datasets_collect(&self, datasets: &mut Vec<DatasetId>) {
        self.a.datasets_collect(datasets);

        if let Some(ref b) = self.b {
            b.datasets_collect(datasets);
        }

        if let Some(ref c) = self.c {
            c.datasets_collect(datasets);
        }
    }
}

impl ExpressionSources {
    pub fn new_a(a: Box<dyn RasterOperator>) -> Self {
        Self {
            a,
            b: None,
            c: None,
        }
    }

    pub fn new_a_b(a: Box<dyn RasterOperator>, b: Box<dyn RasterOperator>) -> Self {
        Self {
            a,
            b: Some(b),
            c: None,
        }
    }

    pub fn new_a_b_c(
        a: Box<dyn RasterOperator>,
        b: Box<dyn RasterOperator>,
        c: Box<dyn RasterOperator>,
    ) -> Self {
        Self {
            a,
            b: Some(b),
            c: Some(c),
        }
    }

    fn number_of_sources(&self) -> usize {
        let a: usize = 1;
        let b: usize = self.b.is_some().into();
        let c: usize = self.c.is_some().into();

        a + b + c
    }

    async fn initialize(
        self,
        context: &dyn ExecutionContext,
    ) -> Result<ExpressionInitializedSources> {
        let b = if let Some(b) = self.b {
            Some(b.initialize(context).await)
        } else {
            None
        };

        let c = if let Some(c) = self.c {
            Some(c.initialize(context).await)
        } else {
            None
        };

        Ok(ExpressionInitializedSources {
            a: self.a.initialize(context).await?,
            b: b.transpose()?,
            c: c.transpose()?,
        })
    }
}

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
impl RasterOperator for NewExpression {
    async fn initialize(
        self: Box<Self>,
        context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        // TODO: handle more then 2 inputs, i.e. 1-8
        ensure!(
            (1..=3).contains(&self.sources.number_of_sources()),
            crate::error::InvalidNumberOfRasterInputs {
                expected: 1..4,
                found: self.sources.number_of_sources()
            }
        );

        // we refer to rasters by A, B, C, …
        let parameters = (0..self.sources.number_of_sources())
            .flat_map(|i| {
                let parameter = index_to_parameter(i);
                let boolean_parameter = format!("{}_is_nodata", &parameter);
                [
                    Parameter::Number(parameter.into()),
                    Parameter::Boolean(boolean_parameter.into()),
                ]
            })
            .chain([Parameter::Number("out_nodata".into())])
            .collect::<Vec<_>>();

        let expression = ExpressionParser::new(&parameters)?.parse(
            "expression", // TODO: generate and store a unique name
            &self.params.expression,
        )?;

        ensure!(
            self.params
                .output_type
                .is_valid(self.params.output_no_data_value),
            crate::error::InvalidNoDataValueValueForOutputDataType
        );

        let sources = self.sources.initialize(context).await?;

        let spatial_reference = sources.a.result_descriptor().spatial_reference;

        for other_spatial_reference in sources
            .iter()
            .skip(1)
            .map(|source| source.result_descriptor().spatial_reference)
        {
            ensure!(
                spatial_reference == other_spatial_reference,
                crate::error::InvalidSpatialReference {
                    expected: spatial_reference,
                    found: other_spatial_reference,
                }
            );
        }

        let result_descriptor = RasterResultDescriptor {
            data_type: self.params.output_type,
            spatial_reference,
            measurement: self
                .params
                .output_measurement
                .as_ref()
                .map_or(Measurement::Unitless, Measurement::clone),
            no_data_value: Some(self.params.output_no_data_value), // TODO: is it possible to have none?
        };

        let initialized_operator = InitializedExpression {
            result_descriptor,
            sources,
            expression,
            map_no_data: self.params.map_no_data,
        };

        Ok(initialized_operator.boxed())
    }
}

pub struct InitializedExpression {
    result_descriptor: RasterResultDescriptor,
    sources: ExpressionInitializedSources,
    expression: ExpressionAst,
    map_no_data: bool,
}

pub struct ExpressionInitializedSources {
    a: Box<dyn InitializedRasterOperator>,
    b: Option<Box<dyn InitializedRasterOperator>>,
    c: Option<Box<dyn InitializedRasterOperator>>,
}

impl ExpressionInitializedSources {
    fn iter(&self) -> impl Iterator<Item = &Box<dyn InitializedRasterOperator>> {
        let mut sources = vec![&self.a];

        if let Some(o) = self.b.as_ref() {
            sources.push(o);
        }

        if let Some(o) = self.c.as_ref() {
            sources.push(o);
        }

        sources.into_iter()
    }
}

impl InitializedRasterOperator for InitializedExpression {
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let output_type = self.result_descriptor().data_type;
        // TODO: allow processing expression without NO DATA
        let output_no_data_value = self.result_descriptor().no_data_value.unwrap_or_default();

        let expression = LinkedExpression::new(&self.expression)?;

        let query_processors: Vec<TypedRasterQueryProcessor> = self
            .sources
            .iter()
            .map(InitializedRasterOperator::query_processor)
            .collect::<Result<_>>()?;

        Ok(match query_processors.len() {
            1 => {
                let [a] = <[_; 1]>::try_from(query_processors).expect("len previously checked");
                call_on_generic_raster_processor!(a, p_a => {
                    call_generic_raster_processor!(
                        output_type,
                        ExpressionQueryProcessor::new(
                            expression,
                            p_a,
                            output_no_data_value.as_(),
                            self.map_no_data,
                        ).boxed()
                    )
                })
            }
            2 => {
                let [a, b] = <[_; 2]>::try_from(query_processors).expect("len previously checked");
                let query_processors = (a.into_f64(), b.into_f64());
                call_generic_raster_processor!(
                    output_type,
                    ExpressionQueryProcessor::new(
                        expression,
                        query_processors,
                        output_no_data_value.as_(),
                        self.map_no_data,
                    )
                    .boxed()
                )

                // TODO: We could save prior conversions by monomophizing the differnt expressions
                //       However, this would lead to lots of compile symbols, e.g., 10x10x10 for this case
                //
                // call_on_bi_generic_raster_processor!(a, b, (p_a, p_b) => {
                //     call_generic_raster_processor!(
                //         output_type,
                //         ExpressionQueryProcessor::new(
                //             expression,
                //             (p_a, p_b),
                //             output_no_data_value.as_(),
                //             self.map_no_data,
                //         ).boxed()
                //     )
                // })
            }
            3 => {
                let [a, b, c] =
                    <[_; 3]>::try_from(query_processors).expect("len previously checked");
                let query_processors = (a.into_f64(), b.into_f64(), c.into_f64());
                call_generic_raster_processor!(
                    output_type,
                    ExpressionQueryProcessor::new(
                        expression,
                        query_processors,
                        output_no_data_value.as_(),
                        self.map_no_data,
                    )
                    .boxed()
                )
            }
            _ => return Err(crate::error::Error::InvalidNumberOfExpressionInputs), // TODO: handle more than three inputs
        })
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext, QueryProcessor};
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use futures::StreamExt;
    use geoengine_datatypes::primitives::{
        Measurement, RasterQueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::raster::{Grid2D, RasterTile2D, TileInformation};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;

    #[test]
    fn deserialize_params() {
        let s = r#"{"expression":"1*A","outputType":"F64","outputNoDataValue":0.0,"outputMeasurement":null,"mapNoData":false}"#;

        assert_eq!(
            serde_json::from_str::<ExpressionParams>(s).unwrap(),
            ExpressionParams {
                expression: "1*A".to_owned(),
                output_type: RasterDataType::F64,
                output_no_data_value: 0.0,
                output_measurement: None,
                map_no_data: false,
            }
        );
    }

    #[test]
    fn deserialize_params_no_data() {
        let s = r#"{"expression":"1*A","outputType":"F64","outputNoDataValue":"nan","outputMeasurement":null,"mapNoData":false}"#;

        assert!(f64::is_nan(
            serde_json::from_str::<ExpressionParams>(s)
                .unwrap()
                .output_no_data_value
        ),);
    }

    #[test]
    fn deserialize_params_missing_no_data() {
        let s = r#"{"expression":"1*A","outputType":"F64","outputNoDataValue":null,"outputMeasurement":null,"mapNoData":false}"#;

        assert!(serde_json::from_str::<ExpressionParams>(s).is_err());
    }

    #[test]
    fn serialize_params() {
        let s = r#"{"expression":"1*A","outputType":"F64","outputNoDataValue":0.0,"outputMeasurement":null,"mapNoData":false}"#;

        assert_eq!(
            s,
            serde_json::to_string(&ExpressionParams {
                expression: "1*A".to_owned(),
                output_type: RasterDataType::F64,
                output_no_data_value: 0.0,
                output_measurement: None,
                map_no_data: false,
            })
            .unwrap()
        );
    }

    #[test]
    fn serialize_params_no_data() {
        let s = r#"{"expression":"1*A","outputType":"F64","outputNoDataValue":"nan","outputMeasurement":null,"mapNoData":false}"#;

        assert_eq!(
            s,
            serde_json::to_string(&ExpressionParams {
                expression: "1*A".to_owned(),
                output_type: RasterDataType::F64,
                output_no_data_value: f64::NAN,
                output_measurement: None,
                map_no_data: false,
            })
            .unwrap()
        );
    }

    #[tokio::test]
    async fn basic_unary() {
        let no_data_value = 3;
        let no_data_value_option = Some(no_data_value);

        let raster_a = make_raster(Some(3));

        let o = NewExpression {
            params: ExpressionParams {
                expression: "2 * A".to_string(),
                output_type: RasterDataType::I8,
                output_no_data_value: no_data_value.as_(), //  cast no_data_value to f64
                output_measurement: Some(Measurement::Unitless),
                map_no_data: false,
            },
            sources: ExpressionSources {
                a: raster_a,
                b: None,
                c: None,
            },
        }
        .boxed()
        .initialize(&MockExecutionContext::test_default())
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = MockQueryContext::new(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 4.).into(),
                        (3., 0.).into(),
                    ),
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &ctx,
            )
            .await
            .unwrap();

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0].as_ref().unwrap().grid_array,
            Grid2D::new(
                [3, 2].into(),
                vec![2, 4, 3, 8, 10, 12],
                no_data_value_option,
            )
            .unwrap()
            .into()
        );
    }

    #[tokio::test]
    async fn unary_map_no_data() {
        let no_data_value = 3;
        let no_data_value_option = Some(no_data_value);

        let raster_a = make_raster(Some(3));

        let o = NewExpression {
            params: ExpressionParams {
                expression: "2 * A".to_string(),
                output_type: RasterDataType::I8,
                output_no_data_value: no_data_value.as_(), //  cast no_data_value to f64
                output_measurement: Some(Measurement::Unitless),
                map_no_data: true,
            },
            sources: ExpressionSources {
                a: raster_a,
                b: None,
                c: None,
            },
        }
        .boxed()
        .initialize(&MockExecutionContext::test_default())
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = MockQueryContext::new(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 4.).into(),
                        (3., 0.).into(),
                    ),
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &ctx,
            )
            .await
            .unwrap();

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0].as_ref().unwrap().grid_array,
            Grid2D::new(
                [3, 2].into(),
                vec![2, 4, 6, 8, 10, 12],
                no_data_value_option,
            )
            .unwrap()
            .into()
        );
    }

    #[tokio::test]
    async fn basic_binary() {
        let no_data_value = 42;
        let no_data_value_option = Some(no_data_value);

        let raster_a = make_raster(None);
        let raster_b = make_raster(None);

        let o = NewExpression {
            params: ExpressionParams {
                expression: "A+B".to_string(),
                output_type: RasterDataType::I8,
                output_no_data_value: no_data_value.as_(), //  cast no_data_valuee to f64
                output_measurement: Some(Measurement::Unitless),
                map_no_data: false,
            },
            sources: ExpressionSources {
                a: raster_a,
                b: Some(raster_b),
                c: None,
            },
        }
        .boxed()
        .initialize(&MockExecutionContext::test_default())
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = MockQueryContext::new(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 4.).into(),
                        (3., 0.).into(),
                    ),
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &ctx,
            )
            .await
            .unwrap();

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0].as_ref().unwrap().grid_array,
            Grid2D::new(
                [3, 2].into(),
                vec![2, 4, 6, 8, 10, 12],
                no_data_value_option,
            )
            .unwrap()
            .into()
        );
    }

    #[tokio::test]
    async fn basic_coalesce() {
        let no_data_value = 42;
        let no_data_value_option = Some(no_data_value);

        let raster_a = make_raster(Some(3));
        let raster_b = make_raster(None);

        let o = NewExpression {
            params: ExpressionParams {
                expression: "if A IS NODATA {
                    B * 2
                } else if A == 6 {
                    out_nodata
                } else {
                    A
                }"
                .to_string(),
                output_type: RasterDataType::I8,
                output_no_data_value: no_data_value.as_(), //  cast no_data_valuee to f64
                output_measurement: Some(Measurement::Unitless),
                map_no_data: true,
            },
            sources: ExpressionSources {
                a: raster_a,
                b: Some(raster_b),
                c: None,
            },
        }
        .boxed()
        .initialize(&MockExecutionContext::test_default())
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = MockQueryContext::new(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 4.).into(),
                        (3., 0.).into(),
                    ),
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &ctx,
            )
            .await
            .unwrap();

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0].as_ref().unwrap().grid_array,
            Grid2D::new([3, 2].into(), vec![1, 2, 6, 4, 5, 42], no_data_value_option,)
                .unwrap()
                .into()
        );
    }

    #[tokio::test]
    async fn basic_ternary() {
        let no_data_value = 3;
        let no_data_value_option = Some(no_data_value);

        let raster_a = make_raster(no_data_value_option);
        let raster_b = make_raster(no_data_value_option);
        let raster_c = make_raster(no_data_value_option);

        let o = NewExpression {
            params: ExpressionParams {
                expression: "A+B+C".to_string(),
                output_type: RasterDataType::I8,
                output_no_data_value: no_data_value.as_(), //  cast no_data_valuee to f64
                output_measurement: Some(Measurement::Unitless),
                map_no_data: false,
            },
            sources: ExpressionSources {
                a: raster_a,
                b: Some(raster_b),
                c: Some(raster_c),
            },
        }
        .boxed()
        .initialize(&MockExecutionContext::test_default())
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = MockQueryContext::new(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 4.).into(),
                        (3., 0.).into(),
                    ),
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &ctx,
            )
            .await
            .unwrap();

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0].as_ref().unwrap().grid_array,
            Grid2D::new(
                [3, 2].into(),
                vec![3, 6, 3, 12, 15, 18],
                no_data_value_option,
            )
            .unwrap()
            .into()
        );
    }

    fn make_raster(no_data_value: Option<i8>) -> Box<dyn RasterOperator> {
        let raster = Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], no_data_value).unwrap();

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_tile_position: [-1, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
                global_geo_transform: TestDefault::test_default(),
            },
            raster.into(),
        );

        MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::I8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(f64::from),
                },
            },
        }
        .boxed()
    }
}
