use self::{codegen::ExpressionAst, compiled::LinkedExpression, parser::ExpressionParser};
use crate::{
    engine::{
        CreateSpan, ExecutionContext, InitializedRasterOperator, Operator, OperatorData,
        OperatorName, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
        TypedRasterQueryProcessor,
    },
    processing::expression::{codegen::Parameter, query_processor::ExpressionQueryProcessor},
    util::Result,
};
use async_trait::async_trait;
use futures::try_join;
use geoengine_datatypes::{
    dataset::DataId,
    primitives::{partitions_extent, time_interval_extent, Measurement, SpatialResolution},
    raster::RasterDataType,
};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use tracing::{span, Level};

pub use self::error::ExpressionError;

mod codegen;
mod compiled;
mod error;
mod functions;
mod parser;
mod query_processor;

/// Parameters for the `Expression` operator.
/// * The `expression` must only contain simple arithmetic
///     calculations.
/// * `output_type` is the data type of the produced raster tiles.
/// * `output_no_data_value` is the no data value of the output raster
/// * `output_measurement` is the measurement description of the output
///
/// # Warning // TODO
/// The operator *currently* only temporally aligns the inputs when there are exactly two sources
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExpressionParams {
    pub expression: String,
    pub output_type: RasterDataType,
    pub output_measurement: Option<Measurement>,
    pub map_no_data: bool,
}

// TODO: rename to `Expression`
/// The `Expression` operator calculates an expression for all pixels of the input rasters and
/// produces raster tiles of a given output type
pub type Expression = Operator<ExpressionParams, ExpressionSources>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::unsafe_derive_deserialize)] // TODO: remove if this warning is a glitch
pub struct ExpressionSources {
    a: Box<dyn RasterOperator>,
    b: Option<Box<dyn RasterOperator>>,
    c: Option<Box<dyn RasterOperator>>,
    d: Option<Box<dyn RasterOperator>>,
    e: Option<Box<dyn RasterOperator>>,
    f: Option<Box<dyn RasterOperator>>,
    g: Option<Box<dyn RasterOperator>>,
    h: Option<Box<dyn RasterOperator>>,
}

impl OperatorData for ExpressionSources {
    fn data_ids_collect(&self, data_ids: &mut Vec<DataId>) {
        for source in self.iter() {
            source.data_ids_collect(data_ids);
        }
    }
}

impl ExpressionSources {
    pub fn new_a(a: Box<dyn RasterOperator>) -> Self {
        Self {
            a,
            b: None,
            c: None,
            d: None,
            e: None,
            f: None,
            g: None,
            h: None,
        }
    }

    pub fn new_a_b(a: Box<dyn RasterOperator>, b: Box<dyn RasterOperator>) -> Self {
        Self {
            a,
            b: Some(b),
            c: None,
            d: None,
            e: None,
            f: None,
            g: None,
            h: None,
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
            d: None,
            e: None,
            f: None,
            g: None,
            h: None,
        }
    }

    fn number_of_sources(&self) -> usize {
        self.iter().count()
    }

    #[allow(clippy::many_single_char_names)]
    async fn initialize(
        self,
        context: &dyn ExecutionContext,
    ) -> Result<ExpressionInitializedSources> {
        if self.iter().count() != self.iter_consecutive().count() {
            return Err(ExpressionError::SourcesMustBeConsecutive.into());
        }

        let (a, b, c, d, e, f, g, h) = try_join!(
            self.a.initialize(context),
            Self::initialize_source(self.b, context),
            Self::initialize_source(self.c, context),
            Self::initialize_source(self.d, context),
            Self::initialize_source(self.e, context),
            Self::initialize_source(self.f, context),
            Self::initialize_source(self.g, context),
            Self::initialize_source(self.h, context),
        )?;

        Ok(ExpressionInitializedSources {
            a,
            b,
            c,
            d,
            e,
            f,
            g,
            h,
        })
    }

    async fn initialize_source(
        source: Option<Box<dyn RasterOperator>>,
        context: &dyn ExecutionContext,
    ) -> Result<Option<Box<dyn InitializedRasterOperator>>> {
        if let Some(source) = source {
            Ok(Some(source.initialize(context).await?))
        } else {
            Ok(None)
        }
    }

    /// Returns all non-empty sources
    #[allow(clippy::borrowed_box)]
    fn iter(
        &self,
    ) -> std::iter::Flatten<std::array::IntoIter<Option<&Box<dyn RasterOperator>>, 8>> {
        [
            Some(&self.a),
            self.b.as_ref(),
            self.c.as_ref(),
            self.d.as_ref(),
            self.e.as_ref(),
            self.f.as_ref(),
            self.g.as_ref(),
            self.h.as_ref(),
        ]
        .into_iter()
        .flatten()
    }

    /// Returns all sources until the first one is empty
    fn iter_consecutive(&self) -> impl Iterator<Item = &Box<dyn RasterOperator>> {
        [
            Some(&self.a),
            self.b.as_ref(),
            self.c.as_ref(),
            self.d.as_ref(),
            self.e.as_ref(),
            self.f.as_ref(),
            self.g.as_ref(),
            self.h.as_ref(),
        ]
        .into_iter()
        .map_while(std::convert::identity)
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
impl RasterOperator for Expression {
    async fn _initialize(
        self: Box<Self>,
        context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        // TODO: handle more then 2 inputs, i.e. 1-8
        ensure!(
            (1..=8).contains(&self.sources.number_of_sources()),
            crate::error::InvalidNumberOfRasterInputs {
                expected: 1..9,
                found: self.sources.number_of_sources()
            }
        );

        // we refer to rasters by A, B, C, …
        let parameters = (0..self.sources.number_of_sources())
            .map(|i| {
                let parameter = index_to_parameter(i);
                Parameter::Number(parameter.into())
            })
            .collect::<Vec<_>>();

        let expression = ExpressionParser::new(&parameters)?.parse(
            "expression", // TODO: generate and store a unique name
            &self.params.expression,
        )?;

        let sources = self.sources.initialize(context).await?;

        let spatial_reference = sources.a.result_descriptor().spatial_reference;

        let in_descriptors = sources
            .iter()
            .map(InitializedRasterOperator::result_descriptor)
            .collect::<Vec<_>>();

        for other_spatial_reference in in_descriptors.iter().skip(1).map(|rd| rd.spatial_reference)
        {
            ensure!(
                spatial_reference == other_spatial_reference,
                crate::error::InvalidSpatialReference {
                    expected: spatial_reference,
                    found: other_spatial_reference,
                }
            );
        }

        let time = time_interval_extent(in_descriptors.iter().map(|d| d.time));
        let bbox = partitions_extent(in_descriptors.iter().map(|d| d.bbox));

        let resolution = in_descriptors
            .iter()
            .map(|d| d.resolution)
            .reduce(|a, b| match (a, b) {
                (Some(a), Some(b)) => {
                    Some(SpatialResolution::new_unchecked(a.x.min(b.x), a.y.min(b.y)))
                }
                _ => None,
            })
            .flatten();

        let result_descriptor = RasterResultDescriptor {
            data_type: self.params.output_type,
            spatial_reference,
            measurement: self
                .params
                .output_measurement
                .as_ref()
                .map_or(Measurement::Unitless, Measurement::clone),
            time,
            bbox,
            resolution,
        };

        let initialized_operator = InitializedExpression {
            result_descriptor,
            sources,
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
    result_descriptor: RasterResultDescriptor,
    sources: ExpressionInitializedSources,
    expression: ExpressionAst,
    map_no_data: bool,
}

pub struct ExpressionInitializedSources {
    a: Box<dyn InitializedRasterOperator>,
    b: Option<Box<dyn InitializedRasterOperator>>,
    c: Option<Box<dyn InitializedRasterOperator>>,
    d: Option<Box<dyn InitializedRasterOperator>>,
    e: Option<Box<dyn InitializedRasterOperator>>,
    f: Option<Box<dyn InitializedRasterOperator>>,
    g: Option<Box<dyn InitializedRasterOperator>>,
    h: Option<Box<dyn InitializedRasterOperator>>,
}

impl ExpressionInitializedSources {
    fn iter(&self) -> impl Iterator<Item = &Box<dyn InitializedRasterOperator>> {
        [
            Some(&self.a),
            self.b.as_ref(),
            self.c.as_ref(),
            self.d.as_ref(),
            self.e.as_ref(),
            self.f.as_ref(),
            self.g.as_ref(),
            self.h.as_ref(),
        ]
        .into_iter()
        .flatten()
    }
}

#[allow(clippy::many_single_char_names, clippy::too_many_lines)]
impl InitializedRasterOperator for InitializedExpression {
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let output_type = self.result_descriptor().data_type;

        let expression = LinkedExpression::new(&self.expression)?;

        let query_processors: Vec<TypedRasterQueryProcessor> = self
            .sources
            .iter()
            .map(InitializedRasterOperator::query_processor)
            .collect::<Result<_>>()?;

        Ok(match query_processors.len() {
            1 => {
                let [a] = <[_; 1]>::try_from(query_processors).expect("len previously checked");
                let query_processor = a.into_f64();
                call_generic_raster_processor!(
                    output_type,
                    ExpressionQueryProcessor::new(expression, query_processor, self.map_no_data)
                        .boxed()
                )

                // TODO: We could save prior conversions by monomophizing the differnt expressions
                //       However, this would lead to lots of compile symbols and to different results than using the
                //       variants with more than one raster.
                //
                // call_on_generic_raster_processor!(a, p_a => {
                //     call_generic_raster_processor!(
                //         output_type,
                //         ExpressionQueryProcessor::new(
                //             expression,
                //             p_a,
                //             output_no_data_value.as_(),
                //             self.map_no_data,
                //         ).boxed()
                //     )
                // })
            }
            2 => {
                let [a, b] = <[_; 2]>::try_from(query_processors).expect("len previously checked");
                let query_processors = (a.into_f64(), b.into_f64());
                call_generic_raster_processor!(
                    output_type,
                    ExpressionQueryProcessor::new(expression, query_processors, self.map_no_data)
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
                let query_processors = [a.into_f64(), b.into_f64(), c.into_f64()];
                call_generic_raster_processor!(
                    output_type,
                    ExpressionQueryProcessor::new(expression, query_processors, self.map_no_data)
                        .boxed()
                )
            }
            4 => {
                let [a, b, c, d] =
                    <[_; 4]>::try_from(query_processors).expect("len previously checked");
                let query_processors = [a.into_f64(), b.into_f64(), c.into_f64(), d.into_f64()];
                call_generic_raster_processor!(
                    output_type,
                    ExpressionQueryProcessor::new(expression, query_processors, self.map_no_data)
                        .boxed()
                )
            }
            5 => {
                let [a, b, c, d, e] =
                    <[_; 5]>::try_from(query_processors).expect("len previously checked");
                let query_processors = [
                    a.into_f64(),
                    b.into_f64(),
                    c.into_f64(),
                    d.into_f64(),
                    e.into_f64(),
                ];
                call_generic_raster_processor!(
                    output_type,
                    ExpressionQueryProcessor::new(expression, query_processors, self.map_no_data)
                        .boxed()
                )
            }
            6 => {
                let [a, b, c, d, e, f] =
                    <[_; 6]>::try_from(query_processors).expect("len previously checked");
                let query_processors = [
                    a.into_f64(),
                    b.into_f64(),
                    c.into_f64(),
                    d.into_f64(),
                    e.into_f64(),
                    f.into_f64(),
                ];
                call_generic_raster_processor!(
                    output_type,
                    ExpressionQueryProcessor::new(expression, query_processors, self.map_no_data)
                        .boxed()
                )
            }

            7 => {
                let [a, b, c, d, e, f, g] =
                    <[_; 7]>::try_from(query_processors).expect("len previously checked");
                let query_processors = [
                    a.into_f64(),
                    b.into_f64(),
                    c.into_f64(),
                    d.into_f64(),
                    e.into_f64(),
                    f.into_f64(),
                    g.into_f64(),
                ];
                call_generic_raster_processor!(
                    output_type,
                    ExpressionQueryProcessor::new(expression, query_processors, self.map_no_data)
                        .boxed()
                )
            }
            8 => {
                let [a, b, c, d, e, f, g, h] =
                    <[_; 8]>::try_from(query_processors).expect("len previously checked");
                let query_processors = [
                    a.into_f64(),
                    b.into_f64(),
                    c.into_f64(),
                    d.into_f64(),
                    e.into_f64(),
                    f.into_f64(),
                    g.into_f64(),
                    h.into_f64(),
                ];
                call_generic_raster_processor!(
                    output_type,
                    ExpressionQueryProcessor::new(expression, query_processors, self.map_no_data)
                        .boxed()
                )
            }
            _ => return Err(crate::error::Error::InvalidNumberOfExpressionInputs),
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
    use geoengine_datatypes::raster::{
        Grid2D, GridOrEmpty, MapElements, MaskedGrid2D, RasterTile2D, TileInformation,
        TilingSpecification,
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
                output_measurement: None,
                map_no_data: false,
            }
        );
    }

    #[test]
    fn serialize_params() {
        let s =
            r#"{"expression":"1*A","outputType":"F64","outputMeasurement":null,"mapNoData":false}"#;

        assert_eq!(
            s,
            serde_json::to_string(&ExpressionParams {
                expression: "1*A".to_owned(),
                output_type: RasterDataType::F64,
                output_measurement: None,
                map_no_data: false,
            })
            .unwrap()
        );
    }

    #[test]
    fn serialize_params_no_data() {
        let s =
            r#"{"expression":"1*A","outputType":"F64","outputMeasurement":null,"mapNoData":false}"#;

        assert_eq!(
            s,
            serde_json::to_string(&ExpressionParams {
                expression: "1*A".to_owned(),
                output_type: RasterDataType::F64,
                output_measurement: None,
                map_no_data: false,
            })
            .unwrap()
        );
    }

    #[tokio::test]
    async fn basic_unary() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let raster_a = make_raster(Some(3));

        let o = Expression {
            params: ExpressionParams {
                expression: "2 * A".to_string(),
                output_type: RasterDataType::I8,
                output_measurement: Some(Measurement::Unitless),
                map_no_data: false,
            },
            sources: ExpressionSources {
                a: raster_a,
                b: None,
                c: None,
                d: None,
                e: None,
                f: None,
                g: None,
                h: None,
            },
        }
        .boxed()
        .initialize(&ctx)
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = MockQueryContext::new(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 3.).into(),
                        (2., 0.).into(),
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
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let raster_a = make_raster(Some(3));

        let o = Expression {
            params: ExpressionParams {
                expression: "2 * A".to_string(),
                output_type: RasterDataType::I8,
                output_measurement: Some(Measurement::Unitless),
                map_no_data: true,
            },
            sources: ExpressionSources {
                a: raster_a,
                b: None,
                c: None,
                d: None,
                e: None,
                f: None,
                g: None,
                h: None,
            },
        }
        .boxed()
        .initialize(&ctx)
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = MockQueryContext::new(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 3.).into(),
                        (2., 0.).into(),
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
            GridOrEmpty::from(
                MaskedGrid2D::new(
                    Grid2D::new([3, 2].into(), vec![2, 4, 0, 8, 10, 12],).unwrap(), // pixels with no data are turned to Default::default wich is 0. And 0 is the out_no_data value.
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
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let raster_a = make_raster(None);
        let raster_b = make_raster(None);

        let o = Expression {
            params: ExpressionParams {
                expression: "A+B".to_string(),
                output_type: RasterDataType::I8,
                output_measurement: Some(Measurement::Unitless),
                map_no_data: false,
            },
            sources: ExpressionSources {
                a: raster_a,
                b: Some(raster_b),
                c: None,
                d: None,
                e: None,
                f: None,
                g: None,
                h: None,
            },
        }
        .boxed()
        .initialize(&ctx)
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = MockQueryContext::new(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 3.).into(),
                        (2., 0.).into(),
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
            Grid2D::new([3, 2].into(), vec![2, 4, 6, 8, 10, 12],)
                .unwrap()
                .into()
        );
    }

    #[tokio::test]
    async fn basic_coalesce() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
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
                output_measurement: Some(Measurement::Unitless),
                map_no_data: true,
            },
            sources: ExpressionSources {
                a: raster_a,
                b: Some(raster_b),
                c: None,
                d: None,
                e: None,
                f: None,
                g: None,
                h: None,
            },
        }
        .boxed()
        .initialize(&ctx)
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = MockQueryContext::new(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 3.).into(),
                        (2., 0.).into(),
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
            origin_coordinate: [0.0, 0.0].into(),
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
                output_measurement: Some(Measurement::Unitless),
                map_no_data: false,
            },
            sources: ExpressionSources {
                a: raster_a,
                b: Some(raster_b),
                c: Some(raster_c),
                d: None,
                e: None,
                f: None,
                g: None,
                h: None,
            },
        }
        .boxed()
        .initialize(&ctx)
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = MockQueryContext::new(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 3.).into(),
                        (2., 0.).into(),
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
            origin_coordinate: [0.0, 0.0].into(),
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
                output_measurement: Some(Measurement::Unitless),
                map_no_data: false,
            },
            sources: ExpressionSources {
                a: raster_a,
                b: Some(raster_b),
                c: Some(raster_c),
                d: Some(raster_d),
                e: Some(raster_e),
                f: Some(raster_f),
                g: Some(raster_g),
                h: Some(raster_h),
            },
        }
        .boxed()
        .initialize(&ctx)
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = MockQueryContext::new(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 3.).into(),
                        (2., 0.).into(),
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
            Grid2D::new([3, 2].into(), vec![8, 16, 24, 32, 40, 48],)
                .unwrap()
                .into()
        );
    }

    #[tokio::test]
    async fn test_functions() {
        let no_data_value = 0;
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let ectx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let raster_a = make_raster(Some(no_data_value));

        let o = Expression {
            params: ExpressionParams {
                expression: "min(A * pi(), 10)".to_string(),
                output_type: RasterDataType::I8,
                output_measurement: Some(Measurement::Unitless),
                map_no_data: false,
            },
            sources: ExpressionSources {
                a: raster_a,
                b: None,
                c: None,
                d: None,
                e: None,
                f: None,
                g: None,
                h: None,
            },
        }
        .boxed()
        .initialize(&ectx)
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = MockQueryContext::new(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 3.).into(),
                        (2., 0.).into(),
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
            Grid2D::new([3, 2].into(), vec![3, 6, 9, 10, 10, 10],)
                .unwrap()
                .into()
        );
    }

    fn make_raster(no_data_value: Option<i8>) -> Box<dyn RasterOperator> {
        let raster = Grid2D::<i8>::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap();

        let real_raster = if let Some(no_data_value) = no_data_value {
            MaskedGrid2D::from(raster)
                .map_elements(|e| {
                    if let Some(v) = e {
                        if v == no_data_value {
                            None
                        } else {
                            Some(v)
                        }
                    } else {
                        None
                    }
                })
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
            real_raster,
        );

        MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::I8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
            },
        }
        .boxed()
    }
}
