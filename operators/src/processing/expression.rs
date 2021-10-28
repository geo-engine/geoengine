use crate::engine::{
    InitializedRasterOperator, Operator, OperatorDatasets, QueryContext, QueryProcessor,
    RasterOperator, RasterQueryProcessor, RasterQueryRectangle, RasterResultDescriptor,
    TypedRasterQueryProcessor,
};
use crate::error::Error;
use crate::util::input::float_with_nan;
use crate::util::Result;
use crate::{call_bi_generic_processor, call_generic_raster_processor};
use crate::{
    engine::ExecutionContext,
    opencl::{ClProgram, CompiledClProgram, IterationType, RasterArgument},
};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::dataset::DatasetId;
use geoengine_datatypes::primitives::{Measurement, SpatialPartition2D};
use geoengine_datatypes::raster::{
    EmptyGrid, Grid2D, GridShapeAccess, Pixel, RasterDataType, RasterTile2D,
};
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::marker::PhantomData;

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
}

// TODO: custom type or simple string?
#[derive(Debug, Clone)]
struct SafeExpression {
    expression: String,
}

impl SafeExpression {
    // TODO: also check this when creating original operator parameters
    fn is_allowed_expression(expression: &str) -> bool {
        // TODO: perform actual syntax checking
        let disallowed_chars: HashSet<_> = ";[]{}#%\"\'\\".chars().collect();
        let disallowed_strs: HashSet<String> =
            vec!["/*".into(), "//".into(), "*/".into(), "return".into()]
                .into_iter()
                .collect();

        expression.chars().all(|c| !disallowed_chars.contains(&c))
            && disallowed_strs.iter().all(|s| !expression.contains(s))
    }
}

impl TryFrom<String> for SafeExpression {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if Self::is_allowed_expression(&value) {
            Ok(Self { expression: value })
        } else {
            Err(Error::InvalidExpression)
        }
    }
}

/// The `Expression` operator calculates an expression for all pixels of the input rasters and
/// produces raster tiles of a given output type
pub type Expression = Operator<ExpressionParams, ExpressionSources>;

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

#[typetag::serde]
#[async_trait]
impl RasterOperator for Expression {
    async fn initialize(
        self: Box<Self>,
        context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        // TODO: handle more then exactly 2 inputs, i.e. 1-8
        ensure!(
            self.sources.number_of_sources() == 2,
            crate::error::InvalidNumberOfRasterInputs {
                expected: 2..3,
                found: self.sources.number_of_sources()
            }
        );

        let expression = SafeExpression::try_from(self.params.expression)?;

        ensure!(
            self.params
                .output_type
                .is_valid(self.params.output_no_data_value),
            crate::error::InvalidNoDataValueValueForOutputDataType
        );

        let sources = self.sources.initialize(context).await?;

        let spatial_reference = sources.a.result_descriptor().spatial_reference;

        for other_spatial_refenence in sources
            .iter()
            .skip(1)
            .map(|source| source.result_descriptor().spatial_reference)
        {
            ensure!(
                spatial_reference == other_spatial_refenence,
                crate::error::InvalidSpatialReference {
                    expected: spatial_reference,
                    found: other_spatial_refenence,
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
        };

        Ok(initialized_operator.boxed())
    }
}

pub struct InitializedExpression {
    result_descriptor: RasterResultDescriptor,
    sources: ExpressionInitializedSources,
    expression: SafeExpression,
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
        // TODO: handle different number of sources

        match &self.sources {
            ExpressionInitializedSources {
                a,
                b: Some(b),
                c: None,
            } => {
                let a = a.query_processor()?;
                let b = b.query_processor()?;

                let expression = self.expression.clone();
                let output_type = self.result_descriptor().data_type;
                // TODO: allow processing expression without NO DATA
                let output_no_data_value =
                    self.result_descriptor().no_data_value.unwrap_or_default();

                call_bi_generic_processor!(a, b, (p_a, p_b) => {
                    let res = call_generic_raster_processor!(
                        output_type,
                        ExpressionQueryProcessor::new(
                            &expression,
                            p_a,
                            p_b,
                            output_no_data_value.as_()
                        ).boxed()
                    );
                    Ok(res)
                })
            }
            _ => Err(crate::error::Error::InvalidNumberOfExpressionInputs), // TODO: handle more than two inputs
        }
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

struct ExpressionQueryProcessor<T1, T2, TO>
where
    T1: Pixel,
    T2: Pixel,
    TO: Pixel,
{
    pub source_a: Box<dyn RasterQueryProcessor<RasterType = T1>>,
    pub source_b: Box<dyn RasterQueryProcessor<RasterType = T2>>,
    pub phantom_data: PhantomData<TO>,
    pub cl_program: CompiledClProgram,
    pub no_data_value: TO,
}

impl<T1, T2, TO> ExpressionQueryProcessor<T1, T2, TO>
where
    T1: Pixel,
    T2: Pixel,
    TO: Pixel,
{
    fn new(
        expression: &SafeExpression,
        source_a: Box<dyn RasterQueryProcessor<RasterType = T1>>,
        source_b: Box<dyn RasterQueryProcessor<RasterType = T2>>,
        no_data_value: TO,
    ) -> Self {
        Self {
            source_a,
            source_b,
            cl_program: Self::create_cl_program(expression),
            phantom_data: PhantomData::default(),
            no_data_value,
        }
    }

    fn create_cl_program(expression: &SafeExpression) -> CompiledClProgram {
        // TODO: generate code for arbitrary amount of inputs
        let source = r#"
__kernel void expressionkernel(
            __global const IN_TYPE0 *in_data0,
            __global const RasterInfo *in_info0,
            __global const IN_TYPE1* in_data1,
            __global const RasterInfo *in_info1,
            __global OUT_TYPE0* out_data,
            __global const RasterInfo *out_info)
{
    uint const gid = get_global_id(0) + get_global_id(1) * in_info0->size[0];
    if (gid >= in_info0->size[0]*in_info0->size[1]*in_info0->size[2])
        return;

    IN_TYPE0 A = in_data0[gid];
    if (ISNODATA0(A, in_info0)) {
        out_data[gid] = out_info->no_data;
        return;
    }

    IN_TYPE0 B = in_data1[gid];
    if (ISNODATA1(B, in_info1)) {
        out_data[gid] = out_info->no_data;
        return;
    }

    OUT_TYPE0 result = %%%EXPRESSION%%%;
	out_data[gid] = result;
}"#
        .replace("%%%EXPRESSION%%%", &expression.expression);

        let mut cl_program = ClProgram::new(IterationType::Raster);
        cl_program.add_input_raster(RasterArgument::new(T1::TYPE));
        cl_program.add_input_raster(RasterArgument::new(T2::TYPE));
        cl_program.add_output_raster(RasterArgument::new(TO::TYPE));

        cl_program.compile(&source, "expressionkernel").unwrap()
    }
}

#[async_trait]
impl<'a, T1, T2, TO> QueryProcessor for ExpressionQueryProcessor<T1, T2, TO>
where
    T1: Pixel,
    T2: Pixel,
    TO: Pixel,
{
    type Output = RasterTile2D<TO>;
    type SpatialBounds = SpatialPartition2D;

    async fn query<'b>(
        &'b self,
        query: RasterQueryRectangle,
        ctx: &'b dyn QueryContext,
    ) -> Result<BoxStream<'b, Result<Self::Output>>> {
        // TODO: validate that tiles actually fit together
        let mut cl_program = self.cl_program.clone();
        Ok(self
            .source_a
            .query(query, ctx)
            .await?
            .zip(self.source_b.query(query, ctx).await?)
            .map(move |(a, b)| match (a, b) {
                (Ok(a), Ok(b)) if a.grid_array.is_empty() && b.grid_array.is_empty() => {
                    Ok(RasterTile2D::new(
                        a.time,
                        a.tile_position,
                        a.global_geo_transform,
                        EmptyGrid::new(a.grid_array.grid_shape(), self.no_data_value).into(),
                    ))
                }

                (Ok(a), Ok(b)) => {
                    let a = a.into_materialized_tile(); // TODO: find cases where we don't need this.
                    let b = b.into_materialized_tile();
                    let mut out = Grid2D::new(
                        a.grid_shape(),
                        vec![TO::zero(); a.grid_array.data.len()], // TODO: correct output size; initialization required?
                        Some(self.no_data_value),                  // TODO
                    )
                    .expect("raster creation must succeed")
                    .into();

                    let a_typed = a.grid_array.into();
                    let b_typed = b.grid_array.into();
                    let mut params = cl_program.runnable();

                    params.set_input_raster(0, &a_typed).unwrap();
                    params.set_input_raster(1, &b_typed).unwrap();
                    params.set_output_raster(0, &mut out).unwrap();
                    cl_program.run(params).unwrap();

                    let raster = Grid2D::<TO>::try_from(out).expect("must be correct");

                    Ok(RasterTile2D::new(
                        a.time,
                        a.tile_position,
                        a.global_geo_transform,
                        raster.into(),
                    ))
                }
                _ => unimplemented!(),
            })
            .boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use geoengine_datatypes::primitives::{
        Measurement, SpatialPartition2D, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::raster::TileInformation;
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;

    #[test]
    fn deserialize_params() {
        let s = r#"{"expression":"1*A","outputType":"F64","outputNoDataValue":0.0,"outputMeasurement":null}"#;

        assert_eq!(
            serde_json::from_str::<ExpressionParams>(s).unwrap(),
            ExpressionParams {
                expression: "1*A".to_owned(),
                output_type: RasterDataType::F64,
                output_no_data_value: 0.0,
                output_measurement: None,
            }
        );
    }

    #[test]
    fn deserialize_params_no_data() {
        let s = r#"{"expression":"1*A","outputType":"F64","outputNoDataValue":"nan","outputMeasurement":null}"#;

        assert!(f64::is_nan(
            serde_json::from_str::<ExpressionParams>(s)
                .unwrap()
                .output_no_data_value
        ),);
    }

    #[test]
    fn deserialize_params_missing_no_data() {
        let s = r#"{"expression":"1*A","outputType":"F64","outputNoDataValue":null,"outputMeasurement":null}"#;

        assert!(serde_json::from_str::<ExpressionParams>(s).is_err());
    }

    #[test]
    fn serialize_params() {
        let s = r#"{"expression":"1*A","outputType":"F64","outputNoDataValue":0.0,"outputMeasurement":null}"#;

        assert_eq!(
            s,
            serde_json::to_string(&ExpressionParams {
                expression: "1*A".to_owned(),
                output_type: RasterDataType::F64,
                output_no_data_value: 0.0,
                output_measurement: None,
            })
            .unwrap()
        );
    }

    #[test]
    fn serialize_params_no_data() {
        let s = r#"{"expression":"1*A","outputType":"F64","outputNoDataValue":"nan","outputMeasurement":null}"#;

        assert_eq!(
            s,
            serde_json::to_string(&ExpressionParams {
                expression: "1*A".to_owned(),
                output_type: RasterDataType::F64,
                output_no_data_value: f64::NAN,
                output_measurement: None,
            })
            .unwrap()
        );
    }

    #[tokio::test]
    async fn basic() {
        let no_data_value = 42;
        let no_data_value_option = Some(no_data_value);

        let raster_a = make_raster();
        let raster_b = make_raster();

        let o = Expression {
            params: ExpressionParams {
                expression: "A+B".to_string(),
                output_type: RasterDataType::I8,
                output_no_data_value: no_data_value.as_(), //  cast no_data_valuee to f64
                output_measurement: Some(Measurement::Unitless),
            },
            sources: ExpressionSources {
                a: raster_a,
                b: Some(raster_b),
                c: None,
            },
        }
        .boxed()
        .initialize(&MockExecutionContext::default())
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

    fn make_raster() -> Box<dyn RasterOperator> {
        let no_data_value = None;
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
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed()
    }
}
