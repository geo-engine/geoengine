use crate::engine::{
    InitializedOperator, InitializedOperatorImpl, InitializedRasterOperator, Operator,
    QueryContext, QueryProcessor, QueryRectangle, RasterOperator, RasterQueryProcessor,
    RasterResultDescriptor, TypedRasterQueryProcessor,
};
use crate::opencl::{CLProgram, CompiledCLProgram, IterationType, RasterArgument};
use crate::util::Result;
use crate::{call_bi_generic_processor, call_generic_raster_processor};
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::primitives::Measurement;
use geoengine_datatypes::raster::{Grid2D, Pixel, RasterDataType, RasterTile2D, TypedValue};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::marker::PhantomData;

/// Parameters for the `Expression` operator.
/// * The `expression` must only contain simple arithmetic
///     calculations.
/// * `output_type` is the data type of the produced raster tiles.
/// * `output_no_data_value` is the no data value of the output raster
/// * `output_measurement` is the measurement description of the output
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ExpressionParams {
    pub expression: String,
    pub output_type: RasterDataType,
    pub output_no_data_value: TypedValue,
    pub output_measurement: Option<Measurement>,
}

// TODO: custom type or simple string?
struct SafeExpression {
    expression: String,
}

/// The `Expression` operator calculates an expression for all pixels of the input rasters and
/// produces raster tiles of a given output type
pub type Expression = Operator<ExpressionParams>;

impl Expression {
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

#[typetag::serde]
impl RasterOperator for Expression {
    fn initialize(
        self: Box<Self>,
        context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<InitializedRasterOperator>> {
        ensure!(
            Self::is_allowed_expression(&self.params.expression),
            crate::error::InvalidExpression
        );
        ensure!(
            self.vector_sources.is_empty(),
            crate::error::InvalidNumberOfVectorInputs {
                expected: 0..0,
                found: self.vector_sources.len()
            }
        );
        ensure!(
            !self.raster_sources.is_empty(),
            crate::error::InvalidNumberOfRasterInputs {
                expected: 1..9,
                found: self.raster_sources.len()
            }
        );

        let raster_sources = self
            .raster_sources
            .into_iter()
            .map(|source| source.initialize(context))
            .collect::<Result<Vec<_>>>()?;

        let spatial_reference = raster_sources[0].result_descriptor().spatial_reference;

        for other_spatial_refenence in raster_sources
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
        };

        Ok(
            InitializedOperatorImpl::new(
                self.params,
                result_descriptor,
                raster_sources,
                vec![],
                (),
            )
            .boxed(),
        )
    }
}

impl InitializedOperator<RasterResultDescriptor, TypedRasterQueryProcessor>
    for InitializedOperatorImpl<ExpressionParams, RasterResultDescriptor, ()>
{
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        // TODO: handle different number of sources

        match *self.raster_sources.as_slice() {
            [ref a, ref b] => {
                let a = a.query_processor()?;
                let b = b.query_processor()?;

                call_bi_generic_processor!(a, b, (p_a, p_b) => {
                    let res = call_generic_raster_processor!(self.params.output_type, ExpressionQueryProcessor::new(
                                &SafeExpression {
                                    expression: self.params.expression.clone(),
                                },
                                p_a,
                                p_b,
                                self.params.output_no_data_value.try_into()?
                            )
                            .boxed());
                    Ok(res)
                })
            }
            _ => unimplemented!(), // TODO: handle more than two inputs
        }
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
    pub cl_program: CompiledCLProgram,
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
            cl_program: Self::create_cl_program(&expression),
            phantom_data: PhantomData::default(),
            no_data_value,
        }
    }

    fn create_cl_program(expression: &SafeExpression) -> CompiledCLProgram {
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

        let mut cl_program = CLProgram::new(IterationType::Raster);
        cl_program.add_input_raster(RasterArgument::new(T1::TYPE));
        cl_program.add_input_raster(RasterArgument::new(T2::TYPE));
        cl_program.add_output_raster(RasterArgument::new(TO::TYPE));

        cl_program.compile(&source, "expressionkernel").unwrap()
    }
}

impl<'a, T1, T2, TO> QueryProcessor for ExpressionQueryProcessor<T1, T2, TO>
where
    T1: Pixel,
    T2: Pixel,
    TO: Pixel,
{
    type Output = RasterTile2D<TO>;

    fn query<'b>(
        &'b self,
        query: QueryRectangle,
        ctx: &'b dyn QueryContext,
    ) -> BoxStream<'b, Result<RasterTile2D<TO>>> {
        // TODO: validate that tiles actually fit together
        let mut cl_program = self.cl_program.clone();
        self.source_a
            .query(query, ctx)
            .zip(self.source_b.query(query, ctx))
            .map(move |(a, b)| match (a, b) {
                (Ok(a), Ok(b)) => {
                    let mut out = Grid2D::new(
                        a.grid_dimension(),
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
                        raster,
                    ))
                }
                _ => unimplemented!(),
            })
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use geoengine_datatypes::primitives::{
        BoundingBox2D, Measurement, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::raster::TileInformation;
    use geoengine_datatypes::spatial_reference::SpatialReference;

    #[tokio::test]
    async fn basic() {
        let a = make_raster();
        let b = make_raster();

        let o = Expression {
            params: ExpressionParams {
                expression: "A+B".to_string(),
                output_type: RasterDataType::I8,
                output_no_data_value: TypedValue::I8(42),
                output_measurement: Some(Measurement::Unitless),
            },
            raster_sources: vec![a, b],
            vector_sources: vec![],
        }
        .boxed()
        .initialize(&MockExecutionContext::default())
        .unwrap();

        let p = o.query_processor().unwrap().get_i8().unwrap();

        let ctx = MockQueryContext::new(1);
        let q = p.query(
            QueryRectangle {
                bbox: BoundingBox2D::new_unchecked((1., 2.).into(), (3., 4.).into()),
                time_interval: Default::default(),
                spatial_resolution: SpatialResolution::one(),
            },
            &ctx,
        );

        let c: Vec<Result<RasterTile2D<i8>>> = q.collect().await;

        assert_eq!(c.len(), 1);

        assert_eq!(
            c[0].as_ref().unwrap().grid_array,
            Grid2D::new([3, 2].into(), vec![2, 4, 6, 8, 10, 12], Some(42),).unwrap()
        );
    }

    fn make_raster() -> Box<dyn RasterOperator> {
        let raster = Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], None).unwrap();

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
                global_geo_transform: Default::default(),
            },
            raster,
        );

        MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::I8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                },
            },
        }
        .boxed()
    }
}
