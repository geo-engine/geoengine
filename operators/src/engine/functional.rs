use super::{
    query_processor::{RasterQueryProcessor, TypedRasterQueryProcessor},
    VectorQueryProcessor,
};
use std::ops::{Add, AddAssign};

pub trait CreateSourceOperator<P> {
    fn create(params: P) -> Self;
}

pub trait CreateUnaryOperator<S, P> {
    fn create<T1>(source: S, params: P) -> Self;
}

pub trait CreateBinaryOperator<S1, S2, P> {
    fn create<T1, T2>(source_a: S1, source_b: S2, params: P) -> Self;
}

pub trait CreateBoxedUnaryRasterProcessor<P> {
    fn create_unary_boxed<T1>(
        source: Box<dyn RasterQueryProcessor<RasterType = T1>>,
        params: P,
    ) -> Box<dyn RasterQueryProcessor<RasterType = T1>>
    where
        T1: Add + AddAssign + Copy + 'static; // + One
}

pub trait CreateBoxedBinaryOperatorInplace<P> {
    fn create_binary_boxed<T1, T2>(
        source_a: Box<dyn RasterQueryProcessor<RasterType = T1>>,
        source_b: Box<dyn RasterQueryProcessor<RasterType = T2>>,
        params: P,
    ) -> Box<dyn RasterQueryProcessor<RasterType = T1>>
    where
        T1: Add + AddAssign + Copy + 'static, // + One
        T2: Add + AddAssign + Into<T1> + Copy + 'static; // + One
}

pub trait CreateBoxedBinaryRasterQueryProcessor<P> {
    type Output;
    fn create_binary_boxed<T1, T2>(
        source_a: Box<dyn RasterQueryProcessor<RasterType = T1>>,
        source_b: Box<dyn RasterQueryProcessor<RasterType = T2>>,
        params: P,
    ) -> Self::Output
    where
        T1: Copy + 'static,
        T2: Copy + 'static;
}

pub trait CreateBoxedBinaryRasterVectorQueryProcessor<P> {
    type Output;
    fn create_binary_boxed<T1, T2>(
        source_a: Box<dyn RasterQueryProcessor<RasterType = T1>>,
        source_b: Box<dyn VectorQueryProcessor<VectorType = T2>>,
        params: P,
    ) -> Self::Output
    where
        T1: Copy + 'static,
        T2: Copy + 'static;
}

pub fn create_operator_unary_raster_u8<O>(
    source: TypedRasterQueryProcessor,
) -> Box<dyn RasterQueryProcessor<RasterType = u8> + 'static>
where
    O: CreateBoxedUnaryRasterProcessor<String>,
{
    println!("create_operator_unary_raster_u8");
    let s = source.get_u8().expect("not u8");

    O::create_unary_boxed(s, "params".to_string())
}

pub fn create_operator_unary_raster_u16<O>(
    source: TypedRasterQueryProcessor,
) -> Box<dyn RasterQueryProcessor<RasterType = u16> + 'static>
where
    O: CreateBoxedUnaryRasterProcessor<String>,
{
    println!("create_operator_unary_raster_u16");
    let s = source.get_u16().expect("not u16");

    O::create_unary_boxed(s, "params".to_string())
}

pub fn create_operator_binary_raster_u8_u8<O>(
    source_a: TypedRasterQueryProcessor,
    source_b: TypedRasterQueryProcessor,
) -> Box<dyn RasterQueryProcessor<RasterType = u8> + 'static>
where
    O: CreateBoxedBinaryOperatorInplace<String> + 'static,
{
    println!("create_operator_binary_raster_u8_u8");
    match (source_a, source_b) {
        (TypedRasterQueryProcessor::U8(a), TypedRasterQueryProcessor::U8(b)) => {
            O::create_binary_boxed(a, b, "params".to_string())
        }
        _ => panic!(),
    }
}

pub fn create_operator_binary_raster_u16_x_commutativ<O>(
    source_a: TypedRasterQueryProcessor,
    source_b: TypedRasterQueryProcessor,
) -> Box<dyn RasterQueryProcessor<RasterType = u16> + 'static>
where
    O: CreateBoxedBinaryOperatorInplace<String> + 'static,
{
    println!("create_operator_binary_raster_u16_x_commutativ");

    match (source_a, source_b) {
        (TypedRasterQueryProcessor::U16(a), TypedRasterQueryProcessor::U8(b)) => {
            O::create_binary_boxed(a, b, "params".to_string())
        }
        (TypedRasterQueryProcessor::U8(a), TypedRasterQueryProcessor::U16(b)) => {
            O::create_binary_boxed(b, a, "params".to_string())
        }
        (TypedRasterQueryProcessor::U16(a), TypedRasterQueryProcessor::U16(b)) => {
            O::create_binary_boxed(a, b, "params".to_string())
        }
        _ => panic!(),
    }
}
