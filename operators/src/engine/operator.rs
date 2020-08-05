use super::query_processor::{
    RasterQueryProcessor, TypedRasterQueryProcessor, TypedVectorQueryProcessor,
    VectorQueryProcessor,
};
use geoengine_datatypes::{collections::MultiPointCollection, raster::RasterDataType};

pub trait Operator {
    /// get the sources of the Operator. TODO: extra trait?
    fn raster_sources(&self) -> &[Box<dyn RasterOperator>];
    //fn raster_sources(&self) -> &[&dyn MetaRasterOperator];

    /// get the sources of the Operator. TODO: extra trait?
    fn vector_sources(&self) -> &[Box<dyn VectorOperator>] {
        &[]
    }
}

#[typetag::serde(tag = "type")]
pub trait VectorOperator: Operator {
    fn result_type(&self) -> () {
        ()
    }

    fn vector_query_processor(&self) -> TypedVectorQueryProcessor {
        println!("MetaVectorOperator: create_vector_op");
        match self.result_type() {
            () => TypedVectorQueryProcessor::MultiPoint(self.multi_point_processor()),
        }
    }

    fn multi_point_processor(
        &self,
    ) -> Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>;
}

/// The MetaRasterOperator is a trait for MetaOperators creating RasterOperators for processing Raster data
#[typetag::serde(tag = "type")]
pub trait RasterOperator: Operator {
    /// The magic method to handle the mapping of the create type to a concrete implementation. More work required! TODO: macro?
    fn create_raster_op(&self) -> TypedRasterQueryProcessor {
        println!("MetaRasterOperator: create_raster_op");
        match self.result_type() {
            RasterDataType::U8 => TypedRasterQueryProcessor::U8(self.u8_query_processor()),
            RasterDataType::U16 => TypedRasterQueryProcessor::U16(self.u16_query_processor()),
            RasterDataType::U32 => TypedRasterQueryProcessor::U32(self.u32_query_processor()),
            RasterDataType::U64 => TypedRasterQueryProcessor::U64(self.u64_query_processor()),
            RasterDataType::I8 => TypedRasterQueryProcessor::I8(self.i8_query_processor()),
            RasterDataType::I16 => TypedRasterQueryProcessor::I16(self.i16_query_processor()),
            RasterDataType::I32 => TypedRasterQueryProcessor::I32(self.i32_query_processor()),
            RasterDataType::I64 => TypedRasterQueryProcessor::I64(self.i64_query_processor()),
            RasterDataType::F32 => TypedRasterQueryProcessor::F32(self.f32_query_processor()),
            RasterDataType::F64 => TypedRasterQueryProcessor::F64(self.f64_query_processor()),
        }
    }

    // there is no way to use generics for the MetaRasterOperators in combination with serialisation -_-. We need to implement the create operator methods. TODO: Macro?
    fn u8_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = u8>>;
    fn u16_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = u16>>;
    fn u32_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = u32>>;
    fn u64_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = u64>>;
    fn i8_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = i8>>;
    fn i16_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = i16>>;
    fn i32_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = i32>>;
    fn i64_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = i64>>;
    fn f32_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = f32>>;
    fn f64_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = f64>>;

    /// get the type the Operator creates.
    fn result_type(&self) -> RasterDataType;
}
