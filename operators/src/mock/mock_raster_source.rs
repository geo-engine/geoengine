use crate::engine::{
    Operator, QueryProcessor, RasterOperator, RasterQueryProcessor, VectorOperator,
};
use futures::{stream, stream::StreamExt};
use geoengine_datatypes::raster::{Raster2D, RasterDataType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct MockRasterSourceProcessor<T> {
    pub data: Vec<Raster2D<T>>,
}

impl<T> MockRasterSourceProcessor<T> {
    fn new(data: Vec<Raster2D<T>>) -> Self {
        Self { data }
    }
}

impl<T: std::marker::Sync + Clone> QueryProcessor for MockRasterSourceProcessor<T> {
    type Output = Raster2D<T>;
    fn query(
        &self,
        _query: crate::engine::QueryRectangle,
        _ctx: crate::engine::QueryContext,
    ) -> futures::stream::BoxStream<crate::util::Result<Self::Output>> {
        stream::iter(self.data.iter().cloned().map(Result::Ok)).boxed()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct MockRasterSource {
    pub data: Vec<Raster2D<u8>>,
    pub raster_type: RasterDataType,
}

impl Operator for MockRasterSource {
    fn raster_sources(&self) -> &[Box<dyn RasterOperator>] {
        &[] // no sources!
    }
    fn vector_sources(&self) -> &[Box<dyn VectorOperator>] {
        &[]
    }
}

#[typetag::serde]
impl RasterOperator for MockRasterSource {
    fn u8_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = u8>> {
        Box::new(MockRasterSourceProcessor::new(self.data.clone()))
    }
    fn u16_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = u16>> {
        todo!()
    }
    fn u32_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = u32>> {
        todo!()
    }
    fn u64_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = u64>> {
        todo!()
    }
    fn i8_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = i8>> {
        todo!()
    }
    fn i16_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = i16>> {
        todo!()
    }
    fn i32_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = i32>> {
        todo!()
    }
    fn i64_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = i64>> {
        todo!()
    }
    fn f32_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = f32>> {
        todo!()
    }
    fn f64_query_processor(&self) -> Box<dyn RasterQueryProcessor<RasterType = f64>> {
        todo!()
    }
    fn result_type(&self) -> RasterDataType {
        self.raster_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde() {
        let raster = Raster2D::new(
            [3, 2].into(),
            vec![1, 2, 3, 4, 5, 6],
            None,
            Default::default(),
            Default::default(),
        )
        .unwrap();

        let mrs = MockRasterSource {
            data: vec![raster],
            raster_type: RasterDataType::U8,
        };

        let bmrs = mrs.boxed();

        let serialized = serde_json::to_string(&bmrs).unwrap();

        let spec = "{\"type\":\"MockRasterSource\",\"data\":[{\"grid_dimension\":{\"dimension_size\":[3,2]},\"data_container\":[1,2,3,4,5,6],\"no_data_value\":null,\"geo_transform\":{\"upper_left_coordinate\":{\"x\":0.0,\"y\":0.0},\"x_pixel_size\":1.0,\"y_pixel_size\":-1.0},\"temporal_bounds\":{\"start\":-9223372036854775808,\"end\":9223372036854775807}}],\"raster_type\":\"U8\"}";
        assert_eq!(&serialized, spec);

        let deserialized: Box<dyn RasterOperator> = serde_json::from_str(&serialized).unwrap();

        let op = deserialized.create_raster_op();
        match op {
            crate::engine::TypedRasterQueryProcessor::U8(_rs) => {}
            crate::engine::TypedRasterQueryProcessor::U16(_) => {}
            crate::engine::TypedRasterQueryProcessor::U32(_) => {}
            crate::engine::TypedRasterQueryProcessor::U64(_) => {}
            crate::engine::TypedRasterQueryProcessor::I8(_) => {}
            crate::engine::TypedRasterQueryProcessor::I16(_) => {}
            crate::engine::TypedRasterQueryProcessor::I32(_) => {}
            crate::engine::TypedRasterQueryProcessor::I64(_) => {}
            crate::engine::TypedRasterQueryProcessor::F32(_) => {}
            crate::engine::TypedRasterQueryProcessor::F64(_) => {}
        }
    }
}
