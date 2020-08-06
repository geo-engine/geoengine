use crate::engine::{
    Operator, QueryProcessor, RasterOperator, RasterQueryProcessor, VectorOperator,
};
use futures::{stream, stream::StreamExt};
use geoengine_datatypes::raster::{RasterDataType, RasterTile2D};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct MockRasterSourceProcessor<T> {
    pub data: Vec<RasterTile2D<T>>,
}

impl<T> MockRasterSourceProcessor<T> {
    fn new(data: Vec<RasterTile2D<T>>) -> Self {
        Self { data }
    }
}

impl<T: std::marker::Sync + Clone> QueryProcessor for MockRasterSourceProcessor<T> {
    type Output = RasterTile2D<T>;
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
    pub data: Vec<RasterTile2D<u8>>,
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
    fn result_type(&self) -> RasterDataType {
        self.raster_type
    }
    fn create_raster_op(&self) -> crate::engine::TypedRasterQueryProcessor {
        match self.result_type() {
            RasterDataType::U8 => crate::engine::TypedRasterQueryProcessor::U8(
                MockRasterSourceProcessor::new(self.data.clone()).boxed(),
            ),
            RasterDataType::U16 => unimplemented!(),
            RasterDataType::U32 => unimplemented!(),
            RasterDataType::U64 => unimplemented!(),
            RasterDataType::I8 => unimplemented!(),
            RasterDataType::I16 => unimplemented!(),
            RasterDataType::I32 => unimplemented!(),
            RasterDataType::I64 => unimplemented!(),
            RasterDataType::F32 => unimplemented!(),
            RasterDataType::F64 => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::{
        primitives::TimeInterval,
        raster::{Raster2D, TileInformation},
    };

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

        let raster_tile = RasterTile2D {
            time: TimeInterval::default(),
            tile: TileInformation {
                geo_transform: Default::default(),
                global_pixel_position: [0, 0].into(),
                global_size_in_tiles: [1, 2].into(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            data: raster,
        };

        let mrs = MockRasterSource {
            data: vec![raster_tile],
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
