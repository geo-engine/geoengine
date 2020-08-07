use crate::engine::{
    Operator, QueryProcessor, RasterOperator, RasterQueryProcessor, TypedRasterQueryProcessor,
    VectorOperator,
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
    fn create_raster_op(&self) -> TypedRasterQueryProcessor {
        fn converted<T>(
            raster_tiles: &[RasterTile2D<u8>],
        ) -> Box<dyn RasterQueryProcessor<RasterType = T>>
        where
            T: 'static + From<u8> + std::marker::Sync + std::marker::Send + Copy,
        {
            let data: Vec<RasterTile2D<T>> = raster_tiles
                .iter()
                .cloned()
                .map(RasterTile2D::convert)
                .collect();
            MockRasterSourceProcessor::new(data).boxed()
        }

        match self.result_type() {
            RasterDataType::U8 => crate::engine::TypedRasterQueryProcessor::U8(
                MockRasterSourceProcessor::new(self.data.clone()).boxed(),
            ),
            RasterDataType::U16 => {
                crate::engine::TypedRasterQueryProcessor::U16(converted(&self.data))
            }
            RasterDataType::U32 => {
                crate::engine::TypedRasterQueryProcessor::U32(converted(&self.data))
            }
            RasterDataType::U64 => {
                crate::engine::TypedRasterQueryProcessor::U64(converted(&self.data))
            }
            RasterDataType::I8 => unimplemented!("cannot cast u8 to i8"),
            RasterDataType::I16 => {
                crate::engine::TypedRasterQueryProcessor::I16(converted(&self.data))
            }
            RasterDataType::I32 => {
                crate::engine::TypedRasterQueryProcessor::I32(converted(&self.data))
            }
            RasterDataType::I64 => {
                crate::engine::TypedRasterQueryProcessor::I64(converted(&self.data))
            }
            RasterDataType::F32 => {
                crate::engine::TypedRasterQueryProcessor::F32(converted(&self.data))
            }
            RasterDataType::F64 => {
                crate::engine::TypedRasterQueryProcessor::F64(converted(&self.data))
            }
        }
    }
    fn result_type(&self) -> RasterDataType {
        self.raster_type
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
        }
        .boxed();

        let serialized = serde_json::to_string(&mrs).unwrap();

        let spec = serde_json::json!({
            "type": "MockRasterSource",
            "data": [{
                "time": {
                    "start": -9_223_372_036_854_775_808_i64,
                    "end": 9_223_372_036_854_775_807_i64
                },
                "tile": {
                    "global_size_in_tiles": {
                        "dimension_size": [1, 2]
                    },
                    "global_tile_position": {
                        "dimension_size": [0, 0]
                    },
                    "global_pixel_position": {
                        "dimension_size": [0, 0]
                    },
                    "tile_size_in_pixels": {
                        "dimension_size": [3, 2]
                    },
                    "geo_transform": {
                        "upper_left_coordinate": {
                            "x": 0.0,
                            "y": 0.0
                        },
                        "x_pixel_size": 1.0,
                        "y_pixel_size": -1.0
                    }
                },
                "data": {
                    "grid_dimension": {
                        "dimension_size": [3, 2]
                    },
                    "data_container": [1, 2, 3, 4, 5, 6],
                    "no_data_value": null,
                    "geo_transform": {
                        "upper_left_coordinate": {
                            "x": 0.0,
                            "y": 0.0
                        },
                        "x_pixel_size": 1.0,
                        "y_pixel_size": -1.0
                    },
                    "temporal_bounds": {
                        "start": -9_223_372_036_854_775_808_i64,
                        "end": 9_223_372_036_854_775_807_i64
                    }
                }
            }],
            "raster_type": "U8"
        })
        .to_string();
        assert_eq!(serialized, spec);

        let deserialized: Box<dyn RasterOperator> = serde_json::from_str(&serialized).unwrap();

        match deserialized.create_raster_op() {
            crate::engine::TypedRasterQueryProcessor::U8(..) => {}
            _ => panic!("wrong raster type"),
        }
    }
}
