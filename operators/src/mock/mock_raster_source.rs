use crate::call_generic_raster_processor;
use crate::engine::{
    InitializedOperator, InitializedOperatorBase, InitializedOperatorImpl,
    InitializedRasterOperator, QueryProcessor, RasterOperator, RasterQueryProcessor,
    RasterResultDescriptor, SourceOperator, TypedRasterQueryProcessor,
};
use crate::util::Result;
use futures::{stream, stream::StreamExt};
use geoengine_datatypes::raster::{FromPrimitive, Pixel, RasterTile2D};
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct MockRasterSourceProcessor<T>
where
    T: Pixel,
{
    pub data: Vec<RasterTile2D<T>>,
}

impl<T> MockRasterSourceProcessor<T>
where
    T: Pixel,
{
    fn new(data: Vec<RasterTile2D<T>>) -> Self {
        Self { data }
    }
}

impl<T> QueryProcessor for MockRasterSourceProcessor<T>
where
    T: Pixel,
{
    type Output = RasterTile2D<T>;
    fn query(
        &self,
        _query: crate::engine::QueryRectangle,
        _ctx: crate::engine::QueryContext,
    ) -> futures::stream::BoxStream<crate::util::Result<Self::Output>> {
        stream::iter(self.data.iter().cloned().map(Result::Ok)).boxed()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct MockRasterSourceParams {
    pub data: Vec<RasterTile2D<u8>>,
    pub result_descriptor: RasterResultDescriptor,
}

pub type MockRasterSource = SourceOperator<MockRasterSourceParams>;

#[typetag::serde]
impl RasterOperator for MockRasterSource {
    fn initialize(
        self: Box<Self>,
        context: &crate::engine::ExecutionContext,
    ) -> Result<Box<InitializedRasterOperator>> {
        InitializedOperatorImpl::create(
            self.params,
            context,
            |_, _, _, _| Ok(()),
            |params, _, _, _, _| Ok(params.result_descriptor),
            vec![],
            vec![],
        )
        .map(InitializedOperatorImpl::boxed)
    }
}

impl InitializedOperator<RasterResultDescriptor, TypedRasterQueryProcessor>
    for InitializedOperatorImpl<MockRasterSourceParams, RasterResultDescriptor, ()>
{
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        fn converted<From, To>(
            raster_tiles: &[RasterTile2D<From>],
        ) -> Box<dyn RasterQueryProcessor<RasterType = To>>
        where
            From: Pixel + AsPrimitive<To>,
            To: Pixel + FromPrimitive<From>,
        {
            let data: Vec<RasterTile2D<To>> = raster_tiles
                .iter()
                .cloned()
                .map(RasterTile2D::convert)
                .collect();
            MockRasterSourceProcessor::new(data).boxed()
        }

        Ok(call_generic_raster_processor!(
            self.result_descriptor().data_type,
            converted(&self.params.data)
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::ExecutionContext;
    use geoengine_datatypes::raster::RasterDataType;
    use geoengine_datatypes::{
        primitives::TimeInterval,
        raster::{Raster2D, TileInformation},
        spatial_reference::SpatialReference,
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
                global_geo_transform: Default::default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            data: raster,
        };

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::wgs84().into(),
                },
            },
        }
        .boxed();

        let serialized = serde_json::to_string(&mrs).unwrap();

        let spec = serde_json::json!({
            "type": "MockRasterSource",
            "params": {
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
                        "global_geo_transform": {
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
                "result_descriptor": {
                    "data_type": "U8",
                    "spatial_reference": "EPSG:4326"
                }
            }
        })
        .to_string();
        assert_eq!(serialized, spec);

        let deserialized: Box<dyn RasterOperator> = serde_json::from_str(&serialized).unwrap();

        let execution_context = ExecutionContext::mock_empty();

        let initialized = deserialized.initialize(&execution_context).unwrap();

        match initialized.query_processor().unwrap() {
            crate::engine::TypedRasterQueryProcessor::U8(..) => {}
            _ => panic!("wrong raster type"),
        }
    }
}
