use crate::engine::{
    InitializedOperator, InitializedOperatorImpl, Operator, QueryProcessor, RasterQueryProcessor,
    TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
};
use crate::util::Result;
use futures::StreamExt;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::raster::Pixel;
use geoengine_datatypes::{
    collections::MultiPointCollection,
    primitives::FeatureData,
    raster::{GridPixelAccess, RasterTile2D},
    spatial_reference::SpatialReferenceOption,
};
use serde::{Deserialize, Serialize};

pub struct MockRasterPointJoinProcessor<R, V> {
    raster_source: R,
    point_source: V,
    feature_name: String,
}

impl<R, V> MockRasterPointJoinProcessor<R, V> {
    pub fn new(raster_source: R, point_source: V, params: MockRasterPointJoinParams) -> Self {
        Self {
            raster_source,
            point_source,
            feature_name: params.feature_name,
        }
    }
}

impl<R, V, T> QueryProcessor for MockRasterPointJoinProcessor<R, V>
where
    R: RasterQueryProcessor<RasterType = T> + Sync,
    T: Pixel,
    V: VectorQueryProcessor<VectorType = MultiPointCollection> + Sync,
{
    type Output = MultiPointCollection;
    fn query(
        &self,
        query: crate::engine::QueryRectangle,
        ctx: crate::engine::QueryContext,
    ) -> futures::stream::BoxStream<crate::util::Result<Self::Output>> {
        let point_stream = self.point_source.vector_query(query, ctx);
        point_stream
            .then(async move |collection| {
                let collection = collection?;
                let mut raster_stream = self.raster_source.raster_query(query, ctx);
                let raster_future = raster_stream.next().await;
                let raster_tile: RasterTile2D<T> =
                    raster_future.ok_or(crate::error::Error::QueryProcessor)??;
                let pixel: T = raster_tile.data.pixel_value_at_grid_index(&(0, 0))?;
                let pixel: f64 = pixel.as_();
                let collection = collection.add_column(
                    &self.feature_name,
                    FeatureData::Number(vec![pixel; collection.len()]),
                )?;
                Ok(collection)
            })
            .boxed()
    }
}

fn create_binary_raster_vector<TR, TV>(
    source_a: Box<dyn RasterQueryProcessor<RasterType = TR>>,
    source_b: Box<dyn VectorQueryProcessor<VectorType = TV>>,
    params: MockRasterPointJoinParams,
) -> MockRasterPointJoinProcessor<
    Box<dyn RasterQueryProcessor<RasterType = TR>>,
    Box<dyn VectorQueryProcessor<VectorType = TV>>,
>
where
    TR: Copy + Sync + 'static,
{
    MockRasterPointJoinProcessor::new(source_a, source_b, params)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MockRasterPointJoinParams {
    pub feature_name: String,
}

pub type MockRasterPointJoinOperator = Operator<MockRasterPointJoinParams>;

#[typetag::serde]
impl VectorOperator for MockRasterPointJoinOperator {
    fn initialize(
        self: Box<Self>,
        context: crate::engine::ExecutionContext,
    ) -> Result<Box<crate::engine::InitializedVectorOperator>> {
        InitializedOperatorImpl::create(
            self.params,
            context,
            |_, _, _, _| Ok(()),
            |_, _, _, _, vs| {
                Ok(VectorResultDescriptor {
                    projection: vs.get(0).map_or_else(
                        || SpatialReferenceOption::None,
                        |o| o.result_descriptor().projection,
                    ),
                    data_type: VectorDataType::MultiPoint,
                })
            },
            self.raster_sources,
            self.vector_sources,
        )
        .map(InitializedOperatorImpl::boxed)
    }
}

impl InitializedOperator<VectorResultDescriptor, TypedVectorQueryProcessor>
    for InitializedOperatorImpl<MockRasterPointJoinParams, VectorResultDescriptor, ()>
{
    fn query_processor(&self) -> Result<crate::engine::TypedVectorQueryProcessor> {
        let raster_source = self.raster_sources[0].query_processor()?;
        let point_source = match self.vector_sources[0].query_processor()? {
            TypedVectorQueryProcessor::MultiPoint(v) => v,
            _ => panic!(),
        };
        Ok(TypedVectorQueryProcessor::MultiPoint(match raster_source {
            crate::engine::TypedRasterQueryProcessor::U8(r) => {
                Box::new(create_binary_raster_vector::<u8, MultiPointCollection>(
                    r,
                    point_source,
                    self.params.clone(),
                ))
            }
            _ => panic!(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        engine::{
            ExecutionContext, QueryContext, QueryRectangle, RasterOperator, RasterResultDescriptor,
        },
        mock::{MockPointSource, MockPointSourceParams, MockRasterSource, MockRasterSourceParams},
    };
    use futures::executor::block_on_stream;
    use geoengine_datatypes::{
        primitives::{BoundingBox2D, Coordinate2D, FeatureDataRef, TimeInterval},
        raster::{Raster2D, RasterDataType, TileInformation},
        spatial_reference::SpatialReference,
    };

    #[test]
    #[allow(clippy::too_many_lines)]
    fn serde() {
        let points = vec![Coordinate2D::new(1., 2.); 3];
        let mps = MockPointSource {
            params: MockPointSourceParams { points },
        }
        .boxed();

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
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    projection: SpatialReference::wgs84().into(),
                },
            },
        }
        .boxed();

        let params = MockRasterPointJoinParams {
            feature_name: "raster_values".to_string(),
        };
        let op = MockRasterPointJoinOperator {
            params,
            raster_sources: vec![mrs],
            vector_sources: vec![mps],
        }
        .boxed();

        let serialized = serde_json::to_string(&op).unwrap();
        let expected = serde_json::json!({
            "type": "MockRasterPointJoinOperator",
            "params": {
                "feature_name": "raster_values"
            },
            "raster_sources": [{
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
                    "result_descriptor": {
                        "data_type": "U8",
                        "projection": "EPSG:4326"
                    }
                }
            }],
            "vector_sources": [{
                "type": "MockPointSource",
                "params": {
                "points": [{
                    "x": 1.0,
                    "y": 2.0
                }, {
                    "x": 1.0,
                    "y": 2.0
                }, {
                    "x": 1.0,
                    "y": 2.0
                }]
            }}]

        })
        .to_string();
        assert_eq!(serialized, expected);
        let _: Box<dyn VectorOperator> = serde_json::from_str(&serialized).unwrap();
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn execute() {
        let points = vec![Coordinate2D::new(1., 2.); 3];
        let mps = MockPointSource {
            params: MockPointSourceParams { points },
        }
        .boxed();

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
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    projection: SpatialReference::wgs84().into(),
                },
            },
        }
        .boxed();
        let new_column_name = "raster_values".to_string();
        let params = MockRasterPointJoinParams {
            feature_name: new_column_name.clone(),
        };
        let op = MockRasterPointJoinOperator {
            params,
            raster_sources: vec![mrs],
            vector_sources: vec![mps],
        }
        .boxed();

        let execution_context = ExecutionContext;

        let initialized = op.initialize(execution_context).unwrap();

        let point_processor = match initialized.query_processor() {
            Ok(TypedVectorQueryProcessor::MultiPoint(processor)) => processor,
            _ => panic!(),
        };

        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new((0., 0.).into(), (4., 4.).into()).unwrap(),
            time_interval: TimeInterval::default(),
        };
        let ctx = QueryContext {
            chunk_byte_size: 2 * std::mem::size_of::<Coordinate2D>(),
        };
        let stream = point_processor.vector_query(query_rectangle, ctx);

        let blocking_stream = block_on_stream(stream);
        let collections: Vec<MultiPointCollection> = blocking_stream.map(Result::unwrap).collect();
        assert_eq!(collections.len(), 2);
        assert_eq!(collections[0].len(), 2);
        assert_eq!(collections[1].len(), 1);

        let column = collections[0].data(&new_column_name).unwrap();
        let numbers = if let FeatureDataRef::Number(numbers) = column {
            numbers
        } else {
            panic!()
        };

        assert_eq!(numbers.as_ref(), &[1.0, 1.0]);

        let column = collections[1].data(&new_column_name).unwrap();
        let numbers = if let FeatureDataRef::Number(numbers) = column {
            numbers
        } else {
            panic!()
        };

        assert_eq!(numbers.as_ref(), &[1.0]);
    }
}
