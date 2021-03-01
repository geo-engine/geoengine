use crate::engine::{
    InitializedOperator, InitializedOperatorImpl, Operator, QueryProcessor, RasterQueryProcessor,
    TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
};
use crate::error;
use crate::util::Result;
use futures::StreamExt;
use geoengine_datatypes::collections::{
    FeatureCollectionInfos, FeatureCollectionModifications, VectorDataType,
};
use geoengine_datatypes::primitives::FeatureDataType;
use geoengine_datatypes::raster::Pixel;
use geoengine_datatypes::{
    collections::MultiPointCollection,
    primitives::FeatureData,
    raster::{GridIndexAccess, RasterTile2D},
};
use serde::{Deserialize, Serialize};
use snafu::ensure;

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
    fn query<'a>(
        &'a self,
        query: crate::engine::QueryRectangle,
        ctx: &'a dyn crate::engine::QueryContext,
    ) -> futures::stream::BoxStream<'a, crate::util::Result<Self::Output>> {
        let point_stream = self.point_source.vector_query(query, ctx);
        point_stream
            .then(async move |collection| {
                let collection = collection?;
                let mut raster_stream = self.raster_source.raster_query(query, ctx);
                let raster_future = raster_stream.next().await;
                let raster_tile: RasterTile2D<T> =
                    raster_future.ok_or(crate::error::Error::QueryProcessor)??;
                let pixel: T = raster_tile.grid_array.get_at_grid_index([0, 0])?;
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
        context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<crate::engine::InitializedVectorOperator>> {
        ensure!(
            self.vector_sources.len() == 1,
            error::InvalidNumberOfVectorInputs {
                expected: 1..2,
                found: self.vector_sources.len(),
            }
        );

        ensure!(
            self.raster_sources.len() == 1,
            error::InvalidNumberOfRasterInputs {
                expected: 1..2,
                found: self.raster_sources.len(),
            }
        );

        let vector_sources = self
            .vector_sources
            .into_iter()
            .map(|o| o.initialize(context))
            .collect::<Result<Vec<_>>>()?;

        ensure!(
            vector_sources[0].result_descriptor().data_type == VectorDataType::MultiPoint,
            error::InvalidType {
                expected: VectorDataType::MultiPoint.to_string(),
                found: vector_sources[0].result_descriptor().data_type.to_string(),
            }
        );

        let raster_sources = self
            .raster_sources
            .into_iter()
            .map(|o| o.initialize(context))
            .collect::<Result<Vec<_>>>()?;

        let result_descriptor = {
            let mut columns = vector_sources[0].result_descriptor().columns.clone();
            if columns
                .insert(self.params.feature_name.clone(), FeatureDataType::Number)
                .is_some()
            {
                return Err(geoengine_datatypes::error::Error::ColumnNameConflict {
                    name: self.params.feature_name,
                }
                .into());
            }

            VectorResultDescriptor {
                spatial_reference: vector_sources[0].result_descriptor().spatial_reference,
                data_type: VectorDataType::MultiPoint,
                columns,
            }
        };

        Ok(InitializedOperatorImpl::new(
            result_descriptor,
            raster_sources,
            vector_sources,
            self.params,
        )
        .boxed())
    }
}

impl InitializedOperator<VectorResultDescriptor, TypedVectorQueryProcessor>
    for InitializedOperatorImpl<VectorResultDescriptor, MockRasterPointJoinParams>
{
    fn query_processor(&self) -> Result<crate::engine::TypedVectorQueryProcessor> {
        let raster_source = self.raster_sources[0].query_processor()?;
        let point_source = self.vector_sources[0]
            .query_processor()?
            .multi_point()
            .expect("checked in initialization");
        Ok(TypedVectorQueryProcessor::MultiPoint(match raster_source {
            crate::engine::TypedRasterQueryProcessor::U8(r) => {
                Box::new(create_binary_raster_vector::<u8, MultiPointCollection>(
                    r,
                    point_source,
                    self.state.clone(),
                ))
            }
            _ => panic!(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use crate::{
        engine::{QueryRectangle, RasterOperator, RasterResultDescriptor},
        mock::{MockPointSource, MockPointSourceParams, MockRasterSource, MockRasterSourceParams},
    };
    use futures::executor::block_on_stream;
    use geoengine_datatypes::primitives::Measurement;
    use geoengine_datatypes::{
        primitives::SpatialResolution,
        primitives::{BoundingBox2D, Coordinate2D, FeatureDataRef, TimeInterval},
        raster::{Grid2D, RasterDataType, TileInformation},
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

        let raster = Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], None).unwrap();

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: Default::default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            raster,
        );

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
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
                        "tile_position": [0, 0],
                        "global_geo_transform": {
                            "origin_coordinate": {
                                "x": 0.0,
                                "y": 0.0
                            },
                            "x_pixel_size": 1.0,
                            "y_pixel_size": -1.0
                        },
                        "grid_array": {
                            "shape": {
                                "shape_array": [3, 2]
                            },
                            "data": [1, 2, 3, 4, 5, 6],
                            "no_data_value": null
                        }
                    }],
                    "result_descriptor": {
                        "data_type": "U8",
                        "spatial_reference": "EPSG:4326",
                        "measurement": "unitless"
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
                }
            }]
        })
        .to_string();
        assert_eq!(serialized, expected);
        let _operator: Box<dyn VectorOperator> = serde_json::from_str(&serialized).unwrap();
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn execute() {
        let points = vec![Coordinate2D::new(1., 2.); 3];
        let mps = MockPointSource {
            params: MockPointSourceParams { points },
        }
        .boxed();

        let raster = Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], None).unwrap();

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: Default::default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            raster,
        );

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
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

        let execution_context = MockExecutionContext::default();

        let initialized = op.initialize(&execution_context).unwrap();

        let point_processor = match initialized.query_processor() {
            Ok(TypedVectorQueryProcessor::MultiPoint(processor)) => processor,
            _ => panic!(),
        };

        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new((0., 0.).into(), (4., 4.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = MockQueryContext::new(2 * std::mem::size_of::<Coordinate2D>());

        let stream = point_processor.vector_query(query_rectangle, &ctx);

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
