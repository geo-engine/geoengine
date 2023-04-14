use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};

use geoengine_datatypes::collections::{
    FeatureCollection, FeatureCollectionInfos, FeatureCollectionModifications,
};
use geoengine_datatypes::raster::{GridIndexAccess, Pixel, RasterDataType};
use geoengine_datatypes::util::arrow::ArrowTyped;

use crate::engine::{
    QueryContext, QueryProcessor, RasterQueryProcessor, TypedRasterQueryProcessor,
    VectorQueryProcessor,
};
use crate::processing::raster_vector_join::aggregator::{
    Aggregator, FirstValueFloatAggregator, FirstValueIntAggregator, MeanValueAggregator,
    TypedAggregator,
};
use crate::processing::raster_vector_join::TemporalAggregationMethod;
use crate::util::Result;
use async_trait::async_trait;
use geoengine_datatypes::primitives::{BoundingBox2D, Geometry, VectorQueryRectangle};

use super::util::{CoveredPixels, FeatureTimeSpanIter, PixelCoverCreator};
use super::{create_feature_aggregator, FeatureAggregationMethod};

pub struct RasterVectorAggregateJoinProcessor<G> {
    collection: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
    raster_processors: Vec<TypedRasterQueryProcessor>,
    column_names: Vec<String>,
    feature_aggregation: FeatureAggregationMethod,
    temporal_aggregation: TemporalAggregationMethod,
}

impl<G> RasterVectorAggregateJoinProcessor<G>
where
    G: Geometry + ArrowTyped,
    FeatureCollection<G>: PixelCoverCreator<G>,
{
    pub fn new(
        collection: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
        raster_processors: Vec<TypedRasterQueryProcessor>,
        column_names: Vec<String>,
        feature_aggregation: FeatureAggregationMethod,
        temporal_aggregation: TemporalAggregationMethod,
    ) -> Self {
        Self {
            collection,
            raster_processors,
            column_names,
            feature_aggregation,
            temporal_aggregation,
        }
    }

    async fn extract_raster_values<P: Pixel>(
        collection: &FeatureCollection<G>,
        raster_processor: &dyn RasterQueryProcessor<RasterType = P>,
        new_column_name: &str,
        feature_aggreation: FeatureAggregationMethod,
        temporal_aggregation: TemporalAggregationMethod,
        query: VectorQueryRectangle,
        ctx: &dyn QueryContext,
    ) -> Result<FeatureCollection<G>> {
        let mut temporal_aggregator =
            Self::create_aggregator::<P>(collection.len(), temporal_aggregation);

        let collection = collection.sort_by_time_asc()?;

        let covered_pixels = collection.create_covered_pixels();

        let collection = covered_pixels.collection_ref();

        for time_span in FeatureTimeSpanIter::new(collection.time_intervals()) {
            let query = VectorQueryRectangle {
                spatial_bounds: query.spatial_bounds,
                time_interval: time_span.time_interval,
                spatial_resolution: query.spatial_resolution,
            };

            let mut rasters = raster_processor.raster_query(query.into(), ctx).await?;

            // TODO: optimize geo access (only specific tiles, etc.)

            let mut feature_aggregator =
                create_feature_aggregator::<P>(collection.len(), feature_aggreation);

            let mut time_end = None;

            while let Some(raster) = rasters.next().await {
                let raster = raster?;

                if let Some(end) = time_end {
                    if end != raster.time.end() {
                        // new time slice => consume old aggregator and create new one
                        temporal_aggregator.add_feature_data(
                            feature_aggregator.into_data(),
                            time_span.time_interval.duration_ms(), // TODO: use individual feature duration?
                        )?;

                        feature_aggregator =
                            create_feature_aggregator::<P>(collection.len(), feature_aggreation);

                        if temporal_aggregator.is_satisfied() {
                            break;
                        }
                    }
                }
                time_end = Some(raster.time.end());

                for feature_index in time_span.feature_index_start..=time_span.feature_index_end {
                    // TODO: don't do random access but use a single iterator
                    let mut satisfied = false;
                    for grid_idx in covered_pixels.covered_pixels(feature_index, &raster) {
                        // try to get the pixel if the coordinate is within the current tile
                        if let Ok(pixel) = raster.get_at_grid_index(grid_idx) {
                            // finally, attach value to feature
                            if let Some(data) = pixel {
                                feature_aggregator.add_value(feature_index, data, 1);
                            } else {
                                // TODO: weigh by area?
                                feature_aggregator.add_null(feature_index);
                            }

                            if feature_aggregator.is_satisfied() {
                                satisfied = true;
                                break;
                            }
                        }
                    }

                    if satisfied {
                        break;
                    }
                }
            }

            temporal_aggregator.add_feature_data(
                feature_aggregator.into_data(),
                time_span.time_interval.duration_ms(), // TODO: use individual feature duration?
            )?;

            if temporal_aggregator.is_satisfied() {
                break;
            }
        }

        collection
            .add_column(new_column_name, temporal_aggregator.into_data())
            .map_err(Into::into)
    }

    fn create_aggregator<P: Pixel>(
        number_of_features: usize,
        aggregation: TemporalAggregationMethod,
    ) -> TypedAggregator {
        match aggregation {
            TemporalAggregationMethod::First => match P::TYPE {
                RasterDataType::U8
                | RasterDataType::U16
                | RasterDataType::U32
                | RasterDataType::U64
                | RasterDataType::I8
                | RasterDataType::I16
                | RasterDataType::I32
                | RasterDataType::I64 => {
                    FirstValueIntAggregator::new(number_of_features).into_typed()
                }
                RasterDataType::F32 | RasterDataType::F64 => {
                    FirstValueFloatAggregator::new(number_of_features).into_typed()
                }
            },
            TemporalAggregationMethod::Mean => {
                MeanValueAggregator::new(number_of_features).into_typed()
            }
            TemporalAggregationMethod::None => {
                unreachable!("this type of aggregator does not lead to this kind of processor")
            }
        }
    }
}

#[async_trait]
impl<G> QueryProcessor for RasterVectorAggregateJoinProcessor<G>
where
    G: Geometry + ArrowTyped + 'static,
    FeatureCollection<G>: PixelCoverCreator<G>,
{
    type Output = FeatureCollection<G>;
    type SpatialBounds = BoundingBox2D;

    async fn _query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let stream = self.collection
            .query(query, ctx).await?
            .and_then(move |mut collection| async move {

                for (raster, new_column_name) in self.raster_processors.iter().zip(&self.column_names) {
                    collection = call_on_generic_raster_processor!(raster, raster => {
                        Self::extract_raster_values(&collection, raster, new_column_name, self.feature_aggregation, self.temporal_aggregation, query, ctx).await?
                    });
                }

                Ok(collection)
            })
            .boxed();

        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::{ChunkByteSize, MockExecutionContext, RasterResultDescriptor};
    use crate::engine::{MockQueryContext, RasterOperator};
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use geoengine_datatypes::collections::{MultiPointCollection, MultiPolygonCollection};
    use geoengine_datatypes::primitives::MultiPolygon;
    use geoengine_datatypes::raster::{Grid2D, RasterTile2D, TileInformation};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_datatypes::{
        primitives::{
            BoundingBox2D, FeatureDataRef, Measurement, MultiPoint, SpatialResolution, TimeInterval,
        },
        raster::TilingSpecification,
    };

    #[tokio::test]
    async fn extract_raster_values_single_raster() {
        let raster_tile = RasterTile2D::<u8>::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
        );

        let raster_source = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
            },
        }
        .boxed();

        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new((0., 0.).into(), [3, 2].into()),
        );

        let raster_source = raster_source.initialize(Default::default(), &execution_context).await.unwrap();

        let points = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                (0.0, 0.0),
                (1.0, 0.0),
                (0.0, -1.0),
                (1.0, -1.0),
                (0.0, -2.0),
                (1.0, -2.0),
            ])
            .unwrap(),
            vec![TimeInterval::default(); 6],
            Default::default(),
        )
        .unwrap();

        let result = RasterVectorAggregateJoinProcessor::extract_raster_values(
            &points,
            &raster_source.query_processor().unwrap().get_u8().unwrap(),
            "foo",
            FeatureAggregationMethod::First,
            TemporalAggregationMethod::First,
            VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new((0.0, -3.0).into(), (2.0, 0.).into()).unwrap(),
                time_interval: Default::default(),
                spatial_resolution: SpatialResolution::new(1., 1.).unwrap(),
            },
            &MockQueryContext::new(ChunkByteSize::MIN),
        )
        .await
        .unwrap();

        if let FeatureDataRef::Int(extracted_data) = result.data("foo").unwrap() {
            assert_eq!(extracted_data.as_ref(), &[1, 2, 3, 4, 5, 6]);
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    #[allow(clippy::float_cmp)]
    async fn extract_raster_values_two_raster_timesteps() {
        let raster_tile_a = RasterTile2D::<u8>::new_with_tile_info(
            TimeInterval::new(0, 10).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1])
                .unwrap()
                .into(),
        );
        let raster_tile_b = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
        );

        let raster_source = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile_a, raster_tile_b],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
            },
        }
        .boxed();

        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new((0., 0.).into(), [3, 2].into()),
        );

        let raster_source = raster_source.initialize(Default::default(), &execution_context).await.unwrap();

        let points = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                (0.0, 0.0),
                (1.0, 0.0),
                (0.0, -1.0),
                (1.0, -1.0),
                (0.0, -2.0),
                (1.0, -2.0),
            ])
            .unwrap(),
            vec![TimeInterval::default(); 6],
            Default::default(),
        )
        .unwrap();

        let result = RasterVectorAggregateJoinProcessor::extract_raster_values(
            &points,
            &raster_source.query_processor().unwrap().get_u8().unwrap(),
            "foo",
            FeatureAggregationMethod::First,
            TemporalAggregationMethod::Mean,
            VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new((0.0, -3.0).into(), (2.0, 0.0).into()).unwrap(),
                time_interval: Default::default(),
                spatial_resolution: SpatialResolution::new(1., 1.).unwrap(),
            },
            &MockQueryContext::new(ChunkByteSize::MIN),
        )
        .await
        .unwrap();

        if let FeatureDataRef::Float(extracted_data) = result.data("foo").unwrap() {
            assert_eq!(extracted_data.as_ref(), &[3.5, 3.5, 3.5, 3.5, 3.5, 3.5]);
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    #[allow(clippy::float_cmp)]
    async fn extract_raster_values_two_spatial_tiles_per_time_step() {
        let raster_tile_a_0 = RasterTile2D::<u8>::new_with_tile_info(
            TimeInterval::new(0, 10).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1])
                .unwrap()
                .into(),
        );
        let raster_tile_a_1 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(0, 10).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 1].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![60, 50, 40, 30, 20, 10])
                .unwrap()
                .into(),
        );
        let raster_tile_b_0 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
        );
        let raster_tile_b_1 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 1].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![10, 20, 30, 40, 50, 60])
                .unwrap()
                .into(),
        );

        let raster_source = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![
                    raster_tile_a_0,
                    raster_tile_a_1,
                    raster_tile_b_0,
                    raster_tile_b_1,
                ],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
            },
        }
        .boxed();

        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new((0., 0.).into(), [3, 2].into()),
        );

        let raster_source = raster_source.initialize(Default::default(), &execution_context).await.unwrap();

        let points = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                vec![(0.0, 0.0), (2.0, 0.0)],
                vec![(1.0, 0.0), (3.0, 0.0)],
            ])
            .unwrap(),
            vec![TimeInterval::default(); 2],
            Default::default(),
        )
        .unwrap();

        let result = RasterVectorAggregateJoinProcessor::extract_raster_values(
            &points,
            &raster_source.query_processor().unwrap().get_u8().unwrap(),
            "foo",
            FeatureAggregationMethod::Mean,
            TemporalAggregationMethod::Mean,
            VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new((0.0, -3.0).into(), (4.0, 0.0).into()).unwrap(),
                time_interval: Default::default(),
                spatial_resolution: SpatialResolution::new(1., 1.).unwrap(),
            },
            &MockQueryContext::new(ChunkByteSize::MIN),
        )
        .await
        .unwrap();

        if let FeatureDataRef::Float(extracted_data) = result.data("foo").unwrap() {
            assert_eq!(
                extracted_data.as_ref(),
                &[(6. + 60. + 1. + 10.) / 4., (5. + 50. + 2. + 20.) / 4.]
            );
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::float_cmp)]
    async fn polygons() {
        let raster_tile_a_0 = RasterTile2D::<u8>::new_with_tile_info(
            TimeInterval::new(0, 10).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1])
                .unwrap()
                .into(),
        );
        let raster_tile_a_1 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(0, 10).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 1].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![60, 50, 40, 30, 20, 10])
                .unwrap()
                .into(),
        );
        let raster_tile_a_2 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(0, 10).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 2].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![160, 150, 140, 130, 120, 110])
                .unwrap()
                .into(),
        );
        let raster_tile_b_0 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
        );
        let raster_tile_b_1 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 1].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![10, 20, 30, 40, 50, 60])
                .unwrap()
                .into(),
        );
        let raster_tile_b_2 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 2].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![110, 120, 130, 140, 150, 160])
                .unwrap()
                .into(),
        );

        let raster_source = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![
                    raster_tile_a_0,
                    raster_tile_a_1,
                    raster_tile_a_2,
                    raster_tile_b_0,
                    raster_tile_b_1,
                    raster_tile_b_2,
                ],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
            },
        }
        .boxed();

        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new((0., 0.).into(), [3, 2].into()),
        );

        let raster_source = raster_source.initialize(Default::default(), &execution_context).await.unwrap();

        let polygons = MultiPolygonCollection::from_data(
            vec![MultiPolygon::new(vec![vec![vec![
                (0.5, -0.5).into(),
                (4., -1.).into(),
                (0.5, -2.5).into(),
                (0.5, -0.5).into(),
            ]]])
            .unwrap()],
            vec![TimeInterval::default(); 1],
            Default::default(),
        )
        .unwrap();

        let result = RasterVectorAggregateJoinProcessor::extract_raster_values(
            &polygons,
            &raster_source.query_processor().unwrap().get_u8().unwrap(),
            "foo",
            FeatureAggregationMethod::Mean,
            TemporalAggregationMethod::Mean,
            VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new((0.0, -3.0).into(), (4.0, 0.0).into()).unwrap(),
                time_interval: Default::default(),
                spatial_resolution: SpatialResolution::new(1., 1.).unwrap(),
            },
            &MockQueryContext::new(ChunkByteSize::MIN),
        )
        .await
        .unwrap();

        if let FeatureDataRef::Float(extracted_data) = result.data("foo").unwrap() {
            assert_eq!(
                extracted_data.as_ref(),
                &[(3. + 1. + 40. + 30. + 140. + 4. + 6. + 30. + 40. + 130.) / 10.]
            );
        } else {
            unreachable!();
        }
    }
}
