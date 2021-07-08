use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};

use geoengine_datatypes::collections::{
    FeatureCollectionInfos, FeatureCollectionModifications, GeometryRandomAccess,
    MultiPointCollection,
};
use geoengine_datatypes::raster::{GridIndexAccess, NoDataValue, Pixel, RasterDataType};

use crate::engine::{
    QueryContext, QueryProcessor, RasterQueryProcessor, TypedRasterQueryProcessor,
    VectorQueryProcessor, VectorQueryRectangle,
};
use crate::processing::raster_vector_join::per_feature_aggregator::{
    FirstValueFloatPerFeatureAggregator, FirstValueIntPerFeatureAggregator,
    MeanValuePerFeatureAggregator, PerFeatureAggregator, TypedPerFeatureAggregator,
};
use crate::processing::raster_vector_join::util::FeatureTimeSpanIter;
use crate::processing::raster_vector_join::TemporalAggregationMethod;
use crate::util::Result;
use async_trait::async_trait;
use geoengine_datatypes::primitives::{BoundingBox2D, MultiPointAccess};

use super::aggregator::{
    AggregationResult, FirstValueFloatAggregator, FirstValueIntAggregator, MeanValueAggregator,
    TypedAggregator,
};
use super::util::FeatureTimeSpan;
use super::FeatureAggregationMethod;
use crate::processing::raster_vector_join::aggregator::Aggregator;

pub struct RasterPointAggregateJoinProcessor {
    points: Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
    raster_processors: Vec<TypedRasterQueryProcessor>,
    column_names: Vec<String>,
    feature_aggregation: FeatureAggregationMethod,
    temporal_aggregation: TemporalAggregationMethod,
}

impl RasterPointAggregateJoinProcessor {
    pub fn new(
        points: Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
        raster_processors: Vec<TypedRasterQueryProcessor>,
        column_names: Vec<String>,
        feature_aggregation: FeatureAggregationMethod,
        temporal_aggregation: TemporalAggregationMethod,
    ) -> Self {
        Self {
            points,
            raster_processors,
            column_names,
            feature_aggregation,
            temporal_aggregation,
        }
    }

    async fn extract_raster_values<P: Pixel>(
        points: &MultiPointCollection,
        raster_processor: &dyn RasterQueryProcessor<RasterType = P>,
        new_column_name: &str,
        feature_aggregation: FeatureAggregationMethod,
        temporal_aggregation: TemporalAggregationMethod,
        query: VectorQueryRectangle,
        ctx: &dyn QueryContext,
    ) -> Result<MultiPointCollection> {
        let mut temporal_aggregator =
            Self::create_temporal_aggregator::<P>(points.len(), temporal_aggregation);

        let points = points.sort_by_time_asc()?;

        for time_span in FeatureTimeSpanIter::new(points.time_intervals()) {
            let query = VectorQueryRectangle {
                spatial_bounds: query.spatial_bounds,
                time_interval: time_span.time_interval,
                spatial_resolution: query.spatial_resolution,
            };

            let mut rasters = raster_processor.raster_query(query.into(), ctx).await?;

            // TODO: optimize geo access (only specific tiles, etc.)

            while let Some(raster) = rasters.next().await {
                let raster = raster?;
                let geo_transform = raster.tile_information().tile_geo_transform();

                for feature_index in time_span.feature_index_start..=time_span.feature_index_end {
                    // TODO: don't do random access but use a single iterator
                    let geometry = points.geometry_at(feature_index).expect("must exist");

                    let mut feature_aggregator =
                        Self::create_feature_aggregator::<P>(feature_aggregation);
                    for coordinate in geometry.points() {
                        let grid_idx = geo_transform.coordinate_to_grid_idx_2d(*coordinate);
                        // try to get the pixel if the coordinate is within the current tile
                        if let Ok(pixel) = raster.get_at_grid_index(grid_idx) {
                            // finally, attach value to feature
                            let is_no_data = raster
                                .no_data_value()
                                .map_or(false, |no_data| pixel == no_data);

                            if is_no_data {
                                feature_aggregator.add_null();
                            } else {
                                feature_aggregator
                                    .add_value(pixel, time_span.time_interval.duration_ms() as u64);
                            }

                            if feature_aggregator.is_satisfied() {
                                break;
                            }
                        }
                    }

                    if feature_aggregator.is_satisfied() {
                        fill_temporal_aggregator(
                            feature_aggregator,
                            &mut temporal_aggregator,
                            feature_index,
                            &time_span,
                        );
                    }
                }

                if temporal_aggregator.is_satisfied() {
                    break;
                }
            }
        }

        points
            .add_column(new_column_name, temporal_aggregator.into_data())
            .map_err(Into::into)
    }

    fn create_temporal_aggregator<P: Pixel>(
        number_of_features: usize,
        aggregation: TemporalAggregationMethod,
    ) -> TypedPerFeatureAggregator {
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
                    FirstValueIntPerFeatureAggregator::new(number_of_features).into_typed()
                }
                RasterDataType::F32 | RasterDataType::F64 => {
                    FirstValueFloatPerFeatureAggregator::new(number_of_features).into_typed()
                }
            },
            TemporalAggregationMethod::Mean => {
                MeanValuePerFeatureAggregator::new(number_of_features).into_typed()
            }
            TemporalAggregationMethod::None => {
                unreachable!("this type of aggregator does not lead to this kind of processor")
            }
        }
    }

    fn create_feature_aggregator<P: Pixel>(
        aggregation: FeatureAggregationMethod,
    ) -> TypedAggregator {
        match aggregation {
            FeatureAggregationMethod::First => match P::TYPE {
                RasterDataType::U8
                | RasterDataType::U16
                | RasterDataType::U32
                | RasterDataType::U64
                | RasterDataType::I8
                | RasterDataType::I16
                | RasterDataType::I32
                | RasterDataType::I64 => FirstValueIntAggregator::new().into_typed(),
                RasterDataType::F32 | RasterDataType::F64 => {
                    FirstValueFloatAggregator::new().into_typed()
                }
            },
            FeatureAggregationMethod::Mean => MeanValueAggregator::new().into_typed(),
        }
    }
}

fn fill_temporal_aggregator(
    feature_aggregator: TypedAggregator,
    temporal_aggregator: &mut TypedPerFeatureAggregator,
    feature_index: usize,
    time_span: &FeatureTimeSpan,
) {
    if let Some(result) = feature_aggregator.result() {
        match result {
            AggregationResult::Int(r) => temporal_aggregator.add_value(
                feature_index,
                r,
                time_span.time_interval.duration_ms() as u64,
            ),
            AggregationResult::Float(r) => temporal_aggregator.add_value(
                feature_index,
                r,
                time_span.time_interval.duration_ms() as u64,
            ),
        }
    } else {
        temporal_aggregator.add_null(feature_index);
    }
}

#[async_trait]
impl QueryProcessor for RasterPointAggregateJoinProcessor {
    type Output = MultiPointCollection;
    type SpatialBounds = BoundingBox2D;

    async fn query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let stream = self.points
            .query(query, ctx).await?
            .and_then(async move |mut points| {

                for (raster, new_column_name) in self.raster_processors.iter().zip(&self.column_names) {
                    points = call_on_generic_raster_processor!(raster, raster => {
                        Self::extract_raster_values(&points, raster, new_column_name, self.feature_aggregation, self.temporal_aggregation, query, ctx).await?
                    });
                }

                Ok(points)
            })
            .boxed();

        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::{MockExecutionContext, RasterResultDescriptor, VectorQueryRectangle};
    use crate::engine::{MockQueryContext, RasterOperator};
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use geoengine_datatypes::raster::{Grid2D, RasterTile2D, TileInformation};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::{
        primitives::{
            BoundingBox2D, FeatureDataRef, Measurement, MultiPoint, SpatialResolution, TimeInterval,
        },
        raster::TilingSpecification,
    };

    #[tokio::test]
    async fn extract_raster_values_single_raster() {
        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: Default::default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], None)
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
                    no_data_value: None,
                },
            },
        }
        .boxed();

        let execution_context = MockExecutionContext {
            tiling_specification: TilingSpecification::new((0., 0.).into(), [3, 2].into()),
            ..Default::default()
        };

        let raster_source = raster_source.initialize(&execution_context).await.unwrap();

        let points = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                vec![(0.0, 0.0)],
                vec![(1.0, 0.0)],
                vec![(0.0, -1.0)],
                vec![(1.0, -1.0)],
                vec![(0.0, -2.0)],
                vec![(1.0, -2.0)],
                vec![(0.0, -2.0), (1.0, -2.0)],
            ])
            .unwrap(),
            vec![TimeInterval::default(); 7],
            Default::default(),
        )
        .unwrap();

        let result = RasterPointAggregateJoinProcessor::extract_raster_values(
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
            &MockQueryContext::new(0),
        )
        .await
        .unwrap();

        if let FeatureDataRef::Int(extracted_data) = result.data("foo").unwrap() {
            assert_eq!(extracted_data.as_ref(), &[1, 2, 3, 4, 5, 6, 5]);
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    #[allow(clippy::float_cmp)]
    async fn extract_raster_values_two_raster_timesteps() {
        let raster_tile_a = RasterTile2D::new_with_tile_info(
            TimeInterval::new(0, 10).unwrap(),
            TileInformation {
                global_geo_transform: Default::default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1], None)
                .unwrap()
                .into(),
        );
        let raster_tile_b = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: Default::default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], None)
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
                    no_data_value: None,
                },
            },
        }
        .boxed();

        let execution_context = MockExecutionContext {
            tiling_specification: TilingSpecification::new((0., 0.).into(), [3, 2].into()),
            ..Default::default()
        };

        let raster_source = raster_source.initialize(&execution_context).await.unwrap();

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

        let result = RasterPointAggregateJoinProcessor::extract_raster_values(
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
            &MockQueryContext::new(0),
        )
        .await
        .unwrap();

        if let FeatureDataRef::Float(extracted_data) = result.data("foo").unwrap() {
            assert_eq!(extracted_data.as_ref(), &[3.5, 3.5, 3.5, 3.5, 3.5, 3.5]);
        } else {
            unreachable!();
        }
    }
}
