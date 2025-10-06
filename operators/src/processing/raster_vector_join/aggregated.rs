use super::util::{CoveredPixels, FeatureTimeSpanIter, PixelCoverCreator};
use super::{FeatureAggregationMethod, RasterInput, create_feature_aggregator};
use crate::{
    engine::{
        QueryContext, QueryProcessor, RasterQueryProcessor, VectorQueryProcessor,
        VectorResultDescriptor,
    },
    processing::raster_vector_join::{
        TemporalAggregationMethod,
        aggregator::{
            Aggregator, FirstValueFloatAggregator, FirstValueIntAggregator, MeanValueAggregator,
            TypedAggregator,
        },
    },
    util::Result,
};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use geoengine_datatypes::raster::RasterTile2D;
use geoengine_datatypes::{
    collections::{FeatureCollection, FeatureCollectionInfos, FeatureCollectionModifications},
    primitives::{
        BandSelection, BoundingBox2D, CacheHint, ColumnSelection, Geometry, RasterQueryRectangle,
        VectorQueryRectangle,
    },
    raster::{GridIndexAccess, Pixel, RasterDataType},
    util::arrow::ArrowTyped,
};

pub struct RasterVectorAggregateJoinProcessor<G> {
    collection: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
    result_descriptor: VectorResultDescriptor,
    raster_inputs: Vec<RasterInput>,
    feature_aggregation: FeatureAggregationMethod,
    feature_aggregation_ignore_no_data: bool,
    temporal_aggregation: TemporalAggregationMethod,
    temporal_aggregation_ignore_no_data: bool,
}

impl<G> RasterVectorAggregateJoinProcessor<G>
where
    G: Geometry + ArrowTyped,
    FeatureCollection<G>: PixelCoverCreator<G>,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        collection: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
        result_descriptor: VectorResultDescriptor,
        raster_inputs: Vec<RasterInput>,
        feature_aggregation: FeatureAggregationMethod,
        feature_aggregation_ignore_no_data: bool,
        temporal_aggregation: TemporalAggregationMethod,
        temporal_aggregation_ignore_no_data: bool,
    ) -> Self {
        Self {
            collection,
            result_descriptor,
            raster_inputs,
            feature_aggregation,
            feature_aggregation_ignore_no_data,
            temporal_aggregation,
            temporal_aggregation_ignore_no_data,
        }
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)] // TODO: refactor to reduce arguments
    async fn extract_raster_values<P: Pixel>(
        collection: &FeatureCollection<G>,
        raster_processor: &dyn RasterQueryProcessor<RasterType = P, Output = RasterTile2D<P>>,
        column_names: &[String],
        feature_aggreation: FeatureAggregationMethod,
        feature_aggregation_ignore_no_data: bool,
        temporal_aggregation: TemporalAggregationMethod,
        temporal_aggregation_ignore_no_data: bool,
        query: VectorQueryRectangle,
        ctx: &dyn QueryContext,
    ) -> Result<FeatureCollection<G>> {
        let mut temporal_band_aggregators = (0..column_names.len())
            .map(|_| {
                Self::create_aggregator::<P>(
                    collection.len(),
                    temporal_aggregation,
                    temporal_aggregation_ignore_no_data,
                )
            })
            .collect::<Vec<_>>();

        let collection = collection.sort_by_time_asc()?;

        let covered_pixels = collection.create_covered_pixels();

        let collection = covered_pixels.collection_ref();

        let mut cache_hint = CacheHint::max_duration();

        let rd = raster_processor.raster_result_descriptor();

        for time_span in FeatureTimeSpanIter::new(collection.time_intervals()) {
            let spatial_bounds = query.spatial_bounds();

            let pixel_bounds = rd
                .tiling_grid_definition(ctx.tiling_specification())
                .tiling_geo_transform()
                .bounding_box_2d_to_intersecting_grid_bounds(&spatial_bounds);

            let raster_query = RasterQueryRectangle::new(
                pixel_bounds,
                time_span.time_interval,
                BandSelection::first(), // FIXME: this should prop. use all bands?
            );

            let mut rasters = raster_processor.raster_query(raster_query, ctx).await?;

            // TODO: optimize geo access (only specific tiles, etc.)

            let mut feature_band_aggregators = (0..column_names.len())
                .map(|_| {
                    create_feature_aggregator::<P>(
                        collection.len(),
                        feature_aggreation,
                        feature_aggregation_ignore_no_data,
                    )
                })
                .collect::<Vec<_>>();

            let mut time_end = None;

            while let Some(raster) = rasters.next().await {
                let raster = raster?;
                let band = raster.band as usize;

                if let Some(end) = time_end {
                    if end != raster.time.end() {
                        // new time slice => consume old aggregator and create new one

                        let new_feature_agg = create_feature_aggregator::<P>(
                            collection.len(),
                            feature_aggreation,
                            feature_aggregation_ignore_no_data,
                        );

                        let olg_feature_agg =
                            std::mem::replace(&mut feature_band_aggregators[band], new_feature_agg);

                        temporal_band_aggregators[band].add_feature_data(
                            olg_feature_agg.into_data(),
                            time_span.time_interval.duration_ms(), // TODO: use individual feature duration?
                        )?;

                        if temporal_band_aggregators[band].is_satisfied() {
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
                                feature_band_aggregators[band].add_value(feature_index, data, 1);
                            } else {
                                // TODO: weigh by area?
                                feature_band_aggregators[band].add_null(feature_index);
                            }

                            if feature_band_aggregators[band].is_satisfied() {
                                satisfied = true;
                                break;
                            }
                        }
                    }

                    if satisfied {
                        break;
                    }
                }

                cache_hint.merge_with(&raster.cache_hint);
            }
            for (band, feature_aggregator) in feature_band_aggregators.into_iter().enumerate() {
                temporal_band_aggregators[band].add_feature_data(
                    feature_aggregator.into_data(),
                    time_span.time_interval.duration_ms(), // TODO: use individual feature duration?
                )?;
            }

            if (0..column_names.len()).all(|band| temporal_band_aggregators[band].is_satisfied()) {
                break;
            }
        }

        let feature_data = temporal_band_aggregators
            .into_iter()
            .map(TypedAggregator::into_data);

        let columns = column_names
            .iter()
            .map(String::as_str)
            .zip(feature_data)
            .collect::<Vec<_>>();

        let mut new_collection = collection
            .add_columns(&columns)
            .map_err(Into::<crate::error::Error>::into)?;

        new_collection.cache_hint = cache_hint;

        Ok(new_collection)
    }

    fn create_aggregator<P: Pixel>(
        number_of_features: usize,
        aggregation: TemporalAggregationMethod,
        ignore_no_data: bool,
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
                    FirstValueIntAggregator::new(number_of_features, ignore_no_data).into_typed()
                }
                RasterDataType::F32 | RasterDataType::F64 => {
                    FirstValueFloatAggregator::new(number_of_features, ignore_no_data).into_typed()
                }
            },
            TemporalAggregationMethod::Mean => {
                MeanValueAggregator::new(number_of_features, ignore_no_data).into_typed()
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
    type Selection = ColumnSelection;
    type ResultDescription = VectorResultDescriptor;

    async fn _query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let stream = self
            .collection
            .query(query.clone(), ctx)
            .await?
            .and_then(move |mut collection| {
                let query = query.clone();
                async move {
                    for raster_input in &self.raster_inputs {
                        collection = call_on_generic_raster_processor!(&raster_input.processor, raster => {
                            Self::extract_raster_values(
                                &collection,
                                raster,
                                &raster_input.column_names,
                                self.feature_aggregation,
                                self.feature_aggregation_ignore_no_data,
                                self.temporal_aggregation,
                                self.temporal_aggregation_ignore_no_data,
                                query.clone(),
                                ctx
                            ).await?
                        });
                    }

                    Ok(collection)
                }
            })
            .boxed();

        Ok(stream)
    }

    fn result_descriptor(&self) -> &Self::ResultDescription {
        &self.result_descriptor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::{
        ChunkByteSize, MockExecutionContext, RasterBandDescriptor, RasterBandDescriptors,
        RasterOperator, RasterResultDescriptor, SpatialGridDescriptor, TimeDescriptor,
        VectorColumnInfo, VectorOperator, WorkflowOperatorPath,
    };
    use crate::mock::{MockFeatureCollectionSource, MockRasterSource, MockRasterSourceParams};
    use geoengine_datatypes::collections::{
        ChunksEqualIgnoringCacheHint, MultiPointCollection, MultiPolygonCollection, VectorDataType,
    };

    use geoengine_datatypes::primitives::{
        BoundingBox2D, CacheHint, Coordinate2D, FeatureData, FeatureDataRef, FeatureDataType,
        Measurement, MultiPoint, MultiPolygon, TimeInterval, TimeStep,
    };
    use geoengine_datatypes::raster::{
        GeoTransform, Grid2D, GridBoundingBox2D, RasterTile2D, TileInformation, TilingSpecification,
    };
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
    use geoengine_datatypes::util::test::TestDefault;

    #[tokio::test]
    async fn extract_raster_values_single_raster() {
        let raster_tile = RasterTile2D::<u8>::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
            CacheHint::default(),
        );

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_irregular(Some(TimeInterval::default())),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
                GridBoundingBox2D::new([0, 0], [2, 1]).unwrap(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let raster_source = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: result_descriptor.clone(),
            },
        }
        .boxed();

        let execution_context =
            MockExecutionContext::new_with_tiling_spec(TilingSpecification::new([3, 2].into()));

        let raster_source = raster_source
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

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
            CacheHint::default(),
        )
        .unwrap();

        let result = RasterVectorAggregateJoinProcessor::extract_raster_values(
            &points,
            &raster_source.query_processor().unwrap().get_u8().unwrap(),
            &["foo".to_string()],
            FeatureAggregationMethod::First,
            false,
            TemporalAggregationMethod::First,
            false,
            VectorQueryRectangle::new(
                BoundingBox2D::new((0.0, -3.0).into(), (2.0, 0.).into()).unwrap(),
                Default::default(),
                ColumnSelection::all(),
            ),
            &execution_context.mock_query_context(ChunkByteSize::MIN),
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
            0,
            Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1])
                .unwrap()
                .into(),
            CacheHint::default(),
        );
        let raster_tile_b = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
            CacheHint::default(),
        );

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_regular_with_epoch(
                Some(TimeInterval::new_unchecked(0, 20)),
                TimeStep::millis(10),
            ),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
                GridBoundingBox2D::new([0, 0], [2, 1]).unwrap(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let raster_source = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile_a, raster_tile_b],
                result_descriptor: result_descriptor.clone(),
            },
        }
        .boxed();

        let execution_context =
            MockExecutionContext::new_with_tiling_spec(TilingSpecification::new([3, 2].into()));

        let raster_source = raster_source
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

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
            vec![TimeInterval::new(0, 20).unwrap(); 6],
            Default::default(),
            CacheHint::default(),
        )
        .unwrap();

        let result = RasterVectorAggregateJoinProcessor::extract_raster_values(
            &points,
            &raster_source.query_processor().unwrap().get_u8().unwrap(),
            &["foo".to_string()],
            FeatureAggregationMethod::First,
            false,
            TemporalAggregationMethod::Mean,
            false,
            VectorQueryRectangle::new(
                BoundingBox2D::new((0.0, -3.0).into(), (2.0, 0.0).into()).unwrap(),
                TimeInterval::new(0, 20).unwrap(),
                ColumnSelection::all(),
            ),
            &execution_context.mock_query_context(ChunkByteSize::MIN),
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
    #[allow(clippy::float_cmp, clippy::too_many_lines)]
    async fn extract_raster_values_two_spatial_tiles_per_time_step() {
        let raster_tile_a_0 = RasterTile2D::<u8>::new_with_tile_info(
            TimeInterval::new(0, 10).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1])
                .unwrap()
                .into(),
            CacheHint::default(),
        );
        let raster_tile_a_1 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(0, 10).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 1].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![60, 50, 40, 30, 20, 10])
                .unwrap()
                .into(),
            CacheHint::default(),
        );
        let raster_tile_b_0 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
            CacheHint::default(),
        );
        let raster_tile_b_1 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 1].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![10, 20, 30, 40, 50, 60])
                .unwrap()
                .into(),
            CacheHint::default(),
        );

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_regular_with_epoch(
                Some(TimeInterval::new_unchecked(0, 20)),
                TimeStep::millis(10),
            ),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
                GridBoundingBox2D::new([0, 0], [2, 3]).unwrap(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let raster_source = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![
                    raster_tile_a_0,
                    raster_tile_a_1,
                    raster_tile_b_0,
                    raster_tile_b_1,
                ],
                result_descriptor: result_descriptor.clone(),
            },
        }
        .boxed();

        let execution_context =
            MockExecutionContext::new_with_tiling_spec(TilingSpecification::new([3, 2].into()));

        let raster_source = raster_source
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let points = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                vec![(0.0, 0.0), (2.0, 0.0)],
                vec![(1.0, 0.0), (3.0, 0.0)],
            ])
            .unwrap(),
            vec![TimeInterval::new(0, 20).unwrap(); 2],
            Default::default(),
            CacheHint::default(),
        )
        .unwrap();

        let result = RasterVectorAggregateJoinProcessor::extract_raster_values(
            &points,
            &raster_source.query_processor().unwrap().get_u8().unwrap(),
            &["foo".to_string()],
            FeatureAggregationMethod::Mean,
            false,
            TemporalAggregationMethod::Mean,
            false,
            VectorQueryRectangle::new(
                BoundingBox2D::new((0.0, -3.0).into(), (4.0, 0.0).into()).unwrap(),
                TimeInterval::new(0, 20).unwrap(),
                ColumnSelection::all(),
            ),
            &execution_context.mock_query_context(ChunkByteSize::MIN),
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
            0,
            Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1])
                .unwrap()
                .into(),
            CacheHint::default(),
        );
        let raster_tile_a_1 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(0, 10).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 1].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![60, 50, 40, 30, 20, 10])
                .unwrap()
                .into(),
            CacheHint::default(),
        );
        let raster_tile_a_2 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(0, 10).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 2].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![160, 150, 140, 130, 120, 110])
                .unwrap()
                .into(),
            CacheHint::default(),
        );
        let raster_tile_b_0 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
            CacheHint::default(),
        );
        let raster_tile_b_1 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 1].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![10, 20, 30, 40, 50, 60])
                .unwrap()
                .into(),
            CacheHint::default(),
        );
        let raster_tile_b_2 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 2].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![110, 120, 130, 140, 150, 160])
                .unwrap()
                .into(),
            CacheHint::default(),
        );

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_regular_with_epoch(
                Some(TimeInterval::new_unchecked(0, 20)),
                TimeStep::millis(10),
            ),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
                GridBoundingBox2D::new([0, 0], [2, 5]).unwrap(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

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
                result_descriptor: result_descriptor.clone(),
            },
        }
        .boxed();

        let execution_context =
            MockExecutionContext::new_with_tiling_spec(TilingSpecification::new([3, 2].into()));

        let raster_source = raster_source
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let polygons = MultiPolygonCollection::from_data(
            vec![
                MultiPolygon::new(vec![vec![vec![
                    (0.5, -0.5).into(),
                    (4., -1.).into(),
                    (0.5, -2.5).into(),
                    (0.5, -0.5).into(),
                ]]])
                .unwrap(),
            ],
            vec![TimeInterval::new(0, 20).unwrap(); 1],
            Default::default(),
            CacheHint::default(),
        )
        .unwrap();

        let result = RasterVectorAggregateJoinProcessor::extract_raster_values(
            &polygons,
            &raster_source.query_processor().unwrap().get_u8().unwrap(),
            &["foo".to_string()],
            FeatureAggregationMethod::Mean,
            false,
            TemporalAggregationMethod::Mean,
            false,
            VectorQueryRectangle::new(
                BoundingBox2D::new((0.0, -3.0).into(), (4.0, 0.0).into()).unwrap(),
                TimeInterval::new(0, 20).unwrap(),
                ColumnSelection::all(),
            ),
            &execution_context.mock_query_context(ChunkByteSize::MIN),
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

    #[tokio::test]
    #[allow(clippy::float_cmp)]
    #[allow(clippy::too_many_lines)]
    async fn polygons_multi_band() {
        let raster_tile_a_0_band_0 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(0, 10).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1])
                .unwrap()
                .into(),
            CacheHint::default(),
        );
        let raster_tile_a_0_band_1 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(0, 10).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            1,
            Grid2D::new([3, 2].into(), vec![255, 254, 253, 251, 250, 249])
                .unwrap()
                .into(),
            CacheHint::default(),
        );

        let raster_tile_a_1_band_0 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(0, 10).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 1].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![60, 50, 40, 30, 20, 10])
                .unwrap()
                .into(),
            CacheHint::default(),
        );
        let raster_tile_a_1_band_1 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(0, 10).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 1].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            1,
            Grid2D::new([3, 2].into(), vec![160, 150, 140, 130, 120, 110])
                .unwrap()
                .into(),
            CacheHint::default(),
        );

        let raster_tile_a_2_band_0 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(0, 10).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 2].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![600, 500, 400, 300, 200, 100])
                .unwrap()
                .into(),
            CacheHint::default(),
        );
        let raster_tile_a_2_band_1 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(0, 10).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 2].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            1,
            Grid2D::new([3, 2].into(), vec![610, 510, 410, 310, 210, 110])
                .unwrap()
                .into(),
            CacheHint::default(),
        );

        let raster_tile_b_0_band_0 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
            CacheHint::default(),
        );
        let raster_tile_b_0_band_1 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            1,
            Grid2D::new([3, 2].into(), vec![11, 22, 33, 44, 55, 66])
                .unwrap()
                .into(),
            CacheHint::default(),
        );
        let raster_tile_b_1_band_0 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 1].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![10, 20, 30, 40, 50, 60])
                .unwrap()
                .into(),
            CacheHint::default(),
        );
        let raster_tile_b_1_band_1 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 1].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            1,
            Grid2D::new([3, 2].into(), vec![100, 220, 300, 400, 500, 600])
                .unwrap()
                .into(),
            CacheHint::default(),
        );

        let raster_tile_b_2_band_0 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 2].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![100, 200, 300, 400, 500, 600])
                .unwrap()
                .into(),
            CacheHint::default(),
        );
        let raster_tile_b_2_band_1 = RasterTile2D::new_with_tile_info(
            TimeInterval::new(10, 20).unwrap(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 2].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            1,
            Grid2D::new([3, 2].into(), vec![101, 201, 301, 401, 501, 601])
                .unwrap()
                .into(),
            CacheHint::default(),
        );

        let raster_source = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![
                    raster_tile_a_0_band_0,
                    raster_tile_a_0_band_1,
                    raster_tile_a_1_band_0,
                    raster_tile_a_1_band_1,
                    raster_tile_a_2_band_0,
                    raster_tile_a_2_band_1,
                    raster_tile_b_0_band_0,
                    raster_tile_b_0_band_1,
                    raster_tile_b_1_band_0,
                    raster_tile_b_1_band_1,
                    raster_tile_b_2_band_0,
                    raster_tile_b_2_band_1,
                ],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U16,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: TimeDescriptor::new_regular_with_epoch(
                        Some(TimeInterval::new_unchecked(0, 20)),
                        TimeStep::millis(10),
                    ),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        TestDefault::test_default(),
                        GridBoundingBox2D::new_min_max(0, 2, 0, 5).unwrap(),
                    ),
                    bands: RasterBandDescriptors::new(vec![
                        RasterBandDescriptor::new_unitless("band_0".into()),
                        RasterBandDescriptor::new_unitless("band_1".into()),
                    ])
                    .unwrap(),
                },
            },
        }
        .boxed();

        let execution_context =
            MockExecutionContext::new_with_tiling_spec(TilingSpecification::new([3, 2].into()));

        let raster = raster_source
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap();

        let polygons = MultiPolygonCollection::from_data(
            vec![
                MultiPolygon::new(vec![vec![vec![
                    (0.5, -0.5).into(),
                    (4., -1.).into(),
                    (0.5, -2.5).into(),
                    (0.5, -0.5).into(),
                ]]])
                .unwrap(),
            ],
            vec![TimeInterval::new(0, 20).unwrap(); 1],
            Default::default(),
            CacheHint::default(),
        )
        .unwrap();

        let polygons = MockFeatureCollectionSource::single(polygons).boxed();

        let points = polygons
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .multi_polygon()
            .unwrap();

        let processor = RasterVectorAggregateJoinProcessor::new(
            points,
            VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                columns: [(
                    "ndvi".to_string(),
                    VectorColumnInfo {
                        data_type: FeatureDataType::Int,
                        measurement: Measurement::Unitless,
                    },
                )]
                .into_iter()
                .collect(),
                time: None,
                bbox: None,
            },
            vec![RasterInput {
                processor: raster,
                column_names: vec!["foo".to_owned(), "foo_1".to_owned()],
            }],
            FeatureAggregationMethod::Mean,
            false,
            TemporalAggregationMethod::Mean,
            false,
        );

        let mut result = processor
            .query(
                VectorQueryRectangle::new(
                    BoundingBox2D::new((0.0, -3.0).into(), (4.0, 0.0).into()).unwrap(),
                    TimeInterval::new_unchecked(0, 20),
                    ColumnSelection::all(),
                ),
                &execution_context.mock_query_context(ChunkByteSize::MAX),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPolygonCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let result = result.remove(0);

        assert!(
            result.chunks_equal_ignoring_cache_hint(
                &MultiPolygonCollection::from_slices(
                    &[MultiPolygon::new(vec![vec![vec![
                        (0.5, -0.5).into(),
                        (4., -1.).into(),
                        (0.5, -2.5).into(),
                        (0.5, -0.5).into(),
                    ]]])
                    .unwrap(),],
                    &[TimeInterval::new(0, 20).unwrap()],
                    &[
                        (
                            "foo",
                            FeatureData::Float(vec![f64::midpoint(
                                (3. + 1. + 40. + 30. + 400.) / 5.,
                                (4. + 6. + 30. + 40. + 300.) / 5.
                            )])
                        ),
                        (
                            "foo_1",
                            FeatureData::Float(vec![f64::midpoint(
                                (251. + 249. + 140. + 130. + 410.) / 5.,
                                (44. + 66. + 300. + 400. + 301.) / 5.
                            )])
                        )
                    ],
                )
                .unwrap()
            )
        );
    }
}
