use crate::adapters::FeatureCollectionStreamExt;
use crate::processing::raster_vector_join::create_feature_aggregator;
use futures::stream::{BoxStream, once as once_stream};
use futures::{StreamExt, TryStreamExt};
use geoengine_datatypes::primitives::{
    BandSelection, BoundingBox2D, CacheHint, ColumnSelection, FeatureDataType, Geometry,
    RasterQueryRectangle, VectorQueryRectangle,
};
use geoengine_datatypes::util::arrow::ArrowTyped;
use std::marker::PhantomData;
use std::sync::Arc;

use geoengine_datatypes::raster::{
    DynamicRasterDataType, GridIdx2D, GridIndexAccess, RasterTile2D,
};
use geoengine_datatypes::{
    collections::FeatureCollectionModifications, primitives::TimeInterval, raster::Pixel,
};

use super::util::{CoveredPixels, PixelCoverCreator};
use crate::engine::{
    QueryContext, QueryProcessor, RasterQueryProcessor, TypedRasterQueryProcessor,
    VectorQueryProcessor, VectorResultDescriptor,
};
use crate::util::Result;
use crate::{adapters::RasterStreamExt, error::Error};
use async_trait::async_trait;
use geoengine_datatypes::collections::GeometryCollection;
use geoengine_datatypes::collections::{FeatureCollection, FeatureCollectionInfos};

use super::aggregator::TypedAggregator;
use super::{FeatureAggregationMethod, RasterInput};

pub struct RasterVectorJoinProcessor<G> {
    collection: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
    result_descriptor: VectorResultDescriptor,
    raster_inputs: Vec<RasterInput>,
    aggregation_method: FeatureAggregationMethod,
    ignore_no_data: bool,
}

impl<G> RasterVectorJoinProcessor<G>
where
    G: Geometry + ArrowTyped + 'static,
    FeatureCollection<G>: GeometryCollection + PixelCoverCreator<G>,
{
    pub fn new(
        collection: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
        result_descriptor: VectorResultDescriptor,
        raster_inputs: Vec<RasterInput>,
        aggregation_method: FeatureAggregationMethod,
        ignore_no_data: bool,
    ) -> Self {
        Self {
            collection,
            result_descriptor,
            raster_inputs,
            aggregation_method,
            ignore_no_data,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn process_collections<'a>(
        collection: BoxStream<'a, Result<FeatureCollection<G>>>,
        raster_processor: &'a TypedRasterQueryProcessor,
        column_names: &'a [String],
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
        aggregation_method: FeatureAggregationMethod,
        ignore_no_data: bool,
    ) -> BoxStream<'a, Result<FeatureCollection<G>>> {
        let stream = collection.and_then(move |collection| {
            Self::process_collection_chunk(
                collection,
                raster_processor,
                column_names,
                query.clone(),
                ctx,
                aggregation_method,
                ignore_no_data,
            )
        });

        stream
            .try_flatten()
            .merge_chunks(ctx.chunk_byte_size().into())
            .boxed()
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_collection_chunk<'a>(
        collection: FeatureCollection<G>,
        raster_processor: &'a TypedRasterQueryProcessor,
        column_names: &'a [String],
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
        aggregation_method: FeatureAggregationMethod,
        ignore_no_data: bool,
    ) -> Result<BoxStream<'a, Result<FeatureCollection<G>>>> {
        if collection.is_empty() {
            tracing::debug!(
                "input collection is empty, returning empty collection, skipping raster query"
            );

            return Self::collection_with_new_null_columns(
                &collection,
                column_names,
                raster_processor.raster_data_type().into(),
            );
        }

        let bbox = collection
            .bbox()
            .and_then(|bbox| bbox.intersection(&query.spatial_bounds));

        let time = collection
            .time_bounds()
            .and_then(|time| time.intersect(&query.time_interval));

        // TODO: also intersect with raster spatial / time bounds

        let (Some(_spatial_bounds), Some(_time_interval)) = (bbox, time) else {
            tracing::debug!(
                "spatial or temporal intersection is empty, returning the same collection, skipping raster query"
            );

            return Self::collection_with_new_null_columns(
                &collection,
                column_names,
                raster_processor.raster_data_type().into(),
            );
        };

        let query = RasterQueryRectangle::from_qrect_and_bands(
            &query,
            BandSelection::first_n(column_names.len() as u32),
        );

        call_on_generic_raster_processor!(raster_processor, raster_processor => {
            Self::process_typed_collection_chunk(
                collection,
                raster_processor,
                column_names,
                query,
                ctx,
                aggregation_method,
                ignore_no_data,
            )
            .await
        })
    }

    fn collection_with_new_null_columns<'a>(
        collection: &FeatureCollection<G>,
        column_names: &'a [String],
        feature_data_type: FeatureDataType,
    ) -> Result<BoxStream<'a, Result<FeatureCollection<G>>>> {
        let feature_data = (0..column_names.len())
            .map(|_| feature_data_type.null_feature_data(collection.len()))
            .collect::<Vec<_>>();

        let columns = column_names
            .iter()
            .map(String::as_str)
            .zip(feature_data)
            .collect::<Vec<_>>();

        let collection = collection.add_columns(&columns)?;

        let collection_stream = once_stream(async move { Ok(collection) }).boxed();
        Ok(collection_stream)
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_typed_collection_chunk<'a, P: Pixel>(
        collection: FeatureCollection<G>,
        raster_processor: &'a dyn RasterQueryProcessor<RasterType = P>,
        column_names: &'a [String],
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
        aggregation_method: FeatureAggregationMethod,
        ignore_no_data: bool,
    ) -> Result<BoxStream<'a, Result<FeatureCollection<G>>>> {
        let raster_query = raster_processor.raster_query(query, ctx).await?;

        let collection = Arc::new(collection);

        let collection_stream = raster_query
            .time_multi_fold(
                move || {
                    Ok(VectorRasterJoiner::new(
                        column_names.len() as u32,
                        aggregation_method,
                        ignore_no_data,
                    ))
                },
                move |accum, raster| {
                    let collection = collection.clone();
                    async move {
                        let accum = accum?;
                        let raster = raster?;
                        accum.extract_raster_values(&collection, &raster)
                    }
                },
            )
            .map(move |accum| accum?.into_collection(column_names));

        Ok(collection_stream.boxed())
    }
}

struct JoinerState<G, C> {
    covered_pixels: C,
    feature_pixels: Option<Vec<Vec<GridIdx2D>>>,
    current_tile: GridIdx2D,
    current_band_idx: u32,
    aggregators: Vec<TypedAggregator>, // one aggregator per band
    g: PhantomData<G>,
}

struct VectorRasterJoiner<G, C> {
    state: Option<JoinerState<G, C>>,
    num_bands: u32,
    aggregation_method: FeatureAggregationMethod,
    ignore_no_data: bool,
    cache_hint: CacheHint,
}

impl<G, C> VectorRasterJoiner<G, C>
where
    G: Geometry + ArrowTyped + 'static,
    C: CoveredPixels<G>,
    FeatureCollection<G>: PixelCoverCreator<G, C = C>,
{
    fn new(
        num_bands: u32,
        aggregation_method: FeatureAggregationMethod,
        ignore_no_data: bool,
    ) -> Self {
        // TODO: is it possible to do the initialization here?

        Self {
            state: None,
            num_bands,
            aggregation_method,
            ignore_no_data,
            cache_hint: CacheHint::max_duration(),
        }
    }

    fn initialize<P: Pixel>(
        &mut self,
        collection: &FeatureCollection<G>,
        raster_time: &TimeInterval,
    ) -> Result<()> {
        // TODO: could be paralellized

        let (indexes, time_intervals): (Vec<_>, Vec<_>) = collection
            .time_intervals()
            .iter()
            .enumerate()
            .filter_map(|(i, time)| {
                time.intersect(raster_time)
                    .map(|time_intersection| (i, time_intersection))
            })
            .unzip();

        let mut valid = vec![false; collection.len()];
        for i in indexes {
            valid[i] = true;
        }

        let collection = collection.filter(valid)?;
        let collection = collection.replace_time(&time_intervals)?;

        self.state = Some(JoinerState::<G, C> {
            aggregators: (0..self.num_bands)
                .map(|_| {
                    create_feature_aggregator::<P>(
                        collection.len(),
                        self.aggregation_method,
                        self.ignore_no_data,
                    )
                })
                .collect(),
            covered_pixels: collection.create_covered_pixels(),
            feature_pixels: None,
            current_tile: [0, 0].into(),
            current_band_idx: 0,
            g: Default::default(),
        });

        Ok(())
    }

    fn extract_raster_values<P: Pixel>(
        mut self,
        initial_collection: &FeatureCollection<G>,
        raster: &RasterTile2D<P>,
    ) -> Result<Self> {
        let state = loop {
            if let Some(state) = &mut self.state {
                break state;
            }

            self.initialize::<P>(initial_collection, &raster.time)?;
        };
        let collection = &state.covered_pixels.collection_ref();
        let aggregator = &mut state.aggregators[raster.band as usize];
        let covered_pixels = &state.covered_pixels;

        if state.feature_pixels.is_some() && raster.tile_position == state.current_tile {
            // same tile as before, but a different band. We can re-use the covered pixels
            state.current_band_idx = raster.band;
            // state
            //     .feature_pixels
            //     .expect("feature_pixels should exist because we checked it above")
        } else {
            // first or new tile, we need to calculcate the covered pixels
            state.current_tile = raster.tile_position;
            state.current_band_idx = raster.band;

            state.feature_pixels = Some(
                (0..collection.len())
                    .map(|feature_index| covered_pixels.covered_pixels(feature_index, raster))
                    .collect::<Vec<_>>(),
            );
        }

        for (feature_index, feature_pixels) in state
            .feature_pixels
            .as_ref()
            .expect("should exist because it was calculated before")
            .iter()
            .enumerate()
        {
            for grid_idx in feature_pixels {
                let Ok(value) = raster.get_at_grid_index(*grid_idx) else {
                    continue; // not found in this raster tile
                };

                if let Some(data) = value {
                    aggregator.add_value(feature_index, data, 1);
                } else {
                    aggregator.add_null(feature_index);
                }
            }
        }

        self.cache_hint.merge_with(&raster.cache_hint);

        Ok(self)
    }

    fn into_collection(self, new_column_names: &[String]) -> Result<FeatureCollection<G>> {
        let Some(state) = self.state else {
            return Err(Error::EmptyInput); // TODO: maybe output empty dataset or just nulls
        };

        let columns = new_column_names
            .iter()
            .map(String::as_str)
            .zip(
                state
                    .aggregators
                    .into_iter()
                    .map(TypedAggregator::into_data),
            )
            .collect::<Vec<_>>();

        let mut new_collection = state.covered_pixels.collection().add_columns(&columns)?;

        new_collection.cache_hint = self.cache_hint;

        Ok(new_collection)
    }
}

#[async_trait]
impl<G> QueryProcessor for RasterVectorJoinProcessor<G>
where
    G: Geometry + ArrowTyped + 'static,
    FeatureCollection<G>: GeometryCollection + PixelCoverCreator<G>,
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
        let mut stream = self.collection.query(query.clone(), ctx).await?;

        // TODO: adjust raster bands to the vector attribute selection in the query once we support it
        for raster_input in &self.raster_inputs {
            tracing::debug!(
                "processing raster for new columns {:?}",
                raster_input.column_names
            );
            // TODO: spawn task
            stream = Self::process_collections(
                stream,
                &raster_input.processor,
                &raster_input.column_names,
                query.clone(),
                ctx,
                self.aggregation_method,
                self.ignore_no_data,
            );
        }

        Ok(stream)
    }

    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::{
        ChunkByteSize, MockExecutionContext, MockQueryContext, QueryProcessor,
        RasterBandDescriptor, RasterBandDescriptors, RasterOperator, RasterResultDescriptor,
        VectorColumnInfo, VectorOperator, WorkflowOperatorPath,
    };
    use crate::mock::{MockFeatureCollectionSource, MockRasterSource, MockRasterSourceParams};
    use crate::source::{GdalSource, GdalSourceParameters};
    use crate::util::gdal::add_ndvi_dataset;
    use geoengine_datatypes::collections::{
        ChunksEqualIgnoringCacheHint, MultiPointCollection, MultiPolygonCollection, VectorDataType,
    };
    use geoengine_datatypes::primitives::SpatialResolution;
    use geoengine_datatypes::primitives::{BoundingBox2D, DateTime, FeatureData, MultiPolygon};
    use geoengine_datatypes::primitives::{CacheHint, Measurement};
    use geoengine_datatypes::primitives::{MultiPoint, TimeInterval};
    use geoengine_datatypes::raster::{
        Grid2D, RasterDataType, TileInformation, TilingSpecification,
    };
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
    use geoengine_datatypes::util::test::TestDefault;

    #[tokio::test]
    async fn both_instant() {
        let time_instant =
            TimeInterval::new_instant(DateTime::new_utc(2014, 1, 1, 0, 0, 0)).unwrap();

        let points = MockFeatureCollectionSource::single(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![
                    vec![(-13.95, 20.05)],
                    vec![(-14.05, 20.05)],
                    vec![(-13.95, 19.95)],
                    vec![(-14.05, 19.95)],
                    vec![(-13.95, 19.95), (-14.05, 19.95)],
                ])
                .unwrap(),
                vec![time_instant; 5],
                Default::default(),
                CacheHint::default(),
            )
            .unwrap(),
        )
        .boxed();

        let mut execution_context = MockExecutionContext::test_default();

        let raster_source = GdalSource {
            params: GdalSourceParameters {
                data: add_ndvi_dataset(&mut execution_context),
            },
        }
        .boxed();

        let points = points
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let rasters = raster_source
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap();

        let processor = RasterVectorJoinProcessor::new(
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
                processor: rasters,
                column_names: vec!["ndvi".to_owned()],
            }],
            FeatureAggregationMethod::First,
            false,
        );

        let mut result = processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: time_instant,
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                    attributes: ColumnSelection::all(),
                },
                &MockQueryContext::new(ChunkByteSize::MAX),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let result = result.remove(0);

        assert!(
            result.chunks_equal_ignoring_cache_hint(
                &MultiPointCollection::from_slices(
                    &MultiPoint::many(vec![
                        vec![(-13.95, 20.05)],
                        vec![(-14.05, 20.05)],
                        vec![(-13.95, 19.95)],
                        vec![(-14.05, 19.95)],
                        vec![(-13.95, 19.95), (-14.05, 19.95)],
                    ])
                    .unwrap(),
                    &[time_instant; 5],
                    // these values are taken from loading the tiff in QGIS
                    &[("ndvi", FeatureData::Int(vec![54, 55, 51, 55, 51]))],
                )
                .unwrap()
            )
        );
    }

    #[tokio::test]
    async fn points_instant() {
        let points = MockFeatureCollectionSource::single(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![
                    (-13.95, 20.05),
                    (-14.05, 20.05),
                    (-13.95, 19.95),
                    (-14.05, 19.95),
                ])
                .unwrap(),
                vec![TimeInterval::new_instant(DateTime::new_utc(2014, 1, 1, 0, 0, 0)).unwrap(); 4],
                Default::default(),
                CacheHint::default(),
            )
            .unwrap(),
        )
        .boxed();

        let mut execution_context = MockExecutionContext::test_default();

        let raster_source = GdalSource {
            params: GdalSourceParameters {
                data: add_ndvi_dataset(&mut execution_context),
            },
        }
        .boxed();

        let points = points
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let rasters = raster_source
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap();

        let processor = RasterVectorJoinProcessor::new(
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
                processor: rasters,
                column_names: vec!["ndvi".to_owned()],
            }],
            FeatureAggregationMethod::First,
            false,
        );

        let mut result = processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::new(
                        DateTime::new_utc(2014, 1, 1, 0, 0, 0),
                        DateTime::new_utc(2014, 3, 1, 0, 0, 0),
                    )
                    .unwrap(),
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                    attributes: ColumnSelection::all(),
                },
                &MockQueryContext::new(ChunkByteSize::MAX),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let result = result.remove(0);

        assert!(
            result.chunks_equal_ignoring_cache_hint(
                &MultiPointCollection::from_slices(
                    &MultiPoint::many(vec![
                        (-13.95, 20.05),
                        (-14.05, 20.05),
                        (-13.95, 19.95),
                        (-14.05, 19.95),
                    ])
                    .unwrap(),
                    &[TimeInterval::new_instant(DateTime::new_utc(2014, 1, 1, 0, 0, 0)).unwrap();
                        4],
                    // these values are taken from loading the tiff in QGIS
                    &[("ndvi", FeatureData::Int(vec![54, 55, 51, 55]))],
                )
                .unwrap()
            )
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn raster_instant() {
        let points = MockFeatureCollectionSource::single(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![
                    (-13.95, 20.05),
                    (-14.05, 20.05),
                    (-13.95, 19.95),
                    (-14.05, 19.95),
                ])
                .unwrap(),
                vec![
                    TimeInterval::new(
                        DateTime::new_utc(2014, 1, 1, 0, 0, 0),
                        DateTime::new_utc(2014, 3, 1, 0, 0, 0),
                    )
                    .unwrap();
                    4
                ],
                Default::default(),
                CacheHint::default(),
            )
            .unwrap(),
        )
        .boxed();

        let mut execution_context = MockExecutionContext::test_default();

        let raster_source = GdalSource {
            params: GdalSourceParameters {
                data: add_ndvi_dataset(&mut execution_context),
            },
        }
        .boxed();

        let points = points
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let rasters = raster_source
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap();

        let processor = RasterVectorJoinProcessor::new(
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
                processor: rasters,
                column_names: vec!["ndvi".to_owned()],
            }],
            FeatureAggregationMethod::First,
            false,
        );

        let mut result = processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::new_instant(DateTime::new_utc(
                        2014, 1, 1, 0, 0, 0,
                    ))
                    .unwrap(),
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                    attributes: ColumnSelection::all(),
                },
                &MockQueryContext::new(ChunkByteSize::MAX),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let result = result.remove(0);

        assert!(
            result.chunks_equal_ignoring_cache_hint(
                &MultiPointCollection::from_slices(
                    &MultiPoint::many(vec![
                        (-13.95, 20.05),
                        (-14.05, 20.05),
                        (-13.95, 19.95),
                        (-14.05, 19.95),
                    ])
                    .unwrap(),
                    &[TimeInterval::new(
                        DateTime::new_utc(2014, 1, 1, 0, 0, 0),
                        DateTime::new_utc(2014, 2, 1, 0, 0, 0),
                    )
                    .unwrap(); 4],
                    // these values are taken from loading the tiff in QGIS
                    &[("ndvi", FeatureData::Int(vec![54, 55, 51, 55]))],
                )
                .unwrap()
            )
        );
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn both_ranges() {
        let points = MockFeatureCollectionSource::single(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![
                    (-13.95, 20.05),
                    (-14.05, 20.05),
                    (-13.95, 19.95),
                    (-14.05, 19.95),
                ])
                .unwrap(),
                vec![
                    TimeInterval::new(
                        DateTime::new_utc(2014, 1, 1, 0, 0, 0),
                        DateTime::new_utc(2014, 3, 1, 0, 0, 0),
                    )
                    .unwrap();
                    4
                ],
                Default::default(),
                CacheHint::default(),
            )
            .unwrap(),
        )
        .boxed();

        let mut execution_context = MockExecutionContext::test_default();

        let raster_source = GdalSource {
            params: GdalSourceParameters {
                data: add_ndvi_dataset(&mut execution_context),
            },
        }
        .boxed();

        let points = points
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let rasters = raster_source
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap();

        let processor = RasterVectorJoinProcessor::new(
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
                processor: rasters,
                column_names: vec!["ndvi".to_owned()],
            }],
            FeatureAggregationMethod::First,
            false,
        );

        let mut result = processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::new(
                        DateTime::new_utc(2014, 1, 1, 0, 0, 0),
                        DateTime::new_utc(2014, 3, 1, 0, 0, 0),
                    )
                    .unwrap(),
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                    attributes: ColumnSelection::all(),
                },
                &MockQueryContext::new(ChunkByteSize::MAX),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let result = result.remove(0);

        let t1 = TimeInterval::new(
            DateTime::new_utc(2014, 1, 1, 0, 0, 0),
            DateTime::new_utc(2014, 2, 1, 0, 0, 0),
        )
        .unwrap();
        let t2 = TimeInterval::new(
            DateTime::new_utc(2014, 2, 1, 0, 0, 0),
            DateTime::new_utc(2014, 3, 1, 0, 0, 0),
        )
        .unwrap();
        assert!(
            result.chunks_equal_ignoring_cache_hint(
                &MultiPointCollection::from_slices(
                    &MultiPoint::many(vec![
                        (-13.95, 20.05),
                        (-14.05, 20.05),
                        (-13.95, 19.95),
                        (-14.05, 19.95),
                        (-13.95, 20.05),
                        (-14.05, 20.05),
                        (-13.95, 19.95),
                        (-14.05, 19.95),
                    ])
                    .unwrap(),
                    &[t1, t1, t1, t1, t2, t2, t2, t2],
                    // these values are taken from loading the tiff in QGIS
                    &[(
                        "ndvi",
                        FeatureData::Int(vec![54, 55, 51, 55, 52, 55, 50, 53])
                    )],
                )
                .unwrap()
            )
        );
    }

    #[tokio::test]
    #[allow(clippy::float_cmp)]
    #[allow(clippy::too_many_lines)]
    async fn extract_raster_values_two_spatial_tiles_per_time_step_mean() {
        let raster_tile_a_0 = RasterTile2D::new_with_tile_info(
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
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new((0., 0.).into(), [3, 2].into()),
        );

        let raster = raster_source
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap();

        let points = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                vec![(0.0, 0.0), (2.0, 0.0)],
                vec![(1.0, 0.0), (3.0, 0.0)],
            ])
            .unwrap(),
            vec![TimeInterval::default(); 2],
            Default::default(),
            CacheHint::default(),
        )
        .unwrap();

        let points = MockFeatureCollectionSource::single(points).boxed();

        let points = points
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let processor = RasterVectorJoinProcessor::new(
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
                column_names: vec!["ndvi".to_owned()],
            }],
            FeatureAggregationMethod::Mean,
            false,
        );

        let mut result = processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((0.0, -3.0).into(), (4.0, 0.0).into())
                        .unwrap(),
                    time_interval: TimeInterval::new_unchecked(0, 20),
                    spatial_resolution: SpatialResolution::new(1., 1.).unwrap(),
                    attributes: ColumnSelection::all(),
                },
                &MockQueryContext::new(ChunkByteSize::MAX),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let result = result.remove(0);

        let t1 = TimeInterval::new(0, 10).unwrap();
        let t2 = TimeInterval::new(10, 20).unwrap();

        assert!(
            result.chunks_equal_ignoring_cache_hint(
                &MultiPointCollection::from_slices(
                    &MultiPoint::many(vec![
                        vec![(0.0, 0.0), (2.0, 0.0)],
                        vec![(1.0, 0.0), (3.0, 0.0)],
                        vec![(0.0, 0.0), (2.0, 0.0)],
                        vec![(1.0, 0.0), (3.0, 0.0)],
                    ])
                    .unwrap(),
                    &[t1, t1, t2, t2],
                    &[(
                        "ndvi",
                        FeatureData::Float(vec![
                            f64::midpoint(6., 60.),
                            f64::midpoint(5., 50.),
                            f64::midpoint(1., 10.),
                            f64::midpoint(2., 20.)
                        ])
                    )],
                )
                .unwrap()
            )
        );
    }

    #[tokio::test]
    #[allow(clippy::float_cmp)]
    #[allow(clippy::too_many_lines)]
    async fn polygons() {
        let raster_tile_a_0 = RasterTile2D::new_with_tile_info(
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
            Grid2D::new([3, 2].into(), vec![600, 500, 400, 300, 200, 100])
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
            Grid2D::new([3, 2].into(), vec![100, 200, 300, 400, 500, 600])
                .unwrap()
                .into(),
            CacheHint::default(),
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
                    data_type: RasterDataType::U16,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new((0., 0.).into(), [3, 2].into()),
        );

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
            vec![TimeInterval::default(); 1],
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

        let processor = RasterVectorJoinProcessor::new(
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
                column_names: vec!["ndvi".to_owned()],
            }],
            FeatureAggregationMethod::Mean,
            false,
        );

        let mut result = processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((0.0, -3.0).into(), (4.0, 0.0).into())
                        .unwrap(),
                    time_interval: TimeInterval::new_unchecked(0, 20),
                    spatial_resolution: SpatialResolution::new(1., 1.).unwrap(),
                    attributes: ColumnSelection::all(),
                },
                &MockQueryContext::new(ChunkByteSize::MAX),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPolygonCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let result = result.remove(0);

        let t1 = TimeInterval::new(0, 10).unwrap();
        let t2 = TimeInterval::new(10, 20).unwrap();

        assert!(
            result.chunks_equal_ignoring_cache_hint(
                &MultiPolygonCollection::from_slices(
                    &[
                        MultiPolygon::new(vec![vec![vec![
                            (0.5, -0.5).into(),
                            (4., -1.).into(),
                            (0.5, -2.5).into(),
                            (0.5, -0.5).into(),
                        ]]])
                        .unwrap(),
                        MultiPolygon::new(vec![vec![vec![
                            (0.5, -0.5).into(),
                            (4., -1.).into(),
                            (0.5, -2.5).into(),
                            (0.5, -0.5).into(),
                        ]]])
                        .unwrap()
                    ],
                    &[t1, t2],
                    &[(
                        "ndvi",
                        FeatureData::Float(vec![
                            (3. + 1. + 40. + 30. + 400.) / 5.,
                            (4. + 6. + 30. + 40. + 300.) / 5.
                        ])
                    )],
                )
                .unwrap()
            )
        );
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
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new(vec![
                        RasterBandDescriptor::new_unitless("band_0".into()),
                        RasterBandDescriptor::new_unitless("band_1".into()),
                    ])
                    .unwrap(),
                },
            },
        }
        .boxed();

        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new((0., 0.).into(), [3, 2].into()),
        );

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
            vec![TimeInterval::default(); 1],
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

        let processor = RasterVectorJoinProcessor::new(
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
        );

        let mut result = processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((0.0, -3.0).into(), (4.0, 0.0).into())
                        .unwrap(),
                    time_interval: TimeInterval::new_unchecked(0, 20),
                    spatial_resolution: SpatialResolution::new(1., 1.).unwrap(),
                    attributes: ColumnSelection::all(),
                },
                &MockQueryContext::new(ChunkByteSize::MAX),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPolygonCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let result = result.remove(0);

        let t1 = TimeInterval::new(0, 10).unwrap();
        let t2 = TimeInterval::new(10, 20).unwrap();

        assert!(
            result.chunks_equal_ignoring_cache_hint(
                &MultiPolygonCollection::from_slices(
                    &[
                        MultiPolygon::new(vec![vec![vec![
                            (0.5, -0.5).into(),
                            (4., -1.).into(),
                            (0.5, -2.5).into(),
                            (0.5, -0.5).into(),
                        ]]])
                        .unwrap(),
                        MultiPolygon::new(vec![vec![vec![
                            (0.5, -0.5).into(),
                            (4., -1.).into(),
                            (0.5, -2.5).into(),
                            (0.5, -0.5).into(),
                        ]]])
                        .unwrap()
                    ],
                    &[t1, t2],
                    &[
                        (
                            "foo",
                            FeatureData::Float(vec![
                                (3. + 1. + 40. + 30. + 400.) / 5.,
                                (4. + 6. + 30. + 40. + 300.) / 5.
                            ])
                        ),
                        (
                            "foo_1",
                            FeatureData::Float(vec![
                                (251. + 249. + 140. + 130. + 410.) / 5.,
                                (44. + 66. + 300. + 400. + 301.) / 5.
                            ])
                        )
                    ],
                )
                .unwrap()
            )
        );
    }
}
