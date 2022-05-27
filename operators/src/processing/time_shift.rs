use crate::engine::{
    ExecutionContext, InitializedRasterOperator, InitializedVectorOperator, Operator, QueryContext,
    RasterOperator, RasterQueryProcessor, RasterResultDescriptor, SingleRasterOrVectorSource,
    TypedRasterQueryProcessor, TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor,
    VectorResultDescriptor,
};
use crate::util::input::RasterOrVectorOperator;
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::collections::{
    FeatureCollection, FeatureCollectionInfos, FeatureCollectionModifications,
};
use geoengine_datatypes::error::{BoxedResultExt, ErrorSource};
use geoengine_datatypes::primitives::{Geometry, RasterQueryRectangle, TimeInterval};
use geoengine_datatypes::primitives::{TimeStep, VectorQueryRectangle};
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use geoengine_datatypes::util::arrow::ArrowTyped;
use serde::{Deserialize, Serialize};
use snafu::Snafu;

/// Project the query rectangle to a new time interval.
pub type TimeShift = Operator<TimeShiftParams, SingleRasterOrVectorSource>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum TimeShiftParams {
    /// Shift the query rectangle relative with a time step
    Relative {
        step: TimeStep,
        direction: RelativeShiftDirection,
    },
    /// Set the time interval to a fixed value
    Absolute { time_interval: TimeInterval },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Copy)]
#[serde(rename_all = "camelCase")]
pub enum RelativeShiftDirection {
    Forward,
    Backward,
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)), module(error))]
pub enum TimeShiftError {
    #[snafu(display("Output type must match the type of the source"))]
    UnmatchedOutput,
    #[snafu(display("Modifying the timestamps of the feature collection failed"))]
    FeatureCollectionTimeModifcation { source: Box<dyn ErrorSource> },
}

#[typetag::serde]
#[async_trait]
impl VectorOperator for TimeShift {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        match (self.sources.source, self.params) {
            (
                RasterOrVectorOperator::Vector(source),
                TimeShiftParams::Relative { step, direction },
            ) => Ok(Box::new(InitializedRelativeVectorTimeShift {
                source: source.initialize(context).await?,
                step,
                direction,
            })),
            (
                RasterOrVectorOperator::Vector(source),
                TimeShiftParams::Absolute { time_interval },
            ) => Ok(Box::new(InitializedAbsoluteVectorTimeShift {
                source: source.initialize(context).await?,
                time_interval,
            })),
            (RasterOrVectorOperator::Raster(_), _) => Err(TimeShiftError::UnmatchedOutput.into()),
        }
    }
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for TimeShift {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        match (self.sources.source, self.params) {
            (
                RasterOrVectorOperator::Raster(source),
                TimeShiftParams::Relative { step, direction },
            ) => Ok(Box::new(InitializedRelativeRasterTimeShift {
                source: source.initialize(context).await?,
                step,
                direction,
            })),
            (
                RasterOrVectorOperator::Raster(source),
                TimeShiftParams::Absolute { time_interval },
            ) => Ok(Box::new(InitializedAbsoluteRasterTimeShift {
                source: source.initialize(context).await?,
                time_interval,
            })),
            (RasterOrVectorOperator::Vector(_), _) => Err(TimeShiftError::UnmatchedOutput.into()),
        }
    }
}

pub struct InitializedRelativeVectorTimeShift {
    source: Box<dyn InitializedVectorOperator>,
    step: TimeStep,
    direction: RelativeShiftDirection,
}

pub struct InitializedAbsoluteVectorTimeShift {
    source: Box<dyn InitializedVectorOperator>,
    time_interval: TimeInterval,
}

pub struct InitializedRelativeRasterTimeShift {
    source: Box<dyn InitializedRasterOperator>,
    step: TimeStep,
    direction: RelativeShiftDirection,
}

pub struct InitializedAbsoluteRasterTimeShift {
    source: Box<dyn InitializedRasterOperator>,
    time_interval: TimeInterval,
}

impl InitializedVectorOperator for InitializedRelativeVectorTimeShift {
    fn result_descriptor(&self) -> &VectorResultDescriptor {
        self.source.result_descriptor()
    }

    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        let source_processor = self.source.query_processor()?;

        Ok(
            call_on_generic_vector_processor!(source_processor, processor => VectorRelativeTimeShiftProcessor {
                processor,
                step: self.step,
                direction: self.direction,
            }.boxed().into()),
        )
    }
}

impl InitializedVectorOperator for InitializedAbsoluteVectorTimeShift {
    fn result_descriptor(&self) -> &VectorResultDescriptor {
        self.source.result_descriptor()
    }

    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        let source_processor = self.source.query_processor()?;

        Ok(
            call_on_generic_vector_processor!(source_processor, processor => VectorAbsoluteTimeShiftProcessor {
                processor,
                time_interval: self.time_interval,
            }.boxed().into()),
        )
    }
}

impl InitializedRasterOperator for InitializedRelativeRasterTimeShift {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        self.source.result_descriptor()
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source_processor = self.source.query_processor()?;

        Ok(
            call_on_generic_raster_processor!(source_processor, processor => RasterRelativeTimeShiftProcessor {
                processor,
                step: self.step,
                direction: self.direction,
            }.boxed().into()),
        )
    }
}

impl InitializedRasterOperator for InitializedAbsoluteRasterTimeShift {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        self.source.result_descriptor()
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source_processor = self.source.query_processor()?;

        Ok(
            call_on_generic_raster_processor!(source_processor, processor => RasterAbsoluteTimeShiftProcessor {
                processor,
                time_interval: self.time_interval,
            }.boxed().into()),
        )
    }
}

pub struct RasterRelativeTimeShiftProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    processor: Q,
    step: TimeStep,
    direction: RelativeShiftDirection,
}

pub struct RasterAbsoluteTimeShiftProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    processor: Q,
    time_interval: TimeInterval,
}

pub struct VectorRelativeTimeShiftProcessor<Q, G>
where
    G: Geometry,
    Q: VectorQueryProcessor<VectorType = FeatureCollection<G>>,
{
    processor: Q,
    step: TimeStep,
    direction: RelativeShiftDirection,
}

pub struct VectorAbsoluteTimeShiftProcessor<Q, G>
where
    G: Geometry,
    Q: VectorQueryProcessor<VectorType = FeatureCollection<G>>,
{
    processor: Q,
    time_interval: TimeInterval,
}

#[async_trait]
impl<Q, G> VectorQueryProcessor for VectorRelativeTimeShiftProcessor<Q, G>
where
    G: Geometry + ArrowTyped + 'static,
    Q: VectorQueryProcessor<VectorType = FeatureCollection<G>>,
{
    type VectorType = FeatureCollection<G>;

    async fn vector_query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::VectorType>>> {
        let time_interval = match self.direction {
            RelativeShiftDirection::Forward => query.time_interval + self.step,
            RelativeShiftDirection::Backward => query.time_interval - self.step,
        }?;
        let query = VectorQueryRectangle {
            spatial_bounds: query.spatial_bounds,
            time_interval,
            spatial_resolution: query.spatial_resolution,
        };
        let stream = self.processor.vector_query(query, ctx).await?;

        let stream = stream.map(|collection| {
            let collection = collection?;

            let time_intervals = collection
                .time_intervals()
                .iter()
                .map(|time| {
                    let start = match self.direction {
                        RelativeShiftDirection::Forward => time.start() - self.step,
                        RelativeShiftDirection::Backward => time.start() + self.step,
                    }?;
                    let end = match self.direction {
                        RelativeShiftDirection::Forward => time.end() - self.step,
                        RelativeShiftDirection::Backward => time.end() + self.step,
                    }?;
                    TimeInterval::new(start, end)
                        .boxed_context(error::FeatureCollectionTimeModifcation)
                        .map_err(Into::into)
                })
                .collect::<Result<Vec<TimeInterval>>>()?;

            collection
                .replace_time(&time_intervals)
                .boxed_context(error::FeatureCollectionTimeModifcation)
                .map_err(Into::into)
        });

        Ok(stream.boxed())
    }
}

#[async_trait]
impl<Q, G> VectorQueryProcessor for VectorAbsoluteTimeShiftProcessor<Q, G>
where
    G: Geometry + ArrowTyped + 'static,
    Q: VectorQueryProcessor<VectorType = FeatureCollection<G>>,
{
    type VectorType = FeatureCollection<G>;

    async fn vector_query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::VectorType>>> {
        let time_start_difference = query.time_interval.start() - self.time_interval.start();
        let time_end_difference = query.time_interval.end() - self.time_interval.end();

        let query = VectorQueryRectangle {
            spatial_bounds: query.spatial_bounds,
            time_interval: self.time_interval,
            spatial_resolution: query.spatial_resolution,
        };
        let stream = self.processor.vector_query(query, ctx).await?;

        let stream = stream.map(move |collection| {
            let collection = collection?;

            let time_intervals = collection
                .time_intervals()
                .iter()
                .map(|time| {
                    TimeInterval::new(
                        time.start() + time_start_difference.num_milliseconds(),
                        time.end() + time_end_difference.num_milliseconds(),
                    )
                    .boxed_context(error::FeatureCollectionTimeModifcation)
                    .map_err(Into::into)
                })
                .collect::<Result<Vec<TimeInterval>>>()?;

            collection
                .replace_time(&time_intervals)
                .boxed_context(error::FeatureCollectionTimeModifcation)
                .map_err(Into::into)
        });

        Ok(stream.boxed())
    }
}

#[async_trait]
impl<Q, P> RasterQueryProcessor for RasterRelativeTimeShiftProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    type RasterType = P;

    async fn raster_query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<Self::RasterType>>>> {
        let time_interval = match self.direction {
            RelativeShiftDirection::Forward => query.time_interval + self.step,
            RelativeShiftDirection::Backward => query.time_interval - self.step,
        }?;
        let query = RasterQueryRectangle {
            spatial_bounds: query.spatial_bounds,
            time_interval,
            spatial_resolution: query.spatial_resolution,
        };
        let stream = self.processor.raster_query(query, ctx).await?;

        let stream = stream.map(|raster| {
            // reverse time shift for results
            let mut raster = raster?;

            raster.time = match self.direction {
                RelativeShiftDirection::Forward => raster.time - self.step,
                RelativeShiftDirection::Backward => raster.time + self.step,
            }?;

            Ok(raster)
        });

        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl<Q, P> RasterQueryProcessor for RasterAbsoluteTimeShiftProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    type RasterType = P;

    async fn raster_query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<Self::RasterType>>>> {
        let time_start_difference = query.time_interval.start() - self.time_interval.start();
        let time_end_difference = query.time_interval.end() - self.time_interval.end();

        let query = RasterQueryRectangle {
            spatial_bounds: query.spatial_bounds,
            time_interval: self.time_interval,
            spatial_resolution: query.spatial_resolution,
        };

        let stream = self.processor.raster_query(query, ctx).await?;

        let stream = stream.map(move |raster| {
            // reverse time shift for results
            let mut raster = raster?;

            raster.time = TimeInterval::new(
                raster.time.start() + time_start_difference.num_milliseconds(),
                raster.time.end() + time_end_difference.num_milliseconds(),
            )?;

            Ok(raster)
        });

        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        engine::{MockExecutionContext, MockQueryContext},
        mock::{MockFeatureCollectionSource, MockRasterSource, MockRasterSourceParams},
        processing::{Expression, ExpressionParams, ExpressionSources},
        source::{GdalSource, GdalSourceParameters},
        util::gdal::add_ndvi_dataset,
    };
    use futures::StreamExt;
    use geoengine_datatypes::{
        collections::MultiPointCollection,
        dataset::InternalDatasetId,
        primitives::{
            BoundingBox2D, DateTime, Measurement, MultiPoint, SpatialPartition2D,
            SpatialResolution, TimeGranularity,
        },
        raster::{EmptyGrid2D, GridOrEmpty, RasterDataType, TileInformation, TilingSpecification},
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };
    use num_traits::AsPrimitive;

    #[test]
    fn test_ser_de_absolute() {
        let time_shift = TimeShift {
            sources: SingleRasterOrVectorSource {
                source: RasterOrVectorOperator::Raster(
                    GdalSource {
                        params: GdalSourceParameters {
                            dataset: InternalDatasetId::from_u128(1337).into(),
                        },
                    }
                    .boxed(),
                ),
            },
            params: TimeShiftParams::Absolute {
                time_interval: TimeInterval::new_unchecked(
                    DateTime::new_utc(2011, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2012, 1, 1, 0, 0, 0),
                ),
            },
        };

        let serialized = serde_json::to_value(&time_shift).unwrap();

        assert_eq!(
            serialized,
            serde_json::json!({
                "params": {
                    "type": "absolute",
                    "time_interval": {
                        "start": 1_293_840_000_000_i64,
                        "end": 1_325_376_000_000_i64
                    }
                },
                "sources": {
                    "source": {
                        "type": "GdalSource",
                        "params": {
                            "dataset": {
                                "type": "internal",
                                "datasetId": "00000000-0000-0000-0000-000000000539"
                            }
                        }
                    }
                }
            })
        );

        let deserialized: TimeShift = serde_json::from_value(serialized).unwrap();

        assert_eq!(time_shift.params, deserialized.params);
    }

    #[test]
    fn test_ser_de_relative() {
        let time_shift = TimeShift {
            sources: SingleRasterOrVectorSource {
                source: RasterOrVectorOperator::Raster(
                    GdalSource {
                        params: GdalSourceParameters {
                            dataset: InternalDatasetId::from_u128(1337).into(),
                        },
                    }
                    .boxed(),
                ),
            },
            params: TimeShiftParams::Relative {
                step: TimeStep {
                    granularity: TimeGranularity::Years,
                    step: 1,
                },
                direction: RelativeShiftDirection::Forward,
            },
        };

        let serialized = serde_json::to_value(&time_shift).unwrap();

        assert_eq!(
            serialized,
            serde_json::json!({
                "params": {
                    "type": "relative",
                    "step": {
                        "granularity": "years",
                        "step": 1
                    },
                    "direction": "forward"
                },
                "sources": {
                    "source": {
                        "type": "GdalSource",
                        "params": {
                            "dataset": {
                                "type": "internal",
                                "datasetId": "00000000-0000-0000-0000-000000000539"
                            }
                        }
                    }
                }
            })
        );

        let deserialized: TimeShift = serde_json::from_value(serialized).unwrap();

        assert_eq!(time_shift.params, deserialized.params);
    }

    #[tokio::test]
    async fn test_absolute_vector_shift() {
        let execution_context = MockExecutionContext::test_default();
        let query_context = MockQueryContext::test_default();

        let source = MockFeatureCollectionSource::single(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![(0., 0.), (1., 1.), (2., 2.)]).unwrap(),
                vec![
                    TimeInterval::new(
                        DateTime::new_utc(2009, 1, 1, 0, 0, 0),
                        DateTime::new_utc_with_millis(2010, 12, 31, 23, 59, 59, 999),
                    )
                    .unwrap(),
                    TimeInterval::new(
                        DateTime::new_utc(2009, 6, 3, 0, 0, 0),
                        DateTime::new_utc(2010, 7, 14, 0, 0, 0),
                    )
                    .unwrap(),
                    TimeInterval::new(
                        DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                        DateTime::new_utc_with_millis(2011, 3, 31, 23, 59, 59, 999),
                    )
                    .unwrap(),
                ],
                Default::default(),
            )
            .unwrap(),
        );

        let time_shift = TimeShift {
            sources: SingleRasterOrVectorSource {
                source: RasterOrVectorOperator::Vector(source.boxed()),
            },
            params: TimeShiftParams::Absolute {
                time_interval: TimeInterval::new(
                    DateTime::new_utc(2009, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2009, 6, 1, 0, 0, 0),
                )
                .unwrap(),
            },
        };

        let query_processor = VectorOperator::boxed(time_shift)
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let mut stream = query_processor
            .vector_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((0., 0.).into(), (2., 2.).into()).unwrap(),
                    time_interval: TimeInterval::new(
                        DateTime::new_utc(2009, 1, 1, 0, 0, 0),
                        DateTime::new_utc(2012, 1, 1, 0, 0, 0),
                    )
                    .unwrap(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &query_context,
            )
            .await
            .unwrap();

        let mut result = Vec::new();
        while let Some(collection) = stream.next().await {
            result.push(collection.unwrap());
        }

        assert_eq!(result.len(), 1);

        let expected = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0., 0.)]).unwrap(),
            vec![TimeInterval::new(
                DateTime::new_utc(2009, 1, 1, 0, 0, 0),
                DateTime::new_utc_with_millis(2013, 8, 1, 23, 59, 59, 999),
            )
            .unwrap()],
            Default::default(),
        )
        .unwrap();

        assert_eq!(result[0], expected);
    }

    #[tokio::test]
    async fn test_relative_vector_shift() {
        let execution_context = MockExecutionContext::test_default();
        let query_context = MockQueryContext::test_default();

        let source = MockFeatureCollectionSource::single(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![(0., 0.), (1., 1.), (2., 2.)]).unwrap(),
                vec![
                    TimeInterval::new(
                        DateTime::new_utc(2009, 1, 1, 0, 0, 0),
                        DateTime::new_utc_with_millis(2010, 12, 31, 23, 59, 59, 999),
                    )
                    .unwrap(),
                    TimeInterval::new(
                        DateTime::new_utc(2009, 6, 3, 0, 0, 0),
                        DateTime::new_utc(2010, 7, 14, 0, 0, 0),
                    )
                    .unwrap(),
                    TimeInterval::new(
                        DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                        DateTime::new_utc_with_millis(2011, 3, 31, 23, 59, 59, 999),
                    )
                    .unwrap(),
                ],
                Default::default(),
            )
            .unwrap(),
        );

        let time_shift = TimeShift {
            sources: SingleRasterOrVectorSource {
                source: RasterOrVectorOperator::Vector(source.boxed()),
            },
            params: TimeShiftParams::Relative {
                step: TimeStep {
                    granularity: TimeGranularity::Years,
                    step: 1,
                },
                direction: RelativeShiftDirection::Backward,
            },
        };

        let query_processor = VectorOperator::boxed(time_shift)
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let mut stream = query_processor
            .vector_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((0., 0.).into(), (2., 2.).into()).unwrap(),
                    time_interval: TimeInterval::new(
                        DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                        DateTime::new_utc(2011, 1, 1, 0, 0, 0),
                    )
                    .unwrap(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &query_context,
            )
            .await
            .unwrap();

        let mut result = Vec::new();
        while let Some(collection) = stream.next().await {
            result.push(collection.unwrap());
        }

        assert_eq!(result.len(), 1);

        let expected = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0., 0.), (1., 1.)]).unwrap(),
            vec![
                TimeInterval::new(
                    DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                    DateTime::new_utc_with_millis(2011, 12, 31, 23, 59, 59, 999),
                )
                .unwrap(),
                TimeInterval::new(
                    DateTime::new_utc(2010, 6, 3, 0, 0, 0),
                    DateTime::new_utc(2011, 7, 14, 0, 0, 0),
                )
                .unwrap(),
            ],
            Default::default(),
        )
        .unwrap();

        assert_eq!(result[0], expected);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_absolute_raster_shift() {
        let no_data_value: u8 = 0;
        let empty_grid = GridOrEmpty::Empty(EmptyGrid2D::new([3, 2].into(), no_data_value));
        let raster_tiles = vec![
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(
                    DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2011, 1, 1, 0, 0, 0),
                ),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                empty_grid.clone(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(
                    DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2011, 1, 1, 0, 0, 0),
                ),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                empty_grid.clone(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(
                    DateTime::new_utc(2011, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2012, 1, 1, 0, 0, 0),
                ),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                empty_grid.clone(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(
                    DateTime::new_utc(2011, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2012, 1, 1, 0, 0, 0),
                ),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                empty_grid.clone(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(
                    DateTime::new_utc(2012, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2013, 1, 1, 0, 0, 0),
                ),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                empty_grid.clone(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(
                    DateTime::new_utc(2012, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2013, 1, 1, 0, 0, 0),
                ),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                empty_grid.clone(),
            ),
        ];

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: Some(no_data_value.as_()),
                },
            },
        }
        .boxed();

        let time_shift = TimeShift {
            sources: SingleRasterOrVectorSource {
                source: RasterOrVectorOperator::Raster(mrs),
            },
            params: TimeShiftParams::Absolute {
                time_interval: TimeInterval::new_unchecked(
                    DateTime::new_utc(2011, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2012, 1, 1, 0, 0, 0),
                ),
            },
        };

        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new((0., 0.).into(), [3, 2].into()),
        );
        let query_context = MockQueryContext::test_default();

        let query_processor = RasterOperator::boxed(time_shift)
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let mut stream = query_processor
            .raster_query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 3.).into(),
                        (4., 0.).into(),
                    ),
                    time_interval: TimeInterval::new(
                        DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                        DateTime::new_utc(2011, 1, 1, 0, 0, 0),
                    )
                    .unwrap(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &query_context,
            )
            .await
            .unwrap();

        let mut result = Vec::new();
        while let Some(tile) = stream.next().await {
            result.push(tile.unwrap());
        }

        assert_eq!(result.len(), 2);

        assert_eq!(
            result[0].time,
            TimeInterval::new_unchecked(
                DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                DateTime::new_utc(2011, 1, 1, 0, 0, 0),
            ),
        );
        assert_eq!(
            result[1].time,
            TimeInterval::new_unchecked(
                DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                DateTime::new_utc(2011, 1, 1, 0, 0, 0),
            ),
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_relative_raster_shift() {
        let no_data_value: u8 = 0;
        let empty_grid = GridOrEmpty::Empty(EmptyGrid2D::new([3, 2].into(), no_data_value));
        let raster_tiles = vec![
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(
                    DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2011, 1, 1, 0, 0, 0),
                ),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                empty_grid.clone(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(
                    DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2011, 1, 1, 0, 0, 0),
                ),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                empty_grid.clone(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(
                    DateTime::new_utc(2011, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2012, 1, 1, 0, 0, 0),
                ),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                empty_grid.clone(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(
                    DateTime::new_utc(2011, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2012, 1, 1, 0, 0, 0),
                ),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                empty_grid.clone(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(
                    DateTime::new_utc(2012, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2013, 1, 1, 0, 0, 0),
                ),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                empty_grid.clone(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(
                    DateTime::new_utc(2012, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2013, 1, 1, 0, 0, 0),
                ),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                empty_grid.clone(),
            ),
        ];

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: Some(no_data_value.as_()),
                },
            },
        }
        .boxed();

        let time_shift = TimeShift {
            sources: SingleRasterOrVectorSource {
                source: RasterOrVectorOperator::Raster(mrs),
            },
            params: TimeShiftParams::Relative {
                step: TimeStep {
                    granularity: TimeGranularity::Years,
                    step: 1,
                },
                direction: RelativeShiftDirection::Forward,
            },
        };

        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new((0., 0.).into(), [3, 2].into()),
        );
        let query_context = MockQueryContext::test_default();

        let query_processor = RasterOperator::boxed(time_shift)
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let mut stream = query_processor
            .raster_query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 3.).into(),
                        (4., 0.).into(),
                    ),
                    time_interval: TimeInterval::new(
                        DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                        DateTime::new_utc(2011, 1, 1, 0, 0, 0),
                    )
                    .unwrap(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &query_context,
            )
            .await
            .unwrap();

        let mut result = Vec::new();
        while let Some(tile) = stream.next().await {
            result.push(tile.unwrap());
        }

        assert_eq!(result.len(), 2);

        assert_eq!(
            result[0].time,
            TimeInterval::new_unchecked(
                DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                DateTime::new_utc(2011, 1, 1, 0, 0, 0),
            ),
        );
        assert_eq!(
            result[1].time,
            TimeInterval::new_unchecked(
                DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                DateTime::new_utc(2011, 1, 1, 0, 0, 0),
            ),
        );
    }

    #[tokio::test]
    async fn test_expression_on_shifted_raster() {
        let mut execution_context = MockExecutionContext::test_default();

        let ndvi_source = GdalSource {
            params: GdalSourceParameters {
                dataset: add_ndvi_dataset(&mut execution_context),
            },
        }
        .boxed();

        let shifted_ndvi_source = RasterOperator::boxed(TimeShift {
            params: TimeShiftParams::Relative {
                step: TimeStep {
                    granularity: TimeGranularity::Months,
                    step: 1,
                },
                direction: RelativeShiftDirection::Backward,
            },
            sources: SingleRasterOrVectorSource {
                source: RasterOrVectorOperator::Raster(ndvi_source.clone()),
            },
        });

        let expression = Expression {
            params: ExpressionParams {
                expression: "A - B".to_string(),
                output_type: RasterDataType::F64,
                output_no_data_value: -9999.,
                output_measurement: None,
                map_no_data: false,
            },
            sources: ExpressionSources::new_a_b(ndvi_source, shifted_ndvi_source),
        }
        .boxed();

        let query_processor = expression
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_f64()
            .unwrap();

        let query_context = MockQueryContext::test_default();

        let mut stream = query_processor
            .raster_query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (-180., 90.).into(),
                        (180., -90.).into(),
                    ),
                    time_interval: TimeInterval::new_instant(DateTime::new_utc(
                        2014, 3, 1, 0, 0, 0,
                    ))
                    .unwrap(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &query_context,
            )
            .await
            .unwrap();

        let mut result = Vec::new();
        while let Some(tile) = stream.next().await {
            result.push(tile.unwrap());
        }

        assert_eq!(result.len(), 4);
        assert_eq!(
            result[0].time,
            TimeInterval::new(
                DateTime::new_utc(2014, 3, 1, 0, 0, 0),
                DateTime::new_utc(2014, 4, 1, 0, 0, 0)
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn test_expression_on_absolute_shifted_raster() {
        let mut execution_context = MockExecutionContext::test_default();

        let ndvi_source = GdalSource {
            params: GdalSourceParameters {
                dataset: add_ndvi_dataset(&mut execution_context),
            },
        }
        .boxed();

        let shifted_ndvi_source = RasterOperator::boxed(TimeShift {
            params: TimeShiftParams::Absolute {
                time_interval: TimeInterval::new_instant(DateTime::new_utc(2014, 5, 1, 0, 0, 0))
                    .unwrap(),
            },
            sources: SingleRasterOrVectorSource {
                source: RasterOrVectorOperator::Raster(ndvi_source),
            },
        });

        let query_processor = shifted_ndvi_source
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let query_context = MockQueryContext::test_default();

        let mut stream = query_processor
            .raster_query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (-180., 90.).into(),
                        (180., -90.).into(),
                    ),
                    time_interval: TimeInterval::new_instant(DateTime::new_utc(
                        2014, 3, 1, 0, 0, 0,
                    ))
                    .unwrap(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &query_context,
            )
            .await
            .unwrap();

        let mut result = Vec::new();
        while let Some(tile) = stream.next().await {
            result.push(tile.unwrap());
        }

        assert_eq!(result.len(), 4);
        assert_eq!(
            result[0].time,
            TimeInterval::new(
                DateTime::new_utc(2014, 3, 1, 0, 0, 0),
                DateTime::new_utc(2014, 4, 1, 0, 0, 0)
            )
            .unwrap()
        );
    }

    // TODO: test for vector shifts, test for reversing the elements
}
