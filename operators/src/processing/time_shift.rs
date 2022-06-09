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
use geoengine_datatypes::primitives::{
    Duration, Geometry, RasterQueryRectangle, TimeGranularity, TimeInstance, TimeInterval,
};
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
        granularity: TimeGranularity,
        value: i32,
    },
    /// Set the time interval to a fixed value
    Absolute { time_interval: TimeInterval },
}

pub trait TimeShiftOperation: Send + Sync + Copy {
    type State: Send + Sync + Copy;

    fn shift(
        &self,
        time_interval: TimeInterval,
    ) -> Result<(TimeInterval, Self::State), TimeShiftError>;

    fn reverse_shift(
        &self,
        time_interval: TimeInterval,
        state: Self::State,
    ) -> Result<TimeInterval, TimeShiftError>;
}

#[derive(Debug, Clone, Copy)]
pub struct RelativeForwardShift {
    step: TimeStep,
}

impl TimeShiftOperation for RelativeForwardShift {
    type State = ();

    fn shift(
        &self,
        time_interval: TimeInterval,
    ) -> Result<(TimeInterval, Self::State), TimeShiftError> {
        let time_interval = time_interval + self.step;
        let time_interval = time_interval.boxed_context(error::TimeOverflow)?;
        Ok((time_interval, ()))
    }

    fn reverse_shift(
        &self,
        time_interval: TimeInterval,
        _state: Self::State,
    ) -> Result<TimeInterval, TimeShiftError> {
        let reversed_time_interval = time_interval - self.step;
        reversed_time_interval.boxed_context(error::TimeOverflow)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RelativeBackwardShift {
    step: TimeStep,
}

impl TimeShiftOperation for RelativeBackwardShift {
    type State = ();

    fn shift(
        &self,
        time_interval: TimeInterval,
    ) -> Result<(TimeInterval, Self::State), TimeShiftError> {
        let time_interval = time_interval - self.step;
        let time_interval = time_interval.boxed_context(error::TimeOverflow)?;
        Ok((time_interval, ()))
    }

    fn reverse_shift(
        &self,
        time_interval: TimeInterval,
        _state: Self::State,
    ) -> Result<TimeInterval, TimeShiftError> {
        let reversed_time_interval = time_interval + self.step;
        reversed_time_interval.boxed_context(error::TimeOverflow)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AbsoluteShift {
    time_interval: TimeInterval,
}

impl TimeShiftOperation for AbsoluteShift {
    type State = (Duration, Duration);

    fn shift(
        &self,
        time_interval: TimeInterval,
    ) -> Result<(TimeInterval, Self::State), TimeShiftError> {
        let time_start_difference = time_interval.start() - self.time_interval.start();
        let time_end_difference = time_interval.end() - self.time_interval.end();

        Ok((
            self.time_interval,
            (time_start_difference, time_end_difference),
        ))
    }

    fn reverse_shift(
        &self,
        time_interval: TimeInterval,
        (time_start_difference, time_end_difference): Self::State,
    ) -> Result<TimeInterval, TimeShiftError> {
        let t1 = time_interval.start() + time_start_difference.num_milliseconds();
        let t2 = time_interval.end() + time_end_difference.num_milliseconds();
        TimeInterval::new(t1, t2).boxed_context(error::FaultyTimeInterval { t1, t2 })
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)), module(error))]
pub enum TimeShiftError {
    #[snafu(display("Output type must match the type of the source"))]
    UnmatchedOutput,
    #[snafu(display("Shifting the time led to an overflowing time interval"))]
    TimeOverflow { source: Box<dyn ErrorSource> },
    #[snafu(display("Shifting the time to a faulty time interval: {t1} / {t2}"))]
    FaultyTimeInterval {
        source: Box<dyn ErrorSource>,
        t1: TimeInstance,
        t2: TimeInstance,
    },
    #[snafu(display("Modifying the timestamps of the feature collection failed"))]
    FeatureCollectionTimeModification { source: Box<dyn ErrorSource> },
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
                TimeShiftParams::Relative { granularity, value },
            ) if value.is_positive() => {
                let source = source.initialize(context).await?;

                let shift = RelativeForwardShift {
                    step: TimeStep {
                        granularity,
                        step: value.unsigned_abs(),
                    },
                };

                let mut result_descriptor = source.result_descriptor().clone();
                if let Some(time) = result_descriptor.time {
                    result_descriptor.time = shift.shift(time).map(|r| r.0).ok();
                }

                Ok(Box::new(InitializedVectorTimeShift {
                    source,
                    result_descriptor,
                    shift,
                }))
            }
            (
                RasterOrVectorOperator::Vector(source),
                TimeShiftParams::Relative { granularity, value },
            ) => {
                let source = source.initialize(context).await?;

                let shift = RelativeBackwardShift {
                    step: TimeStep {
                        granularity,
                        step: value.unsigned_abs(),
                    },
                };

                let mut result_descriptor = source.result_descriptor().clone();
                if let Some(time) = result_descriptor.time {
                    result_descriptor.time = shift.shift(time).map(|r| r.0).ok();
                }

                Ok(Box::new(InitializedVectorTimeShift {
                    source,
                    result_descriptor,
                    shift,
                }))
            }
            (
                RasterOrVectorOperator::Vector(source),
                TimeShiftParams::Absolute { time_interval },
            ) => {
                let source = source.initialize(context).await?;

                let shift = AbsoluteShift { time_interval };

                let mut result_descriptor = source.result_descriptor().clone();
                if let Some(time) = result_descriptor.time {
                    result_descriptor.time = shift.shift(time).map(|r| r.0).ok();
                }

                Ok(Box::new(InitializedVectorTimeShift {
                    source,
                    result_descriptor,
                    shift,
                }))
            }
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
                TimeShiftParams::Relative { granularity, value },
            ) if value.is_positive() => {
                let source = source.initialize(context).await?;

                let shift = RelativeForwardShift {
                    step: TimeStep {
                        granularity,
                        step: value.unsigned_abs(),
                    },
                };

                let mut result_descriptor = source.result_descriptor().clone();
                if let Some(time) = result_descriptor.time {
                    result_descriptor.time = shift.shift(time).map(|r| r.0).ok();
                }

                Ok(Box::new(InitializedRasterTimeShift {
                    source,
                    result_descriptor,
                    shift,
                }))
            }
            (
                RasterOrVectorOperator::Raster(source),
                TimeShiftParams::Relative { granularity, value },
            ) => {
                let source = source.initialize(context).await?;

                let shift = RelativeBackwardShift {
                    step: TimeStep {
                        granularity,
                        step: value.unsigned_abs(),
                    },
                };

                let mut result_descriptor = source.result_descriptor().clone();
                if let Some(time) = result_descriptor.time {
                    result_descriptor.time = shift.shift(time).map(|r| r.0).ok();
                }

                Ok(Box::new(InitializedRasterTimeShift {
                    source,
                    result_descriptor,
                    shift,
                }))
            }
            (
                RasterOrVectorOperator::Raster(source),
                TimeShiftParams::Absolute { time_interval },
            ) => {
                let source = source.initialize(context).await?;

                let shift = AbsoluteShift { time_interval };

                let mut result_descriptor = source.result_descriptor().clone();
                if let Some(time) = result_descriptor.time {
                    result_descriptor.time = shift.shift(time).map(|r| r.0).ok();
                }

                Ok(Box::new(InitializedRasterTimeShift {
                    source,
                    result_descriptor,
                    shift,
                }))
            }
            (RasterOrVectorOperator::Vector(_), _) => Err(TimeShiftError::UnmatchedOutput.into()),
        }
    }
}

pub struct InitializedVectorTimeShift<Shift: TimeShiftOperation> {
    source: Box<dyn InitializedVectorOperator>,
    result_descriptor: VectorResultDescriptor,
    shift: Shift,
}

pub struct InitializedRasterTimeShift<Shift: TimeShiftOperation> {
    source: Box<dyn InitializedRasterOperator>,
    result_descriptor: RasterResultDescriptor,
    shift: Shift,
}

impl<Shift: TimeShiftOperation + 'static> InitializedVectorOperator
    for InitializedVectorTimeShift<Shift>
{
    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        let source_processor = self.source.query_processor()?;

        Ok(
            call_on_generic_vector_processor!(source_processor, processor => VectorTimeShiftProcessor {
                processor,
                shift: self.shift,
            }.boxed().into()),
        )
    }
}

impl<Shift: TimeShiftOperation + 'static> InitializedRasterOperator
    for InitializedRasterTimeShift<Shift>
{
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source_processor = self.source.query_processor()?;

        Ok(
            call_on_generic_raster_processor!(source_processor, processor => RasterTimeShiftProcessor {
                processor,
                shift: self.shift,
            }.boxed().into()),
        )
    }
}

pub struct RasterTimeShiftProcessor<Q, P, Shift: TimeShiftOperation>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    processor: Q,
    shift: Shift,
}

pub struct VectorTimeShiftProcessor<Q, G, Shift: TimeShiftOperation>
where
    G: Geometry,
    Q: VectorQueryProcessor<VectorType = FeatureCollection<G>>,
{
    processor: Q,
    shift: Shift,
}

#[async_trait]
impl<Q, G, Shift> VectorQueryProcessor for VectorTimeShiftProcessor<Q, G, Shift>
where
    G: Geometry + ArrowTyped + 'static,
    Q: VectorQueryProcessor<VectorType = FeatureCollection<G>>,
    Shift: TimeShiftOperation + 'static,
{
    type VectorType = FeatureCollection<G>;

    async fn vector_query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::VectorType>>> {
        let (time_interval, state) = self.shift.shift(query.time_interval)?;

        let query = VectorQueryRectangle {
            spatial_bounds: query.spatial_bounds,
            time_interval,
            spatial_resolution: query.spatial_resolution,
        };
        let stream = self.processor.vector_query(query, ctx).await?;

        let stream = stream.then(move |collection| async move {
            let collection = collection?;
            let shift = self.shift;

            crate::util::spawn_blocking(move || {
                let time_intervals = collection
                    .time_intervals()
                    .iter()
                    .map(move |time| shift.reverse_shift(*time, state))
                    .collect::<Result<Vec<TimeInterval>, TimeShiftError>>()?;

                collection
                    .replace_time(&time_intervals)
                    .boxed_context(error::FeatureCollectionTimeModification)
                    .map_err(Into::into)
            })
            .await?
        });

        Ok(stream.boxed())
    }
}

#[async_trait]
impl<Q, P, Shift> RasterQueryProcessor for RasterTimeShiftProcessor<Q, P, Shift>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
    Shift: TimeShiftOperation,
{
    type RasterType = P;

    async fn raster_query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<Self::RasterType>>>> {
        let (time_interval, state) = self.shift.shift(query.time_interval)?;
        let query = RasterQueryRectangle {
            spatial_bounds: query.spatial_bounds,
            time_interval,
            spatial_resolution: query.spatial_resolution,
        };
        let stream = self.processor.raster_query(query, ctx).await?;

        let stream = stream.map(move |raster| {
            // reverse time shift for results
            let mut raster = raster?;

            raster.time = self.shift.reverse_shift(raster.time, state)?;

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
                granularity: TimeGranularity::Years,
                value: 1,
            },
        };

        let serialized = serde_json::to_value(&time_shift).unwrap();

        assert_eq!(
            serialized,
            serde_json::json!({
                "params": {
                    "type": "relative",
                    "granularity": "years",
                    "value": 1
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
                granularity: TimeGranularity::Years,
                value: -1,
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
                    time: None,
                    bbox: None,
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
                    time: None,
                    bbox: None,
                },
            },
        }
        .boxed();

        let time_shift = TimeShift {
            sources: SingleRasterOrVectorSource {
                source: RasterOrVectorOperator::Raster(mrs),
            },
            params: TimeShiftParams::Relative {
                granularity: TimeGranularity::Years,
                value: 1,
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
                granularity: TimeGranularity::Months,
                value: -1,
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
}
