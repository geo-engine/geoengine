use std::sync::Arc;

use crate::engine::{
    ExecutionContext, InitializedSources, InitializedVectorOperator, Operator, OperatorName,
    QueryContext, SingleVectorSource, TypedVectorQueryProcessor, VectorOperator,
    VectorQueryProcessor, VectorResultDescriptor, WorkflowOperatorPath,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use geoengine_datatypes::collections::{
    FeatureCollection, FeatureCollectionInfos, FeatureCollectionModifications,
};
use geoengine_datatypes::primitives::{Geometry, TimeInterval};
use geoengine_datatypes::primitives::{TimeInstance, TimeStep, VectorQueryRectangle};
use geoengine_datatypes::util::arrow::ArrowTyped;
use log::debug;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt, Snafu};

/// Projection of time information in queries and data
///
/// This operator changes the temporal validity of the queried data.
/// In order to query all valid data, it is necessary to change the query rectangle as well.
///
pub type TimeProjection = Operator<TimeProjectionParams, SingleVectorSource>;

impl OperatorName for TimeProjection {
    const TYPE_NAME: &'static str = "TimeProjection";
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeProjectionParams {
    /// Specify the time step granularity and size
    step: TimeStep,
    /// Define an anchor point for `step`
    /// If `None`, the anchor point is `1970-01-01T00:00:00Z` by default
    step_reference: Option<TimeInstance>,
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)), module(error))]
pub enum TimeProjectionError {
    #[snafu(display("Time step must be larger than zero"))]
    WindowSizeMustNotBeZero,

    #[snafu(display("Query rectangle expansion failed: {}", source))]
    CannotExpandQueryRectangle {
        source: geoengine_datatypes::error::Error,
    },

    #[snafu(display("Feature time interval expansion failed: {}", source))]
    CannotExpandFeatureTimeInterval {
        source: geoengine_datatypes::error::Error,
    },
}

#[typetag::serde]
#[async_trait]
impl VectorOperator for TimeProjection {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        ensure!(self.params.step.step > 0, error::WindowSizeMustNotBeZero);

        let initialized_sources = self.sources.initialize_sources(path, context).await?;

        debug!("Initializing `TimeProjection` with {:?}.", &self.params);

        let step_reference = self
            .params
            .step_reference
            // use UTC 0 as default
            .unwrap_or(TimeInstance::EPOCH_START);

        let mut result_descriptor = initialized_sources.vector.result_descriptor().clone();
        rewrite_result_descriptor(&mut result_descriptor, self.params.step, step_reference)?;

        let initialized_operator = InitializedVectorTimeProjection {
            source: initialized_sources.vector,
            result_descriptor,
            step: self.params.step,
            step_reference,
        };

        Ok(initialized_operator.boxed())
    }

    span_fn!(TimeProjection);
}

fn rewrite_result_descriptor(
    result_descriptor: &mut VectorResultDescriptor,
    step: TimeStep,
    step_reference: TimeInstance,
) -> Result<()> {
    if let Some(time) = result_descriptor.time {
        let start = step.snap_relative(step_reference, time.start())?;
        let end = (step.snap_relative(step_reference, time.end())? + step)?;

        result_descriptor.time = Some(TimeInterval::new(start, end)?);
    }
    Ok(())
}

pub struct InitializedVectorTimeProjection {
    source: Box<dyn InitializedVectorOperator>,
    result_descriptor: VectorResultDescriptor,
    step: TimeStep,
    step_reference: TimeInstance,
}

impl InitializedVectorOperator for InitializedVectorTimeProjection {
    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        let source_processor = self.source.query_processor()?;

        Ok(
            call_on_generic_vector_processor!(source_processor, processor => VectorTimeProjectionProcessor {
                processor,
                step: self.step,
                step_reference: self.step_reference,
            }.boxed().into()),
        )
    }
}

pub struct VectorTimeProjectionProcessor<G>
where
    G: Geometry,
{
    processor: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
    step: TimeStep,
    step_reference: TimeInstance,
}

#[async_trait]
impl<G> VectorQueryProcessor for VectorTimeProjectionProcessor<G>
where
    G: Geometry + ArrowTyped + 'static,
{
    type VectorType = FeatureCollection<G>;

    async fn vector_query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::VectorType>>> {
        let query = self.expand_query_rectangle(query)?;
        let stream = self
            .processor
            .vector_query(query, ctx)
            .await?
            .and_then(|collection| {
                self.expand_feature_collection_result(collection, ctx.thread_pool().clone())
            })
            .boxed();
        Ok(stream)
    }
}

impl<G> VectorTimeProjectionProcessor<G>
where
    G: Geometry + ArrowTyped + 'static,
{
    fn expand_query_rectangle(&self, query: VectorQueryRectangle) -> Result<VectorQueryRectangle> {
        Ok(expand_query_rectangle(
            self.step,
            self.step_reference,
            query,
        )?)
    }

    async fn expand_feature_collection_result(
        &self,
        feature_collection: FeatureCollection<G>,
        thread_pool: Arc<ThreadPool>,
    ) -> Result<FeatureCollection<G>> {
        let step = self.step;
        let step_reference = self.step_reference;

        crate::util::spawn_blocking_with_thread_pool(thread_pool, move || {
            Self::expand_feature_collection_result_inner(feature_collection, step, step_reference)
        })
        .await?
        .map_err(Into::into)
    }

    fn expand_feature_collection_result_inner(
        feature_collection: FeatureCollection<G>,
        step: TimeStep,
        step_reference: TimeInstance,
    ) -> Result<FeatureCollection<G>, TimeProjectionError> {
        let time_intervals: Option<Result<Vec<TimeInterval>, TimeProjectionError>> =
            feature_collection
                .time_intervals()
                .par_iter()
                .with_min_len(128) // TODO: find good default
                .map(|time_interval| expand_time_interval(step, step_reference, *time_interval))
                // TODO: change to [`try_collect_into_vec`](https://github.com/rayon-rs/rayon/issues/713) if available
                .try_fold_with(Vec::new(), |mut acc, time_interval| {
                    acc.push(time_interval?);
                    Ok(acc)
                })
                .try_reduce_with(|a, b| Ok([a, b].concat()));

        match time_intervals {
            Some(Ok(time_intervals)) => feature_collection
                .replace_time(&time_intervals)
                .context(error::CannotExpandFeatureTimeInterval),
            Some(Err(error)) => Err(error),
            None => Ok(feature_collection), // was empty
        }
    }
}

fn expand_query_rectangle(
    step: TimeStep,
    step_reference: TimeInstance,
    query: VectorQueryRectangle,
) -> Result<VectorQueryRectangle, TimeProjectionError> {
    Ok(VectorQueryRectangle {
        spatial_bounds: query.spatial_bounds,
        time_interval: expand_time_interval(step, step_reference, query.time_interval)?,
        spatial_resolution: query.spatial_resolution,
    })
}

fn expand_time_interval(
    step: TimeStep,
    step_reference: TimeInstance,
    time_interval: TimeInterval,
) -> Result<TimeInterval, TimeProjectionError> {
    let start = step.snap_relative_preserve_bounds(step_reference, time_interval.start());
    let mut end = step.snap_relative_preserve_bounds(step_reference, time_interval.end());

    // Since snap_relative snaps to the "left side", we have to add one step to the end
    // This only applies if `time_interval.end()` is not the snap point itself.
    if end < time_interval.end() {
        end = match end + step {
            Ok(end) => end,
            Err(_) => TimeInstance::MAX, // `TimeInterval::MAX` can overflow
        };
    }

    TimeInterval::new(start, end).context(error::CannotExpandQueryRectangle)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        engine::{MockExecutionContext, MockQueryContext},
        mock::MockFeatureCollectionSource,
    };
    use geoengine_datatypes::{
        collections::{MultiPointCollection, VectorDataType},
        primitives::{
            BoundingBox2D, DateTime, MultiPoint, SpatialResolution, TimeGranularity, TimeInterval,
        },
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_expand_query_time_interval() {
        fn assert_time_interval_transform<T1: TryInto<TimeInstance>, T2: TryInto<TimeInstance>>(
            t1: T1,
            t2: T1,
            step: TimeStep,
            step_reference: TimeInstance,
            t1_expanded: T2,
            t2_expanded: T2,
        ) where
            T1::Error: std::fmt::Debug,
            T2::Error: std::fmt::Debug,
        {
            let result = expand_time_interval(
                step,
                step_reference,
                TimeInterval::new(t1.try_into().unwrap(), t2.try_into().unwrap()).unwrap(),
            )
            .unwrap();
            let expected = TimeInterval::new(
                t1_expanded.try_into().unwrap(),
                t2_expanded.try_into().unwrap(),
            )
            .unwrap();

            assert_eq!(
                result,
                expected,
                "[{}, {}) != [{}, {})",
                result.start().as_datetime_string(),
                result.end().as_datetime_string(),
                expected.start().as_datetime_string(),
                expected.end().as_datetime_string(),
            );
        }

        assert_time_interval_transform(
            DateTime::new_utc(2010, 1, 1, 0, 0, 0),
            DateTime::new_utc(2011, 1, 1, 0, 0, 0),
            TimeStep {
                granularity: TimeGranularity::Years,
                step: 1,
            },
            TimeInstance::from_millis(0).unwrap(),
            DateTime::new_utc(2010, 1, 1, 0, 0, 0),
            DateTime::new_utc(2011, 1, 1, 0, 0, 0),
        );

        assert_time_interval_transform(
            DateTime::new_utc(2010, 4, 3, 0, 0, 0),
            DateTime::new_utc(2010, 5, 14, 0, 0, 0),
            TimeStep {
                granularity: TimeGranularity::Years,
                step: 1,
            },
            TimeInstance::from_millis(0).unwrap(),
            DateTime::new_utc(2010, 1, 1, 0, 0, 0),
            DateTime::new_utc(2011, 1, 1, 0, 0, 0),
        );

        assert_time_interval_transform(
            DateTime::new_utc(2009, 4, 3, 0, 0, 0),
            DateTime::new_utc(2010, 5, 14, 0, 0, 0),
            TimeStep {
                granularity: TimeGranularity::Years,
                step: 1,
            },
            TimeInstance::from_millis(0).unwrap(),
            DateTime::new_utc(2009, 1, 1, 0, 0, 0),
            DateTime::new_utc(2011, 1, 1, 0, 0, 0),
        );

        assert_time_interval_transform(
            DateTime::new_utc(2009, 4, 3, 0, 0, 0),
            DateTime::new_utc(2010, 5, 14, 0, 0, 0),
            TimeStep {
                granularity: TimeGranularity::Months,
                step: 6,
            },
            TimeInstance::from(DateTime::new_utc(2010, 3, 1, 0, 0, 0)),
            DateTime::new_utc(2009, 3, 1, 0, 0, 0),
            DateTime::new_utc(2010, 9, 1, 0, 0, 0),
        );

        assert_time_interval_transform(
            DateTime::new_utc(2009, 4, 3, 0, 0, 0),
            DateTime::new_utc(2010, 5, 14, 0, 0, 0),
            TimeStep {
                granularity: TimeGranularity::Months,
                step: 6,
            },
            TimeInstance::from(DateTime::new_utc(2020, 1, 1, 0, 0, 0)),
            DateTime::new_utc(2009, 1, 1, 0, 0, 0),
            DateTime::new_utc(2010, 7, 1, 0, 0, 0),
        );

        assert_time_interval_transform(
            TimeInstance::MIN,
            TimeInstance::MAX,
            TimeStep {
                granularity: TimeGranularity::Months,
                step: 6,
            },
            TimeInstance::from(DateTime::new_utc(2010, 3, 1, 0, 0, 0)),
            TimeInstance::MIN,
            TimeInstance::MAX,
        );

        assert_time_interval_transform(
            TimeInstance::MIN + 1,
            TimeInstance::MAX - 1,
            TimeStep {
                granularity: TimeGranularity::Months,
                step: 6,
            },
            TimeInstance::from(DateTime::new_utc(2010, 3, 1, 0, 0, 0)),
            TimeInstance::MIN,
            TimeInstance::MAX,
        );
    }

    #[tokio::test]
    async fn single_year() {
        let execution_context = MockExecutionContext::test_default();
        let query_context = MockQueryContext::test_default();

        let source = MockFeatureCollectionSource::single(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![(0., 0.), (1., 1.), (2., 2.)]).unwrap(),
                vec![
                    TimeInterval::new(
                        DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                        DateTime::new_utc_with_millis(2010, 12, 31, 23, 59, 59, 999),
                    )
                    .unwrap(),
                    TimeInterval::new(
                        DateTime::new_utc(2010, 6, 3, 0, 0, 0),
                        DateTime::new_utc(2010, 7, 14, 0, 0, 0),
                    )
                    .unwrap(),
                    TimeInterval::new(
                        DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                        DateTime::new_utc_with_millis(2010, 3, 31, 23, 59, 59, 999),
                    )
                    .unwrap(),
                ],
                Default::default(),
            )
            .unwrap(),
        );

        let time_projection = TimeProjection {
            sources: SingleVectorSource {
                vector: source.boxed(),
            },
            params: TimeProjectionParams {
                step: TimeStep {
                    granularity: TimeGranularity::Years,
                    step: 1,
                },
                step_reference: None,
            },
        };

        let query_processor = time_projection
            .boxed()
            .initialize(Default::default(), &execution_context)
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
                        DateTime::new_utc(2010, 4, 3, 0, 0, 0),
                        DateTime::new_utc(2010, 5, 14, 0, 0, 0),
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
            MultiPoint::many(vec![(0., 0.), (1., 1.), (2., 2.)]).unwrap(),
            vec![
                TimeInterval::new(
                    DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2011, 1, 1, 0, 0, 0),
                )
                .unwrap(),
                TimeInterval::new(
                    DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2011, 1, 1, 0, 0, 0),
                )
                .unwrap(),
                TimeInterval::new(
                    DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2011, 1, 1, 0, 0, 0),
                )
                .unwrap(),
            ],
            Default::default(),
        )
        .unwrap();

        assert_eq!(result[0], expected);
    }

    #[tokio::test]
    async fn over_a_year() {
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

        let time_projection = TimeProjection {
            sources: SingleVectorSource {
                vector: source.boxed(),
            },
            params: TimeProjectionParams {
                step: TimeStep {
                    granularity: TimeGranularity::Years,
                    step: 1,
                },
                step_reference: None,
            },
        };

        let query_processor = time_projection
            .boxed()
            .initialize(Default::default(), &execution_context)
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
                        DateTime::new_utc(2010, 4, 3, 0, 0, 0),
                        DateTime::new_utc(2010, 5, 14, 0, 0, 0),
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
            MultiPoint::many(vec![(0., 0.), (1., 1.), (2., 2.)]).unwrap(),
            vec![
                TimeInterval::new(
                    DateTime::new_utc(2009, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2011, 1, 1, 0, 0, 0),
                )
                .unwrap(),
                TimeInterval::new(
                    DateTime::new_utc(2009, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2011, 1, 1, 0, 0, 0),
                )
                .unwrap(),
                TimeInterval::new(
                    DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2012, 1, 1, 0, 0, 0),
                )
                .unwrap(),
            ],
            Default::default(),
        )
        .unwrap();

        assert_eq!(result[0], expected);
    }

    #[test]
    fn it_rewrites_result_descriptor() {
        let mut result_descriptor = VectorResultDescriptor {
            data_type: VectorDataType::MultiPoint,
            spatial_reference: SpatialReference::epsg_4326().into(),
            columns: Default::default(),
            time: Some(TimeInterval::new_unchecked(30_000, 90_000)),
            bbox: None,
        };

        rewrite_result_descriptor(
            &mut result_descriptor,
            TimeStep {
                granularity: TimeGranularity::Minutes,
                step: 1,
            },
            TimeInstance::from_millis_unchecked(0),
        )
        .unwrap();

        assert_eq!(
            result_descriptor,
            VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: Default::default(),
                time: Some(TimeInterval::new_unchecked(0, 120_000)),
                bbox: None,
            }
        );
    }
}
