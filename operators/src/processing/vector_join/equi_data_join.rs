use std::collections::HashMap;
use std::sync::Arc;

use float_cmp::approx_eq;
use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;

use geoengine_datatypes::collections::{
    BuilderProvider, DataCollection, FeatureCollection, FeatureCollectionBuilder,
    FeatureCollectionInfos, FeatureCollectionRowBuilder, GeoFeatureCollectionRowBuilder,
    GeometryRandomAccess,
};
use geoengine_datatypes::primitives::{FeatureDataRef, Geometry, TimeInterval};
use geoengine_datatypes::util::arrow::ArrowTyped;

use crate::adapters::FeatureCollectionChunkMerger;
use crate::engine::VectorQueryRectangle;
use crate::engine::{QueryContext, VectorQueryProcessor};
use crate::error::Error;
use crate::util::Result;
use async_trait::async_trait;
use futures::TryStreamExt;

/// Implements an inner equi-join between a `GeoFeatureCollection` stream and a `DataCollection` stream.
pub struct EquiGeoToDataJoinProcessor<G> {
    left_processor: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
    right_processor: Box<dyn VectorQueryProcessor<VectorType = DataCollection>>,
    left_column: Arc<String>,
    right_column: Arc<String>,
    right_translation_table: Arc<HashMap<String, String>>,
}

impl<G> EquiGeoToDataJoinProcessor<G>
where
    G: Geometry + ArrowTyped + Sync + Send + 'static,
    for<'g> FeatureCollection<G>: GeometryRandomAccess<'g>,
    for<'g> <FeatureCollection<G> as GeometryRandomAccess<'g>>::GeometryType: Into<G>,
    FeatureCollectionRowBuilder<G>: GeoFeatureCollectionRowBuilder<G>,
{
    pub fn new(
        left_processor: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
        right_processor: Box<dyn VectorQueryProcessor<VectorType = DataCollection>>,
        left_column: String,
        right_column: String,
        right_translation_table: HashMap<String, String>,
    ) -> Self {
        Self {
            left_processor,
            right_processor,
            left_column: Arc::new(left_column),
            right_column: Arc::new(right_column),
            right_translation_table: Arc::new(right_translation_table),
        }
    }

    fn join(
        &self,
        left: Arc<FeatureCollection<G>>,
        right: DataCollection,
        chunk_byte_size: usize,
    ) -> Result<BatchBuilderIterator<G>> {
        BatchBuilderIterator::new(
            left,
            right,
            self.left_column.clone(),
            self.right_column.clone(),
            self.right_translation_table.clone(),
            chunk_byte_size,
        )
    }
}

struct BatchBuilderIterator<G>
where
    G: Geometry + ArrowTyped + Sync + Send + 'static,
{
    left: Arc<FeatureCollection<G>>,
    right: DataCollection,
    left_column: Arc<String>,
    right_column: Arc<String>,
    right_translation_table: Arc<HashMap<String, String>>,
    builder: FeatureCollectionBuilder<G>,
    chunk_byte_size: usize,
    left_idx: usize,
    first_iteration: bool,
    has_ended: bool,
}

impl<G> BatchBuilderIterator<G>
where
    G: Geometry + ArrowTyped + Sync + Send + 'static,
    for<'g> FeatureCollection<G>: GeometryRandomAccess<'g>,
    for<'g> <FeatureCollection<G> as GeometryRandomAccess<'g>>::GeometryType: Into<G>,
    FeatureCollectionRowBuilder<G>: GeoFeatureCollectionRowBuilder<G>,
{
    pub fn new(
        left: Arc<FeatureCollection<G>>,
        right: DataCollection,
        left_column: Arc<String>,
        right_column: Arc<String>,
        right_translation_table: Arc<HashMap<String, String>>,
        chunk_byte_size: usize,
    ) -> Result<Self> {
        let mut builder = FeatureCollection::<G>::builder();

        // create header by combining values from both collections
        for (column_name, column_type) in left.column_types() {
            builder.add_column(column_name, column_type)?;
        }
        for (column_name, column_type) in right.column_types() {
            builder.add_column(right_translation_table[&column_name].clone(), column_type)?;
        }

        Ok(Self {
            left,
            right,
            left_column,
            right_column,
            right_translation_table,
            builder,
            chunk_byte_size,
            left_idx: 0,
            first_iteration: true,
            has_ended: false,
        })
    }

    fn join_inner_batch_matches(
        left: &FeatureDataRef,
        right: &FeatureDataRef,
        left_time_interval: TimeInterval,
        right_time_intervals: &[TimeInterval],
        left_idx: usize,
    ) -> Result<Vec<(usize, TimeInterval)>> {
        fn matches<T, F>(
            right_values: &[T],
            equals_left_value: F,
            left_time_interval: TimeInterval,
            right_time_intervals: &[TimeInterval],
        ) -> Vec<(usize, TimeInterval)>
        where
            T: PartialEq + Copy,
            F: Fn(T) -> bool,
        {
            right_values
                .iter()
                .enumerate()
                .filter_map(move |(right_idx, &right_value)| {
                    if !equals_left_value(right_value) {
                        return None;
                    }

                    Some(right_idx)
                        .zip(left_time_interval.intersect(&right_time_intervals[right_idx]))
                })
                .collect()
        }

        let right_indices = match (left, right) {
            (FeatureDataRef::Float(left), FeatureDataRef::Float(right)) => {
                let left_value = left.as_ref()[left_idx];
                matches(
                    right.as_ref(),
                    |right_value| approx_eq!(f64, left_value, right_value),
                    left_time_interval,
                    right_time_intervals,
                )
            }
            (FeatureDataRef::Category(left), FeatureDataRef::Category(right)) => {
                let left_value = left.as_ref()[left_idx];
                matches(
                    right.as_ref(),
                    |right_value| left_value == right_value,
                    left_time_interval,
                    right_time_intervals,
                )
            }
            (FeatureDataRef::Int(left), FeatureDataRef::Int(right)) => {
                let left_value = left.as_ref()[left_idx];
                matches(
                    right.as_ref(),
                    |right_value| left_value == right_value,
                    left_time_interval,
                    right_time_intervals,
                )
            }
            (FeatureDataRef::Text(left), FeatureDataRef::Text(right)) => {
                let left_value = left.as_ref()[left_idx];
                matches(
                    right.as_ref(),
                    |right_value| left_value == right_value,
                    left_time_interval,
                    right_time_intervals,
                )
            }
            (left, right) => {
                return Err(Error::ColumnTypeMismatch {
                    left: left.into(),
                    right: right.into(),
                });
            }
        };

        Ok(right_indices)
    }

    fn compute_batch(&mut self) -> Result<FeatureCollection<G>> {
        let mut builder = self.builder.clone().finish_header();

        let left_join_column = self.left.data(&self.left_column).expect("must exist");
        let right_join_column = self.right.data(&self.right_column).expect("must exist");
        let left_time_intervals = self.left.time_intervals();
        let right_time_intervals = self.right.time_intervals();

        // copy such that `self` is not borrowed
        let mut left_idx = self.left_idx;

        let left_data_lookup: HashMap<String, FeatureDataRef> = self
            .left
            .column_names()
            .map(|column_name| {
                (
                    column_name.clone(),
                    self.left.data(column_name).expect("must exist"),
                )
            })
            .collect();
        let right_data_lookup: HashMap<String, FeatureDataRef> = self
            .right_translation_table
            .iter()
            .map(|(old_column_name, new_column_name)| {
                (
                    new_column_name.clone(),
                    self.right.data(old_column_name).expect("must exist"),
                )
            })
            .collect();

        while left_idx < self.left.len() {
            let geometry: G = self
                .left
                .geometry_at(left_idx)
                .expect("index must exist")
                .into();

            let join_inner_batch_matches = Self::join_inner_batch_matches(
                &left_join_column,
                &right_join_column,
                left_time_intervals[left_idx],
                right_time_intervals,
                left_idx,
            )?;

            // add left value
            for (column_name, feature_data) in &left_data_lookup {
                let data = feature_data.get_unchecked(left_idx);

                for _ in 0..join_inner_batch_matches.len() {
                    builder.push_data(column_name, data.clone())?;
                }
            }

            // add right value
            for (column_name, feature_data) in &right_data_lookup {
                for &(right_idx, _) in &join_inner_batch_matches {
                    let data = feature_data.get_unchecked(right_idx);

                    builder.push_data(column_name, data)?;
                }
            }

            // add time and geo
            for (_, time_interval) in join_inner_batch_matches {
                builder.push_geometry(geometry.clone())?;
                builder.push_time_interval(time_interval)?;
                builder.finish_row();
            }

            left_idx += 1;

            // there could be the degenerated case that one left feature matches with
            // many features of the right side and is twice as large as the chunk byte size
            if !builder.is_empty() && builder.byte_size() > self.chunk_byte_size {
                break;
            }
        }

        self.left_idx = left_idx;

        // if iterator ran through, set flag `has_ended` so that the next call will not
        // produce an empty collection
        if self.left_idx >= self.left.len() {
            self.has_ended = true;
        }

        builder.build().map_err(Into::into)
    }
}

impl<G> Iterator for BatchBuilderIterator<G>
where
    G: Geometry + ArrowTyped + Sync + Send + 'static,
    for<'g> FeatureCollection<G>: GeometryRandomAccess<'g>,
    for<'g> <FeatureCollection<G> as GeometryRandomAccess<'g>>::GeometryType: Into<G>,
    FeatureCollectionRowBuilder<G>: GeoFeatureCollectionRowBuilder<G>,
{
    type Item = Result<FeatureCollection<G>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.has_ended {
            return None;
        }

        match self.compute_batch() {
            Ok(collection) => {
                if self.first_iteration {
                    self.first_iteration = false;
                    return Some(Ok(collection));
                }

                if collection.is_empty() {
                    self.has_ended = true;
                    None
                } else {
                    Some(Ok(collection))
                }
            }
            Err(error) => {
                self.first_iteration = false;
                self.has_ended = true;

                Some(Err(error))
            }
        }
    }
}

#[async_trait]
impl<G> VectorQueryProcessor for EquiGeoToDataJoinProcessor<G>
where
    G: Geometry + ArrowTyped + Sync + Send + 'static,
    for<'g> FeatureCollection<G>: GeometryRandomAccess<'g>,
    for<'g> <FeatureCollection<G> as GeometryRandomAccess<'g>>::GeometryType: Into<G>,
    FeatureCollectionRowBuilder<G>: GeoFeatureCollectionRowBuilder<G>,
{
    type VectorType = FeatureCollection<G>;

    async fn vector_query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::VectorType>>> {
        let result_stream = self
            .left_processor
            .vector_query(query, ctx)
            .await?
            .and_then(async move |left_collection| {
                // This implementation is a nested-loop join
                let left_collection = Arc::new(left_collection);

                let data_query = self.right_processor.vector_query(query, ctx).await?;

                let out = data_query
                    .flat_map(move |right_collection| {
                        match right_collection.and_then(|right_collection| {
                            self.join(
                                left_collection.clone(),
                                right_collection,
                                ctx.chunk_byte_size(),
                            )
                        }) {
                            Ok(batch_iter) => stream::iter(batch_iter).boxed(),
                            Err(e) => stream::once(async { Err(e) }).boxed(),
                        }
                    })
                    .boxed();
                Ok(out)
            })
            .try_flatten();

        Ok(FeatureCollectionChunkMerger::new(result_stream.fuse(), ctx.chunk_byte_size()).boxed())
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on_stream;

    use geoengine_datatypes::collections::MultiPointCollection;
    use geoengine_datatypes::primitives::{
        BoundingBox2D, FeatureData, MultiPoint, SpatialResolution, TimeInterval,
    };

    use crate::engine::{MockExecutionContext, MockQueryContext, VectorOperator};
    use crate::mock::MockFeatureCollectionSource;

    use super::*;
    use crate::processing::vector_join::util::translation_table;

    async fn join_mock_collections(
        left: MultiPointCollection,
        right: DataCollection,
        left_join_column: &str,
        right_join_column: &str,
        right_suffix: &str,
    ) -> Vec<MultiPointCollection> {
        let execution_context = MockExecutionContext::default();

        let left = MockFeatureCollectionSource::single(left)
            .boxed()
            .initialize(&execution_context)
            .await
            .unwrap();
        let right = MockFeatureCollectionSource::single(right)
            .boxed()
            .initialize(&execution_context)
            .await
            .unwrap();

        let left_processor = left.query_processor().unwrap().multi_point().unwrap();
        let right_processor = right.query_processor().unwrap().data().unwrap();

        let query_rectangle = VectorQueryRectangle {
            bbox: BoundingBox2D::new((f64::MIN, f64::MIN).into(), (f64::MAX, f64::MAX).into())
                .unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };

        let ctx = MockQueryContext::new(usize::MAX);

        let processor = EquiGeoToDataJoinProcessor::new(
            left_processor,
            right_processor,
            left_join_column.to_string(),
            right_join_column.to_string(),
            translation_table(
                left.result_descriptor().columns.keys(),
                right.result_descriptor().columns.keys(),
                right_suffix,
            ),
        );

        block_on_stream(processor.vector_query(query_rectangle, &ctx).await.unwrap())
            .collect::<Result<_>>()
            .unwrap()
    }

    #[tokio::test]
    async fn join() {
        let left = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1)]).unwrap(),
            vec![TimeInterval::default(); 2],
            [("foo".to_string(), FeatureData::Int(vec![1, 2]))]
                .iter()
                .cloned()
                .collect(),
        )
        .unwrap();

        let right = DataCollection::from_data(
            vec![],
            vec![TimeInterval::default(); 2],
            [("bar".to_string(), FeatureData::Int(vec![2, 2]))]
                .iter()
                .cloned()
                .collect(),
        )
        .unwrap();

        let expected_result = MultiPointCollection::from_data(
            MultiPoint::many(vec![(1.0, 1.1), (1.0, 1.1)]).unwrap(),
            vec![TimeInterval::default(); 2],
            [
                ("foo".to_string(), FeatureData::Int(vec![2, 2])),
                ("bar".to_string(), FeatureData::Int(vec![2, 2])),
            ]
            .iter()
            .cloned()
            .collect(),
        )
        .unwrap();

        let result = join_mock_collections(left, right, "foo", "bar", "").await;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], expected_result);
    }

    #[tokio::test]
    async fn time_intervals() {
        let left = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1)]).unwrap(),
            vec![
                TimeInterval::new_unchecked(0, 2),
                TimeInterval::new_unchecked(4, 5),
            ],
            [("foo".to_string(), FeatureData::Int(vec![1, 2]))]
                .iter()
                .cloned()
                .collect(),
        )
        .unwrap();

        let right = DataCollection::from_data(
            vec![],
            vec![
                TimeInterval::new_unchecked(1, 3),
                TimeInterval::new_unchecked(5, 6),
            ],
            [("bar".to_string(), FeatureData::Int(vec![1, 2]))]
                .iter()
                .cloned()
                .collect(),
        )
        .unwrap();

        let expected_result = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1)]).unwrap(),
            vec![TimeInterval::new_unchecked(1, 2)],
            [
                ("foo".to_string(), FeatureData::Int(vec![1])),
                ("bar".to_string(), FeatureData::Int(vec![1])),
            ]
            .iter()
            .cloned()
            .collect(),
        )
        .unwrap();

        let result = join_mock_collections(left, right, "foo", "bar", "").await;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], expected_result);
    }

    #[tokio::test]
    async fn name_collision() {
        let left = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1)]).unwrap(),
            vec![TimeInterval::default(); 2],
            [("foo".to_string(), FeatureData::Int(vec![1, 2]))]
                .iter()
                .cloned()
                .collect(),
        )
        .unwrap();

        let right = DataCollection::from_data(
            vec![],
            vec![TimeInterval::default(); 2],
            [("foo".to_string(), FeatureData::Int(vec![1, 2]))]
                .iter()
                .cloned()
                .collect(),
        )
        .unwrap();

        let expected_result = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1)]).unwrap(),
            vec![TimeInterval::default(); 2],
            [
                ("foo".to_string(), FeatureData::Int(vec![1, 2])),
                ("foo2".to_string(), FeatureData::Int(vec![1, 2])),
            ]
            .iter()
            .cloned()
            .collect(),
        )
        .unwrap();

        let result = join_mock_collections(left, right, "foo", "foo", "2").await;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], expected_result);
    }

    #[tokio::test]
    async fn multi_match_geo() {
        let left = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1)]).unwrap(),
            vec![TimeInterval::default(); 2],
            [("foo".to_string(), FeatureData::Int(vec![1, 2]))]
                .iter()
                .cloned()
                .collect(),
        )
        .unwrap();

        let right = DataCollection::from_data(
            vec![],
            vec![TimeInterval::default(); 5],
            [
                ("bar".to_string(), FeatureData::Int(vec![1, 1, 1, 2, 2])),
                (
                    "baz".to_string(),
                    FeatureData::Text(vec![
                        "this".to_string(),
                        "is".to_string(),
                        "the".to_string(),
                        "way".to_string(),
                        "!".to_string(),
                    ]),
                ),
            ]
            .iter()
            .cloned()
            .collect(),
        )
        .unwrap();

        let expected_result = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                (0.0, 0.1),
                (0.0, 0.1),
                (0.0, 0.1),
                (1.0, 1.1),
                (1.0, 1.1),
            ])
            .unwrap(),
            vec![TimeInterval::default(); 5],
            [
                ("foo".to_string(), FeatureData::Int(vec![1, 1, 1, 2, 2])),
                ("bar".to_string(), FeatureData::Int(vec![1, 1, 1, 2, 2])),
                (
                    "baz".to_string(),
                    FeatureData::Text(vec![
                        "this".to_string(),
                        "is".to_string(),
                        "the".to_string(),
                        "way".to_string(),
                        "!".to_string(),
                    ]),
                ),
            ]
            .iter()
            .cloned()
            .collect(),
        )
        .unwrap();

        let result = join_mock_collections(left, right, "foo", "bar", "").await;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], expected_result);
    }

    #[tokio::test]
    async fn no_matches() {
        let left = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1)]).unwrap(),
            vec![TimeInterval::default(); 2],
            [("foo".to_string(), FeatureData::Int(vec![1, 2]))]
                .iter()
                .cloned()
                .collect(),
        )
        .unwrap();

        let right = DataCollection::from_data(
            vec![],
            vec![TimeInterval::default(); 2],
            [("bar".to_string(), FeatureData::Int(vec![3, 4]))]
                .iter()
                .cloned()
                .collect(),
        )
        .unwrap();

        let result = join_mock_collections(left, right, "foo", "bar", "").await;

        // TODO: do we need an empty collection here? (cf. `FeatureCollectionChunkMerger`)
        assert_eq!(result.len(), 0);
    }
}
