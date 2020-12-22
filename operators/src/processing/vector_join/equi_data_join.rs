use std::collections::HashSet;

use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;

use geoengine_datatypes::collections::{
    BuilderProvider, DataCollection, FeatureCollection, FeatureCollectionInfos,
    FeatureCollectionModifications, FeatureCollectionRowBuilder, GeoFeatureCollectionRowBuilder,
    IntoGeometryIterator,
};
use geoengine_datatypes::primitives::{DataRef, FeatureDataRef, Geometry};
use geoengine_datatypes::util::arrow::ArrowTyped;

use crate::adapters::FeatureCollectionChunkMerger;
use crate::engine::{QueryContext, QueryProcessor, QueryRectangle, VectorQueryProcessor};
use crate::error::Error;
use crate::util::Result;

/// Implements an inner equi-join between a `GeoFeatureCollection` stream and a `DataCollection` stream.
pub struct EquiGeoToDataJoinProcessor<G> {
    left_processor: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
    right_processor: Box<dyn VectorQueryProcessor<VectorType = DataCollection>>,
    left_column: String,
    right_column: String,
    right_column_prefix: String,
}

impl<G> EquiGeoToDataJoinProcessor<G>
where
    G: Geometry + ArrowTyped + Sync + Send + 'static,
    for<'g> FeatureCollection<G>: IntoGeometryIterator<'g>,
    for<'g> <FeatureCollection<G> as IntoGeometryIterator<'g>>::GeometryType: Into<G>,
    FeatureCollectionRowBuilder<G>: GeoFeatureCollectionRowBuilder<G>,
{
    pub fn new(
        left_processor: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
        right_processor: Box<dyn VectorQueryProcessor<VectorType = DataCollection>>,
        left_column: String,
        right_column: String,
        right_prefix: String,
    ) -> Self {
        Self {
            left_processor,
            right_processor,
            left_column,
            right_column,
            right_column_prefix: right_prefix,
        }
    }

    fn join<'c>(
        &self,
        left: &'c FeatureCollection<G>,
        right: &DataCollection,
    ) -> Result<FeatureCollection<G>> {
        let right = self.fix_columns(right, &left.column_names().collect())?;

        let left_join_column = left.data(&self.left_column)?;
        let right_join_column = right.data(&self.right_column)?;

        // TODO: use macro
        match (left_join_column, right_join_column) {
            (FeatureDataRef::Number(c1), FeatureDataRef::Number(c2)) => {
                Self::compute_join(&c1, c2, left, &right)
            }
            (FeatureDataRef::Decimal(c1), FeatureDataRef::Decimal(c2)) => {
                Self::compute_join(&c1, c2, left, &right)
            }
            (FeatureDataRef::Text(c1), FeatureDataRef::Text(c2)) => {
                Self::compute_join(&c1, c2, left, &right)
            }
            (FeatureDataRef::Categorical(c1), FeatureDataRef::Categorical(c2)) => {
                Self::compute_join(&c1, c2, left, &right)
            }
            (left, right) => Err(Error::ColumnTypeMismatch {
                left: (&left).into(),
                right: (&right).into(),
            }),
        }
    }

    fn compute_join<'i, D1, D2, T>(
        left_join_values: &D1,
        right_join_values: D2,
        left: &FeatureCollection<G>,
        right: &DataCollection,
    ) -> Result<FeatureCollection<G>>
    where
        D1: DataRef<'i, T> + 'i,
        D2: DataRef<'i, T> + 'i,
        T: 'i + PartialEq,
    {
        let matches_iter = left_join_values
            .as_ref()
            .iter()
            .zip(left.time_intervals())
            .map(move |(left_value, left_time_interval)| {
                right_join_values
                    .as_ref()
                    .iter()
                    .zip(right.time_intervals())
                    .map(|(right_value, right_time_interval)| {
                        (left_value == right_value)
                            && left_time_interval.intersects(right_time_interval)
                    })
                    .collect::<Vec<bool>>()
            });

        let mut builder = FeatureCollection::<G>::builder();

        // create header by combining values from both collections
        for (column_name, column_type) in left
            .column_types()
            .into_iter()
            .chain(right.column_types().into_iter())
        {
            builder.add_column(column_name, column_type)?;
        }

        let mut builder = builder.finish_header();

        for ((left_feature_idx, matches), geometry) in matches_iter
            .enumerate()
            .zip(left.geometries().map(Into::into))
        {
            // TODO: is it better to traverse column-wise?

            for (right_feature_idx, matched) in matches.into_iter().enumerate() {
                if !matched {
                    continue;
                }

                // add geo
                builder.push_geometry(geometry.clone())?;

                // add time
                builder.push_time_interval(
                    left.time_intervals()[left_feature_idx]
                        .intersect(&right.time_intervals()[right_feature_idx])
                        .expect("must intersect for join"),
                )?;

                // add left values
                for column_name in left.column_names() {
                    let feature_data = left
                        .data(column_name)
                        .expect("must exist")
                        .get_unchecked(left_feature_idx);
                    builder.push_data(column_name, feature_data)?;
                }

                // add right values
                for column_name in right.column_names() {
                    let feature_data = right
                        .data(column_name)
                        .expect("must exist")
                        .get_unchecked(right_feature_idx);
                    builder.push_data(column_name, feature_data)?;
                }

                builder.finish_row();
            }
        }

        builder.build().map_err(Into::into)
    }

    /// Fix column by renaming column with duplicate keys
    fn fix_columns(
        &self,
        collection: &DataCollection,
        existing_column_names: &HashSet<&String>,
    ) -> Result<DataCollection> {
        let mut renamings: Vec<(&str, String)> = Vec::new();

        for column_name in collection.column_names() {
            while existing_column_names.contains(column_name) {
                let new_column_name = format!("{}{}", self.right_column_prefix, column_name);
                renamings.push((column_name, new_column_name));
            }
        }

        if renamings.is_empty() {
            Ok(collection.clone())
        } else {
            collection.rename_columns(&renamings).map_err(Into::into)
        }
    }
}

impl<G> VectorQueryProcessor for EquiGeoToDataJoinProcessor<G>
where
    G: Geometry + ArrowTyped + Sync + Send + 'static,
    for<'g> FeatureCollection<G>: IntoGeometryIterator<'g>,
    for<'g> <FeatureCollection<G> as IntoGeometryIterator<'g>>::GeometryType: Into<G>,
    FeatureCollectionRowBuilder<G>: GeoFeatureCollectionRowBuilder<G>,
{
    type VectorType = FeatureCollection<G>;

    fn vector_query(
        &self,
        query: QueryRectangle,
        ctx: QueryContext,
    ) -> BoxStream<Result<Self::VectorType>> {
        let result_stream =
            self.left_processor
                .query(query, ctx)
                .flat_map(move |left_collection| {
                    // This implementation is a nested-loop join

                    let left_collection = match left_collection {
                        Ok(collection) => collection,
                        Err(e) => return stream::once(async { Err(e) }).boxed(),
                    };

                    let data_query = self.right_processor.query(query, ctx);

                    data_query
                        .map(move |right_collection| {
                            self.join(&left_collection, &right_collection?)
                        })
                        .boxed()
                });

        FeatureCollectionChunkMerger::new(result_stream.fuse(), ctx.chunk_byte_size).boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{MockExecutionContextCreator, VectorOperator};
    use crate::mock::MockFeatureCollectionSource;
    use futures::executor::block_on_stream;
    use geoengine_datatypes::collections::MultiPointCollection;
    use geoengine_datatypes::primitives::{
        BoundingBox2D, FeatureData, MultiPoint, SpatialResolution, TimeInterval,
    };

    #[test]
    fn join() {
        let left = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1)]).unwrap(),
            vec![TimeInterval::default(); 2],
            [("foo".to_string(), FeatureData::Decimal(vec![1, 2]))]
                .iter()
                .cloned()
                .collect(),
        )
        .unwrap();

        let right = DataCollection::from_data(
            vec![],
            vec![TimeInterval::default(); 2],
            [("bar".to_string(), FeatureData::Decimal(vec![2, 2]))]
                .iter()
                .cloned()
                .collect(),
        )
        .unwrap();

        let expected_result = MultiPointCollection::from_data(
            MultiPoint::many(vec![(1.0, 1.1), (1.0, 1.1)]).unwrap(),
            vec![TimeInterval::default(); 2],
            [
                ("foo".to_string(), FeatureData::Decimal(vec![2, 2])),
                ("bar".to_string(), FeatureData::Decimal(vec![2, 2])),
            ]
            .iter()
            .cloned()
            .collect(),
        )
        .unwrap();

        let execution_context_creator = MockExecutionContextCreator::default();
        let execution_context = execution_context_creator.context();

        let left = MockFeatureCollectionSource::single(left)
            .boxed()
            .initialize(&execution_context)
            .unwrap();
        let right = MockFeatureCollectionSource::single(right)
            .boxed()
            .initialize(&execution_context)
            .unwrap();

        let left_processor = left.query_processor().unwrap().multi_point().unwrap();
        let right_processor = right.query_processor().unwrap().data().unwrap();

        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new((0., 0.).into(), (4., 4.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };

        let ctx = QueryContext {
            chunk_byte_size: usize::MAX,
        };

        let processor = EquiGeoToDataJoinProcessor::new(
            left_processor,
            right_processor,
            "foo".to_string(),
            "bar".to_string(),
            "".to_string(),
        );

        let result: Vec<MultiPointCollection> =
            block_on_stream(processor.vector_query(query_rectangle, ctx))
                .collect::<Result<_>>()
                .unwrap();

        assert_eq!(result[0], expected_result);
    }

    #[test]
    fn time_intervals() {
        let left = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1)]).unwrap(),
            vec![
                TimeInterval::new_unchecked(0, 2),
                TimeInterval::new_unchecked(4, 5),
            ],
            [("foo".to_string(), FeatureData::Decimal(vec![1, 2]))]
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
            [("bar".to_string(), FeatureData::Decimal(vec![1, 2]))]
                .iter()
                .cloned()
                .collect(),
        )
        .unwrap();

        let expected_result = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1)]).unwrap(),
            vec![TimeInterval::new_unchecked(1, 2)],
            [
                ("foo".to_string(), FeatureData::Decimal(vec![1])),
                ("bar".to_string(), FeatureData::Decimal(vec![1])),
            ]
            .iter()
            .cloned()
            .collect(),
        )
        .unwrap();

        let execution_context_creator = MockExecutionContextCreator::default();
        let execution_context = execution_context_creator.context();

        let left = MockFeatureCollectionSource::single(left)
            .boxed()
            .initialize(&execution_context)
            .unwrap();
        let right = MockFeatureCollectionSource::single(right)
            .boxed()
            .initialize(&execution_context)
            .unwrap();

        let left_processor = left.query_processor().unwrap().multi_point().unwrap();
        let right_processor = right.query_processor().unwrap().data().unwrap();

        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new((0., 0.).into(), (4., 4.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };

        let ctx = QueryContext {
            chunk_byte_size: usize::MAX,
        };

        let processor = EquiGeoToDataJoinProcessor::new(
            left_processor,
            right_processor,
            "foo".to_string(),
            "bar".to_string(),
            "".to_string(),
        );

        let result: Vec<MultiPointCollection> =
            block_on_stream(processor.vector_query(query_rectangle, ctx))
                .collect::<Result<_>>()
                .unwrap();

        assert_eq!(result[0], expected_result);
    }
}
