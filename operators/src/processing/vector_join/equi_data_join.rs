use std::collections::HashSet;

use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;

use geoengine_datatypes::collections::{
    BuilderProvider, DataCollection, FeatureCollection, FeatureCollectionInfos,
    FeatureCollectionModifications, FeatureCollectionRowBuilder, GeoFeatureCollectionRowBuilder,
    IntoGeometryIterator,
};
use geoengine_datatypes::primitives::{DataRef, FeatureDataRef, Geometry, TimeInterval};
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

    fn join_iter<'i, D1, D2, T>(
        left_join_values: &'i D1,
        right_join_values: D2,
        left: &'i FeatureCollection<G>,
        right: &'i DataCollection,
    ) -> impl Iterator<Item = (usize, G, Vec<(usize, TimeInterval)>)> + 'i
    where
        D1: DataRef<'i, T> + 'i,
        D2: DataRef<'i, T> + 'i,
        T: 'i + PartialEq,
    {
        left_join_values
            .as_ref()
            .iter()
            .enumerate()
            .zip(left.geometries())
            .zip(left.time_intervals())
            .map(
                move |(((left_idx, left_value), left_geometry), left_time_interval)| {
                    (
                        left_idx,
                        left_geometry.into(),
                        right_join_values
                            .as_ref()
                            .iter()
                            .enumerate()
                            .zip(right.time_intervals())
                            .filter_map(|((right_idx, right_value), right_time_interval)| {
                                if left_value != right_value {
                                    return None;
                                }

                                Some(right_idx)
                                    .zip(left_time_interval.intersect(right_time_interval))
                            })
                            .collect::<Vec<(usize, TimeInterval)>>(),
                    )
                },
            )
    }

    fn compute_join<'i, D1, D2, T>(
        left_join_values: &'i D1,
        right_join_values: D2,
        left: &'i FeatureCollection<G>,
        right: &'i DataCollection,
    ) -> Result<FeatureCollection<G>>
    where
        D1: DataRef<'i, T> + 'i,
        D2: DataRef<'i, T> + 'i,
        T: 'i + PartialEq,
    {
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

        for (left_feature_idx, geometry, matches) in
            Self::join_iter(left_join_values, right_join_values, left, right)
        {
            // add left values
            for column_name in left.column_names() {
                let feature_data = left.data(column_name).expect("must exist");

                let data = feature_data.get_unchecked(left_feature_idx);

                for _ in &matches {
                    builder.push_data(column_name, data.clone())?;
                }
            }

            // add right values
            for column_name in right.column_names() {
                let feature_data = right.data(column_name).expect("must exist");

                for &(right_feature_idx, _) in &matches {
                    builder
                        .push_data(column_name, feature_data.get_unchecked(right_feature_idx))?;
                }
            }

            // add geo and time
            for (_, time_interval) in matches {
                builder.push_geometry(geometry.clone())?;

                builder.push_time_interval(time_interval)?;

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
