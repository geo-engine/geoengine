use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::StreamExt;
use geoengine_datatypes::collections::{
    BuilderProvider, GeoFeatureCollectionRowBuilder, MultiPointCollection, VectorDataType,
};
use geoengine_datatypes::primitives::{
    BoundingBox2D, Circle, FeatureDataType, FeatureDataValue, MultiPoint, MultiPointAccess,
    TimeInterval,
};
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::adapters::FeatureCollectionStreamExt;
use crate::engine::{
    ExecutionContext, InitializedVectorOperator, Operator, QueryContext, QueryProcessor,
    SingleVectorSource, TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor,
    VectorQueryRectangle, VectorResultDescriptor,
};
use crate::error::{self, Error};
use crate::processing::circle_merging_quadtree::aggregates::MeanAggregator;
use crate::processing::circle_merging_quadtree::circle_of_points::CircleOfPoints;
use crate::processing::circle_merging_quadtree::circle_radius_model::LogScaledRadius;
use crate::util::Result;

use super::aggregates::{AttributeAggregate, AttributeAggregateType, StringSampler};
use super::circle_radius_model::CircleRadiusModel;
use super::grid::Grid;
use super::quadtree::CircleMergingQuadtree;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct VisualPointClusteringParams {
    pub min_radius_px: f64,
    pub delta_px: f64,
    radius_column: String,
    count_column: String,
    column_aggregates: HashMap<String, AttributeAggregateDef>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AttributeAggregateDef {
    pub column_name: String,
    pub aggregate_type: AttributeAggregateType,
}

pub type VisualPointClustering = Operator<VisualPointClusteringParams, SingleVectorSource>;

#[typetag::serde]
#[async_trait]
impl VectorOperator for VisualPointClustering {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        ensure!(
            self.params.min_radius_px > 0.0,
            error::InputMustBeGreaterThanZero {
                name: "min_radius_px"
            }
        );
        ensure!(
            self.params.delta_px >= 0.0,
            error::InputMustBeZeroOrPositive { name: "delta_px" }
        );
        ensure!(!self.params.radius_column.is_empty(), error::EmptyInput);
        ensure!(
            self.params.count_column != self.params.radius_column,
            error::DuplicateOutputColumns
        );

        let radius_model = LogScaledRadius::new(self.params.min_radius_px, self.params.delta_px)?;

        let vector_source = self.sources.vector.initialize(context).await?;

        ensure!(
            vector_source.result_descriptor().data_type == VectorDataType::MultiPoint,
            error::InvalidType {
                expected: VectorDataType::MultiPoint.to_string(),
                found: vector_source.result_descriptor().data_type.to_string(),
            }
        );

        // check that all input columns exist
        for column_name in self.params.column_aggregates.keys() {
            let column_name_exists = vector_source
                .result_descriptor()
                .columns
                .contains_key(column_name);

            ensure!(
                column_name_exists,
                error::MissingInputColumn {
                    name: column_name.clone()
                }
            );
        }

        // check that there are no duplicates in the output columns
        let output_names: HashSet<&String> = self
            .params
            .column_aggregates
            .values()
            .map(|def| &def.column_name)
            .collect();
        ensure!(
            output_names.len() == self.params.column_aggregates.len(),
            error::DuplicateOutputColumns
        );

        // create schema for [`ResultDescriptor`]
        let mut new_columns: HashMap<String, FeatureDataType> =
            HashMap::with_capacity(self.params.column_aggregates.len());
        for attribute_aggregate_def in self.params.column_aggregates.values() {
            new_columns.insert(
                attribute_aggregate_def.column_name.clone(),
                match attribute_aggregate_def.aggregate_type {
                    AttributeAggregateType::MeanNumber => FeatureDataType::Float,
                    AttributeAggregateType::StringSample => FeatureDataType::Text,
                    AttributeAggregateType::Null => {
                        return Err(Error::InvalidType {
                            expected: "not null".to_string(),
                            found: "null".to_string(),
                        })
                    }
                },
            );
        }

        // check that output schema does not interfere with count and radius columns
        ensure!(
            !new_columns.contains_key(&self.params.radius_column)
                && !new_columns.contains_key(&self.params.count_column),
            error::DuplicateOutputColumns
        );

        Ok(InitializedVisualPointClustering {
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: vector_source.result_descriptor().spatial_reference,
                columns: new_columns,
            },
            vector_source,
            radius_model,
            radius_column: self.params.radius_column,
            count_column: self.params.count_column,
            attribute_mapping: self.params.column_aggregates,
        }
        .boxed())
    }
}

pub struct InitializedVisualPointClustering {
    result_descriptor: VectorResultDescriptor,
    vector_source: Box<dyn InitializedVectorOperator>,
    radius_model: LogScaledRadius,
    radius_column: String,
    count_column: String,
    attribute_mapping: HashMap<String, AttributeAggregateDef>,
}

impl InitializedVectorOperator for InitializedVisualPointClustering {
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        match self.vector_source.query_processor()? {
            TypedVectorQueryProcessor::MultiPoint(source) => {
                Ok(TypedVectorQueryProcessor::MultiPoint(
                    VisualPointClusteringProcessor::new(
                        source,
                        self.radius_model,
                        self.radius_column.clone(),
                        self.count_column.clone(),
                        self.result_descriptor.clone(),
                        self.attribute_mapping.clone(),
                    )
                    .boxed(),
                ))
            }
            _ => Err(error::Error::InvalidOperatorType),
        }
    }

    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }
}

pub struct VisualPointClusteringProcessor {
    source: Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
    radius_model: LogScaledRadius,
    radius_column: String,
    count_column: String,
    result_descriptor: VectorResultDescriptor,
    attribute_mapping: HashMap<String, AttributeAggregateDef>,
}

impl VisualPointClusteringProcessor {
    fn new(
        source: Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
        radius_model: LogScaledRadius,
        radius_column: String,
        count_column: String,
        result_descriptor: VectorResultDescriptor,
        attribute_mapping: HashMap<String, AttributeAggregateDef>,
    ) -> Self {
        Self {
            source,
            radius_model,
            radius_column,
            count_column,
            result_descriptor,
            attribute_mapping,
        }
    }

    fn create_point_collection(
        circles_of_points: impl Iterator<Item = CircleOfPoints>,
        radius_column: &str,
        count_column: &str,
        resolution: f64,
        columns: &HashMap<String, FeatureDataType>,
    ) -> Result<MultiPointCollection> {
        let mut builder = MultiPointCollection::builder();

        for (column, &data_type) in columns {
            builder.add_column(column.clone(), data_type)?;
        }

        builder.add_column(radius_column.to_string(), FeatureDataType::Float)?;
        builder.add_column(count_column.to_string(), FeatureDataType::Int)?;

        let mut builder = builder.finish_header();

        for circle_of_points in circles_of_points {
            builder.push_geometry(MultiPoint::new(vec![circle_of_points.circle.center()])?)?;
            builder.push_time_interval(TimeInterval::default())?; // TODO: add time from circle of points

            builder.push_data(
                radius_column,
                FeatureDataValue::Float(circle_of_points.circle.radius() / resolution),
            )?;
            builder.push_data(
                count_column,
                FeatureDataValue::Int(circle_of_points.number_of_points() as i64),
            )?;

            for (column, data_type) in circle_of_points.attribute_aggregates {
                match data_type {
                    AttributeAggregate::MeanNumber(mean_aggregator) => {
                        builder
                            .push_data(&column, FeatureDataValue::Float(mean_aggregator.mean))?;
                    }
                    AttributeAggregate::StringSample(string_sampler) => {
                        builder.push_data(
                            &column,
                            FeatureDataValue::Text(string_sampler.strings.join(", ")),
                        )?;
                    }
                    AttributeAggregate::Null => {
                        builder.push_null(&column)?;
                    }
                };
            }

            builder.finish_row();
        }

        builder.build().map_err(Into::into)
    }

    fn aggregate_from_feature_data(
        feature_data: FeatureDataValue,
        aggregate_type: AttributeAggregateType,
    ) -> AttributeAggregate {
        match (feature_data, aggregate_type) {
            (
                FeatureDataValue::Category(value) | FeatureDataValue::NullableCategory(Some(value)),
                AttributeAggregateType::MeanNumber,
            ) => AttributeAggregate::MeanNumber(MeanAggregator::from_value(f64::from(value))),
            (
                FeatureDataValue::Float(value) | FeatureDataValue::NullableFloat(Some(value)),
                AttributeAggregateType::MeanNumber,
            ) => AttributeAggregate::MeanNumber(MeanAggregator::from_value(value)),
            (
                FeatureDataValue::Int(value) | FeatureDataValue::NullableInt(Some(value)),
                AttributeAggregateType::MeanNumber,
            ) => AttributeAggregate::MeanNumber(MeanAggregator::from_value(value as f64)),
            (
                FeatureDataValue::Text(value) | FeatureDataValue::NullableText(Some(value)),
                AttributeAggregateType::StringSample,
            ) => AttributeAggregate::StringSample(StringSampler::from_value(value)),
            _ => AttributeAggregate::Null,
        }
    }
}

struct GridFoldState {
    grid: Grid<LogScaledRadius>,
    column_mapping: HashMap<String, AttributeAggregateDef>,
}

#[async_trait]
impl QueryProcessor for VisualPointClusteringProcessor {
    type Output = MultiPointCollection;
    type SpatialBounds = BoundingBox2D;

    async fn query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        // we aggregate all points into one collection

        let column_schema = self.result_descriptor.columns.clone();

        let joint_resolution = f64::max(query.spatial_resolution.x, query.spatial_resolution.y);
        let scaled_radius_model = self.radius_model.with_scaled_radii(joint_resolution)?;

        let initial_grid_fold_state = Result::<GridFoldState>::Ok(GridFoldState {
            grid: Grid::new(query.spatial_bounds, scaled_radius_model),
            column_mapping: self.attribute_mapping.clone(),
        });

        let grid_future = self.source.query(query, ctx).await?.fold(
            initial_grid_fold_state,
            |state, feature_collection| async move {
                // TODO: worker thread

                let GridFoldState {
                    mut grid,
                    column_mapping,
                } = state?;

                let feature_collection = feature_collection?;

                for feature in &feature_collection {
                    // TODO: pre-aggregate multi-points differently?
                    for coordinate in feature.geometry.points() {
                        let circle =
                            Circle::from_coordinate(coordinate, grid.radius_model().min_radius());

                        let mut attribute_aggregates = HashMap::with_capacity(column_mapping.len());

                        for (
                            src_column,
                            AttributeAggregateDef {
                                column_name: tgt_column,
                                aggregate_type,
                            },
                        ) in &column_mapping
                        {
                            let attribute_aggregate =
                                if let Some(feature_data) = feature.get(src_column) {
                                    Self::aggregate_from_feature_data(feature_data, *aggregate_type)
                                } else {
                                    AttributeAggregate::Null
                                };

                            attribute_aggregates.insert(tgt_column.clone(), attribute_aggregate);
                        }

                        grid.insert(CircleOfPoints::new_with_one_point(
                            circle,
                            attribute_aggregates,
                        ));
                    }
                }

                Ok(GridFoldState {
                    grid,
                    column_mapping,
                })
            },
        );

        let stream = FuturesUnordered::new();
        stream.push(grid_future);

        let stream = stream.map(move |grid| {
            let GridFoldState {
                grid,
                column_mapping: _,
            } = grid?;

            let mut cmq = CircleMergingQuadtree::new(query.spatial_bounds, *grid.radius_model(), 1);

            // TODO: worker thread
            for circle_of_points in grid.drain() {
                cmq.insert_circle(circle_of_points);
            }

            Self::create_point_collection(
                cmq.into_iter(),
                &self.radius_column,
                &self.count_column,
                joint_resolution,
                &column_schema,
            )
        });

        Ok(stream.merge_chunks(ctx.chunk_byte_size()).boxed())
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::primitives::FeatureData;
    use geoengine_datatypes::primitives::SpatialResolution;

    use crate::{
        engine::{MockExecutionContext, MockQueryContext},
        mock::MockFeatureCollectionSource,
    };

    use super::*;

    #[tokio::test]
    async fn simple_test() {
        let mut coordinates = vec![(0.0, 0.1); 9];
        coordinates.extend_from_slice(&[(50.0, 50.1)]);

        let input = MultiPointCollection::from_data(
            MultiPoint::many(coordinates).unwrap(),
            vec![TimeInterval::default(); 10],
            HashMap::default(),
        )
        .unwrap();

        let operator = VisualPointClustering {
            params: VisualPointClusteringParams {
                min_radius_px: 8.,
                delta_px: 1.,
                radius_column: "radius".to_string(),
                count_column: "count".to_string(),
                column_aggregates: Default::default(),
            },
            sources: SingleVectorSource {
                vector: MockFeatureCollectionSource::single(input).boxed(),
            },
        };

        let execution_context = MockExecutionContext::default();

        let initialized_operator = operator
            .boxed()
            .initialize(&execution_context)
            .await
            .unwrap();

        let query_processor = initialized_operator
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let query_context = MockQueryContext::default();

        let qrect = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
        };

        let query = query_processor.query(qrect, &query_context).await.unwrap();

        let result: Vec<MultiPointCollection> = query.map(Result::unwrap).collect().await;

        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            MultiPointCollection::from_slices(
                &[(0.0, 0.099_999_999_999_999_99), (50.0, 50.1)],
                &[TimeInterval::default(); 2],
                &[
                    ("count", FeatureData::Int(vec![9, 1])),
                    (
                        "radius",
                        FeatureData::Float(vec![10.197_224_577_336_218, 8.])
                    )
                ],
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn simple_test_with_aggregate() {
        let mut coordinates = vec![(0.0, 0.1); 9];
        coordinates.extend_from_slice(&[(50.0, 50.1)]);

        let input = MultiPointCollection::from_slices(
            &MultiPoint::many(coordinates).unwrap(),
            &[TimeInterval::default(); 10],
            &[(
                "foo",
                FeatureData::Float(vec![1., 2., 3., 4., 5., 6., 7., 8., 9., 10.]),
            )],
        )
        .unwrap();

        let operator = VisualPointClustering {
            params: VisualPointClusteringParams {
                min_radius_px: 8.,
                delta_px: 1.,
                radius_column: "radius".to_string(),
                count_column: "count".to_string(),
                column_aggregates: [(
                    "foo".to_string(),
                    AttributeAggregateDef {
                        column_name: "bar".to_string(),
                        aggregate_type: AttributeAggregateType::MeanNumber,
                    },
                )]
                .iter()
                .cloned()
                .collect(),
            },
            sources: SingleVectorSource {
                vector: MockFeatureCollectionSource::single(input).boxed(),
            },
        };

        let execution_context = MockExecutionContext::default();

        let initialized_operator = operator
            .boxed()
            .initialize(&execution_context)
            .await
            .unwrap();

        let query_processor = initialized_operator
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let query_context = MockQueryContext::default();

        let qrect = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
        };

        let query = query_processor.query(qrect, &query_context).await.unwrap();

        let result: Vec<MultiPointCollection> = query.map(Result::unwrap).collect().await;

        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            MultiPointCollection::from_slices(
                &[(0.0, 0.099_999_999_999_999_99), (50.0, 50.1)],
                &[TimeInterval::default(); 2],
                &[
                    ("count", FeatureData::Int(vec![9, 1])),
                    (
                        "radius",
                        FeatureData::Float(vec![10.197_224_577_336_218, 8.])
                    ),
                    ("bar", FeatureData::Float(vec![5., 10.]))
                ],
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn aggregate_of_null() {
        let mut coordinates = vec![(0.0, 0.1); 2];
        coordinates.extend_from_slice(&[(50.0, 50.1); 2]);

        let input = MultiPointCollection::from_slices(
            &MultiPoint::many(coordinates).unwrap(),
            &[TimeInterval::default(); 4],
            &[(
                "foo",
                FeatureData::NullableInt(vec![Some(1), None, None, None]),
            )],
        )
        .unwrap();

        let operator = VisualPointClustering {
            params: VisualPointClusteringParams {
                min_radius_px: 8.,
                delta_px: 1.,
                radius_column: "radius".to_string(),
                count_column: "count".to_string(),
                column_aggregates: [(
                    "foo".to_string(),
                    AttributeAggregateDef {
                        column_name: "foo".to_string(),
                        aggregate_type: AttributeAggregateType::MeanNumber,
                    },
                )]
                .iter()
                .cloned()
                .collect(),
            },
            sources: SingleVectorSource {
                vector: MockFeatureCollectionSource::single(input).boxed(),
            },
        };

        let execution_context = MockExecutionContext::default();

        let initialized_operator = operator
            .boxed()
            .initialize(&execution_context)
            .await
            .unwrap();

        let query_processor = initialized_operator
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let query_context = MockQueryContext::default();

        let qrect = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
        };

        let query = query_processor.query(qrect, &query_context).await.unwrap();

        let result: Vec<MultiPointCollection> = query.map(Result::unwrap).collect().await;

        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            MultiPointCollection::from_slices(
                &[(0.0, 0.1), (50.0, 50.1)],
                &[TimeInterval::default(); 2],
                &[
                    ("count", FeatureData::Int(vec![2, 2])),
                    (
                        "radius",
                        FeatureData::Float(vec![8.693_147_180_559_945, 8.693_147_180_559_945])
                    ),
                    ("foo", FeatureData::NullableFloat(vec![Some(1.), None]))
                ],
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn text_aggregate() {
        let mut coordinates = vec![(0.0, 0.1); 2];
        coordinates.extend_from_slice(&[(50.0, 50.1); 2]);
        coordinates.extend_from_slice(&[(25.0, 25.1); 2]);

        let input = MultiPointCollection::from_slices(
            &MultiPoint::many(coordinates).unwrap(),
            &[TimeInterval::default(); 6],
            &[(
                "text",
                FeatureData::NullableText(vec![
                    Some("foo".to_string()),
                    Some("bar".to_string()),
                    Some("foo".to_string()),
                    None,
                    None,
                    None,
                ]),
            )],
        )
        .unwrap();

        let operator = VisualPointClustering {
            params: VisualPointClusteringParams {
                min_radius_px: 8.,
                delta_px: 1.,
                radius_column: "radius".to_string(),
                count_column: "count".to_string(),
                column_aggregates: [(
                    "text".to_string(),
                    AttributeAggregateDef {
                        column_name: "text".to_string(),
                        aggregate_type: AttributeAggregateType::StringSample,
                    },
                )]
                .iter()
                .cloned()
                .collect(),
            },
            sources: SingleVectorSource {
                vector: MockFeatureCollectionSource::single(input).boxed(),
            },
        };

        let execution_context = MockExecutionContext::default();

        let initialized_operator = operator
            .boxed()
            .initialize(&execution_context)
            .await
            .unwrap();

        let query_processor = initialized_operator
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let query_context = MockQueryContext::default();

        let qrect = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
        };

        let query = query_processor.query(qrect, &query_context).await.unwrap();

        let result: Vec<MultiPointCollection> = query.map(Result::unwrap).collect().await;

        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            MultiPointCollection::from_slices(
                &[(0.0, 0.1), (50.0, 50.1), (25.0, 25.1)],
                &[TimeInterval::default(); 3],
                &[
                    ("count", FeatureData::Int(vec![2, 2, 2])),
                    (
                        "radius",
                        FeatureData::Float(vec![
                            8.693_147_180_559_945,
                            8.693_147_180_559_945,
                            8.693_147_180_559_945
                        ])
                    ),
                    (
                        "text",
                        FeatureData::NullableText(vec![
                            Some("foo, bar".to_string()),
                            Some("foo".to_string()),
                            None
                        ])
                    )
                ],
            )
            .unwrap()
        );
    }
}
