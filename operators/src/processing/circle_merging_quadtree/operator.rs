use std::collections::HashMap;

use async_trait::async_trait;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::StreamExt;
use geoengine_datatypes::collections::{
    BuilderProvider, GeoFeatureCollectionRowBuilder, MultiPointCollection, VectorDataType,
};
use geoengine_datatypes::primitives::{
    BoundingBox2D, FeatureDataType, FeatureDataValue, MultiPoint, MultiPointAccess, TimeInterval,
};
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::adapters::FeatureCollectionStreamExt;
use crate::engine::{
    ExecutionContext, InitializedVectorOperator, Operator, QueryContext, QueryProcessor,
    SingleVectorSource, TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor,
    VectorQueryRectangle, VectorResultDescriptor,
};
use crate::error;
use crate::processing::circle_merging_quadtree::circle_of_points::CircleOfPoints;
use crate::processing::circle_merging_quadtree::circle_radius_model::LogScaledRadius;
use crate::util::Result;

use super::grid::Grid;
use super::quadtree::CircleMergingQuadtree;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct VisualPointClusteringParams {
    pub min_radius_px: f64,
    pub delta_px: f64,
    pub resolution: f64,
    radius_column: String,
    count_column: String,
    // TODO: column mapping for aggregation
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
        ensure!(
            self.params.resolution > 0.0,
            error::InputMustBeGreaterThanZero { name: "resolution" }
        );
        ensure!(!self.params.radius_column.is_empty(), error::EmptyInput);

        let radius_model = LogScaledRadius::new(self.params.min_radius_px, self.params.delta_px)?;

        let vector_source = self.sources.vector.initialize(context).await?;

        ensure!(
            vector_source.result_descriptor().data_type == VectorDataType::MultiPoint,
            error::InvalidType {
                expected: VectorDataType::MultiPoint.to_string(),
                found: vector_source.result_descriptor().data_type.to_string(),
            }
        );

        Ok(InitializedVisualPointClustering {
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: vector_source.result_descriptor().spatial_reference,
                columns: HashMap::new(), // TODO: add columns to descriptor
            },
            vector_source,
            radius_model,
            radius_column: self.params.radius_column,
            count_column: self.params.count_column,
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
}

impl VisualPointClusteringProcessor {
    fn new(
        source: Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
        radius_model: LogScaledRadius,
        radius_column: String,
        count_column: String,
    ) -> Self {
        Self {
            source,
            radius_model,
            radius_column,
            count_column,
        }
    }

    fn create_point_collection(
        circles_of_points: impl Iterator<Item = CircleOfPoints>,
        radius_column: &str,
        count_column: &str,
    ) -> Result<MultiPointCollection> {
        let mut builder = MultiPointCollection::builder();

        // TODO: add columns

        builder.add_column(radius_column.to_string(), FeatureDataType::Float)?;
        builder.add_column(count_column.to_string(), FeatureDataType::Int)?;

        let mut builder = builder.finish_header();

        for circle_of_points in circles_of_points {
            builder.push_geometry(MultiPoint::new(vec![circle_of_points.circle.center()])?)?;
            builder.push_time_interval(TimeInterval::default())?; // TODO: add time from circle of points

            builder.push_data(
                radius_column,
                FeatureDataValue::Float(circle_of_points.circle.radius()),
            )?;
            builder.push_data(
                count_column,
                FeatureDataValue::Int(circle_of_points.number_of_points() as i64),
            )?;

            // TODO: add column data

            builder.finish_row();
        }

        builder.build().map_err(Into::into)
    }
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

        // TODO: lower resolution on memory bound to reduce maximum number of points

        let grid_future = self.source.query(query, ctx).await?.fold(
            Result::<Grid<_>>::Ok(Grid::new(query.spatial_bounds, self.radius_model)),
            |grid, feature_collection| async move {
                // TODO: worker thread

                let mut grid = grid?;
                let feature_collection = feature_collection?;

                for feature in &feature_collection {
                    // TODO: pre-aggregate multi-points differently?
                    for coordiante in feature.geometry.points() {
                        grid.insert_coordinate(*coordiante);
                    }
                }

                Ok(grid)
            },
        );

        let stream = FuturesUnordered::new();
        stream.push(grid_future);

        let stream = stream.map(move |grid| {
            let grid = grid?;

            let mut cmq = CircleMergingQuadtree::new(query.spatial_bounds, *grid.radius_model(), 1);

            // TODO: worker thread
            for circle_of_points in grid.drain() {
                cmq.insert_circle(circle_of_points);
            }

            Self::create_point_collection(cmq.into_iter(), &self.radius_column, &self.count_column)
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
                resolution: 0.1,
                radius_column: "radius".to_string(),
                count_column: "count".to_string(),
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
                        FeatureData::Float(vec![10.197_224_577_336_22, 8.])
                    )
                ],
            )
            .unwrap()
        );
    }
}
