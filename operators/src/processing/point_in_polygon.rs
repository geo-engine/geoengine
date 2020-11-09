use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use snafu::ensure;

use geoengine_datatypes::collections::{
    BuilderProvider, DataCollection, MultiPointCollection, MultiPolygonCollection,
    TypedFeatureCollection, VectorDataType,
};
use geoengine_datatypes::primitives::{FeatureDataRef, FeatureDataType};

use crate::engine::{
    ExecutionContext, InitializedOperator, InitializedOperatorImpl, InitializedVectorOperator,
    Operator, QueryContext, QueryProcessor, QueryRectangle, TypedVectorQueryProcessor,
    VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
};
use crate::error;
use crate::opencl::{CLProgram, ColumnArgument, CompiledCLProgram, IterationType, VectorArgument};
use crate::util::Result;
use std::sync::{Arc, Mutex};

pub type PointInPolygonFilter = Operator<()>;

#[typetag::serde]
impl VectorOperator for PointInPolygonFilter {
    fn initialize(
        self: Box<Self>,
        context: &ExecutionContext,
    ) -> Result<Box<InitializedVectorOperator>> {
        ensure!(
            self.vector_sources.len() == 2,
            error::InvalidNumberOfVectorInputs {
                expected: 2..3,
                found: self.vector_sources.len()
            }
        );
        ensure!(
            self.raster_sources.is_empty(),
            error::InvalidNumberOfRasterInputs {
                expected: 0..1,
                found: self.raster_sources.len()
            }
        );

        let vector_sources = self
            .vector_sources
            .into_iter()
            .map(|o| o.initialize(context))
            .collect::<Result<Vec<Box<InitializedVectorOperator>>>>()?;

        ensure!(
            vector_sources[0].result_descriptor().data_type == VectorDataType::MultiPoint,
            error::InvalidType {
                expected: VectorDataType::MultiPoint.to_string(),
                found: vector_sources[0].result_descriptor().data_type.to_string(),
            }
        );
        ensure!(
            vector_sources[1].result_descriptor().data_type == VectorDataType::MultiPolygon,
            error::InvalidType {
                expected: VectorDataType::MultiPolygon.to_string(),
                found: vector_sources[1].result_descriptor().data_type.to_string(),
            }
        );

        Ok(InitializedPointInPolygonFilter::new(
            (),
            vector_sources[0].result_descriptor(),
            vec![],
            vector_sources,
            (),
        )
        .boxed())
    }
}

pub type InitializedPointInPolygonFilter = InitializedOperatorImpl<(), VectorResultDescriptor, ()>;

impl InitializedOperator<VectorResultDescriptor, TypedVectorQueryProcessor>
    for InitializedPointInPolygonFilter
{
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        let point_processor = if let TypedVectorQueryProcessor::MultiPoint(point_processor) =
            self.vector_sources[0].query_processor()?
        {
            point_processor
        } else {
            unreachable!("checked in `PointInPolygonFilter` constructor");
        };

        let polygon_processor = if let TypedVectorQueryProcessor::MultiPolygon(polygon_processor) =
            self.vector_sources[1].query_processor()?
        {
            polygon_processor
        } else {
            unreachable!("checked in `PointInPolygonFilter` constructor");
        };

        Ok(TypedVectorQueryProcessor::MultiPoint(
            PointInPolygonFilterProcessor::new(point_processor, polygon_processor).boxed(),
        ))
    }
}

pub struct PointInPolygonFilterProcessor {
    points: Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
    polygons: Box<dyn VectorQueryProcessor<VectorType = MultiPolygonCollection>>,
}

impl PointInPolygonFilterProcessor {
    pub fn new(
        points: Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
        polygons: Box<dyn VectorQueryProcessor<VectorType = MultiPolygonCollection>>,
    ) -> Self {
        Self { points, polygons }
    }

    fn filter_points(
        points: &TypedFeatureCollection,
        polygons: &TypedFeatureCollection,
        initial_filter: &[bool],
        cl_program: &mut CompiledCLProgram,
    ) -> Result<Vec<bool>> {
        let mut runnable = cl_program.runnable();

        runnable.set_input_features(0, points)?;
        runnable.set_input_features(1, polygons)?;

        let mut filter_builder = DataCollection::builder();
        filter_builder.add_column("filter".to_string(), FeatureDataType::Decimal)?;
        let mut filter_builder = filter_builder.batch_builder(initial_filter.len(), 0);

        runnable.set_output_features(0, &mut filter_builder)?;

        cl_program.run(runnable)?;

        filter_builder.finish_data()?;
        let filter_collection = filter_builder
            .output
            .expect("must exist")
            .get_data()
            .expect("must be a data collection");

        let filter_column = filter_collection.data("filter")?;
        let filter = match &filter_column {
            FeatureDataRef::Decimal(f) => f.as_ref(),
            _ => unreachable!("must be of decimal type"),
        };

        // TODO: use arrow directly?
        Ok(initial_filter
            .iter()
            .zip(filter.iter().map(|&b| b != 0))
            .map(|(&a, b)| a || b)
            .collect())
    }
}

impl VectorQueryProcessor for PointInPolygonFilterProcessor {
    type VectorType = MultiPointCollection;

    fn vector_query(
        &self,
        query: QueryRectangle,
        ctx: QueryContext,
    ) -> BoxStream<'_, Result<Self::VectorType>> {
        let mut cl_program = CLProgram::new(IterationType::VectorCoordinates);
        cl_program.add_input_features(VectorArgument::new(
            VectorDataType::MultiPoint,
            vec![],
            true,
            true,
        ));
        cl_program.add_input_features(VectorArgument::new(
            VectorDataType::MultiPolygon,
            vec![],
            true,
            true,
        ));
        cl_program.add_output_features(VectorArgument::new(
            VectorDataType::Data,
            vec![ColumnArgument::new(
                "filter".to_string(),
                FeatureDataType::Decimal,
            )],
            false,
            false,
        ));

        let cl_program =
            match cl_program.compile(include_str!("point_in_polygon.cl"), "point_in_polygon") {
                Ok(cl_program) => Arc::new(Mutex::new(cl_program)),
                Err(error) => return futures::stream::once(async { Err(error) }).boxed(),
            };

        self.points
            .query(query, ctx)
            .map(move |points| points.map(|p| (p, cl_program.clone())))
            .and_then(move |(points, cl_program)| async move {
                let initial_filter = vec![false; points.len()];
                let points = TypedFeatureCollection::MultiPoint(points);

                let filter = self
                    .polygons
                    .query(query, ctx)
                    .fold(Ok(initial_filter), |filter, polygons| async {
                        let polygons = polygons?;

                        if polygons.is_empty() {
                            return filter;
                        }

                        let polygons = TypedFeatureCollection::MultiPolygon(polygons);
                        Self::filter_points(
                            &points,
                            &polygons,
                            &filter?,
                            &mut cl_program.lock().unwrap(),
                        )
                    })
                    .await?;

                let points = points.get_points().expect("unpack");
                points.filter(filter).map_err(Into::into)
            })
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::primitives::{
        BoundingBox2D, MultiPoint, MultiPolygon, SpatialResolution, TimeInterval,
    };

    use crate::mock::{MockFeatureCollectionSource, MockFeatureCollectionSourceParams};

    use super::*;

    #[tokio::test]
    async fn all() -> Result<()> {
        let points = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1), (2.0, 3.1)]).unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 3],
            Default::default(),
        )?;

        let point_source = MockFeatureCollectionSource {
            params: MockFeatureCollectionSourceParams {
                collection: points.clone(),
            },
        }
        .boxed();

        let polygon_source = MockFeatureCollectionSource {
            params: MockFeatureCollectionSourceParams {
                collection: MultiPolygonCollection::from_data(
                    vec![MultiPolygon::new(vec![vec![vec![
                        (0.0, 0.0).into(),
                        (0.0, 10.0).into(),
                        (10.0, 10.0).into(),
                        (10.0, 0.0).into(),
                        (0.0, 0.0).into(),
                    ]]])?],
                    vec![TimeInterval::new_unchecked(0, 1); 1],
                    Default::default(),
                )?,
            },
        }
        .boxed();

        let operator = PointInPolygonFilter {
            vector_sources: vec![point_source, polygon_source],
            raster_sources: vec![],
            params: (),
        }
        .boxed()
        .initialize(&ExecutionContext::mock_empty())?;

        let query_processor = operator.query_processor()?.multi_point().unwrap();

        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = QueryContext {
            chunk_byte_size: usize::MAX,
        };

        let query = query_processor.query(query_rectangle, ctx);

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        assert_eq!(result[0], points);

        Ok(())
    }

    #[tokio::test]
    async fn none() -> Result<()> {
        let points = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1), (2.0, 3.1)]).unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 3],
            Default::default(),
        )?;

        let point_source = MockFeatureCollectionSource {
            params: MockFeatureCollectionSourceParams {
                collection: points.clone(),
            },
        }
        .boxed();

        let polygon_source = MockFeatureCollectionSource {
            params: MockFeatureCollectionSourceParams {
                collection: MultiPolygonCollection::from_data(vec![], vec![], Default::default())?,
            },
        }
        .boxed();

        let operator = PointInPolygonFilter {
            vector_sources: vec![point_source, polygon_source],
            raster_sources: vec![],
            params: (),
        }
        .boxed()
        .initialize(&ExecutionContext::mock_empty())?;

        let query_processor = operator.query_processor()?.multi_point().unwrap();

        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = QueryContext {
            chunk_byte_size: usize::MAX,
        };

        let query = query_processor.query(query_rectangle, ctx);

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        assert_eq!(result[0], MultiPointCollection::empty());

        Ok(())
    }
}
