use crate::engine::{CreateSpan, OperatorData, QueryContext};
use crate::{
    engine::{
        ExecutionContext, InitializedVectorOperator, OperatorName, SourceOperator,
        TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
    },
    util::Result,
};
use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::dataset::DataId;
use geoengine_datatypes::primitives::VectorQueryRectangle;
use geoengine_datatypes::{
    collections::MultiPointCollection,
    primitives::{Coordinate2D, TimeInterval},
    spatial_reference::SpatialReference,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{span, Level};

pub struct MockPointSourceProcessor {
    points: Vec<Coordinate2D>,
}

#[async_trait]
impl VectorQueryProcessor for MockPointSourceProcessor {
    type VectorType = MultiPointCollection;
    async fn vector_query<'a>(
        &'a self,
        _query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::VectorType>>> {
        let chunk_size = usize::from(ctx.chunk_byte_size()) / std::mem::size_of::<Coordinate2D>();
        Ok(
            stream::iter(self.points.chunks(chunk_size).map(move |chunk| {
                Ok(MultiPointCollection::from_data(
                    chunk.iter().map(Into::into).collect(),
                    vec![TimeInterval::default(); chunk.len()],
                    HashMap::new(),
                )?)
            }))
            .boxed(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MockPointSourceParams {
    pub points: Vec<Coordinate2D>,
}

pub type MockPointSource = SourceOperator<MockPointSourceParams>;

impl OperatorName for MockPointSource {
    const TYPE_NAME: &'static str = "MockPointSource";
}

impl OperatorData for MockPointSource {
    fn data_ids_collect(&self, _data_ids: &mut Vec<DataId>) {}
}

#[typetag::serde]
#[async_trait]
impl VectorOperator for MockPointSource {
    async fn _initialize(
        self: Box<Self>,
        _context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        Ok(InitializedMockPointSource {
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: Default::default(),
                time: None,
                bbox: None,
            },
            points: self.params.points,
        }
        .boxed())
    }

    fn span(&self) -> CreateSpan {
        || span!(Level::TRACE, MockPointSource::TYPE_NAME)
    }
}

pub struct InitializedMockPointSource {
    result_descriptor: VectorResultDescriptor,
    points: Vec<Coordinate2D>,
}

impl InitializedVectorOperator for InitializedMockPointSource {
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        Ok(TypedVectorQueryProcessor::MultiPoint(
            MockPointSourceProcessor {
                points: self.points.clone(),
            }
            .boxed(),
        ))
    }

    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::QueryProcessor;
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use futures::executor::block_on_stream;
    use geoengine_datatypes::collections::FeatureCollectionInfos;
    use geoengine_datatypes::primitives::{BoundingBox2D, SpatialResolution};
    use geoengine_datatypes::util::test::TestDefault;

    #[test]
    fn serde() {
        let points = vec![Coordinate2D::new(1., 2.); 3];

        let mps = MockPointSource {
            params: MockPointSourceParams { points },
        }
        .boxed();
        let serialized = serde_json::to_string(&mps).unwrap();
        let expect = "{\"type\":\"MockPointSource\",\"params\":{\"points\":[{\"x\":1.0,\"y\":2.0},{\"x\":1.0,\"y\":2.0},{\"x\":1.0,\"y\":2.0}]}}";
        assert_eq!(serialized, expect);

        let _operator: Box<dyn VectorOperator> = serde_json::from_str(&serialized).unwrap();
    }

    #[tokio::test]
    async fn execute() {
        let execution_context = MockExecutionContext::test_default();
        let points = vec![Coordinate2D::new(1., 2.); 3];

        let mps = MockPointSource {
            params: MockPointSourceParams { points },
        }
        .boxed();
        let initialized = mps.initialize(&execution_context).await.unwrap();

        let typed_processor = initialized.query_processor();
        let point_processor = match typed_processor {
            Ok(TypedVectorQueryProcessor::MultiPoint(processor)) => processor,
            _ => panic!(),
        };

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((0., 0.).into(), (4., 4.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = MockQueryContext::new((2 * std::mem::size_of::<Coordinate2D>()).into());

        let stream = point_processor.query(query_rectangle, &ctx).await.unwrap();

        let blocking_stream = block_on_stream(stream);
        let collections: Vec<MultiPointCollection> = blocking_stream.map(Result::unwrap).collect();
        assert_eq!(collections.len(), 2);
        assert_eq!(collections[0].len(), 2);
        assert_eq!(collections[1].len(), 1);
    }
}
