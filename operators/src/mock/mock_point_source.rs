use crate::engine::{QueryContext, QueryProcessor, QueryRectangle};
use crate::{
    engine::{
        ExecutionContext, InitializedOperator, InitializedVectorOperator, SourceOperator,
        TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
    },
    util::Result,
};
use futures::stream::{self, BoxStream, StreamExt};
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::{
    collections::MultiPointCollection,
    primitives::{Coordinate2D, TimeInterval},
    spatial_reference::SpatialReference,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub struct MockPointSourceProcessor {
    points: Vec<Coordinate2D>,
}

impl QueryProcessor for MockPointSourceProcessor {
    type Output = MultiPointCollection;
    fn query<'a>(
        &'a self,
        _query: QueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<MultiPointCollection>>> {
        let chunk_size = ctx.chunk_byte_size() / std::mem::size_of::<Coordinate2D>();
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

#[typetag::serde]
impl VectorOperator for MockPointSource {
    fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<InitializedVectorOperator>> {
        Ok(InitializedMockPointSource {
            result_descritor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: Default::default(),
            },
            points: self.params.points,
        }
        .boxed())
    }
}

pub struct InitializedMockPointSource {
    result_descritor: VectorResultDescriptor,
    points: Vec<Coordinate2D>,
}

impl InitializedOperator<VectorResultDescriptor, TypedVectorQueryProcessor>
    for InitializedMockPointSource
{
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        Ok(TypedVectorQueryProcessor::MultiPoint(
            MockPointSourceProcessor {
                points: self.points.clone(),
            }
            .boxed(),
        ))
    }

    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descritor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use futures::executor::block_on_stream;
    use geoengine_datatypes::collections::FeatureCollectionInfos;
    use geoengine_datatypes::primitives::{BoundingBox2D, SpatialResolution};

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

    #[test]
    fn execute() {
        let execution_context = MockExecutionContext::default();
        let points = vec![Coordinate2D::new(1., 2.); 3];

        let mps = MockPointSource {
            params: MockPointSourceParams { points },
        }
        .boxed();
        let initialized = mps.initialize(&execution_context).unwrap();

        let typed_processor = initialized.query_processor();
        let point_processor = match typed_processor {
            Ok(TypedVectorQueryProcessor::MultiPoint(processor)) => processor,
            _ => panic!(),
        };

        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new((0., 0.).into(), (4., 4.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = MockQueryContext::new(2 * std::mem::size_of::<Coordinate2D>());

        let stream = point_processor.vector_query(query_rectangle, &ctx).unwrap();

        let blocking_stream = block_on_stream(stream);
        let collections: Vec<MultiPointCollection> = blocking_stream.map(Result::unwrap).collect();
        assert_eq!(collections.len(), 2);
        assert_eq!(collections[0].len(), 2);
        assert_eq!(collections[1].len(), 1);
    }
}
