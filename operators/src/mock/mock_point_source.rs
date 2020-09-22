use crate::engine::{QueryContext, QueryProcessor, QueryRectangle};
use crate::{
    engine::{
        ExecutionContext, InitializedOperator, InitializedOperatorImpl, InitializedVectorOperator,
        SourceOperator, TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor,
        VectorResultDescriptor,
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
    fn query(
        &self,
        _query: QueryRectangle,
        ctx: QueryContext,
    ) -> BoxStream<Result<MultiPointCollection>> {
        let chunk_size = ctx.chunk_byte_size / std::mem::size_of::<Coordinate2D>();
        stream::iter(self.points.chunks(chunk_size).map(|chunk| {
            Ok(MultiPointCollection::from_data(
                chunk.iter().map(Into::into).collect(),
                vec![TimeInterval::default(); chunk.len()],
                HashMap::new(),
            )?)
        }))
        .boxed()
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
        context: ExecutionContext,
    ) -> Result<Box<InitializedVectorOperator>> {
        InitializedOperatorImpl::create(
            self.params,
            context,
            |_, _, _, _| Ok(()),
            |_, _, _, _, _| {
                Ok(VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::wgs84().into(),
                })
            },
            vec![],
            vec![],
        )
        .map(InitializedOperatorImpl::boxed)
    }
}

impl InitializedOperator<VectorResultDescriptor, TypedVectorQueryProcessor>
    for InitializedOperatorImpl<MockPointSourceParams, VectorResultDescriptor, ()>
{
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        Ok(TypedVectorQueryProcessor::MultiPoint(
            MockPointSourceProcessor {
                points: self.params.points.clone(),
            }
            .boxed(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on_stream;
    use geoengine_datatypes::primitives::BoundingBox2D;

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

        let _: Box<dyn VectorOperator> = serde_json::from_str(&serialized).unwrap();
    }

    #[test]
    fn execute() {
        let execution_context = ExecutionContext;
        let points = vec![Coordinate2D::new(1., 2.); 3];

        let mps = MockPointSource {
            params: MockPointSourceParams { points },
        }
        .boxed();
        let initialized = mps.initialize(execution_context).unwrap();

        let typed_processor = initialized.query_processor();
        let point_processor = match typed_processor {
            Ok(TypedVectorQueryProcessor::MultiPoint(processor)) => processor,
            _ => panic!(),
        };

        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new((0., 0.).into(), (4., 4.).into()).unwrap(),
            time_interval: TimeInterval::default(),
        };
        let ctx = QueryContext {
            chunk_byte_size: 2 * std::mem::size_of::<Coordinate2D>(),
        };
        let stream = point_processor.vector_query(query_rectangle, ctx);

        let blocking_stream = block_on_stream(stream);
        let collections: Vec<MultiPointCollection> = blocking_stream.map(Result::unwrap).collect();
        assert_eq!(collections.len(), 2);
        assert_eq!(collections[0].len(), 2);
        assert_eq!(collections[1].len(), 1);
    }
}
