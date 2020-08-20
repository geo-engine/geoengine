use crate::engine::{QueryContext, QueryProcessor, QueryRectangle};
use crate::{
    engine::{Operator, TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor},
    util::Result,
};
use futures::stream::{self, BoxStream, StreamExt};
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::primitives::MultiPoint;
use geoengine_datatypes::{
    collections::MultiPointCollection,
    primitives::{Coordinate2D, TimeInterval},
    projection::{Projection, ProjectionOption},
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
                chunk
                    .iter()
                    .map(|x| MultiPoint::new(vec![*x]).expect("cannot fail"))
                    .collect(),
                vec![TimeInterval::default(); chunk.len()],
                HashMap::new(),
            )?)
        }))
        .boxed()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MockPointSource {
    pub points: Vec<Coordinate2D>,
}

impl Operator for MockPointSource {
    fn raster_sources(&self) -> &[Box<dyn crate::engine::RasterOperator>] {
        &[]
    }
    fn vector_sources(&self) -> &[Box<dyn crate::engine::VectorOperator>] {
        &[]
    }
    fn projection(&self) -> ProjectionOption {
        ProjectionOption::Projection(Projection::wgs84())
    }
}

#[typetag::serde]
impl VectorOperator for MockPointSource {
    fn result_type(&self) -> VectorDataType {
        VectorDataType::MultiPoint
    }

    fn vector_processor(&self) -> Result<crate::engine::TypedVectorQueryProcessor> {
        Ok(TypedVectorQueryProcessor::MultiPoint(
            MockPointSourceProcessor {
                points: self.points.clone(),
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

        let mps = MockPointSource { points }.boxed();
        let serialized = serde_json::to_string(&mps).unwrap();
        let expect = "{\"type\":\"MockPointSource\",\"points\":[{\"x\":1.0,\"y\":2.0},{\"x\":1.0,\"y\":2.0},{\"x\":1.0,\"y\":2.0}]}";
        assert_eq!(serialized, expect);

        let _: Box<dyn VectorOperator> = serde_json::from_str(&serialized).unwrap();
    }

    #[test]
    fn execute() {
        let points = vec![Coordinate2D::new(1., 2.); 3];

        let mps = MockPointSource { points }.boxed();
        let typed_processor = mps.vector_processor();
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
