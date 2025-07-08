use crate::engine::{CanonicOperatorName, OperatorData, QueryContext};
use crate::{
    engine::{
        ExecutionContext, InitializedVectorOperator, OperatorName, SourceOperator,
        TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
        WorkflowOperatorPath,
    },
    util::Result,
};
use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::dataset::NamedData;
use geoengine_datatypes::primitives::{BoundingBox2D, CacheHint, VectorQueryRectangle};
use geoengine_datatypes::{
    collections::MultiPointCollection,
    primitives::{Coordinate2D, TimeInterval},
    spatial_reference::SpatialReference,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub struct MockPointSourceProcessor {
    result_descriptor: VectorResultDescriptor,
    points: Vec<Coordinate2D>,
}

#[async_trait]
impl VectorQueryProcessor for MockPointSourceProcessor {
    type VectorType = MultiPointCollection;
    async fn vector_query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::VectorType>>> {
        let chunk_size = usize::from(ctx.chunk_byte_size()) / std::mem::size_of::<Coordinate2D>();
        let spatial_query = query.spatial_bounds();

        Ok(stream::iter(&self.points)
            .filter(move |&coord| std::future::ready(spatial_query.contains_coordinate(coord)))
            .chunks(chunk_size)
            .map(move |chunk| {
                Ok(MultiPointCollection::from_data(
                    chunk.iter().copied().map(Into::into).collect(),
                    vec![TimeInterval::default(); chunk.len()],
                    HashMap::new(),
                    CacheHint::max_duration(),
                )?)
            })
            .boxed())
    }

    fn vector_result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum SpatialBoundsDerive {
    Derive,
    Bounds(BoundingBox2D),
    None,
}

impl Default for SpatialBoundsDerive {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MockPointSourceParams {
    pub points: Vec<Coordinate2D>,
    #[serde(default = "SpatialBoundsDerive::default")]
    pub spatial_bounds: SpatialBoundsDerive,
}

impl MockPointSourceParams {
    pub fn new(points: Vec<Coordinate2D>) -> Self {
        MockPointSourceParams {
            points,
            spatial_bounds: SpatialBoundsDerive::default(),
        }
    }

    pub fn new_with_bounds(points: Vec<Coordinate2D>, spatial_bounds: SpatialBoundsDerive) -> Self {
        MockPointSourceParams {
            points,
            spatial_bounds,
        }
    }
}

pub type MockPointSource = SourceOperator<MockPointSourceParams>;

impl OperatorName for MockPointSource {
    const TYPE_NAME: &'static str = "MockPointSource";
}

impl OperatorData for MockPointSource {
    fn data_names_collect(&self, _data_names: &mut Vec<NamedData>) {}
}

#[typetag::serde]
#[async_trait]
impl VectorOperator for MockPointSource {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        _context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        let bounds = match self.params.spatial_bounds {
            SpatialBoundsDerive::None => None,
            SpatialBoundsDerive::Bounds(b) => Some(b),
            SpatialBoundsDerive::Derive => {
                BoundingBox2D::from_coord_ref_iter(self.params.points.iter())
            }
        };

        Ok(InitializedMockPointSource {
            name: CanonicOperatorName::from(&self),
            path,
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: Default::default(),
                time: None,
                bbox: bounds,
            },
            points: self.params.points,
        }
        .boxed())
    }

    span_fn!(MockPointSource);
}

pub struct InitializedMockPointSource {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    result_descriptor: VectorResultDescriptor,
    points: Vec<Coordinate2D>,
}

impl InitializedVectorOperator for InitializedMockPointSource {
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        Ok(TypedVectorQueryProcessor::MultiPoint(
            MockPointSourceProcessor {
                result_descriptor: self.result_descriptor.clone(),
                points: self.points.clone(),
            }
            .boxed(),
        ))
    }

    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        MockPointSource::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::MockExecutionContext;
    use crate::engine::QueryProcessor;
    use futures::executor::block_on_stream;
    use geoengine_datatypes::collections::FeatureCollectionInfos;
    use geoengine_datatypes::primitives::{BoundingBox2D, ColumnSelection};
    use geoengine_datatypes::util::test::TestDefault;

    #[test]
    fn serde() {
        let points = vec![Coordinate2D::new(1., 2.); 3];

        let mps = MockPointSource {
            params: MockPointSourceParams::new(points),
        }
        .boxed();
        let serialized = serde_json::to_string(&mps).unwrap();
        let expect = "{\"type\":\"MockPointSource\",\"params\":{\"points\":[{\"x\":1.0,\"y\":2.0},{\"x\":1.0,\"y\":2.0},{\"x\":1.0,\"y\":2.0}],\"spatialBounds\":{\"type\":\"none\"}}}";
        assert_eq!(serialized, expect);

        let _operator: Box<dyn VectorOperator> = serde_json::from_str(&serialized).unwrap();
    }

    #[tokio::test]
    async fn execute() {
        let execution_context = MockExecutionContext::test_default();
        let points = vec![Coordinate2D::new(1., 2.); 3];

        let mps = MockPointSource {
            params: MockPointSourceParams::new(points),
        }
        .boxed();
        let initialized = mps
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let typed_processor = initialized.query_processor();
        let Ok(TypedVectorQueryProcessor::MultiPoint(point_processor)) = typed_processor else {
            panic!()
        };

        let query_rectangle = VectorQueryRectangle::with_bounds(
            BoundingBox2D::new((0., 0.).into(), (4., 4.).into()).unwrap(),
            TimeInterval::default(),
            ColumnSelection::all(),
        );
        let ctx =
            execution_context.mock_query_context((2 * std::mem::size_of::<Coordinate2D>()).into());

        let stream = point_processor.query(query_rectangle, &ctx).await.unwrap();

        let blocking_stream = block_on_stream(stream);
        let collections: Vec<MultiPointCollection> = blocking_stream.map(Result::unwrap).collect();
        assert_eq!(collections.len(), 2);
        assert_eq!(collections[0].len(), 2);
        assert_eq!(collections[1].len(), 1);
    }
}
