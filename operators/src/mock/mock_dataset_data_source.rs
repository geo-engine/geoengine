use crate::engine::{
    ExecutionContext, InitializedOperator, InitializedOperatorImpl, InitializedVectorOperator,
    MetaData, QueryContext, QueryProcessor, QueryRectangle, SourceOperator,
    TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
};
use crate::util::Result;
use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::collections::{MultiPointCollection, VectorDataType};
use geoengine_datatypes::dataset::DatasetId;
use geoengine_datatypes::primitives::{Coordinate2D, TimeInterval};
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// TODO: generify this to support all data types
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct MockDatasetDataSourceLoadingInfo {
    pub points: Vec<Coordinate2D>,
}

impl MetaData<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>
    for MockDatasetDataSourceLoadingInfo
{
    fn loading_info(&self, _query: QueryRectangle) -> Result<MockDatasetDataSourceLoadingInfo> {
        Ok(self.clone()) // TODO: intersect points with query rectangle
    }

    fn result_descriptor(&self) -> Result<VectorResultDescriptor> {
        Ok(VectorResultDescriptor {
            data_type: VectorDataType::MultiPoint,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
        })
    }

    fn box_clone(
        &self,
    ) -> Box<dyn MetaData<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>> {
        Box::new(self.clone())
    }
}

// impl LoadingInfoProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>
//     for MockExecutionContext
// {
//     fn loading_info(
//         &self,
//         _dataset: &DatasetId,
//     ) -> Result<Box<dyn LoadingInfo<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>>>
//     {
//         Ok(Box::new(self.loading_info.as_ref().unwrap().clone())
//             as Box<
//                 dyn LoadingInfo<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>,
//             >)
//     }
// }

pub struct MockDatasetDataSourceProcessor {
    loading_info: Box<dyn MetaData<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>>,
}

impl QueryProcessor for MockDatasetDataSourceProcessor {
    type Output = MultiPointCollection;
    fn query<'a>(
        &'a self,
        query: QueryRectangle,
        _ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<MultiPointCollection>>> {
        // TODO: split into `chunk_byte_size`d chunks
        // let chunk_size = ctx.chunk_byte_size() / std::mem::size_of::<Coordinate2D>();

        let loading_info = self.loading_info.loading_info(query)?;

        Ok(stream::once(async move {
            Ok(MultiPointCollection::from_data(
                loading_info.points.iter().map(Into::into).collect(),
                vec![TimeInterval::default(); loading_info.points.len()],
                HashMap::new(),
            )?)
        })
        .boxed())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MockDatasetDataSourceParams {
    pub dataset: DatasetId,
}

pub type MockDatasetDataSource = SourceOperator<MockDatasetDataSourceParams>;

#[typetag::serde]
impl VectorOperator for MockDatasetDataSource {
    fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<InitializedVectorOperator>> {
        let loading_info = context.meta_data(&self.params.dataset)?;
        Ok(Box::new(InitializedOperatorImpl {
            raster_sources: vec![],
            vector_sources: vec![],
            result_descriptor: loading_info.result_descriptor()?,
            state: loading_info,
        }))
    }
}

impl InitializedOperator<VectorResultDescriptor, TypedVectorQueryProcessor>
    for InitializedOperatorImpl<
        VectorResultDescriptor,
        Box<dyn MetaData<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>>,
    >
{
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        Ok(TypedVectorQueryProcessor::MultiPoint(
            MockDatasetDataSourceProcessor {
                loading_info: self.state.clone(),
            }
            .boxed(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use futures::executor::block_on_stream;
    use geoengine_datatypes::collections::FeatureCollectionInfos;
    use geoengine_datatypes::dataset::InternalDatasetId;
    use geoengine_datatypes::primitives::{BoundingBox2D, SpatialResolution};
    use geoengine_datatypes::util::Identifier;

    #[test]
    fn test() {
        let mut execution_context = MockExecutionContext::default();

        let id = DatasetId::Internal(InternalDatasetId::new());
        execution_context.add_meta_data(
            id.clone(),
            Box::new(MockDatasetDataSourceLoadingInfo {
                points: vec![Coordinate2D::new(1., 2.); 3],
            }),
        );

        let mps = MockDatasetDataSource {
            params: MockDatasetDataSourceParams { dataset: id },
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
        assert_eq!(collections.len(), 1);
        assert_eq!(collections[0].len(), 3);
    }
}
