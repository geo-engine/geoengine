use crate::engine::{
    ExecutionContext, InitializedVectorOperator, MetaData, OperatorDatasets, QueryContext,
    ResultDescriptor, SourceOperator, TypedVectorQueryProcessor, VectorOperator,
    VectorQueryProcessor, VectorResultDescriptor,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::collections::{MultiPointCollection, VectorDataType};
use geoengine_datatypes::dataset::DatasetId;
use geoengine_datatypes::primitives::{Coordinate2D, TimeInterval, VectorQueryRectangle};
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// TODO: generify this to support all data types
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct MockDatasetDataSourceLoadingInfo {
    pub points: Vec<Coordinate2D>,
}

#[async_trait]
impl MetaData<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for MockDatasetDataSourceLoadingInfo
{
    async fn loading_info(
        &self,
        _query: VectorQueryRectangle,
    ) -> Result<MockDatasetDataSourceLoadingInfo> {
        Ok(self.clone()) // TODO: intersect points with query rectangle
    }

    async fn result_descriptor(&self) -> Result<VectorResultDescriptor> {
        Ok(VectorResultDescriptor {
            data_type: VectorDataType::MultiPoint,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
        })
    }

    fn box_clone(
        &self,
    ) -> Box<
        dyn MetaData<
            MockDatasetDataSourceLoadingInfo,
            VectorResultDescriptor,
            VectorQueryRectangle,
        >,
    > {
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
    loading_info: Box<
        dyn MetaData<
            MockDatasetDataSourceLoadingInfo,
            VectorResultDescriptor,
            VectorQueryRectangle,
        >,
    >,
}

#[async_trait]
impl VectorQueryProcessor for MockDatasetDataSourceProcessor {
    type VectorType = MultiPointCollection;
    async fn vector_query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        _ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::VectorType>>> {
        // TODO: split into `chunk_byte_size`d chunks
        // let chunk_size = ctx.chunk_byte_size() / std::mem::size_of::<Coordinate2D>();

        let loading_info = self.loading_info.loading_info(query).await?;

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
#[async_trait]
impl VectorOperator for MockDatasetDataSource {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        let loading_info = context.meta_data(&self.params.dataset).await?;

        Ok(InitializedMockDatasetDataSource {
            result_descriptor: loading_info.result_descriptor().await?,
            loading_info,
        }
        .boxed())
    }
}

impl OperatorDatasets for MockDatasetDataSource {
    fn datasets_collect(&self, datasets: &mut Vec<DatasetId>) {
        datasets.push(self.params.dataset.clone());
    }
}

struct InitializedMockDatasetDataSource<R: ResultDescriptor, Q> {
    result_descriptor: R,
    loading_info: Box<dyn MetaData<MockDatasetDataSourceLoadingInfo, R, Q>>,
}

impl InitializedVectorOperator
    for InitializedMockDatasetDataSource<VectorResultDescriptor, VectorQueryRectangle>
{
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        Ok(TypedVectorQueryProcessor::MultiPoint(
            MockDatasetDataSourceProcessor {
                loading_info: self.loading_info.clone(),
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
    use geoengine_datatypes::dataset::InternalDatasetId;
    use geoengine_datatypes::primitives::{BoundingBox2D, SpatialResolution};
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_datatypes::util::Identifier;

    #[tokio::test]
    async fn test() {
        let mut execution_context = MockExecutionContext::test_default();

        let id = DatasetId::Internal {
            dataset_id: InternalDatasetId::new(),
        };
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
        assert_eq!(collections.len(), 1);
        assert_eq!(collections[0].len(), 3);
    }
}
