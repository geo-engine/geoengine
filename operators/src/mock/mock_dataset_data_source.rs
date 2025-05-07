use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedVectorOperator, MetaData, OperatorData,
    OperatorName, QueryContext, ResultDescriptor, SourceOperator, TypedVectorQueryProcessor,
    VectorOperator, VectorQueryProcessor, VectorResultDescriptor, WorkflowOperatorPath,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream;
use futures::stream::BoxStream;
use geoengine_datatypes::collections::{MultiPointCollection, VectorDataType};
use geoengine_datatypes::dataset::NamedData;
use geoengine_datatypes::primitives::CacheHint;
use geoengine_datatypes::primitives::{Coordinate2D, TimeInterval, VectorQueryRectangle};
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// TODO: generify this to support all data types
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize, FromSql, ToSql)]
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
            time: None,
            bbox: None,
        })
    }

    fn box_clone(
        &self,
    ) -> Box<
        dyn MetaData<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>,
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
    result_descriptor: VectorResultDescriptor,
    loading_info: Box<
        dyn MetaData<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>,
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
                CacheHint::max_duration(),
            )?)
        })
        .boxed())
    }

    fn vector_result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MockDatasetDataSourceParams {
    pub data: NamedData,
}

pub type MockDatasetDataSource = SourceOperator<MockDatasetDataSourceParams>;

impl OperatorName for MockDatasetDataSource {
    const TYPE_NAME: &'static str = "MockDatasetDataSource";
}

#[typetag::serde]
#[async_trait]
impl VectorOperator for MockDatasetDataSource {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        let data_id = context.resolve_named_data(&self.params.data).await?;
        let loading_info = context.meta_data(&data_id).await?;

        Ok(InitializedMockDatasetDataSource {
            name: CanonicOperatorName::from(&self),
            path,
            result_descriptor: loading_info.result_descriptor().await?,
            loading_info,
        }
        .boxed())
    }

    span_fn!(MockDatasetDataSource);
}

impl OperatorData for MockDatasetDataSource {
    fn data_names_collect(&self, data_names: &mut Vec<NamedData>) {
        data_names.push(self.params.data.clone());
    }
}

struct InitializedMockDatasetDataSource<R: ResultDescriptor, Q> {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    result_descriptor: R,
    loading_info: Box<dyn MetaData<MockDatasetDataSourceLoadingInfo, R, Q>>,
}

impl InitializedVectorOperator
    for InitializedMockDatasetDataSource<VectorResultDescriptor, VectorQueryRectangle>
{
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        Ok(TypedVectorQueryProcessor::MultiPoint(
            MockDatasetDataSourceProcessor {
                result_descriptor: self.result_descriptor.clone(),
                loading_info: self.loading_info.clone(),
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
        MockDatasetDataSource::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::QueryProcessor;
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use futures::executor::block_on_stream;
    use geoengine_datatypes::collections::FeatureCollectionInfos;
    use geoengine_datatypes::dataset::{DataId, DatasetId, NamedData};
    use geoengine_datatypes::primitives::{BoundingBox2D, ColumnSelection, SpatialResolution};
    use geoengine_datatypes::util::Identifier;
    use geoengine_datatypes::util::test::TestDefault;

    #[tokio::test]
    async fn test() {
        let mut execution_context = MockExecutionContext::test_default();

        let id: DataId = DatasetId::new().into();
        execution_context.add_meta_data(
            id.clone(),
            NamedData::with_system_name("points"),
            Box::new(MockDatasetDataSourceLoadingInfo {
                points: vec![Coordinate2D::new(1., 2.); 3],
            }),
        );

        let mps = MockDatasetDataSource {
            params: MockDatasetDataSourceParams {
                data: NamedData::with_system_name("points"),
            },
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

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((0., 0.).into(), (4., 4.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
            attributes: ColumnSelection::all(),
        };
        let ctx = MockQueryContext::new((2 * std::mem::size_of::<Coordinate2D>()).into());

        let stream = point_processor.query(query_rectangle, &ctx).await.unwrap();

        let blocking_stream = block_on_stream(stream);
        let collections: Vec<MultiPointCollection> = blocking_stream.map(Result::unwrap).collect();
        assert_eq!(collections.len(), 1);
        assert_eq!(collections[0].len(), 3);
    }
}
