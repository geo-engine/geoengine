use crate::error::Result;
use crate::{
    projects::hashmap_projectdb::HashMapProjectDb, users::hashmap_userdb::HashMapUserDb,
    users::session::Session, workflows::registry::HashMapRegistry,
};
use async_trait::async_trait;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use super::{Context, Db};
use crate::contexts::{ExecutionContextImpl, QueryContextImpl};
use crate::datasets::in_memory::HashMapDataSetDb;
use crate::datasets::storage::{AddDataSet, DataSetStore};
use crate::users::user::UserId;
use crate::util::user_input::UserInput;
use crate::util::{config, Identifier};
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::primitives::FeatureDataType;
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::concurrency::ThreadPool;
use geoengine_operators::engine::{StaticMetaData, VectorResultDescriptor};
use geoengine_operators::mock::MockDataSetDataSourceLoadingInfo;
use geoengine_operators::source::{
    OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType, OgrSourceErrorSpec,
};
use std::sync::Arc;

/// A context with references to in-memory versions of the individual databases.
#[derive(Clone, Default)]
pub struct InMemoryContext {
    user_db: Db<HashMapUserDb>,
    project_db: Db<HashMapProjectDb>,
    workflow_registry: Db<HashMapRegistry>,
    data_set_db: Db<HashMapDataSetDb>,
    session: Option<Session>,
    thread_pool: Arc<ThreadPool>,
}

impl InMemoryContext {
    pub async fn new_with_data() -> Self {
        // TODO: scan directory and auto import

        let ctx = Self::default();

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::MultiPoint,
            spatial_reference: SpatialReference::epsg_4326().into(),
            columns: Default::default(),
        };
        let ds = AddDataSet {
            name: "Mock".to_string(),
            description: "A mock data set".to_string(),
            result_descriptor: descriptor.clone().into(),
            source_operator: "MockDataSetDataSource".to_string(),
        };

        let meta = StaticMetaData {
            loading_info: MockDataSetDataSourceLoadingInfo {
                points: vec![(1.0, 2.0).into()],
            },
            result_descriptor: descriptor,
        };

        ctx.data_set_db_ref_mut()
            .await
            .add_data_set(UserId::new(), ds.validated().unwrap(), Box::new(meta))
            .await
            .unwrap();

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::MultiPoint,
            spatial_reference: SpatialReference::epsg_4326().into(),
            columns: [
                ("natlscale".to_string(), FeatureDataType::Number),
                ("scalerank".to_string(), FeatureDataType::Decimal),
                ("featurecla".to_string(), FeatureDataType::Decimal),
                ("name".to_string(), FeatureDataType::Text),
                ("website".to_string(), FeatureDataType::Text),
            ]
            .iter()
            .cloned()
            .collect(),
        };
        let ds = AddDataSet {
            name: "Natural Earth 10m Ports".to_string(),
            description: "Ports from Natural Earth".to_string(),
            result_descriptor: descriptor.clone().into(),
            source_operator: "OgrSource".to_string(),
        };

        let meta = StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: "operators/test-data/vector/data/ne_10m_ports/ne_10m_ports.shp".into(),
                layer_name: "ne_10m_ports".to_string(),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::None,
                columns: Some(OgrSourceColumnSpec {
                    x: "".to_string(),
                    y: None,
                    numeric: vec!["natlscale".to_string()],
                    decimal: vec!["scalerank".to_string()],
                    textual: vec![
                        "featurecla".to_string(),
                        "name".to_string(),
                        "website".to_string(),
                    ],
                }),
                default_geometry: None,
                force_ogr_time_filter: false,
                on_error: OgrSourceErrorSpec::Skip,
                provenance: None,
            },
            result_descriptor: descriptor,
        };

        ctx.data_set_db_ref_mut()
            .await
            .add_data_set(UserId::new(), ds.validated().unwrap(), Box::new(meta))
            .await
            .unwrap();

        ctx
    }
}

#[async_trait]
impl Context for InMemoryContext {
    type UserDB = HashMapUserDb;
    type ProjectDB = HashMapProjectDb;
    type WorkflowRegistry = HashMapRegistry;
    type DataSetDB = HashMapDataSetDb;
    type QueryContext = QueryContextImpl;
    type ExecutionContext = ExecutionContextImpl<HashMapDataSetDb>;

    fn user_db(&self) -> Db<Self::UserDB> {
        self.user_db.clone()
    }
    async fn user_db_ref(&self) -> RwLockReadGuard<'_, Self::UserDB> {
        self.user_db.read().await
    }
    async fn user_db_ref_mut(&self) -> RwLockWriteGuard<'_, Self::UserDB> {
        self.user_db.write().await
    }

    fn project_db(&self) -> Db<Self::ProjectDB> {
        self.project_db.clone()
    }
    async fn project_db_ref(&self) -> RwLockReadGuard<'_, Self::ProjectDB> {
        self.project_db.read().await
    }
    async fn project_db_ref_mut(&self) -> RwLockWriteGuard<'_, Self::ProjectDB> {
        self.project_db.write().await
    }

    fn workflow_registry(&self) -> Db<Self::WorkflowRegistry> {
        self.workflow_registry.clone()
    }
    async fn workflow_registry_ref(&self) -> RwLockReadGuard<'_, Self::WorkflowRegistry> {
        self.workflow_registry.read().await
    }
    async fn workflow_registry_ref_mut(&self) -> RwLockWriteGuard<'_, Self::WorkflowRegistry> {
        self.workflow_registry.write().await
    }

    fn data_set_db(&self) -> Db<Self::DataSetDB> {
        self.data_set_db.clone()
    }
    async fn data_set_db_ref(&self) -> RwLockReadGuard<'_, Self::DataSetDB> {
        self.data_set_db.read().await
    }
    async fn data_set_db_ref_mut(&self) -> RwLockWriteGuard<'_, Self::DataSetDB> {
        self.data_set_db.write().await
    }

    fn query_context(&self) -> Result<Self::QueryContext> {
        Ok(QueryContextImpl {
            // TODO: load config only once
            chunk_byte_size: config::get_config_element::<config::QueryContext>()?.chunk_byte_size,
        })
    }

    fn execution_context(&self, session: &Session) -> Result<Self::ExecutionContext> {
        Ok(ExecutionContextImpl::<HashMapDataSetDb> {
            data_set_db: self.data_set_db.clone(),
            thread_pool: self.thread_pool.clone(),
            user: session.user.id,
        })
    }
}
