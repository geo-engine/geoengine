use std::collections::HashMap;

use super::workflow::{Workflow, WorkflowId};
use crate::contexts::Db;
use crate::error;
use crate::error::Result;
use async_trait::async_trait;

#[async_trait]
pub trait WorkflowRegistry: Send + Sync {
    async fn register(&self, workflow: Workflow) -> Result<WorkflowId>;
    async fn load(&self, id: &WorkflowId) -> Result<Workflow>;
}

#[derive(Default)]
pub struct HashMapRegistry {
    map: Db<HashMap<WorkflowId, Workflow>>,
}

#[async_trait]
impl WorkflowRegistry for HashMapRegistry {
    async fn register(&self, workflow: Workflow) -> Result<WorkflowId> {
        let id = WorkflowId::from_hash(&workflow);
        self.map.write().await.insert(id, workflow);
        Ok(id)
    }

    async fn load(&self, id: &WorkflowId) -> Result<Workflow> {
        self.map
            .read()
            .await
            .get(id)
            .cloned()
            .ok_or(error::Error::NoWorkflowForGivenId)
    }
}
