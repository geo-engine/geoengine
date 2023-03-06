use std::collections::HashMap;

use super::workflow::{Workflow, WorkflowId};
use crate::contexts::{InMemoryDb};
use crate::error;
use crate::error::Result;
use async_trait::async_trait;

#[async_trait]
pub trait WorkflowRegistry: Send + Sync {
    async fn register_workflow(&self, workflow: Workflow) -> Result<WorkflowId>;
    async fn load_workflow(&self, id: &WorkflowId) -> Result<Workflow>;
}

#[derive(Default)]
pub struct HashMapRegistryBackend {
    pub(crate) map: HashMap<WorkflowId, Workflow>,
}

#[async_trait]
impl WorkflowRegistry for InMemoryDb {
    async fn register_workflow(&self, workflow: Workflow) -> Result<WorkflowId> {
        let id = WorkflowId::from_hash(&workflow);
        self.backend
            .workflow_registry
            .write()
            .await
            .map
            .insert(id, workflow);
        Ok(id)
    }

    async fn load_workflow(&self, id: &WorkflowId) -> Result<Workflow> {
        self.backend
            .workflow_registry
            .read()
            .await
            .map
            .get(id)
            .cloned()
            .ok_or(error::Error::NoWorkflowForGivenId)
    }
}
