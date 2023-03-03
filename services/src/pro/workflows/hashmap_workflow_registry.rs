use crate::error::Result;
use crate::{
    error,
    pro::contexts::ProInMemoryDb,
    workflows::{
        registry::WorkflowRegistry,
        workflow::{Workflow, WorkflowId},
    },
};

use async_trait::async_trait;

#[async_trait]
impl WorkflowRegistry for ProInMemoryDb {
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
