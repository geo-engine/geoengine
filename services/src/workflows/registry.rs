use std::collections::HashMap;

use super::workflow::{Workflow, WorkflowId};
use crate::error;
use crate::error::Result;
use async_trait::async_trait;

#[async_trait]
pub trait WorkflowRegistry: Send + Sync {
    async fn register(&mut self, workflow: Workflow) -> Result<WorkflowId>;
    async fn load(&self, id: &WorkflowId) -> Result<Workflow>;
}

#[derive(Default)]
pub struct HashMapRegistry {
    map: HashMap<WorkflowId, Workflow>,
}

#[async_trait]
impl WorkflowRegistry for HashMapRegistry {
    async fn register(&mut self, workflow: Workflow) -> Result<WorkflowId> {
        let id = WorkflowId::from_hash(&workflow);
        self.map.insert(id, workflow);
        Ok(id)
    }

    async fn load(&self, id: &WorkflowId) -> Result<Workflow> {
        self.map
            .get(&id)
            .cloned()
            .ok_or(error::Error::NoWorkflowForGivenId)
    }
}
