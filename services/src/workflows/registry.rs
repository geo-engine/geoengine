use std::collections::HashMap;

use super::workflow::{Workflow, WorkflowId};
use crate::error;
use crate::error::Result;

pub trait WorkflowRegistry: Send + Sync {
    fn register(&mut self, workflow: Workflow) -> Result<WorkflowId>;
    fn load(&self, id: &WorkflowId) -> Result<Workflow>;
}

#[derive(Default)]
pub struct HashMapRegistry {
    map: HashMap<WorkflowId, Workflow>,
}

impl WorkflowRegistry for HashMapRegistry {
    fn register(&mut self, workflow: Workflow) -> Result<WorkflowId> {
        let id = WorkflowId::from_hash(&workflow);
        self.map.insert(id, workflow);
        Ok(id)
    }

    fn load(&self, id: &WorkflowId) -> Result<Workflow> {
        self.map
            .get(&id)
            .cloned()
            .ok_or(error::Error::NoWorkflowForGivenId)
    }
}
