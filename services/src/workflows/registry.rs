use std::collections::HashMap;

use super::workflow::{Workflow, WorkflowId};

pub trait WorkflowRegistry: Send + Sync {
    fn register(&mut self, workflow: Workflow) -> WorkflowId;
    fn load(&self, id: &WorkflowId) -> Option<Workflow>;
}

#[derive(Default)]
pub struct HashMapRegistry {
    map: HashMap<WorkflowId, Workflow>
}

impl WorkflowRegistry for HashMapRegistry {
    fn register(&mut self, workflow: Workflow) -> WorkflowId {
        let id = WorkflowId::from_hash(&workflow);
        self.map.insert(id, workflow);
        id
    }

    fn load(&self, id: &WorkflowId) -> Option<Workflow> {
        self.map.get(&id).cloned()
    }
}
