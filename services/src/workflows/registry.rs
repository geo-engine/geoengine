use std::collections::HashMap;
use uuid::Uuid;

use super::Workflow;

identifier!(WorkflowId);

impl WorkflowId {
    pub fn from_hash(workflow: &Workflow) -> Self {
        Self { id: Uuid::new_v5(&Uuid::NAMESPACE_OID, serde_json::to_string(workflow).unwrap().as_bytes()) }
    }
}

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
