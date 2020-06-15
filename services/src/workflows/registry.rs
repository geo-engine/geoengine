use std::collections::HashMap;
use uuid::Uuid;

use super::Workflow;

identifier!(WorkflowIdentifier);

impl WorkflowIdentifier {
    pub fn from_hash(workflow: &Workflow) -> Self {
        Self { id: Uuid::new_v5(&Uuid::NAMESPACE_OID, serde_json::to_string(workflow).unwrap().as_bytes()) }
    }
}

pub trait WorkflowRegistry: Send + Sync {
    fn register(&mut self, workflow: Workflow) -> WorkflowIdentifier;
    fn load(&self, id: &WorkflowIdentifier) -> Option<Workflow>;
}

#[derive(Default)]
pub struct HashMapRegistry {
    map: HashMap<WorkflowIdentifier, Workflow>
}

impl WorkflowRegistry for HashMapRegistry {
    fn register(&mut self, workflow: Workflow) -> WorkflowIdentifier {
        let id = WorkflowIdentifier::from_hash(&workflow);
        self.map.insert(id, workflow);
        id
    }

    fn load(&self, id: &WorkflowIdentifier) -> Option<Workflow> {
        self.map.get(&id).cloned()
    }
}
