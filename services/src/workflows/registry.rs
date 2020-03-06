use std::collections::HashMap;
use uuid::Uuid;
use serde::{Deserialize, Serialize};

use super::Workflow;
use core::fmt;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct WorkflowIdentifier {
    id: Uuid,
}

impl WorkflowIdentifier {
    pub fn from_uuid(id: Uuid) -> Self {
        Self { id }
    }

    pub fn random() -> Self {
        Self { id: Uuid::new_v4() }
    }

    pub fn from_hash(workflow: &Workflow) -> Self {
        Self { id: Uuid::new_v5(&Uuid::NAMESPACE_OID, serde_json::to_string(workflow).unwrap().as_bytes()) }
    }
}

impl fmt::Display for WorkflowIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id)
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
        self.map.insert(id.clone(), workflow);
        id
    }

    fn load(&self, id: &WorkflowIdentifier) -> Option<Workflow> {
        self.map.get(&id).cloned()
    }
}
