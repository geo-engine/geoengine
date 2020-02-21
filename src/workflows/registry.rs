use std::collections::HashMap;

use super::Workflow;

type WorkflowIdentifier = usize;

pub trait WorkflowRegistry: Send + Sync {
    fn register(&mut self, workflow: Workflow) -> WorkflowIdentifier;
    fn load(&self, id: &WorkflowIdentifier) -> Option<Workflow>;
}

pub struct HashMapRegistry {
    map: HashMap<usize, Workflow>
}

impl HashMapRegistry {
    pub fn new() -> Self {
        Self { map: HashMap::new() }
    }
}

impl WorkflowRegistry for HashMapRegistry {
    fn register(&mut self, workflow: Workflow) -> WorkflowIdentifier {
        let id = self.map.len();
        self.map.insert(id, workflow);
        id
    }

    fn load(&self, id: &WorkflowIdentifier) -> Option<Workflow> {
        self.map.get(&id).map(|w| w.clone())
    }
}
