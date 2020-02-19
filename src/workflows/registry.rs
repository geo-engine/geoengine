use std::collections::HashMap;

use super::Workflow;

pub trait WorkflowRegistry {
    type WorkflowIdentifier;

    fn register(&mut self, workflow: Workflow) -> Self::WorkflowIdentifier;
    fn load(&self, id: &Self::WorkflowIdentifier) -> Option<Workflow>;
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
    type WorkflowIdentifier = usize;

    fn register(&mut self, workflow: Workflow) -> Self::WorkflowIdentifier {
        let id = self.map.len();
        self.map.insert(id, workflow);
        id
    }

    fn load(&self, id: &Self::WorkflowIdentifier) -> Option<Workflow> {
        self.map.get(&id).map(|w| w.clone())
    }
}
