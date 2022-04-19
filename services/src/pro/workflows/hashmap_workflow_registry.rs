use crate::error::{self, Result};
use crate::pro::contexts::ProInMemoryStore;
use crate::storage::Store;
use crate::util::user_input::Validated;
use crate::workflows::workflow::{Workflow, WorkflowId, WorkflowListing};

use async_trait::async_trait;

#[async_trait]
impl Store<Workflow> for ProInMemoryStore {
    async fn create(&mut self, item: Validated<Workflow>) -> Result<WorkflowId> {
        let workflow = item.user_input;
        let id = WorkflowId::from_hash(&workflow);
        self.workflows.insert(id, workflow);
        Ok(id)
    }

    async fn read(&self, id: &WorkflowId) -> Result<Workflow> {
        self.workflows
            .get(id)
            .cloned()
            .ok_or(error::Error::NoWorkflowForGivenId)
    }

    async fn update(&mut self, id: &WorkflowId, item: Validated<Workflow>) -> Result<()> {
        let workflow = item.user_input;
        self.workflows.insert(*id, workflow);
        Ok(())
    }

    async fn delete(&mut self, id: &WorkflowId) -> Result<()> {
        self.workflows.remove(id);
        Ok(())
    }

    async fn list(&self, _options: ()) -> Result<Vec<WorkflowListing>> {
        self.workflows
            .iter()
            .map(|(id, _)| {
                let listing = WorkflowListing { id: *id };
                Ok(listing)
            })
            .collect()
    }
}
