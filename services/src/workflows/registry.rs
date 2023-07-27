

use super::workflow::{Workflow, WorkflowId};

use crate::error::Result;
use async_trait::async_trait;

#[async_trait]
pub trait WorkflowRegistry: Send + Sync {
    async fn register_workflow(&self, workflow: Workflow) -> Result<WorkflowId>;
    async fn load_workflow(&self, id: &WorkflowId) -> Result<Workflow>;
}
