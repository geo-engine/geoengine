use super::workflow::{Workflow, WorkflowId};

use crate::error::Result;
use async_trait::async_trait;

#[async_trait]
pub trait TxWorkflowRegistry: Send + Sync {
    async fn register_workflow_in_tx(
        &self,
        workflow: Workflow,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<WorkflowId>;
}

#[async_trait]
pub trait WorkflowRegistry: Send + Sync {
    async fn register_workflow(&self, workflow: Workflow) -> Result<WorkflowId>;
    async fn load_workflow(&self, id: &WorkflowId) -> Result<Workflow>;
}
