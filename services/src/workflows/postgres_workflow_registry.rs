use crate::error;
use crate::workflows::workflow::WorkflowId;
use crate::{error::Result, util::identifiers::Identifier};
use async_trait::async_trait;
use bb8_postgres::{bb8::Pool, tokio_postgres::NoTls, PostgresConnectionManager};
use snafu::ResultExt;

use super::{registry::WorkflowRegistry, workflow::Workflow};

pub struct PostgresWorkflowRegistry {
    conn_pool: Pool<PostgresConnectionManager<NoTls>>, // TODO: support Tls connection as well
}

impl PostgresWorkflowRegistry {
    pub fn new(conn_pool: Pool<PostgresConnectionManager<NoTls>>) -> Self {
        Self { conn_pool }
    }
}

#[async_trait]
impl WorkflowRegistry for PostgresWorkflowRegistry {
    async fn register(&mut self, workflow: Workflow) -> Result<WorkflowId> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("INSERT INTO workflows (id, workflow) VALUES ($1, $2);")
            .await?;

        let workflow_id = WorkflowId::from_hash(&workflow);

        conn.execute(
            &stmt,
            &[
                &workflow_id.uuid(),
                &serde_json::to_value(&workflow).context(error::SerdeJson)?,
            ],
        )
        .await?;

        Ok(workflow_id)
    }

    async fn load(&self, id: &WorkflowId) -> Result<Workflow> {
        // TODO: authorization
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("SELECT id, workflow FROM workflows WHERE id = $1")
            .await?;

        let row = conn.query_one(&stmt, &[&id.uuid()]).await?;

        Ok(serde_json::from_str(&row.get::<usize, String>(0)).context(error::SerdeJson)?)
    }
}
