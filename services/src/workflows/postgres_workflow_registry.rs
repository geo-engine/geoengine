use crate::contexts::PostgresDb;
use crate::error::Result;
use crate::workflows::workflow::{Workflow, WorkflowId};
use crate::{error, workflows::registry::WorkflowRegistry};
use async_trait::async_trait;
use bb8_postgres::{
    tokio_postgres::tls::MakeTlsConnect, tokio_postgres::tls::TlsConnect, tokio_postgres::Socket,
};
use snafu::ResultExt;

use super::registry::TxWorkflowRegistry;

#[async_trait]
impl<Tls> TxWorkflowRegistry for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn register_workflow_in_tx(
        &self,
        workflow: Workflow,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<WorkflowId> {
        let workflow_id = WorkflowId::from_hash(&workflow);

        tx.execute(
            "INSERT INTO workflows (id, workflow) VALUES ($1, $2) 
            ON CONFLICT DO NOTHING;",
            &[
                &workflow_id,
                &serde_json::to_value(&workflow).context(error::SerdeJson)?,
            ],
        )
        .await?;

        Ok(workflow_id)
    }
}

#[async_trait]
impl<Tls> WorkflowRegistry for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn register_workflow(&self, workflow: Workflow) -> Result<WorkflowId> {
        let mut conn = self.conn_pool.get().await?;
        let tx = conn.transaction().await?;

        let id = self.register_workflow_in_tx(workflow, &tx).await?;

        tx.commit().await?;

        Ok(id)
    }

    async fn load_workflow(&self, id: &WorkflowId) -> Result<Workflow> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("SELECT workflow FROM workflows WHERE id = $1")
            .await?;

        let row = conn.query(&stmt, &[&id]).await?;

        if row.is_empty() {
            return Err(error::Error::NoWorkflowForGivenId);
        }

        Ok(serde_json::from_value(row[0].get(0)).context(error::SerdeJson)?)
    }
}
