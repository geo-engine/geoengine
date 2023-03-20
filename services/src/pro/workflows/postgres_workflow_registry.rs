use crate::error::Result;
use crate::pro::contexts::PostgresDb;
use crate::workflows::workflow::{Workflow, WorkflowId};
use crate::{error, workflows::registry::WorkflowRegistry};
use async_trait::async_trait;
use bb8_postgres::{
    tokio_postgres::tls::MakeTlsConnect, tokio_postgres::tls::TlsConnect, tokio_postgres::Socket,
};
use snafu::ResultExt;

#[async_trait]
impl<Tls> WorkflowRegistry for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn register_workflow(&self, workflow: Workflow) -> Result<WorkflowId> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "INSERT INTO workflows (id, workflow) VALUES ($1, $2) 
            ON CONFLICT DO NOTHING;",
            )
            .await?;

        let workflow_id = WorkflowId::from_hash(&workflow);

        conn.execute(
            &stmt,
            &[
                &workflow_id,
                &serde_json::to_value(&workflow).context(error::SerdeJson)?,
            ],
        )
        .await?;

        Ok(workflow_id)
    }

    async fn load_workflow(&self, id: &WorkflowId) -> Result<Workflow> {
        // TODO: authorization
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
