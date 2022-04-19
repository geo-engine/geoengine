use crate::error;
use crate::error::Result;
use crate::pro::contexts::PostgresStore;
use crate::storage::Store;
use crate::util::user_input::Validated;
use crate::workflows::workflow::{Workflow, WorkflowId, WorkflowListing};
use async_trait::async_trait;
use bb8_postgres::{
    tokio_postgres::tls::MakeTlsConnect, tokio_postgres::tls::TlsConnect, tokio_postgres::Socket,
};
use snafu::ResultExt;

#[async_trait]
impl<Tls> Store<Workflow> for PostgresStore<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn create(&mut self, workflow: Validated<Workflow>) -> Result<WorkflowId> {
        let workflow = workflow.user_input;
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

    async fn read(&self, id: &WorkflowId) -> Result<Workflow> {
        // TODO: authorization
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("SELECT workflow FROM workflows WHERE id = $1")
            .await?;

        let row = conn.query_one(&stmt, &[&id]).await?;

        Ok(serde_json::from_value(row.get(0)).context(error::SerdeJson)?)
    }

    async fn update(&mut self, _id: &WorkflowId, _workflow: Validated<Workflow>) -> Result<()> {
        todo!()
    }

    async fn delete(&mut self, _id: &WorkflowId) -> Result<()> {
        todo!()
    }

    async fn list(&self, _options: ()) -> Result<Vec<WorkflowListing>> {
        todo!()
    }
}
