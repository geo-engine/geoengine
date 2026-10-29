use crate::contexts::PostgresDb;
use crate::error::Result;
use crate::processing_graphs::processing_graph::{ProcessingGraph, ProcessingGraphId};
use crate::processing_graphs::registry::TxProcessingGraphRegistry;
use crate::{error, processing_graphs::registry::ProcessingGraphRegistry};
use async_trait::async_trait;
use bb8_postgres::{
    tokio_postgres::Socket, tokio_postgres::tls::MakeTlsConnect, tokio_postgres::tls::TlsConnect,
};
use snafu::ResultExt;

#[async_trait]
impl<Tls> TxProcessingGraphRegistry for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn register_processing_graph_in_tx(
        &self,
        processing_graph: ProcessingGraph,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<ProcessingGraphId> {
        let processing_graph_id = ProcessingGraphId::from_hash(&processing_graph);

        tx.execute(
            "INSERT INTO processing_graphs (id, processing_graph) VALUES ($1, $2) 
            ON CONFLICT DO NOTHING;",
            &[
                &processing_graph_id,
                &serde_json::to_value(&processing_graph).context(error::SerdeJson)?,
            ],
        )
        .await?;

        Ok(processing_graph_id)
    }
}

#[async_trait]
impl<Tls> ProcessingGraphRegistry for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn register_processing_graph(
        &self,
        processing_graph: ProcessingGraph,
    ) -> Result<ProcessingGraphId> {
        let mut conn = self.conn_pool.get().await?;
        let tx = conn.transaction().await?;

        let id = self
            .register_processing_graph_in_tx(processing_graph, &tx)
            .await?;

        tx.commit().await?;

        Ok(id)
    }

    async fn load_processing_graph(&self, id: &ProcessingGraphId) -> Result<ProcessingGraph> {
        // TODO: add authorization beyond the fact that you can only access processing graphs if you happen to know the id.
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("SELECT processing_graph FROM processing_graphs WHERE id = $1")
            .await?;

        let row = conn.query(&stmt, &[&id]).await?;

        if row.is_empty() {
            return Err(error::Error::NoProcessingGraphForGivenId);
        }

        Ok(serde_json::from_value(row[0].get(0)).context(error::SerdeJson)?)
    }
}
