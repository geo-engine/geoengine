use super::processing_graph::{ProcessingGraph, ProcessingGraphId};

use crate::error::Result;
use async_trait::async_trait;

#[async_trait]
pub trait TxProcessingGraphRegistry: Send + Sync {
    async fn register_processing_graph_in_tx(
        &self,
        workflow: ProcessingGraph,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<ProcessingGraphId>;
}

#[async_trait]
pub trait ProcessingGraphRegistry: Send + Sync {
    async fn register_processing_graph(
        &self,
        workflow: ProcessingGraph,
    ) -> Result<ProcessingGraphId>;
    async fn load_processing_graph(&self, id: &ProcessingGraphId) -> Result<ProcessingGraph>;
}
