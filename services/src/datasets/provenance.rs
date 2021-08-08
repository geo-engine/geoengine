use crate::error::Result;
use async_trait::async_trait;
use geoengine_datatypes::dataset::DatasetId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ProvenanceOutput {
    pub dataset: DatasetId,
    pub provenance: Option<Provenance>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct Provenance {
    pub citation: String,
    pub license: String,
    pub uri: String,
}

#[async_trait]
pub trait ProvenanceProvider {
    /// get the provenance information for the `dataset`
    // TODO: should take a session as input, but this would make the DatasetProviderDefinition's initialize method generic and then it cannot be make into an object anymore
    async fn provenance(&self, dataset: &DatasetId) -> Result<ProvenanceOutput>;
}
