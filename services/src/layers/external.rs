use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataId, DataProviderId};
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_operators::engine::{
    MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};

use crate::datasets::listing::ProvenanceOutput;
use crate::error::Result;

use super::listing::LayerCollectionProvider;

#[typetag::serde(tag = "type")]
#[async_trait]
pub trait DataProviderDefinition:
    CloneableDataProviderDefinition + Send + Sync + std::fmt::Debug
{
    /// create the actual provider for data listing and access
    async fn initialize(self: Box<Self>) -> Result<Box<dyn DataProvider>>;

    /// the type of the provider
    fn type_name(&self) -> String;

    /// name of the external data source
    fn name(&self) -> String;

    /// id of the provider
    fn id(&self) -> DataProviderId;
}

pub trait CloneableDataProviderDefinition {
    fn clone_boxed_provider(&self) -> Box<dyn DataProviderDefinition>;
}

impl<T> CloneableDataProviderDefinition for T
where
    T: 'static + DataProviderDefinition + Clone,
{
    fn clone_boxed_provider(&self) -> Box<dyn DataProviderDefinition> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn DataProviderDefinition> {
    fn clone(&self) -> Box<dyn DataProviderDefinition> {
        self.clone_boxed_provider()
    }
}

/// A provider of layers that are not hosted by Geo Engine itself but some external party
// TODO: Authorization: the provider needs to accept credentials for the external data source.
//       The credentials should be generic s.t. they are independent of the Session type and
//       extensible to new provider types. E.g. a key-value map of strings where the provider
//       checks that the necessary information is present and how they are incorporated in
//       the requests.
#[async_trait]
pub trait DataProvider: LayerCollectionProvider
    + MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    + MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    + MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    + Send
    + Sync
    + std::fmt::Debug
{
    // TODO: unify provenance method for internal and external provider as a separate trait. We need to figure out session handling before, though.
    async fn provenance(&self, id: &DataId) -> Result<ProvenanceOutput>;

    /// Propagates `Any`-casting to the underlying provider
    fn as_any(&self) -> &dyn std::any::Any;
}
