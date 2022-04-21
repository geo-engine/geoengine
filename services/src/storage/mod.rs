pub mod in_memory;

use std::collections::HashMap;

use crate::{
    datasets::{
        listing::ExternalDatasetProvider,
        storage::{Dataset, ExternalDatasetProviderDefinition, MetaDataDefinition},
        upload::{Upload, UploadId},
    },
    error::Result,
    util::user_input::{UserInput, Validated},
    workflows::workflow::{Workflow, WorkflowId},
};

use async_trait::async_trait;
use geoengine_datatypes::dataset::DatasetId;

pub trait GeoEngineStore:
    Store<Workflow>
    + Store<Dataset>
    + Store<MetaDataDefinition>
    + Store<Upload>
    + Store<Box<dyn ExternalDatasetProviderDefinition>>
{
}

pub trait Storable: Listable<Self> + Send + Sync + Sized {
    type Id;
    type Item: UserInput; // TODO: redundant? just require Storable to be UserInput
    type ItemListing;
    type ListOptions: ListOption<Item = Self>;
}

pub trait ListOption: UserInput + Sized {
    type Item;

    fn offset(&self) -> u32;
    fn limit(&self) -> u32;

    fn compare_items(&self, a: &Self::Item, b: &Self::Item) -> std::cmp::Ordering;

    fn retain(&self, item: &Self::Item) -> bool;
}

pub trait Listable<S: Storable> {
    fn list(&self, id: &S::Id) -> S::ItemListing;
}

#[async_trait]
pub trait Store<S>: Send + Sync
where
    S: Storable + Send + Sync,
{
    async fn create(&mut self, item: Validated<S::Item>) -> Result<S::Id>;
    async fn create_with_id(&mut self, id: &S::Id, item: Validated<S::Item>) -> Result<S::Id>;
    async fn read(&self, id: &S::Id) -> Result<S::Item>;
    async fn update(&mut self, id: &S::Id, item: Validated<S::Item>) -> Result<()>;
    async fn delete(&mut self, id: &S::Id) -> Result<()>;

    // TODO: move into a separate trait?
    async fn list(&self, options: Validated<S::ListOptions>) -> Result<Vec<S::ItemListing>>;
}

#[derive(Debug)]
pub struct StoredDataset {
    pub dataset: Dataset,
    pub meta_data: MetaDataDefinition,
}
