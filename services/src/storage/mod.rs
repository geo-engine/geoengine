use std::collections::HashMap;

use crate::{
    error::Result,
    util::user_input::{UserInput, Validated},
    workflows::workflow::{Workflow, WorkflowId},
};

use async_trait::async_trait;

pub trait GeoEngineStore: Store<Workflow> {}

pub trait Storable {
    type Id;
    type Item: UserInput;
    type ItemListing;
    type ListOptions;
}

#[async_trait]
pub trait Store<S>
where
    S: Storable,
{
    async fn create(&mut self, item: Validated<S::Item>) -> Result<S::Id>;
    async fn read(&self, id: &S::Id) -> Result<S::Item>;
    async fn update(&mut self, id: &S::Id, item: Validated<S::Item>) -> Result<()>;
    async fn delete(&mut self, id: &S::Id) -> Result<()>;

    async fn list(&self, options: S::ListOptions) -> Result<Vec<S::ItemListing>>;
}

#[derive(Debug, Default)]
pub struct InMemoryStore {
    pub(crate) workflows: HashMap<WorkflowId, Workflow>,
}

impl GeoEngineStore for InMemoryStore {}
