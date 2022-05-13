use async_trait::async_trait;
use crate::{engine::{RasterQueryProcessor, QueryContext}, error::Error};
use geoengine_datatypes::primitives::{QueryRectangle, SpatialPartition2D};
use eval_metrics::classification::{MultiConfusionMatrix};
use core::pin::Pin;
use futures::Stream;


#[async_trait]
pub trait Classifier {

    type ReturnType;

    async fn train(
        &mut self, 
        query: QueryRectangle<SpatialPartition2D>,
        query_ctx: &dyn QueryContext,
    ) -> Result<Self::ReturnType, Error>;

    async fn validate(
        &self, 
        query: QueryRectangle<SpatialPartition2D>,
        query_ctx: &dyn QueryContext,
    ) -> Result<MultiConfusionMatrix, Error>;

    //TODO Specify output operator
    async fn predictor(
        &self,
        query: QueryRectangle<SpatialPartition2D>,
        query_ctx: &dyn QueryContext, 
    ) -> ();

}