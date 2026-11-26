use super::{MultiBandGdalLoadingInfo, MultiBandGdalLoadingInfoQueryRectangle};
use crate::engine::{FootprintPrecision, RasterResultDescriptor};
use crate::error::Error;
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use geoengine_datatypes::dataset::DataId;
use geoengine_datatypes::primitives::{
    BandSelection, QueryRectangle, SpatialPartition2D, TimeInterval,
};
use std::fmt::Debug;

/// The meta data of a multi band GDAL dataset. It answers the three questions of its source, each
/// with the inputs it needs: the time steps of a time, the files to read, and where and when
/// there is data.
#[async_trait]
pub trait MultiBandGdalMetaData: Debug + Send + Sync {
    async fn result_descriptor(&self) -> Result<RasterResultDescriptor>;

    /// The gap-free time steps that intersect `time`, one by one, so a caller that takes the
    /// first ones pays only for them.
    async fn time_steps(
        &self,
        time: TimeInterval,
    ) -> Result<BoxStream<'static, Result<TimeInterval>>>;

    /// The files to read `query` from, in parts, see [`MultiBandGdalLoadingInfo`].
    async fn loading_info(
        &self,
        query: MultiBandGdalLoadingInfoQueryRectangle,
    ) -> Result<MultiBandGdalLoadingInfo>;

    /// Where the time steps of `query` have data, in parts of steps in a row. By default the
    /// areas of the files of each step; a provider that can tell it cheaper overrides it.
    async fn footprints(
        &self,
        query: QueryRectangle<SpatialPartition2D, BandSelection>,
        _precision: FootprintPrecision,
    ) -> Result<BoxStream<'static, Result<Vec<(TimeInterval, StepFootprint)>>>> {
        let bands = query.attributes().as_vec();
        let parts = self
            .loading_info(MultiBandGdalLoadingInfoQueryRectangle::with_query_rectangle(query))
            .await?;
        Ok(parts.map_ok(move |part| part.footprints(&bands)).boxed())
    }

    fn box_clone(&self) -> Box<dyn MultiBandGdalMetaData>;
}

impl Clone for Box<dyn MultiBandGdalMetaData> {
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

/// Where a time step has data.
#[derive(Clone, Debug, PartialEq)]
pub enum StepFootprint {
    /// Somewhere in the query; the provider did not tell where, e.g. from a count of a catalog.
    Somewhere,
    /// Within these areas; none for a step without data.
    Areas(Vec<SpatialPartition2D>),
}

/// Gives the [`MultiBandGdalMetaData`] of a dataset. A provider without multi band GDAL datasets
/// implements it with the default.
#[async_trait]
pub trait MultiBandGdalMetaDataProvider {
    async fn multi_band_gdal_meta_data(
        &self,
        _id: &DataId,
    ) -> Result<Box<dyn MultiBandGdalMetaData>> {
        Err(Error::NotImplemented)
    }
}
