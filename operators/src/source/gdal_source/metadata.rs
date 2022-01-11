use std::collections::HashMap;

use async_trait::async_trait;
use geoengine_datatypes::primitives::{TimeInstance, TimeInterval, TimeStep, TimeStepIter};
use serde::{Deserialize, Serialize};

use crate::{
    engine::{MetaData, RasterQueryRectangle, RasterResultDescriptor},
    util::Result,
};

use super::{
    DynamicGdalLoadingInfoPartIterator, GdalDatasetParameters, GdalLoadingInfo,
    GdalLoadingInfoPart, GdalLoadingInfoPartIterator, GdalSourceTimePlaceholder,
};

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetaDataStatic {
    pub time: Option<TimeInterval>,
    pub params: GdalDatasetParameters,
    pub result_descriptor: RasterResultDescriptor,
}

#[async_trait]
impl MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for GdalMetaDataStatic
{
    async fn loading_info(&self, query: RasterQueryRectangle) -> Result<GdalLoadingInfo> {
        let valid = self.time.unwrap_or_default();

        let parts = if valid.intersects(&query.time_interval) {
            vec![GdalLoadingInfoPart {
                time: valid,
                params: self.params.clone(),
            }]
            .into_iter()
        } else {
            vec![].into_iter()
        };

        Ok(GdalLoadingInfo {
            info: GdalLoadingInfoPartIterator::Static { parts },
        })
    }

    async fn result_descriptor(&self) -> Result<RasterResultDescriptor> {
        Ok(self.result_descriptor.clone())
    }

    fn box_clone(
        &self,
    ) -> Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> {
        Box::new(self.clone())
    }
}

/// Meta data for a regular time series that begins (is anchored) at `start` with multiple gdal data
/// sets `step` time apart. The `time_placeholders` in the file path of the dataset are replaced with the
/// specified time `reference` in specified time `format`.
// TODO: `start` is actually more a reference time, because the time series also goes in
//        negative direction. Maybe it would be better to have a real start and end time, then
//        everything before start and after end is just one big nodata raster instead of many
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetaDataRegular {
    pub result_descriptor: RasterResultDescriptor,
    pub params: GdalDatasetParameters,
    pub time_placeholders: HashMap<String, GdalSourceTimePlaceholder>,
    pub start: TimeInstance,
    pub step: TimeStep,
}

#[async_trait]
impl MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for GdalMetaDataRegular
{
    async fn loading_info(&self, query: RasterQueryRectangle) -> Result<GdalLoadingInfo> {
        let snapped_start = self
            .step
            .snap_relative(self.start, query.time_interval.start())?;

        let snapped_interval =
            TimeInterval::new_unchecked(snapped_start, query.time_interval.end()); // TODO: snap end?

        let time_iterator =
            TimeStepIter::new_with_interval_incl_start(snapped_interval, self.step)?;

        Ok(GdalLoadingInfo {
            info: GdalLoadingInfoPartIterator::Dynamic(DynamicGdalLoadingInfoPartIterator::new(
                time_iterator,
                self.params.clone(),
                self.time_placeholders.clone(),
                self.step,
                query.time_interval.end(),
            )?),
        })
    }

    async fn result_descriptor(&self) -> Result<RasterResultDescriptor> {
        Ok(self.result_descriptor.clone())
    }

    fn box_clone(
        &self,
    ) -> Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> {
        Box::new(self.clone())
    }
}

/// Meta data for 4D `NetCDF` CF datasets
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetadataNetCdfCf {
    pub result_descriptor: RasterResultDescriptor,
    pub params: GdalDatasetParameters,
    pub start: TimeInstance,
    pub step: TimeStep,
}

#[async_trait]
impl MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for GdalMetadataNetCdfCf
{
    async fn loading_info(&self, query: RasterQueryRectangle) -> Result<GdalLoadingInfo> {
        let snapped_start = self
            .step
            .snap_relative(self.start, query.time_interval.start())?;

        let snapped_interval =
            TimeInterval::new_unchecked(snapped_start, query.time_interval.end()); // TODO: snap end?

        let time_iterator =
            TimeStepIter::new_with_interval_incl_start(snapped_interval, self.step)?;

        Ok(GdalLoadingInfo {
            info: GdalLoadingInfoPartIterator::NetCdfCf(NetCdfCfGdalLoadingInfoPartIterator::new(
                time_iterator,
                self.params.clone(),
                self.step,
                self.start,
                query.time_interval.end(),
            )),
        })
    }

    async fn result_descriptor(&self) -> Result<RasterResultDescriptor> {
        Ok(self.result_descriptor.clone())
    }

    fn box_clone(
        &self,
    ) -> Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct NetCdfCfGdalLoadingInfoPartIterator {
    time_step_iter: TimeStepIter,
    params: GdalDatasetParameters,
    step: TimeStep,
    dataset_time_start: TimeInstance,
    max_t2: TimeInstance,
}

impl NetCdfCfGdalLoadingInfoPartIterator {
    fn new(
        time_step_iter: TimeStepIter,
        params: GdalDatasetParameters,
        step: TimeStep,
        dataset_time_start: TimeInstance,
        max_t2: TimeInstance,
    ) -> Self {
        Self {
            time_step_iter,
            params,
            step,
            dataset_time_start,
            max_t2,
        }
    }
}

impl Iterator for NetCdfCfGdalLoadingInfoPartIterator {
    type Item = Result<GdalLoadingInfoPart>;

    fn next(&mut self) -> Option<Self::Item> {
        let t1 = self.time_step_iter.next()?;

        // our first band is the reference time
        // TODO: what if it intersects the reference time with t1 < ref?
        if t1 < self.dataset_time_start {
            return None;
        }

        let t2 = t1 + self.step;
        let t2 = t2.unwrap_or(self.max_t2);

        let time_interval = TimeInterval::new_unchecked(t1, t2);

        // TODO: off by one if date is larger than reference time
        let steps_between = self
            .step
            .num_steps_in_interval(TimeInterval::new_unchecked(self.dataset_time_start, t1))
            .unwrap(); // TODO: what to do if this fails?

        let mut params = self.params.clone();
        params.rasterband_channel = 1 + steps_between as usize;

        dbg!(
            &params,
            t1.as_rfc3339(),
            self.dataset_time_start.as_rfc3339()
        );

        Some(Ok(GdalLoadingInfoPart {
            time: time_interval,
            params,
        }))
    }
}
