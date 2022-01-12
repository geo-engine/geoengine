use std::collections::HashMap;

use async_trait::async_trait;
use geoengine_datatypes::primitives::{TimeInstance, TimeInterval, TimeStep, TimeStepIter};
use serde::{Deserialize, Serialize};

use crate::{
    engine::{MetaData, RasterQueryRectangle, RasterResultDescriptor},
    error::Error,
    util::Result,
};

use super::{GdalDatasetParameters, GdalSourceTimePlaceholder};

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
pub struct DynamicGdalLoadingInfoPartIterator {
    time_step_iter: TimeStepIter,
    params: GdalDatasetParameters,
    time_placeholders: HashMap<String, GdalSourceTimePlaceholder>,
    step: TimeStep,
    max_t2: TimeInstance,
}

impl DynamicGdalLoadingInfoPartIterator {
    fn new(
        time_step_iter: TimeStepIter,
        params: GdalDatasetParameters,
        time_placeholders: HashMap<String, GdalSourceTimePlaceholder>,
        step: TimeStep,
        max_t2: TimeInstance,
    ) -> Result<Self> {
        // TODO: maybe fail on deserialization
        if time_placeholders.is_empty()
            || time_placeholders.keys().any(String::is_empty)
            || time_placeholders
                .values()
                .any(|value| value.format.is_empty())
        {
            return Err(Error::DynamicGdalSourceSpecHasEmptyTimePlaceholders);
        }

        Ok(Self {
            time_step_iter,
            params,
            time_placeholders,
            step,
            max_t2,
        })
    }
}

impl Iterator for DynamicGdalLoadingInfoPartIterator {
    type Item = Result<GdalLoadingInfoPart>;

    fn next(&mut self) -> Option<Self::Item> {
        let t1 = self.time_step_iter.next()?;

        let t2 = t1 + self.step;
        let t2 = t2.unwrap_or(self.max_t2);

        let time_interval = TimeInterval::new_unchecked(t1, t2);

        let loading_info_part = self
            .params
            .replace_time_placeholders(&self.time_placeholders, time_interval)
            .map(|loading_info_part_params| GdalLoadingInfoPart {
                time: time_interval,
                params: loading_info_part_params,
            });

        Some(loading_info_part)
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

    // fn band_offset(reference_time: TimeInstance, step: TimeStep, input_time: TimeInstance) -> usize {
    //     reference_time + step * band_index as TimeInstance
    // }
}

impl Iterator for NetCdfCfGdalLoadingInfoPartIterator {
    type Item = Result<GdalLoadingInfoPart>;

    fn next(&mut self) -> Option<Self::Item> {
        let t1 = self.time_step_iter.next()?;

        // our first band is the reference time
        // TODO: what if it intersects the reference time with t1 < ref?
        if t1 < self.dataset_time_start || t1 >= self.max_t2 {
            return None;
        }

        // snap t1 relative to reference time
        // TODO: error handling instead of `ok`
        let t1 = self.step.snap_relative(self.dataset_time_start, t1).ok()?;

        let t2 = t1 + self.step;
        let t2 = t2.unwrap_or(self.max_t2);

        let time_interval = TimeInterval::new_unchecked(t1, t2);

        // TODO: off by one if date is larger than reference time
        let steps_between = if t1 == self.dataset_time_start {
            0
        } else {
            1 + self
                .step
                .num_steps_in_interval(TimeInterval::new_unchecked(self.dataset_time_start, t1))
                .unwrap() // TODO: what to do if this fails?
        };

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

#[derive(Debug, Clone)]
pub struct GdalLoadingInfo {
    /// partitions of dataset sorted by time
    pub info: GdalLoadingInfoPartIterator,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum GdalLoadingInfoPartIterator {
    Static {
        parts: std::vec::IntoIter<GdalLoadingInfoPart>,
    },
    Dynamic(DynamicGdalLoadingInfoPartIterator),
    NetCdfCf(NetCdfCfGdalLoadingInfoPartIterator),
}

impl Iterator for GdalLoadingInfoPartIterator {
    type Item = Result<GdalLoadingInfoPart>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            GdalLoadingInfoPartIterator::Static { parts } => parts.next().map(Result::Ok),
            GdalLoadingInfoPartIterator::Dynamic(iter) => iter.next(),
            GdalLoadingInfoPartIterator::NetCdfCf(iter) => iter.next(),
        }
    }
}

/// one temporal slice of the dataset that requires reading from exactly one Gdal dataset
#[derive(Debug, Clone, PartialEq)]
pub struct GdalLoadingInfoPart {
    pub time: TimeInterval,
    pub params: GdalDatasetParameters,
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use geoengine_datatypes::primitives::TimeGranularity;

    use crate::source::{FileNotFoundHandling, GdalDatasetGeoTransform};

    use super::*;

    #[test]
    fn netcdf_cf_time_steps() {
        let time_start = TimeInstance::from(NaiveDate::from_ymd(2010, 1, 1).and_hms(0, 0, 0));
        let time_end = TimeInstance::from(NaiveDate::from_ymd(2022, 1, 1).and_hms(0, 0, 0));
        let time_step = TimeStep {
            step: 1,
            granularity: TimeGranularity::Years,
        };
        let mut iter = NetCdfCfGdalLoadingInfoPartIterator {
            time_step_iter: TimeStepIter::new_with_interval_incl_start(
                TimeInterval::new(time_start, time_end).unwrap(),
                time_step,
            )
            .unwrap(),
            params: GdalDatasetParameters {
                file_path: "path/to/ds".into(),
                rasterband_channel: 0,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: (0., 0.).into(),
                    x_pixel_size: 1.,
                    y_pixel_size: 1.,
                },
                width: 128,
                height: 128,
                file_not_found_handling: FileNotFoundHandling::Error,
                no_data_value: None,
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
            },
            step: time_step,
            dataset_time_start: time_start,
            max_t2: time_end,
        };

        let step_1 = iter.next().unwrap().unwrap();

        assert_eq!(
            step_1.time,
            TimeInterval::new(
                TimeInstance::from(NaiveDate::from_ymd(2010, 1, 1).and_hms(0, 0, 0)),
                TimeInstance::from(NaiveDate::from_ymd(2011, 1, 1).and_hms(0, 0, 0))
            )
            .unwrap()
        );
        assert_eq!(step_1.params.rasterband_channel, 1);

        let step_2 = iter.next().unwrap().unwrap();

        assert_eq!(
            step_2.time,
            TimeInterval::new(
                TimeInstance::from(NaiveDate::from_ymd(2011, 1, 1).and_hms(0, 0, 0)),
                TimeInstance::from(NaiveDate::from_ymd(2012, 1, 1).and_hms(0, 0, 0))
            )
            .unwrap()
        );
        assert_eq!(step_2.params.rasterband_channel, 2);

        let step_3 = iter.next().unwrap().unwrap();

        assert_eq!(
            step_3.time,
            TimeInterval::new(
                TimeInstance::from(NaiveDate::from_ymd(2012, 1, 1).and_hms(0, 0, 0)),
                TimeInstance::from(NaiveDate::from_ymd(2013, 1, 1).and_hms(0, 0, 0))
            )
            .unwrap()
        );
        assert_eq!(step_3.params.rasterband_channel, 3);

        for i in 4..=12 {
            let step = iter.next().unwrap().unwrap();

            assert_eq!(step.params.rasterband_channel, i);
        }

        assert!(iter.next().is_none());
    }

    #[test]
    fn netcdf_cf_time_step_instant() {
        fn iter_for_instance(instance: TimeInstance) -> NetCdfCfGdalLoadingInfoPartIterator {
            let time_step = TimeStep {
                step: 1,
                granularity: TimeGranularity::Years,
            };
            NetCdfCfGdalLoadingInfoPartIterator {
                time_step_iter: TimeStepIter::new_with_interval_incl_start(
                    TimeInterval::new_instant(instance).unwrap(),
                    time_step,
                )
                .unwrap(),
                params: GdalDatasetParameters {
                    file_path: "path/to/ds".into(),
                    rasterband_channel: 0,
                    geo_transform: GdalDatasetGeoTransform {
                        origin_coordinate: (0., 0.).into(),
                        x_pixel_size: 1.,
                        y_pixel_size: 1.,
                    },
                    width: 128,
                    height: 128,
                    file_not_found_handling: FileNotFoundHandling::Error,
                    no_data_value: None,
                    properties_mapping: None,
                    gdal_open_options: None,
                    gdal_config_options: None,
                },
                step: time_step,
                dataset_time_start: TimeInstance::from(
                    NaiveDate::from_ymd(2010, 1, 1).and_hms(0, 0, 0),
                ),
                max_t2: TimeInstance::from(NaiveDate::from_ymd(2022, 1, 1).and_hms(0, 0, 0)),
            }
        }

        let mut iter = iter_for_instance(TimeInstance::from(
            NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
        ));

        let step = iter.next().unwrap().unwrap();

        assert_eq!(
            step.time,
            TimeInterval::new(
                TimeInstance::from(NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0)),
                TimeInstance::from(NaiveDate::from_ymd(2015, 1, 1).and_hms(0, 0, 0))
            )
            .unwrap()
        );
        assert_eq!(step.params.rasterband_channel, 5);

        assert!(iter.next().is_none());

        let mut iter = iter_for_instance(TimeInstance::from(
            NaiveDate::from_ymd(2010, 1, 1).and_hms(0, 0, 0),
        ));

        let step = iter.next().unwrap().unwrap();

        assert_eq!(
            step.time,
            TimeInterval::new(
                TimeInstance::from(NaiveDate::from_ymd(2010, 1, 1).and_hms(0, 0, 0)),
                TimeInstance::from(NaiveDate::from_ymd(2011, 1, 1).and_hms(0, 0, 0))
            )
            .unwrap()
        );
        assert_eq!(step.params.rasterband_channel, 1);

        assert!(iter.next().is_none());

        let mut iter = iter_for_instance(TimeInstance::from(
            NaiveDate::from_ymd(2021, 1, 1).and_hms(0, 0, 0),
        ));

        let step = iter.next().unwrap().unwrap();

        assert_eq!(
            step.time,
            TimeInterval::new(
                TimeInstance::from(NaiveDate::from_ymd(2021, 1, 1).and_hms(0, 0, 0)),
                TimeInstance::from(NaiveDate::from_ymd(2022, 1, 1).and_hms(0, 0, 0))
            )
            .unwrap()
        );
        assert_eq!(step.params.rasterband_channel, 12);

        assert!(iter.next().is_none());

        assert!(iter_for_instance(TimeInstance::from(
            NaiveDate::from_ymd(2009, 1, 1).and_hms(0, 0, 0),
        ))
        .next()
        .is_none());

        assert!(iter_for_instance(TimeInstance::from(
            NaiveDate::from_ymd(2022, 1, 1).and_hms(0, 0, 0),
        ))
        .next()
        .is_none());
    }
}
