use std::collections::HashMap;

use async_trait::async_trait;
use geoengine_datatypes::primitives::{
    RasterQueryRectangle, TimeInstance, TimeInterval, TimeStep, TimeStepIter,
};
use serde::{Deserialize, Serialize};

use crate::{
    engine::{MetaData, RasterResultDescriptor},
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
            vec![GdalLoadingInfoTemporalSlice {
                time: valid,
                params: Some(self.params.clone()),
            }]
            .into_iter()
        } else {
            vec![].into_iter()
        };

        Ok(GdalLoadingInfo {
            info: GdalLoadingInfoTemporalSliceIterator::Static { parts },
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

/// Meta data for a regular time series that begins and ends at the given `data_time` interval with multiple gdal data
/// sets `step` time apart. The `time_placeholders` in the file path of the dataset are replaced with the
/// specified time `reference` in specified time `format`. Inside the `data_time` the gdal source will load the data
/// from the files and outside it will create nodata.
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetaDataRegular {
    pub result_descriptor: RasterResultDescriptor,
    pub params: GdalDatasetParameters,
    pub time_placeholders: HashMap<String, GdalSourceTimePlaceholder>,
    pub data_time: TimeInterval,
    pub step: TimeStep,
}

#[async_trait]
impl MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for GdalMetaDataRegular
{
    async fn loading_info(&self, query: RasterQueryRectangle) -> Result<GdalLoadingInfo> {
        Ok(GdalLoadingInfo {
            info: GdalLoadingInfoTemporalSliceIterator::Dynamic(
                DynamicGdalLoadingInfoPartIterator::new(
                    self.params.clone(),
                    self.time_placeholders.clone(),
                    self.step,
                    query.time_interval,
                    self.data_time,
                )?,
            ),
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
    /// We use the end to specify the last, non-inclusive valid time point.
    /// Queries behind this point return no data.
    /// TODO: Alternatively, we could think about using the number of possible time steps in the future.
    pub end: TimeInstance,
    pub step: TimeStep,
    /// A band offset specifies the first band index to use for the first point in time.
    /// All other time steps are added to this offset.
    pub band_offset: usize,
}

#[async_trait]
// TODO: handle queries before and after valid time like in `GdalMetaDataRegular`
impl MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for GdalMetadataNetCdfCf
{
    async fn loading_info(&self, query: RasterQueryRectangle) -> Result<GdalLoadingInfo> {
        // special case: single step
        if self.start == self.end || self.step.step == 0 {
            let time = TimeInterval::new(self.start, self.end)?;

            let mut params = self.params.clone();
            params.rasterband_channel = 1 /* GDAL starts at 1 */ + self.band_offset;

            return GdalMetaDataStatic {
                time: Some(time),
                params,
                result_descriptor: self.result_descriptor.clone(),
            }
            .loading_info(query)
            .await;
        }

        let snapped_start = self
            .step
            .snap_relative(self.start, query.time_interval.start())?;

        let snapped_interval =
            TimeInterval::new_unchecked(snapped_start, query.time_interval.end()); // TODO: snap end?

        let time_iterator = TimeStepIter::new_with_interval(snapped_interval, self.step)?;

        Ok(GdalLoadingInfo {
            info: GdalLoadingInfoTemporalSliceIterator::NetCdfCf(
                NetCdfCfGdalLoadingInfoPartIterator::new(
                    time_iterator,
                    self.params.clone(),
                    self.step,
                    self.start,
                    self.end,
                    self.band_offset,
                ),
            ),
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

// TODO: custom deserializer that checks that that params are sorted and do not overlap
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetaDataList {
    pub result_descriptor: RasterResultDescriptor,
    pub params: Vec<GdalLoadingInfoTemporalSlice>,
}

#[async_trait]
impl MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle> for GdalMetaDataList {
    async fn loading_info(&self, query: RasterQueryRectangle) -> Result<GdalLoadingInfo> {
        #[allow(clippy::needless_collect)]
        let parts = self
            .params
            .iter()
            .filter(|item| item.time.intersects(&query.time_interval))
            .cloned()
            .collect::<Vec<_>>();

        Ok(GdalLoadingInfo {
            info: GdalLoadingInfoTemporalSliceIterator::Static {
                parts: parts.into_iter(),
            },
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
/// An iterator for gdal loading infos based on time placeholders that generates
/// a new loading info for each time step within `data_time` and an empty loading info
/// for the time before and after `date_time`.
pub struct DynamicGdalLoadingInfoPartIterator {
    time_step_iter: TimeStepIter,
    params: GdalDatasetParameters,
    time_placeholders: HashMap<String, GdalSourceTimePlaceholder>,
    step: TimeStep,
    query_time: TimeInterval,
    data_time: TimeInterval,
    state: DynamicGdalLoadingInfoPartIteratorState,
}

#[derive(Debug, Clone)]
enum DynamicGdalLoadingInfoPartIteratorState {
    BeforeDataTime,
    WithinDataTime,
    AfterDataTime,
    Finished,
}

impl DynamicGdalLoadingInfoPartIterator {
    fn new(
        params: GdalDatasetParameters,
        time_placeholders: HashMap<String, GdalSourceTimePlaceholder>,
        step: TimeStep,
        query_time: TimeInterval,
        data_time: TimeInterval,
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

        // depending on whether the query starts before, within or after the data, the time step iterator has to begin at a different point in time
        // because it only produces time steps within the data time. Before and after are handled separately.
        let (snapped_start, state) = if query_time.start() < data_time.start() {
            (
                data_time.start(),
                DynamicGdalLoadingInfoPartIteratorState::BeforeDataTime,
            )
        } else if query_time.start() < data_time.end() {
            (
                step.snap_relative(data_time.start(), query_time.start())?,
                DynamicGdalLoadingInfoPartIteratorState::WithinDataTime,
            )
        } else {
            (
                data_time.end(),
                DynamicGdalLoadingInfoPartIteratorState::AfterDataTime,
            )
        };

        // cap at end of data
        let mut end = query_time.end().min(data_time.end());

        // snap the end time to the _next_ step within in the `data_time`
        let snapped_end = step.snap_relative(data_time.start(), end)?;
        if snapped_end < end {
            end = (snapped_end + step)?;
        }

        // ensure start <= end
        end = end.max(snapped_start);

        let snapped_interval = TimeInterval::new_unchecked(snapped_start, end);

        let time_step_iter = TimeStepIter::new_with_interval(snapped_interval, step)?;

        Ok(Self {
            time_step_iter,
            params,
            time_placeholders,
            step,
            query_time,
            data_time,
            state,
        })
    }
}

impl Iterator for DynamicGdalLoadingInfoPartIterator {
    type Item = Result<GdalLoadingInfoTemporalSlice>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            DynamicGdalLoadingInfoPartIteratorState::BeforeDataTime => {
                if self.query_time.end() > self.data_time.start() {
                    self.state = DynamicGdalLoadingInfoPartIteratorState::WithinDataTime;
                } else {
                    self.state = DynamicGdalLoadingInfoPartIteratorState::Finished;
                }
                Some(Ok(GdalLoadingInfoTemporalSlice {
                    time: TimeInterval::new_unchecked(TimeInstance::MIN, self.data_time.start()),
                    params: None,
                }))
            }
            DynamicGdalLoadingInfoPartIteratorState::WithinDataTime => {
                if let Some(t) = self.time_step_iter.next() {
                    let t2 = t + self.step;
                    let t2 = t2.unwrap_or_else(|_| self.data_time.end());
                    let time_interval = TimeInterval::new_unchecked(t, t2);

                    let loading_info_part = self
                        .params
                        .replace_time_placeholders(&self.time_placeholders, time_interval)
                        .map(|loading_info_part_params| GdalLoadingInfoTemporalSlice {
                            time: time_interval,
                            params: Some(loading_info_part_params),
                        })
                        .map_err(Into::into);

                    Some(loading_info_part)
                } else {
                    self.state = DynamicGdalLoadingInfoPartIteratorState::Finished;

                    if self.query_time.end() > self.data_time.end() {
                        Some(Ok(GdalLoadingInfoTemporalSlice {
                            time: TimeInterval::new_unchecked(
                                self.data_time.end(),
                                TimeInstance::MAX,
                            ),
                            params: None,
                        }))
                    } else {
                        None
                    }
                }
            }
            DynamicGdalLoadingInfoPartIteratorState::AfterDataTime => {
                self.state = DynamicGdalLoadingInfoPartIteratorState::Finished;

                Some(Ok(GdalLoadingInfoTemporalSlice {
                    time: TimeInterval::new_unchecked(self.data_time.end(), TimeInstance::MAX),
                    params: None,
                }))
            }
            DynamicGdalLoadingInfoPartIteratorState::Finished => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NetCdfCfGdalLoadingInfoPartIterator {
    time_step_iter: TimeStepIter,
    params: GdalDatasetParameters,
    step: TimeStep,
    dataset_time_start: TimeInstance,
    max_t2: TimeInstance,
    band_offset: usize,
}

impl NetCdfCfGdalLoadingInfoPartIterator {
    fn new(
        time_step_iter: TimeStepIter,
        params: GdalDatasetParameters,
        step: TimeStep,
        dataset_time_start: TimeInstance,
        max_t2: TimeInstance,
        band_offset: usize,
    ) -> Self {
        Self {
            time_step_iter,
            params,
            step,
            dataset_time_start,
            max_t2,
            band_offset,
        }
    }
}

impl Iterator for NetCdfCfGdalLoadingInfoPartIterator {
    type Item = Result<GdalLoadingInfoTemporalSlice>;

    fn next(&mut self) -> Option<Self::Item> {
        let t1 = self.time_step_iter.next()?;

        // snap t1 relative to reference time
        let t1 = self.step.snap_relative(self.dataset_time_start, t1).ok()?;

        let t2 = t1 + self.step;
        let t2 = t2.unwrap_or(self.max_t2);

        let time_interval = TimeInterval::new_unchecked(t1, t2);

        // TODO: how to prevent generating loads of empty time intervals for a very small t1?
        if t1 < self.dataset_time_start {
            return Some(Ok(GdalLoadingInfoTemporalSlice {
                time: time_interval,
                params: None,
            }));
        }

        if t1 >= self.max_t2 {
            return None;
        }

        let steps_between = match self
            .step
            .num_steps_in_interval(TimeInterval::new_unchecked(self.dataset_time_start, t1))
        {
            Ok(num_steps) => num_steps,
            // should only happen when time intervals are faulty
            Err(error) => return Some(Err(error.into())),
        };

        // our first band is the reference time
        let mut params = self.params.clone();
        params.rasterband_channel =
            1 /* GDAL starts at 1 */ + self.band_offset + steps_between as usize;

        Some(Ok(GdalLoadingInfoTemporalSlice {
            time: time_interval,
            params: Some(params),
        }))
    }
}

#[derive(Debug, Clone)]
pub struct GdalLoadingInfo {
    /// partitions of dataset sorted by time
    pub info: GdalLoadingInfoTemporalSliceIterator,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum GdalLoadingInfoTemporalSliceIterator {
    Static {
        parts: std::vec::IntoIter<GdalLoadingInfoTemporalSlice>,
    },
    Dynamic(DynamicGdalLoadingInfoPartIterator),
    NetCdfCf(NetCdfCfGdalLoadingInfoPartIterator),
}

impl Iterator for GdalLoadingInfoTemporalSliceIterator {
    type Item = Result<GdalLoadingInfoTemporalSlice>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            GdalLoadingInfoTemporalSliceIterator::Static { parts } => parts.next().map(Result::Ok),
            GdalLoadingInfoTemporalSliceIterator::Dynamic(iter) => iter.next(),
            GdalLoadingInfoTemporalSliceIterator::NetCdfCf(iter) => iter.next(),
        }
    }
}

/// one temporal slice of the dataset that requires reading from exactly one Gdal dataset
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GdalLoadingInfoTemporalSlice {
    pub time: TimeInterval,
    pub params: Option<GdalDatasetParameters>,
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        hashmap,
        primitives::{
            DateTime, DateTimeParseFormat, Measurement, SpatialPartition2D, SpatialResolution,
            TimeGranularity,
        },
        raster::RasterDataType,
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    use crate::source::{FileNotFoundHandling, GdalDatasetGeoTransform, TimeReference};

    use super::*;

    fn create_regular_metadata() -> GdalMetaDataRegular {
        let no_data_value = Some(0.);

        GdalMetaDataRegular {
            result_descriptor: RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::epsg_4326().into(),
                measurement: Measurement::Unitless,
                time: None,
                bbox: None,
                resolution: None,
            },
            params: GdalDatasetParameters {
                file_path: "/foo/bar_%TIME%.tiff".into(),
                rasterband_channel: 0,
                geo_transform: TestDefault::test_default(),
                width: 360,
                height: 180,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value,
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: true,
                retry: None,
            },
            time_placeholders: hashmap! {
                "%TIME%".to_string() => GdalSourceTimePlaceholder {
                    format: DateTimeParseFormat::custom("%f".to_string()),
                    reference: TimeReference::Start,
                },
            },
            data_time: TimeInterval::new_unchecked(
                TimeInstance::from_millis_unchecked(0),
                TimeInstance::from_millis_unchecked(33),
            ),
            step: TimeStep {
                granularity: TimeGranularity::Millis,
                step: 11,
            },
        }
    }

    #[tokio::test]
    async fn test_regular_meta_data_result_descriptor() {
        let meta_data = create_regular_metadata();

        assert_eq!(
            meta_data.result_descriptor().await.unwrap(),
            RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::epsg_4326().into(),
                measurement: Measurement::Unitless,
                time: None,
                bbox: None,
                resolution: None,
            }
        );
    }

    #[tokio::test]
    async fn test_regular_meta_data_0_30() {
        let meta_data = create_regular_metadata();

        assert_eq!(
            meta_data
                .loading_info(RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 1.).into(),
                        (1., 0.).into()
                    ),
                    time_interval: TimeInterval::new_unchecked(0, 30),
                    spatial_resolution: SpatialResolution::one(),
                })
                .await
                .unwrap()
                .info
                .map(|p| {
                    let p = p.unwrap();
                    (
                        p.time,
                        p.params.map(|p| p.file_path.to_str().unwrap().to_owned()),
                    )
                })
                .collect::<Vec<_>>(),
            &[
                (
                    TimeInterval::new_unchecked(0, 11),
                    Some("/foo/bar_000000000.tiff".to_owned())
                ),
                (
                    TimeInterval::new_unchecked(11, 22),
                    Some("/foo/bar_011000000.tiff".to_owned())
                ),
                (
                    TimeInterval::new_unchecked(22, 33),
                    Some("/foo/bar_022000000.tiff".to_owned())
                ),
            ]
        );
    }

    #[tokio::test]
    async fn test_regular_meta_data_default_time() {
        let meta_data = create_regular_metadata();

        assert_eq!(
            meta_data
                .loading_info(RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 1.).into(),
                        (1., 0.).into()
                    ),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                })
                .await
                .unwrap()
                .info
                .map(|p| {
                    let p = p.unwrap();
                    (
                        p.time,
                        p.params.map(|p| p.file_path.to_str().unwrap().to_owned()),
                    )
                })
                .collect::<Vec<_>>(),
            &[
                (TimeInterval::new_unchecked(TimeInstance::MIN, 0), None),
                (
                    TimeInterval::new_unchecked(0, 11),
                    Some("/foo/bar_000000000.tiff".to_owned())
                ),
                (
                    TimeInterval::new_unchecked(11, 22),
                    Some("/foo/bar_011000000.tiff".to_owned())
                ),
                (
                    TimeInterval::new_unchecked(22, 33),
                    Some("/foo/bar_022000000.tiff".to_owned())
                ),
                (TimeInterval::new_unchecked(33, TimeInstance::MAX), None)
            ]
        );
    }

    #[tokio::test]
    async fn test_regular_meta_data_before_data() {
        let meta_data = create_regular_metadata();

        assert_eq!(
            meta_data
                .loading_info(RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 1.).into(),
                        (1., 0.).into()
                    ),
                    time_interval: TimeInterval::new_unchecked(-10, -5),
                    spatial_resolution: SpatialResolution::one(),
                })
                .await
                .unwrap()
                .info
                .map(|p| {
                    let p = p.unwrap();
                    (
                        p.time,
                        p.params.map(|p| p.file_path.to_str().unwrap().to_owned()),
                    )
                })
                .collect::<Vec<_>>(),
            &[(TimeInterval::new_unchecked(TimeInstance::MIN, 0), None),]
        );
    }

    #[tokio::test]
    async fn test_regular_meta_data_after_data() {
        let meta_data = create_regular_metadata();

        assert_eq!(
            meta_data
                .loading_info(RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 1.).into(),
                        (1., 0.).into()
                    ),
                    time_interval: TimeInterval::new_unchecked(50, 55),
                    spatial_resolution: SpatialResolution::one(),
                })
                .await
                .unwrap()
                .info
                .map(|p| {
                    let p = p.unwrap();
                    (
                        p.time,
                        p.params.map(|p| p.file_path.to_str().unwrap().to_owned()),
                    )
                })
                .collect::<Vec<_>>(),
            &[(TimeInterval::new_unchecked(33, TimeInstance::MAX), None)]
        );
    }

    #[tokio::test]
    async fn test_regular_meta_data_0_22() {
        let meta_data = create_regular_metadata();

        assert_eq!(
            meta_data
                .loading_info(RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 1.).into(),
                        (1., 0.).into()
                    ),
                    time_interval: TimeInterval::new_unchecked(0, 22),
                    spatial_resolution: SpatialResolution::one(),
                })
                .await
                .unwrap()
                .info
                .map(|p| {
                    let p = p.unwrap();
                    (
                        p.time,
                        p.params.map(|p| p.file_path.to_str().unwrap().to_owned()),
                    )
                })
                .collect::<Vec<_>>(),
            &[
                (
                    TimeInterval::new_unchecked(0, 11),
                    Some("/foo/bar_000000000.tiff".to_owned())
                ),
                (
                    TimeInterval::new_unchecked(11, 22),
                    Some("/foo/bar_011000000.tiff".to_owned())
                ),
            ]
        );
    }

    #[tokio::test]
    async fn test_regular_meta_data_0_20() {
        let meta_data = create_regular_metadata();

        assert_eq!(
            meta_data
                .loading_info(RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 1.).into(),
                        (1., 0.).into()
                    ),
                    time_interval: TimeInterval::new_unchecked(0, 20),
                    spatial_resolution: SpatialResolution::one(),
                })
                .await
                .unwrap()
                .info
                .map(|p| {
                    let p = p.unwrap();
                    (
                        p.time,
                        p.params.map(|p| p.file_path.to_str().unwrap().to_owned()),
                    )
                })
                .collect::<Vec<_>>(),
            &[
                (
                    TimeInterval::new_unchecked(0, 11),
                    Some("/foo/bar_000000000.tiff".to_owned())
                ),
                (
                    TimeInterval::new_unchecked(11, 22),
                    Some("/foo/bar_011000000.tiff".to_owned())
                ),
            ]
        );
    }

    #[tokio::test]
    async fn test_meta_data_list() {
        let no_data_value = Some(0.);

        let meta_data = GdalMetaDataList {
            result_descriptor: RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::epsg_4326().into(),
                measurement: Measurement::Unitless,
                time: None,
                bbox: None,
                resolution: None,
            },
            params: vec![
                GdalLoadingInfoTemporalSlice {
                    time: TimeInterval::new_unchecked(0, 1),
                    params: Some(GdalDatasetParameters {
                        file_path: "/foo/bar_0.tiff".into(),
                        rasterband_channel: 0,
                        geo_transform: TestDefault::test_default(),
                        width: 360,
                        height: 180,
                        file_not_found_handling: FileNotFoundHandling::NoData,
                        no_data_value,
                        properties_mapping: None,
                        gdal_open_options: None,
                        gdal_config_options: None,
                        allow_alphaband_as_mask: true,
                        retry: None,
                    }),
                },
                GdalLoadingInfoTemporalSlice {
                    time: TimeInterval::new_unchecked(1, 5),
                    params: Some(GdalDatasetParameters {
                        file_path: "/foo/bar_1.tiff".into(),
                        rasterband_channel: 0,
                        geo_transform: TestDefault::test_default(),
                        width: 360,
                        height: 180,
                        file_not_found_handling: FileNotFoundHandling::NoData,
                        no_data_value,
                        properties_mapping: None,
                        gdal_open_options: None,
                        gdal_config_options: None,
                        allow_alphaband_as_mask: true,
                        retry: None,
                    }),
                },
                GdalLoadingInfoTemporalSlice {
                    time: TimeInterval::new_unchecked(5, 6),
                    params: Some(GdalDatasetParameters {
                        file_path: "/foo/bar_2.tiff".into(),
                        rasterband_channel: 0,
                        geo_transform: TestDefault::test_default(),
                        width: 360,
                        height: 180,
                        file_not_found_handling: FileNotFoundHandling::NoData,
                        no_data_value,
                        properties_mapping: None,
                        gdal_open_options: None,
                        gdal_config_options: None,
                        allow_alphaband_as_mask: true,
                        retry: None,
                    }),
                },
            ],
        };

        assert_eq!(
            meta_data.result_descriptor().await.unwrap(),
            RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::epsg_4326().into(),
                measurement: Measurement::Unitless,
                time: None,
                bbox: None,
                resolution: None,
            }
        );

        assert_eq!(
            meta_data
                .loading_info(RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 1.).into(),
                        (1., 0.).into()
                    ),
                    time_interval: TimeInterval::new_unchecked(0, 3),
                    spatial_resolution: SpatialResolution::one(),
                })
                .await
                .unwrap()
                .info
                .map(|p| p
                    .unwrap()
                    .params
                    .unwrap()
                    .file_path
                    .to_str()
                    .unwrap()
                    .to_owned())
                .collect::<Vec<_>>(),
            &["/foo/bar_0.tiff", "/foo/bar_1.tiff",]
        );
    }

    #[tokio::test]
    async fn netcdf_cf_single_time_step() {
        let time_start = TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0));
        let time_end = TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0));
        let time_step = TimeStep {
            step: 0,
            granularity: TimeGranularity::Years,
        };

        let metadata = GdalMetadataNetCdfCf {
            result_descriptor: RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::epsg_4326().into(),
                measurement: Measurement::Unitless,
                time: None,
                bbox: None,
                resolution: None,
            },
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
                allow_alphaband_as_mask: true,
                retry: None,
            },
            start: time_start,
            end: time_end,
            step: time_step,
            band_offset: 0,
        };

        let query = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 128.).into(), (128., 0.).into()),
            time_interval: TimeInterval::new(time_start, time_end).unwrap(),
            spatial_resolution: SpatialResolution::one(),
        };

        let loading_info = metadata.loading_info(query).await.unwrap();
        let mut iter = loading_info.info;

        let step_1 = iter.next().unwrap().unwrap();

        assert_eq!(
            step_1.time,
            TimeInterval::new(
                TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0))
            )
            .unwrap()
        );
        assert_eq!(step_1.params.unwrap().rasterband_channel, 1);

        assert!(iter.next().is_none());
    }

    #[tokio::test]
    async fn netcdf_cf_single_time_step_with_offset() {
        let time_start = TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0));
        let time_end = TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0));
        let time_step = TimeStep {
            step: 0,
            granularity: TimeGranularity::Years,
        };

        let metadata = GdalMetadataNetCdfCf {
            result_descriptor: RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::epsg_4326().into(),
                measurement: Measurement::Unitless,
                time: None,
                bbox: None,
                resolution: None,
            },
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
                allow_alphaband_as_mask: true,
                retry: None,
            },
            start: time_start,
            end: time_end,
            step: time_step,
            band_offset: 1,
        };

        let query = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 128.).into(), (128., 0.).into()),
            time_interval: TimeInterval::new(time_start, time_end).unwrap(),
            spatial_resolution: SpatialResolution::one(),
        };

        let loading_info = metadata.loading_info(query).await.unwrap();
        let mut iter = loading_info.info;

        let step_1 = iter.next().unwrap().unwrap();

        assert_eq!(
            step_1.time,
            TimeInterval::new(
                TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0))
            )
            .unwrap()
        );
        assert_eq!(step_1.params.unwrap().rasterband_channel, 2);

        assert!(iter.next().is_none());
    }

    #[tokio::test]
    async fn netcdf_cf_time_steps_before_after() {
        let time_start = TimeInstance::from(DateTime::new_utc(2010, 1, 1, 0, 0, 0));
        let time_end = TimeInstance::from(DateTime::new_utc(2012, 1, 1, 0, 0, 0));
        let time_step = TimeStep {
            step: 1,
            granularity: TimeGranularity::Years,
        };

        let metadata = GdalMetadataNetCdfCf {
            result_descriptor: RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::epsg_4326().into(),
                measurement: Measurement::Unitless,
                time: None,
                bbox: None,
                resolution: None,
            },
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
                allow_alphaband_as_mask: true,
                retry: None,
            },
            start: time_start,
            end: time_end,
            step: time_step,
            band_offset: 0,
        };

        let query = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 128.).into(), (128., 0.).into()),
            time_interval: TimeInterval::new_unchecked(
                TimeInstance::from(DateTime::new_utc(2009, 7, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2013, 3, 1, 0, 0, 0)),
            ),
            spatial_resolution: SpatialResolution::one(),
        };

        let loading_info = metadata.loading_info(query).await.unwrap();
        let mut iter = loading_info.info;

        let step_1 = iter.next().unwrap().unwrap();

        assert_eq!(
            step_1.time,
            TimeInterval::new(
                TimeInstance::from(DateTime::new_utc(2009, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2010, 1, 1, 0, 0, 0))
            )
            .unwrap()
        );
        assert!(step_1.params.is_none());

        let step_2 = iter.next().unwrap().unwrap();

        assert_eq!(
            step_2.time,
            TimeInterval::new(
                TimeInstance::from(DateTime::new_utc(2010, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2011, 1, 1, 0, 0, 0))
            )
            .unwrap()
        );
        assert_eq!(step_2.params.unwrap().rasterband_channel, 1);

        let step_3 = iter.next().unwrap().unwrap();

        assert_eq!(
            step_3.time,
            TimeInterval::new(
                TimeInstance::from(DateTime::new_utc(2011, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2012, 1, 1, 0, 0, 0))
            )
            .unwrap()
        );
        assert_eq!(step_3.params.unwrap().rasterband_channel, 2);

        assert!(iter.next().is_none());
    }

    #[test]
    fn netcdf_cf_time_steps() {
        let time_start = TimeInstance::from(DateTime::new_utc(2010, 1, 1, 0, 0, 0));
        let time_end = TimeInstance::from(DateTime::new_utc(2022, 1, 1, 0, 0, 0));
        let time_step = TimeStep {
            step: 1,
            granularity: TimeGranularity::Years,
        };
        let mut iter = NetCdfCfGdalLoadingInfoPartIterator {
            time_step_iter: TimeStepIter::new_with_interval(
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
                allow_alphaband_as_mask: true,
                retry: None,
            },
            step: time_step,
            dataset_time_start: time_start,
            max_t2: time_end,
            band_offset: 0,
        };

        let step_1 = iter.next().unwrap().unwrap();

        assert_eq!(
            step_1.time,
            TimeInterval::new(
                TimeInstance::from(DateTime::new_utc(2010, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2011, 1, 1, 0, 0, 0))
            )
            .unwrap()
        );
        assert_eq!(step_1.params.unwrap().rasterband_channel, 1);

        let step_2 = iter.next().unwrap().unwrap();

        assert_eq!(
            step_2.time,
            TimeInterval::new(
                TimeInstance::from(DateTime::new_utc(2011, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2012, 1, 1, 0, 0, 0))
            )
            .unwrap()
        );
        assert_eq!(step_2.params.unwrap().rasterband_channel, 2);

        let step_3 = iter.next().unwrap().unwrap();

        assert_eq!(
            step_3.time,
            TimeInterval::new(
                TimeInstance::from(DateTime::new_utc(2012, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2013, 1, 1, 0, 0, 0))
            )
            .unwrap()
        );
        assert_eq!(step_3.params.unwrap().rasterband_channel, 3);

        for i in 4..=12 {
            let step = iter.next().unwrap().unwrap();

            assert_eq!(step.params.unwrap().rasterband_channel, i);
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
                time_step_iter: TimeStepIter::new_with_interval(
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
                    allow_alphaband_as_mask: true,
                    retry: None,
                },
                step: time_step,
                dataset_time_start: TimeInstance::from(DateTime::new_utc(2010, 1, 1, 0, 0, 0)),
                max_t2: TimeInstance::from(DateTime::new_utc(2022, 1, 1, 0, 0, 0)),
                band_offset: 0,
            }
        }

        let mut iter =
            iter_for_instance(TimeInstance::from(DateTime::new_utc(2014, 1, 1, 0, 0, 0)));

        let step = iter.next().unwrap().unwrap();

        assert_eq!(
            step.time,
            TimeInterval::new(
                TimeInstance::from(DateTime::new_utc(2014, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2015, 1, 1, 0, 0, 0))
            )
            .unwrap()
        );
        assert_eq!(step.params.unwrap().rasterband_channel, 5);

        assert!(iter.next().is_none());

        let mut iter =
            iter_for_instance(TimeInstance::from(DateTime::new_utc(2010, 1, 1, 0, 0, 0)));

        let step = iter.next().unwrap().unwrap();

        assert_eq!(
            step.time,
            TimeInterval::new(
                TimeInstance::from(DateTime::new_utc(2010, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2011, 1, 1, 0, 0, 0))
            )
            .unwrap()
        );
        assert_eq!(step.params.unwrap().rasterband_channel, 1);

        assert!(iter.next().is_none());

        let mut iter =
            iter_for_instance(TimeInstance::from(DateTime::new_utc(2021, 1, 1, 0, 0, 0)));

        let step = iter.next().unwrap().unwrap();

        assert_eq!(
            step.time,
            TimeInterval::new(
                TimeInstance::from(DateTime::new_utc(2021, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2022, 1, 1, 0, 0, 0))
            )
            .unwrap()
        );
        assert_eq!(step.params.unwrap().rasterband_channel, 12);

        assert!(iter.next().is_none());

        let iter = iter_for_instance(TimeInstance::from(DateTime::new_utc(2009, 1, 1, 0, 0, 0)))
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(
            iter.time,
            TimeInterval::new(
                TimeInstance::from(DateTime::new_utc(2009, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2010, 1, 1, 0, 0, 0))
            )
            .unwrap(),
        );
        assert!(iter.params.is_none());

        assert!(
            iter_for_instance(TimeInstance::from(DateTime::new_utc(2022, 1, 1, 0, 0, 0),))
                .next()
                .is_none()
        );
    }
}
