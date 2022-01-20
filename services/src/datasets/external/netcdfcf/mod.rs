use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::datasets::listing::DatasetListOptions;
use crate::datasets::listing::{ExternalDatasetProvider, ProvenanceOutput};
use crate::error::Error;
use crate::{
    datasets::{listing::DatasetListing, storage::ExternalDatasetProviderDefinition},
    util::user_input::Validated,
};
use async_trait::async_trait;
use chrono::NaiveDate;
use gdal::Metadata;
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, ExternalDatasetId};
use geoengine_datatypes::primitives::{
    Measurement, RasterQueryRectangle, TimeGranularity, TimeInstance, TimeInterval, TimeStep,
    VectorQueryRectangle,
};
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::TypedResultDescriptor;
use geoengine_operators::source::GdalMetadataNetCdfCf;
use geoengine_operators::util::gdal::{
    gdal_open_dataset, gdal_parameters_from_dataset, raster_descriptor_from_dataset,
};
use geoengine_operators::{
    engine::{MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use log::debug;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

pub use self::error::NetCdfCf4DProviderError;

mod error;

type Result<T, E = NetCdfCf4DProviderError> = std::result::Result<T, E>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetCdfCfDataProviderDefinition {
    pub id: DatasetProviderId,
    pub name: String,
    pub path: PathBuf,
}

#[typetag::serde]
#[async_trait]
impl ExternalDatasetProviderDefinition for NetCdfCfDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> crate::error::Result<Box<dyn ExternalDatasetProvider>> {
        Ok(Box::new(NetCdfCfDataProvider {
            id: self.id,
            path: self.path,
        }))
    }

    fn type_name(&self) -> String {
        "NetCdfCfProviderDefinition".to_owned()
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DatasetProviderId {
        self.id
    }
}

pub struct NetCdfCfDataProvider {
    id: DatasetProviderId,
    path: PathBuf,
}

struct SubDataset {
    name: String,
    desc: String,
}

impl NetCdfCfDataProvider {
    fn subdatasets_from_subdatasets_metadata(metadata: &[String]) -> Result<Vec<SubDataset>> {
        // TODO: Read field `standard_name` from metadata

        let mut subdatasets = vec![];
        for i in (0..metadata.len()).step_by(2) {
            let name = metadata
                .get(i)
                .and_then(|s| s.split_once('='))
                .ok_or(NetCdfCf4DProviderError::CannotComputeSubdatasetsFromMetadata)?
                .1
                .to_owned();
            let desc = metadata
                .get(i + 1)
                .and_then(|s| s.split_once('='))
                .ok_or(NetCdfCf4DProviderError::CannotComputeSubdatasetsFromMetadata)?
                .1
                .to_owned();

            subdatasets.push(SubDataset { name, desc });
        }
        Ok(subdatasets)
    }

    fn listing_from_netcdf(id: DatasetProviderId, path: &Path) -> Result<Vec<DatasetListing>> {
        let ds = gdal_open_dataset(path).context(error::InvalidDatasetIdFile)?;

        let title = ds
            .metadata_item("NC_GLOBAL#title", "")
            .ok_or(NetCdfCf4DProviderError::MissingTitle)?;

        let spatial_reference = SpatialReference::from_str(
            &ds.metadata_item("NC_GLOBAL#geospatial_bounds_crs", "")
                .ok_or(NetCdfCf4DProviderError::MissingCrs)?,
        )
        .context(error::CannotParseCrs)?
        .into();

        let subdatasets = Self::subdatasets_from_subdatasets_metadata(
            &ds.metadata_domain("SUBDATASETS")
                .ok_or(NetCdfCf4DProviderError::MissingSubdatasets)?,
        )?;

        let mut subdataset_iter = subdatasets.into_iter();

        let entities_ds = subdataset_iter
            .next()
            .ok_or(NetCdfCf4DProviderError::MissingEntities)?;

        // TODO: make parsing of entities dimensions more robust
        let num_entities: u32 = entities_ds.desc[1..entities_ds
            .desc
            .find('x')
            .ok_or(NetCdfCf4DProviderError::CannotSplitNumberOfEntities)?]
            .parse()
            .context(error::CannotParseNumberOfEntities)?;

        let mut datasets = Vec::new();
        for group_ds in subdataset_iter {
            let data_type = if let Some(data_type) = datatype_from_desc(&group_ds.desc) {
                data_type
            } else {
                return Err(NetCdfCf4DProviderError::DataTypeNotYetImplemented {
                    data_type: group_ds.desc,
                });
            };

            for entity_idx in 0..num_entities {
                let group_name = group_ds
                    .name
                    .rsplit_once(':')
                    .ok_or(NetCdfCf4DProviderError::MissingGroupName)?
                    .1;
                let file_name = path
                    .file_name()
                    .ok_or(NetCdfCf4DProviderError::MissingFileName)?
                    .to_string_lossy();

                let group_titles = titles_of_group_hierarchy(&ds, group_name)?.join(" > ");

                datasets.push(DatasetListing {
                    id: DatasetId::External(ExternalDatasetId {
                        provider_id: id,
                        dataset_id: format!("{file_name}::{group_name}::{entity_idx}"),
                    }),
                    name: format!("{title}: {group_titles} > {entity_idx}"),
                    description: "".to_owned(), // TODO
                    tags: vec![],               // TODO
                    source_operator: "GdalSource".to_owned(),
                    result_descriptor: TypedResultDescriptor::Raster(RasterResultDescriptor {
                        data_type,
                        spatial_reference,
                        measurement: Measurement::Unitless, // TODO
                        no_data_value: None, // we don't want to open the dataset at this point. We should get rid of the result descriptor in the listing in general
                    }),
                    symbology: None,
                });
            }
        }

        Ok(datasets)
    }

    async fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>>
    {
        let dataset = dataset
            .external()
            .ok_or(NetCdfCf4DProviderError::InvalidExternalDatasetId { provider: self.id })?;

        let split: Vec<_> = dataset.dataset_id.split("::").collect();

        let [path, group, entity_idx] = if let [path, group, entity_idx] = split.as_slice() {
            [path, group, entity_idx]
        } else {
            return Err(NetCdfCf4DProviderError::InvalidDatasetIdLength {
                length: split.len(),
            });
        };
        let entity_idx = entity_idx
            .parse::<u32>()
            .context(error::DatasetIdEntityNotANumber)?;

        let path = self.path.join(path);

        let gdal_path = format!(
            "NETCDF:{path}:{group}",
            path = path.to_string_lossy(),
            group = group
        );

        let ds = gdal_open_dataset(Path::new(&gdal_path)).context(error::InvalidDatasetIdFile)?;

        let result_descriptor = raster_descriptor_from_dataset(&ds, 1, None)
            .context(error::GeneratingResultDescriptorFromDataset)?; // use band 1 because bands are homogeneous

        let (start, end, step) = parse_time_coverage(
            &ds.metadata_item("NC_GLOBAL#time_coverage_start", "")
                .ok_or(NetCdfCf4DProviderError::MissingTimeCoverageStart)?,
            &ds.metadata_item("NC_GLOBAL#time_coverage_end", "")
                .ok_or(NetCdfCf4DProviderError::MissingTimeCoverageEnd)?,
            &ds.metadata_item("NC_GLOBAL#time_coverage_resolution", "")
                .ok_or(NetCdfCf4DProviderError::MissingTimeCoverageResolution)?,
        )?;

        let num_time_steps = steps_in_time_coverage(start, end, step)?;

        let params = gdal_parameters_from_dataset(&ds, 1, Path::new(&gdal_path), Some(0), None)
            .context(error::GeneratingParametersFromDataset)?;

        Ok(Box::new(GdalMetadataNetCdfCf {
            params,
            result_descriptor,
            start,
            end, // TODO: Use this or time dimension size (number of steps)?
            step,
            band_offset: (entity_idx * num_time_steps) as usize,
        }))
    }
}

fn steps_in_time_coverage(start: TimeInstance, end: TimeInstance, step: TimeStep) -> Result<u32> {
    let time_interval =
        TimeInterval::new(start, end).context(error::InvalidTimeCoverageInterval)?;

    let steps = step
        .num_steps_in_interval(time_interval)
        .context(error::CannotCalculateStepsInTimeCoverageInterval)?;

    Ok(steps + 1)
}

fn parse_time_coverage(
    start: &str,
    end: &str,
    resolution: &str,
) -> Result<(TimeInstance, TimeInstance, TimeStep)> {
    let start = start
        .parse::<i32>()
        .context(error::CannotConvertTimeCoverageToInt)?;
    let end = end
        .parse::<i32>()
        .context(error::CannotConvertTimeCoverageToInt)?;

    Ok(match resolution {
        "Yearly" | "every 1 year" => {
            let start = TimeInstance::from(NaiveDate::from_ymd(start, 1, 1).and_hms(0, 0, 0));
            // end + 1 because it is exclusive for us but inclusive in the metadata
            let end = TimeInstance::from(NaiveDate::from_ymd(end + 1, 1, 1).and_hms(0, 0, 0));
            let step = TimeStep {
                granularity: TimeGranularity::Years,
                step: 1,
            };
            (start, end, step)
        }
        "decade" => {
            let start = TimeInstance::from(NaiveDate::from_ymd(start, 1, 1).and_hms(0, 0, 0));
            // end + 1 because it is exclusive for us but inclusive in the metadata
            let end = TimeInstance::from(NaiveDate::from_ymd(end + 1, 1, 1).and_hms(0, 0, 0));
            let step = TimeStep {
                granularity: TimeGranularity::Years,
                step: 10,
            };
            (start, end, step)
        }
        _ => return Err(NetCdfCf4DProviderError::NotYetImplemented), // TODO
    })
}

fn titles_of_group_hierarchy(ds: &gdal::Dataset, groups: &str) -> Result<Vec<String>> {
    let mut groups = groups
        .trim_start_matches('/')
        .split('/')
        .collect::<Vec<_>>();

    groups.pop(); // remove trailing `/ebv_cube`

    let mut group_titles = Vec::with_capacity(groups.len());
    let mut metadata_key_prefix = String::new();

    for group in groups {
        metadata_key_prefix.push('/');
        metadata_key_prefix.push_str(group);

        let metadata_key = format!("{metadata_key_prefix}/NC_GLOBAL#standard_name");

        if let Some(title) = ds.metadata_item(&metadata_key, "") {
            group_titles.push(title);
        } else {
            return Err(NetCdfCf4DProviderError::NoTitleForGroup { metadata_key });
        }
    }

    Ok(group_titles)
}

fn strip_datatype_info(desc: &str) -> Option<&str> {
    if desc.is_empty() {
        return None;
    }

    let desc = &desc[..desc.len() - 1];

    Some(desc.rsplit_once('(')?.1)
}

fn datatype_from_desc(desc: &str) -> Option<RasterDataType> {
    let desc = strip_datatype_info(desc)?;

    // TODO: add unsigned integers

    match desc {
        "8-bit integer" => Some(RasterDataType::I8),
        "16-bit integer" => Some(RasterDataType::I16),
        "32-bit integer" => Some(RasterDataType::I32),
        "64-bit integer" => Some(RasterDataType::I64),
        "32-bit floating-point" => Some(RasterDataType::F32),
        "64-bit floating-point" => Some(RasterDataType::F64),
        _ => None,
    }
}

#[async_trait]
impl ExternalDatasetProvider for NetCdfCfDataProvider {
    async fn list(
        &self,
        options: Validated<DatasetListOptions>,
    ) -> crate::error::Result<Vec<DatasetListing>> {
        // TODO: user right management
        // TODO: options

        let mut dir = tokio::fs::read_dir(&self.path).await?;

        let mut datasets = vec![];
        while let Some(entry) = dir.next_entry().await? {
            if !entry.path().is_file() {
                continue;
            }

            let id = self.id;
            let listing =
                tokio::task::spawn_blocking(move || Self::listing_from_netcdf(id, &entry.path()))
                    .await?;

            match listing {
                Ok(listing) => datasets.extend(listing),
                Err(e) => debug!("Failed to list dataset: {}", e),
            }
        }

        // TODO: react to filter and sort options
        // TODO: don't compute everything and filter then
        let datasets = datasets
            .into_iter()
            .skip(options.user_input.offset as usize)
            .take(options.user_input.limit as usize)
            .collect();

        Ok(datasets)
    }

    async fn provenance(&self, dataset: &DatasetId) -> crate::error::Result<ProvenanceOutput> {
        Ok(ProvenanceOutput {
            dataset: dataset.clone(),
            provenance: None,
        })
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for NetCdfCfDataProvider
{
    async fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        // TODO spawn blocking
        self.meta_data(dataset)
            .await
            .map_err(|_| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(Error::InvalidExternalDatasetId { provider: self.id }),
            })
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for NetCdfCfDataProvider
{
    async fn meta_data(
        &self,
        _dataset: &DatasetId,
    ) -> Result<
        Box<
            dyn MetaData<
                MockDatasetDataSourceLoadingInfo,
                VectorResultDescriptor,
                VectorQueryRectangle,
            >,
        >,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for NetCdfCfDataProvider
{
    async fn meta_data(
        &self,
        _dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{SpatialPartition2D, SpatialResolution},
        spatial_reference::SpatialReferenceAuthority,
        test_data,
    };
    use geoengine_operators::source::{
        FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalLoadingInfoPart,
    };

    use super::*;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_listing_from_netcdf_m() {
        let provider_id =
            DatasetProviderId::from_str("bf6bb6ea-5d5d-467d-bad1-267bf3a54470").unwrap();

        let listing = NetCdfCfDataProvider::listing_from_netcdf(
            provider_id,
            test_data!("netcdf4d/dataset_m.nc"),
        )
        .unwrap();

        assert_eq!(listing.len(), 6);

        let result_descriptor: TypedResultDescriptor = RasterResultDescriptor {
            data_type: RasterDataType::I16,
            spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 4326).into(),
            measurement: Measurement::Unitless,
            no_data_value: None,
        }
        .into();

        assert_eq!(
            listing[0],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id: "dataset_m.nc::/metric_1/ebv_cube::0".into(),
                }),
                name: "Test dataset metric: Random metric 1 > 0".into(),
                description: "".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor: result_descriptor.clone(),
                symbology: None
            }
        );
        assert_eq!(
            listing[1],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id: "dataset_m.nc::/metric_1/ebv_cube::1".into(),
                }),
                name: "Test dataset metric: Random metric 1 > 1".into(),
                description: "".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor: result_descriptor.clone(),
                symbology: None
            }
        );
        assert_eq!(
            listing[2],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id: "dataset_m.nc::/metric_1/ebv_cube::2".into(),
                }),
                name: "Test dataset metric: Random metric 1 > 2".into(),
                description: "".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor: result_descriptor.clone(),
                symbology: None
            }
        );
        assert_eq!(
            listing[3],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id: "dataset_m.nc::/metric_2/ebv_cube::0".into(),
                }),
                name: "Test dataset metric: Random metric 2 > 0".into(),
                description: "".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor: result_descriptor.clone(),
                symbology: None
            }
        );
        assert_eq!(
            listing[4],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id: "dataset_m.nc::/metric_2/ebv_cube::1".into(),
                }),
                name: "Test dataset metric: Random metric 2 > 1".into(),
                description: "".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor: result_descriptor.clone(),
                symbology: None
            }
        );
        assert_eq!(
            listing[5],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id: "dataset_m.nc::/metric_2/ebv_cube::2".into(),
                }),
                name: "Test dataset metric: Random metric 2 > 2".into(),
                description: "".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor,
                symbology: None
            }
        );
    }

    #[tokio::test]
    async fn test_listing_from_netcdf_sm() {
        let provider_id =
            DatasetProviderId::from_str("bf6bb6ea-5d5d-467d-bad1-267bf3a54470").unwrap();

        let listing = NetCdfCfDataProvider::listing_from_netcdf(
            provider_id,
            test_data!("netcdf4d/dataset_sm.nc"),
        )
        .unwrap();

        assert_eq!(listing.len(), 20);

        let result_descriptor: TypedResultDescriptor = RasterResultDescriptor {
            data_type: RasterDataType::I16,
            spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 3035).into(),
            measurement: Measurement::Unitless,
            no_data_value: None,
        }
        .into();

        assert_eq!(
            listing[0],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id: "dataset_sm.nc::/scenario_1/metric_1/ebv_cube::0".into(),
                }),
                name: "Test dataset metric and scenario: Sustainability > Random metric 1 > 0"
                    .into(),
                description: "".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor: result_descriptor.clone(),
                symbology: None
            }
        );
        assert_eq!(
            listing[19],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id: "dataset_sm.nc::/scenario_5/metric_2/ebv_cube::1".into(),
                }),
                name: "Test dataset metric and scenario: Fossil-fueled Development > Random metric 2 > 1".into(),
                description: "".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor,
                symbology: None
            }
        );
    }

    // TODO: verify
    // TODO: do the samme for `dataset_m.nc`
    #[tokio::test]
    async fn test_metadata_from_netcdf_sm() {
        let provider = NetCdfCfDataProvider {
            id: DatasetProviderId::from_str("bf6bb6ea-5d5d-467d-bad1-267bf3a54470").unwrap(),
            path: test_data!("netcdf4d/").to_path_buf(),
        };

        let metadata = provider
            .meta_data(&DatasetId::External(ExternalDatasetId {
                provider_id: provider.id,
                dataset_id: "dataset_sm.nc::/scenario_5/metric_2/ebv_cube::1".into(),
            }))
            .await
            .unwrap();

        assert_eq!(
            metadata.result_descriptor().await.unwrap(),
            RasterResultDescriptor {
                data_type: RasterDataType::I16,
                spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 3035)
                    .into(),
                measurement: Measurement::Unitless,
                no_data_value: Some(-9999.),
            }
        );

        let loading_info = metadata
            .loading_info(RasterQueryRectangle {
                spatial_bounds: SpatialPartition2D::new(
                    (43.945_312_5, 0.791_015_625_25).into(),
                    (44.033_203_125, 0.703_125_25).into(),
                )
                .unwrap(),
                time_interval: TimeInstance::from(NaiveDate::from_ymd(2001, 4, 1).and_hms(0, 0, 0))
                    .into(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    0.000_343_322_7, // 256 pixel
                    0.000_343_322_7, // 256 pixel
                ),
            })
            .await
            .unwrap();

        let mut loading_info_parts = Vec::<GdalLoadingInfoPart>::new();
        for part in loading_info.info {
            loading_info_parts.push(part.unwrap());
        }

        assert_eq!(loading_info_parts.len(), 1);

        let file_path = format!(
            "NETCDF:{absolute_file_path}:/scenario_5/metric_2/ebv_cube",
            absolute_file_path = test_data!("netcdf4d/dataset_sm.nc")
                .canonicalize()
                .unwrap()
                .to_string_lossy()
        )
        .into();

        assert_eq!(
            loading_info_parts[0],
            GdalLoadingInfoPart {
                time: TimeInterval::new_unchecked(946_684_800_000, 1_262_304_000_000),
                params: GdalDatasetParameters {
                    file_path,
                    rasterband_channel: 4,
                    geo_transform: GdalDatasetGeoTransform {
                        origin_coordinate: (3_580_000.0, 2_370_000.0).into(),
                        x_pixel_size: 1000.0,
                        y_pixel_size: -1000.0,
                    },
                    width: 10,
                    height: 10,
                    file_not_found_handling: FileNotFoundHandling::Error,
                    no_data_value: Some(-9999.),
                    properties_mapping: None,
                    gdal_open_options: None,
                    gdal_config_options: None
                }
            }
        );
    }

    #[test]
    fn test_strip_datatype() {
        assert_eq!(
            strip_datatype_info("[3x3x5x5] /metric_1/ebv_cube (16-bit integer)"),
            Some("16-bit integer")
        );
    }

    #[test]
    fn test_datatype_from_desc() {
        assert_eq!(
            datatype_from_desc("[3x3x5x5] /metric_1/ebv_cube (16-bit integer)"),
            Some(RasterDataType::I16)
        );
        assert_eq!(
            datatype_from_desc("[3x3x5x5] /metric_1/ebv_cube (32-bit floating-point)"),
            Some(RasterDataType::F32)
        );
    }

    // #[test]
    // fn test_access() {
    // let ds = gdal_open_dataset(test_data!("netcdf4d/dataset_m.nc").file_prefix()).unwrap();

    // let subdatasets = ds.metadata_domain("SUBDATASETS").unwrap();

    // for i in (0..subdatasets.len()).step_by(2) {
    //     let name = subdatasets[i].split_once('=').unwrap().1;

    //     if !name.contains("entities") {
    //         continue;
    //     }

    //     dbg!(&name);

    //     let ds = gdal_open_dataset(Path::new(&name)).unwrap();

    //     for band in 1..=ds.raster_count() {
    //         let b = ds.rasterband(band).unwrap();

    //         // let xs = b.x_size();
    //         let ys = b.y_size();

    //         dbg!(ys);
    //     }
    // }

    // let dataset = gdal_open_dataset(Path::new(&format!(
    //     "NETCDF:{}:entities",
    //     test_data!("netcdf4d/dataset_m.nc").to_string_lossy()
    // )))
    // .unwrap();

    // dbg!(dataset.raster_count());

    // let band = dataset.rasterband(1).unwrap();

    // dbg!(band.x_size());
    // dbg!(band.y_size());
    // dbg!(band.block_size());
    // dbg!(band.band_type());

    // let block_index: (usize, usize) = (0, 0);

    // let size = band.block_size();
    // let pixels = (size.0 * size.1) as usize;
    // let mut data: Vec<u8> = Vec::with_capacity(pixels);

    // let rv = unsafe {
    //     gdal_sys::GDALReadBlock(
    //         band.gdal_object_ptr(),
    //         block_index.0 as c_int,
    //         block_index.1 as c_int,
    //         data.as_mut_ptr() as GDALRasterBandH,
    //     )
    // };

    // unsafe {
    //     data.set_len(pixels);
    // };

    //     let data = band.read_block::<u8>((0, 0)).unwrap();

    //     dbg!(data);

    //     gdal_sys::GDALDataType
    // }
}
