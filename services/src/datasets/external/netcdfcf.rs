use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::datasets::listing::{ExternalDatasetProvider, ProvenanceOutput};
use crate::error::Error;
use crate::{datasets::listing::DatasetListOptions, error::Result};
use crate::{
    datasets::{
        listing::DatasetListing,
        storage::{DatasetDefinition, ExternalDatasetProviderDefinition, MetaDataDefinition},
    },
    error,
    util::user_input::Validated,
};
use async_trait::async_trait;
use chrono::NaiveDate;
use futures::StreamExt;
use gdal::{Dataset, Metadata};
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, ExternalDatasetId};
use geoengine_datatypes::primitives::{
    Measurement, TimeGranularity, TimeInstance, TimeInterval, TimeStep,
};
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
use geoengine_operators::engine::TypedResultDescriptor;
use geoengine_operators::source::{
    FileNotFoundHandling, GdalDatasetParameters, GdalMetadataNetCdfCf,
};
use geoengine_operators::util::gdal::{
    gdal_open_dataset, gdal_parameters_from_dataset, raster_descriptor_from_dataset,
};
use geoengine_operators::{
    engine::{
        MetaData, MetaDataProvider, RasterQueryRectangle, RasterResultDescriptor,
        VectorQueryRectangle, VectorResultDescriptor,
    },
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use log::debug;
use serde::{Deserialize, Serialize};

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
        let mut subdatasets = vec![];
        for i in (0..metadata.len()).step_by(2) {
            let name = metadata
                .get(i)
                .and_then(|s| s.split_once('='))
                .ok_or(Error::NetCdfCfMissingMetaData)?
                .1
                .to_owned();
            let desc = metadata
                .get(i + 1)
                .and_then(|s| s.split_once('='))
                .ok_or(Error::NetCdfCfMissingMetaData)?
                .1
                .to_owned();

            subdatasets.push(SubDataset { name, desc });
        }
        Ok(subdatasets)
    }

    fn listing_from_netcdf(id: DatasetProviderId, path: &Path) -> Result<Vec<DatasetListing>> {
        let ds = gdal_open_dataset(path)?;

        // TODO: report more details in error
        let title = ds
            .metadata_item("NC_GLOBAL#title", "")
            .ok_or(Error::NetCdfCfMissingMetaData)?;

        let spatial_reference = SpatialReference::from_str(
            &ds.metadata_item("NC_GLOBAL#geospatial_bounds_crs", "")
                .ok_or(Error::NetCdfCfMissingMetaData)?,
        )?
        .into();

        let subdatasets = Self::subdatasets_from_subdatasets_metadata(
            &ds.metadata_domain("SUBDATASETS")
                .ok_or(Error::NetCdfCfMissingMetaData)?,
        )?;

        let mut subdataset_iter = subdatasets.into_iter();

        let entities_ds = subdataset_iter
            .next()
            .ok_or(Error::NetCdfCfMissingMetaData)?;

        // TODO: make parsing of entities dimensions more robust
        let num_entities: u32 = entities_ds.desc[1..entities_ds
            .desc
            .find('x')
            .ok_or(Error::NetCdfCfMissingMetaData)?]
            .parse()
            .map_err(|_| Error::NetCdfCfMissingMetaData)?;

        let mut datasets = Vec::new();
        for group_ds in subdataset_iter {
            // TODO: properly parse the data type from metadata
            let data_type = if group_ds.desc.contains("32-bit floating-point") {
                RasterDataType::F32
            } else {
                return Err(Error::NotYetImplemented);
            };

            for entity in 0..num_entities {
                datasets.push(DatasetListing {
                    id: DatasetId::External(ExternalDatasetId {
                        provider_id: id,
                        dataset_id: format!(
                            "{file_name}::{group_name}::{entity_idx}",
                            file_name = path
                                .file_name()
                                .ok_or(Error::NetCdfCfMissingMetaData)?
                                .to_string_lossy(),
                            group_name = group_ds
                                .name
                                .rsplit_once(':')
                                .ok_or(Error::NetCdfCfMissingMetaData)?
                                .1,
                            entity_idx = entity
                        ),
                    }),
                    name: format!(
                        "{title} {group_name} {entity_idx}",
                        title = title,
                        group_name = group_ds.name,
                        entity_idx = entity
                    ),
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
            .ok_or(geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(Error::InvalidExternalDatasetId { provider: self.id }),
            })?;
        let split: Vec<_> = dataset.dataset_id.split("::").collect();

        let path = split
            .get(0)
            .ok_or(Error::InvalidExternalDatasetId { provider: self.id })?;

        let group = split
            .get(1)
            .ok_or(Error::InvalidExternalDatasetId { provider: self.id })?;

        let entity_idx: u32 = split
            .get(2)
            .ok_or(Error::InvalidExternalDatasetId { provider: self.id })
            .and_then(|s| s.parse().map_err(|_| Error::NetCdfCfMissingMetaData))?;

        let path = self.path.join(path);

        let gdal_path = format!(
            "NETCDF:{path}:{group}",
            path = path.to_string_lossy(),
            group = group
        );

        let ds = gdal_open_dataset(Path::new(&gdal_path))?;

        let time_coverage_start: i32 = ds
            .metadata_item("NC_GLOBAL#time_coverage_start", "")
            .ok_or(Error::NetCdfCfMissingMetaData)
            .and_then(|s| s.parse().map_err(|_| Error::NetCdfCfMissingMetaData))?;
        let time_coverage_end: i32 = ds
            .metadata_item("NC_GLOBAL#time_coverage_end", "")
            .ok_or(Error::NetCdfCfMissingMetaData)
            .and_then(|s| s.parse().map_err(|_| Error::NetCdfCfMissingMetaData))?;
        let time_coverage_resolution = ds
            .metadata_item("NC_GLOBAL#time_coverage_resolution", "")
            .ok_or(Error::NetCdfCfMissingMetaData)?;

        let result_descriptor = raster_descriptor_from_dataset(&ds, 1, None)?; // use band 1 because bands are homogeneous

        let (start, end, step) = match time_coverage_resolution.as_str() {
            "Yearly" => {
                let start = TimeInstance::from(
                    NaiveDate::from_ymd(time_coverage_start, 1, 1).and_hms(0, 0, 0),
                );
                // end + 1 because it is exclusive for us but inclusive in the metadata
                let end = TimeInstance::from(
                    NaiveDate::from_ymd(time_coverage_end + 1, 1, 1).and_hms(0, 0, 0),
                );
                let step = TimeStep {
                    granularity: TimeGranularity::Years,
                    step: 1,
                };
                (start, end, step)
            }
            _ => return Err(Error::NotYetImplemented), // TODO
        };

        let num_time_steps = step.num_steps_in_interval(TimeInterval::new(start, end)?)? + 1;
        dbg!("#############################", &num_time_steps);
        Ok(Box::new(GdalMetadataNetCdfCf {
            params: gdal_parameters_from_dataset(&ds, 1, Path::new(&gdal_path), Some(0), None)?,
            result_descriptor,
            start,
            end, // TODO: Use this or time dimension size (number of steps)?
            step,
            band_offset: (entity_idx * num_time_steps) as usize,
        }))
    }
}

#[async_trait]
impl ExternalDatasetProvider for NetCdfCfDataProvider {
    async fn list(&self, _options: Validated<DatasetListOptions>) -> Result<Vec<DatasetListing>> {
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

        Ok(datasets)
    }

    async fn provenance(&self, dataset: &DatasetId) -> Result<ProvenanceOutput> {
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
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
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
    use std::path::Path;

    use super::*;

    #[test]
    fn test_name() {
        let ds = gdal_open_dataset(Path::new(
            "/home/michael/rust-projects/geoengine/netcdf_data/dataset_sm.nc",
        ))
        .unwrap();
        // let meta = ds.metadata_domains();
        // let meta = dbg!(meta);

        let global = ds.metadata_domain("");

        let title = ds.metadata_item("NC_GLOBAL#title", "").unwrap();
        let time_coverage_start = ds
            .metadata_item("NC_GLOBAL#time_coverage_start", "")
            .unwrap();
        let time_coverage_end = ds.metadata_item("NC_GLOBAL#time_coverage_end", "").unwrap();
        let time_coverage_resolution = ds
            .metadata_item("NC_GLOBAL#time_coverage_resolution", "")
            .unwrap();

        dbg!(title);
        dbg!(time_coverage_start);
        dbg!(time_coverage_end);
        dbg!(time_coverage_resolution);

        let subdatasets = ds.metadata_domain("SUBDATASETS").unwrap();

        for i in (0..subdatasets.len()).step_by(2) {
            let name = subdatasets[i].split_once('=').unwrap().1;
            dbg!(&name);

            let ds = gdal_open_dataset(Path::new(&name)).unwrap();

            for band in 1..=ds.raster_count() {
                let b = ds.rasterband(band).unwrap();

                let xs = b.x_size();
                let ys = b.y_size();

                dbg!(ys);
            }
        }
    }
}
