use crate::datasets::listing::DatasetListOptions;
use crate::datasets::listing::{ExternalDatasetProvider, ProvenanceOutput};
use crate::projects::{RasterSymbology, Symbology};
use crate::{
    datasets::{listing::DatasetListing, storage::ExternalDatasetProviderDefinition},
    util::user_input::Validated,
};
use async_trait::async_trait;
use gdal::{DatasetOptions, GdalOpenFlags};
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, ExternalDatasetId};
use geoengine_datatypes::operations::image::{Colorizer, RgbaColor};
use geoengine_datatypes::primitives::{
    DateTime, DateTimeParseFormat, Measurement, RasterQueryRectangle, TimeGranularity,
    TimeInstance, TimeInterval, TimeStep, VectorQueryRectangle,
};
use geoengine_datatypes::raster::{GdalGeoTransform, RasterDataType};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::TypedResultDescriptor;
use geoengine_operators::source::{
    FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalMetadataNetCdfCf,
};
use geoengine_operators::util::gdal::gdal_open_dataset_ex;
use geoengine_operators::{
    engine::{MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use log::debug;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub use self::error::NetCdfCf4DProviderError;
use self::gdalmd::MdGroup;

mod error;
pub mod gdalmd;

type Result<T, E = NetCdfCf4DProviderError> = std::result::Result<T, E>;

/// Singleton Provider with id `1690c483-b17f-4d98-95c8-00a64849cd0b`
pub const NETCDF_CF_PROVIDER_ID: DatasetProviderId =
    DatasetProviderId::from_u128(0x1690_c483_b17f_4d98_95c8_00a6_4849_cd0b);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetCdfCfDataProviderDefinition {
    pub name: String,
    pub path: PathBuf,
}

#[derive(Debug)]
pub struct NetCdfCfDataProvider {
    pub path: PathBuf,
}

#[typetag::serde]
#[async_trait]
impl ExternalDatasetProviderDefinition for NetCdfCfDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> crate::error::Result<Box<dyn ExternalDatasetProvider>> {
        Ok(Box::new(NetCdfCfDataProvider { path: self.path }))
    }

    fn type_name(&self) -> String {
        "NetCdfCfProviderDefinition".to_owned()
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DatasetProviderId {
        NETCDF_CF_PROVIDER_ID
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetCdfOverview {
    pub file_name: String,
    pub title: String,
    pub summary: String,
    pub spatial_reference: SpatialReference,
    pub groups: Vec<NetCdfGroup>,
    pub entities: Vec<NetCdfEntity>,
    pub time: TimeInterval,
    pub time_step: TimeStep,
    pub colorizer: Colorizer,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetCdfGroup {
    pub name: String,
    pub title: String,
    pub description: String,
    // TODO: would actually be nice if it were inside dataset/entity
    pub data_type: Option<RasterDataType>,
    // TODO: would actually be nice if it were inside dataset/entity
    pub unit: String,
    pub groups: Vec<NetCdfGroup>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetCdfEntity {
    pub id: usize,
    pub name: String,
}

trait ToNetCdfSubgroup {
    fn to_net_cdf_subgroup(&self) -> Result<NetCdfGroup>;
}

impl<'a> ToNetCdfSubgroup for MdGroup<'a> {
    fn to_net_cdf_subgroup(&self) -> Result<NetCdfGroup> {
        let name = self.name.clone();
        let title = self
            .attribute_as_string("standard_name")
            .unwrap_or_default();
        let description = self.attribute_as_string("long_name").unwrap_or_default();
        let unit = self.attribute_as_string("units").unwrap_or_default();

        let group_names = self.group_names();

        if group_names.is_empty() {
            let data_type = Some(self.datatype_of_numeric_array("ebv_cube")?);

            return Ok(NetCdfGroup {
                name,
                title,
                description,
                data_type,
                unit,
                groups: Vec::new(),
            });
        }

        let mut groups = Vec::with_capacity(group_names.len());

        for subgroup in group_names {
            groups.push(self.open_group(&subgroup)?.to_net_cdf_subgroup()?);
        }

        Ok(NetCdfGroup {
            name,
            title,
            description,
            data_type: None,
            unit,
            groups,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NetCdfCf4DDatasetId {
    pub file_name: String,
    pub group_names: Vec<String>,
    pub entity: usize,
}

impl NetCdfCfDataProvider {
    pub fn build_netcdf_tree(provider_path: &Path, dataset_path: &Path) -> Result<NetCdfOverview> {
        let path = provider_path.join(dataset_path);

        let ds = gdal_open_dataset_ex(
            &path,
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_MULTIDIM_RASTER,
                allowed_drivers: Some(&["netCDF"]),
                open_options: None,
                sibling_files: None,
            },
        )
        .context(error::InvalidDatasetIdFile)?;

        let root_group = MdGroup::from_dataset(&ds)?;

        let title = root_group
            .attribute_as_string("title")
            .context(error::MissingTitle)?;

        let summary = root_group
            .attribute_as_string("summary")
            .context(error::MissingSummary)?;

        let spatial_reference = root_group
            .attribute_as_string("geospatial_bounds_crs")
            .context(error::MissingCrs)?;
        let spatial_reference: SpatialReference =
            SpatialReference::from_str(&spatial_reference).context(error::CannotParseCrs)?;

        let entities = root_group
            .dimension_as_string_array("entity")
            .context(error::MissingEntities)?
            .into_iter()
            .enumerate()
            .map(|(id, name)| NetCdfEntity { id, name })
            .collect::<Vec<_>>();

        let groups = root_group
            .group_names()
            .iter()
            .map(|name| root_group.open_group(name)?.to_net_cdf_subgroup())
            .collect::<Result<Vec<_>>>()?;

        let start = root_group
            .attribute_as_string("time_coverage_start")
            .context(error::MissingTimeCoverageStart)?;
        let end = root_group
            .attribute_as_string("time_coverage_end")
            .context(error::MissingTimeCoverageEnd)?;
        let step = root_group
            .attribute_as_string("time_coverage_resolution")
            .context(error::MissingTimeCoverageResolution)?;

        let (time_start, time_end, time_step) = parse_time_coverage(&start, &end, &step)?;

        let colorizer = load_colorizer(&path).or_else(|error| {
            debug!("Use fallback colorizer: {:?}", error);
            fallback_colorizer()
        })?;

        Ok(NetCdfOverview {
            file_name: path
                .strip_prefix(provider_path)
                .context(error::DatasetIsNotInProviderPath)?
                .to_string_lossy()
                .to_string(),
            title,
            summary,
            spatial_reference,
            groups,
            entities,
            time: TimeInterval::new(time_start, time_end)
                .context(error::InvalidTimeRangeForDataset)?,
            time_step,
            colorizer,
        })
    }

    pub(crate) fn listing_from_netcdf(
        id: DatasetProviderId,
        provider_path: &Path,
        dataset_path: &Path,
    ) -> Result<Vec<DatasetListing>> {
        let tree = Self::build_netcdf_tree(provider_path, dataset_path)?;

        let mut paths: VecDeque<Vec<&NetCdfGroup>> = tree.groups.iter().map(|s| vec![s]).collect();

        let mut listings = Vec::new();

        while let Some(path) = paths.pop_front() {
            let tail = path.last().context(error::PathToDataIsEmpty)?;

            if !tail.groups.is_empty() {
                for subgroup in &tail.groups {
                    let mut updated_path = path.clone();
                    updated_path.push(subgroup);
                    paths.push_back(updated_path);
                }

                continue;
            }

            // emit datasets

            let group_title_path = path
                .iter()
                .map(|s| s.title.as_str())
                .collect::<Vec<&str>>()
                .join(" > ");

            let group_names = path.iter().map(|s| s.name.clone()).collect::<Vec<String>>();

            let data_type = tail.data_type.context(error::MissingDataType)?;

            for entity in &tree.entities {
                let dataset_id = NetCdfCf4DDatasetId {
                    file_name: tree.file_name.clone(),
                    group_names: group_names.clone(),
                    entity: entity.id,
                };

                listings.push(DatasetListing {
                    id: DatasetId::External(ExternalDatasetId {
                        provider_id: id,
                        dataset_id: serde_json::to_string(&dataset_id).unwrap_or_default(),
                    }),
                    name: format!(
                        "{title}: {group_title_path} > {entity_name}",
                        title = tree.title,
                        entity_name = entity.name
                    ),
                    description: tree.summary.clone(),
                    tags: vec![], // TODO: where to get from file?
                    source_operator: "GdalSource".to_owned(),
                    result_descriptor: TypedResultDescriptor::Raster(RasterResultDescriptor {
                        data_type,
                        spatial_reference: tree.spatial_reference.into(),
                        measurement: derive_measurement(tail.unit.clone()),
                        no_data_value: None, // we don't want to open the dataset at this point. We should get rid of the result descriptor in the listing in general
                    }),
                    symbology: Some(Symbology::Raster(RasterSymbology {
                        opacity: 1.0,
                        colorizer: tree.colorizer.clone(),
                    })),
                });
            }
        }

        Ok(listings)
    }

    async fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>>
    {
        let dataset =
            dataset
                .external()
                .ok_or(NetCdfCf4DProviderError::InvalidExternalDatasetId {
                    provider: NETCDF_CF_PROVIDER_ID,
                })?;

        let dataset_id: NetCdfCf4DDatasetId =
            serde_json::from_str(&dataset.dataset_id).context(error::CannotParseDatasetId)?;

        let path = self.path.join(dataset_id.file_name);

        // check that file does not "escape" the provider path
        if let Err(source) = path.strip_prefix(self.path.as_path()) {
            return Err(NetCdfCf4DProviderError::DatasetIsNotInProviderPath { source });
        }

        let gdal_path = format!(
            "NETCDF:{path}:/{group}/ebv_cube",
            path = path.to_string_lossy(),
            group = dataset_id.group_names.join("/")
        );

        let dataset = gdal_open_dataset_ex(
            &path,
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_MULTIDIM_RASTER,
                allowed_drivers: Some(&["netCDF"]),
                open_options: None,
                sibling_files: None,
            },
        )
        .context(error::InvalidDatasetIdFile)?;

        let root_group = MdGroup::from_dataset(&dataset)?;

        let start = root_group
            .attribute_as_string("time_coverage_start")
            .context(error::MissingTimeCoverageStart)?;
        let end = root_group
            .attribute_as_string("time_coverage_end")
            .context(error::MissingTimeCoverageEnd)?;
        let step = root_group
            .attribute_as_string("time_coverage_resolution")
            .context(error::MissingTimeCoverageResolution)?;

        let (start, end, step) = parse_time_coverage(&start, &end, &step)?;

        let geo_transform = {
            let crs_array = root_group.open_array("crs")?;
            let geo_transform = crs_array
                .attribute_as_string("GeoTransform")
                .context(error::CannotGetGeoTransform)?;
            parse_geo_transform(&geo_transform)?
        };

        // traverse groups
        let mut group_stack = vec![root_group];

        // let mut group = root_group;
        for group_name in &dataset_id.group_names {
            group_stack.push(
                group_stack
                    .last()
                    .expect("at least root group in here")
                    .open_group(group_name)?,
            );
            // group = group.open_group(group_name)?;
        }

        let data_array = group_stack
            .last()
            .expect("at least root group in here")
            .open_array("ebv_cube")?;

        let dimensions = data_array.dimensions()?;

        let result_descriptor = RasterResultDescriptor {
            data_type: data_array.data_type()?,
            spatial_reference: data_array.spatial_reference()?,
            measurement: derive_measurement(data_array.unit().context(error::CannotRetrieveUnit)?),
            no_data_value: data_array.no_data_value(),
        };

        let params = GdalDatasetParameters {
            file_path: gdal_path.into(),
            rasterband_channel: 0, // we calculate offsets in our source
            geo_transform,
            file_not_found_handling: FileNotFoundHandling::Error,
            no_data_value: result_descriptor.no_data_value,
            properties_mapping: None,
            width: dimensions.lon,
            height: dimensions.lat,
            gdal_open_options: None,
            gdal_config_options: None,
        };

        Ok(Box::new(GdalMetadataNetCdfCf {
            params,
            result_descriptor,
            start,
            end, // TODO: Use this or time dimension size (number of steps)?
            step,
            band_offset: dataset_id.entity as usize * dimensions.time,
        }))
    }
}

fn derive_measurement(unit: String) -> Measurement {
    if unit.trim().is_empty() || unit == "no unit" {
        return Measurement::Unitless;
    }

    // TODO: other types of measurements

    Measurement::continuous(String::default(), Some(unit))
}

/// Load a colorizer from a path that is `path` with suffix `.colorizer.json`.
fn load_colorizer(path: &Path) -> Result<Colorizer> {
    use std::io::Read;

    let colorizer_path = path.with_extension("colorizer.json");

    let mut file = std::fs::File::open(colorizer_path).context(error::CannotOpenColorizerFile)?;

    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .context(error::CannotReadColorizerFile)?;

    let colorizer: Colorizer =
        serde_json::from_str(&contents).context(error::CannotParseColorizer)?;

    Ok(colorizer)
}

/// A simple colorizer between 0 and 255
/// TODO: generate better default by using `NetCDF` metadata
fn fallback_colorizer() -> Result<Colorizer> {
    Colorizer::linear_gradient(
        vec![
            (0.0.try_into().expect("not nan"), RgbaColor::black()).into(),
            (255.0.try_into().expect("not nan"), RgbaColor::white()).into(),
        ],
        RgbaColor::transparent(),
        RgbaColor::transparent(),
    )
    .context(error::CannotCreateFallbackColorizer)
}

fn parse_geo_transform(input: &str) -> Result<GdalDatasetGeoTransform> {
    let numbers: Vec<f64> = input
        .split_whitespace()
        .map(|s| s.parse().context(error::InvalidGeoTransformNumbers))
        .collect::<Result<Vec<_>>>()?;

    if numbers.len() != 6 {
        return Err(NetCdfCf4DProviderError::InvalidGeoTransformLength {
            length: numbers.len(),
        });
    }

    let gdal_geo_transform: GdalGeoTransform = [
        numbers[0], numbers[1], numbers[2], numbers[3], numbers[4], numbers[5],
    ];

    Ok(gdal_geo_transform.into())
}

fn parse_date(input: &str) -> Result<DateTime> {
    if let Ok(year) = input.parse::<i32>() {
        return DateTime::new_utc_checked(year, 1, 1, 0, 0, 0)
            .context(error::TimeCoverageYearOverflows { year });
    }

    DateTime::parse_from_str(input, &DateTimeParseFormat::ymd()).map_err(|e| {
        NetCdfCf4DProviderError::CannotParseTimeCoverageDate {
            source: Box::new(e),
        }
    })
}

fn parse_time_step(input: &str) -> Result<TimeStep> {
    let duration_str = if let Some(duration_str) = input.strip_prefix('P') {
        duration_str
    } else {
        return Err(NetCdfCf4DProviderError::TimeCoverageResolutionMustStartWithP);
    };

    let parts = duration_str
        .split('-')
        .map(str::parse)
        .collect::<Result<Vec<u32>, std::num::ParseIntError>>()
        .context(error::TimeCoverageResolutionMustConsistsOnlyOfIntParts)?;

    if parts.is_empty() {
        return Err(NetCdfCf4DProviderError::TimeCoverageResolutionPartsMustNotBeEmpty);
    }

    Ok(match parts.as_slice() {
        [year, 0, 0, ..] => TimeStep {
            granularity: TimeGranularity::Years,
            step: *year,
        },
        [0, month, 0, ..] => TimeStep {
            granularity: TimeGranularity::Months,
            step: *month,
        },
        [0, 0, day, ..] => TimeStep {
            granularity: TimeGranularity::Days,
            step: *day,
        },
        // TODO: fix format and parse other options
        _ => return Err(NetCdfCf4DProviderError::NotYetImplemented),
    })
}

fn parse_time_coverage(
    start: &str,
    end: &str,
    resolution: &str,
) -> Result<(TimeInstance, TimeInstance, TimeStep)> {
    // TODO: parse datetimes

    let start: TimeInstance = parse_date(start)?.into();
    let end: TimeInstance = parse_date(end)?.into();
    let step = parse_time_step(resolution)?;

    // add one step to provide a right side boundary for the close-open interval
    let end = (end + step).context(error::CannotDefineTimeCoverageEnd)?;

    Ok((start, end, step))
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

            let provider_path = self.path.clone();
            let relative_path = if let Ok(p) = entry.path().strip_prefix(&provider_path) {
                p.to_path_buf()
            } else {
                // cannot actually happen since `entry` is listed from `provider_path`
                continue;
            };

            let listing = tokio::task::spawn_blocking(move || {
                Self::listing_from_netcdf(NETCDF_CF_PROVIDER_ID, &provider_path, &relative_path)
            })
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
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
        self.meta_data(dataset).await.map_err(|error| {
            geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(error),
            }
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
        primitives::{SpatialPartition2D, SpatialResolution, TimeInterval},
        spatial_reference::SpatialReferenceAuthority,
        test_data,
    };
    use geoengine_operators::source::{
        FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters,
        GdalLoadingInfoTemporalSlice,
    };

    use super::*;

    #[test]
    fn test_parse_time_coverage() {
        let result = parse_time_coverage("2010", "2020", "P0001-00-00").unwrap();
        let expected = (
            TimeInstance::from(DateTime::new_utc(2010, 1, 1, 0, 0, 0)),
            TimeInstance::from(DateTime::new_utc(2021, 1, 1, 0, 0, 0)),
            TimeStep {
                granularity: TimeGranularity::Years,
                step: 1,
            },
        );
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_date() {
        assert_eq!(
            parse_date("2010").unwrap(),
            DateTime::new_utc(2010, 1, 1, 0, 0, 0)
        );
        assert_eq!(
            parse_date("-1000").unwrap(),
            DateTime::new_utc(-1000, 1, 1, 0, 0, 0)
        );
        assert_eq!(
            parse_date("2010-04-02").unwrap(),
            DateTime::new_utc(2010, 4, 2, 0, 0, 0)
        );
        assert_eq!(
            parse_date("-1000-04-02").unwrap(),
            DateTime::new_utc(-1000, 4, 2, 0, 0, 0)
        );
    }

    #[test]
    fn test_parse_time_step() {
        assert_eq!(
            parse_time_step("P0001-00-00").unwrap(),
            TimeStep {
                granularity: TimeGranularity::Years,
                step: 1,
            }
        );
        assert_eq!(
            parse_time_step("P0005-00-00").unwrap(),
            TimeStep {
                granularity: TimeGranularity::Years,
                step: 5,
            }
        );
        assert_eq!(
            parse_time_step("P0010-00-00").unwrap(),
            TimeStep {
                granularity: TimeGranularity::Years,
                step: 10,
            }
        );
        assert_eq!(
            parse_time_step("P0000-06-00").unwrap(),
            TimeStep {
                granularity: TimeGranularity::Months,
                step: 6,
            }
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_listing_from_netcdf_m() {
        let provider_id =
            DatasetProviderId::from_str("bf6bb6ea-5d5d-467d-bad1-267bf3a54470").unwrap();

        let listing = NetCdfCfDataProvider::listing_from_netcdf(
            provider_id,
            test_data!("netcdf4d"),
            Path::new("dataset_m.nc"),
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

        let symbology = Some(Symbology::Raster(RasterSymbology {
            opacity: 1.0,
            colorizer: Colorizer::LinearGradient {
                breakpoints: vec![
                    (0.0.try_into().unwrap(), RgbaColor::new(0, 0, 0, 255)).into(),
                    (
                        255.0.try_into().unwrap(),
                        RgbaColor::new(255, 255, 255, 255),
                    )
                        .into(),
                ],
                no_data_color: RgbaColor::new(0, 0, 0, 0),
                default_color: RgbaColor::new(0, 0, 0, 0),
            },
        }));

        assert_eq!(
            listing[0],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id: serde_json::json!({
                        "fileName": "dataset_m.nc",
                        "groupNames": ["metric_1"],
                        "entity": 0
                    })
                    .to_string(),
                }),
                name: "Test dataset metric: Random metric 1 > entity01".into(),
                description: "CFake description of test dataset with metric.".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor: result_descriptor.clone(),
                symbology: symbology.clone(),
            }
        );
        assert_eq!(
            listing[1],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id: serde_json::json!({
                        "fileName": "dataset_m.nc",
                        "groupNames": ["metric_1"],
                        "entity": 1
                    })
                    .to_string(),
                }),
                name: "Test dataset metric: Random metric 1 > entity02".into(),
                description: "CFake description of test dataset with metric.".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor: result_descriptor.clone(),
                symbology: symbology.clone(),
            }
        );
        assert_eq!(
            listing[2],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id: serde_json::json!({
                        "fileName": "dataset_m.nc",
                        "groupNames": ["metric_1"],
                        "entity": 2
                    })
                    .to_string(),
                }),
                name: "Test dataset metric: Random metric 1 > entity03".into(),
                description: "CFake description of test dataset with metric.".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor: result_descriptor.clone(),
                symbology: symbology.clone(),
            }
        );
        assert_eq!(
            listing[3],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id: serde_json::json!({
                        "fileName": "dataset_m.nc",
                        "groupNames": ["metric_2"],
                        "entity": 0
                    })
                    .to_string(),
                }),
                name: "Test dataset metric: Random metric 2 > entity01".into(),
                description: "CFake description of test dataset with metric.".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor: result_descriptor.clone(),
                symbology: symbology.clone(),
            }
        );
        assert_eq!(
            listing[4],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id: serde_json::json!({
                        "fileName": "dataset_m.nc",
                        "groupNames": ["metric_2"],
                        "entity": 1
                    })
                    .to_string(),
                }),
                name: "Test dataset metric: Random metric 2 > entity02".into(),
                description: "CFake description of test dataset with metric.".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor: result_descriptor.clone(),
                symbology: symbology.clone(),
            }
        );
        assert_eq!(
            listing[5],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id: serde_json::json!({
                        "fileName": "dataset_m.nc",
                        "groupNames": ["metric_2"],
                        "entity": 2
                    })
                    .to_string(),
                }),
                name: "Test dataset metric: Random metric 2 > entity03".into(),
                description: "CFake description of test dataset with metric.".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor,
                symbology,
            }
        );
    }

    #[tokio::test]
    async fn test_listing_from_netcdf_sm() {
        let provider_id =
            DatasetProviderId::from_str("bf6bb6ea-5d5d-467d-bad1-267bf3a54470").unwrap();

        let listing = NetCdfCfDataProvider::listing_from_netcdf(
            provider_id,
            test_data!("netcdf4d"),
            Path::new("dataset_sm.nc"),
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

        let symbology = Some(Symbology::Raster(RasterSymbology {
            opacity: 1.0,
            colorizer: Colorizer::LinearGradient {
                breakpoints: vec![
                    (0.0.try_into().unwrap(), RgbaColor::new(68, 1, 84, 255)).into(),
                    (50.0.try_into().unwrap(), RgbaColor::new(33, 145, 140, 255)).into(),
                    (100.0.try_into().unwrap(), RgbaColor::new(253, 231, 37, 255)).into(),
                ],
                no_data_color: RgbaColor::new(0, 0, 0, 0),
                default_color: RgbaColor::new(0, 0, 0, 0),
            },
        }));

        assert_eq!(
            listing[0],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id: serde_json::json!({
                        "fileName": "dataset_sm.nc",
                        "groupNames": ["scenario_1", "metric_1"],
                        "entity": 0
                    })
                    .to_string(),
                }),
                name:
                    "Test dataset metric and scenario: Sustainability > Random metric 1 > entity01"
                        .into(),
                description: "Fake description of test dataset with metric and scenario.".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor: result_descriptor.clone(),
                symbology: symbology.clone(),
            }
        );
        assert_eq!(
            listing[19],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id: serde_json::json!({
                        "fileName": "dataset_sm.nc",
                        "groupNames": ["scenario_5", "metric_2"],
                        "entity": 1
                    })
                    .to_string(),
                }),
                name: "Test dataset metric and scenario: Fossil-fueled Development > Random metric 2 > entity02".into(),
                description: "Fake description of test dataset with metric and scenario.".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor,
                symbology,
            }
        );
    }

    #[tokio::test]
    async fn test_metadata_from_netcdf_sm() {
        let provider = NetCdfCfDataProvider {
            path: test_data!("netcdf4d/").to_path_buf(),
        };

        let metadata = provider
            .meta_data(&DatasetId::External(ExternalDatasetId {
                provider_id: NETCDF_CF_PROVIDER_ID,
                dataset_id: serde_json::json!({
                    "fileName": "dataset_sm.nc",
                    "groupNames": ["scenario_5", "metric_2"],
                    "entity": 1
                })
                .to_string(),
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
                time_interval: TimeInstance::from(DateTime::new_utc(2001, 4, 1, 0, 0, 0)).into(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    0.000_343_322_7, // 256 pixel
                    0.000_343_322_7, // 256 pixel
                ),
            })
            .await
            .unwrap();

        let mut loading_info_parts = Vec::<GdalLoadingInfoTemporalSlice>::new();
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
            GdalLoadingInfoTemporalSlice {
                time: TimeInterval::new_unchecked(946_684_800_000, 1_262_304_000_000),
                params: Some(GdalDatasetParameters {
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
                })
            }
        );
    }
}
