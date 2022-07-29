#[cfg(feature = "ebv")]
pub use self::ebvportal_provider::{EbvPortalDataProvider, EBV_PROVIDER_ID};
pub use self::error::NetCdfCf4DProviderError;
use self::gdalmd::MdGroup;
pub use self::overviews::OverviewGeneration;
use self::overviews::{create_overviews, METADATA_FILE_NAME};
use crate::datasets::listing::ProvenanceOutput;
use crate::error::Error;
use crate::layers::external::DataProvider;
use crate::layers::external::DataProviderDefinition;
use crate::layers::layer::CollectionItem;
use crate::layers::layer::Layer;
use crate::layers::layer::LayerCollectionListOptions;
use crate::layers::layer::LayerCollectionListing;
use crate::layers::layer::LayerListing;
use crate::layers::layer::ProviderLayerCollectionId;
use crate::layers::layer::ProviderLayerId;
use crate::layers::listing::LayerCollectionId;
use crate::layers::listing::LayerCollectionProvider;
use crate::util::user_input::Validated;
use crate::workflows::workflow::Workflow;
use async_trait::async_trait;
use gdal::{DatasetOptions, GdalOpenFlags};
use geoengine_datatypes::dataset::DataProviderId;
use geoengine_datatypes::dataset::LayerId;
use geoengine_datatypes::dataset::{DataId, ExternalDataId};
use geoengine_datatypes::operations::image::{Colorizer, RgbaColor};
use geoengine_datatypes::primitives::{
    DateTime, DateTimeParseFormat, Measurement, RasterQueryRectangle, TimeGranularity,
    TimeInstance, TimeInterval, TimeStep, VectorQueryRectangle,
};
use geoengine_datatypes::raster::{GdalGeoTransform, RasterDataType};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::RasterOperator;
use geoengine_operators::engine::TypedOperator;
use geoengine_operators::source::GdalSource;
use geoengine_operators::source::GdalSourceParameters;
use geoengine_operators::source::{
    FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters,
    GdalLoadingInfoTemporalSlice, GdalMetaDataList, GdalMetadataNetCdfCf,
};
use geoengine_operators::util::gdal::gdal_open_dataset_ex;
use geoengine_operators::{
    engine::{MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::json;
use snafu::{OptionExt, ResultExt};
use std::collections::VecDeque;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use walkdir::{DirEntry, WalkDir};

#[cfg(feature = "ebv")]
mod ebvportal_api;
#[cfg(feature = "ebv")]
mod ebvportal_provider;
pub mod error;
pub mod gdalmd;
mod overviews;

type Result<T, E = NetCdfCf4DProviderError> = std::result::Result<T, E>;

/// Singleton Provider with id `1690c483-b17f-4d98-95c8-00a64849cd0b`
pub const NETCDF_CF_PROVIDER_ID: DataProviderId =
    DataProviderId::from_u128(0x1690_c483_b17f_4d98_95c8_00a6_4849_cd0b);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetCdfCfDataProviderDefinition {
    pub name: String,
    pub path: PathBuf,
    pub overviews: PathBuf,
}

#[derive(Debug)]
pub struct NetCdfCfDataProvider {
    pub path: PathBuf,
    pub overviews: PathBuf,
}

#[typetag::serde]
#[async_trait]
impl DataProviderDefinition for NetCdfCfDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> crate::error::Result<Box<dyn DataProvider>> {
        Ok(Box::new(NetCdfCfDataProvider {
            path: self.path,
            overviews: self.overviews,
        }))
    }

    fn type_name(&self) -> String {
        "NetCdfCfProviderDefinition".to_owned()
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DataProviderId {
        NETCDF_CF_PROVIDER_ID
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NetCdfOverview {
    pub file_name: String,
    pub title: String,
    pub summary: String,
    pub spatial_reference: SpatialReference,
    pub groups: Vec<NetCdfGroup>,
    pub entities: Vec<NetCdfEntity>,
    pub time_coverage: TimeCoverage,
    pub colorizer: Colorizer,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NetCdfGroup {
    pub name: String,
    pub title: String,
    pub description: String,
    // TODO: would actually be nice if it were inside dataset/entity
    pub data_type: Option<RasterDataType>,
    pub data_range: Option<(f64, f64)>,
    // TODO: would actually be nice if it were inside dataset/entity
    pub unit: String,
    pub groups: Vec<NetCdfGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NetCdfEntity {
    pub id: usize,
    pub name: String,
}

trait ToNetCdfSubgroup {
    fn to_net_cdf_subgroup(&self, compute_stats: bool) -> Result<NetCdfGroup>;
}

impl<'a> ToNetCdfSubgroup for MdGroup<'a> {
    fn to_net_cdf_subgroup(&self, compute_stats: bool) -> Result<NetCdfGroup> {
        let name = self.name.clone();
        debug!("to_net_cdf_subgroup for {name} with stats={compute_stats}");

        let title = self
            .attribute_as_string("standard_name")
            .unwrap_or_default();
        let description = self.attribute_as_string("long_name").unwrap_or_default();
        let unit = self.attribute_as_string("units").unwrap_or_default();

        let group_names = self.group_names();

        if group_names.is_empty() {
            let data_type = Some(self.datatype_of_numeric_array("ebv_cube")?);

            let data_range = if compute_stats {
                let array = self.open_array("ebv_cube")?;
                Some(array.min_max().context(error::CannotComputeMinMax)?)
            } else {
                None
            };

            return Ok(NetCdfGroup {
                name,
                title,
                description,
                data_type,
                data_range,
                unit,
                groups: Vec::new(),
            });
        }

        let mut groups = Vec::with_capacity(group_names.len());

        for subgroup in group_names {
            groups.push(
                self.open_group(&subgroup)?
                    .to_net_cdf_subgroup(compute_stats)?,
            );
        }

        Ok(NetCdfGroup {
            name,
            title,
            description,
            data_type: None,
            data_range: None,
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
    fn netcdf_tree_from_overviews(
        overview_path: &Path,
        dataset_path: &Path,
    ) -> Option<NetCdfOverview> {
        let tree_file_path = overview_path.join(dataset_path).join(METADATA_FILE_NAME);
        let file = std::fs::File::open(&tree_file_path).ok()?;
        let buf_reader = BufReader::new(file);
        serde_json::from_reader::<_, NetCdfOverview>(buf_reader).ok()
    }

    pub fn build_netcdf_tree(
        provider_path: &Path,
        overview_path: Option<&Path>,
        dataset_path: &Path,
        compute_stats: bool,
    ) -> Result<NetCdfOverview> {
        if let Some(netcdf_tree) = overview_path.and_then(|overview_path| {
            NetCdfCfDataProvider::netcdf_tree_from_overviews(overview_path, dataset_path)
        }) {
            return Ok(netcdf_tree);
        }

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
            .map(|name| {
                root_group
                    .open_group(name)?
                    .to_net_cdf_subgroup(compute_stats)
            })
            .collect::<Result<Vec<_>>>()?;

        let time_coverage = time_coverage(&root_group)?;

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
            time_coverage,
            colorizer,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn listing_from_netcdf(
        id: DataProviderId,
        provider_path: &Path,
        overview_path: Option<&Path>,
        dataset_path: &Path,
        compute_stats: bool,
    ) -> Result<Vec<LayerListing>> {
        let tree =
            Self::build_netcdf_tree(provider_path, overview_path, dataset_path, compute_stats)?;

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

            for entity in &tree.entities {
                let dataset_id = NetCdfCf4DDatasetId {
                    file_name: tree.file_name.clone(),
                    group_names: group_names.clone(),
                    entity: entity.id,
                };

                listings.push(LayerListing {
                    id: ProviderLayerId {
                        provider_id: id,
                        layer_id: LayerId(serde_json::to_string(&dataset_id).unwrap_or_default()),
                    },
                    name: format!(
                        "{title}: {group_title_path} > {entity_name}",
                        title = tree.title,
                        entity_name = entity.name
                    ),
                    description: tree.summary.clone(),
                });
            }
        }

        Ok(listings)
    }

    #[allow(clippy::too_many_lines)] // TODO: refactor method
    fn meta_data(
        path: &Path,
        overviews: &Path,
        id: &DataId,
    ) -> Result<Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>>
    {
        let dataset = id
            .external()
            .ok_or(NetCdfCf4DProviderError::InvalidExternalDataId {
                provider: NETCDF_CF_PROVIDER_ID,
            })?;

        let dataset_id: NetCdfCf4DDatasetId =
            serde_json::from_str(&dataset.layer_id.0).context(error::CannotParseDatasetId)?;

        // try to load from overviews
        if let Some(loading_info) = Self::meta_data_from_overviews(overviews, &dataset_id) {
            return match loading_info {
                Metadata::NetCDF(loading_info) => Ok(Box::new(loading_info)),
                Metadata::List(loading_info) => Ok(Box::new(loading_info)),
            };
        }

        let dataset_id: NetCdfCf4DDatasetId =
            serde_json::from_str(&dataset.layer_id.0).context(error::CannotParseDatasetId)?;

        let path = path.join(&dataset_id.file_name);

        // check that file does not "escape" the provider path
        if let Err(source) = path.strip_prefix(&path) {
            return Err(NetCdfCf4DProviderError::DatasetIsNotInProviderPath { source });
        }

        let group_path = dataset_id.group_names.join("/");
        let gdal_path = format!(
            "NETCDF:{path}:/{group_path}/ebv_cube",
            path = path.to_string_lossy()
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

        let time_coverage = time_coverage(&root_group)?;

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

            time: None,
            bbox: None,
        };

        let params = GdalDatasetParameters {
            file_path: gdal_path.into(),
            rasterband_channel: 0, // we calculate offsets below
            geo_transform,
            file_not_found_handling: FileNotFoundHandling::Error,
            no_data_value: data_array.no_data_value(), // we could also leave this empty. The gdal source will try to get the correct one.
            properties_mapping: None,
            width: dimensions.lon,
            height: dimensions.lat,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
        };

        Ok(match time_coverage {
            TimeCoverage::Regular { start, end, step } => Box::new(GdalMetadataNetCdfCf {
                params,
                result_descriptor,
                start,
                end, // TODO: Use this or time dimension size (number of steps)?
                step,
                band_offset: dataset_id.entity as usize * dimensions.time,
            }),
            TimeCoverage::List { time_stamps } => {
                let mut params_list = Vec::with_capacity(time_stamps.len());
                for (i, time_instance) in time_stamps.iter().enumerate() {
                    let mut params = params.clone();

                    params.rasterband_channel =
                        dataset_id.entity as usize * dimensions.time + i + 1;

                    params_list.push(GdalLoadingInfoTemporalSlice {
                        time: TimeInterval::new_instant(*time_instance)
                            .context(error::InvalidTimeCoverageInterval)?,
                        params: Some(params),
                    });
                }

                Box::new(GdalMetaDataList {
                    result_descriptor,
                    params: params_list,
                })
            }
        })
    }

    fn meta_data_from_overviews(
        overview_path: &Path,
        dataset_id: &NetCdfCf4DDatasetId,
    ) -> Option<Metadata> {
        let loading_info_path = overview_path
            .join(&dataset_id.file_name)
            .join(&dataset_id.group_names.join("/"))
            .join("ebv_cube.json");

        let loading_info_file = match std::fs::File::open(&loading_info_path) {
            Ok(file) => file,
            Err(_) => {
                debug!("No overview for {dataset_id:?}");
                return None;
            }
        };

        debug!("Using overview for {dataset_id:?}. Overview path is {overview_path:?}.");

        let loading_info: Metadata =
            serde_json::from_reader(BufReader::new(loading_info_file)).ok()?;

        match loading_info {
            Metadata::NetCDF(mut loading_info) => {
                let time_steps_per_entity = loading_info
                    .step
                    .num_steps_in_interval(
                        TimeInterval::new(loading_info.start, loading_info.end).ok()?,
                    )
                    .ok()?;

                // change start band wrt. entity
                loading_info.band_offset = dataset_id.entity * time_steps_per_entity as usize;

                Some(Metadata::NetCDF(loading_info))
            }
            Metadata::List(mut loading_info) => {
                let time_steps_per_entity = loading_info.params.len();

                // change start band wrt. entity
                for temporal_slice in &mut loading_info.params {
                    if let Some(params) = &mut temporal_slice.params {
                        params.rasterband_channel +=
                            dataset_id.entity * time_steps_per_entity as usize;
                    }
                }

                Some(Metadata::List(loading_info))
            }
        }
    }

    pub fn list_files(&self) -> Result<Vec<PathBuf>> {
        let is_overview_dir = |e: &DirEntry| -> bool { e.path() == self.overviews };

        let mut files = vec![];

        for entry in WalkDir::new(&self.path)
            .into_iter()
            .filter_entry(|e| !is_overview_dir(e))
        {
            let entry = entry.map_err(|e| NetCdfCf4DProviderError::InvalidDirectory {
                source: Box::new(e),
            })?;
            let path = entry.path();

            if !path.is_file() {
                continue;
            }
            if path.extension().map_or(true, |extension| extension != "nc") {
                continue;
            }

            match path.strip_prefix(&self.path) {
                Ok(path) => files.push(path.to_owned()),
                Err(_) => {
                    // we can safely ignore it since it must be a file in the provider path
                    continue;
                }
            };
        }

        Ok(files)
    }

    pub fn create_overviews(&self, dataset_path: &Path) -> Result<OverviewGeneration> {
        create_overviews(&self.path, dataset_path, &self.overviews)
    }

    fn is_netcdf_file(&self, path: &Path) -> bool {
        let real_path = self.path.join(path);
        real_path.is_file() && real_path.extension() == Some("nc".as_ref())
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

/// A simple viridis colorizer between 0 and 255
fn fallback_colorizer() -> Result<Colorizer> {
    Colorizer::linear_gradient(
        vec![
            (
                0.0.try_into().expect("not nan"),
                RgbaColor::new(68, 1, 84, 255),
            )
                .into(),
            (
                36.428_571_428_571_42.try_into().expect("not nan"),
                RgbaColor::new(70, 50, 126, 255),
            )
                .into(),
            (
                72.857_142_857_142_85.try_into().expect("not nan"),
                RgbaColor::new(54, 92, 141, 255),
            )
                .into(),
            (
                109.285_714_285_714_28.try_into().expect("not nan"),
                RgbaColor::new(39, 127, 142, 255),
            )
                .into(),
            (
                109.285_714_285_714_28.try_into().expect("not nan"),
                RgbaColor::new(31, 161, 135, 255),
            )
                .into(),
            (
                182.142_857_142_857_1.try_into().expect("not nan"),
                RgbaColor::new(74, 193, 109, 255),
            )
                .into(),
            (
                218.571_428_571_428_53.try_into().expect("not nan"),
                RgbaColor::new(160, 218, 57, 255),
            )
                .into(),
            (
                255.0.try_into().expect("not nan"),
                RgbaColor::new(253, 231, 37, 255),
            )
                .into(),
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

fn parse_time_step(input: &str) -> Result<Option<TimeStep>> {
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

    // check if the time step string contains only zeros.
    if parts.iter().all(num_traits::Zero::is_zero) {
        return Ok(None);
    }

    if parts.is_empty() {
        return Err(NetCdfCf4DProviderError::TimeCoverageResolutionPartsMustNotBeEmpty);
    }

    Ok(Some(match parts.as_slice() {
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
    }))
}

fn parse_time_coverage(start: &str, end: &str, resolution: &str) -> Result<TimeCoverage> {
    // TODO: parse datetimes

    let start: TimeInstance = parse_date(start)?.into();
    let end: TimeInstance = parse_date(end)?.into();
    let step_option = parse_time_step(resolution)?;

    if let Some(step) = step_option {
        // add one step to provide a right side boundary for the close-open interval
        let end = (end + step).context(error::CannotDefineTimeCoverageEnd)?;
        return Ok(TimeCoverage::Regular { start, end, step });
    }

    // there is no step. Data must be valid for start. TODO: Should this be a TimeInterval?
    Ok(TimeCoverage::List {
        time_stamps: vec![start],
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum TimeCoverage {
    #[serde(rename_all = "camelCase")]
    Regular {
        start: TimeInstance,
        end: TimeInstance,
        step: TimeStep,
    },
    #[serde(rename_all = "camelCase")]
    List { time_stamps: Vec<TimeInstance> },
}

fn time_coverage(root_group: &MdGroup) -> Result<TimeCoverage> {
    let start = root_group
        .attribute_as_string("time_coverage_start")
        .context(error::MissingTimeCoverageStart)?;
    let end = root_group
        .attribute_as_string("time_coverage_end")
        .context(error::MissingTimeCoverageEnd)?;
    let step = root_group
        .attribute_as_string("time_coverage_resolution")
        .context(error::MissingTimeCoverageResolution)?;

    // we can parse coverages starting with `P`,
    let time_p_res = parse_time_coverage(&start, &end, &step);
    if time_p_res.is_ok() {
        debug!(
            "Using time parsed from: start: {start}, end:{end}, step: {step} -> {:?} ",
            time_p_res.as_ref().expect("was just checked with ok")
        );
        return time_p_res;
    }

    // something went wrong parsing a regular time as defined in the NetCDF CF standard.
    debug!("Could not parse time from: start: {start}, end:{end}, step: {step}");

    // try to read time from dimension:
    time_coverage_from_dimension(root_group)
}

fn time_coverage_from_dimension(root_group: &MdGroup) -> Result<TimeCoverage> {
    // TODO: are there other variants for the time unit?
    // `:units = "days since 1860-01-01 00:00:00.0";`

    let days_since_1860 = root_group
        .dimension_as_double_array("time")
        .context(error::MissingTimeDimension)?;

    let unix_offset_millis = TimeInstance::from(DateTime::new_utc(1860, 1, 1, 0, 0, 0)).inner();

    let mut time_stamps = Vec::with_capacity(days_since_1860.len());
    for days in days_since_1860 {
        let days = days;
        let hours = days * 24.;
        let seconds = hours * 60. * 60.;
        let milliseconds = seconds * 1_000.;

        time_stamps.push(
            TimeInstance::from_millis(milliseconds as i64 + unix_offset_millis)
                .context(error::InvalidTimeCoverageInstant)?,
        );
    }

    Ok(TimeCoverage::List { time_stamps })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum Metadata {
    NetCDF(GdalMetadataNetCdfCf),
    List(GdalMetaDataList),
}

#[async_trait]
impl DataProvider for NetCdfCfDataProvider {
    async fn provenance(&self, id: &DataId) -> crate::error::Result<ProvenanceOutput> {
        Ok(ProvenanceOutput {
            data: id.clone(),
            provenance: None,
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
enum NetCdfLayerCollectionId {
    Path {
        path: PathBuf,
    },
    Group {
        path: PathBuf,
        groups: Vec<String>,
    },
    Entity {
        path: PathBuf,
        groups: Vec<String>,
        entity: usize,
    },
}

impl TryFrom<NetCdfLayerCollectionId> for LayerCollectionId {
    type Error = crate::error::Error;

    fn try_from(id: NetCdfLayerCollectionId) -> crate::error::Result<Self> {
        Ok(LayerCollectionId(serde_json::to_string(&id)?))
    }
}

impl TryFrom<NetCdfLayerCollectionId> for LayerId {
    type Error = crate::error::Error;

    fn try_from(id: NetCdfLayerCollectionId) -> crate::error::Result<Self> {
        Ok(LayerId(serde_json::to_string(&id)?))
    }
}

async fn listing_from_dir(base: &Path, path: &Path) -> crate::error::Result<Vec<CollectionItem>> {
    let mut dir = tokio::fs::read_dir(base.join(&path)).await?;

    let mut items = vec![];
    while let Some(entry) = dir.next_entry().await? {
        if entry.path().is_dir() || entry.path().extension() == Some("nc".as_ref()) {
            items.push(CollectionItem::Collection(LayerCollectionListing {
                id: ProviderLayerCollectionId {
                    provider_id: NETCDF_CF_PROVIDER_ID,
                    collection_id: NetCdfLayerCollectionId::Path {
                        path: entry
                            .path()
                            .strip_prefix(base)
                            .map_err(|_| Error::InvalidLayerCollectionId)?
                            .to_owned(),
                    }
                    .try_into()?,
                },
                name: entry.file_name().to_string_lossy().to_string(),
                description: "".to_string(),
            }));
        }
    }

    Ok(items)
}

/// List all the groups in the group given by the path `groups`.
pub fn list_groups(
    netcdf_groups: Vec<NetCdfGroup>,
    groups: &[String],
) -> crate::error::Result<Vec<NetCdfGroup>> {
    if groups.is_empty() {
        return Ok(netcdf_groups);
    }

    let mut group_stack = groups.iter().collect::<Vec<_>>();
    let mut group = netcdf_groups
        .into_iter()
        .find(|g| g.name == *group_stack.remove(0))
        .ok_or(Error::InvalidLayerCollectionId)?;

    while !group_stack.is_empty() {
        group = group
            .groups
            .into_iter()
            .find(|g| g.name == *group_stack.remove(0))
            .ok_or(Error::InvalidLayerCollectionId)?;
    }

    Ok(group.groups)
}

async fn listing_from_netcdf_file(
    file_path: PathBuf,
    groups: &[String],
    provider_path: PathBuf,
    overview_path: PathBuf,
    options: &LayerCollectionListOptions,
) -> crate::error::Result<Vec<CollectionItem>> {
    let fp = file_path.clone();
    let tree = tokio::task::spawn_blocking(move || {
        NetCdfCfDataProvider::build_netcdf_tree(&provider_path, Some(&overview_path), &fp, false)
            .map_err(|_| Error::InvalidLayerCollectionId)
    })
    .await??;

    let groups_list = list_groups(tree.groups, groups)?;

    if groups_list.is_empty() {
        tree.entities
            .into_iter()
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .map(|entity| {
                Ok(CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        layer_id: NetCdfLayerCollectionId::Entity {
                            path: file_path.clone(),
                            groups: groups.to_owned(),
                            entity: entity.id,
                        }
                        .try_into()?,
                    },
                    name: entity.name,
                    description: "".to_string(),
                }))
            })
            .collect()
    } else {
        let out_groups = groups.to_owned();

        groups_list
            .into_iter()
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .map(|group| {
                let mut out_groups = out_groups.clone();
                out_groups.push(group.name.clone());
                Ok(CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: NetCdfLayerCollectionId::Group {
                            path: file_path.clone(),
                            groups: out_groups,
                        }
                        .try_into()?,
                    },
                    name: group.title.clone(),
                    description: group.description,
                }))
            })
            .collect()
    }
}

#[async_trait]
impl LayerCollectionProvider for NetCdfCfDataProvider {
    async fn collection_items(
        &self,
        collection: &LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> crate::error::Result<Vec<CollectionItem>> {
        let id: NetCdfLayerCollectionId = serde_json::from_str(&collection.0)?;

        match id {
            NetCdfLayerCollectionId::Path { path } if self.path.join(&path).is_dir() => {
                listing_from_dir(&self.path, &path).await
            }
            NetCdfLayerCollectionId::Path { path } if self.is_netcdf_file(&path) => {
                listing_from_netcdf_file(
                    path,
                    &[],
                    self.path.clone(),
                    self.overviews.clone(),
                    &options.user_input,
                )
                .await
            }
            NetCdfLayerCollectionId::Group { path, groups } if self.is_netcdf_file(&path) => {
                listing_from_netcdf_file(
                    path,
                    &groups,
                    self.path.clone(),
                    self.overviews.clone(),
                    &options.user_input,
                )
                .await
            }
            _ => return Err(Error::InvalidLayerCollectionId),
        }
    }

    async fn root_collection_id(&self) -> crate::error::Result<LayerCollectionId> {
        Ok(NetCdfLayerCollectionId::Path { path: ".".into() }.try_into()?)
    }

    async fn get_layer(&self, id: &LayerId) -> crate::error::Result<Layer> {
        let netcdf_id = serde_json::from_str(&id.0)?;

        match netcdf_id {
            NetCdfLayerCollectionId::Entity {
                path,
                groups,
                entity,
            } => {
                let rp = path.clone();

                let provider_path = self.path.clone();
                let overviews_path = self.overviews.clone();

                let tree = tokio::task::spawn_blocking(move || {
                    NetCdfCfDataProvider::build_netcdf_tree(
                        &provider_path,
                        Some(&overviews_path),
                        &rp,
                        false,
                    )
                    .map_err(|_| Error::InvalidLayerCollectionId)
                })
                .await??;

                let netcdf_entity = tree
                    .entities
                    .into_iter()
                    .find(|e| e.id == entity)
                    .ok_or(Error::UnknownLayerId { id: id.clone() })?;

                Ok(Layer {
                    id: ProviderLayerId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        layer_id: id.clone(),
                    },
                    name: format!("{} > {}", tree.title, netcdf_entity.name),
                    description: format!("{} > {}", tree.title, netcdf_entity.name),
                    workflow: Workflow {
                        operator: TypedOperator::Raster(
                            GdalSource {
                                params:
                                    GdalSourceParameters {
                                        data:
                                            DataId::External(
                                                ExternalDataId {
                                                    provider_id: NETCDF_CF_PROVIDER_ID,
                                                    layer_id:
                                                        LayerId(
                                                            json!({
                                                                "fileName": tree.file_name,
                                                                "groupNames": groups,
                                                                "entity": entity
                                                            })
                                                            .to_string(),
                                                        ),
                                                },
                                            ),
                                    },
                            }
                            .boxed(),
                        ),
                    },
                    symbology: None,
                })
            }
            _ => return Err(Error::InvalidLayerId),
        }
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for NetCdfCfDataProvider
{
    async fn meta_data(
        &self,
        id: &DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        let dataset = id.clone();
        let path = self.path.clone();
        let overviews = self.overviews.clone();
        crate::util::spawn_blocking(move || {
            Self::meta_data(&path, &overviews, &dataset).map_err(|error| {
                geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(error),
                }
            })
        })
        .await?
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for NetCdfCfDataProvider
{
    async fn meta_data(
        &self,
        _id: &DataId,
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
        _id: &DataId,
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
        dataset::LayerId,
        primitives::{SpatialPartition2D, SpatialResolution, TimeInterval},
        spatial_reference::SpatialReferenceAuthority,
        test_data,
        util::gdal::hide_gdal_errors,
    };
    use geoengine_operators::source::{
        FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters,
        GdalLoadingInfoTemporalSlice,
    };

    use super::*;

    #[test]
    fn test_parse_time_coverage() {
        let result = parse_time_coverage("2010", "2020", "P0001-00-00").unwrap();
        let expected = TimeCoverage::Regular {
            start: TimeInstance::from(DateTime::new_utc(2010, 1, 1, 0, 0, 0)),
            end: TimeInstance::from(DateTime::new_utc(2021, 1, 1, 0, 0, 0)),
            step: TimeStep {
                granularity: TimeGranularity::Years,
                step: 1,
            },
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_zero_time_coverage() {
        let result = parse_time_coverage("2010", "2020", "P0000-00-00").unwrap();
        assert_eq!(
            result,
            TimeCoverage::List {
                time_stamps: vec![DateTime::new_utc(2010, 1, 1, 0, 0, 0).into()]
            }
        );
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
            Some(TimeStep {
                granularity: TimeGranularity::Years,
                step: 1,
            })
        );
        assert_eq!(
            parse_time_step("P0005-00-00").unwrap(),
            Some(TimeStep {
                granularity: TimeGranularity::Years,
                step: 5,
            })
        );
        assert_eq!(
            parse_time_step("P0010-00-00").unwrap(),
            Some(TimeStep {
                granularity: TimeGranularity::Years,
                step: 10,
            })
        );
        assert_eq!(
            parse_time_step("P0000-06-00").unwrap(),
            Some(TimeStep {
                granularity: TimeGranularity::Months,
                step: 6,
            })
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_listing_from_netcdf_m() {
        let provider_id = DataProviderId::from_str("bf6bb6ea-5d5d-467d-bad1-267bf3a54470").unwrap();

        let listing = NetCdfCfDataProvider::listing_from_netcdf(
            provider_id,
            test_data!("netcdf4d"),
            None,
            Path::new("dataset_m.nc"),
            false,
        )
        .unwrap();

        assert_eq!(listing.len(), 6);

        assert_eq!(
            listing[0],
            LayerListing {
                id: ProviderLayerId {
                    provider_id,
                    layer_id: LayerId(
                        serde_json::json!({
                            "fileName": "dataset_m.nc",
                            "groupNames": ["metric_1"],
                            "entity": 0
                        })
                        .to_string()
                    ),
                },
                name: "Test dataset metric: Random metric 1 > entity01".into(),
                description: "CFake description of test dataset with metric.".into(),
            }
        );
        assert_eq!(
            listing[1],
            LayerListing {
                id: ProviderLayerId {
                    provider_id,
                    layer_id: LayerId(
                        serde_json::json!({
                            "fileName": "dataset_m.nc",
                            "groupNames": ["metric_1"],
                            "entity": 1
                        })
                        .to_string()
                    ),
                },
                name: "Test dataset metric: Random metric 1 > entity02".into(),
                description: "CFake description of test dataset with metric.".into(),
            }
        );
        assert_eq!(
            listing[2],
            LayerListing {
                id: ProviderLayerId {
                    provider_id,
                    layer_id: LayerId(
                        serde_json::json!({
                            "fileName": "dataset_m.nc",
                            "groupNames": ["metric_1"],
                            "entity": 2
                        })
                        .to_string()
                    ),
                },
                name: "Test dataset metric: Random metric 1 > entity03".into(),
                description: "CFake description of test dataset with metric.".into(),
            }
        );
        assert_eq!(
            listing[3],
            LayerListing {
                id: ProviderLayerId {
                    provider_id,
                    layer_id: LayerId(
                        serde_json::json!({
                            "fileName": "dataset_m.nc",
                            "groupNames": ["metric_2"],
                            "entity": 0
                        })
                        .to_string()
                    ),
                },
                name: "Test dataset metric: Random metric 2 > entity01".into(),
                description: "CFake description of test dataset with metric.".into(),
            }
        );
        assert_eq!(
            listing[4],
            LayerListing {
                id: ProviderLayerId {
                    provider_id,
                    layer_id: LayerId(
                        serde_json::json!({
                            "fileName": "dataset_m.nc",
                            "groupNames": ["metric_2"],
                            "entity": 1
                        })
                        .to_string()
                    ),
                },
                name: "Test dataset metric: Random metric 2 > entity02".into(),
                description: "CFake description of test dataset with metric.".into(),
            }
        );
        assert_eq!(
            listing[5],
            LayerListing {
                id: ProviderLayerId {
                    provider_id,
                    layer_id: LayerId(
                        serde_json::json!({
                            "fileName": "dataset_m.nc",
                            "groupNames": ["metric_2"],
                            "entity": 2
                        })
                        .to_string()
                    ),
                },
                name: "Test dataset metric: Random metric 2 > entity03".into(),
                description: "CFake description of test dataset with metric.".into(),
            }
        );
    }

    #[tokio::test]
    async fn test_listing_from_netcdf_sm() {
        let provider_id = DataProviderId::from_str("bf6bb6ea-5d5d-467d-bad1-267bf3a54470").unwrap();

        let listing = NetCdfCfDataProvider::listing_from_netcdf(
            provider_id,
            test_data!("netcdf4d"),
            None,
            Path::new("dataset_sm.nc"),
            false,
        )
        .unwrap();

        assert_eq!(listing.len(), 20);

        assert_eq!(
            listing[0],
            LayerListing {
                id: ProviderLayerId {
                    provider_id,
                    layer_id: LayerId(
                        serde_json::json!({
                            "fileName": "dataset_sm.nc",
                            "groupNames": ["scenario_1", "metric_1"],
                            "entity": 0
                        })
                        .to_string()
                    ),
                },
                name:
                    "Test dataset metric and scenario: Sustainability > Random metric 1 > entity01"
                        .into(),
                description: "Fake description of test dataset with metric and scenario.".into(),
            }
        );
        assert_eq!(
            listing[19],
            LayerListing {
                id: ProviderLayerId {
                    provider_id,
                    layer_id: LayerId(serde_json::json!({
                        "fileName": "dataset_sm.nc",
                        "groupNames": ["scenario_5", "metric_2"],
                        "entity": 1
                    })
                    .to_string()),
                },
                name: "Test dataset metric and scenario: Fossil-fueled Development > Random metric 2 > entity02".into(),
                description: "Fake description of test dataset with metric and scenario.".into(),
            }
        );
    }

    #[tokio::test]
    async fn test_metadata_from_netcdf_sm() {
        let provider = NetCdfCfDataProvider {
            path: test_data!("netcdf4d/").to_path_buf(),
            overviews: test_data!("netcdf4d/overviews").to_path_buf(),
        };

        let metadata = provider
            .meta_data(&DataId::External(ExternalDataId {
                provider_id: NETCDF_CF_PROVIDER_ID,
                layer_id: LayerId(
                    serde_json::json!({
                        "fileName": "dataset_sm.nc",
                        "groupNames": ["scenario_5", "metric_2"],
                        "entity": 1
                    })
                    .to_string(),
                ),
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
                time: None,
                bbox: None,
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
                    gdal_config_options: None,
                    allow_alphaband_as_mask: true,
                })
            }
        );
    }

    #[test]
    fn list_files() {
        let provider = NetCdfCfDataProvider {
            path: test_data!("netcdf4d/").to_path_buf(),
            overviews: test_data!("netcdf4d/overviews").to_path_buf(),
        };

        let expected_files: Vec<PathBuf> = vec!["dataset_m.nc".into(), "dataset_sm.nc".into()];
        let mut files = provider.list_files().unwrap();
        files.sort();

        assert_eq!(files, expected_files);
    }

    #[tokio::test]
    async fn test_loading_info_from_index() {
        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        let provider = NetCdfCfDataProvider {
            path: test_data!("netcdf4d/").to_path_buf(),
            overviews: overview_folder.path().to_path_buf(),
        };

        provider
            .create_overviews(Path::new("dataset_sm.nc"))
            .unwrap();

        let metadata = provider
            .meta_data(&DataId::External(ExternalDataId {
                provider_id: NETCDF_CF_PROVIDER_ID,
                layer_id: LayerId(
                    serde_json::json!({
                        "fileName": "dataset_sm.nc",
                        "groupNames": ["scenario_5", "metric_2"],
                        "entity": 1
                    })
                    .to_string(),
                ),
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
                time: None,
                bbox: None,
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

        let file_path = overview_folder
            .path()
            .join("dataset_sm.nc/scenario_5/metric_2/ebv_cube.tiff");

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
                    gdal_config_options: None,
                    allow_alphaband_as_mask: true,
                })
            }
        );
    }

    #[tokio::test]
    async fn test_listing_from_netcdf_sm_from_index() {
        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        let provider = NetCdfCfDataProvider {
            path: test_data!("netcdf4d/").to_path_buf(),
            overviews: overview_folder.path().to_path_buf(),
        };

        provider
            .create_overviews(Path::new("dataset_sm.nc"))
            .unwrap();

        let provider_id = DataProviderId::from_str("bf6bb6ea-5d5d-467d-bad1-267bf3a54470").unwrap();

        let listing = NetCdfCfDataProvider::listing_from_netcdf(
            provider_id,
            test_data!("netcdf4d"),
            Some(overview_folder.path()),
            Path::new("dataset_sm.nc"),
            false,
        )
        .unwrap();

        assert_eq!(listing.len(), 20);

        assert_eq!(
            listing[0],
            LayerListing {
                id: ProviderLayerId {
                    provider_id,
                    layer_id: LayerId(
                        serde_json::json!({
                            "fileName": "dataset_sm.nc",
                            "groupNames": ["scenario_1", "metric_1"],
                            "entity": 0
                        })
                        .to_string()
                    ),
                },
                name:
                    "Test dataset metric and scenario: Sustainability > Random metric 1 > entity01"
                        .into(),
                description: "Fake description of test dataset with metric and scenario.".into(),
            }
        );
        assert_eq!(
            listing[19],
            LayerListing {
                id: ProviderLayerId {
                    provider_id,
                    layer_id: LayerId(serde_json::json!({
                        "fileName": "dataset_sm.nc",
                        "groupNames": ["scenario_5", "metric_2"],
                        "entity": 1
                    })
                    .to_string()),
                },
                name: "Test dataset metric and scenario: Fossil-fueled Development > Random metric 2 > entity02".into(),
                description: "Fake description of test dataset with metric and scenario.".into(),
            }
        );
    }
}
