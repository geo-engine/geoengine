pub use self::ebvportal_provider::{EbvPortalDataProvider, EBV_PROVIDER_ID};
pub use self::error::NetCdfCf4DProviderError;
use self::overviews::remove_overviews;
use self::overviews::InProgressFlag;
pub use self::overviews::OverviewGeneration;
use self::overviews::{create_overviews, METADATA_FILE_NAME};
use crate::api::model::datatypes::{
    DataId, DataProviderId, ExternalDataId, LayerId, ResamplingMethod,
};
use crate::datasets::external::netcdfcf::overviews::LOADING_INFO_FILE_NAME;
use crate::datasets::listing::ProvenanceOutput;
use crate::datasets::storage::MetaDataDefinition;
use crate::error::Error;
use crate::layers::external::DataProvider;
use crate::layers::external::DataProviderDefinition;
use crate::layers::layer::Layer;
use crate::layers::layer::LayerCollectionListOptions;
use crate::layers::layer::LayerCollectionListing;
use crate::layers::layer::LayerListing;
use crate::layers::layer::ProviderLayerCollectionId;
use crate::layers::layer::ProviderLayerId;
use crate::layers::layer::{CollectionItem, LayerCollection};
use crate::layers::listing::LayerCollectionId;
use crate::layers::listing::LayerCollectionProvider;
use crate::projects::RasterSymbology;
use crate::projects::Symbology;
use crate::tasks::TaskContext;
use crate::workflows::workflow::Workflow;
use async_trait::async_trait;
use gdal::raster::{Dimension, GdalDataType, Group};
use gdal::{DatasetOptions, GdalOpenFlags};
use geoengine_datatypes::error::BoxedResultExt;
use geoengine_datatypes::operations::image::{Colorizer, DefaultColors, RgbaColor};
use geoengine_datatypes::primitives::{
    DateTime, DateTimeParseFormat, Measurement, RasterQueryRectangle, TimeGranularity,
    TimeInstance, TimeInterval, TimeStep, TimeStepIter, VectorQueryRectangle,
};
use geoengine_datatypes::raster::{GdalGeoTransform, RasterDataType};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_datatypes::util::canonicalize_subpath;
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
use std::collections::HashMap;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use walkdir::{DirEntry, WalkDir};

mod ebvportal_api;
mod ebvportal_provider;
pub mod error;
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
    pub name: String,
    pub path: PathBuf,
    pub overviews: PathBuf,
}

#[typetag::serde]
#[async_trait]
impl DataProviderDefinition for NetCdfCfDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> crate::error::Result<Box<dyn DataProvider>> {
        Ok(Box::new(NetCdfCfDataProvider {
            name: self.name,
            path: self.path,
            overviews: self.overviews,
        }))
    }

    fn type_name(&self) -> &'static str {
        "NetCdfCfProviderDefinition"
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
    pub creator_name: Option<String>,
    pub creator_email: Option<String>,
    pub creator_institution: Option<String>,
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
    /// Creates group information for a given `group_path`.
    /// If you pass `stats_for_group`, it will be used to extract the group statistics (min/max).
    /// If the `HashMap` is empty, the group statistics will be set to `None`.
    fn to_net_cdf_subgroup(
        &self,
        group_path: &Path,
        stats_for_group: &HashMap<String, (f64, f64)>,
    ) -> Result<NetCdfGroup>;
}

impl<'a> ToNetCdfSubgroup for Group<'a> {
    fn to_net_cdf_subgroup(
        &self,
        group_path: &Path,
        stats_for_group: &HashMap<String, (f64, f64)>,
    ) -> Result<NetCdfGroup> {
        let name = self.name();
        debug!(
            "to_net_cdf_subgroup for {name} with stats={stats_available}",
            stats_available = !stats_for_group.is_empty()
        );

        let title = self
            .attribute("standard_name")
            .map(|a| a.read_as_string())
            .unwrap_or_default();
        let description = self
            .attribute("long_name")
            .map(|a| a.read_as_string())
            .unwrap_or_default();
        let unit = self
            .attribute("units")
            .map(|a| a.read_as_string())
            .unwrap_or_default();

        let group_names = self.group_names(Default::default());

        if group_names.is_empty() {
            let data_type = Some(
                RasterDataType::from_gdal_data_type(
                    self.open_md_array("ebv_cube", Default::default())
                        .context(error::GdalMd)?
                        .datatype()
                        .numeric_datatype()
                        .try_into()
                        .unwrap_or(GdalDataType::Float64),
                )
                .unwrap_or(RasterDataType::F64),
            );

            let data_range = stats_for_group
                .get(&group_path.to_string_lossy().to_string())
                .copied();

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
            let group_path = group_path.join(&subgroup);
            groups.push(
                self.open_group(&subgroup, Default::default())
                    .context(error::GdalMd)?
                    .to_net_cdf_subgroup(&group_path, stats_for_group)?,
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
    pub entity: usize,
    pub file_name: String,
    pub group_names: Vec<String>,
}

impl NetCdfCfDataProvider {
    fn netcdf_tree_from_overviews(
        overview_path: &Path,
        dataset_path: &Path,
    ) -> Option<NetCdfOverview> {
        let overview_dataset_path = overview_path.join(dataset_path);

        if InProgressFlag::is_in_progress(&overview_dataset_path) {
            return None;
        }

        let tree_file_path = overview_dataset_path.join(METADATA_FILE_NAME);
        let file = std::fs::File::open(tree_file_path).ok()?;
        let buf_reader = BufReader::new(file);
        serde_json::from_reader::<_, NetCdfOverview>(buf_reader).ok()
    }

    pub fn build_netcdf_tree(
        provider_path: &Path,
        overview_path: Option<&Path>,
        dataset_path: &Path,
        stats_for_group: &HashMap<String, (f64, f64)>,
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

        let root_group = ds.root_group().context(error::GdalMd)?;

        let title = root_group
            .attribute("title")
            .context(error::MissingTitle)?
            .read_as_string();

        let summary = root_group
            .attribute("summary")
            .context(error::MissingSummary)?
            .read_as_string();

        let spatial_reference = root_group
            .attribute("geospatial_bounds_crs")
            .context(error::MissingCrs)?
            .read_as_string();
        let spatial_reference: SpatialReference =
            SpatialReference::from_str(&spatial_reference).context(error::CannotParseCrs)?;

        let entities = root_group
            .open_md_array("entity", Default::default())
            .context(error::MissingEntities)?
            .read_as_string_array()
            .context(error::GdalMd)?
            .into_iter()
            .enumerate()
            .map(|(id, name)| NetCdfEntity { id, name })
            .collect::<Vec<_>>();

        let groups = root_group
            .group_names(Default::default())
            .iter()
            .map(|name| {
                root_group
                    .open_group(name, Default::default())
                    .context(error::GdalMd)?
                    .to_net_cdf_subgroup(Path::new(name), stats_for_group)
            })
            .collect::<Result<Vec<_>>>()?;

        let time_coverage = TimeCoverage::from_root_group(&root_group)?;

        let colorizer = load_colorizer(&path).or_else(|error| {
            debug!("Use fallback colorizer: {:?}", error);
            fallback_colorizer()
        })?;

        let creator_name = root_group
            .attribute("creator_name")
            .map(|a| a.read_as_string())
            .ok();

        let creator_email = root_group
            .attribute("creator_email")
            .map(|a| a.read_as_string())
            .ok();

        let creator_institution = root_group
            .attribute("creator_institution")
            .map(|a| a.read_as_string())
            .ok();

        Ok(NetCdfOverview {
            file_name: path
                .strip_prefix(provider_path)
                .boxed_context(error::DatasetIsNotInProviderPath)?
                .to_string_lossy()
                .to_string(),
            title,
            summary,
            spatial_reference,
            groups,
            entities,
            time_coverage,
            colorizer,
            creator_name,
            creator_email,
            creator_institution,
        })
    }

    #[allow(clippy::too_many_lines)] // TODO: refactor method
    fn meta_data(
        path: &Path,
        overviews: &Path,
        id: &DataId,
    ) -> Result<Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>>
    {
        const LON_DIMENSION_INDEX: usize = 3;
        const LAT_DIMENSION_INDEX: usize = 2;
        const TIME_DIMENSION_INDEX: usize = 1;

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
                MetaDataDefinition::GdalMetadataNetCdfCf(loading_info) => {
                    Ok(Box::new(loading_info))
                }
                MetaDataDefinition::GdalMetaDataList(loading_info) => Ok(Box::new(loading_info)),
                MetaDataDefinition::GdalMetaDataRegular(loading_info) => Ok(Box::new(loading_info)),
                _ => Err(NetCdfCf4DProviderError::UnsupportedMetaDataDefinition),
            };
        }

        let dataset_id: NetCdfCf4DDatasetId =
            serde_json::from_str(&dataset.layer_id.0).context(error::CannotParseDatasetId)?;

        let path = canonicalize_subpath(path, Path::new(&dataset_id.file_name)).map_err(|_| {
            NetCdfCf4DProviderError::FileIsNotInProviderPath {
                file: dataset_id.file_name.clone(),
            }
        })?;

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

        let root_group = dataset.root_group().context(error::GdalMd)?;

        let time_coverage = TimeCoverage::from_root_group(&root_group)?;

        let geo_transform = {
            let crs_array = root_group
                .open_md_array("crs", Default::default())
                .context(error::GdalMd)?;
            let geo_transform = crs_array
                .attribute("GeoTransform")
                .context(error::CannotGetGeoTransform)?
                .read_as_string();
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
                    .open_group(group_name, Default::default())
                    .context(error::GdalMd)?,
            );
        }

        let data_array = group_stack
            .last()
            .expect("at least root group in here")
            .open_md_array("ebv_cube", Default::default())
            .context(error::GdalMd)?;

        let dimensions = data_array.dimensions().context(error::GdalMd)?;

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::from_gdal_data_type(
                data_array
                    .datatype()
                    .numeric_datatype()
                    .try_into()
                    .unwrap_or(GdalDataType::Float64),
            )
            .unwrap_or(RasterDataType::F64),
            spatial_reference: SpatialReference::try_from(
                data_array.spatial_reference().context(error::GdalMd)?,
            )
            .context(error::CannotParseCrs)?
            .into(),
            measurement: derive_measurement(data_array.unit()),
            time: None,
            bbox: None,
            resolution: None,
        };

        let params = GdalDatasetParameters {
            file_path: gdal_path.into(),
            rasterband_channel: 0, // we calculate offsets below
            geo_transform,
            file_not_found_handling: FileNotFoundHandling::Error,
            no_data_value: data_array.no_data_value_as_double(), // we could also leave this empty. The gdal source will try to get the correct one.
            properties_mapping: None,
            width: dimensions
                .get(LON_DIMENSION_INDEX)
                .map(Dimension::size)
                .unwrap_or_default(),
            height: dimensions
                .get(LAT_DIMENSION_INDEX)
                .map(Dimension::size)
                .unwrap_or_default(),
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        };

        let dimensions_time = dimensions
            .get(TIME_DIMENSION_INDEX)
            .map(Dimension::size)
            .unwrap_or_default();
        Ok(match time_coverage {
            TimeCoverage::Regular { start, end, step } => Box::new(GdalMetadataNetCdfCf {
                params,
                result_descriptor,
                start,
                end, // TODO: Use this or time dimension size (number of steps)?
                step,
                band_offset: dataset_id.entity * dimensions_time,
            }),
            TimeCoverage::List { time_stamps } => {
                let mut params_list = Vec::with_capacity(time_stamps.len());
                for (i, time_instance) in time_stamps.iter().enumerate() {
                    let mut params = params.clone();

                    params.rasterband_channel = dataset_id.entity * dimensions_time + i + 1;

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
    ) -> Option<MetaDataDefinition> {
        let loading_info_path = overview_path
            .join(&dataset_id.file_name)
            .join(dataset_id.group_names.join("/"))
            .join(dataset_id.entity.to_string())
            .join(LOADING_INFO_FILE_NAME);

        let Ok(loading_info_file) = std::fs::File::open(loading_info_path) else {
            debug!("No overview for {dataset_id:?}");
            return None;
        };

        debug!("Using overview for {dataset_id:?}. Overview path is {overview_path:?}.");

        let loading_info: MetaDataDefinition =
            serde_json::from_reader(BufReader::new(loading_info_file)).ok()?;

        match loading_info {
            MetaDataDefinition::GdalMetaDataList(loading_info) => {
                Some(MetaDataDefinition::GdalMetaDataList(loading_info))
            }
            MetaDataDefinition::GdalMetaDataRegular(loading_info) => {
                Some(MetaDataDefinition::GdalMetaDataRegular(loading_info))
            }
            _ => None, // we only support some definitions here
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

    pub fn create_overviews<C: TaskContext>(
        &self,
        dataset_path: &Path,
        resampling_method: Option<ResamplingMethod>,
        task_context: &C,
    ) -> Result<OverviewGeneration> {
        create_overviews(
            &self.path,
            dataset_path,
            &self.overviews,
            resampling_method,
            task_context,
        )
    }

    pub fn remove_overviews(&self, dataset_path: &Path, force: bool) -> Result<()> {
        remove_overviews(dataset_path, &self.overviews, force)
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
        DefaultColors::OverUnder {
            over_color: RgbaColor::white(),
            under_color: RgbaColor::black(),
        },
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
    let Some(duration_str) = input.strip_prefix('P') else {
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

impl TimeCoverage {
    fn from_root_group(root_group: &Group) -> Result<TimeCoverage> {
        let start = root_group
            .attribute("time_coverage_start")
            .context(error::MissingTimeCoverageStart)?
            .read_as_string();
        let end = root_group
            .attribute("time_coverage_end")
            .context(error::MissingTimeCoverageEnd)?
            .read_as_string();
        let step = root_group
            .attribute("time_coverage_resolution")
            .context(error::MissingTimeCoverageResolution)?
            .read_as_string();

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
        Self::from_dimension(root_group)
    }

    fn from_dimension(root_group: &Group) -> Result<TimeCoverage> {
        // TODO: are there other variants for the time unit?
        // `:units = "days since 1860-01-01 00:00:00.0";`

        let days_since_1860 = {
            let time_array = root_group
                .open_md_array("time", Default::default())
                .context(error::MissingTimeDimension)?;

            let number_of_values = time_array.num_elements() as usize;

            time_array
                .read_as::<f64>(vec![0], vec![number_of_values])
                .context(error::MissingTimeDimension)?
        };

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

    fn number_of_time_steps(&self) -> Result<u32> {
        match self {
            TimeCoverage::Regular { start, end, step } => {
                let time_interval = TimeInterval::new(*start, *end);
                let time_steps = time_interval
                    .and_then(|time_interval| step.num_steps_in_interval(time_interval));
                time_steps.context(error::InvalidTimeCoverageInterval)
            }
            TimeCoverage::List { time_stamps } => Ok(time_stamps.len() as u32),
        }
    }

    fn time_steps(&self) -> Result<Vec<TimeInstance>> {
        match self {
            TimeCoverage::Regular {
                start,
                end: _,
                step,
            } => {
                let time_step_iter = TimeStepIter::new(*start, *step, self.number_of_time_steps()?)
                    .context(error::InvalidTimeCoverageInterval)?;
                Ok(time_step_iter.collect())
            }
            TimeCoverage::List { time_stamps } => Ok(time_stamps.clone()),
        }
    }
}

#[async_trait]
impl DataProvider for NetCdfCfDataProvider {
    async fn provenance(&self, id: &DataId) -> crate::error::Result<ProvenanceOutput> {
        Ok(ProvenanceOutput {
            data: id.clone(),
            provenance: None,
        })
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "camelCase")]
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

impl FromStr for NetCdfLayerCollectionId {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = if s.starts_with("root") {
            s.replace("root", ".")
        } else {
            s.to_string()
        };

        let split_pos = s.find(".nc").map(|i| i + ".nc".len());

        if let Some(split_pos) = split_pos {
            let (path, rest) = s.split_at(split_pos);

            if rest.is_empty() {
                return Ok(NetCdfLayerCollectionId::Path {
                    path: PathBuf::from(s),
                });
            }

            let r = rest[1..].split('/').collect::<Vec<_>>();

            Ok(match *r.as_slice() {
                [.., entity] if entity.ends_with(".entity") => NetCdfLayerCollectionId::Entity {
                    path: PathBuf::from(path),
                    groups: r[..r.len() - 1].iter().map(ToString::to_string).collect(),
                    entity: entity[0..entity.len() - ".entity".len()]
                        .parse()
                        .map_err(|_| crate::error::Error::InvalidLayerCollectionId)?,
                },
                _ => NetCdfLayerCollectionId::Group {
                    path: PathBuf::from(path),
                    groups: r.iter().map(ToString::to_string).collect(),
                },
            })
        } else {
            Ok(NetCdfLayerCollectionId::Path {
                path: PathBuf::from(s),
            })
        }
    }
}

fn path_to_string(path: &Path) -> String {
    path.components()
        .map(|c| match c.as_os_str().to_string_lossy().as_ref() {
            "." => "root".to_string(),
            s => s.to_string(),
        })
        .collect::<Vec<_>>()
        .join("/")
}

impl TryFrom<NetCdfLayerCollectionId> for LayerCollectionId {
    type Error = crate::error::Error;

    fn try_from(id: NetCdfLayerCollectionId) -> crate::error::Result<Self> {
        let s = match id {
            NetCdfLayerCollectionId::Path { path } => path_to_string(&path),
            NetCdfLayerCollectionId::Group { path, groups } => {
                format!("{}/{}", path_to_string(&path), groups.join("/"))
            }
            NetCdfLayerCollectionId::Entity { .. } => {
                return Err(crate::error::Error::InvalidLayerCollectionId)
            }
        };

        Ok(LayerCollectionId(s))
    }
}

impl TryFrom<NetCdfLayerCollectionId> for LayerId {
    type Error = crate::error::Error;

    fn try_from(id: NetCdfLayerCollectionId) -> crate::error::Result<Self> {
        let s = match id {
            NetCdfLayerCollectionId::Entity {
                path,
                groups,
                entity,
            } => format!(
                "{}/{}/{}.entity",
                path_to_string(&path),
                groups.join("/"),
                entity
            ),
            _ => return Err(crate::error::Error::InvalidLayerId),
        };

        Ok(LayerId(s))
    }
}

async fn listing_from_dir(
    provider_name: &str,
    collection: &LayerCollectionId,
    overview_path: &Path,
    base: &Path,
    path: &Path,
    options: &LayerCollectionListOptions,
) -> crate::error::Result<LayerCollection> {
    let dir_path = base.join(path);

    let (name, description) = if path == Path::new(".") {
        (
            provider_name.to_string(),
            "NetCdfCfProviderDefinition".to_string(),
        )
    } else {
        (
            path.file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default(),
            String::new(),
        )
    };

    let mut dir = tokio::fs::read_dir(&dir_path).await?;

    let mut items = vec![];
    while let Some(entry) = dir.next_entry().await? {
        if entry.path().canonicalize()? == overview_path.canonicalize()? {
            continue;
        }

        if entry.path().is_dir() {
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
                description: String::new(),
                properties: Default::default(),
            }));
        } else if entry.path().extension() == Some("nc".as_ref()) {
            let fp = entry
                .path()
                .strip_prefix(base)
                .map_err(|_| crate::error::Error::SubPathMustNotEscapeBasePath {
                    base: base.to_owned(),
                    sub_path: entry.path(),
                })?
                .to_owned();
            let b = base.to_owned();
            let tree = tokio::task::spawn_blocking(move || {
                NetCdfCfDataProvider::build_netcdf_tree(&b, None, &fp, &Default::default())
                    .map_err(|_| Error::InvalidLayerCollectionId)
            })
            .await??;

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
                name: tree.title,
                description: tree.summary,
                properties: Default::default(),
            }));
        }
    }

    items.sort_by(|a, b| a.name().cmp(b.name()));
    let items = items
        .into_iter()
        .skip(options.offset as usize)
        .take(options.limit as usize)
        .collect();

    Ok(LayerCollection {
        id: ProviderLayerCollectionId {
            provider_id: NETCDF_CF_PROVIDER_ID,
            collection_id: collection.clone(),
        },
        name,
        description,
        items,
        entry_label: None,
        properties: vec![],
    })
}

/// find the group given by the path `groups`.
pub fn find_group(
    netcdf_groups: Vec<NetCdfGroup>,
    groups: &[String],
) -> crate::error::Result<Option<NetCdfGroup>> {
    if groups.is_empty() {
        return Ok(None);
    }

    let mut group_stack = groups.iter().collect::<Vec<_>>();
    let target = group_stack.remove(0);
    let mut group = netcdf_groups
        .into_iter()
        .find(|g| g.name == *target)
        .ok_or(Error::InvalidLayerCollectionId)?;

    while !group_stack.is_empty() {
        let target = group_stack.remove(0);
        group = group
            .groups
            .into_iter()
            .find(|g| g.name == *target)
            .ok_or(Error::InvalidLayerCollectionId)?;
    }

    Ok(Some(group))
}

pub fn layer_from_netcdf_overview(
    provider_id: DataProviderId,
    layer_id: &LayerId,
    overview: NetCdfOverview,
    groups: &[String],
    entity: usize,
) -> crate::error::Result<Layer> {
    let netcdf_entity = overview
        .entities
        .into_iter()
        .find(|e| e.id == entity)
        .ok_or(Error::UnknownLayerId {
            id: layer_id.clone(),
        })?;

    let time_steps = match overview.time_coverage {
        TimeCoverage::Regular { start, end, step } => {
            if step.step == 0 {
                vec![start]
            } else {
                TimeStepIter::new_with_interval(TimeInterval::new(start, end)?, step)?.collect()
            }
        }
        TimeCoverage::List { time_stamps } => time_stamps,
    };

    let group = find_group(overview.groups, groups)?.ok_or(Error::InvalidLayerId)?;

    let (data_range, colorizer) = if let Some(data_range) = group.data_range {
        (
            data_range,
            overview.colorizer.rescale(data_range.0, data_range.1)?,
        )
    } else {
        let colorizer = overview.colorizer;
        ((colorizer.min_value(), colorizer.max_value()), colorizer)
    };

    Ok(Layer {
        id: ProviderLayerId {
            provider_id,
            layer_id: layer_id.clone(),
        },
        name: netcdf_entity.name.clone(),
        description: netcdf_entity.name,
        workflow: Workflow {
            operator: TypedOperator::Raster(
                GdalSource {
                    params: GdalSourceParameters {
                        data: DataId::External(ExternalDataId {
                            provider_id,
                            layer_id: LayerId(
                                json!({
                                    "fileName": overview.file_name,
                                    "groupNames": groups,
                                    "entity": entity
                                })
                                .to_string(),
                            ),
                        })
                        .into(),
                    },
                }
                .boxed(),
            ),
        },
        symbology: Some(Symbology::Raster(RasterSymbology {
            opacity: 1.0,
            colorizer: colorizer.into(),
        })),
        properties: [(
            "author".to_string(),
            format!(
                "{}, {}, {}",
                overview
                    .creator_name
                    .unwrap_or_else(|| "unknown".to_string()),
                overview
                    .creator_email
                    .unwrap_or_else(|| "unknown".to_string()),
                overview
                    .creator_institution
                    .unwrap_or_else(|| "unknown".to_string())
            ),
        )
            .into()]
        .into_iter()
        .collect(),
        metadata: [
            ("timeSteps".to_string(), serde_json::to_string(&time_steps)?),
            ("dataRange".to_string(), serde_json::to_string(&data_range)?),
        ]
        .into_iter()
        .collect(),
    })
}

async fn listing_from_netcdf_file(
    collection: &LayerCollectionId,
    relative_file_path: PathBuf,
    groups: &[String],
    provider_path: PathBuf,
    overview_path: PathBuf,
    options: &LayerCollectionListOptions,
) -> crate::error::Result<LayerCollection> {
    let fp = relative_file_path.clone();
    let tree = tokio::task::spawn_blocking(move || {
        NetCdfCfDataProvider::build_netcdf_tree(
            &provider_path,
            Some(&overview_path),
            &fp,
            &Default::default(),
        )
        .map_err(|_| Error::InvalidLayerCollectionId)
    })
    .await??;

    let group = find_group(tree.groups.clone(), groups)?;

    let (name, description) = group.as_ref().map_or_else(
        || (tree.title, tree.summary),
        |g| (g.title.clone(), g.description.clone()),
    );

    let properties = if groups.is_empty() {
        [(
            "author".to_string(),
            format!(
                "{}, {}, {}",
                tree.creator_name.unwrap_or_else(|| "unknown".to_string()),
                tree.creator_email.unwrap_or_else(|| "unknown".to_string()),
                tree.creator_institution
                    .unwrap_or_else(|| "unknown".to_string())
            ),
        )
            .into()]
        .into_iter()
        .collect()
    } else {
        vec![]
    };

    let groups_list = group.map_or(tree.groups, |g| g.groups);

    let items = if groups_list.is_empty() {
        tree.entities
            .into_iter()
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .map(|entity| {
                Ok(CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        layer_id: NetCdfLayerCollectionId::Entity {
                            path: relative_file_path.clone(),
                            groups: groups.to_owned(),
                            entity: entity.id,
                        }
                        .try_into()?,
                    },
                    name: entity.name,
                    description: String::new(),
                    properties: vec![],
                }))
            })
            .collect::<crate::error::Result<Vec<CollectionItem>>>()?
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
                            path: relative_file_path.clone(),
                            groups: out_groups,
                        }
                        .try_into()?,
                    },
                    name: group.title.clone(),
                    description: group.description,
                    properties: Default::default(),
                }))
            })
            .collect::<crate::error::Result<Vec<CollectionItem>>>()?
    };

    Ok(LayerCollection {
        id: ProviderLayerCollectionId {
            provider_id: NETCDF_CF_PROVIDER_ID,
            collection_id: collection.clone(),
        },
        name,
        description,
        items,
        entry_label: None,
        properties,
    })
}

#[async_trait]
impl LayerCollectionProvider for NetCdfCfDataProvider {
    async fn load_layer_collection(
        &self,
        collection: &LayerCollectionId,
        options: LayerCollectionListOptions,
    ) -> crate::error::Result<LayerCollection> {
        let id = NetCdfLayerCollectionId::from_str(&collection.0)?;
        Ok(match id {
            NetCdfLayerCollectionId::Path { path }
                if canonicalize_subpath(&self.path, &path).is_ok()
                    && self.path.join(&path).is_dir() =>
            {
                listing_from_dir(
                    &self.name,
                    collection,
                    &self.overviews,
                    &self.path,
                    &path,
                    &options,
                )
                .await?
            }
            NetCdfLayerCollectionId::Path { path }
                if canonicalize_subpath(&self.path, &path).is_ok()
                    && self.is_netcdf_file(&path) =>
            {
                listing_from_netcdf_file(
                    collection,
                    path,
                    &[],
                    self.path.clone(),
                    self.overviews.clone(),
                    &options,
                )
                .await?
            }
            NetCdfLayerCollectionId::Group { path, groups }
                if canonicalize_subpath(&self.path, &path).is_ok()
                    && self.is_netcdf_file(&path) =>
            {
                listing_from_netcdf_file(
                    collection,
                    path,
                    &groups,
                    self.path.clone(),
                    self.overviews.clone(),
                    &options,
                )
                .await?
            }
            _ => return Err(Error::InvalidLayerCollectionId),
        })
    }

    async fn get_root_layer_collection_id(&self) -> crate::error::Result<LayerCollectionId> {
        Ok(NetCdfLayerCollectionId::Path { path: ".".into() }.try_into()?)
    }

    async fn load_layer(&self, id: &LayerId) -> crate::error::Result<Layer> {
        let netcdf_id = NetCdfLayerCollectionId::from_str(&id.0)?;

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
                        &Default::default(),
                    )
                    .map_err(|_| Error::InvalidLayerCollectionId)
                })
                .await??;

                layer_from_netcdf_overview(NETCDF_CF_PROVIDER_ID, id, tree, &groups, entity)
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
        id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        let dataset = id.clone().into();
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
        _id: &geoengine_datatypes::dataset::DataId,
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
        _id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::contexts::{SessionContext, SimpleApplicationContext};
    use crate::datasets::external::netcdfcf::ebvportal_provider::EbvPortalDataProviderDefinition;
    use crate::layers::storage::LayerProviderDb;

    use crate::{
        contexts::InMemoryContext, tasks::util::NopTaskContext,
        util::tests::add_land_cover_to_datasets,
    };
    use geoengine_datatypes::plots::{PlotData, PlotMetaData};
    use geoengine_datatypes::{
        primitives::{
            BoundingBox2D, PlotQueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval,
        },
        spatial_reference::SpatialReferenceAuthority,
        test_data,
        util::{gdal::hide_gdal_errors, test::TestDefault},
    };
    use geoengine_operators::{
        engine::{MockQueryContext, PlotOperator, TypedPlotQueryProcessor, WorkflowOperatorPath},
        plot::{
            MeanRasterPixelValuesOverTime, MeanRasterPixelValuesOverTimeParams,
            MeanRasterPixelValuesOverTimePosition,
        },
        processing::{Expression, ExpressionParams, ExpressionSources},
        source::{
            FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters,
            GdalLoadingInfoTemporalSlice,
        },
    };

    #[test]
    fn it_parses_netcdf_layer_collection_ids() {
        assert!(matches!(
            NetCdfLayerCollectionId::from_str("root"),
            Ok(NetCdfLayerCollectionId::Path { path }) if path == Path::new(".")
        ));

        assert!(matches!(
            NetCdfLayerCollectionId::from_str("root/foo/bar"),
            Ok(NetCdfLayerCollectionId::Path { path }) if path == Path::new("./foo/bar")
        ));

        assert!(matches!(
            NetCdfLayerCollectionId::from_str("root/foo/bar/baz.nc"),
            Ok(NetCdfLayerCollectionId::Path { path }) if path == Path::new("./foo/bar/baz.nc")
        ));

        assert!(matches!(
            NetCdfLayerCollectionId::from_str("root/foo/bar/baz.nc/group1"),
            Ok(NetCdfLayerCollectionId::Group { path, groups }) if path == Path::new("./foo/bar/baz.nc") && groups == ["group1"]
        ));

        assert!(matches!(
            NetCdfLayerCollectionId::from_str("root/foo/bar/baz.nc/group1/group2"),
            Ok(NetCdfLayerCollectionId::Group { path, groups }) if path == Path::new("./foo/bar/baz.nc") && groups == ["group1", "group2"]
        ));

        assert!(matches!(
            NetCdfLayerCollectionId::from_str("root/foo/bar/baz.nc/group1/group2/7.entity"),
            Ok(NetCdfLayerCollectionId::Entity { path, groups, entity }) if path == Path::new("./foo/bar/baz.nc") && groups == ["group1", "group2"] && entity == 7
        ));

        assert!(matches!(
            NetCdfLayerCollectionId::from_str("root/foo/bar/baz.nc/7.entity"),
            Ok(NetCdfLayerCollectionId::Entity { path, groups, entity }) if path == Path::new("./foo/bar/baz.nc") && groups.is_empty() && entity == 7
        ));
    }

    #[test]
    fn it_serializes_netcdf_layer_collection_ids() {
        let id: LayerCollectionId = NetCdfLayerCollectionId::Path {
            path: PathBuf::from("."),
        }
        .try_into()
        .unwrap();
        assert_eq!(id.to_string(), "root");

        let id: LayerCollectionId = NetCdfLayerCollectionId::Path {
            path: PathBuf::from("./foo/bar"),
        }
        .try_into()
        .unwrap();
        assert_eq!(id.to_string(), "root/foo/bar");

        let id: LayerCollectionId = NetCdfLayerCollectionId::Path {
            path: PathBuf::from("./foo/bar/baz.nc"),
        }
        .try_into()
        .unwrap();
        assert_eq!(id.to_string(), "root/foo/bar/baz.nc");

        let id: LayerCollectionId = NetCdfLayerCollectionId::Group {
            path: PathBuf::from("./foo/bar/baz.nc"),
            groups: vec!["group1".to_string(), "group2".to_string()],
        }
        .try_into()
        .unwrap();
        assert_eq!(id.to_string(), "root/foo/bar/baz.nc/group1/group2");

        let id: LayerId = NetCdfLayerCollectionId::Entity {
            path: PathBuf::from("./foo/bar/baz.nc"),
            groups: vec!["group1".to_string(), "group2".to_string()],
            entity: 7,
        }
        .try_into()
        .unwrap();
        assert_eq!(id.to_string(), "root/foo/bar/baz.nc/group1/group2/7.entity");
    }

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
    async fn test_listing() {
        let provider = Box::new(NetCdfCfDataProviderDefinition {
            name: "NetCdfCfDataProvider".to_string(),
            path: test_data!("netcdf4d").into(),
            overviews: test_data!("netcdf4d/overviews").into(),
        })
        .initialize()
        .await
        .unwrap();

        let root_id = provider.get_root_layer_collection_id().await.unwrap();

        let collection = provider
            .load_layer_collection(
                &root_id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: NETCDF_CF_PROVIDER_ID,
                    collection_id: root_id,
                },
                name: "NetCdfCfDataProvider".to_string(),
                description: "NetCdfCfProviderDefinition".to_string(),
                items: vec![
                    CollectionItem::Collection(LayerCollectionListing {
                        id: ProviderLayerCollectionId {
                            provider_id: NETCDF_CF_PROVIDER_ID,
                            collection_id: LayerCollectionId("dataset_irr_ts.nc".to_string())
                        },
                        name: "Test dataset irregular timesteps".to_string(),
                        description: "Fake description of test dataset with metric and irregular timestep definition.".to_string(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Collection(LayerCollectionListing {
                        id: ProviderLayerCollectionId {
                            provider_id: NETCDF_CF_PROVIDER_ID,
                            collection_id: LayerCollectionId("dataset_m.nc".to_string())
                        },
                        name: "Test dataset metric".to_string(),
                        description: "CFake description of test dataset with metric.".to_string(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Collection(LayerCollectionListing {
                        id: ProviderLayerCollectionId {
                            provider_id: NETCDF_CF_PROVIDER_ID,
                            collection_id: LayerCollectionId("dataset_sm.nc".to_string())
                        },
                        name: "Test dataset metric and scenario".to_string(),
                        description: "Fake description of test dataset with metric and scenario.".to_string(),
                        properties: Default::default(),
                    })
                ],
                entry_label: None,
                properties: vec![]
            }
        );
    }

    #[tokio::test]
    async fn test_listing_from_netcdf_m() {
        let provider = Box::new(NetCdfCfDataProviderDefinition {
            name: "NetCdfCfDataProvider".to_string(),
            path: test_data!("netcdf4d").into(),
            overviews: test_data!("netcdf4d/overviews").into(),
        })
        .initialize()
        .await
        .unwrap();

        let id = LayerCollectionId("dataset_m.nc".to_string());

        let collection = provider
            .load_layer_collection(
                &id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: NETCDF_CF_PROVIDER_ID,
                    collection_id: id,
                },
                name: "Test dataset metric".to_string(),
                description: "CFake description of test dataset with metric.".to_string(),
                items: vec![CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_m.nc/metric_1".to_string())
                    },
                    name: "Random metric 1".to_string(),
                    description: "Randomly created data" .to_string(),
                    properties: Default::default(),
                }), CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_m.nc/metric_2".to_string()) 
                    },
                    name: "Random metric 2".to_string(), 
                    description: "Randomly created data".to_string(),
                    properties: Default::default(),
                })],
                entry_label: None,
                properties: vec![("author".to_string(), "Luise Quoß, luise.quoss@idiv.de, German Centre for Integrative Biodiversity Research (iDiv)".to_string()).into()]
            }
        );
    }

    #[tokio::test]
    async fn test_listing_from_netcdf_sm() {
        let provider = Box::new(NetCdfCfDataProviderDefinition {
            name: "NetCdfCfDataProvider".to_string(),
            path: test_data!("netcdf4d").into(),
            overviews: test_data!("netcdf4d/overviews").into(),
        })
        .initialize()
        .await
        .unwrap();

        let id = LayerCollectionId("dataset_sm.nc".to_string());

        let collection = provider
            .load_layer_collection(
                &id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: NETCDF_CF_PROVIDER_ID,
                    collection_id: id,
                },
                name: "Test dataset metric and scenario".to_string(),
                description: "Fake description of test dataset with metric and scenario.".to_string(),
                items: vec![CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_sm.nc/scenario_1".to_string())
                    },
                    name: "Sustainability".to_string(),
                    description: "SSP1-RCP2.6" .to_string(),
                    properties: Default::default(),
                }), CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_sm.nc/scenario_2".to_string())
                    },
                    name: "Middle of the Road ".to_string(),
                    description: "SSP2-RCP4.5".to_string(),
                    properties: Default::default(),
                }), CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_sm.nc/scenario_3".to_string())
                    },
                    name: "Regional Rivalry".to_string(), 
                    description: "SSP3-RCP6.0".to_string(),
                    properties: Default::default(),
                }), CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_sm.nc/scenario_4".to_string())
                    },
                    name: "Inequality".to_string(),
                    description: "SSP4-RCP6.0".to_string(),
                    properties: Default::default(),
                }), CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_sm.nc/scenario_5".to_string())
                    },
                    name: "Fossil-fueled Development".to_string(),
                    description: "SSP5-RCP8.5".to_string(),
                    properties: Default::default(),
                })],
                entry_label: None,
                properties: vec![("author".to_string(), "Luise Quoß, luise.quoss@idiv.de, German Centre for Integrative Biodiversity Research (iDiv)".to_string()).into()]
            }
        );
    }

    #[tokio::test]
    async fn test_metadata_from_netcdf_sm() {
        let provider = NetCdfCfDataProvider {
            name: "Test Provider".to_string(),
            path: test_data!("netcdf4d/").to_path_buf(),
            overviews: test_data!("netcdf4d/overviews").to_path_buf(),
        };

        let metadata = provider
            .meta_data(
                &DataId::External(ExternalDataId {
                    provider_id: NETCDF_CF_PROVIDER_ID,
                    layer_id: LayerId(
                        serde_json::json!({
                            "fileName": "dataset_sm.nc",
                            "groupNames": ["scenario_5", "metric_2"],
                            "entity": 1
                        })
                        .to_string(),
                    ),
                })
                .into(),
            )
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
                resolution: None,
            }
        );

        let loading_info = metadata
            .loading_info(RasterQueryRectangle::with_partition_and_resolution(
                SpatialPartition2D::new(
                    (43.945_312_5, 0.791_015_625_25).into(),
                    (44.033_203_125, 0.703_125_25).into(),
                )
                .unwrap(),
                SpatialResolution::new_unchecked(
                    0.000_343_322_7, // 256 pixel
                    0.000_343_322_7, // 256 pixel
                ),
                TimeInstance::from(DateTime::new_utc(2001, 4, 1, 0, 0, 0)).into(),
            ))
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
                    retry: None,
                })
            }
        );
    }

    #[test]
    fn list_files() {
        let provider = NetCdfCfDataProvider {
            name: "Test Provider".to_string(),
            path: test_data!("netcdf4d/").to_path_buf(),
            overviews: test_data!("netcdf4d/overviews").to_path_buf(),
        };

        let expected_files: Vec<PathBuf> = vec![
            "dataset_irr_ts.nc".into(),
            "dataset_m.nc".into(),
            "dataset_sm.nc".into(),
        ];
        let mut files = provider.list_files().unwrap();
        files.sort();

        assert_eq!(files, expected_files);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_loading_info_from_index() {
        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        let provider = NetCdfCfDataProvider {
            name: "Test Provider".to_string(),
            path: test_data!("netcdf4d/").to_path_buf(),
            overviews: overview_folder.path().to_path_buf(),
        };

        provider
            .create_overviews(Path::new("dataset_sm.nc"), None, &NopTaskContext)
            .unwrap();

        let metadata = provider
            .meta_data(
                &DataId::External(ExternalDataId {
                    provider_id: NETCDF_CF_PROVIDER_ID,
                    layer_id: LayerId(
                        serde_json::json!({
                            "fileName": "dataset_sm.nc",
                            "groupNames": ["scenario_5", "metric_2"],
                            "entity": 1
                        })
                        .to_string(),
                    ),
                })
                .into(),
            )
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
                resolution: Some(SpatialResolution::new_unchecked(1000.0, 1000.0)),
            }
        );

        let loading_info = metadata
            .loading_info(RasterQueryRectangle::with_partition_and_resolution(
                SpatialPartition2D::new(
                    (43.945_312_5, 0.791_015_625_25).into(),
                    (44.033_203_125, 0.703_125_25).into(),
                )
                .unwrap(),
                SpatialResolution::new_unchecked(
                    0.000_343_322_7, // 256 pixel
                    0.000_343_322_7, // 256 pixel
                ),
                TimeInstance::from(DateTime::new_utc(2001, 4, 1, 0, 0, 0)).into(),
            ))
            .await
            .unwrap();

        let mut loading_info_parts = Vec::<GdalLoadingInfoTemporalSlice>::new();
        for part in loading_info.info {
            loading_info_parts.push(part.unwrap());
        }

        assert_eq!(loading_info_parts.len(), 1);

        let file_path = overview_folder
            .path()
            .join("dataset_sm.nc/scenario_5/metric_2/1/2000-01-01T00:00:00.000Z.tiff");

        assert_eq!(
            loading_info_parts[0],
            GdalLoadingInfoTemporalSlice {
                time: TimeInterval::new_unchecked(946_684_800_000, 1_262_304_000_000),
                params: Some(GdalDatasetParameters {
                    file_path,
                    rasterband_channel: 1,
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
                    retry: None,
                })
            }
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_listing_from_netcdf_sm_from_index() {
        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        let provider = Box::new(NetCdfCfDataProviderDefinition {
            name: "NetCdfCfDataProvider".to_string(),
            path: test_data!("netcdf4d").into(),
            overviews: overview_folder.path().to_path_buf(),
        })
        .initialize()
        .await
        .unwrap();

        let id = LayerCollectionId("dataset_sm.nc".to_string());

        let collection = provider
            .load_layer_collection(
                &id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: NETCDF_CF_PROVIDER_ID,
                    collection_id: id,
                },
                name: "Test dataset metric and scenario".to_string(),
                description: "Fake description of test dataset with metric and scenario.".to_string(),
                items: vec![CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_sm.nc/scenario_1".to_string())
                    },
                    name: "Sustainability".to_string(),
                    description: "SSP1-RCP2.6" .to_string(),
                    properties: Default::default(),
                }), CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_sm.nc/scenario_2".to_string())
                    },
                    name: "Middle of the Road ".to_string(),
                    description: "SSP2-RCP4.5".to_string(),
                    properties: Default::default(),
                }), CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_sm.nc/scenario_3".to_string())
                    },
                    name: "Regional Rivalry".to_string(),
                    description: "SSP3-RCP6.0".to_string(),
                    properties: Default::default(),
                }), CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_sm.nc/scenario_4".to_string())
                    },
                    name: "Inequality".to_string(),
                    description: "SSP4-RCP6.0".to_string(),
                    properties: Default::default(),
                }), CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_sm.nc/scenario_5".to_string()) 
                    },
                    name: "Fossil-fueled Development".to_string(), 
                    description: "SSP5-RCP8.5".to_string(),
                    properties: Default::default(),
                })],
                entry_label: None,
                properties: vec![("author".to_string(), "Luise Quoß, luise.quoss@idiv.de, German Centre for Integrative Biodiversity Research (iDiv)".to_string()).into()]
            }
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_irregular_time_series() {
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;

        let land_cover_dataset_id = add_land_cover_to_datasets(&ctx).await;

        let provider_definition: Box<dyn DataProviderDefinition> =
            Box::new(EbvPortalDataProviderDefinition {
                name: "EBV Portal".to_string(),
                path: test_data!("netcdf4d/").into(),
                base_url: "https://portal.geobon.org/api/v1".try_into().unwrap(),
                overviews: test_data!("netcdf4d/overviews/").into(),
            });

        ctx.db()
            .add_layer_provider(provider_definition)
            .await
            .unwrap();

        let operator = MeanRasterPixelValuesOverTime {
            params: MeanRasterPixelValuesOverTimeParams {
                time_position: MeanRasterPixelValuesOverTimePosition::Start,
                area: false,
            },
            sources: Expression {
                params: ExpressionParams {
                    expression: "A".to_string(),
                    output_type: RasterDataType::F64,
                    output_measurement: None,
                    map_no_data: false,
                },
                sources: ExpressionSources::new_a_b(
                    GdalSource {
                        params: GdalSourceParameters {
                            data: ExternalDataId {
                                provider_id: EBV_PROVIDER_ID,
                                layer_id: LayerId(
                                    serde_json::json!({
                                        "fileName": "dataset_irr_ts.nc",
                                        "groupNames": ["metric_1"],
                                        "entity": 0
                                    })
                                    .to_string(),
                                ),
                            }
                            .into(),
                        },
                    }
                    .boxed(),
                    GdalSource {
                        params: GdalSourceParameters {
                            data: land_cover_dataset_id.into(),
                        },
                    }
                    .boxed(),
                ),
            }
            .boxed()
            .into(),
        }
        .boxed();

        // let execution_context = MockExecutionContext::test_default();
        let execution_context = ctx.execution_context().unwrap();

        let initialized_operator = operator
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let TypedPlotQueryProcessor::JsonVega(processor) = initialized_operator.query_processor().unwrap() else {
            panic!("wrong plot type");
        };

        let query_context = MockQueryContext::test_default();

        let result = processor
            .plot_query(
                PlotQueryRectangle::with_bounds_and_resolution(
                    BoundingBox2D::new(
                        (46.478_278_849, 40.584_655_660_000_1).into(),
                        (87.323_796_021_000_1, 55.434_550_273).into(),
                    )
                    .unwrap(),
                    TimeInterval::new(
                        DateTime::new_utc(1900, 4, 1, 0, 0, 0),
                        DateTime::new_utc_with_millis(2055, 4, 1, 0, 0, 0, 1),
                    )
                    .unwrap(),
                    SpatialResolution::new_unchecked(0.1, 0.1),
                ),
                &query_context,
            )
            .await
            .unwrap();

        assert_eq!(result, PlotData {
            vega_string: "{\"$schema\":\"https://vega.github.io/schema/vega-lite/v4.17.0.json\",\"data\":{\"values\":[{\"x\":\"2015-01-01T00:00:00+00:00\",\"y\":46.34280000000002},{\"x\":\"2055-01-01T00:00:00+00:00\",\"y\":43.54399999999997}]},\"description\":\"Area Plot\",\"encoding\":{\"x\":{\"field\":\"x\",\"title\":\"Time\",\"type\":\"temporal\"},\"y\":{\"field\":\"y\",\"title\":\"\",\"type\":\"quantitative\"}},\"mark\":{\"line\":true,\"point\":true,\"type\":\"line\"}}".to_string(),
            metadata: PlotMetaData::None,
        });
    }
}
