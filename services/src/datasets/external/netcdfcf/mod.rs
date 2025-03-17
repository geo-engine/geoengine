use self::database::NetCdfDatabaseListingConfig;
use self::loading::{
    LayerCollectionIdFn, LayerCollectionParts, create_layer, create_layer_collection_from_parts,
};
use self::metadata::{Creator, DataRange, NetCdfGroupMetadata, NetCdfOverviewMetadata};
use self::overviews::create_overviews;
use self::overviews::{OverviewCreationOptions, remove_overviews};
use crate::contexts::GeoEngineDb;
use crate::datasets::external::netcdfcf::loading::{ParamModification, create_loading_info};
use crate::datasets::listing::ProvenanceOutput;
use crate::error::Error;
use crate::layers::external::DataProvider;
use crate::layers::external::DataProviderDefinition;
use crate::layers::layer::LayerCollectionListOptions;
use crate::layers::layer::LayerCollectionListing;
use crate::layers::layer::ProviderLayerCollectionId;
use crate::layers::layer::{CollectionItem, LayerCollection};
use crate::layers::layer::{Layer, ProviderLayerId};
use crate::layers::listing::LayerCollectionProvider;
use crate::layers::listing::{LayerCollectionId, ProviderCapabilities, SearchCapabilities};
use crate::tasks::TaskContext;
use async_trait::async_trait;
use gdal::raster::{Dimension, GdalDataType, Group};
use gdal::{DatasetOptions, GdalOpenFlags};
use geoengine_datatypes::dataset::{DataId, DataProviderId, LayerId, NamedData};
use geoengine_datatypes::error::BoxedResultExt;
use geoengine_datatypes::operations::image::{Colorizer, RgbaColor};
use geoengine_datatypes::primitives::{
    CacheTtlSeconds, DateTime, Measurement, RasterQueryRectangle, TimeInstance,
    VectorQueryRectangle,
};
use geoengine_datatypes::raster::{GdalGeoTransform, RasterDataType};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_datatypes::util::canonicalize_subpath;
use geoengine_datatypes::util::gdal::ResamplingMethod;
use geoengine_operators::engine::RasterBandDescriptor;
use geoengine_operators::engine::RasterBandDescriptors;
use geoengine_operators::source::{
    FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters,
};
use geoengine_operators::util::gdal::gdal_open_dataset_ex;
use geoengine_operators::{
    engine::{MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use log::debug;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use walkdir::{DirEntry, WalkDir};

pub use self::database::NetCdfCfProviderDb;
pub use self::ebvportal_provider::{
    EBV_PROVIDER_ID, EbvPortalDataProvider, EbvPortalDataProviderDefinition,
};
pub use self::error::NetCdfCf4DProviderError;
pub use self::overviews::OverviewGeneration;

pub(crate) mod database;
mod ebvportal_api;
mod ebvportal_provider;
pub mod error;
pub(crate) mod loading;
mod metadata;
pub(crate) mod overviews;

pub(crate) type Result<T, E = NetCdfCf4DProviderError> = std::result::Result<T, E>;

/// Singleton Provider with id `1690c483-b17f-4d98-95c8-00a64849cd0b`
pub const NETCDF_CF_PROVIDER_ID: DataProviderId =
    DataProviderId::from_u128(0x1690_c483_b17f_4d98_95c8_00a6_4849_cd0b);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NetCdfCfDataProviderDefinition {
    pub name: String,
    pub description: String,
    pub priority: Option<i16>,
    /// Path were the `NetCDF` data can be found
    pub data: PathBuf,
    /// Path were overview files are stored
    pub overviews: PathBuf,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
}

#[derive(Debug)]
pub struct NetCdfCfDataProvider<D: GeoEngineDb> {
    pub id: DataProviderId,
    pub name: String,
    pub description: String,
    pub data: PathBuf,
    pub overviews: PathBuf,
    pub cache_ttl: CacheTtlSeconds,
    pub db: Arc<D>,
}

#[async_trait]
impl<D: GeoEngineDb> DataProviderDefinition<D> for NetCdfCfDataProviderDefinition {
    async fn initialize(self: Box<Self>, db: D) -> crate::error::Result<Box<dyn DataProvider>> {
        let id = DataProviderDefinition::<D>::id(&*self);
        Ok(Box::new(Self::_initialize(*self, id, db)) as Box<dyn DataProvider>)
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

    fn priority(&self) -> i16 {
        self.priority.unwrap_or(0)
    }
}

impl NetCdfCfDataProviderDefinition {
    fn _initialize<D: GeoEngineDb>(self, id: DataProviderId, db: D) -> NetCdfCfDataProvider<D> {
        NetCdfCfDataProvider {
            id,
            name: self.name,
            description: self.description,
            data: self.data,
            overviews: self.overviews,
            cache_ttl: self.cache_ttl,
            db: Arc::new(db),
        }
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

impl From<NetCdfOverview> for NetCdfOverviewMetadata {
    fn from(value: NetCdfOverview) -> Self {
        Self {
            file_name: value.file_name,
            title: value.title,
            summary: value.summary,
            spatial_reference: value.spatial_reference,
            colorizer: value.colorizer,
            creator: Creator::new(
                value.creator_name,
                value.creator_email,
                value.creator_institution,
            ),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NetCdfGroup {
    pub name: String,
    pub title: String,
    pub description: String,
    // TODO: would actually be nice if it were inside dataset/entity
    pub data_type: Option<RasterDataType>,
    pub data_range: Option<DataRange>,
    // TODO: would actually be nice if it were inside dataset/entity
    pub unit: String,
    pub groups: Vec<NetCdfGroup>,
}

impl From<NetCdfGroup> for NetCdfGroupMetadata {
    fn from(value: NetCdfGroup) -> Self {
        Self {
            name: value.name,
            title: value.title,
            description: value.description,
            data_type: value.data_type,
            data_range: value.data_range,
            unit: value.unit,
        }
    }
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
        stats_for_group: &HashMap<String, DataRange>,
    ) -> Result<NetCdfGroup>;
}

impl ToNetCdfSubgroup for Group<'_> {
    fn to_net_cdf_subgroup(
        &self,
        group_path: &Path,
        stats_for_group: &HashMap<String, DataRange>,
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

impl NetCdfCf4DDatasetId {
    pub fn as_named_data(&self, provider_id: &DataProviderId) -> serde_json::Result<NamedData> {
        Ok(
            geoengine_datatypes::dataset::NamedData::with_system_provider(
                provider_id.to_string(),
                serde_json::to_string(&self)?,
            ),
        )
    }
}

#[allow(clippy::implicit_hasher)] // we don't use different hashers here
pub fn build_netcdf_tree(
    provider_path: &Path,
    dataset_path: &Path,
    stats_for_group: &HashMap<String, DataRange>,
) -> Result<NetCdfOverview> {
    let provider_path = provider_path
        .canonicalize()
        .boxed_context(error::InvalidDirectory)?;
    let path = canonicalize_subpath(&provider_path, dataset_path).map_err(|_| {
        NetCdfCf4DProviderError::FileIsNotInProviderPath {
            file: dataset_path.to_string_lossy().into(),
        }
    })?;

    let ds = gdal_netcdf_open(None, &path)?;

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

    let time_coverage = TimeCoverage::from_dimension(&root_group)?;

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

impl<D: GeoEngineDb> NetCdfCfDataProvider<D> {
    async fn meta_data(
        db: &D,
        provider_id: DataProviderId,
        path: &Path,
        id: &DataId,
        cache_ttl: CacheTtlSeconds,
    ) -> Result<Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>>
    {
        let DataId::External(dataset) = id else {
            return Err(NetCdfCf4DProviderError::InvalidExternalDataId {
                data_id: id.clone(),
            });
        };

        let dataset_id: NetCdfCf4DDatasetId =
            serde_json::from_str(&dataset.layer_id.0).context(error::CannotParseDatasetId)?;

        // try to load from overviews
        if let Some(mut loading_info) = {
            let dataset_id = dataset_id.clone();
            db.loading_info(
                provider_id,
                &dataset_id.file_name,
                &dataset_id.group_names,
                dataset_id.entity,
            )
            .await?
        } {
            for params in &mut loading_info.params {
                params.cache_ttl = cache_ttl;
            }
            return Ok(Box::new(loading_info));
        }

        let path = path.to_owned();
        crate::util::spawn_blocking(move || {
            Self::meta_data_from_netcdf(&path, &dataset_id, cache_ttl)
        })
        .await
        .boxed_context(error::UnexpectedExecution)?
    }

    fn meta_data_from_netcdf(
        base_path: &Path,
        dataset_id: &NetCdfCf4DDatasetId,
        cache_ttl: CacheTtlSeconds,
    ) -> Result<Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>>
    {
        const LON_DIMENSION_INDEX: usize = 3;
        const LAT_DIMENSION_INDEX: usize = 2;
        const TIME_DIMENSION_INDEX: usize = 1;

        let dataset = gdal_netcdf_open(Some(base_path), Path::new(&dataset_id.file_name))?;

        let root_group = dataset.root_group().context(error::GdalMd)?;

        let time_coverage = TimeCoverage::from_dimension(&root_group)?;

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
            time: None,
            bbox: None,
            resolution: None,
            bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                "band".into(),
                derive_measurement(data_array.unit()),
            )])
            .context(error::GeneratingResultDescriptorFromDataset)?,
        };

        let params = GdalDatasetParameters {
            file_path: netcfg_gdal_path(
                Some(base_path),
                Path::new(&dataset_id.file_name),
                &dataset_id.group_names,
            )?,
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

        Ok(Box::new(create_loading_info(
            result_descriptor,
            &params,
            time_coverage
                .time_steps()
                .iter()
                .enumerate()
                .map(|(i, time_instance)| ParamModification::Channel {
                    channel: dataset_id.entity * dimensions_time + i + 1,
                    time_instance: *time_instance,
                }),
            cache_ttl,
        )))
    }

    pub fn list_files(&self) -> Result<Vec<PathBuf>> {
        let is_overview_dir = |e: &DirEntry| -> bool { e.path() == self.overviews };

        let mut files = vec![];

        for entry in WalkDir::new(&self.data)
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
            if path.extension().is_none_or(|extension| extension != "nc") {
                continue;
            }

            match path.strip_prefix(&self.data) {
                Ok(path) => files.push(path.to_owned()),
                Err(_) => {
                    // we can safely ignore it since it must be a file in the provider path
                    continue;
                }
            };
        }

        Ok(files)
    }

    pub async fn create_overviews<C: TaskContext + 'static>(
        &self,
        dataset_path: &Path,
        resampling_method: Option<ResamplingMethod>,
        task_context: C,
    ) -> Result<OverviewGeneration> {
        if self
            .db
            .overviews_exist(self.id, &dataset_path.to_string_lossy())
            .await?
        {
            return Ok(OverviewGeneration::Skipped);
        }

        let details = create_overviews(
            task_context,
            self.db.clone(),
            OverviewCreationOptions {
                provider_id: self.id,
                provider_path: &self.data,
                overview_path: &self.overviews,
                dataset_path,
                resampling_method,
                check_file_only: false,
            },
        )
        .await?;

        Ok(OverviewGeneration::Created { details })
    }

    pub async fn refresh_overview_metadata<C: TaskContext + 'static>(
        &self,
        dataset_path: &Path,
        task_context: C,
    ) -> Result<OverviewGeneration> {
        let details = create_overviews(
            task_context,
            self.db.clone(),
            OverviewCreationOptions {
                provider_id: self.id,
                provider_path: &self.data,
                overview_path: &self.overviews,
                dataset_path,
                resampling_method: None,
                check_file_only: true,
            },
        )
        .await?;

        Ok(OverviewGeneration::Created { details })
    }

    pub async fn remove_overviews(&self, dataset_path: &Path, force: bool) -> Result<()> {
        remove_overviews(
            self.id,
            dataset_path,
            &self.overviews,
            self.db.clone(),
            force,
        )
        .await?;

        Ok(())
    }

    fn is_netcdf_file(&self, path: &Path) -> bool {
        let real_path = self.data.join(path);
        real_path.is_file() && real_path.extension() == Some("nc".as_ref())
    }

    async fn _load_layer_collection<ID: LayerCollectionIdFn>(
        &self,
        collection: &LayerCollectionId,
        options: LayerCollectionListOptions,
        id_fn: ID,
    ) -> crate::error::Result<LayerCollection> {
        async fn generate_listing_from_netcdf<D: NetCdfCfProviderDb, ID: LayerCollectionIdFn>(
            db: &D,
            config: NetCdfListingConfig<'_>,
            id_fn: ID,
        ) -> crate::error::Result<LayerCollection> {
            listing_from_netcdf(db, config, id_fn).await
        }

        let id = NetCdfLayerCollectionId::from_str(&collection.0)?;
        Ok(match id {
            NetCdfLayerCollectionId::Path { path }
                if canonicalize_subpath(&self.data, &path).is_ok()
                    && self.data.join(&path).is_dir() =>
            {
                listing_from_dir(
                    self.id,
                    &self.name,
                    collection,
                    &self.overviews,
                    &self.data,
                    &path,
                    &options,
                )
                .await?
            }
            NetCdfLayerCollectionId::Path { path }
                if canonicalize_subpath(&self.data, &path).is_ok()
                    && self.is_netcdf_file(&path) =>
            {
                generate_listing_from_netcdf(
                    self.db.as_ref(),
                    NetCdfListingConfig {
                        provider_id: self.id,
                        collection,
                        relative_file_path: path,
                        groups: &[],
                        provider_path: self.data.clone(),
                        options: &options,
                    },
                    id_fn,
                )
                .await?
            }
            NetCdfLayerCollectionId::Group { path, groups }
                if canonicalize_subpath(&self.data, &path).is_ok()
                    && self.is_netcdf_file(&path) =>
            {
                generate_listing_from_netcdf(
                    self.db.as_ref(),
                    NetCdfListingConfig {
                        provider_id: self.id,
                        collection,
                        relative_file_path: path,
                        groups: &groups,
                        provider_path: self.data.clone(),
                        options: &options,
                    },
                    id_fn,
                )
                .await?
            }
            _ => return Err(Error::InvalidLayerCollectionId),
        })
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
                25.5.try_into().expect("not nan"),
                RgbaColor::new(72, 36, 117, 255),
            )
                .into(),
            (
                51.0.try_into().expect("not nan"),
                RgbaColor::new(65, 68, 135, 255),
            )
                .into(),
            (
                76.5.try_into().expect("not nan"),
                RgbaColor::new(52, 95, 141, 255),
            )
                .into(),
            (
                102.0.try_into().expect("not nan"),
                RgbaColor::new(42, 120, 142, 255),
            )
                .into(),
            (
                127.5.try_into().expect("not nan"),
                RgbaColor::new(33, 144, 140, 255),
            )
                .into(),
            (
                153.0.try_into().expect("not nan"),
                RgbaColor::new(34, 168, 132, 255),
            )
                .into(),
            (
                178.5.try_into().expect("not nan"),
                RgbaColor::new(67, 190, 112, 255),
            )
                .into(),
            (
                204.0.try_into().expect("not nan"),
                RgbaColor::new(122, 209, 81, 255),
            )
                .into(),
            (
                229.5.try_into().expect("not nan"),
                RgbaColor::new(187, 222, 39, 255),
            )
                .into(),
            (
                255.0.try_into().expect("not nan"),
                RgbaColor::new(253, 231, 37, 255),
            )
                .into(),
        ],
        RgbaColor::transparent(),
        RgbaColor::white(),
        RgbaColor::black(),
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TimeCoverage {
    time_stamps: Vec<TimeInstance>,
}

impl TimeCoverage {
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
            let hours = days * 24.;
            let seconds = hours * 60. * 60.;
            let milliseconds = seconds * 1_000.;

            time_stamps.push(
                TimeInstance::from_millis(milliseconds as i64 + unix_offset_millis)
                    .context(error::InvalidTimeCoverageInstant)?,
            );
        }

        Ok(Self { time_stamps })
    }

    // fn number_of_time_steps(&self) -> usize {
    //     self.time_stamps.len()
    // }

    fn time_steps(&self) -> &[TimeInstance] {
        self.time_stamps.as_slice()
    }
}

#[async_trait]
impl<D: GeoEngineDb> DataProvider for NetCdfCfDataProvider<D> {
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

fn netcdf_group_to_layer_collection_id(path: &Path, groups: &[String]) -> LayerCollectionId {
    LayerCollectionId(format!("{}/{}", path_to_string(path), groups.join("/")))
}

impl TryFrom<NetCdfLayerCollectionId> for LayerCollectionId {
    type Error = crate::error::Error;

    fn try_from(id: NetCdfLayerCollectionId) -> crate::error::Result<Self> {
        Ok(match id {
            NetCdfLayerCollectionId::Path { path } => LayerCollectionId(path_to_string(&path)),
            NetCdfLayerCollectionId::Group { path, groups } => {
                netcdf_group_to_layer_collection_id(&path, &groups)
            }
            NetCdfLayerCollectionId::Entity { .. } => {
                return Err(crate::error::Error::InvalidLayerCollectionId);
            }
        })
    }
}

fn netcdf_entity_to_layer_id(path: &Path, groups: &[String], entity: usize) -> LayerId {
    LayerId(format!(
        "{}/{}/{}.entity",
        path_to_string(path),
        groups.join("/"),
        entity
    ))
}

impl TryFrom<NetCdfLayerCollectionId> for LayerId {
    type Error = crate::error::Error;

    fn try_from(id: NetCdfLayerCollectionId) -> crate::error::Result<Self> {
        Ok(match id {
            NetCdfLayerCollectionId::Entity {
                path,
                groups,
                entity,
            } => netcdf_entity_to_layer_id(&path, &groups, entity),
            _ => return Err(crate::error::Error::InvalidLayerId),
        })
    }
}

struct NetCdfCfIdFn;

impl LayerCollectionIdFn for NetCdfCfIdFn {
    fn layer_collection_id(&self, path: &Path, groups: &[String]) -> LayerCollectionId {
        netcdf_group_to_layer_collection_id(path, groups)
    }

    fn layer_id(&self, path: &Path, groups: &[String], entity: usize) -> LayerId {
        netcdf_entity_to_layer_id(path, groups, entity)
    }
}

async fn listing_from_dir(
    provider_id: DataProviderId,
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
            items.push(CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                id: ProviderLayerCollectionId {
                    provider_id,
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
                build_netcdf_tree(&b, &fp, &Default::default())
                    .map_err(|_| Error::InvalidLayerCollectionId)
            })
            .await??;

            items.push(CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                id: ProviderLayerCollectionId {
                    provider_id,
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
            provider_id,
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

fn determine_data_range_and_colorizer(
    data_range: Option<DataRange>,
    colorizer: Colorizer,
) -> crate::error::Result<(DataRange, Colorizer)> {
    Ok(if let Some(data_range) = data_range {
        (
            data_range,
            colorizer.rescale(data_range.min(), data_range.max())?,
        )
    } else {
        (
            DataRange::new(colorizer.min_value(), colorizer.max_value()),
            colorizer,
        )
    })
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

    let time_steps = overview.time_coverage.time_stamps;

    let group = find_group(overview.groups, groups)?.ok_or(Error::InvalidLayerId)?;

    let (data_range, colorizer) =
        determine_data_range_and_colorizer(group.data_range, overview.colorizer)?;

    Ok(create_layer(
        ProviderLayerId {
            provider_id,
            layer_id: layer_id.clone(),
        },
        NetCdfCf4DDatasetId {
            file_name: overview.file_name,
            group_names: groups.to_owned(),
            entity,
        }
        .as_named_data(&provider_id)
        .context(error::CannotSerializeLayer)?,
        netcdf_entity,
        colorizer,
        &Creator::new(
            overview.creator_name,
            overview.creator_email,
            overview.creator_institution,
        ),
        &time_steps,
        data_range,
    )?)
}

struct NetCdfListingConfig<'a> {
    provider_id: DataProviderId,
    collection: &'a LayerCollectionId,
    relative_file_path: PathBuf,
    groups: &'a [String],
    provider_path: PathBuf,
    options: &'a LayerCollectionListOptions,
}

async fn listing_from_netcdf<D: NetCdfCfProviderDb, ID: LayerCollectionIdFn>(
    db: &D,
    NetCdfListingConfig {
        provider_id,
        collection,
        relative_file_path,
        groups,
        provider_path,
        options,
    }: NetCdfListingConfig<'_>,
    id_fn: ID,
) -> crate::error::Result<LayerCollection> {
    let id_fn = Arc::new(id_fn);

    let query_file_name = relative_file_path.to_string_lossy();
    if let Some(layer_collection) = db
        .layer_collection(
            NetCdfDatabaseListingConfig {
                provider_id,
                collection,
                file_name: &query_file_name,
                group_path: groups,
                options,
            },
            id_fn.clone(),
        )
        .await?
    {
        return Ok(layer_collection);
    }

    listing_from_netcdf_with_file(
        NetCdfFileListingConfig {
            provider_id,
            collection,
            relative_file_path,
            groups,
            provider_path,
            options,
        },
        id_fn,
    )
    .await
}

struct NetCdfFileListingConfig<'a> {
    provider_id: DataProviderId,
    collection: &'a LayerCollectionId,
    relative_file_path: PathBuf,
    groups: &'a [String],
    provider_path: PathBuf,
    options: &'a LayerCollectionListOptions,
}

async fn listing_from_netcdf_with_file<ID: LayerCollectionIdFn>(
    NetCdfFileListingConfig {
        provider_id,
        collection,
        relative_file_path,
        groups,
        provider_path,
        options,
    }: NetCdfFileListingConfig<'_>,
    id_fn: Arc<ID>,
) -> crate::error::Result<LayerCollection> {
    let tree = tokio::task::spawn_blocking(move || {
        build_netcdf_tree(&provider_path, &relative_file_path, &Default::default())
            .map_err(|_| Error::InvalidLayerCollectionId)
    })
    .await??;

    let group = find_group(tree.groups.clone(), groups)?;

    let entities = tree.entities;
    let (group_metadata, subgroups) = if let Some(group) = group {
        (
            Some(NetCdfGroupMetadata {
                name: group.name,
                title: group.title,
                description: group.description,
                data_type: group.data_type,
                data_range: group.data_range,
                unit: group.unit,
            }),
            group.groups,
        )
    } else {
        (None, tree.groups)
    };

    let overview_metadata = NetCdfOverviewMetadata {
        file_name: tree.file_name,
        title: tree.title,
        summary: tree.summary,
        spatial_reference: tree.spatial_reference,
        colorizer: tree.colorizer,
        creator: Creator::new(
            tree.creator_name,
            tree.creator_email,
            tree.creator_institution,
        ),
    };

    Ok(create_layer_collection_from_parts(
        LayerCollectionParts {
            provider_id,
            collection_id: collection.clone(),
            group_path: groups,
            overview: overview_metadata,
            group: group_metadata,
            subgroups: subgroups
                .into_iter()
                .skip(options.offset as usize)
                .take(options.limit as usize)
                .map(Into::into)
                .collect(),
            entities: entities
                .into_iter()
                .skip(options.offset as usize)
                .take(options.limit as usize),
        },
        id_fn.as_ref(),
    ))
}

#[async_trait]
impl<D: GeoEngineDb> LayerCollectionProvider for NetCdfCfDataProvider<D> {
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            listing: true,
            search: SearchCapabilities::none(),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    async fn load_layer_collection(
        &self,
        collection: &LayerCollectionId,
        options: LayerCollectionListOptions,
    ) -> crate::error::Result<LayerCollection> {
        #[allow(clippy::used_underscore_items)] // TODO: maybe rename?
        self._load_layer_collection(collection, options, NetCdfCfIdFn)
            .await
    }

    async fn get_root_layer_collection_id(&self) -> crate::error::Result<LayerCollectionId> {
        Ok(NetCdfLayerCollectionId::Path { path: ".".into() }.try_into()?)
    }

    async fn load_layer(&self, id: &LayerId) -> crate::error::Result<Layer> {
        let netcdf_id = NetCdfLayerCollectionId::from_str(&id.0)?;

        let NetCdfLayerCollectionId::Entity {
            path,
            groups,
            entity,
        } = netcdf_id
        else {
            return Err(Error::InvalidLayerId);
        };

        let dataset_path = path.clone();

        let provider_path = self.data.clone();

        let query_file_name = dataset_path.to_string_lossy();

        if let Some(layer) = self
            .db
            .layer(self.id, id, &query_file_name, &groups, entity)
            .await?
        {
            // listing from database

            return Ok(layer);
        }

        // listing from file directly

        let tree = tokio::task::spawn_blocking(move || {
            build_netcdf_tree(&provider_path, &dataset_path, &Default::default())
                .map_err(|_| Error::InvalidLayerCollectionId)
        })
        .await??;

        layer_from_netcdf_overview(self.id, id, tree, &groups, entity)
    }
}

#[async_trait]
impl<D: GeoEngineDb> MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for NetCdfCfDataProvider<D>
{
    async fn meta_data(
        &self,
        id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        async fn meta_data_transaction<D: GeoEngineDb>(
            provider: &NetCdfCfDataProvider<D>,
            id: &geoengine_datatypes::dataset::DataId,
        ) -> Result<Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>>
        {
            let result = NetCdfCfDataProvider::<D>::meta_data(
                &provider.db,
                provider.id,
                &provider.data,
                id,
                provider.cache_ttl,
            )
            .await?;

            Ok(result)
        }

        meta_data_transaction(self, id).await.map_err(|error| {
            geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(error),
            }
        })
    }
}

#[async_trait]
impl<D: GeoEngineDb>
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for NetCdfCfDataProvider<D>
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
impl<D: GeoEngineDb>
    MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for NetCdfCfDataProvider<D>
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

fn netcfg_gdal_path(
    base_path: Option<&Path>,
    path: &Path,
    group_path: &[String],
) -> Result<PathBuf> {
    let path = if let Some(base_path) = base_path {
        canonicalize_subpath(base_path, path).map_err(|_| {
            NetCdfCf4DProviderError::FileIsNotInProviderPath {
                file: path.to_string_lossy().into(),
            }
        })?
    } else {
        path.to_owned()
    };

    let path = path.to_string_lossy().to_string();
    let group_path = group_path.join("/");
    let gdal_path = format!("NETCDF:{path}:/{group_path}/ebv_cube");
    Ok(PathBuf::from(gdal_path))
}

fn gdal_netcdf_open(base_path: Option<&Path>, path: &Path) -> Result<gdal::Dataset> {
    let path = if let Some(base_path) = base_path {
        canonicalize_subpath(base_path, path).map_err(|_| {
            NetCdfCf4DProviderError::FileIsNotInProviderPath {
                file: path.to_string_lossy().into(),
            }
        })?
    } else {
        path.to_owned()
    };

    let dataset = gdal_open_dataset_ex(
        &path,
        DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_READONLY | GdalOpenFlags::GDAL_OF_MULTIDIM_RASTER,
            allowed_drivers: Some(&["netCDF"]),
            open_options: None,
            sibling_files: None,
        },
    )
    .context(error::InvalidDatasetIdFile)?;

    Ok(dataset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::SessionContext;
    use crate::contexts::{PostgresContext, PostgresDb, PostgresSessionContext};
    use crate::datasets::external::netcdfcf::ebvportal_provider::EbvPortalDataProviderDefinition;
    use crate::ge_context;
    use crate::layers::layer::LayerListing;
    use crate::layers::storage::LayerProviderDb;
    use crate::{tasks::util::NopTaskContext, util::tests::add_land_cover_to_datasets};
    use geoengine_datatypes::dataset::ExternalDataId;
    use geoengine_datatypes::plots::{PlotData, PlotMetaData};
    use geoengine_datatypes::primitives::{BandSelection, PlotSeriesSelection};
    use geoengine_datatypes::raster::RenameBands;
    use geoengine_datatypes::{
        primitives::{
            BoundingBox2D, PlotQueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval,
        },
        spatial_reference::SpatialReferenceAuthority,
        test_data,
        util::gdal::hide_gdal_errors,
    };
    use geoengine_operators::engine::{
        MultipleRasterSources, RasterBandDescriptors, RasterOperator, SingleRasterSource,
    };
    use geoengine_operators::processing::{
        RasterStacker, RasterStackerParams, RasterTypeConversion, RasterTypeConversionParams,
    };
    use geoengine_operators::source::{GdalSource, GdalSourceParameters};
    use geoengine_operators::{
        engine::{PlotOperator, TypedPlotQueryProcessor, WorkflowOperatorPath},
        plot::{
            MeanRasterPixelValuesOverTime, MeanRasterPixelValuesOverTimeParams,
            MeanRasterPixelValuesOverTimePosition,
        },
        processing::{Expression, ExpressionParams},
        source::{
            FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters,
            GdalLoadingInfoTemporalSlice,
        },
    };
    use tokio_postgres::NoTls;

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

    #[ge_context::test]
    async fn test_listing(_app_ctx: PostgresContext<NoTls>, ctx: PostgresSessionContext<NoTls>) {
        let provider = Box::new(NetCdfCfDataProviderDefinition {
            name: "NetCdfCfDataProvider".to_string(),
            description: "NetCdfCfProviderDefinition".to_string(),
            priority: Some(-2),
            data: test_data!("netcdf4d").into(),
            overviews: test_data!("netcdf4d/overviews").into(),
            cache_ttl: Default::default(),
        })
        .initialize(ctx.db())
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

        pretty_assertions::assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: NETCDF_CF_PROVIDER_ID,
                    collection_id: root_id,
                },
                name: "NetCdfCfDataProvider".to_string(),
                description: "NetCdfCfProviderDefinition".to_string(),
                items: vec![
                    CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                        id: ProviderLayerCollectionId {
                            provider_id: NETCDF_CF_PROVIDER_ID,
                            collection_id: LayerCollectionId("Biodiversity".to_string())
                        },
                        name: "Biodiversity".to_string(),
                        description: String::new(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                        id: ProviderLayerCollectionId {
                            provider_id: NETCDF_CF_PROVIDER_ID,
                            collection_id: LayerCollectionId("dataset_irr_ts.nc".to_string())
                        },
                        name: "Test dataset irregular timesteps".to_string(),
                        description: "Fake description of test dataset with metric and irregular timestep definition.".to_string(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                        id: ProviderLayerCollectionId {
                            provider_id: NETCDF_CF_PROVIDER_ID,
                            collection_id: LayerCollectionId("dataset_m.nc".to_string())
                        },
                        name: "Test dataset metric".to_string(),
                        description: "CFake description of test dataset with metric.".to_string(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                        id: ProviderLayerCollectionId {
                            provider_id: NETCDF_CF_PROVIDER_ID,
                            collection_id: LayerCollectionId("dataset_sm.nc".to_string())
                        },
                        name: "Test dataset metric and scenario".to_string(),
                        description: "Fake description of test dataset with metric and scenario.".to_string(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                        id: ProviderLayerCollectionId {
                            provider_id: NETCDF_CF_PROVIDER_ID,
                            collection_id: LayerCollectionId("dataset_esri.nc".to_string())
                        },
                        name: "Test dataset metric monthly".to_string(),
                        description: "Fake description of test dataset with metric.".to_string(),
                        properties: Default::default(),
                    }),
                ],
                entry_label: None,
                properties: vec![]
            }
        );
    }

    #[ge_context::test]
    async fn test_listing_from_netcdf_m(ctx: PostgresSessionContext<NoTls>) {
        let provider = Box::new(NetCdfCfDataProviderDefinition {
            name: "NetCdfCfDataProvider".to_string(),
            description: "NetCdfCfProviderDefinition".to_string(),
            priority: Some(-3),
            data: test_data!("netcdf4d").into(),
            overviews: test_data!("netcdf4d/overviews").into(),
            cache_ttl: Default::default(),
        })
        .initialize(ctx.db())
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

        pretty_assertions::assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: NETCDF_CF_PROVIDER_ID,
                    collection_id: id,
                },
                name: "Test dataset metric".to_string(),
                description: "CFake description of test dataset with metric.".to_string(),
                items: vec![CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_m.nc/metric_1".to_string())
                    },
                    name: "Random metric 1".to_string(),
                    description: "Randomly created data" .to_string(),
                    properties: Default::default(),
                }), CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
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

    #[ge_context::test]
    async fn test_listing_from_netcdf_sm(ctx: PostgresSessionContext<NoTls>) {
        let provider = Box::new(NetCdfCfDataProviderDefinition {
            name: "NetCdfCfDataProvider".to_string(),
            description: "NetCdfCfProviderDefinition".to_string(),
            priority: Some(-4),
            data: test_data!("netcdf4d").into(),
            overviews: test_data!("netcdf4d/overviews").into(),
            cache_ttl: Default::default(),
        })
        .initialize(ctx.db())
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

        pretty_assertions::assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: NETCDF_CF_PROVIDER_ID,
                    collection_id: id,
                },
                name: "Test dataset metric and scenario".to_string(),
                description: "Fake description of test dataset with metric and scenario.".to_string(),
                items: vec![CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_sm.nc/scenario_1".to_string())
                    },
                    name: "Sustainability".to_string(),
                    description: "SSP1-RCP2.6" .to_string(),
                    properties: Default::default(),
                }), CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_sm.nc/scenario_2".to_string())
                    },
                    name: "Middle of the Road ".to_string(),
                    description: "SSP2-RCP4.5".to_string(),
                    properties: Default::default(),
                }), CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_sm.nc/scenario_3".to_string())
                    },
                    name: "Regional Rivalry".to_string(),
                    description: "SSP3-RCP6.0".to_string(),
                    properties: Default::default(),
                }), CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_sm.nc/scenario_4".to_string())
                    },
                    name: "Inequality".to_string(),
                    description: "SSP4-RCP6.0".to_string(),
                    properties: Default::default(),
                }), CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
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

    #[ge_context::test]
    async fn test_metadata_from_netcdf_sm(ctx: PostgresSessionContext<NoTls>) {
        let provider = NetCdfCfDataProvider {
            id: NETCDF_CF_PROVIDER_ID,
            name: "Test Provider".to_string(),
            description: "Test Provider".to_string(),
            data: test_data!("netcdf4d/").to_path_buf(),
            overviews: test_data!("netcdf4d/overviews").to_path_buf(),
            cache_ttl: Default::default(),
            db: Arc::new(ctx.db()),
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
                time: None,
                bbox: None,
                resolution: None,
                bands: RasterBandDescriptors::new_single_band(),
            }
        );

        let loading_info = metadata
            .loading_info(RasterQueryRectangle {
                spatial_bounds: SpatialPartition2D::new(
                    (43.945_312_5, 0.791_015_625_25).into(),
                    (44.033_203_125, 0.703_125_25).into(),
                )
                .unwrap(),
                time_interval: TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0)).into(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    0.000_343_322_7, // 256 pixel
                    0.000_343_322_7, // 256 pixel
                ),
                attributes: BandSelection::first(),
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
                time: TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0)).into(),
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
                },),
                cache_ttl: CacheTtlSeconds::default(),
            }
        );
    }

    #[ge_context::test]
    #[allow(clippy::unused_async)]
    async fn list_files(_app_ctx: PostgresContext<NoTls>, ctx: PostgresSessionContext<NoTls>) {
        let provider = NetCdfCfDataProvider {
            id: NETCDF_CF_PROVIDER_ID,
            name: "Test Provider".to_string(),
            description: "Test Provider".to_string(),
            data: test_data!("netcdf4d/").to_path_buf(),
            overviews: test_data!("netcdf4d/overviews").to_path_buf(),
            cache_ttl: Default::default(),
            db: Arc::new(ctx.db()),
        };

        let expected_files: Vec<PathBuf> = vec![
            "Biodiversity/dataset_daily.nc".into(),
            "Biodiversity/dataset_monthly.nc".into(),
            "dataset_esri.nc".into(),
            "dataset_irr_ts.nc".into(),
            "dataset_m.nc".into(),
            "dataset_sm.nc".into(),
        ];
        let mut files = provider.list_files().unwrap();
        files.sort();

        assert_eq!(files, expected_files);
    }

    #[ge_context::test]
    async fn test_loading_info_from_index(ctx: PostgresSessionContext<NoTls>) {
        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        let provider = NetCdfCfDataProvider {
            id: NETCDF_CF_PROVIDER_ID,
            name: "Test Provider".to_string(),
            description: "Test Provider".to_string(),
            data: test_data!("netcdf4d/").to_path_buf(),
            overviews: overview_folder.path().to_path_buf(),
            cache_ttl: Default::default(),
            db: Arc::new(ctx.db()),
        };

        provider
            .create_overviews(Path::new("dataset_sm.nc"), None, NopTaskContext)
            .await
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

        pretty_assertions::assert_eq!(
            metadata.result_descriptor().await.unwrap(),
            RasterResultDescriptor {
                data_type: RasterDataType::I16,
                spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 3035)
                    .into(),
                time: None,
                bbox: None,
                resolution: Some(SpatialResolution::new_unchecked(1000.0, 1000.0)),
                bands: RasterBandDescriptors::new_single_band(),
            }
        );

        let loading_info = metadata
            .loading_info(RasterQueryRectangle {
                spatial_bounds: SpatialPartition2D::new(
                    (43.945_312_5, 0.791_015_625_25).into(),
                    (44.033_203_125, 0.703_125_25).into(),
                )
                .unwrap(),
                time_interval: TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0)).into(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    0.000_343_322_7, // 256 pixel
                    0.000_343_322_7, // 256 pixel
                ),
                attributes: BandSelection::first(),
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
            .join("dataset_sm.nc/scenario_5/metric_2/1/2000-01-01T00:00:00.000Z.tiff");

        assert_eq!(
            loading_info_parts[0],
            GdalLoadingInfoTemporalSlice {
                time: TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0)).into(),
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
                }),
                cache_ttl: CacheTtlSeconds::default(),
            }
        );
    }

    #[ge_context::test]
    async fn test_listing_from_netcdf_sm_from_index(ctx: PostgresSessionContext<NoTls>) {
        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        let provider = Box::new(NetCdfCfDataProviderDefinition {
            name: "NetCdfCfDataProvider".to_string(),
            description: "NetCdfCfProviderDefinition".to_string(),
            priority: Some(-5),
            data: test_data!("netcdf4d").into(),
            overviews: overview_folder.path().to_path_buf(),
            cache_ttl: Default::default(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        provider
            .as_any()
            .downcast_ref::<NetCdfCfDataProvider<PostgresDb<NoTls>>>()
            .unwrap()
            .create_overviews(Path::new("dataset_sm.nc"), None, NopTaskContext)
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

        pretty_assertions::assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: NETCDF_CF_PROVIDER_ID,
                    collection_id: id,
                },
                name: "Test dataset metric and scenario".to_string(),
                description: "Fake description of test dataset with metric and scenario.".to_string(),
                items: vec![CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_sm.nc/scenario_1".to_string())
                    },
                    name: "Sustainability".to_string(),
                    description: "SSP1-RCP2.6" .to_string(),
                    properties: Default::default(),
                }), CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_sm.nc/scenario_2".to_string())
                    },
                    name: "Middle of the Road ".to_string(),
                    description: "SSP2-RCP4.5".to_string(),
                    properties: Default::default(),
                }), CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_sm.nc/scenario_3".to_string())
                    },
                    name: "Regional Rivalry".to_string(),
                    description: "SSP3-RCP6.0".to_string(),
                    properties: Default::default(),
                }), CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                    id: ProviderLayerCollectionId {
                        provider_id: NETCDF_CF_PROVIDER_ID,
                        collection_id: LayerCollectionId("dataset_sm.nc/scenario_4".to_string())
                    },
                    name: "Inequality".to_string(),
                    description: "SSP4-RCP6.0".to_string(),
                    properties: Default::default(),
                }), CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
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

    #[ge_context::test(user = "admin")]
    #[allow(clippy::too_many_lines)]
    async fn test_irregular_time_series(ctx: PostgresSessionContext<NoTls>) {
        let land_cover_dataset_name = add_land_cover_to_datasets(&ctx.db()).await;

        let provider_definition = EbvPortalDataProviderDefinition {
            name: "EBV Portal".to_string(),
            description: "EBV Portal".to_string(),
            priority: Some(-1),
            data: test_data!("netcdf4d/").into(),
            base_url: "https://portal.geobon.org/api/v1".try_into().unwrap(),
            overviews: test_data!("netcdf4d/overviews/").into(),
            cache_ttl: Default::default(),
        };

        ctx.db()
            .add_layer_provider(provider_definition.into())
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
                    output_band: None,
                    map_no_data: false,
                },
                sources: SingleRasterSource {
                    raster: RasterStacker {
                        params: RasterStackerParams {
                            rename_bands: RenameBands::Default,
                        },
                        sources: MultipleRasterSources {
                            rasters: vec![
                                GdalSource {
                                    params: GdalSourceParameters {
                                        data: geoengine_datatypes::dataset::NamedData::with_system_provider(
                                            EBV_PROVIDER_ID.to_string(),
                                            serde_json::json!({
                                                "fileName": "dataset_irr_ts.nc",
                                                "groupNames": ["metric_1"],
                                                "entity": 0
                                            })
                                            .to_string(),
                                        ),
                                    },
                                }
                                .boxed(),
                                RasterTypeConversion {
                                    params: RasterTypeConversionParams {
                                        output_data_type: RasterDataType::I16,
                                    },
                                    sources: SingleRasterSource {
                                        raster: GdalSource {
                                            params: GdalSourceParameters {
                                                data: land_cover_dataset_name.into(),
                                            },
                                        }.boxed(),
                                    }
                                }.boxed(),
                            ],
                         }
                    }.boxed()
                }
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

        let TypedPlotQueryProcessor::JsonVega(processor) =
            initialized_operator.query_processor().unwrap()
        else {
            panic!("wrong plot type");
        };

        let query_context = ctx
            .query_context(Default::default(), Default::default())
            .unwrap();

        let result = processor
            .plot_query(
                PlotQueryRectangle {
                    spatial_bounds: BoundingBox2D::new(
                        (46.478_278_849, 40.584_655_660_000_1).into(),
                        (87.323_796_021_000_1, 55.434_550_273).into(),
                    )
                    .unwrap(),
                    time_interval: TimeInterval::new(
                        DateTime::new_utc(1900, 4, 1, 0, 0, 0),
                        DateTime::new_utc_with_millis(2055, 4, 1, 0, 0, 0, 1),
                    )
                    .unwrap(),
                    spatial_resolution: SpatialResolution::new_unchecked(0.1, 0.1),
                    attributes: PlotSeriesSelection::all(),
                },
                &query_context,
            )
            .await
            .unwrap();

        assert_eq!(result, PlotData {
            vega_string: "{\"$schema\":\"https://vega.github.io/schema/vega-lite/v4.17.0.json\",\"data\":{\"values\":[{\"x\":\"2015-01-01T00:00:00+00:00\",\"y\":46.342800000000004},{\"x\":\"2055-01-01T00:00:00+00:00\",\"y\":43.54399999999997}]},\"description\":\"Area Plot\",\"encoding\":{\"x\":{\"field\":\"x\",\"title\":\"Time\",\"type\":\"temporal\"},\"y\":{\"field\":\"y\",\"title\":\"\",\"type\":\"quantitative\"}},\"mark\":{\"line\":true,\"point\":true,\"type\":\"line\"}}".to_string(),
            metadata: PlotMetaData::None,
        });
    }

    #[ge_context::test]
    async fn it_lists_with_and_without_overviews(ctx: PostgresSessionContext<NoTls>) {
        async fn get_all_collections(
            provider: &dyn DataProvider,
            root_id: LayerCollectionId,
        ) -> (Vec<LayerCollection>, Vec<LayerListing>) {
            let mut layer_collection_ids = vec![root_id];
            let mut all_collections = Vec::new();
            let mut all_layers = Vec::new();

            while let Some(id) = layer_collection_ids.pop() {
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

                for item in &collection.items {
                    match item {
                        CollectionItem::Collection(listing) => {
                            layer_collection_ids.push(listing.id.collection_id.clone());
                        }
                        CollectionItem::Layer(layer) => {
                            all_layers.push(layer.clone());
                        }
                    }
                }

                all_collections.push(collection);
            }

            (all_collections, all_layers)
        }

        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        let provider = Box::new(NetCdfCfDataProviderDefinition {
            name: "NetCdfCfDataProvider".to_string(),
            description: "NetCdfCfProviderDefinition".to_string(),
            priority: None,
            data: test_data!("netcdf4d").into(),
            overviews: overview_folder.path().to_path_buf(),
            cache_ttl: Default::default(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let file_names = vec![
            "dataset_irr_ts.nc",
            "dataset_m.nc",
            "dataset_sm.nc",
            "Biodiversity/dataset_monthly.nc",
            // "Biodiversity/dataset_daily.nc", // TODO: get fewer entities/timestamps
        ];

        for file_name in file_names {
            let root_id = LayerCollectionId(file_name.to_string());

            let (collections_before_overviews, layers_before_overviews) =
                get_all_collections(&*provider, root_id.clone()).await;

            provider
                .as_any()
                .downcast_ref::<NetCdfCfDataProvider<PostgresDb<NoTls>>>()
                .unwrap()
                .create_overviews(Path::new(file_name), None, NopTaskContext)
                .await
                .unwrap();

            let (collections_after_overviews, layers_after_overviews) =
                get_all_collections(&*provider, root_id).await;

            pretty_assertions::assert_eq!(
                collections_before_overviews,
                collections_after_overviews
            );

            pretty_assertions::assert_eq!(layers_before_overviews, layers_after_overviews);
        }
    }

    #[ge_context::test]
    async fn it_loads_with_and_without_overviews(ctx: PostgresSessionContext<NoTls>) {
        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        let provider = Box::new(NetCdfCfDataProviderDefinition {
            name: "NetCdfCfDataProvider".to_string(),
            description: "NetCdfCfProviderDefinition".to_string(),
            priority: None,
            data: test_data!("netcdf4d").into(),
            overviews: overview_folder.path().to_path_buf(),
            cache_ttl: Default::default(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let file_name = "Biodiversity/dataset_monthly.nc";

        let layer_id =
            netcdf_entity_to_layer_id(Path::new(file_name), &["metric_2".to_string()], 2);

        let layer_before_overviews = provider.load_layer(&layer_id).await.unwrap();

        provider
            .as_any()
            .downcast_ref::<NetCdfCfDataProvider<PostgresDb<NoTls>>>()
            .unwrap()
            .create_overviews(Path::new(file_name), None, NopTaskContext)
            .await
            .unwrap();

        let layer_after_overviews = provider.load_layer(&layer_id).await.unwrap();

        // equal layers without `colorizer` and `dataRange` (which are calculated only in the overviews)
        pretty_assertions::assert_eq!(layer_before_overviews.id, layer_after_overviews.id);
        pretty_assertions::assert_eq!(layer_before_overviews.name, layer_after_overviews.name);
        pretty_assertions::assert_eq!(
            layer_before_overviews.description,
            layer_after_overviews.description
        );
        pretty_assertions::assert_eq!(
            layer_before_overviews.workflow,
            layer_after_overviews.workflow
        );
        pretty_assertions::assert_eq!(
            layer_before_overviews.properties,
            layer_after_overviews.properties
        );
        pretty_assertions::assert_eq!(
            layer_before_overviews.metadata["timeSteps"],
            layer_after_overviews.metadata["timeSteps"]
        );
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_refreshes_metadata_only(ctx: PostgresSessionContext<NoTls>) {
        async fn query_collection_name(
            provider: &NetCdfCfDataProvider<PostgresDb<NoTls>>,
            id: &LayerCollectionId,
        ) -> String {
            let collection = provider
                .load_layer_collection(
                    id,
                    LayerCollectionListOptions {
                        offset: 0,
                        limit: 20,
                    },
                )
                .await
                .unwrap();
            collection.name
        }

        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        let provider = Box::new(NetCdfCfDataProviderDefinition {
            name: "NetCdfCfDataProvider".to_string(),
            description: "NetCdfCfProviderDefinition".to_string(),
            priority: None,
            data: test_data!("netcdf4d").into(),
            overviews: overview_folder.path().to_path_buf(),
            cache_ttl: Default::default(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let file_name = "dataset_sm.nc";
        let id = LayerCollectionId(file_name.to_string());

        let provider = provider
            .as_any()
            .downcast_ref::<NetCdfCfDataProvider<PostgresDb<NoTls>>>()
            .unwrap();

        provider
            .create_overviews(Path::new(file_name), None, NopTaskContext)
            .await
            .unwrap();

        assert_eq!(
            query_collection_name(provider, &id).await,
            "Test dataset metric and scenario"
        );

        let layer_id = netcdf_entity_to_layer_id(
            Path::new(file_name),
            &["scenario_2".to_string(), "metric_2".to_string()],
            2,
        );

        assert_eq!(
            provider.load_layer(&layer_id).await.unwrap().metadata["dataRange"],
            "[1.0,98.0]"
        );

        // manipulate a field in the metadata

        let db = ctx.db();

        db.test_execute_with_transaction(
            "UPDATE ebv_provider_overviews SET title = 'MANIPULATED' WHERE file_name = $1",
            &[&file_name],
        )
        .await
        .unwrap();

        db.test_execute_with_transaction(
            "UPDATE ebv_provider_groups SET data_range = ARRAY[5, 23] WHERE file_name = $1",
            &[&file_name],
        )
        .await
        .unwrap();

        assert_eq!(query_collection_name(provider, &id).await, "MANIPULATED");

        assert_eq!(
            provider.load_layer(&layer_id).await.unwrap().metadata["dataRange"],
            "[5.0,23.0]"
        );

        provider
            .refresh_overview_metadata(Path::new(file_name), NopTaskContext)
            .await
            .unwrap();

        // after refresh, the metadata should be correct

        assert_eq!(
            query_collection_name(provider, &id).await,
            "Test dataset metric and scenario"
        );

        assert_eq!(
            provider.load_layer(&layer_id).await.unwrap().metadata["dataRange"],
            "[1.0,98.0]"
        );

        // forcefully remove file to test error handling

        tokio::fs::remove_file(
            overview_folder
                .path()
                .join(file_name)
                .join("scenario_2/metric_2/1/2010-01-01T00:00:00.000Z.tiff"),
        )
        .await
        .unwrap();

        provider
            .refresh_overview_metadata(Path::new(file_name), NopTaskContext)
            .await
            .unwrap_err();
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_handles_esri_in_creation_and_refresh(ctx: PostgresSessionContext<NoTls>) {
        async fn query_collection_name(
            provider: &NetCdfCfDataProvider<PostgresDb<NoTls>>,
            id: &LayerCollectionId,
        ) -> String {
            let collection = provider
                .load_layer_collection(
                    id,
                    LayerCollectionListOptions {
                        offset: 0,
                        limit: 20,
                    },
                )
                .await
                .unwrap();
            collection.name
        }

        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        let provider = Box::new(NetCdfCfDataProviderDefinition {
            name: "NetCdfCfDataProvider".to_string(),
            description: "NetCdfCfProviderDefinition".to_string(),
            priority: None,
            data: test_data!("netcdf4d").into(),
            overviews: overview_folder.path().to_path_buf(),
            cache_ttl: Default::default(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let file_name = "dataset_esri.nc";
        let id = LayerCollectionId(file_name.to_string());

        let provider = provider
            .as_any()
            .downcast_ref::<NetCdfCfDataProvider<PostgresDb<NoTls>>>()
            .unwrap();

        provider
            .create_overviews(Path::new(file_name), None, NopTaskContext)
            .await
            .unwrap();

        assert_eq!(
            query_collection_name(provider, &id).await,
            "Test dataset metric monthly"
        );

        let layer_id =
            netcdf_entity_to_layer_id(Path::new(file_name), &["metric_2".to_string()], 2);

        assert_eq!(
            provider.load_layer(&layer_id).await.unwrap().metadata["dataRange"],
            "[1.0,98.0]"
        );

        // manipulate a field in the metadata

        let db = ctx.db();

        db.test_execute_with_transaction(
            "UPDATE ebv_provider_overviews SET title = 'MANIPULATED' WHERE file_name = $1",
            &[&file_name],
        )
        .await
        .unwrap();

        db.test_execute_with_transaction(
            "UPDATE ebv_provider_groups SET data_range = ARRAY[5, 23] WHERE file_name = $1",
            &[&file_name],
        )
        .await
        .unwrap();

        assert_eq!(query_collection_name(provider, &id).await, "MANIPULATED");

        assert_eq!(
            provider.load_layer(&layer_id).await.unwrap().metadata["dataRange"],
            "[5.0,23.0]"
        );

        provider
            .refresh_overview_metadata(Path::new(file_name), NopTaskContext)
            .await
            .unwrap();

        // after refresh, the metadata should be correct

        assert_eq!(
            query_collection_name(provider, &id).await,
            "Test dataset metric monthly"
        );

        assert_eq!(
            provider.load_layer(&layer_id).await.unwrap().metadata["dataRange"],
            "[1.0,98.0]"
        );

        // forcefully remove file to test error handling

        tokio::fs::remove_file(
            overview_folder
                .path()
                .join(file_name)
                .join("metric_2/1/2000-01-01T00:00:00.000Z.tiff"),
        )
        .await
        .unwrap();

        provider
            .refresh_overview_metadata(Path::new(file_name), NopTaskContext)
            .await
            .unwrap_err();
    }
}
