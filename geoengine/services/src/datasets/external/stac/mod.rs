use crate::contexts::GeoEngineDb;
use crate::datasets::listing::ProvenanceOutput;
use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollection, LayerCollectionListOptions, LayerCollectionListing,
    LayerListing, ProviderLayerCollectionId, ProviderLayerId,
};
use crate::layers::listing::{
    LayerCollectionId, LayerCollectionProvider, ProviderCapabilities, SearchCapabilities,
};
use crate::util::join_base_url_and_path;
use crate::workflows::workflow::Workflow;
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataId, DataProviderId, LayerId, NamedData};
use geoengine_datatypes::operations::reproject::{
    CoordinateProjection, CoordinateProjector, ReprojectClipped,
};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, CacheHint, RasterQueryRectangle, SpatialResolution, TimeDimension,
    TimeInstance, TimeInterval, VectorQueryRectangle,
};
use geoengine_datatypes::raster::{GeoTransform, GridBoundingBox2D, GridIdx2D, RasterDataType};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterBandDescriptors, RasterOperator, RasterResultDescriptor,
    SpatialGridDescriptor, TimeDescriptor, TypedOperator, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{
    FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalLoadingInfo,
    MultiBandGdalLoadingInfo, MultiBandGdalLoadingInfoQueryRectangle, MultiBandGdalSource,
    MultiBandGdalSourceParameters, OgrSourceDataset, TileFile,
};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::str::FromStr;
use url::Url;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "StacDataProviderDefinition")]
#[serde(rename_all = "camelCase")]
pub struct StacDataProviderDefinition {
    pub name: String,
    pub id: DataProviderId,
    pub description: String,
    pub priority: Option<i16>,
    pub api_url: String,
    pub collection_name: String,
    pub s3_config: Option<StacProviderS3Config>,
    pub time_dimension: TimeDimension, // TODO: should this be on dataset level?
    pub datasets: Vec<StacProviderDataset>,
    // TODO: page limit(?)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "StacProviderS3Config")]
pub struct StacProviderS3Config {
    pub endpoint: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

/// A geo engine dataset derived from a STAC collectin.
/// As all bands and tiles of a geo engine data set must have the same data type, resolution and projection,
/// a stac collection will be split into multiple geo engine datasets if it contains bands with different data types, resolutions or projections.
/// In order to make them browsable they are defined as part of the stac provider definition.
///
/// TODO: different approach would be to just provide data type, resolution and projection + bands and compute all combinations as possible datasets,
/// but not all combinations actually exist and would lead to empty collection.
///
/// TODO: could also be gathered from collection api and probeb from items
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "StacProviderDataset")]
pub struct StacProviderDataset {
    pub name: String, // TODO: derive from collection name + data type + resolution + projection?
    pub description: String,
    pub data_type: RasterDataType,
    pub resolution: SpatialResolution,
    pub projection: SpatialReference,
    pub spatial_grid: SpatialGridDescriptor, // TODO: this could be fetched from STAC, however it is dependent on the projection and the STAC collection API does not include this information for all projections but only the first one. so we would have to probe the items API...
    pub bands: Vec<StacProviderDatasetBand>, // bands in order!
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "StacProviderDatasetBand")]
pub struct StacProviderDatasetBand {
    pub name: String,
}

#[async_trait]
impl<D: GeoEngineDb> DataProviderDefinition<D> for StacDataProviderDefinition {
    async fn initialize(self: Box<Self>, _db: D) -> crate::error::Result<Box<dyn DataProvider>> {
        if self.time_dimension == TimeDimension::Irregular {
            return Err(crate::error::Error::StacIrregularTimeDimensionNotSupported);
        }
        Ok(Box::new(StacDataProvider::new(
            self.id,
            self.name,
            self.description,
            self.api_url,
            self.collection_name,
            self.s3_config,
            self.time_dimension,
            self.datasets,
        )))
    }

    fn type_name(&self) -> &'static str {
        "Stac"
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DataProviderId {
        self.id
    }

    fn priority(&self) -> i16 {
        self.priority.unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
pub struct StacDataProvider {
    id: DataProviderId,
    name: String,
    description: String,
    api_url: String,
    collection_name: String,
    s3_config: Option<StacProviderS3Config>,
    time_dimension: TimeDimension,
    datasets: Vec<StacProviderDataset>,
}

impl StacDataProvider {
    pub fn new(
        id: DataProviderId,
        name: String,
        description: String,
        api_url: String,
        collection_name: String,
        s3_config: Option<StacProviderS3Config>,
        time_dimension: TimeDimension,
        datasets: Vec<StacProviderDataset>,
    ) -> Self {
        Self {
            id,
            name,
            description,
            api_url,
            collection_name,
            s3_config,
            time_dimension,
            datasets,
        }
    }
}

const ROOT_COLLECTION_ID: &str = "root";

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum GroupingDimension {
    DataType,
    Resolution,
    Projection,
}

impl GroupingDimension {
    fn id(self) -> &'static str {
        match self {
            Self::DataType => "dataTypes",
            Self::Resolution => "resolutions",
            Self::Projection => "projections",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::DataType => "data type",
            Self::Resolution => "resolution",
            Self::Projection => "projection",
        }
    }

    fn all() -> [Self; 3] {
        [Self::DataType, Self::Resolution, Self::Projection]
    }

    fn from_id(value: &str) -> Option<Self> {
        match value {
            "dataTypes" => Some(Self::DataType),
            "resolutions" => Some(Self::Resolution),
            "projections" => Some(Self::Projection),
            _ => None,
        }
    }
}

impl StacDataProvider {
    fn dataset_dimension_display_value(
        dataset: &StacProviderDataset,
        dimension: GroupingDimension,
    ) -> String {
        match dimension {
            GroupingDimension::DataType => format!("{:?}", dataset.data_type),
            GroupingDimension::Resolution => {
                format!("{}x{}", dataset.resolution.x, dataset.resolution.y)
            }
            GroupingDimension::Projection => dataset.projection.to_string(),
        }
    }

    fn dataset_dimension_slug_value(
        dataset: &StacProviderDataset,
        dimension: GroupingDimension,
    ) -> String {
        match dimension {
            GroupingDimension::DataType => format!("{:?}", dataset.data_type).to_ascii_lowercase(),
            GroupingDimension::Resolution => {
                format!("{}x{}", dataset.resolution.x, dataset.resolution.y)
            }
            GroupingDimension::Projection => dataset
                .projection
                .to_string()
                .to_ascii_lowercase()
                .replace(':', ""),
        }
    }

    fn dataset_matches(
        dataset: &StacProviderDataset,
        filters: &[(GroupingDimension, &str)],
    ) -> bool {
        filters.iter().all(|(dimension, slug)| {
            Self::dataset_dimension_slug_value(dataset, *dimension) == *slug
        })
    }

    fn available_values(
        &self,
        dimension: GroupingDimension,
        filters: &[(GroupingDimension, &str)],
    ) -> Vec<(String, String)> {
        let values_by_slug = self
            .datasets
            .iter()
            .filter(|dataset| Self::dataset_matches(dataset, filters))
            .fold(BTreeMap::new(), |mut acc, dataset| {
                let slug = Self::dataset_dimension_slug_value(dataset, dimension);
                let display = Self::dataset_dimension_display_value(dataset, dimension);
                acc.entry(slug).or_insert(display);
                acc
            });

        values_by_slug.into_iter().collect::<Vec<_>>()
    }

    fn dataset_layer_id(index: usize) -> LayerId {
        LayerId(format!("dataset/{index}"))
    }

    fn dataset_index_from_layer_id(id: &LayerId) -> Option<usize> {
        id.0.strip_prefix("dataset/")
            .and_then(|value| value.parse::<usize>().ok())
    }

    fn dataset_from_data_id(
        &self,
        id: &DataId,
    ) -> Result<&StacProviderDataset, geoengine_operators::error::Error> {
        let external = id
            .external()
            .ok_or(geoengine_operators::error::Error::DataIdTypeMissMatch)?;

        let dataset_index = Self::dataset_index_from_layer_id(&external.layer_id)
            .ok_or(geoengine_operators::error::Error::UnknownDataId)?;

        self.datasets
            .get(dataset_index)
            .ok_or(geoengine_operators::error::Error::UnknownDataId)
    }

    fn paginate(
        items: Vec<CollectionItem>,
        options: LayerCollectionListOptions,
    ) -> Vec<CollectionItem> {
        items
            .into_iter()
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .collect()
    }
}

#[async_trait]
impl LayerCollectionProvider for StacDataProvider {
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
        let collection_path = collection.0.trim_start_matches('/');

        let items =
            if collection_path == ROOT_COLLECTION_ID {
                GroupingDimension::all()
                    .into_iter()
                    .map(|dimension| {
                        CollectionItem::Collection(LayerCollectionListing {
                            r#type: Default::default(),
                            id: ProviderLayerCollectionId {
                                provider_id: self.id,
                                collection_id: LayerCollectionId(dimension.id().to_owned()),
                            },
                            name: format!("By {}", dimension.label()),
                            description: format!("Browse datasets by {}", dimension.label()),
                            properties: vec![],
                        })
                    })
                    .collect::<Vec<_>>()
            } else {
                let parts = collection_path.split('/').collect::<Vec<_>>();

                match parts.as_slice() {
                    [first_dimension_id] => {
                        let first_dimension = GroupingDimension::from_id(first_dimension_id)
                            .ok_or(crate::error::Error::UnknownLayerCollectionId {
                                id: collection.clone(),
                            })?;

                        self.available_values(first_dimension, &[])
                            .into_iter()
                            .map(|(slug, display)| {
                                CollectionItem::Collection(LayerCollectionListing {
                                    r#type: Default::default(),
                                    id: ProviderLayerCollectionId {
                                        provider_id: self.id,
                                        collection_id: LayerCollectionId(format!(
                                            "{}/{slug}",
                                            first_dimension.id(),
                                        )),
                                    },
                                    name: display.clone(),
                                    description: format!(
                                        "Datasets with {} {}",
                                        first_dimension.label(),
                                        display,
                                    ),
                                    properties: vec![],
                                })
                            })
                            .collect::<Vec<_>>()
                    }
                    [first_dimension_id, first_value] => {
                        let first_dimension = GroupingDimension::from_id(first_dimension_id)
                            .ok_or(crate::error::Error::UnknownLayerCollectionId {
                                id: collection.clone(),
                            })?;

                        GroupingDimension::all()
                            .into_iter()
                            .filter(|dimension| *dimension != first_dimension)
                            .filter(|dimension| {
                                !self
                                    .available_values(*dimension, &[(first_dimension, first_value)])
                                    .is_empty()
                            })
                            .map(|dimension| {
                                CollectionItem::Collection(LayerCollectionListing {
                                    r#type: Default::default(),
                                    id: ProviderLayerCollectionId {
                                        provider_id: self.id,
                                        collection_id: LayerCollectionId(format!(
                                            "{}/{first_value}/{}",
                                            first_dimension.id(),
                                            dimension.id()
                                        )),
                                    },
                                    name: format!("By {}", dimension.label()),
                                    description: format!(
                                        "Filter datasets by {} with {} {}",
                                        dimension.label(),
                                        first_dimension.label(),
                                        first_value,
                                    ),
                                    properties: vec![],
                                })
                            })
                            .collect::<Vec<_>>()
                    }
                    [first_dimension_id, first_value, second_dimension_id] => {
                        let first_dimension = GroupingDimension::from_id(first_dimension_id)
                            .ok_or(crate::error::Error::UnknownLayerCollectionId {
                                id: collection.clone(),
                            })?;
                        let second_dimension = GroupingDimension::from_id(second_dimension_id)
                            .ok_or(crate::error::Error::UnknownLayerCollectionId {
                                id: collection.clone(),
                            })?;

                        ensure!(
                            first_dimension != second_dimension,
                            crate::error::UnknownLayerCollectionId {
                                id: collection.clone(),
                            }
                        );

                        self.available_values(second_dimension, &[(first_dimension, first_value)])
                            .into_iter()
                            .map(|(slug, display)| {
                                CollectionItem::Collection(LayerCollectionListing {
                                    r#type: Default::default(),
                                    id: ProviderLayerCollectionId {
                                        provider_id: self.id,
                                        collection_id: LayerCollectionId(format!(
                                            "{}/{first_value}/{}/{slug}",
                                            first_dimension.id(),
                                            second_dimension.id(),
                                        )),
                                    },
                                    name: display.clone(),
                                    description: format!(
                                        "Datasets with {} {} and {} {}",
                                        first_dimension.label(),
                                        first_value,
                                        second_dimension.label(),
                                        display,
                                    ),
                                    properties: vec![],
                                })
                            })
                            .collect::<Vec<_>>()
                    }
                    [
                        first_dimension_id,
                        first_value,
                        second_dimension_id,
                        second_value,
                    ] => {
                        let first_dimension = GroupingDimension::from_id(first_dimension_id)
                            .ok_or(crate::error::Error::UnknownLayerCollectionId {
                                id: collection.clone(),
                            })?;
                        let second_dimension = GroupingDimension::from_id(second_dimension_id)
                            .ok_or(crate::error::Error::UnknownLayerCollectionId {
                                id: collection.clone(),
                            })?;

                        ensure!(
                            first_dimension != second_dimension,
                            crate::error::UnknownLayerCollectionId {
                                id: collection.clone(),
                            }
                        );

                        let mut items = self
                            .datasets
                            .iter()
                            .enumerate()
                            .filter(|(_, dataset)| {
                                Self::dataset_matches(
                                    dataset,
                                    &[
                                        (first_dimension, first_value),
                                        (second_dimension, second_value),
                                    ],
                                )
                            })
                            .map(|(index, dataset)| {
                                CollectionItem::Layer(LayerListing {
                                    r#type: Default::default(),
                                    id: ProviderLayerId {
                                        provider_id: self.id,
                                        layer_id: Self::dataset_layer_id(index),
                                    },
                                    name: dataset.name.clone(),
                                    description: dataset.description.clone(),
                                    properties: vec![],
                                })
                            })
                            .collect::<Vec<_>>();

                        items.sort_by_key(|item| item.name().to_owned());
                        items
                    }
                    _ => {
                        return Err(crate::error::Error::UnknownLayerCollectionId {
                            id: collection.clone(),
                        });
                    }
                }
            };

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: self.id,
                collection_id: collection.clone(),
            },
            name: self.name.clone(),
            description: self.description.clone(),
            items: Self::paginate(items, options),
            entry_label: None,
            properties: vec![],
        })
    }

    async fn get_root_layer_collection_id(&self) -> crate::error::Result<LayerCollectionId> {
        Ok(LayerCollectionId(ROOT_COLLECTION_ID.to_owned()))
    }

    async fn load_layer(&self, id: &LayerId) -> crate::error::Result<Layer> {
        let dataset_index = Self::dataset_index_from_layer_id(id)
            .ok_or(crate::error::Error::UnknownLayerId { id: id.clone() })?;

        let dataset = self
            .datasets
            .get(dataset_index)
            .ok_or(crate::error::Error::UnknownLayerId { id: id.clone() })?;

        Ok(Layer {
            id: ProviderLayerId {
                provider_id: self.id,
                layer_id: id.clone(),
            },
            name: dataset.name.clone(),
            description: dataset.description.clone(),
            workflow: Workflow::Legacy {
                operator: TypedOperator::Raster(
                    MultiBandGdalSource {
                        params: MultiBandGdalSourceParameters::new(NamedData {
                            namespace: None,
                            provider: Some(self.id.to_string()),
                            name: id.to_string(),
                        }),
                    }
                    .boxed(),
                ),
            },
            symbology: None,
            properties: vec![],
            metadata: HashMap::new(),
        })
    }
}

#[derive(Debug, Clone)]
struct StacMultiBandMetaData {
    api_url: String,
    collection_name: String,
    s3_config: Option<StacProviderS3Config>,
    time_dimension: TimeDimension,
    dataset: StacProviderDataset,
}

#[async_trait]
impl
    MetaData<
        MultiBandGdalLoadingInfo,
        RasterResultDescriptor,
        MultiBandGdalLoadingInfoQueryRectangle,
    > for StacMultiBandMetaData
{
    async fn loading_info(
        &self,
        query: MultiBandGdalLoadingInfoQueryRectangle,
    ) -> geoengine_operators::util::Result<MultiBandGdalLoadingInfo> {
        let base_url = Url::from_str(&self.api_url)
            .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?;
        let items_url = join_base_url_and_path(
            &base_url,
            &format!("collections/{}/items", self.collection_name),
        )
        .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?;

        let bbox = stac_query_bbox(
            query.query_rectangle.spatial_bounds(),
            self.dataset.projection,
        )?;
        let time_interval =
            stac_query_time_interval(query.query_rectangle.time_interval(), self.time_dimension)?;
        let time_start = time_interval.start();
        let time_end = time_interval.end();

        let mut query_params = vec![
            (
                "bbox".to_owned(),
                format!(
                    "{},{},{},{}",
                    bbox.lower_left().x,
                    bbox.lower_left().y,
                    bbox.upper_right().x,
                    bbox.upper_right().y
                ),
            ),
            (
                "datetime".to_owned(),
                format!(
                    "{}/{}",
                    time_start
                        .as_date_time()
                        .ok_or(geoengine_operators::error::Error::InvalidDataProviderConfig)?
                        .to_datetime_string_with_millis(),
                    time_end
                        .as_date_time()
                        .ok_or(geoengine_operators::error::Error::InvalidDataProviderConfig)?
                        .to_datetime_string_with_millis(),
                ),
            ),
            ("limit".to_owned(), "100".to_owned()),
            (
                "fields".to_owned(),
                "stac_version,properties.datetime,assets.*.href,assets.*.data_type,assets.*.bands,assets.*.proj:code,assets.*.proj:shape,assets.*.proj:transform".to_owned(),
            ),
        ];

        let client = reqwest::Client::new();
        let mut next_url = Some(items_url);
        let mut use_initial_query = true;

        // TODO: intersect with query bands?

        let selected_bands = self
            .dataset
            .bands
            .iter()
            .enumerate()
            .map(|(idx, band)| (band.name.clone(), idx as u32))
            .collect::<HashMap<_, _>>();

        let mut files = Vec::new();
        let mut time_steps = Vec::new();

        while let Some(url) = next_url {
            let mut request = client.get(url.clone());
            if use_initial_query {
                request = request.query(&query_params);
                use_initial_query = false;
                query_params.clear();
            }

            let item_collection: stac::ItemCollection = request
                .send()
                .await
                .map_err(
                    |e| geoengine_operators::error::Error::QueryingProcessorFailed {
                        source: Box::new(e),
                    },
                )?
                .json()
                .await
                .map_err(
                    |e| geoengine_operators::error::Error::QueryingProcessorFailed {
                        source: Box::new(e),
                    },
                )?;

            for item in item_collection.items {
                if item.version != stac::Version::v1_1_0 {
                    continue;
                }

                let Some(item_datetime) = item.properties.datetime else {
                    continue;
                };

                let time_start = TimeInstance::from_millis(item_datetime.timestamp_millis())
                    .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?;
                let time =
                    TimeInterval::new(time_start, time_start + i64::from(24 * 60 * 60 * 1000))
                        .map_err(|_e| {
                            geoengine_operators::error::Error::InvalidDataProviderConfig
                        })?;

                time_steps.push(time);

                if !query.fetch_tiles {
                    continue;
                }

                for (asset_key, asset) in item.assets {
                    if data_type_from_asset_v1_1_0(&asset) != Some(self.dataset.data_type) {
                        continue;
                    }

                    if !proj_code_matches_dataset(&asset.additional_fields, self.dataset.projection)
                    {
                        continue;
                    }

                    let Some(geo_transform) = geo_transform_from_fields(&asset.additional_fields)
                    else {
                        continue;
                    };

                    let Some((height, width)) = proj_shape_from_fields(&asset.additional_fields)
                    else {
                        continue;
                    };

                    // TODO: approx equals? or no tolerance?
                    if (geo_transform.x_pixel_size().abs() - self.dataset.resolution.x).abs() > 1e-9
                        || (geo_transform.y_pixel_size().abs() - self.dataset.resolution.y).abs()
                            > 1e-9
                    {
                        continue;
                    }

                    let Ok(asset_band_names) = band_names_from_asset_v1_1_0(&asset_key, &asset)
                    else {
                        continue;
                    };

                    let grid_bounds = GridBoundingBox2D::new(
                        GridIdx2D::new([0, 0]),
                        GridIdx2D::new([(width as isize) - 1, (height as isize) - 1]),
                    )
                    .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?;
                    let spatial_partition = geo_transform.grid_to_spatial_bounds(&grid_bounds);

                    let file_path = gdal_file_path(&asset.href)
                        .ok_or(geoengine_operators::error::Error::InvalidDataProviderConfig)?;

                    let gdal_config_options = self.gdal_config_options_for_file_path(&file_path);

                    for (asset_band_idx, asset_band_name) in asset_band_names.iter().enumerate() {
                        let Some(dataset_band_idx) = selected_bands.get(asset_band_name) else {
                            continue;
                        };

                        files.push(TileFile {
                            time,
                            spatial_partition,
                            band: *dataset_band_idx,
                            z_index: 0, // TODO: compute z-index
                            params: GdalDatasetParameters {
                                file_path: file_path.clone(),
                                rasterband_channel: asset_band_idx + 1,
                                geo_transform: GdalDatasetGeoTransform {
                                    origin_coordinate: geo_transform.origin_coordinate(),
                                    x_pixel_size: geo_transform.x_pixel_size(),
                                    y_pixel_size: geo_transform.y_pixel_size(),
                                },
                                width,
                                height,
                                file_not_found_handling: FileNotFoundHandling::Error,
                                no_data_value: None,
                                properties_mapping: None,
                                gdal_open_options: None,
                                gdal_config_options: gdal_config_options.clone(),
                                allow_alphaband_as_mask: false,
                                retry: None,
                            },
                        });
                    }
                }
            }

            next_url = item_collection
                .links
                .into_iter()
                .find(|link| link.rel == "next")
                .and_then(|link| Url::parse(&link.href).ok());
        }

        time_steps.sort_by(|a, b| a.start().cmp(&b.start()).then(a.end().cmp(&b.end())));
        time_steps.dedup();

        if time_steps.is_empty() {
            time_steps.push(query.query_rectangle.time_interval());
        }

        files.sort_by(|a, b| {
            a.time
                .start()
                .cmp(&b.time.start())
                .then(a.time.end().cmp(&b.time.end()))
                .then(a.band.cmp(&b.band))
                .then(a.z_index.cmp(&b.z_index))
        });

        Ok(MultiBandGdalLoadingInfo::new(
            time_steps,
            files,
            CacheHint::default(),
        ))

        // Ok(MultiBandGdalLoadingInfo::new(
        //     vec![query.query_rectangle.time_interval()],
        //     vec![],
        //     CacheHint::default(),
        // ))
    }

    async fn result_descriptor(&self) -> geoengine_operators::util::Result<RasterResultDescriptor> {
        let time = match self.time_dimension {
            TimeDimension::Regular(regular) => {
                TimeDescriptor::new(None, TimeDimension::Regular(regular))
            }
            TimeDimension::Irregular => {
                unreachable!("irregular time dimension rejected at provider initialization")
            }
        };
        Ok(RasterResultDescriptor {
            data_type: self.dataset.data_type,
            spatial_reference: self.dataset.projection.into(),
            time,
            spatial_grid: self.dataset.spatial_grid.clone().into(),
            bands: RasterBandDescriptors::new_multiple_bands(self.dataset.bands.len() as u32),
        })
    }

    fn box_clone(
        &self,
    ) -> Box<
        dyn MetaData<
                MultiBandGdalLoadingInfo,
                RasterResultDescriptor,
                MultiBandGdalLoadingInfoQueryRectangle,
            >,
    > {
        Box::new(self.clone())
    }
}

#[async_trait]
impl DataProvider for StacDataProvider {
    async fn provenance(&self, _id: &DataId) -> crate::error::Result<ProvenanceOutput> {
        todo!("stac provenance blueprint")
    }
}

#[async_trait]
impl
    MetaDataProvider<
        MultiBandGdalLoadingInfo,
        RasterResultDescriptor,
        MultiBandGdalLoadingInfoQueryRectangle,
    > for StacDataProvider
{
    async fn meta_data(
        &self,
        id: &DataId,
    ) -> geoengine_operators::util::Result<
        Box<
            dyn MetaData<
                    MultiBandGdalLoadingInfo,
                    RasterResultDescriptor,
                    MultiBandGdalLoadingInfoQueryRectangle,
                >,
        >,
    > {
        let dataset = self.dataset_from_data_id(id)?;

        Ok(Box::new(StacMultiBandMetaData {
            api_url: self.api_url.clone(),
            collection_name: self.collection_name.clone(),
            s3_config: self.s3_config.clone(),
            time_dimension: self.time_dimension,
            dataset: dataset.clone(),
        }))
    }
}

fn stac_query_bbox(
    spatial_bounds: geoengine_datatypes::primitives::SpatialPartition2D,
    spatial_reference: SpatialReference,
) -> geoengine_operators::util::Result<geoengine_datatypes::primitives::BoundingBox2D> {
    let projector =
        CoordinateProjector::from_known_srs(spatial_reference, SpatialReference::epsg_4326())
            .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?;

    spatial_bounds
        .as_bbox()
        .reproject_clipped(&projector)
        .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?
        .ok_or(geoengine_operators::error::Error::InvalidDataProviderConfig)
}

fn stac_query_time_interval(
    query_time_interval: TimeInterval,
    time_dimension: TimeDimension,
) -> geoengine_operators::util::Result<TimeInterval> {
    match time_dimension {
        TimeDimension::Regular(regular) => {
            let start = regular
                .snap_prev(query_time_interval.start())
                .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?;
            let end = regular
                .snap_next(query_time_interval.end())
                .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?;

            let end = if end <= start {
                (start + regular.step)
                    .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?
            } else {
                end
            };

            TimeInterval::new(start, end)
                .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)
        }
        TimeDimension::Irregular => Ok(query_time_interval),
    }
}

impl StacMultiBandMetaData {
    fn gdal_config_options_for_file_path(
        &self,
        file_path: &PathBuf,
    ) -> Option<Vec<(String, String)>> {
        if !file_path.to_string_lossy().starts_with("/vsis3/") {
            return None;
        }

        self.s3_config.as_ref().map(|config| {
            let mut options = vec![
                ("AWS_S3_ENDPOINT".to_owned(), config.endpoint.clone()),
                ("AWS_VIRTUAL_HOSTING".to_owned(), "FALSE".to_owned()),
            ];

            if let Some(access_key) = &config.access_key {
                options.push(("AWS_ACCESS_KEY_ID".to_owned(), access_key.clone()));
            }

            if let Some(secret_key) = &config.secret_key {
                options.push(("AWS_SECRET_ACCESS_KEY".to_owned(), secret_key.clone()));
            }

            options
        })
    }
}

fn gdal_file_path(href: &str) -> Option<PathBuf> {
    if href.starts_with("http") {
        return Some(PathBuf::from(format!("/vsicurl/{href}")));
    }

    href.strip_prefix("s3://")
        .map(|s3_path| PathBuf::from(format!("/vsis3/{s3_path}")))
}

fn proj_shape_from_fields(
    fields: &serde_json::Map<String, serde_json::Value>,
) -> Option<(usize, usize)> {
    let proj_shape = fields.get("proj:shape")?.as_array()?;
    if proj_shape.len() != 2 {
        return None;
    }

    let height = proj_shape.first()?.as_u64()? as usize;
    let width = proj_shape.get(1)?.as_u64()? as usize;

    Some((height, width))
}

fn geo_transform_from_fields(
    fields: &serde_json::Map<String, serde_json::Value>,
) -> Option<GeoTransform> {
    let proj_transform = fields.get("proj:transform")?;
    let proj_transform_array = proj_transform.as_array()?;
    if proj_transform_array.len() != 6 {
        return None;
    }

    let proj_transform_values = proj_transform_array
        .iter()
        .map(serde_json::Value::as_f64)
        .collect::<Option<Vec<_>>>()?;

    let gdal_geotransform = [
        proj_transform_values[2],
        proj_transform_values[0],
        proj_transform_values[1],
        proj_transform_values[5],
        proj_transform_values[3],
        proj_transform_values[4],
    ];

    Some(gdal_geotransform.into())
}

fn data_type_from_asset_v1_1_0(asset: &stac::Asset) -> Option<RasterDataType> {
    asset
        .data_type
        .as_ref()
        .and_then(raster_data_type_from_stac_data_type)
}

fn raster_data_type_from_stac_data_type(
    data_type: &stac_extensions::raster::DataType,
) -> Option<RasterDataType> {
    match data_type {
        stac_extensions::raster::DataType::UInt8 => Some(RasterDataType::U8),
        stac_extensions::raster::DataType::UInt16 => Some(RasterDataType::U16),
        stac_extensions::raster::DataType::UInt32 => Some(RasterDataType::U32),
        stac_extensions::raster::DataType::Int16 => Some(RasterDataType::I16),
        stac_extensions::raster::DataType::Int32 => Some(RasterDataType::I32),
        stac_extensions::raster::DataType::Float32 => Some(RasterDataType::F32),
        stac_extensions::raster::DataType::Float64 => Some(RasterDataType::F64),
        _ => None,
    }
}

fn band_names_from_asset_v1_1_0(asset_key: &str, asset: &stac::Asset) -> Result<Vec<String>, ()> {
    if asset.bands.is_empty() {
        return Err(());
    }

    let prefix = if asset.bands.len() > 1 {
        format!("{asset_key}_")
    } else {
        String::new()
    };

    let mut names = Vec::new();
    for band in &asset.bands {
        let Some(band_name) = &band.name else {
            return Err(());
        };
        names.push(format!("{}{}", prefix, band_name));
    }

    Ok(names)
}

fn proj_code_matches_dataset(
    fields: &serde_json::Map<String, serde_json::Value>,
    dataset_projection: SpatialReference,
) -> bool {
    let Some(code) = fields.get("proj:code") else {
        return false;
    };

    let Some(proj_code) = proj_code_as_srs_string(code) else {
        return false;
    };

    proj_code == dataset_projection.to_string()
}

fn proj_code_as_srs_string(value: &serde_json::Value) -> Option<String> {
    if let Some(code_number) = value.as_u64() {
        return Some(format!("EPSG:{code_number}"));
    }

    let code_str = value.as_str()?.trim();
    if code_str.contains(':') {
        return Some(code_str.to_ascii_uppercase());
    }

    if let Ok(code_number) = code_str.parse::<u32>() {
        return Some(format!("EPSG:{code_number}"));
    }

    None
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for StacDataProvider
{
    async fn meta_data(
        &self,
        _id: &DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        todo!("stac raster metadata blueprint")
    }
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for StacDataProvider
{
    async fn meta_data(
        &self,
        _id: &DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        todo!("stac vector metadata blueprint")
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for StacDataProvider
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
        todo!("stac mock vector metadata blueprint")
    }
}

#[cfg(test)]
mod tests {
    // use super::*;
    // use crate::{
    //     api::model::{
    //         datatypes::{GeoTransform, GridBoundingBox2D, GridIdx2D, SpatialGridDefinition},
    //         operators::{SpatialGridDescriptor, SpatialGridDescriptorState},
    //     },
    //     layers::layer::CollectionItem,
    // };
    // use std::str::FromStr;

    // fn test_provider() -> StacDataProvider {
    //     let provider_id = DataProviderId::from_str("2e2a303d-d0fd-4f4e-996f-0f34e1fe24f7")
    //         .expect("valid provider id");

    //     let epsg_4326 = SpatialReference::from_str("EPSG:4326").expect("valid srs");
    //     let epsg_3857 = SpatialReference::from_str("EPSG:3857").expect("valid srs");

    //     let datasets = vec![
    //         StacProviderDataset {
    //             name: "u8-10m-4326".to_owned(),
    //             description: "U8 at 10m in EPSG:4326".to_owned(),
    //             data_type: RasterDataType::U8,
    //             resolution: SpatialResolution::new_unchecked(10., 10.),
    //             projection: epsg_4326,
    //             spatial_grid: SpatialGridDescriptor {
    //                 spatial_grid: SpatialGridDefinition {
    //                     geo_transform: GeoTransform {
    //                         origin_coordinate: todo!(),
    //                         x_pixel_size: todo!(),
    //                         y_pixel_size: todo!(),
    //                     },
    //                     grid_bounds: GridBoundingBox2D {
    //                         top_left_idx: GridIdx2D { x_idx: 0, y_idx: 0 },
    //                         bottom_right_idx: GridIdx2D { x_idx: 1, y_idx: 1 }, // TODO, but will be overridden when adding tiles anyway
    //                     }, // TODO from  query bbox and asset proj:shape??
    //                 },
    //                 descriptor: SpatialGridDescriptorState::Source,
    //             },
    //             bands: vec![StacProviderDatasetBand {
    //                 name: "B1".to_owned(),
    //             }],
    //         },
    //         StacProviderDataset {
    //             name: "u8-10m-3857".to_owned(),
    //             description: "U8 at 10m in EPSG:3857".to_owned(),
    //             data_type: RasterDataType::U8,
    //             resolution: SpatialResolution::new_unchecked(10., 10.),
    //             projection: epsg_3857,
    //             bands: vec![StacProviderDatasetBand {
    //                 name: "B2".to_owned(),
    //             }],
    //         },
    //         StacProviderDataset {
    //             name: "u16-20m-4326".to_owned(),
    //             description: "U16 at 20m in EPSG:4326".to_owned(),
    //             data_type: RasterDataType::U16,
    //             resolution: SpatialResolution::new_unchecked(20., 20.),
    //             projection: epsg_4326,
    //             bands: vec![StacProviderDatasetBand {
    //                 name: "B3".to_owned(),
    //             }],
    //         },
    //     ];

    //     StacDataProvider::new(
    //         provider_id,
    //         "STAC test provider".to_owned(),
    //         "Provider for testing collection traversal".to_owned(),
    //         datasets,
    //     )
    // }

    // fn list_options() -> LayerCollectionListOptions {
    //     LayerCollectionListOptions {
    //         offset: 0,
    //         limit: 100,
    //     }
    // }

    // #[tokio::test]
    // async fn lists_root_and_dimension_collections() {
    //     let provider = test_provider();
    //     let root = provider
    //         .get_root_layer_collection_id()
    //         .await
    //         .expect("root id must be available");

    //     let collection = provider
    //         .load_layer_collection(&root, list_options())
    //         .await
    //         .expect("root collection must load");

    //     assert_eq!(collection.items.len(), 3);

    //     let names = collection
    //         .items
    //         .iter()
    //         .map(|item| item.name().to_owned())
    //         .collect::<Vec<_>>();

    //     assert!(names.contains(&"By data type".to_owned()));
    //     assert!(names.contains(&"By resolution".to_owned()));
    //     assert!(names.contains(&"By projection".to_owned()));
    // }

    // #[tokio::test]
    // async fn traverses_to_layers_with_existing_combinations_only() {
    //     let provider = test_provider();

    //     let by_data_type = provider
    //         .load_layer_collection(&LayerCollectionId("dataTypes".to_owned()), list_options())
    //         .await
    //         .expect("first grouping must load");

    //     let u8_collection_id = by_data_type
    //         .items
    //         .into_iter()
    //         .find_map(|item| match item {
    //             CollectionItem::Collection(collection)
    //                 if collection.id.collection_id.0 == "dataTypes/u8" =>
    //             {
    //                 Some(collection.id.collection_id)
    //             }
    //             _ => None,
    //         })
    //         .expect("U8 selection must exist");

    //     let next_groupings = provider
    //         .load_layer_collection(&u8_collection_id, list_options())
    //         .await
    //         .expect("second-level groupings must load");

    //     let by_resolution_collection_id = next_groupings
    //         .items
    //         .into_iter()
    //         .find_map(|item| match item {
    //             CollectionItem::Collection(collection)
    //                 if collection.id.collection_id.0.ends_with("/resolutions") =>
    //             {
    //                 Some(collection.id.collection_id)
    //             }
    //             _ => None,
    //         })
    //         .expect("resolution grouping must exist");

    //     let resolution_values = provider
    //         .load_layer_collection(&by_resolution_collection_id, list_options())
    //         .await
    //         .expect("resolution values must load");

    //     assert_eq!(resolution_values.items.len(), 1);

    //     let selected_resolution = resolution_values
    //         .items
    //         .into_iter()
    //         .find_map(|item| match item {
    //             CollectionItem::Collection(collection) => Some(collection.id.collection_id),
    //             _ => None,
    //         })
    //         .expect("resolution selection must exist");

    //     let layers = provider
    //         .load_layer_collection(&selected_resolution, list_options())
    //         .await
    //         .expect("final layer collection must load");

    //     assert_eq!(layers.items.len(), 2);

    //     let mut layer_ids = layers
    //         .items
    //         .into_iter()
    //         .filter_map(|item| match item {
    //             CollectionItem::Layer(layer) => Some(layer.id.layer_id),
    //             _ => None,
    //         })
    //         .collect::<Vec<_>>();
    //     layer_ids.sort_by(|a, b| a.0.cmp(&b.0));

    //     let loaded_layer = provider
    //         .load_layer(&layer_ids[0])
    //         .await
    //         .expect("layer id from listing must be loadable");

    //     assert!(
    //         loaded_layer.name == "u8-10m-3857" || loaded_layer.name == "u8-10m-4326",
    //         "loaded layer must match one of the filtered datasets"
    //     );
    // }
}
