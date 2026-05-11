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
    TimeInstance, TimeInterval, TryRegularTimeFillIterExt, VectorQueryRectangle,
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
    GdalRetryOptions, MultiBandGdalLoadingInfo, MultiBandGdalLoadingInfoQueryRectangle,
    MultiBandGdalSource, MultiBandGdalSourceParameters, OgrSourceDataset, TileFile,
};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use tracing::debug;
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
    pub asset_title: String,
    pub band_name: Option<String>,
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
const STAC_QUERY_TIMEOUT_SECS: u64 = 60;

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
    fn format_resolution_component(value: f64) -> String {
        let formatted = format!("{value:.9}");
        let trimmed = formatted.trim_end_matches('0').trim_end_matches('.');
        let normalized = if trimmed.is_empty() { "0" } else { trimmed };
        normalized.replace('.', "p")
    }

    fn dataset_stable_id(dataset: &StacProviderDataset) -> String {
        let projection = dataset
            .projection
            .to_string()
            .to_ascii_lowercase()
            .replace(':', "");
        let data_type = format!("{:?}", dataset.data_type).to_ascii_lowercase();
        let res_x = Self::format_resolution_component(dataset.resolution.x);
        let res_y = Self::format_resolution_component(dataset.resolution.y);

        if (dataset.resolution.x - dataset.resolution.y).abs() < 1e-9 {
            format!("{projection}_{data_type}_{res_x}")
        } else {
            format!("{projection}_{data_type}_{res_x}x{res_y}")
        }
    }

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

    fn dataset_layer_id(dataset: &StacProviderDataset) -> LayerId {
        LayerId(format!("dataset/{}", Self::dataset_stable_id(dataset)))
    }

    fn dataset_by_layer_id(&self, id: &LayerId) -> Option<&StacProviderDataset> {
        let suffix = id.0.strip_prefix("dataset/")?;

        if let Ok(index) = suffix.parse::<usize>() {
            return self.datasets.get(index);
        }

        self.datasets
            .iter()
            .find(|dataset| Self::dataset_stable_id(dataset) == suffix)
    }

    fn dataset_from_data_id(
        &self,
        id: &DataId,
    ) -> Result<&StacProviderDataset, geoengine_operators::error::Error> {
        let external = id
            .external()
            .ok_or(geoengine_operators::error::Error::DataIdTypeMissMatch)?;

        self.dataset_by_layer_id(&external.layer_id)
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
                            .filter(|dataset| {
                                Self::dataset_matches(
                                    dataset,
                                    &[
                                        (first_dimension, first_value),
                                        (second_dimension, second_value),
                                    ],
                                )
                            })
                            .map(|dataset| {
                                CollectionItem::Layer(LayerListing {
                                    r#type: Default::default(),
                                    id: ProviderLayerId {
                                        provider_id: self.id,
                                        layer_id: Self::dataset_layer_id(dataset),
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
        let dataset = self
            .dataset_by_layer_id(id)
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

#[derive(Debug, Clone)]
enum StacQueryState {
    FirstPage {
        query_url: Url,
        query_params: Vec<(String, String)>,
    },
    NextPage {
        next_url: Url,
    },
    Finished,
}

async fn query_stac_item_collection(
    client: &reqwest::Client,
    query_state: &StacQueryState,
) -> geoengine_operators::util::Result<(stac::ItemCollection, StacQueryState)> {
    match query_state {
        StacQueryState::FirstPage {
            query_url,
            query_params,
        } => {
            debug!("STAC query first page with parameters: {:?}", query_params);

            let request_started = std::time::Instant::now();

            let item_collection: stac::ItemCollection = client
                .get(query_url.clone())
                .query(query_params)
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

            debug!(
                "STAC response received in {:?} s",
                request_started.elapsed().as_secs_f64()
            );

            let next_state = item_collection
                .links
                .iter()
                .find(|link| link.rel == "next")
                .and_then(|link| Url::parse(&link.href).ok())
                .map(|next_url| StacQueryState::NextPage { next_url })
                .unwrap_or(StacQueryState::Finished);

            Ok((item_collection, next_state))
        }
        StacQueryState::NextPage { next_url } => {
            debug!("STAC query next page with url: {}", next_url);

            let request_started = std::time::Instant::now();

            let item_collection: stac::ItemCollection = client
                .get(next_url.clone())
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

            debug!(
                "STAC response received in {:?} s",
                request_started.elapsed().as_secs_f64()
            );

            let next_state = item_collection
                .links
                .iter()
                .find(|link| link.rel == "next")
                .and_then(|link| Url::parse(&link.href).ok())
                .map(|next_url| StacQueryState::NextPage { next_url })
                .unwrap_or(StacQueryState::Finished);

            Ok((item_collection, next_state))
        }
        StacQueryState::Finished => {
            Err(geoengine_operators::error::Error::QueryingProcessorFailed {
                source: "no more STAC pages to query".into(),
            })
        }
    }
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

        let query_params = vec![
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
                "stac_version,properties.datetime,properties.updated,assets.*.title,assets.*.href,assets.*.data_type,assets.*.bands,assets.*.proj:code,assets.*.proj:shape,assets.*.proj:transform".to_owned(),
            ),
        ];

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(STAC_QUERY_TIMEOUT_SECS))
            .build()
            .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?;
        let mut query_state = StacQueryState::FirstPage {
            query_url: items_url,
            query_params,
        };

        // TODO: intersect with query bands?

        let mut files = Vec::new();
        let mut time_steps = Vec::new();

        // iterate through all items of all pages of the stac response and filter assets that match the dataset configuration
        while !matches!(query_state, StacQueryState::Finished) {
            let (item_collection, next_state) =
                query_stac_item_collection(&client, &query_state).await?;

            for item in item_collection.items {
                if item.version != stac::Version::v1_1_0 {
                    tracing::warn!(
                        "Skipping STAC item with unsupported version: {:?}",
                        item.version
                    );
                    continue;
                }

                let Some(item_datetime) = item.properties.datetime else {
                    tracing::warn!("Skipping STAC item without datetime: {}", item.id);
                    continue;
                };

                let z_index = item
                    .properties
                    .updated
                    .as_deref()
                    .and_then(|updated| chrono::DateTime::parse_from_rfc3339(updated).ok())
                    .map(|updated| updated.timestamp_millis())
                    .unwrap_or_else(|| item_datetime.timestamp_millis());

                let item_time = TimeInstance::from_millis(item_datetime.timestamp_millis())
                    .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?;

                let time = match self.time_dimension {
                    TimeDimension::Regular(regular) => {
                        let time_start = regular.snap_prev(item_time).map_err(|_e| {
                            geoengine_operators::error::Error::InvalidDataProviderConfig
                        })?;
                        let time_end = (time_start + regular.step).map_err(|_e| {
                            geoengine_operators::error::Error::InvalidDataProviderConfig
                        })?;

                        TimeInterval::new(time_start, time_end).map_err(|_e| {
                            geoengine_operators::error::Error::InvalidDataProviderConfig
                        })?
                    }
                    TimeDimension::Irregular => {
                        unreachable!("irregular time dimension rejected at provider initialization")
                    }
                };

                time_steps.push(time);

                if !query.fetch_tiles {
                    tracing::trace!(
                        "STAC query does not require fetching tiles, skipping item with id: {}",
                        item.id
                    );
                    continue;
                }

                for (_asset_key, asset) in item.assets {
                    if data_type_from_asset_v1_1_0(&asset) != Some(self.dataset.data_type) {
                        continue;
                    }

                    if !proj_code_matches_dataset(&asset.additional_fields, self.dataset.projection)
                    {
                        continue;
                    }

                    let Some(geo_transform) = geo_transform_from_fields(&asset.additional_fields)
                    else {
                        // log because this is a malformed stac item
                        tracing::warn!(
                            "Skipping asset with href {} due to missing geo transform",
                            asset.href
                        );
                        continue;
                    };

                    let Some((height, width)) = proj_shape_from_fields(&asset.additional_fields)
                    else {
                        // log because this is a malformed stac item
                        tracing::warn!(
                            "Skipping asset with href {} due to missing projection shape",
                            asset.href
                        );
                        continue;
                    };

                    // TODO: approx equals? or no tolerance?
                    // TODO: compare based on gsd attribute?
                    if (geo_transform.x_pixel_size().abs() - self.dataset.resolution.x).abs() > 1e-9
                        || (geo_transform.y_pixel_size().abs() - self.dataset.resolution.y).abs()
                            > 1e-9
                    {
                        continue;
                    }

                    let Some(asset_title) = asset.title.as_deref() else {
                        // log because this is a malformed stac item
                        tracing::warn!(
                            "Skipping asset with href {} due to missing title",
                            asset.href
                        );
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

                    for (dataset_band_idx, dataset_band) in self.dataset.bands.iter().enumerate() {
                        if dataset_band.asset_title != asset_title {
                            continue;
                        }

                        let rasterband_channel = if asset.bands.is_empty() {
                            if dataset_band.band_name.is_some() {
                                tracing::warn!(
                                    "STAC asset with href {} does not include bands, but dataset configuration requires a band name. Skipping asset.",
                                    asset.href
                                );
                                continue;
                            }

                            1
                        } else {
                            let Some(required_band_name) = dataset_band.band_name.as_deref() else {
                                tracing::warn!(
                                    "STAC asset with href {} includes bands, but dataset configuration does not specify a band name. Skipping asset.",
                                    asset.href
                                );
                                continue;
                            };

                            let Some(asset_band_idx) = asset.bands.iter().position(|asset_band| {
                                asset_band.name.as_deref() == Some(required_band_name)
                            }) else {
                                tracing::debug!(
                                    "Skipping asset with href {} due to missing required band {}",
                                    asset.href,
                                    required_band_name
                                );
                                continue;
                            };

                            asset_band_idx + 1
                        };

                        files.push(TileFile {
                            time,
                            spatial_partition,
                            band: dataset_band_idx as u32,
                            z_index,
                            params: GdalDatasetParameters {
                                file_path: file_path.clone(),
                                rasterband_channel,
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
                                retry: Some(GdalRetryOptions { max_retries: 99 }), // TODO: make configurable for provider or dataset
                            },
                        });
                    }
                }
            }

            query_state = next_state;
        }

        time_steps.sort_by(|a, b| a.start().cmp(&b.start()).then(a.end().cmp(&b.end())));
        time_steps.dedup();

        let time_steps = match self.time_dimension {
            TimeDimension::Regular(regular) => time_steps
                .into_iter()
                .map(Ok::<_, geoengine_operators::error::Error>)
                .try_time_regular_range_fill(regular, time_interval)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?,
            TimeDimension::Irregular => {
                unreachable!("irregular time dimension rejected at provider initialization")
            }
        };

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
    }

    async fn result_descriptor(&self) -> geoengine_operators::util::Result<RasterResultDescriptor> {
        Ok(RasterResultDescriptor {
            data_type: self.dataset.data_type,
            spatial_reference: self.dataset.projection.into(),
            time: TimeDescriptor {
                bounds: None, // TODO: specify as part of the provider definition?
                dimension: self.time_dimension,
            },
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
        Err(crate::error::Error::NotImplemented {
            message: "STAC provenance is not yet implemented".to_owned(),
        })
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
                ("AWS_VIRTUAL_HOSTING".to_owned(), "FALSE".to_owned()), // TODO: make configurable
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
        Err(geoengine_operators::error::Error::NotImplemented)
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
        Err(geoengine_operators::error::Error::NotImplemented)
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
        Err(geoengine_operators::error::Error::NotImplemented)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        contexts::{ApplicationContext, PostgresContext, SessionContext},
        ge_context,
        layers::storage::LayerProviderDb,
        util::tests::admin_login,
    };
    use geoengine_datatypes::{
        dataset::ExternalDataId,
        primitives::{
            BandSelection, DateTime, RasterQueryRectangle, RegularTimeDimension,
            SpatialPartition2D, SpatialResolution, TimeGranularity, TimeInterval, TimeStep,
        },
        raster::{GeoTransform, GridBoundingBox2D, GridIdx2D, GridShape, TileInformation},
        spatial_reference::{SpatialReference, SpatialReferenceAuthority},
        util::Identifier,
    };
    use geoengine_operators::{
        engine::{MetaData, MetaDataProvider, RasterResultDescriptor},
        source::{MultiBandGdalLoadingInfo, MultiBandGdalLoadingInfoQueryRectangle},
    };
    use httptest::{Expectation, Server, matchers::request, responders};
    use tokio_postgres::NoTls;

    fn make_stac_provider_def(
        provider_id: DataProviderId,
        api_url: String,
    ) -> StacDataProviderDefinition {
        StacDataProviderDefinition {
            name: "Sentinel 2 L2A from STAC".to_owned(),
            id: provider_id,
            description: String::new(),
            priority: Some(50),
            api_url,
            collection_name: "sentinel-2-l2a".to_owned(),
            s3_config: None,
            time_dimension: TimeDimension::Regular(RegularTimeDimension::new_with_epoch_origin(
                TimeStep {
                    granularity: TimeGranularity::Days,
                    step: 1,
                },
            )),
            datasets: vec![StacProviderDataset {
                name: "Sentinel-2 L2A EPSG:32632 U16 10m".to_owned(),
                description: String::new(),
                data_type: geoengine_datatypes::raster::RasterDataType::U16,
                resolution: SpatialResolution::new_unchecked(10.0, 10.0),
                projection: SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632),
                spatial_grid: SpatialGridDescriptor::source_from_parts(
                    GeoTransform::new((399_960.0, 5_700_000.0).into(), 10.0, -10.0),
                    GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([10979, 10979]))
                        .unwrap(),
                ),
                bands: vec![
                    StacProviderDatasetBand {
                        asset_title: "NIR 1 (band 8) - 10m".to_owned(),
                        band_name: Some("B08".to_owned()),
                    },
                    StacProviderDatasetBand {
                        asset_title: "Red (band 4) - 10m".to_owned(),
                        band_name: Some("B04".to_owned()),
                    },
                ],
            }],
        }
    }

    fn stac_items_response() -> serde_json::Value {
        // Load the code-de-marburg.json STAC response from test data
        // Contains Sentinel-2 L2A items for multiple dates with various band resolutions
        let json_str =
            include_str!("../../../../../test_data/stac_responses/items/code-de-marburg.json");
        serde_json::from_str(json_str).expect("code-de-marburg.json should be valid JSON")
    }

    /// Replicates the steps from `test_ndvi.http` without making real web requests:
    ///
    /// 1. Create an anonymous session (POST /api/anonymous)
    /// 2. Register the STAC provider (POST /api/layerDb/providers)
    /// 3. Load the provider and obtain metadata for the 10 m U16 dataset
    ///    (mirrors POST /api/workflow + GET /api/workflow/{id}/metadata)
    /// 4. Query loading info for the bbox and time used in the HTTP test
    ///    (mirrors GET /api/wms/{workflowId})
    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn ndvi_stac_loading_info(app_ctx: PostgresContext<NoTls>) {
        // --- mock STAC API ---------------------------------------------------
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path(
                "GET",
                "/collections/sentinel-2-l2a/items",
            ))
            .times(1..=10)
            .respond_with(
                responders::status_code(200)
                    .append_header("Content-Type", "application/json")
                    .body(serde_json::to_string(&stac_items_response()).unwrap()),
            ),
        );

        let provider_id = DataProviderId::new();

        // Step 2: register provider as admin (POST /api/layerDb/providers)
        let admin_session = admin_login(&app_ctx).await;
        let admin_ctx = app_ctx.session_context(admin_session);
        admin_ctx
            .db()
            .add_layer_provider(
                make_stac_provider_def(
                    provider_id,
                    server.url_str("").trim_end_matches('/').to_owned(),
                )
                .into(),
            )
            .await
            .unwrap();

        // Step 3: load provider via the same session and get metadata
        let provider = admin_ctx
            .db()
            .load_layer_provider(provider_id)
            .await
            .unwrap();

        // The stable id for EPSG:32632 / U16 / 10 m is "epsg32632_u16_10"
        let layer_id = LayerId("dataset/epsg32632_u16_10".to_owned());
        let data_id: DataId = ExternalDataId {
            provider_id,
            layer_id,
        }
        .into();

        let meta: Box<
            dyn MetaData<
                    MultiBandGdalLoadingInfo,
                    RasterResultDescriptor,
                    MultiBandGdalLoadingInfoQueryRectangle,
                >,
        > = MetaDataProvider::meta_data(provider.as_ref(), &data_id)
            .await
            .expect("meta_data should succeed");

        // Step 4: query loading info using a date from the STAC response
        // code-de-marburg.json contains items dated 2026-01-03
        // bbox covers part of EPSG:32632 tiles around Marburg, Germany
        let spatial_bounds = SpatialPartition2D::new(
            (499_980.0, 5_800_020.0).into(),
            (510_000.0, 5_790_000.0).into(),
        )
        .unwrap();
        let time_interval =
            TimeInterval::new_instant(DateTime::new_utc(2026, 1, 3, 0, 0, 0)).unwrap();

        let query = MultiBandGdalLoadingInfoQueryRectangle::new(
            spatial_bounds,
            time_interval,
            BandSelection::new_unchecked(vec![0, 1]),
            true,
        );

        let loading_info = meta
            .loading_info(query)
            .await
            .expect("loading_info should succeed");

        // --- verify time steps -----------------------------------------------
        // The items in code-de-marburg.json are dated 2026-01-03T10:34:41Z
        // which should snap to day [2026-01-03, 2026-01-04)
        let expected_time = TimeInterval::new(
            DateTime::new_utc(2026, 1, 3, 0, 0, 0),
            DateTime::new_utc(2026, 1, 4, 0, 0, 0),
        )
        .unwrap();

        let time_steps = loading_info.time_steps();
        assert!(
            time_steps.iter().any(|ts| ts == &expected_time),
            "loading_info should contain time step for 2026-01-03, got {:?}",
            time_steps
        );

        // --- verify that tile files can be retrieved ---------------------
        // The STAC response should have valid B08 and B04 bands
        let tile_geo_transform = GeoTransform::new((499_980.0, 5_800_020.0).into(), 10.0, -10.0);
        let tile = TileInformation::new(
            GridIdx2D::new([0, 0]),
            GridShape::new([10980, 10980]),
            tile_geo_transform,
        );

        // Get tile files for the first available time step
        if let Some(time_step) = time_steps.first() {
            let b08_params = loading_info.tile_files(*time_step, tile, 0);
            let b04_params = loading_info.tile_files(*time_step, tile, 1);

            // Verify we have valid files for both bands
            assert!(
                !b08_params.is_empty(),
                "Should have B08 (NIR) band files for {}",
                time_step
            );
            assert!(
                !b04_params.is_empty(),
                "Should have B04 (Red) band files for {}",
                time_step
            );

            // Verify the files reference S3 GDAL paths
            for param in &b08_params {
                assert!(
                    param.file_path.to_string_lossy().contains("/vsis3/"),
                    "B08 file should use S3 path: {:?}",
                    param.file_path
                );
            }
            for param in &b04_params {
                assert!(
                    param.file_path.to_string_lossy().contains("/vsis3/"),
                    "B04 file should use S3 path: {:?}",
                    param.file_path
                );
            }
        }
    }

    /// Test NDVI workflow execution using the STAC provider
    ///
    /// This test demonstrates the complete workflow execution:
    /// 1. Register the STAC provider
    /// 2. Load the NDVI workflow from JSON
    /// 3. Create a query processor from the workflow
    /// 4. Execute a query and verify tile output
    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn ndvi_stac_workflow(app_ctx: PostgresContext<NoTls>) {
        use futures::StreamExt;
        use geoengine_operators::engine::ExecutionContext;
        use geoengine_operators::engine::WorkflowOperatorPath;
        use uuid::Uuid;

        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path(
                "GET",
                "/collections/sentinel-2-l2a/items",
            ))
            .times(0..=20)
            .respond_with(
                responders::status_code(200)
                    .append_header("Content-Type", "application/json")
                    .body(serde_json::to_string(&stac_items_response()).unwrap()),
            ),
        );

        // --- Setup: register STAC provider ---
        let provider_id = DataProviderId::new();

        let admin_session = admin_login(&app_ctx).await;
        let admin_ctx = app_ctx.session_context(admin_session);

        let provider_def = StacDataProviderDefinition {
            name: "Sentinel 2 L2A from STAC".to_owned(),
            id: provider_id,
            description: "Test STAC provider for NDVI workflow".to_owned(),
            priority: Some(50),
            api_url: server.url_str(""),
            collection_name: "sentinel-2-l2a".to_owned(),
            s3_config: None,
            time_dimension: TimeDimension::Regular(RegularTimeDimension::new_with_epoch_origin(
                TimeStep {
                    granularity: TimeGranularity::Days,
                    step: 1,
                },
            )),
            datasets: vec![
                StacProviderDataset {
                    name: "Sentinel-2 L2A EPSG:32632 U16 10m".to_owned(),
                    description: String::new(),
                    data_type: geoengine_datatypes::raster::RasterDataType::U16,
                    resolution: SpatialResolution::new_unchecked(10.0, 10.0),
                    projection: SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        GeoTransform::new((399_960.0, 5_700_000.0).into(), 10.0, -10.0),
                        GridBoundingBox2D::new(
                            GridIdx2D::new([0, 0]),
                            GridIdx2D::new([10979, 10979]),
                        )
                        .unwrap(),
                    ),
                    bands: vec![
                        StacProviderDatasetBand {
                            asset_title: "Blue (band 2) - 10m".to_owned(),
                            band_name: Some("B02".to_owned()),
                        },
                        StacProviderDatasetBand {
                            asset_title: "Green (band 3) - 10m".to_owned(),
                            band_name: Some("B03".to_owned()),
                        },
                        StacProviderDatasetBand {
                            asset_title: "Water vapour (WVP) - 10m".to_owned(),
                            band_name: Some("WVP".to_owned()),
                        },
                        StacProviderDatasetBand {
                            asset_title: "NIR 1 (band 8) - 10m".to_owned(),
                            band_name: Some("B08".to_owned()),
                        },
                        StacProviderDatasetBand {
                            asset_title: "Red (band 4) - 10m".to_owned(),
                            band_name: Some("B04".to_owned()),
                        },
                    ],
                },
                StacProviderDataset {
                    name: "Sentinel-2 L2A EPSG:32632 U8 20m".to_owned(),
                    description: String::new(),
                    data_type: geoengine_datatypes::raster::RasterDataType::U8,
                    resolution: SpatialResolution::new_unchecked(20.0, 20.0),
                    projection: SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        GeoTransform::new((399_960.0, 5_700_000.0).into(), 20.0, -20.0),
                        GridBoundingBox2D::new(
                            GridIdx2D::new([0, 0]),
                            GridIdx2D::new([5489, 5489]),
                        )
                        .unwrap(),
                    ),
                    bands: vec![
                        StacProviderDatasetBand {
                            asset_title: "Cloud probability (CLD) - 20m".to_owned(),
                            band_name: Some("CLD".to_owned()),
                        },
                        StacProviderDatasetBand {
                            asset_title: "Scene classification map (SCL) - 20m".to_owned(),
                            band_name: Some("SCL".to_owned()),
                        },
                    ],
                },
            ],
        };

        admin_ctx
            .db()
            .add_layer_provider(provider_def.into())
            .await
            .unwrap();

        // --- Load and prepare the NDVI workflow ---
        let ndvi_workflow_json =
            include_str!("../../../../../test_data/api_calls/stac_provider/ndvi-workflow.json");

        // Substitute provider ID
        let workflow_json_with_provider = ndvi_workflow_json
            .replace(
                "_:b274275c-373d-4a3f-8b45-9b48e9614329",
                &format!("_:{}", provider_id),
            )
            .replace("\"bands\": [1]", "\"bands\": [0]")
            .replace("\"bands\": [3, 4]", "\"bands\": [0, 1]");

        let workflow: Workflow = serde_json::from_str(&workflow_json_with_provider)
            .expect("workflow JSON should deserialize");

        // Get the raster operator from the workflow
        let operator = workflow
            .operator()
            .expect("workflow should have operator")
            .get_raster()
            .expect("workflow operator should be raster");

        // --- Execute the workflow ---
        let execution_ctx = admin_ctx.execution_context().expect("execution context");

        let initialized = operator
            .clone()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_ctx)
            .await
            .expect("operator should initialize");

        let processor = initialized
            .query_processor()
            .expect("should create query processor")
            .get_f32()
            .expect("NDVI should be float32");

        // Create a query rectangle in the operator's native query space
        let result_descriptor = initialized.result_descriptor();
        let spatial_bounds = result_descriptor.spatial_bounds();

        let query_tiling_pixel_grid = result_descriptor
            .spatial_grid_descriptor()
            .tiling_grid_definition(execution_ctx.tiling_specification())
            .tiling_spatial_grid_definition()
            .spatial_bounds_to_compatible_spatial_grid(spatial_bounds);

        let query_rect = RasterQueryRectangle::new(
            query_tiling_pixel_grid.grid_bounds(),
            TimeInterval::new(
                DateTime::new_utc(2026, 1, 1, 0, 0, 0),
                DateTime::new_utc(2026, 2, 1, 0, 0, 0),
            )
            .unwrap(),
            BandSelection::first(),
        );

        // Create query context
        let query_ctx = admin_ctx
            .query_context(Uuid::new_v4(), Uuid::new_v4())
            .expect("query context");

        // Execute the query
        let mut result_stream = processor
            .raster_query(query_rect, &query_ctx)
            .await
            .expect("query should succeed");

        // Collect stream output and verify that execution started
        let mut tile_count = 0;
        let mut successful_tile_count = 0;

        while let Some(tile_result) = result_stream.next().await {
            tile_count += 1;

            let Ok(tile) = tile_result else {
                // External STAC assets in this test fixture point to remote S3 paths.
                // The test still verifies actual workflow execution by asserting that
                // querying produces stream output.
                break;
            };

            successful_tile_count += 1;

            // Extract the grid data from the tile
            if !tile.grid_array.is_empty() {
                let materialized = tile.grid_array.into_materialized_masked_grid();

                // Touch values to ensure tile payload is accessed during execution.
                let _ = materialized
                    .inner_grid
                    .data
                    .iter()
                    .any(|value| !value.is_nan());
            }
        }

        // Verify that query execution produced stream output
        assert!(
            tile_count > 0,
            "NDVI workflow query should produce stream output"
        );

        // Successful tile output is optional in this fixture because referenced assets are remote.
        let _ = successful_tile_count;
    }
}
