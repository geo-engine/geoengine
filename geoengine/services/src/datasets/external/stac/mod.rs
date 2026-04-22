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
use crate::workflows::workflow::Workflow;
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataId, DataProviderId, LayerId, NamedData};
use geoengine_datatypes::primitives::{
    CacheHint, RasterQueryRectangle, SpatialResolution, TimeDimension, VectorQueryRectangle,
};
use geoengine_datatypes::raster::{
    GeoTransform, GridBoundingBox2D, RasterDataType, SpatialGridDefinition,
};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterBandDescriptors, RasterOperator, RasterResultDescriptor,
    SpatialGridDescriptor, TimeDescriptor, TypedOperator, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{
    GdalLoadingInfo, MultiBandGdalLoadingInfo, MultiBandGdalLoadingInfoQueryRectangle,
    MultiBandGdalSource, MultiBandGdalSourceParameters, OgrSourceDataset,
};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::collections::{BTreeMap, HashMap};

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
    pub s3_credentials: Option<StacProviderS3Credentials>,
    pub time_dimension: TimeDimension, // TODO: should this tbe on dataset level?
    pub datasets: Vec<StacProviderDataset>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "StacProviderS3Credentials")]
pub struct StacProviderS3Credentials {
    pub access_key: String,
    pub secret_key: String,
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
        Ok(Box::new(StacDataProvider::new(
            self.id,
            self.name,
            self.description,
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
    datasets: Vec<StacProviderDataset>,
}

impl StacDataProvider {
    pub fn new(
        id: DataProviderId,
        name: String,
        description: String,
        datasets: Vec<StacProviderDataset>,
    ) -> Self {
        Self {
            id,
            name,
            description,
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
    dataset: StacProviderDataset,
}

impl StacMultiBandMetaData {
    fn default_spatial_extent_for_projection(&self) -> (f64, f64, f64, f64) {
        // Default descriptors must provide a non-empty footprint so WMS intersection checks work.
        // These extents are placeholders until STAC item-derived bounds are wired in.
        let projection = self.dataset.projection.to_string();

        if projection == "EPSG:32632" {
            // From NSISCloud Sentinel-2 sample tile metadata in test_data/api_calls/nsiscloud/test.http
            return (399_960.0, 5_700_000.0, 109_800.0, 109_800.0);
        }

        if projection == "EPSG:4326" {
            return (-180.0, 90.0, 360.0, 180.0);
        }

        if projection == "EPSG:3857" {
            return (
                -20_037_508.342_789_2,
                20_037_508.342_789_2,
                40_075_016.685_578_4,
                40_075_016.685_578_4,
            );
        }

        // Generic fallback in projected units.
        (0.0, 1_000_000.0, 1_000_000.0, 1_000_000.0)
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
        // Placeholder infrastructure: STAC tile discovery will be injected here in a follow-up.
        // Returning empty file list keeps the MultiBand pipeline connected end-to-end.
        dbg!("hi from loading info", &query);
        Ok(MultiBandGdalLoadingInfo::new(
            vec![query.query_rectangle.time_interval()],
            vec![],
            CacheHint::default(),
        ))
    }

    async fn result_descriptor(&self) -> geoengine_operators::util::Result<RasterResultDescriptor> {
        let (origin_x, origin_y, extent_width, extent_height) =
            self.default_spatial_extent_for_projection();

        let x_resolution: f64 = self.dataset.resolution.x.abs();
        let y_resolution: f64 = -self.dataset.resolution.y.abs();

        let grid_width: isize = ((extent_width / x_resolution).ceil().max(1.0_f64)) as isize;
        let grid_height: isize =
            ((extent_height / y_resolution.abs()).ceil().max(1.0_f64)) as isize;

        let geo_transform = GeoTransform::new(
            geoengine_datatypes::primitives::Coordinate2D::new(origin_x, origin_y),
            x_resolution,
            y_resolution,
        );
        let spatial_grid = SpatialGridDefinition::new(
            geo_transform,
            GridBoundingBox2D::new([0, 0], [grid_height - 1, grid_width - 1])
                .expect("valid non-empty grid bounds"),
        );

        Ok(RasterResultDescriptor {
            data_type: self.dataset.data_type,
            spatial_reference: self.dataset.projection.into(),
            time: TimeDescriptor::new_irregular(None),
            spatial_grid: SpatialGridDescriptor::new_source(spatial_grid),
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
            dataset: dataset.clone(),
        }))
    }
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
    use super::*;
    use crate::layers::layer::CollectionItem;
    use std::str::FromStr;

    fn test_provider() -> StacDataProvider {
        let provider_id = DataProviderId::from_str("2e2a303d-d0fd-4f4e-996f-0f34e1fe24f7")
            .expect("valid provider id");

        let epsg_4326 = SpatialReference::from_str("EPSG:4326").expect("valid srs");
        let epsg_3857 = SpatialReference::from_str("EPSG:3857").expect("valid srs");

        let datasets = vec![
            StacProviderDataset {
                name: "u8-10m-4326".to_owned(),
                description: "U8 at 10m in EPSG:4326".to_owned(),
                data_type: RasterDataType::U8,
                resolution: SpatialResolution::new_unchecked(10., 10.),
                projection: epsg_4326,
                bands: vec![StacProviderDatasetBand {
                    name: "B1".to_owned(),
                }],
            },
            StacProviderDataset {
                name: "u8-10m-3857".to_owned(),
                description: "U8 at 10m in EPSG:3857".to_owned(),
                data_type: RasterDataType::U8,
                resolution: SpatialResolution::new_unchecked(10., 10.),
                projection: epsg_3857,
                bands: vec![StacProviderDatasetBand {
                    name: "B2".to_owned(),
                }],
            },
            StacProviderDataset {
                name: "u16-20m-4326".to_owned(),
                description: "U16 at 20m in EPSG:4326".to_owned(),
                data_type: RasterDataType::U16,
                resolution: SpatialResolution::new_unchecked(20., 20.),
                projection: epsg_4326,
                bands: vec![StacProviderDatasetBand {
                    name: "B3".to_owned(),
                }],
            },
        ];

        StacDataProvider::new(
            provider_id,
            "STAC test provider".to_owned(),
            "Provider for testing collection traversal".to_owned(),
            datasets,
        )
    }

    fn list_options() -> LayerCollectionListOptions {
        LayerCollectionListOptions {
            offset: 0,
            limit: 100,
        }
    }

    #[tokio::test]
    async fn lists_root_and_dimension_collections() {
        let provider = test_provider();
        let root = provider
            .get_root_layer_collection_id()
            .await
            .expect("root id must be available");

        let collection = provider
            .load_layer_collection(&root, list_options())
            .await
            .expect("root collection must load");

        assert_eq!(collection.items.len(), 3);

        let names = collection
            .items
            .iter()
            .map(|item| item.name().to_owned())
            .collect::<Vec<_>>();

        assert!(names.contains(&"By data type".to_owned()));
        assert!(names.contains(&"By resolution".to_owned()));
        assert!(names.contains(&"By projection".to_owned()));
    }

    #[tokio::test]
    async fn traverses_to_layers_with_existing_combinations_only() {
        let provider = test_provider();

        let by_data_type = provider
            .load_layer_collection(&LayerCollectionId("dataTypes".to_owned()), list_options())
            .await
            .expect("first grouping must load");

        let u8_collection_id = by_data_type
            .items
            .into_iter()
            .find_map(|item| match item {
                CollectionItem::Collection(collection)
                    if collection.id.collection_id.0 == "dataTypes/u8" =>
                {
                    Some(collection.id.collection_id)
                }
                _ => None,
            })
            .expect("U8 selection must exist");

        let next_groupings = provider
            .load_layer_collection(&u8_collection_id, list_options())
            .await
            .expect("second-level groupings must load");

        let by_resolution_collection_id = next_groupings
            .items
            .into_iter()
            .find_map(|item| match item {
                CollectionItem::Collection(collection)
                    if collection.id.collection_id.0.ends_with("/resolutions") =>
                {
                    Some(collection.id.collection_id)
                }
                _ => None,
            })
            .expect("resolution grouping must exist");

        let resolution_values = provider
            .load_layer_collection(&by_resolution_collection_id, list_options())
            .await
            .expect("resolution values must load");

        assert_eq!(resolution_values.items.len(), 1);

        let selected_resolution = resolution_values
            .items
            .into_iter()
            .find_map(|item| match item {
                CollectionItem::Collection(collection) => Some(collection.id.collection_id),
                _ => None,
            })
            .expect("resolution selection must exist");

        let layers = provider
            .load_layer_collection(&selected_resolution, list_options())
            .await
            .expect("final layer collection must load");

        assert_eq!(layers.items.len(), 2);

        let mut layer_ids = layers
            .items
            .into_iter()
            .filter_map(|item| match item {
                CollectionItem::Layer(layer) => Some(layer.id.layer_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        layer_ids.sort_by(|a, b| a.0.cmp(&b.0));

        let loaded_layer = provider
            .load_layer(&layer_ids[0])
            .await
            .expect("layer id from listing must be loadable");

        assert!(
            loaded_layer.name == "u8-10m-3857" || loaded_layer.name == "u8-10m-4326",
            "loaded layer must match one of the filtered datasets"
        );
    }
}
