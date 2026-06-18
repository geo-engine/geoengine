use super::{StacDataProvider, StacProviderDataset};
use crate::error;
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollection, LayerCollectionListOptions, LayerCollectionListing,
    LayerListing, ProviderLayerCollectionId, ProviderLayerId,
};
use crate::layers::listing::{
    LayerCollectionId, LayerCollectionProvider, ProviderCapabilities, SearchCapabilities,
};
use crate::workflows::workflow::Workflow;
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataId, LayerId, NamedData};
use geoengine_operators::engine::{RasterOperator, TypedOperator};
use geoengine_operators::source::{MultiBandGdalSource, MultiBandGdalSourceParameters};
use snafu::ensure;
use std::collections::BTreeMap;

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
    fn format_resolution_value(x: f64, y: f64) -> String {
        let x = Self::format_resolution_component(x);
        let y = Self::format_resolution_component(y);

        if x == y { x } else { format!("{x}x{y}") }
    }

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
                Self::format_resolution_value(dataset.resolution.x, dataset.resolution.y)
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
                Self::format_resolution_value(dataset.resolution.x, dataset.resolution.y)
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

    pub(super) fn dataset_by_layer_id(&self, id: &LayerId) -> Option<&StacProviderDataset> {
        let suffix = id.0.strip_prefix("dataset/")?;

        if let Ok(index) = suffix.parse::<usize>() {
            return self.datasets.get(index);
        }

        self.datasets
            .iter()
            .find(|dataset| Self::dataset_stable_id(dataset) == suffix)
    }

    pub(super) fn dataset_from_data_id(
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

    fn collection_items_for_path(
        &self,
        collection: &LayerCollectionId,
        collection_path: &str,
    ) -> error::Result<Vec<CollectionItem>> {
        if collection_path == ROOT_COLLECTION_ID {
            return Ok(self.root_collection_items());
        }

        let parts = collection_path.split('/').collect::<Vec<_>>();

        match parts.as_slice() {
            [first_dimension_id] => self.items_by_first_dimension(collection, first_dimension_id),
            [first_dimension_id, first_value] => self.items_for_second_dimension_selector(
                collection,
                first_dimension_id,
                first_value,
            ),
            [first_dimension_id, first_value, second_dimension_id] => self
                .items_by_second_dimension(
                    collection,
                    first_dimension_id,
                    first_value,
                    second_dimension_id,
                ),
            [
                first_dimension_id,
                first_value,
                second_dimension_id,
                second_value,
            ] => self.layer_items_for_two_dimension_filters(
                collection,
                first_dimension_id,
                first_value,
                second_dimension_id,
                second_value,
            ),
            _ => Err(error::Error::UnknownLayerCollectionId {
                id: collection.clone(),
            }),
        }
    }

    fn root_collection_items(&self) -> Vec<CollectionItem> {
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
    }

    fn parse_dimension_or_err(
        collection: &LayerCollectionId,
        dimension_id: &str,
    ) -> error::Result<GroupingDimension> {
        GroupingDimension::from_id(dimension_id).ok_or(error::Error::UnknownLayerCollectionId {
            id: collection.clone(),
        })
    }

    fn items_by_first_dimension(
        &self,
        collection: &LayerCollectionId,
        first_dimension_id: &str,
    ) -> error::Result<Vec<CollectionItem>> {
        let first_dimension = Self::parse_dimension_or_err(collection, first_dimension_id)?;

        Ok(self
            .available_values(first_dimension, &[])
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
                    description: format!("Datasets with {} {}", first_dimension.label(), display,),
                    properties: vec![],
                })
            })
            .collect::<Vec<_>>())
    }

    fn items_for_second_dimension_selector(
        &self,
        collection: &LayerCollectionId,
        first_dimension_id: &str,
        first_value: &str,
    ) -> error::Result<Vec<CollectionItem>> {
        let first_dimension = Self::parse_dimension_or_err(collection, first_dimension_id)?;

        Ok(GroupingDimension::all()
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
            .collect::<Vec<_>>())
    }

    fn items_by_second_dimension(
        &self,
        collection: &LayerCollectionId,
        first_dimension_id: &str,
        first_value: &str,
        second_dimension_id: &str,
    ) -> error::Result<Vec<CollectionItem>> {
        let first_dimension = Self::parse_dimension_or_err(collection, first_dimension_id)?;
        let second_dimension = Self::parse_dimension_or_err(collection, second_dimension_id)?;

        ensure!(
            first_dimension != second_dimension,
            error::UnknownLayerCollectionId {
                id: collection.clone(),
            }
        );

        Ok(self
            .available_values(second_dimension, &[(first_dimension, first_value)])
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
            .collect::<Vec<_>>())
    }

    fn layer_items_for_two_dimension_filters(
        &self,
        collection: &LayerCollectionId,
        first_dimension_id: &str,
        first_value: &str,
        second_dimension_id: &str,
        second_value: &str,
    ) -> error::Result<Vec<CollectionItem>> {
        let first_dimension = Self::parse_dimension_or_err(collection, first_dimension_id)?;
        let second_dimension = Self::parse_dimension_or_err(collection, second_dimension_id)?;

        ensure!(
            first_dimension != second_dimension,
            error::UnknownLayerCollectionId {
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

        Ok(items)
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
    ) -> error::Result<LayerCollection> {
        let collection_path = collection.0.trim_start_matches('/');
        let items = self.collection_items_for_path(collection, collection_path)?;

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

    async fn get_root_layer_collection_id(&self) -> error::Result<LayerCollectionId> {
        Ok(LayerCollectionId(ROOT_COLLECTION_ID.to_owned()))
    }

    async fn load_layer(&self, id: &LayerId) -> error::Result<Layer> {
        let dataset = self
            .dataset_by_layer_id(id)
            .ok_or(error::Error::UnknownLayerId { id: id.clone() })?;

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
            metadata: std::collections::HashMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::dataset::DataProviderId;
    use geoengine_datatypes::primitives::{
        RegularTimeDimension, SpatialResolution, TimeDimension, TimeGranularity, TimeStep,
    };
    use geoengine_datatypes::raster::{GeoTransform, GridBoundingBox2D, GridIdx2D};
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceAuthority};
    use geoengine_datatypes::util::Identifier;
    use geoengine_operators::engine::SpatialGridDescriptor;

    fn sample_provider() -> StacDataProvider {
        StacDataProvider::new(
            DataProviderId::new(),
            "Sentinel 2 L2A from STAC".to_owned(),
            String::new(),
            "http://example.com".to_owned(),
            "sentinel-2-l2a".to_owned(),
            None,
            TimeDimension::Regular(RegularTimeDimension::new_with_epoch_origin(TimeStep {
                granularity: TimeGranularity::Days,
                step: 1,
            })),
            vec![StacProviderDataset {
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
                bands: vec![],
            }],
            None,
            None,
        )
    }

    fn sample_provider_with_projection_variants() -> StacDataProvider {
        StacDataProvider::new(
            DataProviderId::new(),
            "Sentinel 2 L2A from STAC".to_owned(),
            String::new(),
            "http://example.com".to_owned(),
            "sentinel-2-l2a".to_owned(),
            None,
            TimeDimension::Regular(RegularTimeDimension::new_with_epoch_origin(TimeStep {
                granularity: TimeGranularity::Days,
                step: 1,
            })),
            vec![
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
                    bands: vec![],
                },
                StacProviderDataset {
                    name: "Sentinel-2 L2A EPSG:32633 U16 10m".to_owned(),
                    description: String::new(),
                    data_type: geoengine_datatypes::raster::RasterDataType::U16,
                    resolution: SpatialResolution::new_unchecked(10.0, 10.0),
                    projection: SpatialReference::new(SpatialReferenceAuthority::Epsg, 32633),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        GeoTransform::new((399_960.0, 5_700_000.0).into(), 10.0, -10.0),
                        GridBoundingBox2D::new(
                            GridIdx2D::new([0, 0]),
                            GridIdx2D::new([10979, 10979]),
                        )
                        .unwrap(),
                    ),
                    bands: vec![],
                },
                StacProviderDataset {
                    name: "Sentinel-2 L2A EPSG:32632 U8 10m".to_owned(),
                    description: String::new(),
                    data_type: geoengine_datatypes::raster::RasterDataType::U8,
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
                    bands: vec![],
                },
            ],
            None,
            None,
        )
    }

    #[test]
    fn dataset_stable_id_uses_projection_type_and_resolution() {
        let provider = sample_provider();
        let dataset = &provider.datasets[0];

        assert_eq!(
            StacDataProvider::dataset_stable_id(dataset),
            "epsg32632_u16_10"
        );
    }

    #[tokio::test]
    async fn root_layer_collection_lists_grouping_dimensions() {
        let provider = sample_provider();
        let root = provider
            .load_layer_collection(
                &LayerCollectionId(ROOT_COLLECTION_ID.to_owned()),
                Default::default(),
            )
            .await
            .expect("root collection should load");

        assert_eq!(root.items.len(), 3);
        assert_eq!(root.items[0].name(), "By data type");
        assert_eq!(root.items[1].name(), "By resolution");
        assert_eq!(root.items[2].name(), "By projection");
    }

    #[tokio::test]
    async fn deep_path_data_type_then_resolution_yields_projection_specific_layers() {
        let provider = sample_provider_with_projection_variants();

        let by_resolution = provider
            .load_layer_collection(
                &LayerCollectionId("dataTypes/u16".to_owned()),
                Default::default(),
            )
            .await
            .expect("data type branch should load");

        assert!(
            by_resolution
                .items
                .iter()
                .any(|item| item.name() == "By resolution")
        );

        let resolutions = provider
            .load_layer_collection(
                &LayerCollectionId("dataTypes/u16/resolutions".to_owned()),
                Default::default(),
            )
            .await
            .expect("resolution branch should load");

        assert_eq!(resolutions.items.len(), 1);
        assert_eq!(resolutions.items[0].name(), "10");

        let layers = provider
            .load_layer_collection(
                &LayerCollectionId("dataTypes/u16/resolutions/10".to_owned()),
                Default::default(),
            )
            .await
            .expect("deep path should resolve to layers");

        assert_eq!(layers.items.len(), 2);

        let mut layer_ids = layers
            .items
            .iter()
            .map(|item| match item {
                CollectionItem::Layer(layer) => layer.id.layer_id.0.clone(),
                CollectionItem::Collection(_) => {
                    panic!("deep path should only contain layers")
                }
            })
            .collect::<Vec<_>>();
        layer_ids.sort();

        assert_eq!(
            layer_ids,
            vec![
                "dataset/epsg32632_u16_10".to_owned(),
                "dataset/epsg32633_u16_10".to_owned(),
            ]
        );
    }
}
