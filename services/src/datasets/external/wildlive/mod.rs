use crate::{
    config::{self},
    contexts::GeoEngineDb,
    datasets::{
        external::wildlive::auth::retrieve_refresh_token_from_local_code,
        listing::{Provenance, ProvenanceOutput},
    },
    layers::{
        external::{DataProvider, DataProviderDefinition},
        layer::{
            CollectionItem, Layer, LayerCollection, LayerCollectionListOptions,
            LayerCollectionListing, LayerListing, ProviderLayerCollectionId, ProviderLayerId,
        },
        listing::{
            LayerCollectionId, LayerCollectionProvider, ProviderCapabilities, SearchCapabilities,
        },
    },
    users::UserId,
    util::{Secret, oidc::RefreshToken, postgres::DatabaseConnectionConfig},
    workflows::workflow::Workflow,
};
use async_trait::async_trait;
use cache::DatasetInDb;
use datasets::{captures_dataset, project_stations_dataset, projects_dataset};
use geoengine_datatypes::{
    collections::VectorDataType,
    dataset::{DataId, DataProviderId, LayerId, NamedData},
    error::BoxedResultExt,
    primitives::{
        CacheTtlSeconds, DateTime, Duration, FeatureDataType, Measurement, RasterQueryRectangle,
        VectorQueryRectangle,
    },
    spatial_reference::SpatialReference,
};
use geoengine_operators::{
    engine::{
        MetaData, MetaDataProvider, RasterResultDescriptor, StaticMetaData, VectorColumnInfo,
        VectorOperator, VectorResultDescriptor,
    },
    mock::MockDatasetDataSourceLoadingInfo,
    source::{
        GdalLoadingInfo, OgrSource, OgrSourceColumnSpec, OgrSourceDataset,
        OgrSourceDatasetTimeType, OgrSourceDurationSpec, OgrSourceErrorSpec, OgrSourceParameters,
        OgrSourceTimeFormat,
    },
};
use oauth2::AccessToken;
use postgres_protocol::escape::escape_literal;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::error;
use url::Url;

use crate::layers::external::TypedDataProviderDefinition;
pub use cache::WildliveDbCache;
pub use error::WildliveError;

mod auth;
mod cache;
mod datasets;
mod error;

type Result<T, E = WildliveError> = std::result::Result<T, E>;

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub struct WildliveDataConnectorDefinition {
    pub id: DataProviderId,
    pub name: String,
    pub description: String,
    /// An `OpenID` Connect refresh token for the `WildLIVE!` Portal API
    #[serde(flatten)]
    pub auth: Option<WildliveDataConnectorAuth>,
    pub priority: Option<i16>,
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub struct WildliveDataConnectorAuth {
    pub user: String,
    pub refresh_token: Secret<RefreshToken>,
    pub expiry_date: DateTime,
}

impl WildliveDataConnectorDefinition {
    fn refresh_token_option(&self) -> Option<&RefreshToken> {
        self.auth.as_ref().map(|a| a.refresh_token.as_ref())
    }
}

#[derive(Debug)]
pub struct WildliveDataConnector<D: GeoEngineDb> {
    definition: WildliveDataConnectorDefinition,
    #[allow(dead_code)]
    local_oidc_config: config::Oidc,
    wildlive_config: config::Wildlive,
    user: Option<UserId>,
    db: Arc<D>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", tag = "type")]
enum WildliveCollectionId {
    Projects,
    #[serde(rename_all = "camelCase")]
    Project {
        project_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
enum WildliveLayerId {
    Projects,
    #[serde(rename_all = "camelCase")]
    Stations {
        project_id: String,
    },
    #[serde(rename_all = "camelCase")]
    Captures {
        project_id: String,
    },
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct OidcAuthToken {
    code: String,
    pkce_verifier: String,
    redirect_uri: Url,
}

#[async_trait]
impl<D: GeoEngineDb> DataProviderDefinition<D> for WildliveDataConnectorDefinition {
    async fn initialize(self: Box<Self>, db: D) -> crate::error::Result<Box<dyn DataProvider>> {
        // TODO: Usually, for non-system data connectors, we need to namespace our layers and data by
        // the user that created the data connector. However, currently the implementation does not adhere to this.
        // So for now, we can omit using a namespace.
        let user = None;

        Ok(Box::new(WildliveDataConnector {
            definition: *self,
            user,
            local_oidc_config: config::get_config_element::<config::Oidc>()
                .boxed_context(error::MissingConfiguration)?,
            wildlive_config: config::get_config_element::<config::Wildlive>()
                .boxed_context(error::MissingConfiguration)?,
            db: Arc::new(db),
        }))
    }

    fn type_name(&self) -> &'static str {
        "WildLIVE!"
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

    async fn update(
        &self,
        new: TypedDataProviderDefinition,
    ) -> crate::error::Result<TypedDataProviderDefinition> {
        let TypedDataProviderDefinition::WildliveDataConnectorDefinition(mut new) = new else {
            return Err(WildliveError::WrongConnectorType.into());
        };

        let Some(new_auth) = &mut new.auth else {
            // no auth given in new definition, nothing to do
            return Ok(TypedDataProviderDefinition::WildliveDataConnectorDefinition(new));
        };

        if new_auth.refresh_token.is_unknown()
            && let Some(self_auth) = &self.auth
        {
            // no new auth, so copy the old one
            new_auth.clone_from(self_auth);

            return Ok(TypedDataProviderDefinition::WildliveDataConnectorDefinition(new));
        }

        // if it is JSON, let's assume it is an auth code
        let Ok(auth_token) = serde_json::from_str::<OidcAuthToken>(new_auth.refresh_token.as_str())
        else {
            // otherwise, assume it is a refresh token
            return Ok(TypedDataProviderDefinition::WildliveDataConnectorDefinition(new));
        };

        let local_config = config::get_config_element::<config::Oidc>()
            .boxed_context(error::MissingConfiguration)?;
        let wildlive_config = config::get_config_element::<config::Wildlive>()
            .boxed_context(error::MissingConfiguration)?;

        let tokens = retrieve_refresh_token_from_local_code(
            local_config,
            wildlive_config.oidc,
            &auth_token.code,
            &auth_token.pkce_verifier,
            auth_token.redirect_uri,
        )
        .await?;

        new_auth.refresh_token = Secret::new(tokens.refresh_token.into());
        new_auth.expiry_date =
            DateTime::now() + Duration::seconds(tokens.refresh_expires_in as i64);
        new_auth.user = tokens.user.unwrap_or_else(|| "<unknown user>".to_string());

        Ok(TypedDataProviderDefinition::WildliveDataConnectorDefinition(new))
    }
}

#[async_trait]
impl<D: GeoEngineDb> DataProvider for WildliveDataConnector<D> {
    async fn provenance(&self, id: &DataId) -> crate::error::Result<ProvenanceOutput> {
        Ok(ProvenanceOutput {
            data: id.clone(),
            provenance: Some(vec![
                // TODO: check if this is correct
                Provenance {
                    citation: "WildLIVE! Portal".to_string(),
                    license: "CC-BY-4.0 (https://spdx.org/licenses/CC-BY-4.0)".to_string(),
                    uri: "https://wildlive.senckenberg.de".to_string(),
                },
            ]),
        })
    }
}

#[async_trait]
impl<D: GeoEngineDb> LayerCollectionProvider for WildliveDataConnector<D> {
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            listing: true,
            search: SearchCapabilities::none(),
        }
    }

    fn name(&self) -> &str {
        &self.definition.name
    }

    fn description(&self) -> &str {
        &self.definition.description
    }

    async fn load_layer_collection(
        &self,
        collection_id: &LayerCollectionId,
        options: LayerCollectionListOptions,
    ) -> crate::error::Result<LayerCollection> {
        match WildliveCollectionId::try_from(collection_id.clone())? {
            WildliveCollectionId::Projects => {
                let mut items: Vec<_> = vec![CollectionItem::Layer(LayerListing {
                    r#type: Default::default(),
                    id: self.layer_id(WildliveLayerId::Projects)?,
                    name: "Projects".to_string(),
                    description: "Overview of all projects".to_string(),
                    properties: Vec::new(),
                })];

                let api_token = update_and_get_token_for_request(
                    &self.definition,
                    &self.wildlive_config.oidc,
                    self.db.as_ref(),
                )
                .await?;
                let projects =
                    datasets::projects(&self.wildlive_config.api_endpoint, api_token.as_ref())
                        .await?;

                for project in projects {
                    items.push(CollectionItem::Collection(LayerCollectionListing {
                        r#type: Default::default(),
                        id: self.collection_id(WildliveCollectionId::Project {
                            project_id: project.id,
                        })?,
                        name: project.name,
                        description: project.description,
                        properties: Vec::new(),
                    }));
                }

                Ok(LayerCollection {
                    id: self.collection_id(WildliveCollectionId::Projects)?,
                    name: "WildLIVE! Projects".to_string(),
                    description: "List of all projects".to_string(),
                    // TODO: paginate request instead of loading all projects
                    items: items
                        .drain(subset_range(options.offset, options.limit, items.len()))
                        .collect(),
                    entry_label: None,
                    properties: Vec::new(),
                })
            }
            WildliveCollectionId::Project { project_id } => {
                let mut items: Vec<_> = vec![
                    CollectionItem::Layer(LayerListing {
                        r#type: Default::default(),
                        id: self.layer_id(WildliveLayerId::Stations {
                            project_id: project_id.clone(),
                        })?,
                        name: "Stations".to_string(),
                        description: "Overview of all project stations".to_string(),
                        properties: Vec::new(),
                    }),
                    CollectionItem::Layer(LayerListing {
                        r#type: Default::default(),
                        id: self.layer_id(WildliveLayerId::Captures {
                            project_id: project_id.clone(),
                        })?,
                        name: "Captures".to_string(),
                        description: "Overview of all project captures".to_string(),
                        properties: Vec::new(),
                    }),
                ];

                Ok(LayerCollection {
                    id: self.collection_id(WildliveCollectionId::Project { project_id })?,
                    name: "WildLIVE! Projects".to_string(),
                    description: "List of all projects".to_string(),
                    items: items
                        .drain(subset_range(options.offset, options.limit, items.len()))
                        .collect(),
                    entry_label: None,
                    properties: Vec::new(),
                })
            }
        }
    }

    async fn get_root_layer_collection_id(&self) -> crate::error::Result<LayerCollectionId> {
        Ok(WildliveCollectionId::Projects.try_into()?)
    }

    async fn load_layer(&self, id: &LayerId) -> crate::error::Result<Layer> {
        match WildliveLayerId::try_from(id.clone())? {
            WildliveLayerId::Projects => Ok(Layer {
                id: self.layer_id(WildliveLayerId::Projects)?,
                name: "Projects".to_string(),
                description: "Overview of all projects".to_string(),
                workflow: Workflow {
                    operator: OgrSource {
                        params: OgrSourceParameters {
                            data: self.named_data(WildliveLayerId::Projects)?,
                            attribute_projection: None,
                            attribute_filters: None,
                        },
                    }
                    .boxed()
                    .into(),
                },
                symbology: None,
                properties: Vec::new(),
                metadata: Default::default(),
            }),
            WildliveLayerId::Stations { project_id } => Ok(Layer {
                id: self.layer_id(WildliveLayerId::Stations {
                    project_id: project_id.clone(),
                })?,
                name: format!("Stations for project {project_id}"),
                description: format!("Overview of all stations within project {project_id}"),
                workflow: Workflow {
                    operator: OgrSource {
                        params: OgrSourceParameters {
                            data: self.named_data(WildliveLayerId::Stations { project_id })?,
                            attribute_projection: None,
                            attribute_filters: None,
                        },
                    }
                    .boxed()
                    .into(),
                },
                symbology: None,
                properties: Vec::new(),
                metadata: Default::default(),
            }),
            WildliveLayerId::Captures { project_id } => Ok(Layer {
                id: self.layer_id(WildliveLayerId::Captures {
                    project_id: project_id.clone(),
                })?,
                name: format!("Captures for project {project_id}"),
                description: format!("Overview of all captures within project {project_id}"),
                workflow: Workflow {
                    operator: OgrSource {
                        params: OgrSourceParameters {
                            data: self.named_data(WildliveLayerId::Captures { project_id })?,
                            attribute_projection: None,
                            attribute_filters: None,
                        },
                    }
                    .boxed()
                    .into(),
                },
                symbology: None,
                properties: Vec::new(),
                metadata: Default::default(),
            }),
        }
    }
}

fn subset_range(offset: u32, limit: u32, length: usize) -> std::ops::Range<usize> {
    let offset = offset as usize;
    let limit = limit as usize;

    if offset >= length {
        return 0..0;
    }

    offset..(offset + limit).min(length)
}

#[async_trait]
impl<D: GeoEngineDb>
    MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for WildliveDataConnector<D>
{
    async fn meta_data(
        &self,
        id: &DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        let config = config::get_config_element::<config::Wildlive>()
            .map_err(|_| geoengine_operators::error::Error::InvalidDataProviderConfig)?;

        let layer_id = if let DataId::External(data_id) = id {
            data_id.layer_id.clone()
        } else {
            return Err(geoengine_operators::error::Error::InvalidDataId);
        };

        let layer_id = WildliveLayerId::try_from(layer_id)
            .map_err(|_| geoengine_operators::error::Error::InvalidDataId)?;

        meta_data(&self.definition, &config, self.db.clone(), layer_id)
            .await
            .map_err(|error| geoengine_operators::error::Error::MetaData {
                source: Box::new(error),
            })
    }
}

#[async_trait]
impl<D: GeoEngineDb>
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for WildliveDataConnector<D>
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
impl<D: GeoEngineDb> MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for WildliveDataConnector<D>
{
    async fn meta_data(
        &self,
        _id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

impl<D: GeoEngineDb> WildliveDataConnector<D> {
    fn layer_id(&self, id: WildliveLayerId) -> Result<ProviderLayerId> {
        Ok(ProviderLayerId {
            provider_id: self.definition.id,
            layer_id: id.try_into()?,
        })
    }

    fn collection_id(&self, id: WildliveCollectionId) -> Result<ProviderLayerCollectionId> {
        Ok(ProviderLayerCollectionId {
            provider_id: self.definition.id,
            collection_id: id.try_into()?,
        })
    }

    fn named_data(&self, id: WildliveLayerId) -> Result<NamedData> {
        Ok(NamedData {
            namespace: self.user.map(|u| u.to_string()),
            provider: Some(self.definition.id.to_string()),
            name: LayerId::try_from(id)?.0,
        })
    }
}

async fn update_and_get_token_for_request(
    definition: &WildliveDataConnectorDefinition,
    config: &config::WildliveOidc,
    _db: &impl GeoEngineDb,
) -> Result<Option<AccessToken>> {
    let Some(refresh_token) = definition.refresh_token_option() else {
        return Ok(None);
    };

    let http_client = reqwest::ClientBuilder::new()
        // Following redirects opens the client up to SSRF vulnerabilities.
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .boxed_context(error::UnexpectedExecution)?;

    let tokens =
        auth::retrieve_access_and_refresh_token(&http_client, config, refresh_token).await?;

    let mut definition = definition.clone();
    definition.auth = Some(WildliveDataConnectorAuth {
        user: tokens
            .user
            .clone()
            .unwrap_or_else(|| "<unknown user>".to_string()),
        refresh_token: Secret::new(tokens.refresh_token.clone().into()),
        expiry_date: DateTime::now() + Duration::seconds(tokens.refresh_expires_in as i64),
    });

    // TODO: update here in case the old one becomes invalid
    // db.update_layer_provider_definition(definition.id, definition.into())
    //     .await
    //     .boxed_context(error::UnexpectedExecution)?;

    Ok(Some(tokens.access_token))
}

async fn meta_data<D: GeoEngineDb>(
    definition: &WildliveDataConnectorDefinition,
    config: &config::Wildlive,
    db: Arc<D>,
    layer_id: WildliveLayerId,
) -> Result<Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>> {
    let db_config = DatabaseConnectionConfig::from(
        // TODO: - is there a way to get the current connection config?
        //       - handle error
        config::get_config_element::<config::Postgres>().expect("configuration is there"),
    );
    let layer_name = layer_id.table_name().to_string();

    match layer_id {
        WildliveLayerId::Projects => {
            project_metadata(definition, config, db, db_config, layer_name).await
        }
        WildliveLayerId::Stations { project_id } => {
            stations_metadata(definition, config, db, db_config, layer_name, &project_id).await
        }
        WildliveLayerId::Captures { project_id } => {
            captures_metadata(definition, config, db, db_config, layer_name, project_id).await
        }
    }
}

async fn project_metadata<D: GeoEngineDb>(
    definition: &WildliveDataConnectorDefinition,
    config: &config::Wildlive,
    db: Arc<D>,
    db_config: DatabaseConnectionConfig,
    layer_name: String,
) -> Result<Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>> {
    if !db.has_projects(definition.id).await? {
        let api_token: Option<AccessToken> =
            update_and_get_token_for_request(definition, &config.oidc, db.as_ref()).await?;
        let dataset = projects_dataset(&config.api_endpoint, api_token.as_ref()).await?;
        db.insert_projects(definition.id, &dataset).await?;
    }

    let unitless_column_info = VectorColumnInfo {
        data_type: FeatureDataType::Text,
        measurement: Measurement::Unitless,
    };

    let columns: [(String, VectorColumnInfo); 3] = [
        ("project_id".to_string(), unitless_column_info.clone()),
        ("name".to_string(), unitless_column_info.clone()),
        ("description".to_string(), unitless_column_info.clone()),
    ];

    Ok(Box::new(StaticMetaData {
        loading_info: OgrSourceDataset {
            file_name: db_config.ogr_pg_config().into(),
            layer_name,
            data_type: Some(VectorDataType::MultiPolygon),
            time: OgrSourceDatasetTimeType::None,
            default_geometry: None,
            columns: Some(OgrSourceColumnSpec {
                format_specifics: None,
                x: String::new(),
                y: None,
                int: vec![],
                float: vec![],
                text: columns
                    .iter()
                    .filter_map(|(name, info)| {
                        if info.data_type == FeatureDataType::Text {
                            Some(name.clone())
                        } else {
                            None
                        }
                    })
                    .collect(),
                bool: vec![],
                datetime: vec![],
                rename: None,
            }),
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Abort,
            sql_query: None,
            attribute_query: Some(format!(
                "cache_date = current_date AND provider_id = {}",
                escape_literal(&definition.id.to_string())
            )),
            cache_ttl: CacheTtlSeconds::new(60 * 60),
        },
        result_descriptor: VectorResultDescriptor {
            data_type: VectorDataType::MultiPolygon,
            spatial_reference: SpatialReference::epsg_4326().into(),
            columns: columns.iter().cloned().collect(),
            time: None, // TODO
            bbox: None, // TODO
        },
        phantom: std::marker::PhantomData,
    }))
}

async fn stations_metadata<D: GeoEngineDb>(
    definition: &WildliveDataConnectorDefinition,
    config: &config::Wildlive,
    db: Arc<D>,
    db_config: DatabaseConnectionConfig,
    layer_name: String,
    project_id: &str,
) -> Result<Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>> {
    if !db.has_stations(definition.id, project_id).await? {
        let api_token =
            update_and_get_token_for_request(definition, &config.oidc, db.as_ref()).await?;
        let dataset =
            project_stations_dataset(&config.api_endpoint, api_token.as_ref(), project_id).await?;
        db.insert_stations(definition.id, project_id, &dataset)
            .await?;
    }

    let unitless_column_info = VectorColumnInfo {
        data_type: FeatureDataType::Text,
        measurement: Measurement::Unitless,
    };

    let columns: [(String, VectorColumnInfo); 5] = [
        ("station_id".to_string(), unitless_column_info.clone()),
        ("project_id".to_string(), unitless_column_info.clone()),
        ("name".to_string(), unitless_column_info.clone()),
        ("description".to_string(), unitless_column_info.clone()),
        ("location".to_string(), unitless_column_info.clone()),
    ];

    Ok(Box::new(StaticMetaData {
        loading_info: OgrSourceDataset {
            file_name: db_config.ogr_pg_config().into(),
            layer_name,
            data_type: Some(VectorDataType::MultiPoint),
            time: OgrSourceDatasetTimeType::None,
            default_geometry: None,
            columns: Some(OgrSourceColumnSpec {
                format_specifics: None,
                x: String::new(),
                y: None,
                int: vec![],
                float: vec![],
                text: columns
                    .iter()
                    .filter_map(|(name, info)| {
                        if info.data_type == FeatureDataType::Text {
                            Some(name.clone())
                        } else {
                            None
                        }
                    })
                    .collect(),
                bool: vec![],
                datetime: vec![],
                rename: None,
            }),
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Abort,
            sql_query: None,
            attribute_query: Some(format!(
                "cache_date = current_date AND provider_id = {} AND project_id = {}",
                escape_literal(&definition.id.to_string()),
                escape_literal(project_id)
            )),
            cache_ttl: CacheTtlSeconds::new(60 * 60),
        },
        result_descriptor: VectorResultDescriptor {
            data_type: VectorDataType::MultiPoint,
            spatial_reference: SpatialReference::epsg_4326().into(),
            columns: columns.iter().cloned().collect(),
            time: None, // TODO
            bbox: None, // TODO
        },
        phantom: std::marker::PhantomData,
    }))
}

async fn captures_metadata<D: GeoEngineDb>(
    definition: &WildliveDataConnectorDefinition,
    config: &config::Wildlive,
    db: Arc<D>,
    db_config: DatabaseConnectionConfig,
    layer_name: String,
    project_id: String,
) -> Result<Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>> {
    if !db.has_captures(definition.id, &project_id).await? {
        let api_token =
            update_and_get_token_for_request(definition, &config.oidc, db.as_ref()).await?;
        let dataset =
            captures_dataset(&config.api_endpoint, api_token.as_ref(), &project_id).await?;

        db.insert_captures(definition.id, &project_id, &dataset)
            .await?;
    }

    let unitless_column_info = VectorColumnInfo {
        data_type: FeatureDataType::Text,
        measurement: Measurement::Unitless,
    };

    let columns: [(String, VectorColumnInfo); 8] = [
        ("image_object_id".to_string(), unitless_column_info.clone()),
        ("project_id".to_string(), unitless_column_info.clone()),
        ("station_setup_id".to_string(), unitless_column_info.clone()),
        (
            "capture_time_stamp".to_string(),
            VectorColumnInfo {
                data_type: FeatureDataType::DateTime,
                measurement: Measurement::Unitless,
            },
        ),
        (
            "accepted_name_usage_id".to_string(),
            unitless_column_info.clone(),
        ),
        ("vernacular_name".to_string(), unitless_column_info.clone()),
        ("scientific_name".to_string(), unitless_column_info.clone()),
        ("content_url".to_string(), unitless_column_info.clone()),
    ];

    Ok(Box::new(StaticMetaData {
        loading_info: OgrSourceDataset {
            file_name: db_config.ogr_pg_config().into(),
            layer_name,
            data_type: Some(VectorDataType::MultiPoint),
            time: OgrSourceDatasetTimeType::Start {
                start_field: "capture_time_stamp".into(),
                start_format: OgrSourceTimeFormat::Auto,
                duration: OgrSourceDurationSpec::Zero,
            },
            default_geometry: None,
            columns: Some(OgrSourceColumnSpec {
                format_specifics: None,
                x: String::new(),
                y: None,
                int: vec![],
                float: vec![],
                text: columns
                    .iter()
                    .filter_map(|(name, info)| {
                        if info.data_type == FeatureDataType::Text {
                            Some(name.clone())
                        } else {
                            None
                        }
                    })
                    .collect(),
                bool: vec![],
                datetime: columns
                    .iter()
                    .filter_map(|(name, info)| {
                        if info.data_type == FeatureDataType::DateTime {
                            Some(name.clone())
                        } else {
                            None
                        }
                    })
                    .collect(),
                rename: None,
            }),
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Abort,
            sql_query: None,
            attribute_query: Some(format!(
                "cache_date = current_date AND provider_id = {} AND project_id = {}",
                escape_literal(&definition.id.to_string()),
                escape_literal(&project_id)
            )),
            cache_ttl: CacheTtlSeconds::new(60 * 60),
        },
        result_descriptor: VectorResultDescriptor {
            data_type: VectorDataType::MultiPoint,
            spatial_reference: SpatialReference::epsg_4326().into(),
            columns: columns.iter().cloned().collect(),
            time: None, // TODO
            bbox: None, // TODO
        },
        phantom: std::marker::PhantomData,
    }))
}

impl TryFrom<WildliveCollectionId> for LayerCollectionId {
    type Error = WildliveError;

    fn try_from(value: WildliveCollectionId) -> std::result::Result<Self, Self::Error> {
        let string = serde_urlencoded::to_string(value)
            .boxed_context(error::UnableToSerializeCollectionId)?;
        Ok(LayerCollectionId(string))
    }
}

impl TryFrom<LayerCollectionId> for WildliveCollectionId {
    type Error = WildliveError;

    fn try_from(value: LayerCollectionId) -> std::result::Result<Self, Self::Error> {
        serde_urlencoded::from_str(&value.0).boxed_context(error::UnableToSerializeCollectionId)
    }
}

impl TryFrom<WildliveLayerId> for LayerId {
    type Error = WildliveError;

    fn try_from(value: WildliveLayerId) -> std::result::Result<Self, Self::Error> {
        let string = serde_urlencoded::to_string(value)
            .boxed_context(error::UnableToSerializeCollectionId)?;
        Ok(LayerId(string))
    }
}

impl TryFrom<LayerId> for WildliveLayerId {
    type Error = WildliveError;

    fn try_from(value: LayerId) -> std::result::Result<Self, Self::Error> {
        serde_urlencoded::from_str(&value.0).boxed_context(error::UnableToSerializeCollectionId)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        contexts::{PostgresSessionContext, SessionContext},
        datasets::external::wildlive::datasets::{PAGE_SIZE, SearchRequest},
        ge_context,
        util::join_base_url_and_path,
    };
    use geoengine_datatypes::{
        collections::VectorDataType,
        dataset::ExternalDataId,
        primitives::{
            BoundingBox2D, CacheTtlSeconds, ColumnSelection, Coordinate2D, FeatureDataType,
            Measurement, SpatialResolution, TimeInterval,
        },
        spatial_reference::SpatialReference,
        test_data,
    };
    use geoengine_operators::{
        engine::VectorColumnInfo,
        source::{OgrSourceColumnSpec, OgrSourceDatasetTimeType, OgrSourceErrorSpec},
    };
    use httptest::{
        Expectation, all_of,
        matchers::{self, request},
        responders,
    };
    use std::path::Path;
    use tokio_postgres::NoTls;

    #[test]
    fn it_serializes_collection_ids() {
        let root_collection_id = WildliveCollectionId::Projects;
        let root_collection_layer_id =
            LayerCollectionId::try_from(root_collection_id.clone()).unwrap();

        assert_eq!(root_collection_layer_id.to_string(), "type=projects");
        assert_eq!(
            WildliveCollectionId::try_from(root_collection_layer_id).unwrap(),
            root_collection_id
        );

        let project_collection_id = WildliveCollectionId::Project {
            project_id: "wildlive/853cb65c033fa30bfa1c".to_string(),
        };
        let project_collection_layer_id =
            LayerCollectionId::try_from(project_collection_id.clone()).unwrap();

        assert_eq!(
            project_collection_layer_id.to_string(),
            "type=project&projectId=wildlive%2F853cb65c033fa30bfa1c"
        );
        assert_eq!(
            WildliveCollectionId::try_from(project_collection_layer_id).unwrap(),
            project_collection_id
        );
    }

    #[test]
    fn it_serializes_layer_ids() {
        let projects_id = WildliveLayerId::Projects;
        let projects_layer_id = LayerId::try_from(projects_id.clone()).unwrap();

        assert_eq!(projects_layer_id.to_string(), "type=projects");
        assert_eq!(
            WildliveLayerId::try_from(projects_layer_id).unwrap(),
            projects_id
        );

        let stations_id = WildliveLayerId::Stations {
            project_id: "wildlive/853cb65c033fa30bfa1c".to_string(),
        };
        let stations_layer_id = LayerId::try_from(stations_id.clone()).unwrap();

        assert_eq!(
            stations_layer_id.to_string(),
            "type=stations&projectId=wildlive%2F853cb65c033fa30bfa1c"
        );
        assert_eq!(
            WildliveLayerId::try_from(stations_layer_id).unwrap(),
            stations_id
        );

        let captures_id = WildliveLayerId::Captures {
            project_id: "wildlive/853cb65c033fa30bfa1c".to_string(),
        };
        let captures_layer_id = LayerId::try_from(captures_id.clone()).unwrap();
        assert_eq!(
            captures_layer_id.to_string(),
            "type=captures&projectId=wildlive%2F853cb65c033fa30bfa1c"
        );
        assert_eq!(
            WildliveLayerId::try_from(captures_layer_id).unwrap(),
            captures_id
        );
    }

    fn json_responder(path: &Path) -> impl responders::Responder + use<> {
        let json = std::fs::read_to_string(path).unwrap();
        responders::status_code(200)
            .append_header("Content-Type", "application/json")
            .body(json)
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_shows_an_overview_of_all_projects(
        ctx: PostgresSessionContext<NoTls>,
        db_config: DatabaseConnectionConfig,
    ) {
        let mock_server = httptest::Server::run();

        mock_server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path("/api/search"),
                request::query(matchers::url_decoded(matchers::contains((
                    "query",
                    "type:project"
                )))),
            ])
            .respond_with(json_responder(test_data!(
                "wildlive/responses/projects.json"
            ))),
        );

        mock_server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::path("/api/search"),
                request::body(matchers::json_decoded(matchers::eq(SearchRequest {
                    query: "(/inStationsLayout:\"wildlive/667cc39364fd45136c7a\" OR /inStationsLayout:\"wildlive/151c43fdd5881eba0bd5\") AND type:StationSetup".to_string(),
                    filter: Some(
                        [
                            "/id",
                            "/content/inStationsLayout",
                            "/content/decimalLatitude",
                            "/content/decimalLongitude",
                        ]
                        .map(ToString::to_string)
                        .to_vec(),
                    ),
                    page_num: 0,
                    page_size: PAGE_SIZE,
                }))
            ),
            ])
            .respond_with(json_responder(test_data!(
                "wildlive/responses/station_coordinates.json"
            ))),
        );

        // let api_endpoint = Url::parse("https://wildlive.senckenberg.de/api/").unwrap();
        let api_endpoint = Url::parse(&mock_server.url_str("/api/")).unwrap();

        // crate::util::tests::initialize_debugging_in_test();

        let connector = WildliveDataConnector {
            definition: WildliveDataConnectorDefinition {
                id: DataProviderId::from_u128(12_345_678_901_234_567_890_123_456_789_012_u128),
                name: "WildLIVE! Portal Connector".to_string(),
                description: "WildLIVE! Portal Connector".to_string(),
                auth: None,
                priority: None,
            },
            user: None,
            local_oidc_config: config::Oidc {
                enabled: true,
                issuer: join_base_url_and_path(&api_endpoint, "/auth/realms/wildlive-portal/")
                    .unwrap(),
                client_id: "geoengine".to_string(),
                client_secret: None,
                scopes: ["openid", "offline_access"]
                    .map(ToString::to_string)
                    .to_vec(),
                token_encryption_password: None,
            },
            wildlive_config: config::Wildlive {
                api_endpoint: api_endpoint.clone(),
                oidc: config::WildliveOidc {
                    issuer: join_base_url_and_path(&api_endpoint, "/auth/realms/wildlive-portal/")
                        .unwrap(),
                    client_id: "geoengine".to_string(),
                    client_secret: None,
                    broker_provider: "wildlive-portal".to_string(),
                },
            },
            db: Arc::new(ctx.db()),
        };

        let layer = connector
            .load_layer(&WildliveLayerId::Projects.try_into().unwrap())
            .await
            .unwrap();
        assert_eq!(
            layer,
            Layer {
                id: ProviderLayerId {
                    provider_id: connector.definition.id,
                    layer_id: WildliveLayerId::Projects.try_into().unwrap(),
                },
                name: "Projects".to_string(),
                description: "Overview of all projects".to_string(),
                workflow: Workflow {
                    operator: OgrSource {
                        params: OgrSourceParameters {
                            data: connector.named_data(WildliveLayerId::Projects).unwrap(),
                            attribute_projection: None,
                            attribute_filters: None,
                        },
                    }
                    .boxed()
                    .into(),
                },
                symbology: None,
                properties: Vec::new(),
                metadata: Default::default(),
            }
        );

        let metadata: Box<
            dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
        > = MetaDataProvider::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
            ::meta_data(&connector, &DataId::External(ExternalDataId {
                provider_id: layer.id.provider_id,
                layer_id: layer.id.layer_id,
            }))
            .await
            .unwrap();

        let loading_info = metadata
            .loading_info(VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    Coordinate2D { x: 0.0, y: 0.0 },
                    Coordinate2D { x: 1.0, y: 1.0 },
                )
                .unwrap(),
                time_interval: TimeInterval::new(0, 1).unwrap(),
                spatial_resolution: SpatialResolution::new(1.0, 1.0).unwrap(),
                attributes: ColumnSelection::all(),
            })
            .await
            .unwrap();

        // In the test config, it says `active_schema=pg_temp` before it was replaced with the test schema.
        assert_common_prefix(
            loading_info.file_name.to_str().unwrap(),
            &db_config.ogr_pg_config(),
            "active_schema=",
        );

        assert_eq!(
            loading_info,
            OgrSourceDataset {
                file_name: loading_info.file_name.clone(),
                layer_name: WildliveLayerId::Projects.table_name().to_string(),
                data_type: Some(VectorDataType::MultiPolygon),
                time: OgrSourceDatasetTimeType::None,
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: None,
                    x: String::new(),
                    y: None,
                    int: vec![],
                    float: vec![],
                    text: vec![
                        "project_id".to_string(),
                        "name".to_string(),
                        "description".to_string(),
                    ],
                    bool: vec![],
                    datetime: vec![],
                    rename: None,
                }),
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Abort,
                sql_query: None,
                attribute_query: Some(
                    "cache_date = current_date AND provider_id = '0000009b-d30a-3c64-5943-dd1690a03a14'".to_string()
                ),
                cache_ttl: CacheTtlSeconds::new(3600),
            }
        );

        assert_eq!(
            metadata.result_descriptor().await.unwrap(),
            VectorResultDescriptor {
                data_type: VectorDataType::MultiPolygon,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: [
                    (
                        "project_id".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "name".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "description".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                ]
                .iter()
                .cloned()
                .collect(),
                time: None,
                bbox: None,
            }
        );
    }

    fn assert_common_prefix(a: &str, b: &str, until: &str) {
        let b_prefix = b.split_once(until).unwrap().0;
        assert!(
            a.starts_with(b_prefix),
            "{a} does not start with {b_prefix}"
        );
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_shows_a_captures_dataset(
        ctx: PostgresSessionContext<NoTls>,
        db_config: DatabaseConnectionConfig,
    ) {
        let mock_server = httptest::Server::run();

        mock_server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path("/api/objects/wildlive/ef7833589d61b2d2a905"),
            ])
            .respond_with(json_responder(test_data!(
                "wildlive/responses/project.json"
            ))),
        );

        mock_server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::path("/api/search"),
                request::body(matchers::json_decoded(matchers::eq(SearchRequest {
                    query: "(/inStationsLayout:\"wildlive/667cc39364fd45136c7a\" OR /inStationsLayout:\"wildlive/151c43fdd5881eba0bd5\") AND type:StationSetup".to_string(),
                    filter: Some(
                        [
                            "/id",
                            "/content/id",
                            "/content/name",
                            "/content/location",
                            "/content/description",
                            "/content/decimalLatitude",
                            "/content/decimalLongitude",
                        ]
                        .map(ToString::to_string)
                        .to_vec(),
                    ),
                    page_num: 0,
                    page_size: PAGE_SIZE,
                }))
            ),
            ])
            .respond_with(json_responder(test_data!(
                "wildlive/responses/station_setups.json"
            ))),
        );

        mock_server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::path("/api/search"),
                request::body(matchers::json_decoded(matchers::eq(SearchRequest {
                    query: "type:ImageObject AND (/atStation:\"wildlive/024b9357f1e23877a243\" OR /atStation:\"wildlive/0ff0ce1ddfcfb0aff407\" OR /atStation:\"wildlive/16f3b0b65b4a58acb782\" OR /atStation:\"wildlive/229392d20de8b45e8114\" OR /atStation:\"wildlive/259cfcfd85fcb0ce276d\" OR /atStation:\"wildlive/2cd0a46deb9e47b0518f\" OR /atStation:\"wildlive/3204d6391519562525ec\" OR /atStation:\"wildlive/33516c1ce3b7e26c296d\" OR /atStation:\"wildlive/358df8fa949f35e91a64\" OR /atStation:\"wildlive/468ba2036b2a4ff004c9\" OR /atStation:\"wildlive/498ae1629861699f5323\" OR /atStation:\"wildlive/52baefeffeb2648fdaf7\" OR /atStation:\"wildlive/6bf42fa2eb245604bb31\" OR /atStation:\"wildlive/797cb6f275e9fc8afa4b\" OR /atStation:\"wildlive/79e043c3053fb39df381\" OR /atStation:\"wildlive/8ced32ac3ca4f646a53b\" OR /atStation:\"wildlive/a43254afb230ce163256\" OR /atStation:\"wildlive/c2bd44066dbda6f0d1ac\" OR /atStation:\"wildlive/de7f4396c2689d1fbf6d\" OR /atStation:\"wildlive/ea64f18b8fa1dec31196\" OR /atStation:\"wildlive/f421dc2239b8fd7a1980\")".to_string(),
                    filter: Some(
                        [
                            "/id",
                            "/content/id",
                            "/content/captureTimeStamp",
                            "/content/atStation",
                            "/content/hasAnnotations",
                            "/content/contentUrl"
                        ]
                        .map(ToString::to_string)
                        .to_vec(),
                    ),
                    page_num: 0,
                    page_size: PAGE_SIZE,
                }))),
            ])
            .respond_with(json_responder(test_data!(
                "wildlive/responses/image_objects.json"
            ))),
        );

        mock_server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::path("/api/search"),
                request::body(matchers::json_decoded(matchers::eq(SearchRequest {
                    query: "type:Annotation AND (/id:\"wildlive/7ef5664c43cf26299b09\" OR /id:\"wildlive/ebe8d5f722782b0bee73\" OR /id:\"wildlive/a1eaa469eec33a0d3a39\")".to_string(),
                    filter: Some(
                        [
                            "/id",
                            "/content/id",
                            "/content/hasTarget",
                            "/content/hasBody",
                        ]
                        .map(ToString::to_string)
                        .to_vec(),
                    ),
                    page_num: 0,
                    page_size: PAGE_SIZE,
                }))),
            ])
            .respond_with(json_responder(test_data!(
                "wildlive/responses/annotations.json"
            ))),
        );

        // let api_endpoint = Url::parse("https://wildlive.senckenberg.de/api/").unwrap();
        let api_endpoint = Url::parse(&mock_server.url_str("/api/")).unwrap();

        // crate::util::tests::initialize_debugging_in_test();

        let connector = WildliveDataConnector {
            definition: WildliveDataConnectorDefinition {
                id: DataProviderId::from_u128(12_345_678_901_234_567_890_123_456_789_012_u128),
                name: "WildLIVE! Portal Connector".to_string(),
                description: "WildLIVE! Portal Connector".to_string(),
                auth: None,
                priority: None,
            },
            user: None,
            local_oidc_config: config::Oidc {
                enabled: true,
                issuer: join_base_url_and_path(&api_endpoint, "/auth/realms/wildlive-portal/")
                    .unwrap(),
                client_id: "geoengine".to_string(),
                client_secret: None,
                scopes: ["openid", "offline_access"]
                    .map(ToString::to_string)
                    .to_vec(),
                token_encryption_password: None,
            },
            wildlive_config: config::Wildlive {
                api_endpoint: api_endpoint.clone(),
                oidc: config::WildliveOidc {
                    issuer: join_base_url_and_path(&api_endpoint, "/auth/realms/wildlive-portal/")
                        .unwrap(),
                    client_id: "geoengine".to_string(),
                    client_secret: None,
                    broker_provider: "wildlive-portal".to_string(),
                },
            },
            db: Arc::new(ctx.db()),
        };

        let project_id = "wildlive/ef7833589d61b2d2a905";

        let layer = connector
            .load_layer(
                &WildliveLayerId::Captures {
                    project_id: project_id.into(),
                }
                .try_into()
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            layer,
            Layer {
                id: ProviderLayerId {
                    provider_id: connector.definition.id,
                    layer_id: WildliveLayerId::Captures {
                        project_id: project_id.into(),
                    }
                    .try_into()
                    .unwrap(),
                },
                name: format!("Captures for project {project_id}"),
                description: format!("Overview of all captures within project {project_id}"),
                workflow: Workflow {
                    operator: OgrSource {
                        params: OgrSourceParameters {
                            data: connector
                                .named_data(WildliveLayerId::Captures {
                                    project_id: project_id.into()
                                })
                                .unwrap(),
                            attribute_projection: None,
                            attribute_filters: None,
                        },
                    }
                    .boxed()
                    .into(),
                },
                symbology: None,
                properties: Vec::new(),
                metadata: Default::default(),
            }
        );

        let metadata: Box<
            dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
        > = MetaDataProvider::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
            ::meta_data(&connector, &DataId::External(ExternalDataId {
                provider_id: layer.id.provider_id,
                layer_id: layer.id.layer_id,
            }))
            .await
            .unwrap();

        let loading_info = metadata
            .loading_info(VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    Coordinate2D { x: 0.0, y: 0.0 },
                    Coordinate2D { x: 1.0, y: 1.0 },
                )
                .unwrap(),
                time_interval: TimeInterval::new(0, 1).unwrap(),
                spatial_resolution: SpatialResolution::new(1.0, 1.0).unwrap(),
                attributes: ColumnSelection::all(),
            })
            .await
            .unwrap();

        // In the test config, it says `active_schema=pg_temp` before it was replaced with the test schema.
        assert_common_prefix(
            loading_info.file_name.to_str().unwrap(),
            &db_config.ogr_pg_config(),
            "active_schema=",
        );

        assert_eq!(
            loading_info,
            OgrSourceDataset {
                file_name: loading_info.file_name.clone(),
                layer_name: WildliveLayerId::Captures {
                    project_id: project_id.into()
                }
                .table_name()
                .to_string(),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::Start {
                    start_field: "capture_time_stamp".into(),
                    start_format: OgrSourceTimeFormat::Auto,
                    duration: OgrSourceDurationSpec::Zero
                },
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: None,
                    x: String::new(),
                    y: None,
                    int: vec![],
                    float: vec![],
                    text: [
                        "image_object_id",
                        "project_id",
                        "station_setup_id",
                        "accepted_name_usage_id",
                        "vernacular_name",
                        "scientific_name",
                        "content_url",
                    ]
                    .map(ToString::to_string)
                    .to_vec(),
                    bool: vec![],
                    datetime: ["capture_time_stamp"].map(ToString::to_string).to_vec(),
                    rename: None,
                }),
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Abort,
                sql_query: None,
                attribute_query: Some(format!(
                    "cache_date = current_date AND provider_id = '0000009b-d30a-3c64-5943-dd1690a03a14' AND project_id = '{project_id}'"
                )),
                cache_ttl: CacheTtlSeconds::new(3600),
            }
        );

        assert_eq!(
            metadata.result_descriptor().await.unwrap(),
            VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: [
                    (
                        "accepted_name_usage_id".into(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless
                        }
                    ),
                    (
                        "vernacular_name".into(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless
                        }
                    ),
                    (
                        "scientific_name".into(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless
                        }
                    ),
                    (
                        "station_setup_id".into(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless
                        }
                    ),
                    (
                        "content_url".into(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless
                        },
                    ),
                    (
                        "image_object_id".into(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless
                        },
                    ),
                    (
                        "project_id".into(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless
                        }
                    ),
                    (
                        "capture_time_stamp".into(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::DateTime,
                            measurement: Measurement::Unitless
                        }
                    )
                ]
                .iter()
                .cloned()
                .collect(),
                time: None,
                bbox: None,
            }
        );
    }

    // TODO: add test that ensures that the user is set correctly

    // TODO: add test that ensures that the auth token is refreshed correctly
}
