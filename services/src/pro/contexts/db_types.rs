use crate::{
    error::Error,
    pro::datasets::{
        CopernicusDataspaceDataProviderDefinition, GdalRetries,
        SentinelS2L2ACogsProviderDefinition, StacApiRetries, TypedProDataProviderDefinition,
    },
};
use geoengine_datatypes::{dataset::DataProviderId, delegate_from_to_sql, util::StringPair};
use postgres_types::{FromSql, ToSql};

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "StacApiRetries")]
pub struct StacApiRetriesDbType {
    pub number_of_retries: i64,
    pub initial_delay_ms: i64,
    pub exponential_backoff_factor: f64,
}

impl From<&StacApiRetries> for StacApiRetriesDbType {
    fn from(other: &StacApiRetries) -> Self {
        Self {
            number_of_retries: other.number_of_retries as i64,
            initial_delay_ms: other.initial_delay_ms as i64,
            exponential_backoff_factor: other.exponential_backoff_factor,
        }
    }
}

impl TryFrom<StacApiRetriesDbType> for StacApiRetries {
    type Error = Error;

    fn try_from(other: StacApiRetriesDbType) -> Result<Self, Self::Error> {
        Ok(Self {
            number_of_retries: other.number_of_retries as usize,
            initial_delay_ms: other.initial_delay_ms as u64,
            exponential_backoff_factor: other.exponential_backoff_factor,
        })
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "GdalRetries")]
pub struct GdalRetriesDbType {
    pub number_of_retries: i64,
}

impl From<&GdalRetries> for GdalRetriesDbType {
    fn from(other: &GdalRetries) -> Self {
        Self {
            number_of_retries: other.number_of_retries as i64,
        }
    }
}

impl TryFrom<GdalRetriesDbType> for GdalRetries {
    type Error = Error;

    fn try_from(other: GdalRetriesDbType) -> Result<Self, Self::Error> {
        Ok(Self {
            number_of_retries: other.number_of_retries as usize,
        })
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "ProDataProviderDefinition")]
pub struct TypedProDataProviderDefinitionDbType {
    copernicus_dataspace_provider_definition: Option<CopernicusDataspaceDataProviderDefinition>,
    sentinel_s2_l2_a_cogs_provider_definition: Option<SentinelS2L2ACogsProviderDefinition>,
}

impl From<&TypedProDataProviderDefinition> for TypedProDataProviderDefinitionDbType {
    fn from(other: &TypedProDataProviderDefinition) -> Self {
        match other {
            TypedProDataProviderDefinition::CopernicusDataspaceDataProviderDefinition(
                data_provider_definition,
            ) => Self {
                copernicus_dataspace_provider_definition: Some(data_provider_definition.clone()),
                sentinel_s2_l2_a_cogs_provider_definition: None,
            },
            TypedProDataProviderDefinition::SentinelS2L2ACogsProviderDefinition(
                data_provider_definition,
            ) => Self {
                copernicus_dataspace_provider_definition: None,
                sentinel_s2_l2_a_cogs_provider_definition: Some(data_provider_definition.clone()),
            },
        }
    }
}

impl TryFrom<TypedProDataProviderDefinitionDbType> for TypedProDataProviderDefinition {
    type Error = Error;

    #[allow(clippy::too_many_lines)]
    fn try_from(
        result_descriptor: TypedProDataProviderDefinitionDbType,
    ) -> Result<Self, Self::Error> {
        match result_descriptor {
            TypedProDataProviderDefinitionDbType {
                copernicus_dataspace_provider_definition: None,
                sentinel_s2_l2_a_cogs_provider_definition: Some(data_provider_definition),
            } => Ok(
                TypedProDataProviderDefinition::SentinelS2L2ACogsProviderDefinition(
                    data_provider_definition,
                ),
            ),
            TypedProDataProviderDefinitionDbType {
                copernicus_dataspace_provider_definition: Some(data_provider_definition),
                sentinel_s2_l2_a_cogs_provider_definition: None,
            } => Ok(
                TypedProDataProviderDefinition::CopernicusDataspaceDataProviderDefinition(
                    data_provider_definition,
                ),
            ),

            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "CopernicusDataspaceDataProviderDefinition")]
pub struct CopernicusDataspaceDataProviderDefinitionDbType {
    pub name: String,
    pub description: String,
    pub id: DataProviderId,
    pub stac_url: String,
    pub s3_url: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub gdal_config: Vec<StringPair>,
    pub priority: Option<i16>,
}

impl From<&CopernicusDataspaceDataProviderDefinition>
    for CopernicusDataspaceDataProviderDefinitionDbType
{
    fn from(value: &CopernicusDataspaceDataProviderDefinition) -> Self {
        Self {
            name: value.name.clone(),
            description: value.description.clone(),
            id: value.id,
            stac_url: value.stac_url.clone(),
            s3_url: value.s3_url.clone(),
            s3_access_key: value.s3_access_key.clone(),
            s3_secret_key: value.s3_secret_key.clone(),
            gdal_config: value.gdal_config.iter().map(|v| v.clone().into()).collect(),
            priority: value.priority,
        }
    }
}

impl TryFrom<CopernicusDataspaceDataProviderDefinitionDbType>
    for CopernicusDataspaceDataProviderDefinition
{
    type Error = Error;

    fn try_from(
        value: CopernicusDataspaceDataProviderDefinitionDbType,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.clone(),
            description: value.description.clone(),
            id: value.id,
            stac_url: value.stac_url.clone(),
            s3_url: value.s3_url.clone(),
            s3_access_key: value.s3_access_key.clone(),
            s3_secret_key: value.s3_secret_key.clone(),
            gdal_config: value
                .gdal_config
                .iter()
                .map(|v| v.clone().into_inner().into())
                .collect(),
            priority: value.priority,
        })
    }
}

delegate_from_to_sql!(GdalRetries, GdalRetriesDbType);
delegate_from_to_sql!(StacApiRetries, StacApiRetriesDbType);
delegate_from_to_sql!(
    CopernicusDataspaceDataProviderDefinition,
    CopernicusDataspaceDataProviderDefinitionDbType
);
delegate_from_to_sql!(
    TypedProDataProviderDefinition,
    TypedProDataProviderDefinitionDbType
);

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        dataset::DataProviderId, primitives::CacheTtlSeconds, util::Identifier,
    };

    use super::*;
    use crate::{
        pro::datasets::{StacBand, StacQueryBuffer, StacZone},
        util::postgres::assert_sql_type,
        util::tests::with_temp_context,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::too_many_lines)]
    async fn test_postgres_type_serialization() {
        with_temp_context(|app_ctx, _| async move {
            let pool = app_ctx.pool.get().await.unwrap();

            assert_sql_type(
                &pool,
                "StacApiRetries",
                [StacApiRetries {
                    number_of_retries: 3,
                    initial_delay_ms: 4,
                    exponential_backoff_factor: 5.,
                }],
            )
            .await;

            assert_sql_type(
                &pool,
                "GdalRetries",
                [GdalRetries {
                    number_of_retries: 3,
                }],
            )
            .await;

            assert_sql_type(
                &pool,
                "StacBand",
                [StacBand {
                    name: "band".to_owned(),
                    no_data_value: Some(133.7),
                    data_type: geoengine_datatypes::raster::RasterDataType::F32,
                }],
            )
            .await;

            assert_sql_type(
                &pool,
                "StacZone",
                [StacZone {
                    name: "zone".to_owned(),
                    epsg: 4326,
                }],
            )
            .await;

            assert_sql_type(
                &pool,
                "SentinelS2L2ACogsProviderDefinition",
                [SentinelS2L2ACogsProviderDefinition {
                    name: "foo".to_owned(),
                    id: DataProviderId::new(),
                    description: "A provider".to_owned(),
                    priority: Some(1),
                    api_url: "http://api.url".to_owned(),
                    bands: vec![StacBand {
                        name: "band".to_owned(),
                        no_data_value: Some(133.7),
                        data_type: geoengine_datatypes::raster::RasterDataType::F32,
                    }],
                    zones: vec![StacZone {
                        name: "zone".to_owned(),
                        epsg: 4326,
                    }],
                    stac_api_retries: StacApiRetries {
                        number_of_retries: 3,
                        initial_delay_ms: 4,
                        exponential_backoff_factor: 5.,
                    },
                    gdal_retries: GdalRetries {
                        number_of_retries: 3,
                    },
                    cache_ttl: CacheTtlSeconds::new(60),
                    query_buffer: StacQueryBuffer {
                        start_seconds: 1,
                        end_seconds: 1,
                    },
                }],
            )
            .await;

            assert_sql_type(
                &pool,
                "CopernicusDataspaceDataProviderDefinition",
                [CopernicusDataspaceDataProviderDefinition {
                    name: "foo".to_owned(),
                    description: "A provider".to_owned(),
                    priority: Some(3),
                    id: DataProviderId::new(),
                    stac_url: "https://catalogue.dataspace.copernicus.eu/stac".to_string(),
                    s3_url: "dataspace.copernicus.eu".to_string(),
                    s3_access_key: "XYZ".to_string(),
                    s3_secret_key: "XYZ".to_string(),
                    gdal_config: vec![("key".to_owned(), "value".to_owned()).into()],
                }],
            )
            .await;

            assert_sql_type(
                &pool,
                "ProDataProviderDefinition",
                [
                    TypedProDataProviderDefinition::CopernicusDataspaceDataProviderDefinition(
                        CopernicusDataspaceDataProviderDefinition {
                            name: "foo".to_owned(),
                            description: "A provider".to_owned(),
                            priority: Some(3),
                            id: DataProviderId::new(),
                            stac_url: "https://catalogue.dataspace.copernicus.eu/stac".to_string(),
                            s3_url: "dataspace.copernicus.eu".to_string(),
                            s3_access_key: "XYZ".to_string(),
                            s3_secret_key: "XYZ".to_string(),
                            gdal_config: vec![("key".to_owned(), "value".to_owned()).into()],
                        },
                    ),
                    TypedProDataProviderDefinition::SentinelS2L2ACogsProviderDefinition(
                        SentinelS2L2ACogsProviderDefinition {
                            name: "foo".to_owned(),
                            description: "A provider".to_owned(),
                            priority: Some(3),
                            id: DataProviderId::new(),
                            api_url: "http://api.url".to_owned(),
                            bands: vec![StacBand {
                                name: "band".to_owned(),
                                no_data_value: Some(133.7),
                                data_type: geoengine_datatypes::raster::RasterDataType::F32,
                            }],
                            zones: vec![StacZone {
                                name: "zone".to_owned(),
                                epsg: 4326,
                            }],
                            stac_api_retries: StacApiRetries {
                                number_of_retries: 3,
                                initial_delay_ms: 4,
                                exponential_backoff_factor: 5.,
                            },
                            gdal_retries: GdalRetries {
                                number_of_retries: 3,
                            },
                            cache_ttl: CacheTtlSeconds::new(60),
                            query_buffer: StacQueryBuffer {
                                start_seconds: 1,
                                end_seconds: 1,
                            },
                        },
                    ),
                ],
            )
            .await;
        })
        .await;
    }
}
