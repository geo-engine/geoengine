use crate::{
    delegate_from_to_sql,
    error::Error,
    pro::datasets::{
        GdalRetries, SentinelS2L2ACogsProviderDefinition, StacApiRetries,
        TypedProDataProviderDefinition,
    },
};
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
    sentinel_s2_l2_a_cogs_provider_definition: Option<SentinelS2L2ACogsProviderDefinition>,
}

impl From<&TypedProDataProviderDefinition> for TypedProDataProviderDefinitionDbType {
    fn from(other: &TypedProDataProviderDefinition) -> Self {
        match other {
            TypedProDataProviderDefinition::SentinelS2L2ACogsProviderDefinition(
                data_provider_definition,
            ) => Self {
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
                sentinel_s2_l2_a_cogs_provider_definition: Some(data_provider_definition),
            } => Ok(
                TypedProDataProviderDefinition::SentinelS2L2ACogsProviderDefinition(
                    data_provider_definition,
                ),
            ),

            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

delegate_from_to_sql!(GdalRetries, GdalRetriesDbType);
delegate_from_to_sql!(StacApiRetries, StacApiRetriesDbType);
delegate_from_to_sql!(
    TypedProDataProviderDefinition,
    TypedProDataProviderDefinitionDbType
);

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{primitives::CacheTtlSeconds, util::Identifier};

    use super::*;
    use crate::{
        api::model::datatypes::DataProviderId,
        pro::{
            datasets::{StacBand, StacZone},
            util::tests::with_pro_temp_context,
        },
        util::postgres::assert_sql_type,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::too_many_lines)]
    async fn test_postgres_type_serialization() {
        with_pro_temp_context(|app_ctx, _| async move {
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
                }],
            )
            .await;

            assert_sql_type(
                &pool,
                "ProDataProviderDefinition",
                [
                    TypedProDataProviderDefinition::SentinelS2L2ACogsProviderDefinition(
                        SentinelS2L2ACogsProviderDefinition {
                            name: "foo".to_owned(),
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
                        },
                    ),
                ],
            )
            .await;
        })
        .await;
    }
}
