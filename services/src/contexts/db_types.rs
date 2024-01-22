use crate::{
    datasets::{
        dataset_listing_provider::DatasetLayerListingProviderDefinition,
        external::{
            aruna::ArunaDataProviderDefinition,
            edr::{EdrDataProviderDefinition, EdrVectorSpec},
            gbif::GbifDataProviderDefinition,
            gfbio_abcd::GfbioAbcdDataProviderDefinition,
            gfbio_collections::GfbioCollectionsDataProviderDefinition,
            netcdfcf::{EbvPortalDataProviderDefinition, NetCdfCfDataProviderDefinition},
            pangaea::PangaeaDataProviderDefinition,
        },
        listing::Provenance,
        storage::MetaDataDefinition,
    },
    error::Error,
    layers::external::TypedDataProviderDefinition,
    projects::{
        ColorParam, DerivedColor, DerivedNumber, LineSymbology, NumberParam, PointSymbology,
        PolygonSymbology, RasterSymbology, Symbology,
    },
    util::postgres::DatabaseConnectionConfig,
};
use geoengine_datatypes::{
    dataset::DataProviderId,
    delegate_from_to_sql,
    operations::image::{Colorizer, RgbaColor},
    primitives::{CacheTtlSeconds, VectorQueryRectangle},
};
use geoengine_operators::{
    engine::{StaticMetaData, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{
        GdalMetaDataList, GdalMetaDataRegular, GdalMetaDataStatic, GdalMetadataNetCdfCf,
        OgrSourceDataset,
    },
};
use postgres_types::{FromSql, ToSql};

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "ColorParam")]
pub struct ColorParamDbType {
    color: Option<RgbaColor>,
    attribute: Option<String>,
    colorizer: Option<Colorizer>,
}

impl From<&ColorParam> for ColorParamDbType {
    fn from(value: &ColorParam) -> Self {
        match value {
            ColorParam::Static { color } => Self {
                color: Some(*color),
                attribute: None,
                colorizer: None,
            },
            ColorParam::Derived(DerivedColor {
                attribute,
                colorizer,
            }) => Self {
                color: None,
                attribute: Some(attribute.clone()),
                colorizer: Some(colorizer.clone()),
            },
        }
    }
}

impl TryFrom<ColorParamDbType> for ColorParam {
    type Error = Error;

    fn try_from(value: ColorParamDbType) -> Result<Self, Self::Error> {
        match value {
            ColorParamDbType {
                color: Some(color),
                attribute: None,
                colorizer: None,
            } => Ok(Self::Static { color }),
            ColorParamDbType {
                color: None,
                attribute: Some(attribute),
                colorizer: Some(colorizer),
            } => Ok(Self::Derived(DerivedColor {
                attribute,
                colorizer,
            })),
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "NumberParam")]
pub struct NumberParamDbType {
    value: Option<i64>,
    attribute: Option<String>,
    factor: Option<f64>,
    default_value: Option<f64>,
}

impl From<&NumberParam> for NumberParamDbType {
    fn from(value: &NumberParam) -> Self {
        match value {
            NumberParam::Static { value } => Self {
                value: Some(*value as i64),
                attribute: None,
                factor: None,
                default_value: None,
            },
            NumberParam::Derived(DerivedNumber {
                attribute,
                factor,
                default_value,
            }) => Self {
                value: None,
                attribute: Some(attribute.clone()),
                factor: Some(*factor),
                default_value: Some(*default_value),
            },
        }
    }
}

impl TryFrom<NumberParamDbType> for NumberParam {
    type Error = Error;

    fn try_from(value: NumberParamDbType) -> Result<Self, Self::Error> {
        match value {
            NumberParamDbType {
                value: Some(value),
                attribute: None,
                factor: None,
                default_value: None,
            } => Ok(Self::Static {
                value: value as usize,
            }),
            NumberParamDbType {
                value: None,
                attribute: Some(attribute),
                factor: Some(factor),
                default_value: Some(default_value),
            } => Ok(Self::Derived(DerivedNumber {
                attribute,
                factor,
                default_value,
            })),
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "Symbology")]
pub struct SymbologyDbType {
    raster: Option<RasterSymbology>,
    point: Option<PointSymbology>,
    line: Option<LineSymbology>,
    polygon: Option<PolygonSymbology>,
}

impl From<&Symbology> for SymbologyDbType {
    fn from(symbology: &Symbology) -> Self {
        match symbology {
            Symbology::Raster(raster) => SymbologyDbType {
                raster: Some(raster.clone()),
                point: None,
                line: None,
                polygon: None,
            },
            Symbology::Point(point) => SymbologyDbType {
                raster: None,
                point: Some(point.clone()),
                line: None,
                polygon: None,
            },
            Symbology::Line(line) => SymbologyDbType {
                raster: None,
                point: None,
                line: Some(line.clone()),
                polygon: None,
            },
            Symbology::Polygon(polygon) => SymbologyDbType {
                raster: None,
                point: None,
                line: None,
                polygon: Some(polygon.clone()),
            },
        }
    }
}

impl TryFrom<SymbologyDbType> for Symbology {
    type Error = Error;

    fn try_from(symbology: SymbologyDbType) -> Result<Self, Self::Error> {
        match symbology {
            SymbologyDbType {
                raster: Some(raster),
                point: None,
                line: None,
                polygon: None,
            } => Ok(Self::Raster(raster)),
            SymbologyDbType {
                raster: None,
                point: Some(point),
                line: None,
                polygon: None,
            } => Ok(Self::Point(point)),
            SymbologyDbType {
                raster: None,
                point: None,
                line: Some(line),
                polygon: None,
            } => Ok(Self::Line(line)),
            SymbologyDbType {
                raster: None,
                point: None,
                line: None,
                polygon: Some(polygon),
            } => Ok(Self::Polygon(polygon)),
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "MetaDataDefinition")]
pub struct MetaDataDefinitionDbType {
    mock_meta_data: Option<
        StaticMetaData<
            MockDatasetDataSourceLoadingInfo,
            VectorResultDescriptor,
            VectorQueryRectangle,
        >,
    >,
    ogr_meta_data:
        Option<StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
    gdal_meta_data_regular: Option<GdalMetaDataRegular>,
    gdal_static: Option<GdalMetaDataStatic>,
    gdal_metadata_net_cdf_cf: Option<GdalMetadataNetCdfCf>,
    gdal_meta_data_list: Option<GdalMetaDataList>,
}

impl From<&MetaDataDefinition> for MetaDataDefinitionDbType {
    fn from(other: &MetaDataDefinition) -> Self {
        match other {
            MetaDataDefinition::MockMetaData(meta_data) => Self {
                mock_meta_data: Some(meta_data.clone()),
                ogr_meta_data: None,
                gdal_meta_data_regular: None,
                gdal_static: None,
                gdal_metadata_net_cdf_cf: None,
                gdal_meta_data_list: None,
            },
            MetaDataDefinition::OgrMetaData(meta_data) => Self {
                mock_meta_data: None,
                ogr_meta_data: Some(meta_data.clone()),
                gdal_meta_data_regular: None,
                gdal_static: None,
                gdal_metadata_net_cdf_cf: None,
                gdal_meta_data_list: None,
            },
            MetaDataDefinition::GdalMetaDataRegular(meta_data) => Self {
                mock_meta_data: None,
                ogr_meta_data: None,
                gdal_meta_data_regular: Some(meta_data.clone()),
                gdal_static: None,
                gdal_metadata_net_cdf_cf: None,
                gdal_meta_data_list: None,
            },
            MetaDataDefinition::GdalStatic(meta_data) => Self {
                mock_meta_data: None,
                ogr_meta_data: None,
                gdal_meta_data_regular: None,
                gdal_static: Some(meta_data.clone()),
                gdal_metadata_net_cdf_cf: None,
                gdal_meta_data_list: None,
            },
            MetaDataDefinition::GdalMetadataNetCdfCf(meta_data) => Self {
                mock_meta_data: None,
                ogr_meta_data: None,
                gdal_meta_data_regular: None,
                gdal_static: None,
                gdal_metadata_net_cdf_cf: Some(meta_data.clone()),
                gdal_meta_data_list: None,
            },
            MetaDataDefinition::GdalMetaDataList(meta_data) => Self {
                mock_meta_data: None,
                ogr_meta_data: None,
                gdal_meta_data_regular: None,
                gdal_static: None,
                gdal_metadata_net_cdf_cf: None,
                gdal_meta_data_list: Some(meta_data.clone()),
            },
        }
    }
}

impl TryFrom<MetaDataDefinitionDbType> for MetaDataDefinition {
    type Error = Error;

    fn try_from(other: MetaDataDefinitionDbType) -> Result<Self, Self::Error> {
        match other {
            MetaDataDefinitionDbType {
                mock_meta_data: Some(meta_data),
                ogr_meta_data: None,
                gdal_meta_data_regular: None,
                gdal_static: None,
                gdal_metadata_net_cdf_cf: None,
                gdal_meta_data_list: None,
            } => Ok(MetaDataDefinition::MockMetaData(meta_data)),
            MetaDataDefinitionDbType {
                mock_meta_data: None,
                ogr_meta_data: Some(meta_data),
                gdal_meta_data_regular: None,
                gdal_static: None,
                gdal_metadata_net_cdf_cf: None,
                gdal_meta_data_list: None,
            } => Ok(MetaDataDefinition::OgrMetaData(meta_data)),
            MetaDataDefinitionDbType {
                mock_meta_data: None,
                ogr_meta_data: None,
                gdal_meta_data_regular: Some(meta_data),
                gdal_static: None,
                gdal_metadata_net_cdf_cf: None,
                gdal_meta_data_list: None,
            } => Ok(MetaDataDefinition::GdalMetaDataRegular(meta_data)),
            MetaDataDefinitionDbType {
                mock_meta_data: None,
                ogr_meta_data: None,
                gdal_meta_data_regular: None,
                gdal_static: Some(meta_data),
                gdal_metadata_net_cdf_cf: None,
                gdal_meta_data_list: None,
            } => Ok(MetaDataDefinition::GdalStatic(meta_data)),
            MetaDataDefinitionDbType {
                mock_meta_data: None,
                ogr_meta_data: None,
                gdal_meta_data_regular: None,
                gdal_static: None,
                gdal_metadata_net_cdf_cf: Some(meta_data),
                gdal_meta_data_list: None,
            } => Ok(MetaDataDefinition::GdalMetadataNetCdfCf(meta_data)),
            MetaDataDefinitionDbType {
                mock_meta_data: None,
                ogr_meta_data: None,
                gdal_meta_data_regular: None,
                gdal_static: None,
                gdal_metadata_net_cdf_cf: None,
                gdal_meta_data_list: Some(meta_data),
            } => Ok(MetaDataDefinition::GdalMetaDataList(meta_data)),
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "DatabaseConnectionConfig")]
pub struct DatabaseConnectionConfigDbType {
    pub host: String,
    pub port: i32,
    pub database: String,
    pub schema: String,
    pub user: String,
    pub password: String,
}

impl From<&DatabaseConnectionConfig> for DatabaseConnectionConfigDbType {
    fn from(other: &DatabaseConnectionConfig) -> Self {
        Self {
            host: other.host.clone(),
            port: i32::from(other.port),
            database: other.database.clone(),
            schema: other.schema.clone(),
            user: other.user.clone(),
            password: other.password.clone(),
        }
    }
}

impl TryFrom<DatabaseConnectionConfigDbType> for DatabaseConnectionConfig {
    type Error = Error;

    fn try_from(other: DatabaseConnectionConfigDbType) -> Result<Self, Self::Error> {
        Ok(Self {
            host: other.host,
            port: other.port as u16,
            database: other.database,
            schema: other.schema,
            user: other.user,
            password: other.password,
        })
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "GfbioCollectionsDataProviderDefinition")]
pub struct GfbioCollectionsDataProviderDefinitionDbType {
    pub name: String,
    pub collection_api_url: String,
    pub collection_api_auth_token: String,
    pub abcd_db_config: DatabaseConnectionConfig,
    pub pangaea_url: String,
    pub cache_ttl: CacheTtlSeconds,
}

impl From<&GfbioCollectionsDataProviderDefinition>
    for GfbioCollectionsDataProviderDefinitionDbType
{
    fn from(other: &GfbioCollectionsDataProviderDefinition) -> Self {
        Self {
            name: other.name.clone(),
            collection_api_url: other.collection_api_url.clone().into(),
            collection_api_auth_token: other.collection_api_auth_token.clone(),
            abcd_db_config: other.abcd_db_config.clone(),
            pangaea_url: other.pangaea_url.clone().into(),
            cache_ttl: other.cache_ttl,
        }
    }
}

impl TryFrom<GfbioCollectionsDataProviderDefinitionDbType>
    for GfbioCollectionsDataProviderDefinition
{
    type Error = Error;

    fn try_from(other: GfbioCollectionsDataProviderDefinitionDbType) -> Result<Self, Self::Error> {
        Ok(Self {
            name: other.name,
            collection_api_url: other.collection_api_url.as_str().try_into()?,
            collection_api_auth_token: other.collection_api_auth_token,
            abcd_db_config: other.abcd_db_config,
            pangaea_url: other.pangaea_url.as_str().try_into()?,
            cache_ttl: other.cache_ttl,
        })
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "EbvPortalDataProviderDefinition")]
pub struct EbvPortalDataProviderDefinitionDbType {
    pub name: String,
    pub path: String,
    pub base_url: String,
    pub overviews: String,
    pub cache_ttl: CacheTtlSeconds,
}

impl From<&EbvPortalDataProviderDefinition> for EbvPortalDataProviderDefinitionDbType {
    fn from(other: &EbvPortalDataProviderDefinition) -> Self {
        Self {
            name: other.name.clone(),
            path: other.path.to_string_lossy().to_string(),
            base_url: other.base_url.clone().into(),
            overviews: other.overviews.to_string_lossy().to_string(),
            cache_ttl: other.cache_ttl,
        }
    }
}

impl TryFrom<EbvPortalDataProviderDefinitionDbType> for EbvPortalDataProviderDefinition {
    type Error = Error;

    fn try_from(other: EbvPortalDataProviderDefinitionDbType) -> Result<Self, Self::Error> {
        Ok(Self {
            name: other.name,
            path: other.path.into(),
            base_url: other.base_url.as_str().try_into()?,
            overviews: other.overviews.into(),
            cache_ttl: other.cache_ttl,
        })
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "NetCdfCfDataProviderDefinition")]
pub struct NetCdfCfDataProviderDefinitionDbType {
    pub name: String,
    pub path: String,
    pub overviews: String,
    pub cache_ttl: CacheTtlSeconds,
}

impl From<&NetCdfCfDataProviderDefinition> for NetCdfCfDataProviderDefinitionDbType {
    fn from(other: &NetCdfCfDataProviderDefinition) -> Self {
        Self {
            name: other.name.clone(),
            path: other.path.to_string_lossy().to_string(),
            overviews: other.overviews.to_string_lossy().to_string(),
            cache_ttl: other.cache_ttl,
        }
    }
}

impl TryFrom<NetCdfCfDataProviderDefinitionDbType> for NetCdfCfDataProviderDefinition {
    type Error = Error;

    fn try_from(other: NetCdfCfDataProviderDefinitionDbType) -> Result<Self, Self::Error> {
        Ok(Self {
            name: other.name,
            path: other.path.into(),
            overviews: other.overviews.into(),
            cache_ttl: other.cache_ttl,
        })
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "PangaeaDataProviderDefinition")]
pub struct PangaeaDataProviderDefinitionDbType {
    pub name: String,
    pub base_url: String,
    pub cache_ttl: CacheTtlSeconds,
}

impl From<&PangaeaDataProviderDefinition> for PangaeaDataProviderDefinitionDbType {
    fn from(other: &PangaeaDataProviderDefinition) -> Self {
        Self {
            name: other.name.clone(),
            base_url: other.base_url.clone().into(),
            cache_ttl: other.cache_ttl,
        }
    }
}

impl TryFrom<PangaeaDataProviderDefinitionDbType> for PangaeaDataProviderDefinition {
    type Error = Error;

    fn try_from(other: PangaeaDataProviderDefinitionDbType) -> Result<Self, Self::Error> {
        Ok(Self {
            name: other.name,
            base_url: other.base_url.as_str().try_into()?,
            cache_ttl: other.cache_ttl,
        })
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "EdrVectorSpec")]
pub struct EdrVectorSpecDbType {
    x: String,
    y: Option<String>,
    time: String,
}

impl From<&EdrVectorSpec> for EdrVectorSpecDbType {
    fn from(other: &EdrVectorSpec) -> Self {
        Self {
            x: other.x.clone(),
            y: other.y.clone(),
            time: other.time.clone(),
        }
    }
}

impl TryFrom<EdrVectorSpecDbType> for EdrVectorSpec {
    type Error = Error;

    fn try_from(other: EdrVectorSpecDbType) -> Result<Self, Self::Error> {
        Ok(Self {
            x: other.x,
            y: other.y,
            time: other.time,
        })
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "EdrDataProviderDefinition")]
pub struct EdrDataProviderDefinitionDbType {
    name: String,
    id: DataProviderId,
    base_url: String,
    vector_spec: Option<EdrVectorSpec>,
    cache_ttl: CacheTtlSeconds,
    /// List of vertical reference systems with a discrete scale
    discrete_vrs: Vec<String>,
    provenance: Option<Vec<Provenance>>,
}

impl From<&EdrDataProviderDefinition> for EdrDataProviderDefinitionDbType {
    fn from(other: &EdrDataProviderDefinition) -> Self {
        Self {
            name: other.name.clone(),
            id: other.id,
            base_url: other.base_url.clone().into(),
            vector_spec: other.vector_spec.clone(),
            cache_ttl: other.cache_ttl,
            discrete_vrs: other.discrete_vrs.clone(),
            provenance: other.provenance.clone(),
        }
    }
}

impl TryFrom<EdrDataProviderDefinitionDbType> for EdrDataProviderDefinition {
    type Error = Error;

    fn try_from(other: EdrDataProviderDefinitionDbType) -> Result<Self, Self::Error> {
        Ok(Self {
            name: other.name,
            id: other.id,
            base_url: other.base_url.as_str().try_into()?,
            vector_spec: other.vector_spec,
            cache_ttl: other.cache_ttl,
            discrete_vrs: other.discrete_vrs,
            provenance: other.provenance,
        })
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "DataProviderDefinition")]
#[allow(clippy::struct_field_names)] // same postfix because of postgres mapping
pub struct TypedDataProviderDefinitionDbType {
    aruna_data_provider_definition: Option<ArunaDataProviderDefinition>,
    dataset_layer_listing_provider_definition: Option<DatasetLayerListingProviderDefinition>,
    gbif_data_provider_definition: Option<GbifDataProviderDefinition>,
    gfbio_abcd_data_provider_definition: Option<GfbioAbcdDataProviderDefinition>,
    gfbio_collections_data_provider_definition: Option<GfbioCollectionsDataProviderDefinition>,
    ebv_portal_data_provider_definition: Option<EbvPortalDataProviderDefinition>,
    net_cdf_cf_data_provider_definition: Option<NetCdfCfDataProviderDefinition>,
    pangaea_data_provider_definition: Option<PangaeaDataProviderDefinition>,
    edr_data_provider_definition: Option<EdrDataProviderDefinition>,
}

#[allow(clippy::too_many_lines)]
impl From<&TypedDataProviderDefinition> for TypedDataProviderDefinitionDbType {
    fn from(other: &TypedDataProviderDefinition) -> Self {
        match other {
            TypedDataProviderDefinition::ArunaDataProviderDefinition(data_provider_definition) => {
                Self {
                    aruna_data_provider_definition: Some(data_provider_definition.clone()),
                    dataset_layer_listing_provider_definition: None,
                    gbif_data_provider_definition: None,
                    gfbio_abcd_data_provider_definition: None,
                    gfbio_collections_data_provider_definition: None,
                    ebv_portal_data_provider_definition: None,
                    net_cdf_cf_data_provider_definition: None,
                    pangaea_data_provider_definition: None,
                    edr_data_provider_definition: None,
                }
            }
            TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(
                data_provider_definition,
            ) => Self {
                aruna_data_provider_definition: None,
                dataset_layer_listing_provider_definition: Some(data_provider_definition.clone()),
                gbif_data_provider_definition: None,
                gfbio_abcd_data_provider_definition: None,
                gfbio_collections_data_provider_definition: None,
                ebv_portal_data_provider_definition: None,
                net_cdf_cf_data_provider_definition: None,
                pangaea_data_provider_definition: None,
                edr_data_provider_definition: None,
            },
            TypedDataProviderDefinition::GbifDataProviderDefinition(data_provider_definition) => {
                Self {
                    aruna_data_provider_definition: None,
                    dataset_layer_listing_provider_definition: None,
                    gbif_data_provider_definition: Some(data_provider_definition.clone()),
                    gfbio_abcd_data_provider_definition: None,
                    gfbio_collections_data_provider_definition: None,
                    ebv_portal_data_provider_definition: None,
                    net_cdf_cf_data_provider_definition: None,
                    pangaea_data_provider_definition: None,
                    edr_data_provider_definition: None,
                }
            }
            TypedDataProviderDefinition::GfbioAbcdDataProviderDefinition(
                data_provider_definition,
            ) => Self {
                aruna_data_provider_definition: None,
                dataset_layer_listing_provider_definition: None,
                gbif_data_provider_definition: None,
                gfbio_abcd_data_provider_definition: Some(data_provider_definition.clone()),
                gfbio_collections_data_provider_definition: None,
                ebv_portal_data_provider_definition: None,
                net_cdf_cf_data_provider_definition: None,
                pangaea_data_provider_definition: None,
                edr_data_provider_definition: None,
            },
            TypedDataProviderDefinition::GfbioCollectionsDataProviderDefinition(
                data_provider_definition,
            ) => Self {
                aruna_data_provider_definition: None,
                dataset_layer_listing_provider_definition: None,
                gbif_data_provider_definition: None,
                gfbio_abcd_data_provider_definition: None,
                gfbio_collections_data_provider_definition: Some(data_provider_definition.clone()),
                ebv_portal_data_provider_definition: None,
                net_cdf_cf_data_provider_definition: None,
                pangaea_data_provider_definition: None,
                edr_data_provider_definition: None,
            },
            TypedDataProviderDefinition::EbvPortalDataProviderDefinition(
                data_provider_definition,
            ) => Self {
                aruna_data_provider_definition: None,
                dataset_layer_listing_provider_definition: None,
                gbif_data_provider_definition: None,
                gfbio_abcd_data_provider_definition: None,
                gfbio_collections_data_provider_definition: None,
                ebv_portal_data_provider_definition: Some(data_provider_definition.clone()),
                net_cdf_cf_data_provider_definition: None,
                pangaea_data_provider_definition: None,
                edr_data_provider_definition: None,
            },
            TypedDataProviderDefinition::NetCdfCfDataProviderDefinition(
                data_provider_definition,
            ) => Self {
                aruna_data_provider_definition: None,
                dataset_layer_listing_provider_definition: None,
                gbif_data_provider_definition: None,
                gfbio_abcd_data_provider_definition: None,
                gfbio_collections_data_provider_definition: None,
                ebv_portal_data_provider_definition: None,
                net_cdf_cf_data_provider_definition: Some(data_provider_definition.clone()),
                pangaea_data_provider_definition: None,
                edr_data_provider_definition: None,
            },
            TypedDataProviderDefinition::PangaeaDataProviderDefinition(
                data_provider_definition,
            ) => Self {
                aruna_data_provider_definition: None,
                dataset_layer_listing_provider_definition: None,
                gbif_data_provider_definition: None,
                gfbio_abcd_data_provider_definition: None,
                gfbio_collections_data_provider_definition: None,
                ebv_portal_data_provider_definition: None,
                net_cdf_cf_data_provider_definition: None,
                pangaea_data_provider_definition: Some(data_provider_definition.clone()),
                edr_data_provider_definition: None,
            },
            TypedDataProviderDefinition::EdrDataProviderDefinition(data_provider_definition) => {
                Self {
                    aruna_data_provider_definition: None,
                    dataset_layer_listing_provider_definition: None,
                    gbif_data_provider_definition: None,
                    gfbio_abcd_data_provider_definition: None,
                    gfbio_collections_data_provider_definition: None,
                    ebv_portal_data_provider_definition: None,
                    net_cdf_cf_data_provider_definition: None,
                    pangaea_data_provider_definition: None,
                    edr_data_provider_definition: Some(data_provider_definition.clone()),
                }
            }
        }
    }
}

impl TryFrom<TypedDataProviderDefinitionDbType> for TypedDataProviderDefinition {
    type Error = Error;

    #[allow(clippy::too_many_lines)]
    fn try_from(result_descriptor: TypedDataProviderDefinitionDbType) -> Result<Self, Self::Error> {
        match result_descriptor {
            TypedDataProviderDefinitionDbType {
                aruna_data_provider_definition: Some(data_provider_definition),
                dataset_layer_listing_provider_definition: None,
                gbif_data_provider_definition: None,
                gfbio_abcd_data_provider_definition: None,
                gfbio_collections_data_provider_definition: None,
                ebv_portal_data_provider_definition: None,
                net_cdf_cf_data_provider_definition: None,
                pangaea_data_provider_definition: None,
                edr_data_provider_definition: None,
            } => Ok(TypedDataProviderDefinition::ArunaDataProviderDefinition(
                data_provider_definition,
            )),
            TypedDataProviderDefinitionDbType {
                aruna_data_provider_definition: None,
                dataset_layer_listing_provider_definition: Some(data_provider_definition),
                gbif_data_provider_definition: None,
                gfbio_abcd_data_provider_definition: None,
                gfbio_collections_data_provider_definition: None,
                ebv_portal_data_provider_definition: None,
                net_cdf_cf_data_provider_definition: None,
                pangaea_data_provider_definition: None,
                edr_data_provider_definition: None,
            } => Ok(
                TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(
                    data_provider_definition,
                ),
            ),
            TypedDataProviderDefinitionDbType {
                aruna_data_provider_definition: None,
                dataset_layer_listing_provider_definition: None,
                gbif_data_provider_definition: Some(data_provider_definition),
                gfbio_abcd_data_provider_definition: None,
                gfbio_collections_data_provider_definition: None,
                ebv_portal_data_provider_definition: None,
                net_cdf_cf_data_provider_definition: None,
                pangaea_data_provider_definition: None,
                edr_data_provider_definition: None,
            } => Ok(TypedDataProviderDefinition::GbifDataProviderDefinition(
                data_provider_definition,
            )),
            TypedDataProviderDefinitionDbType {
                aruna_data_provider_definition: None,
                dataset_layer_listing_provider_definition: None,
                gbif_data_provider_definition: None,
                gfbio_abcd_data_provider_definition: Some(data_provider_definition),
                gfbio_collections_data_provider_definition: None,
                ebv_portal_data_provider_definition: None,
                net_cdf_cf_data_provider_definition: None,
                pangaea_data_provider_definition: None,
                edr_data_provider_definition: None,
            } => Ok(
                TypedDataProviderDefinition::GfbioAbcdDataProviderDefinition(
                    data_provider_definition,
                ),
            ),
            TypedDataProviderDefinitionDbType {
                aruna_data_provider_definition: None,
                dataset_layer_listing_provider_definition: None,
                gbif_data_provider_definition: None,
                gfbio_abcd_data_provider_definition: None,
                gfbio_collections_data_provider_definition: Some(data_provider_definition),
                ebv_portal_data_provider_definition: None,
                net_cdf_cf_data_provider_definition: None,
                pangaea_data_provider_definition: None,
                edr_data_provider_definition: None,
            } => Ok(
                TypedDataProviderDefinition::GfbioCollectionsDataProviderDefinition(
                    data_provider_definition,
                ),
            ),
            TypedDataProviderDefinitionDbType {
                aruna_data_provider_definition: None,
                dataset_layer_listing_provider_definition: None,
                gbif_data_provider_definition: None,
                gfbio_abcd_data_provider_definition: None,
                gfbio_collections_data_provider_definition: None,
                ebv_portal_data_provider_definition: Some(data_provider_definition),
                net_cdf_cf_data_provider_definition: None,
                pangaea_data_provider_definition: None,
                edr_data_provider_definition: None,
            } => Ok(
                TypedDataProviderDefinition::EbvPortalDataProviderDefinition(
                    data_provider_definition,
                ),
            ),
            TypedDataProviderDefinitionDbType {
                aruna_data_provider_definition: None,
                dataset_layer_listing_provider_definition: None,
                gbif_data_provider_definition: None,
                gfbio_abcd_data_provider_definition: None,
                gfbio_collections_data_provider_definition: None,
                ebv_portal_data_provider_definition: None,
                net_cdf_cf_data_provider_definition: Some(data_provider_definition),
                pangaea_data_provider_definition: None,
                edr_data_provider_definition: None,
            } => Ok(TypedDataProviderDefinition::NetCdfCfDataProviderDefinition(
                data_provider_definition,
            )),
            TypedDataProviderDefinitionDbType {
                aruna_data_provider_definition: None,
                dataset_layer_listing_provider_definition: None,
                gbif_data_provider_definition: None,
                gfbio_abcd_data_provider_definition: None,
                gfbio_collections_data_provider_definition: None,
                ebv_portal_data_provider_definition: None,
                net_cdf_cf_data_provider_definition: None,
                pangaea_data_provider_definition: Some(data_provider_definition),
                edr_data_provider_definition: None,
            } => Ok(TypedDataProviderDefinition::PangaeaDataProviderDefinition(
                data_provider_definition,
            )),
            TypedDataProviderDefinitionDbType {
                aruna_data_provider_definition: None,
                dataset_layer_listing_provider_definition: None,
                gbif_data_provider_definition: None,
                gfbio_abcd_data_provider_definition: None,
                gfbio_collections_data_provider_definition: None,
                ebv_portal_data_provider_definition: None,
                net_cdf_cf_data_provider_definition: None,
                pangaea_data_provider_definition: None,
                edr_data_provider_definition: Some(data_provider_definition),
            } => Ok(TypedDataProviderDefinition::EdrDataProviderDefinition(
                data_provider_definition,
            )),
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

delegate_from_to_sql!(ColorParam, ColorParamDbType);
delegate_from_to_sql!(DatabaseConnectionConfig, DatabaseConnectionConfigDbType);
delegate_from_to_sql!(
    EbvPortalDataProviderDefinition,
    EbvPortalDataProviderDefinitionDbType
);
delegate_from_to_sql!(EdrDataProviderDefinition, EdrDataProviderDefinitionDbType);
delegate_from_to_sql!(EdrVectorSpec, EdrVectorSpecDbType);
delegate_from_to_sql!(
    GfbioCollectionsDataProviderDefinition,
    GfbioCollectionsDataProviderDefinitionDbType
);
delegate_from_to_sql!(MetaDataDefinition, MetaDataDefinitionDbType);
delegate_from_to_sql!(NumberParam, NumberParamDbType);
delegate_from_to_sql!(
    NetCdfCfDataProviderDefinition,
    NetCdfCfDataProviderDefinitionDbType
);
delegate_from_to_sql!(
    PangaeaDataProviderDefinition,
    PangaeaDataProviderDefinitionDbType
);
delegate_from_to_sql!(Symbology, SymbologyDbType);
delegate_from_to_sql!(
    TypedDataProviderDefinition,
    TypedDataProviderDefinitionDbType
);
