use super::loading_info::{GdalMdMetaData, MdVariable};
use crate::engine::RasterResultDescriptor;
use crate::error::Error;
use crate::source::gdal_worker_process::MdAxisMapping;
use geoengine_datatypes::{delegate_from_to_sql, primitives::CacheTtlSeconds};
use postgres_types::{FromSql, ToSql};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ToSql, FromSql)]
#[postgres(name = "MdAxisMappingDiscriminator")]
pub enum MdAxisMappingDiscriminatorDbType {
    X,
    Y,
    Time,
    Fixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ToSql, FromSql)]
#[postgres(name = "MdAxisMapping")]
pub struct MdAxisMappingDbType {
    pub discriminant: MdAxisMappingDiscriminatorDbType,
    pub fixed_index: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, ToSql, FromSql)]
#[postgres(name = "MdVariable")]
pub struct MdVariableDbType {
    pub name: String,
    pub axis_mapping: Vec<MdAxisMappingDbType>,
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "GdalMdMetaData")]
pub struct GdalMdMetaDataDbType {
    pub result_descriptor: RasterResultDescriptor,
    pub variables: Vec<MdVariableDbType>,
    pub wrap: bool,
    pub cache_ttl: Option<CacheTtlSeconds>,
}

impl From<&MdAxisMapping> for MdAxisMappingDbType {
    fn from(other: &MdAxisMapping) -> Self {
        let (discriminant, fixed_index) = match *other {
            MdAxisMapping::X => (MdAxisMappingDiscriminatorDbType::X, None),
            MdAxisMapping::Y => (MdAxisMappingDiscriminatorDbType::Y, None),
            MdAxisMapping::Time => (MdAxisMappingDiscriminatorDbType::Time, None),
            MdAxisMapping::Fixed { index } => {
                (MdAxisMappingDiscriminatorDbType::Fixed, Some(index))
            }
        };
        Self {
            discriminant,
            fixed_index,
        }
    }
}

impl TryFrom<MdAxisMappingDbType> for MdAxisMapping {
    type Error = Error;

    fn try_from(other: MdAxisMappingDbType) -> Result<Self, Error> {
        Ok(match other.discriminant {
            MdAxisMappingDiscriminatorDbType::X => MdAxisMapping::X,
            MdAxisMappingDiscriminatorDbType::Y => MdAxisMapping::Y,
            MdAxisMappingDiscriminatorDbType::Time => MdAxisMapping::Time,
            MdAxisMappingDiscriminatorDbType::Fixed => MdAxisMapping::Fixed {
                index: other
                    .fixed_index
                    .ok_or(geoengine_datatypes::error::Error::UnexpectedInvalidDbTypeConversion)?,
            },
        })
    }
}

impl From<&MdVariable> for MdVariableDbType {
    fn from(other: &MdVariable) -> Self {
        Self {
            name: other.name.clone(),
            axis_mapping: other.axis_mapping.iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<MdVariableDbType> for MdVariable {
    type Error = Error;

    fn try_from(other: MdVariableDbType) -> Result<Self, Error> {
        Ok(Self {
            name: other.name,
            axis_mapping: other
                .axis_mapping
                .into_iter()
                .map(MdAxisMapping::try_from)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl From<&GdalMdMetaData> for GdalMdMetaDataDbType {
    fn from(other: &GdalMdMetaData) -> Self {
        Self {
            result_descriptor: other.result_descriptor.clone(),
            variables: other.variables.iter().map(Into::into).collect(),
            wrap: other.wrap,
            cache_ttl: other.cache_ttl,
        }
    }
}

impl TryFrom<GdalMdMetaDataDbType> for GdalMdMetaData {
    type Error = Error;

    fn try_from(other: GdalMdMetaDataDbType) -> Result<Self, Error> {
        Ok(Self {
            result_descriptor: other.result_descriptor,
            variables: other
                .variables
                .into_iter()
                .map(MdVariable::try_from)
                .collect::<Result<_, _>>()?,
            wrap: other.wrap,
            cache_ttl: other.cache_ttl,
        })
    }
}

delegate_from_to_sql!(GdalMdMetaData, GdalMdMetaDataDbType);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_round_trips_axis_mappings() {
        let variable = MdVariable {
            name: "pv".to_string(),
            axis_mapping: vec![
                MdAxisMapping::Time,
                MdAxisMapping::Y,
                MdAxisMapping::X,
                MdAxisMapping::Fixed { index: 3 },
            ],
        };
        let db = MdVariableDbType::from(&variable);
        assert_eq!(MdVariable::try_from(db).unwrap(), variable);
    }

    #[test]
    fn it_rejects_fixed_axis_without_index() {
        let db = MdAxisMappingDbType {
            discriminant: MdAxisMappingDiscriminatorDbType::Fixed,
            fixed_index: None,
        };
        assert!(MdAxisMapping::try_from(db).is_err());
    }
}
