use geoengine_datatypes::delegate_from_to_sql;
use postgres_types::{FromSql, ToSql};

use crate::error::Error;

#[derive(Debug, ToSql, FromSql)]
pub enum MlModelInputNoDataHandlingVariant {
    EncodedNoData,
    SkipIfNoData,
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "MlModelInputNoDataHandling")]
pub struct MlModelInputNoDataHandlingDbType {
    no_data_value: Option<f32>,
    variant: MlModelInputNoDataHandlingVariant,
}

impl From<&crate::machine_learning::MlModelInputNoDataHandling>
    for MlModelInputNoDataHandlingDbType
{
    fn from(value: &crate::machine_learning::MlModelInputNoDataHandling) -> Self {
        match value {
            crate::machine_learning::MlModelInputNoDataHandling::EncodedNoData {
                no_data_value,
            } => Self {
                no_data_value: Some(*no_data_value),
                variant: MlModelInputNoDataHandlingVariant::EncodedNoData,
            },
            crate::machine_learning::MlModelInputNoDataHandling::SkipIfNoData => Self {
                no_data_value: None,
                variant: MlModelInputNoDataHandlingVariant::SkipIfNoData,
            },
        }
    }
}

impl TryFrom<MlModelInputNoDataHandlingDbType>
    for crate::machine_learning::MlModelInputNoDataHandling
{
    type Error = Error;

    fn try_from(value: MlModelInputNoDataHandlingDbType) -> Result<Self, Self::Error> {
        match value.variant {
            MlModelInputNoDataHandlingVariant::EncodedNoData => Ok(
                crate::machine_learning::MlModelInputNoDataHandling::EncodedNoData {
                    no_data_value: value.no_data_value.ok_or(
                        geoengine_datatypes::error::Error::UnexpectedInvalidDbTypeConversion,
                    )?,
                },
            ),
            MlModelInputNoDataHandlingVariant::SkipIfNoData => {
                Ok(crate::machine_learning::MlModelInputNoDataHandling::SkipIfNoData)
            }
        }
    }
}

#[derive(Debug, ToSql, FromSql)]
pub enum MlModelOutputNoDataHandlingVariant {
    EncodedNoData,
    NanIsNoData,
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "MlModelOutputNoDataHandling")]
pub struct MlModelOutputNoDataHandlingDbType {
    no_data_value: Option<f32>,
    variant: MlModelOutputNoDataHandlingVariant,
}

impl From<&crate::machine_learning::MlModelOutputNoDataHandling>
    for MlModelOutputNoDataHandlingDbType
{
    fn from(value: &crate::machine_learning::MlModelOutputNoDataHandling) -> Self {
        match value {
            crate::machine_learning::MlModelOutputNoDataHandling::EncodedNoData {
                no_data_value,
            } => Self {
                no_data_value: Some(*no_data_value),
                variant: MlModelOutputNoDataHandlingVariant::EncodedNoData,
            },
            crate::machine_learning::MlModelOutputNoDataHandling::NanIsNoData => Self {
                no_data_value: None,
                variant: MlModelOutputNoDataHandlingVariant::NanIsNoData,
            },
        }
    }
}

impl TryFrom<MlModelOutputNoDataHandlingDbType>
    for crate::machine_learning::MlModelOutputNoDataHandling
{
    type Error = Error;

    fn try_from(value: MlModelOutputNoDataHandlingDbType) -> Result<Self, Self::Error> {
        match value.variant {
            MlModelOutputNoDataHandlingVariant::EncodedNoData => Ok(
                crate::machine_learning::MlModelOutputNoDataHandling::EncodedNoData {
                    no_data_value: value.no_data_value.ok_or(
                        geoengine_datatypes::error::Error::UnexpectedInvalidDbTypeConversion,
                    )?,
                },
            ),
            MlModelOutputNoDataHandlingVariant::NanIsNoData => {
                Ok(crate::machine_learning::MlModelOutputNoDataHandling::NanIsNoData)
            }
        }
    }
}

delegate_from_to_sql!(
    super::MlModelInputNoDataHandling,
    MlModelInputNoDataHandlingDbType
);

delegate_from_to_sql!(
    super::MlModelOutputNoDataHandling,
    MlModelOutputNoDataHandlingDbType
);
