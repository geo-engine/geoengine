use geoengine_datatypes::dataset::{DatasetId, NamedData};
use postgres_types::{FromSql, ToSql};
use serde::{de::Visitor, Deserialize, Serialize};
use snafu::Snafu;
use std::str::FromStr;
use strum::IntoStaticStr;
use utoipa::{IntoParams, ToSchema};

/// A (optionally namespaced) name for a `Dataset`.
/// It can be resolved into a [`DataId`] if you know the data provider.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, IntoParams, ToSql, FromSql)]
pub struct DatasetName {
    pub namespace: Option<String>,
    pub name: String,
}

#[derive(Snafu, IntoStaticStr, Debug)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum DatasetNameError {
    #[snafu(display("DatasetName is empty"))]
    IsEmpty,
    #[snafu(display("invalid character '{invalid_char}' in named data"))]
    InvalidCharacter { invalid_char: String },
    #[snafu(display("named data must consist of at most two parts"))]
    TooManyParts,
}

impl DatasetName {
    /// Canonicalize a name that reflects the system namespace and provider.
    fn canonicalize<S: Into<String> + PartialEq<&'static str>>(
        name: S,
        system_name: &'static str,
    ) -> Option<String> {
        if name == system_name {
            None
        } else {
            Some(name.into())
        }
    }

    pub fn new<S: Into<String>>(namespace: Option<String>, name: S) -> Self {
        Self {
            namespace,
            name: name.into(),
        }
    }
}

impl std::fmt::Display for DatasetName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let d = geoengine_datatypes::dataset::NAME_DELIMITER;
        match (&self.namespace, &self.name) {
            (None, name) => write!(f, "{name}"),
            (Some(namespace), name) => {
                write!(f, "{namespace}{d}{name}")
            }
        }
    }
}

impl FromStr for DatasetName {
    type Err = DatasetNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut strings = [None, None];
        let mut split = s.split(geoengine_datatypes::dataset::NAME_DELIMITER);

        for (buffer, part) in strings.iter_mut().zip(&mut split) {
            if part.is_empty() {
                return Err(DatasetNameError::IsEmpty);
            }

            if let Some(c) = part
                .matches(geoengine_datatypes::dataset::is_invalid_name_char)
                .next()
            {
                return Err(DatasetNameError::InvalidCharacter {
                    invalid_char: c.to_string(),
                });
            }

            *buffer = Some(part.to_string());
        }

        if split.next().is_some() {
            return Err(DatasetNameError::TooManyParts);
        }

        match strings {
            [Some(namespace), Some(name)] => Ok(DatasetName {
                namespace: DatasetName::canonicalize(
                    namespace,
                    geoengine_datatypes::dataset::SYSTEM_NAMESPACE,
                ),
                name,
            }),
            [Some(name), None] => Ok(DatasetName {
                namespace: None,
                name,
            }),
            _ => Err(DatasetNameError::IsEmpty),
        }
    }
}

impl Serialize for DatasetName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let d = geoengine_datatypes::dataset::NAME_DELIMITER;
        let serialized = match (&self.namespace, &self.name) {
            (None, name) => name.to_string(),
            (Some(namespace), name) => {
                format!("{namespace}{d}{name}")
            }
        };

        serializer.serialize_str(&serialized)
    }
}

impl<'de> Deserialize<'de> for DatasetName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(DatasetNameDeserializeVisitor)
    }
}

struct DatasetNameDeserializeVisitor;

impl Visitor<'_> for DatasetNameDeserializeVisitor {
    type Value = DatasetName;

    /// always keep in sync with [`is_allowed_name_char`]
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            formatter,
            "a string consisting of a namespace and name name, separated by a colon, only using alphanumeric characters, underscores & dashes"
        )
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        DatasetName::from_str(s).map_err(|e| E::custom(e.to_string()))
    }
}

impl From<NamedData> for DatasetName {
    fn from(
        NamedData {
            namespace,
            provider: _,
            name,
        }: NamedData,
    ) -> Self {
        Self { namespace, name }
    }
}

impl From<&NamedData> for DatasetName {
    fn from(named_data: &NamedData) -> Self {
        Self {
            namespace: named_data.namespace.clone(),
            name: named_data.name.clone(),
        }
    }
}

// impl From<&geoengine_datatypes::dataset::NamedData> for DatasetName {
//     fn from(named_data: &geoengine_datatypes::dataset::NamedData) -> Self {
//         Self {
//             namespace: named_data.namespace.clone(),
//             name: named_data.name.clone(),
//         }
//     }
// }

impl From<DatasetName> for NamedData {
    fn from(DatasetName { namespace, name }: DatasetName) -> Self {
        NamedData {
            namespace,
            provider: None,
            name,
        }
    }
}

// impl From<DatasetName> for geoengine_datatypes::dataset::NamedData {
//     fn from(DatasetName { namespace, name }: DatasetName) -> Self {
//         geoengine_datatypes::dataset::NamedData {
//             namespace,
//             provider: None,
//             name,
//         }
//     }
// }

impl<'a> ToSchema<'a> for DatasetName {
    fn schema() -> (&'a str, utoipa::openapi::RefOr<utoipa::openapi::Schema>) {
        use utoipa::openapi::*;
        (
            "DatasetName",
            ObjectBuilder::new().schema_type(SchemaType::String).into(),
        )
    }
}

#[derive(Debug)]
pub struct DatasetIdAndName {
    pub id: DatasetId,
    pub name: DatasetName,
}
