use crate::identifier;
use serde::{de::Visitor, Deserialize, Serialize};

identifier!(DataProviderId);

// Identifier for datasets managed by Geo Engine
identifier!(DatasetId);

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
/// The identifier for loadable data. It is used in the source operators to get the loading info (aka parametrization)
/// for accessing the data. Internal data is loaded from datasets, external from `DataProvider`s.
pub enum DataId {
    #[serde(rename_all = "camelCase")]
    Internal {
        #[serde(flatten)]
        dataset: InternalDataset,
    },
    #[serde(rename_all = "camelCase")]
    External(ExternalDataId),
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", untagged)]
pub enum InternalDataset {
    #[serde(rename_all = "camelCase")]
    DatasetId { dataset_id: DatasetId },
    #[serde(rename_all = "camelCase")]
    Name { name: DatasetName },
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct DatasetName {
    pub namespace: String,
    pub name: String,
}

pub const SYSTEM_NAMESPACE: &str = "_";

// TODO: move this to services at some point
impl<'de> Deserialize<'de> for DatasetName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(DatasetNameDeserializeVisitor)
    }
}

// TODO: move this to services at some point
impl Serialize for DatasetName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("{}:{}", self.namespace, self.name))
    }
}

struct DatasetNameDeserializeVisitor;

/// Checks if a character is allowed in a dataset name.
#[inline]
pub fn is_allowed_name_char(c: char) -> bool {
    // `is_ascii_alphanumeric` plus some special characters

    matches!(
        c,
        '0'..='9' // digits
        | 'A'..='Z' | 'a'..='z' // ascii
        | '_' | '-' // special characters
    )
}

impl<'de> Visitor<'de> for DatasetNameDeserializeVisitor {
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
        let mut chars = s.chars();

        let mut namespace = String::new();

        for c in &mut chars {
            if c == ':' {
                break;
            }

            if !is_allowed_name_char(c) {
                return Err(E::custom(format!(
                    "invalid character '{c}' in dataset name"
                )));
            }

            namespace.push(c);
        }

        if namespace.is_empty() {
            return Err(E::custom("empty namespace in dataset name"));
        }

        let mut name = String::new();

        for c in chars {
            if c == ':' {
                return Err(E::custom(format!(
                    "invalid separator '{c}' after dataset name's name part"
                )));
            }

            if !is_allowed_name_char(c) {
                return Err(E::custom(format!(
                    "invalid character '{c}' in dataset name"
                )));
            }

            name.push(c);
        }

        if name.is_empty() {
            return Err(E::custom("empty name in dataset name"));
        }

        Ok(DatasetName { namespace, name })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct LayerId(pub String);

impl std::fmt::Display for LayerId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalDataId {
    pub provider_id: DataProviderId,
    pub layer_id: LayerId,
}

impl DataId {
    pub fn internal(&self) -> Option<InternalDataset> {
        let Self::Internal { dataset } = self else {
            return None;
        };
        Some(dataset.clone())
    }

    pub fn external(&self) -> Option<ExternalDataId> {
        if let Self::External(id) = self {
            return Some(id.clone());
        }
        None
    }
}

impl From<DatasetId> for DataId {
    fn from(value: DatasetId) -> Self {
        DataId::Internal {
            dataset: InternalDataset::from(value),
        }
    }
}

impl From<DatasetId> for InternalDataset {
    fn from(dataset_id: DatasetId) -> Self {
        InternalDataset::DatasetId { dataset_id }
    }
}

impl From<ExternalDataId> for DataId {
    fn from(value: ExternalDataId) -> Self {
        DataId::External(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ser_de_dataset_id() {
        let json = serde_json::json!({
            "type": "internal",
            "datasetId": "2bfa5144-1da0-4f62-83c1-e15ae333081b"
        });

        let data_id: DataId = serde_json::from_value(json.clone()).unwrap();

        assert_eq!(
            data_id,
            DataId::Internal {
                dataset: InternalDataset::DatasetId {
                    dataset_id: DatasetId::from_u128(0x2bfa_5144_1da0_4f62_83c1_e15a_e333_081b)
                }
            }
        );

        assert_eq!(serde_json::to_value(&data_id).unwrap(), json);
    }

    #[test]
    fn test_ser_de_dataset_name() {
        let json = serde_json::json!({
            "type": "internal",
            "name": "foo:bar"
        });

        let data_id: DataId = serde_json::from_value(json.clone()).unwrap();

        assert_eq!(
            data_id,
            DataId::Internal {
                dataset: InternalDataset::Name {
                    name: DatasetName {
                        namespace: "foo".to_string(),
                        name: "bar".to_string(),
                    }
                }
            }
        );

        assert_eq!(serde_json::to_value(&data_id).unwrap(), json);
    }
}
