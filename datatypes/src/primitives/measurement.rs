use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum Measurement {
    Unitless,
    Continuous {
        measurement: String,
        unit: Option<String>,
    },
    Classification {
        measurement: String,
        classes: HashMap<u8, String>,
    },
}

impl Measurement {
    pub fn continuous(measurement: String, unit: Option<String>) -> Self {
        Self::Continuous { measurement, unit }
    }

    pub fn classification(measurement: String, classes: HashMap<u8, String>) -> Self {
        Self::Classification {
            measurement,
            classes,
        }
    }
}

impl Default for Measurement {
    fn default() -> Self {
        Self::Unitless
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, utoipa::ToSchema)]
pub struct ContinuousMeasurement {
    pub measurement: String,
    pub unit: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(
    try_from = "SerializableClassificationMeasurement",
    into = "SerializableClassificationMeasurement"
)]
pub struct ClassificationMeasurement {
    pub measurement: String,
    pub classes: HashMap<u8, String>,
}

/// A type that is solely for serde's serializability.
/// You cannot serialize floats as JSON map keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializableClassificationMeasurement {
    pub measurement: String,
    // use a BTreeMap to preserve the order of the keys
    pub classes: BTreeMap<String, String>,
}

impl From<ClassificationMeasurement> for SerializableClassificationMeasurement {
    fn from(measurement: ClassificationMeasurement) -> Self {
        let mut classes = BTreeMap::new();
        for (k, v) in measurement.classes {
            classes.insert(k.to_string(), v);
        }
        Self {
            measurement: measurement.measurement,
            classes,
        }
    }
}

impl TryFrom<SerializableClassificationMeasurement> for ClassificationMeasurement {
    type Error = <u8 as FromStr>::Err;

    fn try_from(measurement: SerializableClassificationMeasurement) -> Result<Self, Self::Error> {
        let mut classes = HashMap::with_capacity(measurement.classes.len());
        for (k, v) in measurement.classes {
            classes.insert(k.parse::<u8>()?, v);
        }
        Ok(Self {
            measurement: measurement.measurement,
            classes,
        })
    }
}

impl From<Option<Measurement>> for Measurement {
    fn from(measurement: Option<Measurement>) -> Self {
        measurement.unwrap_or(Measurement::Unitless)
    }
}

impl fmt::Display for Measurement {
    /// Human readable measurement info
    ///
    /// # Examples
    /// ```rust
    /// use geoengine_datatypes::primitives::Measurement;
    /// use std::collections::HashMap;
    ///
    /// assert_eq!(format!("{}", Measurement::Unitless), "");
    /// assert_eq!(format!("{}", Measurement::continuous("foo".into(), Some("bar".into()))), "foo in bar");
    /// assert_eq!(format!("{}", Measurement::continuous("foo".into(), None)), "foo");
    /// assert_eq!(format!("{}", Measurement::classification("foobar".into(), HashMap::new())), "foobar");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Measurement::Unitless => Ok(()),
            Measurement::Continuous {
                measurement,
                unit: unit_option,
            } => {
                write!(f, "{}", measurement)?;
                if let Some(unit) = unit_option {
                    return write!(f, " in {}", unit);
                }
                Ok(())
            }
            Measurement::Classification { measurement, .. } => {
                write!(f, "{}", measurement)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn continous_serialization() {
        let measurement = Measurement::continuous("foo".into(), Some("bar".into()));
        let serialized = serde_json::to_string(&measurement).unwrap();
        assert_eq!(
            serialized,
            r#"{"type":"continuous","measurement":"foo","unit":"bar"}"#
        );
        let deserialized: Measurement = serde_json::from_str(&serialized).unwrap();
        assert_eq!(measurement, deserialized);
    }

    #[test]
    fn classification_serialization() {
        let measurement = Measurement::classification(
            "foo".into(),
            HashMap::from([(1_u8, "bar".to_string()), (2, "baz".to_string())]),
        );
        let serialized = serde_json::to_string(&measurement).unwrap();
        assert_eq!(
            serialized,
            r#"{"type":"classification","measurement":"foo","classes":{"1":"bar","2":"baz"}}"#
        );
        let deserialized: Measurement = serde_json::from_str(&serialized).unwrap();
        assert_eq!(measurement, deserialized);
    }

    #[test]
    fn unitless_serialization() {
        let measurement = Measurement::Unitless;
        let serialized = serde_json::to_string(&measurement).unwrap();
        assert_eq!(serialized, r#"{"type":"unitless"}"#);
        let deserialized: Measurement = serde_json::from_str(&serialized).unwrap();
        assert_eq!(measurement, deserialized);
    }
}
