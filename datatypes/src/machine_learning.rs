use crate::{
    dataset::{is_invalid_name_char, SYSTEM_NAMESPACE},
    raster::RasterDataType,
};
use serde::{de::Visitor, Deserialize, Serialize};
use snafu::Snafu;
use std::path::PathBuf;
use std::str::FromStr;
use strum::IntoStaticStr;

const NAME_DELIMITER: char = ':';

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct MlModelName {
    pub namespace: Option<String>,
    pub name: String,
}

#[derive(Snafu, IntoStaticStr, Debug)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum MlModelNameError {
    #[snafu(display("MlModelName is empty"))]
    IsEmpty,
    #[snafu(display("invalid character '{invalid_char}' in named model"))]
    InvalidCharacter { invalid_char: String },
    #[snafu(display("ml model name must consist of at most two parts"))]
    TooManyParts,
}

impl MlModelName {
    /// Canonicalize a name that reflects the system namespace and model.
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

impl Serialize for MlModelName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let d = NAME_DELIMITER;
        let serialized = match (&self.namespace, &self.name) {
            (None, name) => name.to_string(),
            (Some(namespace), name) => {
                format!("{namespace}{d}{name}")
            }
        };

        serializer.serialize_str(&serialized)
    }
}

impl<'de> Deserialize<'de> for MlModelName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(MlModelNameDeserializeVisitor)
    }
}

impl FromStr for MlModelName {
    type Err = MlModelNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut strings = [None, None];
        let mut split = s.split(NAME_DELIMITER);

        for (buffer, part) in strings.iter_mut().zip(&mut split) {
            if part.is_empty() {
                return Err(MlModelNameError::IsEmpty);
            }

            if let Some(c) = part.matches(is_invalid_name_char).next() {
                return Err(MlModelNameError::InvalidCharacter {
                    invalid_char: c.to_string(),
                });
            }

            *buffer = Some(part.to_string());
        }

        if split.next().is_some() {
            return Err(MlModelNameError::TooManyParts);
        }

        match strings {
            [Some(namespace), Some(name)] => Ok(MlModelName {
                namespace: MlModelName::canonicalize(namespace, SYSTEM_NAMESPACE),
                name,
            }),
            [Some(name), None] => Ok(MlModelName {
                namespace: None,
                name,
            }),
            _ => Err(MlModelNameError::IsEmpty),
        }
    }
}

struct MlModelNameDeserializeVisitor;

impl Visitor<'_> for MlModelNameDeserializeVisitor {
    type Value = MlModelName;

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
        MlModelName::from_str(s).map_err(|e| E::custom(e.to_string()))
    }
}

// For now we assume all models are pixel-wise, i.e., they take a single pixel with multiple bands as input and produce a single output value.
// To support different inputs, we would need a more sophisticated logic to produce the inputs for the model.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub struct MlModelMetadata {
    pub file_path: PathBuf,
    pub input_type: RasterDataType,
    pub num_input_bands: u32, // number of features per sample (bands per pixel)
    pub output_type: RasterDataType, // TODO: support multiple outputs, e.g. one band for the probability of prediction
                                     // TODO: output measurement, e.g. classification or regression, label names for classification. This would have to be provided by the model creator along the model file as it cannot be extracted from the model file(?)
}
