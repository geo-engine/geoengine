use crate::{
    dataset::{SYSTEM_NAMESPACE, is_invalid_name_char},
    raster::{GridShape2D, GridSize},
};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize, de::Visitor};
use snafu::Snafu;
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

/// A struct describing tensor shape for `MlModelMetadata`
#[derive(Debug, Copy, Clone, Eq, PartialEq, Deserialize, Serialize, ToSql, FromSql)]
pub struct MlTensorShape3D {
    pub y: u32,
    pub x: u32,
    pub bands: u32, // TODO: named attributes?
}

impl MlTensorShape3D {
    pub fn new_y_x_bands(y: u32, x: u32, bands: u32) -> Self {
        Self { y, x, bands }
    }

    pub fn new_single_pixel_bands(bands: u32) -> Self {
        Self { y: 1, x: 1, bands }
    }

    pub fn new_single_pixel_single_band() -> Self {
        Self::new_single_pixel_bands(1)
    }

    pub fn axis_size_y(&self) -> u32 {
        self.y
    }

    pub fn axis_size_x(&self) -> u32 {
        self.x
    }

    pub fn yx_matches_tile_shape(&self, tile_shape: &GridShape2D) -> bool {
        self.axis_size_x() as usize == tile_shape.axis_size_x()
            && self.axis_size_y() as usize == tile_shape.axis_size_y()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ml_model_name_from_str() {
        const ML_MODEL_NAME: &str = "myModelName";
        let mln = MlModelName::from_str(ML_MODEL_NAME).unwrap();
        assert_eq!(mln.name, ML_MODEL_NAME);
        assert!(mln.namespace.is_none());
    }

    #[test]
    fn ml_model_name_from_str_prefixed() {
        const ML_MODEL_NAME: &str = "d5328854-6190-4af9-ad69-4e74b0961ac9:myModelName";
        let mln = MlModelName::from_str(ML_MODEL_NAME).unwrap();
        assert_eq!(mln.name, "myModelName".to_string());
        assert_eq!(
            mln.namespace,
            Some("d5328854-6190-4af9-ad69-4e74b0961ac9".to_string())
        );
    }

    #[test]
    fn ml_model_name_from_str_system() {
        const ML_MODEL_NAME: &str = "_:myModelName";
        let mln = MlModelName::from_str(ML_MODEL_NAME).unwrap();
        assert_eq!(mln.name, "myModelName".to_string());
        assert!(mln.namespace.is_none());
    }
}
