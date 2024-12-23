use postgres_types::{FromSql, ToSql};
use serde::{de::Visitor, Deserialize, Serialize};
use utoipa::ToSchema;

const NAME_DELIMITER: char = ':';

#[derive(Debug, Clone, Hash, Eq, PartialEq, FromSql, ToSql)]
pub struct MlModelName {
    pub namespace: Option<String>,
    pub name: String,
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

impl From<MlModelName> for geoengine_datatypes::machine_learning::MlModelName {
    fn from(name: MlModelName) -> Self {
        Self {
            namespace: name.namespace,
            name: name.name,
        }
    }
}

impl From<geoengine_datatypes::machine_learning::MlModelName> for MlModelName {
    fn from(name: geoengine_datatypes::machine_learning::MlModelName) -> Self {
        Self {
            namespace: name.namespace,
            name: name.name,
        }
    }
}

impl std::fmt::Display for MlModelName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let d = NAME_DELIMITER;
        match (&self.namespace, &self.name) {
            (None, name) => write!(f, "{name}"),
            (Some(namespace), name) => {
                write!(f, "{namespace}{d}{name}")
            }
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
        let mut strings = [None, None];
        let mut split = s.split(NAME_DELIMITER);

        for (buffer, part) in strings.iter_mut().zip(&mut split) {
            if part.is_empty() {
                return Err(E::custom("empty part in named data"));
            }

            if let Some(c) = part
                .matches(geoengine_datatypes::dataset::is_invalid_name_char)
                .next()
            {
                return Err(E::custom(format!("invalid character '{c}' in named model")));
            }

            *buffer = Some(part.to_string());
        }

        if split.next().is_some() {
            return Err(E::custom("named model must consist of at most two parts"));
        }

        match strings {
            [Some(namespace), Some(name)] => Ok(MlModelName {
                namespace: MlModelName::canonicalize(
                    namespace,
                    geoengine_datatypes::dataset::SYSTEM_NAMESPACE,
                ),
                name,
            }),
            [Some(name), None] => Ok(MlModelName {
                namespace: None,
                name,
            }),
            _ => Err(E::custom("empty named data")),
        }
    }
}

impl<'a> ToSchema<'a> for MlModelName {
    fn schema() -> (&'a str, utoipa::openapi::RefOr<utoipa::openapi::Schema>) {
        use utoipa::openapi::*;
        (
            "MlModelName",
            ObjectBuilder::new().schema_type(SchemaType::String).into(),
        )
    }
}
