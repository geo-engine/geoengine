use crate::identifier;
use serde::{Deserialize, Serialize, de::Visitor};

identifier!(DataProviderId);

// Identifier for datasets managed by Geo Engine
identifier!(DatasetId);

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
/// The identifier for loadable data. It is used in the source operators to get the loading info (aka parametrization)
/// for accessing the data. Internal data is loaded from datasets, external from `DataProvider`s.
pub enum DataId {
    #[serde(rename_all = "camelCase")]
    Internal { dataset_id: DatasetId },
    #[serde(rename_all = "camelCase")]
    External(ExternalDataId),
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
    pub fn internal(&self) -> Option<DatasetId> {
        let Self::Internal { dataset_id } = self else {
            return None;
        };

        Some(*dataset_id)
    }

    pub fn external(&self) -> Option<ExternalDataId> {
        if let Self::External(id) = self {
            return Some(id.clone());
        }
        None
    }
}

impl From<DatasetId> for DataId {
    fn from(dataset_id: DatasetId) -> Self {
        DataId::Internal { dataset_id }
    }
}

impl From<ExternalDataId> for DataId {
    fn from(value: ExternalDataId) -> Self {
        DataId::External(value)
    }
}

pub const NAME_DELIMITER: char = ':';
pub const NAME_BRACE: char = '`';
pub const SYSTEM_NAMESPACE: &str = "_";
pub const SYSTEM_PROVIDER: &str = "_";

/// The user-facing identifier for loadable data.
/// It can be resolved into a [`DataId`].
///
/// It is a triple of namespace, provider and name.
/// The namespace and provider are optional and default to the system namespace and provider.
///
/// # Examples
///
/// * `dataset` -> `NamedData { namespace: None, provider: None, name: "dataset" }`
/// * `namespace:dataset` -> `NamedData { namespace: Some("namespace"), provider: None, name: "dataset" }`
/// * `namespace:provider:dataset` -> `NamedData { namespace: Some("namespace"), provider: Some("provider"), name: "dataset" }`
///
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct NamedData {
    pub namespace: Option<String>,
    pub provider: Option<String>,
    pub name: String,
}

impl NamedData {
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

    /// Creates a `NamedData` with the system's namepsace and a name.
    pub fn with_system_name<S: Into<String>>(name: S) -> Self {
        Self {
            namespace: None,
            provider: None,
            name: name.into(),
        }
    }

    pub fn with_namespaced_name<S1: Into<String> + PartialEq<&'static str>, S2: Into<String>>(
        namespace: S1,
        name: S2,
    ) -> Self {
        Self {
            namespace: Self::canonicalize(namespace, SYSTEM_NAMESPACE),
            provider: None,
            name: name.into(),
        }
    }

    /// Creates a `NamedData` with the system's namepsace, a provider and a name.
    pub fn with_system_provider<S1: Into<String> + PartialEq<&'static str>, S2: Into<String>>(
        provider: S1,
        name: S2,
    ) -> Self {
        Self {
            namespace: None,
            provider: Self::canonicalize(provider, SYSTEM_PROVIDER),
            name: name.into(),
        }
    }
}

impl std::fmt::Display for NamedData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let d = NAME_DELIMITER;
        match (&self.namespace, &self.provider, &self.name) {
            (None, None, name) => write!(f, "{name}"),
            (None, Some(provider), name) => {
                write!(f, "{SYSTEM_NAMESPACE}{d}{provider}{d}{name}")
            }
            (Some(namespace), None, name) => {
                write!(f, "{namespace}{d}{name}")
            }
            (Some(namespace), Some(provider), name) => {
                write!(f, "{namespace}{d}{provider}{d}{name}")
            }
        }
    }
}

// TODO: move this to services at some point
impl Serialize for NamedData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let name = if self
            .name
            .contains(|c| c == NAME_DELIMITER || c == NAME_BRACE || is_invalid_name_char(c))
        {
            brace_name(&self.name)
        } else {
            self.name.clone()
        };

        let d = NAME_DELIMITER;
        let serialized = match (&self.namespace, &self.provider, name) {
            (None, None, name) => name,
            (None, Some(provider), name) => {
                format!("{SYSTEM_NAMESPACE}{d}{provider}{d}{name}")
            }
            (Some(namespace), None, name) => {
                format!("{namespace}{d}{name}")
            }
            (Some(namespace), Some(provider), name) => {
                format!("{namespace}{d}{provider}{d}{name}")
            }
        };

        serializer.serialize_str(&serialized)
    }
}

fn brace_name(name: &str) -> String {
    let mut result = String::with_capacity(name.len() + 2);
    result.push(NAME_BRACE);
    for c in name.chars() {
        if c == NAME_BRACE {
            result.push(NAME_BRACE);
        }
        result.push(c);
    }
    result.push(NAME_BRACE);
    result
}

// TODO: move this to services at some point
impl<'de> Deserialize<'de> for NamedData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(NamedDataDeserializeVisitor)
    }
}

struct NamedDataDeserializeVisitor;

impl NamedDataDeserializeVisitor {
    fn parse_string<E>(s: &str) -> Result<[String; 3], E>
    where
        E: serde::de::Error,
    {
        enum Mode {
            Simple,
            BraceStart { num_braces: usize },
            Braced,
            BraceEnd { num_braces: usize },
        }

        let mut strings = [String::new(), String::new(), String::new()];
        let mut strings_idx = 0;
        let mut mode = Mode::Simple;

        for c in s.chars() {
            if strings_idx >= strings.len() {
                return Err(E::custom("named data must consist of at most three parts"));
            }

            match mode {
                Mode::Simple => match c {
                    NAME_DELIMITER => {
                        strings_idx += 1;
                    }
                    NAME_BRACE => {
                        mode = Mode::BraceStart { num_braces: 1 };
                    }
                    c if !is_allowed_name_char(c) => {
                        return Err(E::custom(format!(
                            "character '{c}' is not allowed in a dataset name"
                        )));
                    }
                    c => strings[strings_idx].push(c),
                },
                Mode::BraceStart { num_braces } => match c {
                    NAME_BRACE => {
                        mode = Mode::BraceStart {
                            num_braces: num_braces + 1,
                        };
                    }
                    c if num_braces % 2 == 0 => {
                        return Err(E::custom(format!(
                            "character '{c}' must not follow {num_braces} braces"
                        )));
                    }
                    c => {
                        mode = Mode::Braced;

                        for _ in 0..(num_braces / 2) {
                            strings[strings_idx].push(NAME_BRACE);
                        }

                        strings[strings_idx].push(c);
                    }
                },
                Mode::Braced => match c {
                    NAME_BRACE => {
                        mode = Mode::BraceEnd { num_braces: 1 };
                    }
                    c => strings[strings_idx].push(c),
                },
                Mode::BraceEnd { num_braces } => match c {
                    NAME_BRACE => {
                        mode = Mode::BraceEnd {
                            num_braces: num_braces + 1,
                        };
                    }
                    NAME_DELIMITER if num_braces % 2 == 1 => {
                        for _ in 0..(num_braces / 2) {
                            strings[strings_idx].push(NAME_BRACE);
                        }

                        mode = Mode::Simple;
                        strings_idx += 1;
                    }
                    c if num_braces % 2 == 1 => {
                        return Err(E::custom(format!(
                            "character '{c}' must not follow {num_braces} braces"
                        )));
                    }
                    c => {
                        for _ in 0..(num_braces / 2) {
                            strings[strings_idx].push(NAME_BRACE);
                        }

                        mode = Mode::Braced;
                        strings[strings_idx].push(c);
                    }
                },
            }
        }

        match mode {
            Mode::Simple => { /* nothing to do */ }
            Mode::BraceStart { num_braces: _ } => {
                return Err(E::custom("must not start with opening braces"));
            }
            Mode::Braced => {
                return Err(E::custom("name was not closed with closing braces"));
            }
            Mode::BraceEnd { num_braces } => {
                if num_braces % 2 == 0 {
                    return Err(E::custom("name was not closed with closing braces"));
                }

                for _ in 0..(num_braces / 2) {
                    strings[strings_idx].push(NAME_BRACE);
                }
            }
        }

        Ok(strings)
    }
}

impl Visitor<'_> for NamedDataDeserializeVisitor {
    type Value = NamedData;

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
        let strings = Self::parse_string(s)?;
        match strings {
            [namespace, provider, name]
                if !namespace.is_empty() && !provider.is_empty() && !name.is_empty() =>
            {
                Ok(NamedData {
                    namespace: NamedData::canonicalize(namespace, SYSTEM_NAMESPACE),
                    provider: NamedData::canonicalize(provider, SYSTEM_PROVIDER),
                    name,
                })
            }
            [namespace, name, _] if !namespace.is_empty() && !name.is_empty() => Ok(NamedData {
                namespace: NamedData::canonicalize(namespace, SYSTEM_NAMESPACE),
                provider: None,
                name,
            }),
            [name, _, _] if !name.is_empty() => Ok(NamedData {
                namespace: None,
                provider: None,
                name,
            }),
            _ => Err(E::custom("empty named data")),
        }
    }
}

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

/// Checks if a character is not allowed in a dataset name.
#[inline]
pub fn is_invalid_name_char(c: char) -> bool {
    !is_allowed_name_char(c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ser_de_dataset_name_only_name() {
        let json = serde_json::json!("foobar");

        let named_data: NamedData = serde_json::from_value(json.clone()).unwrap();

        assert_eq!(
            named_data,
            NamedData {
                namespace: None,
                provider: None,
                name: "foobar".to_string()
            }
        );

        assert_eq!(serde_json::to_value(&named_data).unwrap(), json);
    }

    #[test]
    fn test_ser_de_dataset_name_namespace() {
        let json = serde_json::json!("foo:bar");

        let named_data: NamedData = serde_json::from_value(json.clone()).unwrap();

        assert_eq!(
            named_data,
            NamedData {
                namespace: Some("foo".to_string()),
                provider: None,
                name: "bar".to_string()
            }
        );

        assert_eq!(serde_json::to_value(&named_data).unwrap(), json);
    }

    #[test]
    fn test_ser_de_dataset_name_system_namespace() {
        let json = serde_json::json!("_:bar");

        let named_data: NamedData = serde_json::from_value(json).unwrap();

        assert_eq!(
            named_data,
            NamedData {
                namespace: None,
                provider: None,
                name: "bar".to_string()
            }
        );

        assert_eq!(
            serde_json::to_value(&named_data).unwrap(),
            serde_json::json!("bar")
        );
    }

    #[test]
    fn test_ser_de_dataset_name_provider() {
        let json = serde_json::json!("foo:bar:baz");

        let named_data: NamedData = serde_json::from_value(json.clone()).unwrap();

        assert_eq!(
            named_data,
            NamedData {
                namespace: Some("foo".to_string()),
                provider: Some("bar".to_string()),
                name: "baz".to_string()
            }
        );

        assert_eq!(serde_json::to_value(&named_data).unwrap(), json);
    }

    #[test]
    fn test_ser_de_dataset_name_system_provider() {
        let json = serde_json::json!("foo:_:baz");

        let named_data: NamedData = serde_json::from_value(json).unwrap();

        assert_eq!(
            named_data,
            NamedData {
                namespace: Some("foo".to_string()),
                provider: None,
                name: "baz".to_string()
            }
        );

        assert_eq!(
            serde_json::to_value(&named_data).unwrap(),
            serde_json::json!("foo:baz")
        );
    }

    #[test]
    fn test_ser_de_dataset_name_errors() {
        serde_json::from_value::<NamedData>(serde_json::json!("foo:bar:baz:boo")).unwrap_err();
        serde_json::from_value::<NamedData>(serde_json::json!("")).unwrap_err();
        serde_json::from_value::<NamedData>(serde_json::json!(":b:c")).unwrap_err();
        serde_json::from_value::<NamedData>(serde_json::json!(":::")).unwrap_err();
    }

    #[test]
    fn test_bracing_deserialization() {
        assert_eq!(
            serde_json::from_value::<NamedData>(serde_json::json!("foo:`bar:baz`")).unwrap(),
            NamedData {
                namespace: Some("foo".to_string()),
                provider: None,
                name: "bar:baz".to_string()
            }
        );

        assert_eq!(
            serde_json::from_value::<NamedData>(serde_json::json!("foo:bar:`fizz:buzz`")).unwrap(),
            NamedData {
                namespace: Some("foo".to_string()),
                provider: Some("bar".to_string()),
                name: "fizz:buzz".to_string()
            }
        );

        assert_eq!(
            serde_json::from_value::<NamedData>(serde_json::json!("foo:```Code```")).unwrap(),
            NamedData {
                namespace: Some("foo".to_string()),
                provider: None,
                name: "`Code`".to_string()
            }
        );

        assert_eq!(
            serde_json::from_value::<NamedData>(serde_json::json!("foo:`bar``baz`")).unwrap(),
            NamedData {
                namespace: Some("foo".to_string()),
                provider: None,
                name: "bar`baz".to_string()
            }
        );

        serde_json::from_value::<NamedData>(serde_json::json!("foo:bar:`")).unwrap_err();
    }

    #[test]
    fn test_bracing_serialization() {
        assert_eq!(
            serde_json::to_value(NamedData {
                namespace: Some("foo".to_string()),
                provider: None,
                name: "bar:baz".to_string()
            })
            .unwrap(),
            serde_json::json!("foo:`bar:baz`"),
        );

        assert_eq!(
            serde_json::to_value(NamedData {
                namespace: Some("foo".to_string()),
                provider: Some("bar".to_string()),
                name: "fizz:buzz".to_string()
            })
            .unwrap(),
            serde_json::json!("foo:bar:`fizz:buzz`"),
        );

        assert_eq!(
            serde_json::to_value(NamedData {
                namespace: Some("foo".to_string()),
                provider: None,
                name: "`Code`".to_string()
            })
            .unwrap(),
            serde_json::json!("foo:```Code```"),
        );

        assert_eq!(
            serde_json::to_value(NamedData {
                namespace: Some("foo".to_string()),
                provider: None,
                name: "bar`baz".to_string()
            })
            .unwrap(),
            serde_json::json!("foo:`bar``baz`"),
        );

        assert_eq!(
            serde_json::to_value(NamedData {
                namespace: Some("foo".to_string()),
                provider: None,
                name: "bar".to_string()
            })
            .unwrap(),
            serde_json::json!("foo:bar"),
        );
    }
}
