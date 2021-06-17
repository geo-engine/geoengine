use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
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
            Measurement::Classification { measurement, .. } => write!(f, "{}", measurement),
        }
    }
}
