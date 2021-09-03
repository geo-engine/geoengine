use std::fmt::Display;

use crate::error::Error;
use crate::util::Result;

use serde::{Deserialize, Serialize};

const MAX_STRINGS_IN_SAMPLE: usize = 3;

#[derive(Debug, Clone, PartialEq)]
pub enum AttributeAggregate {
    MeanNumber(MeanAggregator),
    StringSample(StringSampler),
    Null, // Representing a missing aggregate
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AttributeAggregateType {
    MeanNumber,
    StringSample,
    Null, // Representing a missing aggregate
}

pub trait AttributeAggregator {
    fn merge(&mut self, other: &Self) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct MeanAggregator {
    pub mean: f64,
    n: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StringSampler {
    pub strings: Vec<String>,
}

impl Default for AttributeAggregate {
    fn default() -> Self {
        Self::Null
    }
}

impl MeanAggregator {
    pub fn from_value(value: f64) -> Self {
        MeanAggregator { mean: value, n: 1 }
    }
}

impl Display for AttributeAggregateType {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            AttributeAggregateType::MeanNumber => fmt.write_str("MeanNumber"),
            AttributeAggregateType::StringSample => fmt.write_str("StringSample"),
            AttributeAggregateType::Null => fmt.write_str("Null"),
        }
    }
}

impl AttributeAggregate {
    /// Merge two aggregates. Sets it to `Null` if there is any error.
    pub fn merge(&mut self, other: &AttributeAggregate) {
        // TODO: nulls are ignored by now but we should make this user-defined in the future

        let mut self_and_other = (self, other);

        let was_error = match &mut self_and_other {
            (AttributeAggregate::MeanNumber(a), AttributeAggregate::MeanNumber(b)) => a.merge(b),

            (AttributeAggregate::StringSample(a), AttributeAggregate::StringSample(b)) => {
                a.merge(b)
            }
            // if there is null on the other side, just leave it as it is
            (_, AttributeAggregate::Null) => Ok(()),
            // if there is null on this side, just take the other side
            (this @ &mut AttributeAggregate::Null, other) => {
                **this = other.clone();
                Ok(())
            }
            // for now, if anything weird happens, just set it to null
            (this, o) => Err(Error::InvalidType {
                expected: this.variant_type().to_string(),
                found: o.variant_type().to_string(),
            }),
        }
        .is_err();

        if was_error {
            *self_and_other.0 = AttributeAggregate::Null;
        }
    }

    pub fn variant_type(&self) -> AttributeAggregateType {
        match self {
            AttributeAggregate::MeanNumber(_) => AttributeAggregateType::MeanNumber,
            AttributeAggregate::StringSample(_) => AttributeAggregateType::StringSample,
            AttributeAggregate::Null => AttributeAggregateType::Null,
        }
    }

    pub fn mean_number(&self) -> Option<&MeanAggregator> {
        match self {
            AttributeAggregate::MeanNumber(a) => Some(a),
            _ => None,
        }
    }

    pub fn string_sample(&self) -> Option<&StringSampler> {
        match self {
            AttributeAggregate::StringSample(a) => Some(a),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, AttributeAggregate::Null)
    }
}

impl AttributeAggregator for MeanAggregator {
    fn merge(&mut self, other: &Self) -> Result<()> {
        self.mean = ((self.mean * self.n as f64) + (other.mean * other.n as f64))
            / (self.n + other.n) as f64;
        self.n += other.n;
        Ok(())
    }
}

impl StringSampler {
    pub fn from_value(value: String) -> Self {
        Self {
            strings: vec![value],
        }
    }
}

impl AttributeAggregator for StringSampler {
    fn merge(&mut self, other: &Self) -> Result<()> {
        // TODO: use sampling

        let discrepancy = MAX_STRINGS_IN_SAMPLE - self.strings.len();

        for new_string in other.strings.iter().take(discrepancy) {
            self.strings.push(new_string.clone());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_mean() {
        let mut mean = MeanAggregator::from_value(4.);
        mean.merge(&MeanAggregator::from_value(2.)).unwrap();

        assert_eq!(mean.mean, 3.);

        mean.merge(&MeanAggregator::from_value(3.)).unwrap();

        assert_eq!(mean.mean, 3.);

        mean.merge(&MeanAggregator::from_value(7.)).unwrap();

        assert_eq!(mean.mean, 4.);

        let mut mean_a = MeanAggregator::from_value(4.);
        let mut mean_b = MeanAggregator::from_value(8.);
        for _ in 0..10 {
            mean_a.merge(&MeanAggregator::from_value(4.)).unwrap();
            mean_b.merge(&MeanAggregator::from_value(8.)).unwrap();
        }
        mean_a.merge(&mean_b).unwrap();

        assert_eq!(mean_a.mean, 6.);
    }

    #[test]
    fn test_strings() {
        // TODO: this test will change when sampling is implemented

        let mut strings = StringSampler::from_value("foo".to_string());
        strings
            .merge(&StringSampler::from_value("bar".to_string()))
            .unwrap();
        strings
            .merge(&StringSampler::from_value("baz".to_string()))
            .unwrap();
        strings
            .merge(&StringSampler::from_value("bau".to_string()))
            .unwrap();

        assert_eq!(strings.strings, vec!["foo", "bar", "baz"]);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_attribute_aggregate_mean() {
        let mut mean = AttributeAggregate::MeanNumber(MeanAggregator::from_value(1.));
        mean.merge(&AttributeAggregate::MeanNumber(MeanAggregator::from_value(
            3.,
        )));

        assert_eq!(mean.mean_number().unwrap().mean, 2.);

        mean.merge(&AttributeAggregate::Null);

        assert_eq!(mean.mean_number().unwrap().mean, 2.);
    }

    #[test]
    fn test_attribute_aggregate_strings() {
        let mut strings =
            AttributeAggregate::StringSample(StringSampler::from_value("foo".to_string()));
        strings.merge(&AttributeAggregate::StringSample(
            StringSampler::from_value("bar".to_string()),
        ));

        assert_eq!(
            &strings.string_sample().unwrap().strings,
            &["foo".to_string(), "bar".to_string()]
        );

        strings.merge(&AttributeAggregate::Null);

        assert_eq!(
            &strings.string_sample().unwrap().strings,
            &["foo".to_string(), "bar".to_string()]
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_attribute_aggregate_nulls() {
        let mut mean = AttributeAggregate::Null;
        mean.merge(&AttributeAggregate::MeanNumber(MeanAggregator::from_value(
            42.,
        )));

        assert_eq!(mean.mean_number().unwrap().mean, 42.);

        mean.merge(&AttributeAggregate::StringSample(
            StringSampler::from_value("foo".to_string()),
        ));

        assert!(mean.is_null());
    }
}
