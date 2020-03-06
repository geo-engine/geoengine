use crate::primitives::{FeatureData, FeatureDataRef, TimeInterval};
use crate::util::Result;

/// This trait defines common features of all feature collections
pub trait FeatureCollection {
    /// Returns the number of features
    fn len(&self) -> usize;

    /// Returns whether the feature collection contains no features
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns whether this feature collection is simple, i.e., contains no multi-types
    fn is_simple(&self) -> bool;

    /// Reserved name for feature column
    const FEATURE_COLUMN_NAME: &'static str = "__features";

    /// Reserved name for time column
    const TIME_COLUMN_NAME: &'static str = "__time";

    /// Checks for name conflicts with reserved names
    fn is_reserved_name(name: &str) -> bool {
        name == Self::FEATURE_COLUMN_NAME || name == Self::TIME_COLUMN_NAME
    }

    /// Retrieve column data
    fn data(&self, column: &str) -> Result<FeatureDataRef>;

    /// Retrieve time intervals
    fn time_intervals(&self) -> &[TimeInterval];

    /// Creates a copy of the collection with an additional column
    fn add_column(&self, new_column: &str, data: FeatureData) -> Result<Self>
    where
        Self: Sized;

    /// Removes a column and returns an updated collection
    fn remove_column(&self, column: &str) -> Result<Self>
    where
        Self: Sized;
}

#[cfg(test)]
mod test {
    use super::*;

    struct Dummy(Vec<u16>);

    impl FeatureCollection for Dummy {
        fn len(&self) -> usize {
            self.0.len()
        }
        fn is_simple(&self) -> bool {
            unimplemented!()
        }
        fn data(&self, _column: &str) -> Result<FeatureDataRef> {
            unimplemented!()
        }
        fn time_intervals(&self) -> &[TimeInterval] {
            unimplemented!()
        }
        fn add_column(&self, _new_column: &str, _data: FeatureData) -> Result<Self> {
            unimplemented!()
        }
        fn remove_column(&self, _column: &str) -> Result<Self> {
            unimplemented!()
        }
    }

    #[test]
    fn is_empty() {
        assert!(Dummy(Vec::new()).is_empty());
        assert!(!Dummy(vec![1, 2, 3]).is_empty());
    }

    #[test]
    fn is_reserved_name() {
        assert!(Dummy::is_reserved_name("__features"));
        assert!(!Dummy::is_reserved_name("foobar"));
    }
}
