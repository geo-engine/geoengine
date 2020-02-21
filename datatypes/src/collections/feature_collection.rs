use crate::primitives::FeatureDataRef;
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
    const FEATURE_FIELD: &'static str = "__features";

    /// Reserved name for time column
    const TIME_FIELD: &'static str = "__time";

    /// Checks for name conflicts with reserved names
    fn is_reserved_name(name: &str) -> bool {
        name == Self::FEATURE_FIELD || name == Self::TIME_FIELD
    }

    fn data(&self, field: &str) -> Result<FeatureDataRef>;
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
        fn data(&self, _field: &str) -> Result<FeatureDataRef> {
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
