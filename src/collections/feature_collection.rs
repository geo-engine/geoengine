/// This trait defines common features of all featurecollections
pub trait FeatureCollection {
    /// Returns the number of features
    fn len(&self) -> usize;

    /// Returns whether the feature collection contains no features
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod test {
    use crate::collections::FeatureCollection;

    #[test]
    fn is_empty() {
        struct Dummy(Vec<u16>);
        impl FeatureCollection for Dummy {
            fn len(&self) -> usize {
                self.0.len()
            }
        }

        assert!(Dummy(Vec::new()).is_empty());
        assert!(!Dummy(vec![1, 2, 3]).is_empty());
    }
}
