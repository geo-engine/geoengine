use crate::primitives::Coordinate;

/// Adds filter functionality to a feature_collection.
pub trait Filterable {
    /// Filters the feature_collection by copying the data into a new feature_collection
    fn filter(&self, mask: &[bool]) -> Self;

    /// Filters the feature_collection using a predicate function by copying the data into a new feature_collection
    fn filter_with_predicate<P>(&self, predicate: P) -> Self
    where
        P: FnMut(&Coordinate) -> bool;

    /// Filters the feature_collection inplace
    fn filter_inplace(&mut self, mask: &[bool]);

    /// Filters the feature_collection inplace using a predicate function
    fn filter_inplace_with_predicate<P>(&mut self, predicate: P)
    where
        P: FnMut(&Coordinate) -> bool;
}
