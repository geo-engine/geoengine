use crate::primitives::Coordinate;
use crate::util::Result;
use snafu::Snafu;

/// Adds filter functionality to a feature_collection.
pub trait Filterable: Sized {
    /// Filters the feature_collection by copying the data into a new feature_collection
    fn filter(&self, mask: &[bool]) -> Result<Self>;

    /// Filters the feature_collection using a predicate function by copying the data into a new feature_collection
    fn filter_with_predicate<P>(&self, predicate: P) -> Self
    where
        P: FnMut(&[Coordinate]) -> bool;

    /// Filters the feature_collection inplace
    fn filter_inplace(&mut self, mask: &[bool]) -> Result<()>;

    /// Filters the feature_collection inplace using a predicate function
    fn filter_inplace_with_predicate<P>(&mut self, predicate: P)
    where
        P: FnMut(&[Coordinate]) -> bool;
}

#[derive(Debug, Snafu)]
pub enum FilterableError {
    #[snafu(display("Mask does not match features"))]
    MaskDoesNotMatchFeatures,
}
