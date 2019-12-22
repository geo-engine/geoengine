use crate::util::Result;

/// Adds filter functionality to a feature_collection.
pub trait Filterable: Sized {
    /// Filters the feature_collection by copying the data into a new feature_collection
    fn filter(&self, mask: Vec<bool>) -> Result<Self>;
}
