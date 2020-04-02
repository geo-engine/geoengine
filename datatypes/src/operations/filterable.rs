use crate::util::Result;

/// Adds filter functionality to a feature collection.
pub trait Filterable: Sized {
    /// Filters the feature collection by copying the data into a new feature collection
    ///
    /// # Errors
    ///
    /// This method fails if the `mask`'s length does not equal the length of the feature collection
    ///
    fn filter(&self, mask: Vec<bool>) -> Result<Self>;
}
