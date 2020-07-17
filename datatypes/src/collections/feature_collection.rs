use crate::collections::IntoGeometryOptionsIterator;
use crate::primitives::{FeatureData, FeatureDataRef, TimeInterval};
use crate::util::Result;

/// This trait defines common features of all feature collections
pub trait FeatureCollection
where
    Self: Clone + Sized + PartialEq,
{
    /// Returns the number of features
    fn len(&self) -> usize;

    /// Returns whether the feature collection contains no features
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns whether this feature collection is simple, i.e., contains no multi-types
    fn is_simple(&self) -> bool;

    /// Reserved name for geometry column
    const GEOMETRY_COLUMN_NAME: &'static str = "__geometry";

    /// Reserved name for time column
    const TIME_COLUMN_NAME: &'static str = "__time";

    /// Checks for name conflicts with reserved names
    fn is_reserved_name(name: &str) -> bool {
        name == Self::GEOMETRY_COLUMN_NAME || name == Self::TIME_COLUMN_NAME
    }

    /// Retrieve column data
    ///
    /// # Errors
    ///
    /// This method fails if there is no `column` with that name
    ///
    fn data(&self, column: &str) -> Result<FeatureDataRef>;

    /// Retrieve time intervals
    fn time_intervals(&self) -> &[TimeInterval];

    /// Creates a copy of the collection with an additional column
    ///
    /// # Errors
    ///
    /// Adding a column fails if the column does already exist or the length does not match the length of the collection
    ///
    fn add_column(&self, new_column: &str, data: FeatureData) -> Result<Self>;

    /// Creates a copy of the collection with additional columns
    ///
    /// # Errors
    ///
    /// Adding columns fails if any column does already exist or the lengths do not match the length of the collection
    ///
    fn add_columns(&self, new_columns: &[(&str, FeatureData)]) -> Result<Self>;

    /// Removes a column and returns an updated collection
    ///
    /// # Errors
    ///
    /// Removing a column fails if the column does not exist (or is reserved, e.g., the geometry column)
    ///
    fn remove_column(&self, column: &str) -> Result<Self>;

    /// Removes columns and returns an updated collection
    ///
    /// # Errors
    ///
    /// Removing columns fails if any column does not exist (or is reserved, e.g., the geometry column)
    ///
    fn remove_columns(&self, columns: &[&str]) -> Result<Self>;

    /// Filters the feature collection by copying the data into a new feature collection
    ///
    /// # Errors
    ///
    /// This method fails if the `mask`'s length does not equal the length of the feature collection
    ///
    fn filter(&self, mask: Vec<bool>) -> Result<Self>;

    /// Appends a collection to another one
    ///
    /// # Errors
    ///
    /// This method fails if the columns do not match
    ///
    fn append(&self, other: &Self) -> Result<Self>;

    /// Serialize the feature collection to a geo json string
    fn to_geo_json<'i>(&'i self) -> String
    where
        Self: IntoGeometryOptionsIterator<'i>;
}

#[cfg(test)]
mod test_default_impls {
    use super::*;

    #[derive(Clone, PartialEq)]
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
        fn add_columns(&self, _new_columns: &[(&str, FeatureData)]) -> Result<Self> {
            unimplemented!()
        }
        fn remove_column(&self, _column: &str) -> Result<Self> {
            unimplemented!()
        }
        fn remove_columns(&self, _columns: &[&str]) -> Result<Self> {
            unimplemented!()
        }
        fn filter(&self, _mask: Vec<bool>) -> Result<Self> {
            unimplemented!()
        }
        fn append(&self, _other: &Self) -> Result<Self> {
            unimplemented!()
        }
        fn to_geo_json<'i>(&'i self) -> String
        where
            Self: IntoGeometryOptionsIterator<'i>,
        {
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
        assert!(Dummy::is_reserved_name("__geometry"));
        assert!(!Dummy::is_reserved_name("foobar"));
    }
}
