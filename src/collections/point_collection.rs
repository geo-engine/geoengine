use crate::collections::{FeatureCollection, FeatureCollectionError};
use crate::operations::{Filterable, FilterableError};
use crate::primitives::Coordinate;
use crate::util::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PointCollection {
    feature_indices: Vec<usize>,
    coordinates: Vec<Coordinate>,
}

impl Default for PointCollection {
    fn default() -> Self {
        Self {
            feature_indices: vec![0],
            coordinates: Vec::new(),
        }
    }
}

impl PointCollection {
    /// Create a new, empty collection
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{PointCollection, FeatureCollection};
    ///
    /// let pc = PointCollection::new();
    ///
    /// assert_eq!(pc.len(), 0);
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a collection from data and perform checks
    pub fn from_data(feature_indices: Vec<usize>, coordinates: Vec<Coordinate>) -> Result<Self> {
        let instance = Self {
            feature_indices,
            coordinates,
        };

        if instance.is_valid() {
            Ok(instance)
        } else {
            Err(FeatureCollectionError::UnmatchedFeatureIndices.into())
        }
    }

    /// Create a collection from data without checking its validity
    pub unsafe fn from_data_unchecked(
        feature_indices: Vec<usize>,
        coordinates: Vec<Coordinate>,
    ) -> Self {
        Self {
            feature_indices,
            coordinates,
        }
    }

    /// Add a new point to the colleciton
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::collections::{PointCollection, FeatureCollection};
    ///
    /// let mut pc = PointCollection::new();
    /// pc.add_point((0.0, 0.0).into());
    ///
    /// assert_eq!(pc.len(), 1);
    /// ```
    pub fn add_point(&mut self, coordinate: Coordinate) {
        self.coordinates.push(coordinate);
        self.feature_indices.push(self.coordinates.len());
    }

    /// Add a new multi point to the colleciton
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::collections::{PointCollection, FeatureCollection};
    ///
    /// let mut pc = PointCollection::new();
    /// pc.add_multipoint(&[(0.0, 0.0).into(), (1.0, 1.0).into()]);
    ///
    /// assert_eq!(pc.len(), 1);
    /// ```
    pub fn add_multipoint(&mut self, coordinates: &[Coordinate]) {
        if !coordinates.is_empty() {
            self.coordinates.extend_from_slice(coordinates);
            self.feature_indices.push(self.coordinates.len());
        }
    }

    /// Checks whether this collection is valid
    fn is_valid(&self) -> bool {
        // vector must not be empty
        let last_feature_index = if let Some(&i) = self.feature_indices.last() {
            i
        } else {
            return false;
        };

        // last index has to point one position beyond the coordinates
        if last_feature_index != self.coordinates.len() {
            return false;
        }

        // TODO: do we really want to check this?
        // feature indices have to be monotonically increasing
        if self
            .feature_indices
            .windows(2)
            .any(|coords| coords[0] >= coords[1])
        {
            return false;
        }

        true
    }

    /// Allows iterating over geo::Point.
    /// Does not check if this collection represents multi points or simple points.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::collections::{PointCollection, FeatureCollection};
    ///
    /// let mut pc = PointCollection::new();
    /// pc.add_point((0.0, 0.0).into());
    /// pc.add_point((1.0, 1.0).into());
    ///
    /// let mut geo_points = pc.geo_points_iter();
    /// assert_eq!(geo_points.next().unwrap(), geo::Point::new(0.0, 0.0));
    /// assert_eq!(geo_points.next().unwrap(), geo::Point::new(1.0, 1.0));
    ///
    /// assert_eq!(pc.len(), 2);
    /// ```
    ///
    pub fn geo_points_iter<'c>(&'c self) -> impl Iterator<Item = geo::Point<f64>> + 'c {
        self.coordinates.iter().map(|c| geo::Point::new(c.x, c.y))
    }

    /// Allows iterating over geo::MultiPoint.
    /// Does not check if this collection represents multi points or simple points.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::collections::{PointCollection, FeatureCollection};
    ///
    /// let mut pc = PointCollection::new();
    /// pc.add_point((0.0, 0.0).into());
    /// pc.add_multipoint(&[(1.0, 1.0).into(), (2.0, 2.0).into()]);
    ///
    /// let mut geo_points = pc.geo_multi_points_iter();
    /// assert_eq!(geo_points.next().unwrap(), vec![(0.0, 0.0)].into());
    /// assert_eq!(
    ///     geo_points.next().unwrap(),
    ///     vec![(1.0, 1.0), (2.0, 2.0)].into()
    /// );
    ///
    /// assert_eq!(pc.len(), 2);
    /// ```
    ///
    pub fn geo_multi_points_iter<'c>(&'c self) -> impl Iterator<Item = geo::MultiPoint<f64>> + 'c {
        self.feature_indices.windows(2).map(move |window| {
            let (start, end) = (window[0], window[1]);
            self.coordinates[start..end]
                .iter()
                .map(|c| geo::Point::new(c.x, c.y))
                .collect()
        })
    }
}

impl FeatureCollection for PointCollection {
    fn len(&self) -> usize {
        self.feature_indices.len() - 1
    }

    fn is_simple(&self) -> bool {
        (self.feature_indices.len() - 1) == self.coordinates.len()
    }

    fn remove_last_feature(&mut self) -> Result<()> {
        if self.feature_indices.len() <= 1 {
            return Err(FeatureCollectionError::DeleteFromEmpty.into());
        }

        self.feature_indices.pop().unwrap();

        self.coordinates
            .resize_with(*self.feature_indices.last().unwrap(), || unreachable!());

        Ok(())
    }
}

impl Filterable for PointCollection {
    fn filter(&self, mask: &[bool]) -> Result<Self> {
        if mask.len() != self.feature_indices.len() - 1 {
            return Err(FilterableError::MaskDoesNotMatchFeatures.into());
        }

        let mut filtered_feature_indices = Vec::new();
        let mut filtered_coordinates = Vec::new();

        for ((start, end), &flag) in self
            .feature_indices
            .windows(2)
            .map(|window| (window[0], window[1]))
            .zip(mask)
        {
            if flag {
                filtered_feature_indices.push(filtered_coordinates.len());

                filtered_coordinates.extend_from_slice(&self.coordinates[start..end]);
            }
        }
        filtered_feature_indices.push(filtered_coordinates.len());

        Ok(Self {
            feature_indices: filtered_feature_indices,
            coordinates: filtered_coordinates,
        })
    }

    fn filter_with_predicate<P>(&self, mut predicate: P) -> Self
    where
        P: FnMut(&[Coordinate]) -> bool,
    {
        let mut filtered_feature_indices = Vec::new();
        let mut filtered_coordinates = Vec::new();

        for (start, end) in self
            .feature_indices
            .windows(2)
            .map(|window| (window[0], window[1]))
        {
            let coordinates = &self.coordinates[start..end]; // point or multipoint coordinates
            if predicate(coordinates) {
                filtered_feature_indices.push(filtered_coordinates.len());

                filtered_coordinates.extend_from_slice(coordinates);
            }
        }
        filtered_feature_indices.push(filtered_coordinates.len());

        Self {
            feature_indices: filtered_feature_indices,
            coordinates: filtered_coordinates,
        }
    }

    fn filter_inplace(&mut self, mask: &[bool]) -> Result<()> {
        if mask.len() != self.feature_indices.len() - 1 {
            return Err(FilterableError::MaskDoesNotMatchFeatures.into());
        }

        let mut feature_index = 0;
        let mut coordinate_start = 0;
        for (i, &flag) in mask.iter().enumerate() {
            let (start, end) = (self.feature_indices[i], self.feature_indices[i + 1]);
            if flag {
                self.feature_indices[feature_index] = coordinate_start;

                let number_of_coordinates = end - start;

                self.coordinates.copy_within(start..end, coordinate_start);

                feature_index += 1;
                coordinate_start += number_of_coordinates;
            }
        }
        self.feature_indices[feature_index] = coordinate_start;

        self.feature_indices
            .resize_with(feature_index + 1, || unreachable!());
        self.coordinates
            .resize_with(coordinate_start, || unreachable!());

        Ok(())
    }

    fn filter_inplace_with_predicate<P>(&mut self, mut predicate: P)
    where
        P: FnMut(&[Coordinate]) -> bool,
    {
        let mut feature_index = 0;
        let mut coordinate_start = 0;
        for i in 0..self.len() {
            let (start, end) = (self.feature_indices[i], self.feature_indices[i + 1]);
            if predicate(&self.coordinates[start..end]) {
                // point or multipoint coordinates
                self.feature_indices[feature_index] = coordinate_start;

                let number_of_coordinates = end - start;

                self.coordinates.copy_within(start..end, coordinate_start);

                feature_index += 1;
                coordinate_start += number_of_coordinates;
            }
        }
        self.feature_indices[feature_index] = coordinate_start;

        self.feature_indices
            .resize_with(feature_index + 1, || unreachable!());
        self.coordinates
            .resize_with(coordinate_start, || unreachable!());
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn default() {
        let new = PointCollection::new();
        let default = PointCollection::default();

        assert_eq!(new.feature_indices, default.feature_indices);
        assert_eq!(new.coordinates, default.coordinates);
    }

    #[test]
    fn from_data() {
        let features = vec![0, 1, 2];
        let coordinates = vec![(0.0, 0.0).into(), (1.0, 1.0).into()];

        let pc1 = PointCollection::from_data(features.clone(), coordinates.clone()).unwrap();
        let pc2 =
            unsafe { PointCollection::from_data_unchecked(features.clone(), coordinates.clone()) };

        assert_eq!(pc1.feature_indices, pc2.feature_indices);
        assert_eq!(pc1.coordinates, pc2.coordinates);

        PointCollection::from_data(Vec::new(), Vec::new()).unwrap_err();

        let invalid_pc = unsafe { PointCollection::from_data_unchecked(Vec::new(), Vec::new()) };

        assert!(!invalid_pc.is_valid());
    }

    #[test]
    fn filter() {
        let mut pc = PointCollection::new();
        pc.add_point((0.0, 0.0).into());
        pc.add_point((1.0, 1.0).into());
        pc.add_point((2.0, 2.0).into());

        assert_eq!(pc.len(), 3);

        let filtered = pc.filter(&[true, false, true]).unwrap();

        assert_eq!(filtered.len(), 2);
        assert_eq!(
            filtered.coordinates,
            vec![(0.0, 0.0).into(), (2.0, 2.0).into()]
        );
    }

    #[test]
    fn filter_with_predicate() {
        let mut pc = PointCollection::new();
        pc.add_point((0.0, 0.0).into());
        pc.add_point((1.0, 1.0).into());
        pc.add_point((2.0, 2.0).into());

        assert_eq!(pc.len(), 3);

        let filtered = pc.filter_with_predicate(|points| points[0] != (1.0, 1.0).into());

        assert_eq!(filtered.len(), 2);
        assert_eq!(
            filtered.coordinates,
            vec![(0.0, 0.0).into(), (2.0, 2.0).into()]
        );
    }

    #[test]
    fn filter_inplace() {
        let mut pc = PointCollection::new();
        pc.add_point((0.0, 0.0).into());
        pc.add_point((1.0, 1.0).into());
        pc.add_point((2.0, 2.0).into());
        assert_eq!(pc.len(), 3);

        pc.filter_inplace(&[true, false, true]).unwrap();

        assert_eq!(pc.len(), 2);
        assert_eq!(pc.coordinates, vec![(0.0, 0.0).into(), (2.0, 2.0).into()]);
    }

    #[test]
    fn filter_inplace_with_predicate() {
        let mut pc = PointCollection::new();
        pc.add_point((0.0, 0.0).into());
        pc.add_point((1.0, 1.0).into());
        pc.add_point((2.0, 2.0).into());

        assert_eq!(pc.len(), 3);

        pc.filter_inplace_with_predicate(|points| points[0] != (1.0, 1.0).into());

        assert_eq!(pc.len(), 2);
        assert_eq!(pc.coordinates, vec![(0.0, 0.0).into(), (2.0, 2.0).into()]);
    }

    #[test]
    fn add_point() {
        let mut pc = PointCollection::new();

        pc.add_point((0.0, 0.0).into());

        assert_eq!(pc.len(), 1);
        assert_eq!(pc.coordinates, vec![(0.0, 0.0).into()]);
        assert_eq!(pc.feature_indices, vec![0, 1]);

        pc.add_multipoint(&[(1.0, 1.0).into(), (2.0, 2.0).into()]);

        assert_eq!(pc.len(), 2);
        assert_eq!(
            pc.coordinates,
            vec![(0.0, 0.0).into(), (1.0, 1.0).into(), (2.0, 2.0).into()]
        );
        assert_eq!(pc.feature_indices, vec![0, 1, 3]);
    }

    #[test]
    fn empty() {
        let mut pc = PointCollection::new();

        assert!(pc.is_empty());

        pc.add_point((0.1, 2.3).into());

        assert!(!pc.is_empty());
    }

    #[test]
    fn is_simple() {
        assert!(PointCollection::new().is_simple());

        let mut pc = PointCollection::new();
        pc.add_point((0.0, 0.0).into());
        pc.add_point((1.0, 1.0).into());
        pc.add_point((2.0, 2.0).into());

        assert!(pc.is_simple());

        pc.add_multipoint(&[(3.0, 3.0).into()]);

        assert!(pc.is_simple());

        pc.add_multipoint(&[(4.0, 4.0).into(), (5.0, 5.0).into()]);

        assert!(!pc.is_simple());
    }

    #[test]
    fn delete() {
        let mut pc = PointCollection::new();
        pc.add_point((0.0, 0.0).into());
        pc.add_point((1.0, 1.0).into());

        assert_eq!(pc.len(), 2);

        pc.remove_last_feature().unwrap();

        assert_eq!(pc.len(), 1);

        pc.remove_last_feature().unwrap();

        assert!(pc.is_empty());

        pc.add_multipoint(&[(4.0, 4.0).into(), (5.0, 5.0).into()]);

        assert_eq!(pc.len(), 1);

        pc.remove_last_feature().unwrap();

        assert!(pc.is_empty());
    }
}
