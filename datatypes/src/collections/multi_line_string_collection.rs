use crate::collections::{
    FeatureCollection, FeatureCollectionRowBuilder, GeoFeatureCollectionRowBuilder,
    IntoGeometryIterator,
};
use crate::primitives::{Coordinate2D, MultiLineString, MultiLineStringAccess, MultiLineStringRef};
use crate::util::arrow::downcast_array;
use crate::util::Result;
use arrow::array::{Array, FixedSizeListArray, Float64Array, ListArray};
use std::slice;

/// This collection contains temporal `MultiLineString`s and miscellaneous data.
pub type MultiLineStringCollection = FeatureCollection<MultiLineString>;

impl<'l> IntoGeometryIterator<'l> for MultiLineStringCollection {
    type GeometryIterator = MultiLineStringIterator<'l>;
    type GeometryType = MultiLineStringRef<'l>;

    fn geometries(&'l self) -> Self::GeometryIterator {
        let geometry_column: &ListArray = downcast_array(
            &self
                .table
                .column_by_name(Self::GEOMETRY_COLUMN_NAME)
                .expect("Column must exist since it is in the metadata"),
        );

        Self::GeometryIterator::new(geometry_column, self.len())
    }
}

/// A collection iterator for `MultiLineString`s
pub struct MultiLineStringIterator<'l> {
    geometry_column: &'l ListArray,
    index: usize,
    length: usize,
}

impl<'l> MultiLineStringIterator<'l> {
    pub fn new(geometry_column: &'l ListArray, length: usize) -> Self {
        Self {
            geometry_column,
            index: 0,
            length,
        }
    }
}

impl<'l> Iterator for MultiLineStringIterator<'l> {
    type Item = MultiLineStringRef<'l>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.length {
            return None;
        }

        let line_array_ref = self.geometry_column.value(self.index);
        let line_array: &ListArray = downcast_array(&line_array_ref);

        let number_of_lines = line_array.len();
        let mut line_coordinate_slices = Vec::with_capacity(number_of_lines);

        for line_index in 0..number_of_lines {
            let coordinate_array_ref = line_array.value(line_index);
            let coordinate_array: &FixedSizeListArray = downcast_array(&coordinate_array_ref);

            let number_of_coordinates = coordinate_array.len();

            let float_array_ref = coordinate_array.value(0);
            let float_array: &Float64Array = downcast_array(&float_array_ref);

            line_coordinate_slices.push(unsafe {
                #[allow(clippy::cast_ptr_alignment)]
                slice::from_raw_parts(
                    float_array.raw_values() as *const Coordinate2D,
                    number_of_coordinates,
                )
            });
        }

        self.index += 1; // increment!

        Some(MultiLineStringRef::new_unchecked(line_coordinate_slices))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.length - self.index;
        (remaining, Some(remaining))
    }

    fn count(self) -> usize {
        self.length - self.index
    }
}

impl GeoFeatureCollectionRowBuilder<MultiLineString>
    for FeatureCollectionRowBuilder<MultiLineString>
{
    fn push_geometry(&mut self, geometry: MultiLineString) -> Result<()> {
        let line_builder = self.geometries_builder.values();

        for line in geometry.lines() {
            let coordinate_builder = line_builder.values();

            for coordinate in line {
                coordinate_builder
                    .values()
                    .append_slice(coordinate.as_ref())?;

                coordinate_builder.append(true)?;
            }

            line_builder.append(true)?;
        }

        self.geometries_builder.append(true)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::collections::BuilderProvider;
    use crate::primitives::TimeInterval;

    #[test]
    fn single_line() {
        let mut builder = MultiLineStringCollection::builder().finish_header();

        builder
            .push_geometry(
                MultiLineString::new(vec![vec![(0.0, 0.1).into(), (1.0, 1.1).into()]]).unwrap(),
            )
            .unwrap();
        builder
            .push_geometry(
                MultiLineString::new(vec![vec![
                    (4.0, 4.1).into(),
                    (5.0, 5.1).into(),
                    (6.0, 6.1).into(),
                ]])
                .unwrap(),
            )
            .unwrap();

        for _ in 0..2 {
            builder.push_time_interval(TimeInterval::default()).unwrap();

            builder.finish_row();
        }

        let collection = builder.build().unwrap();

        assert_eq!(collection.len(), 2);
        assert_eq!(
            collection.time_intervals(),
            &[TimeInterval::default(), TimeInterval::default()]
        );

        assert!(collection.is_simple());

        let mut geometry_iter = collection.geometries();
        assert_eq!(
            geometry_iter.next().unwrap().lines(),
            &[&[(0.0, 0.1).into(), (1.0, 1.1).into()]]
        );
        assert_eq!(
            geometry_iter.next().unwrap().lines(),
            &[&[(4.0, 4.1).into(), (5.0, 5.1).into(), (6.0, 6.1).into(),]]
        );
        assert!(geometry_iter.next().is_none());
    }

    #[test]
    fn multi_lines() {
        let mut builder = MultiLineStringCollection::builder().finish_header();

        builder
            .push_geometry(
                MultiLineString::new(vec![vec![(0.0, 0.1).into(), (1.0, 1.1).into()]]).unwrap(),
            )
            .unwrap();
        builder
            .push_geometry(
                MultiLineString::new(vec![
                    vec![(4.0, 4.1).into(), (5.0, 5.1).into(), (6.0, 6.1).into()],
                    vec![(7.0, 7.1).into(), (8.0, 8.1).into(), (9.0, 9.1).into()],
                ])
                .unwrap(),
            )
            .unwrap();

        for _ in 0..2 {
            builder.push_time_interval(TimeInterval::default()).unwrap();

            builder.finish_row();
        }

        let collection = builder.build().unwrap();

        assert_eq!(collection.len(), 2);
        assert_eq!(
            collection.time_intervals(),
            &[TimeInterval::default(), TimeInterval::default()]
        );

        assert!(!collection.is_simple());

        let mut geometry_iter = collection.geometries();
        assert_eq!(
            geometry_iter.next().unwrap().lines(),
            &[&[(0.0, 0.1).into(), (1.0, 1.1).into()]]
        );
        assert_eq!(
            geometry_iter.next().unwrap().lines(),
            &[
                &[(4.0, 4.1).into(), (5.0, 5.1).into(), (6.0, 6.1).into()],
                &[(7.0, 7.1).into(), (8.0, 8.1).into(), (9.0, 9.1).into()],
            ]
        );
        assert!(geometry_iter.next().is_none());
    }

    #[test]
    fn filter() {
        let mut builder = MultiLineStringCollection::builder().finish_header();

        builder
            .push_geometry(
                MultiLineString::new(vec![vec![(0.0, 0.1).into(), (1.0, 1.1).into()]]).unwrap(),
            )
            .unwrap();
        builder
            .push_geometry(
                MultiLineString::new(vec![
                    vec![(4.0, 4.1).into(), (5.0, 5.1).into(), (6.0, 6.1).into()],
                    vec![(7.0, 7.1).into(), (8.0, 8.1).into(), (9.0, 9.1).into()],
                ])
                .unwrap(),
            )
            .unwrap();
        builder
            .push_geometry(
                MultiLineString::new(vec![
                    vec![(10.0, 10.1).into(), (11.0, 11.1).into()],
                    vec![
                        (12.0, 12.1).into(),
                        (13.0, 13.1).into(),
                        (14.0, 14.1).into(),
                    ],
                ])
                .unwrap(),
            )
            .unwrap();

        for _ in 0..3 {
            builder.push_time_interval(TimeInterval::default()).unwrap();

            builder.finish_row();
        }

        let collection = builder.build().unwrap();

        assert_eq!(collection.len(), 3);

        let collection = collection.filter(vec![true, false, true]).unwrap();

        assert_eq!(collection.len(), 2);

        let mut geometry_iter = collection.geometries();
        assert_eq!(
            geometry_iter.next().unwrap().lines(),
            &[&[(0.0, 0.1).into(), (1.0, 1.1).into()]]
        );
        assert_eq!(
            geometry_iter.next().unwrap().lines(),
            &[
                &[(10.0, 10.1).into(), (11.0, 11.1).into()] as &[_],
                &[
                    (12.0, 12.1).into(),
                    (13.0, 13.1).into(),
                    (14.0, 14.1).into(),
                ] as &[_],
            ]
        );
        assert!(geometry_iter.next().is_none());
    }

    #[test]
    fn append() {
        let mut builder = MultiLineStringCollection::builder().finish_header();

        builder
            .push_geometry(
                MultiLineString::new(vec![vec![(0.0, 0.1).into(), (1.0, 1.1).into()]]).unwrap(),
            )
            .unwrap();
        builder.push_time_interval(TimeInterval::default()).unwrap();
        builder.finish_row();

        let collection_a = builder.build().unwrap();

        let mut builder = MultiLineStringCollection::builder().finish_header();

        builder
            .push_geometry(
                MultiLineString::new(vec![
                    vec![(4.0, 4.1).into(), (5.0, 5.1).into(), (6.0, 6.1).into()],
                    vec![(7.0, 7.1).into(), (8.0, 8.1).into(), (9.0, 9.1).into()],
                ])
                .unwrap(),
            )
            .unwrap();
        builder.push_time_interval(TimeInterval::default()).unwrap();
        builder.finish_row();

        let collection_b = builder.build().unwrap();

        let collection_c = collection_a.append(&collection_b).unwrap();

        assert_eq!(collection_a.len(), 1);
        assert_eq!(collection_b.len(), 1);
        assert_eq!(collection_c.len(), 2);

        assert_eq!(collection_a.is_simple(), true);
        assert_eq!(collection_b.is_simple(), false);
        assert_eq!(collection_c.is_simple(), false);

        let mut geometry_iter = collection_c.geometries();
        assert_eq!(
            geometry_iter.next().unwrap().lines(),
            &[&[(0.0, 0.1).into(), (1.0, 1.1).into()]]
        );
        assert_eq!(
            geometry_iter.next().unwrap().lines(),
            &[
                &[(4.0, 4.1).into(), (5.0, 5.1).into(), (6.0, 6.1).into()] as &[_],
                &[(7.0, 7.1).into(), (8.0, 8.1).into(), (9.0, 9.1).into()] as &[_],
            ]
        );
        assert!(geometry_iter.next().is_none());
    }
}
