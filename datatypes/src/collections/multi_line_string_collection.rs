use crate::collections::{
    FeatureCollection, FeatureCollectionInfos, FeatureCollectionIterator, FeatureCollectionRow,
    FeatureCollectionRowBuilder, GeoFeatureCollectionRowBuilder, GeometryCollection,
    GeometryRandomAccess, IntoGeometryIterator,
};
use crate::primitives::{Coordinate2D, MultiLineString, MultiLineStringAccess, MultiLineStringRef};
use crate::util::arrow::{downcast_array, ArrowTyped};
use crate::util::Result;
use arrow::{
    array::{Array, ArrayData, FixedSizeListArray, Float64Array, ListArray},
    buffer::Buffer,
    datatypes::DataType,
};
use rayon::iter::plumbing::Producer;
use rayon::prelude::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use std::{slice, sync::Arc};

use super::geo_feature_collection::ReplaceRawArrayCoords;

/// This collection contains temporal `MultiLineString`s and miscellaneous data.
pub type MultiLineStringCollection = FeatureCollection<MultiLineString>;

impl GeometryCollection for MultiLineStringCollection {
    fn coordinates(&self) -> &[Coordinate2D] {
        let geometries_ref = self
            .table
            .column_by_name(Self::GEOMETRY_COLUMN_NAME)
            .expect("There should exist a geometry column because it is added during creation of the collection");
        let geometries: &ListArray = downcast_array(geometries_ref);

        let line_strings_ref = geometries.values();
        let line_strings: &ListArray = downcast_array(line_strings_ref);

        let coordinates_ref = line_strings.values();
        let coordinates: &FixedSizeListArray = downcast_array(coordinates_ref);

        let number_of_coordinates = coordinates.len();

        let floats_ref = coordinates.values();
        let floats: &Float64Array = downcast_array(floats_ref);

        unsafe {
            slice::from_raw_parts(
                floats.values().as_ptr().cast::<Coordinate2D>(),
                number_of_coordinates,
            )
        }
    }

    fn feature_offsets(&self) -> &[i32] {
        let geometries_ref = self
            .table
            .column_by_name(Self::GEOMETRY_COLUMN_NAME)
            .expect("There should exist a geometry column because it is added during creation of the collection");
        let geometries: &ListArray = downcast_array(geometries_ref);

        geometries.offsets()
    }
}

impl MultiLineStringCollection {
    pub fn line_string_offsets(&self) -> &[i32] {
        let geometries_ref = self
            .table
            .column_by_name(Self::GEOMETRY_COLUMN_NAME)
            .expect("There should exist a geometry column because it is added during creation of the collection");
        let geometries: &ListArray = downcast_array(geometries_ref);

        let line_strings_ref = geometries.values();
        let line_strings: &ListArray = downcast_array(line_strings_ref);

        line_strings.offsets()
    }
}

impl<'l> IntoGeometryIterator<'l> for MultiLineStringCollection {
    type GeometryIterator = MultiLineStringIterator<'l>;
    type GeometryType = MultiLineStringRef<'l>;

    fn geometries(&'l self) -> Self::GeometryIterator {
        let geometry_column: &ListArray = downcast_array(
            self.table
                .column_by_name(Self::GEOMETRY_COLUMN_NAME)
                .expect("There should exist a geometry column because it is added during creation of the collection"),
        );

        Self::GeometryIterator::new(geometry_column, self.len())
    }
}

impl<'a> IntoIterator for &'a MultiLineStringCollection {
    type Item = FeatureCollectionRow<'a, MultiLineStringRef<'a>>;
    type IntoIter = FeatureCollectionIterator<'a, MultiLineStringIterator<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        FeatureCollectionIterator::new::<MultiLineString>(self, self.geometries())
    }
}

/// A collection iterator for [`MultiLineString`]s
pub struct MultiLineStringIterator<'l> {
    geometry_column: &'l ListArray,
    index: usize,
    length: usize,
}

/// A parallel collection iterator for [`MultiLineString`}s
pub struct MultiLineStringParIterator<'l>(MultiLineStringIterator<'l>);

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
                    float_array.values().as_ptr().cast::<Coordinate2D>(),
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

impl<'l> DoubleEndedIterator for MultiLineStringIterator<'l> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index >= self.length {
            return None;
        }

        let line_array_ref = self.geometry_column.value(self.length - 1);
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
                    float_array.values().as_ptr().cast::<Coordinate2D>(),
                    number_of_coordinates,
                )
            });
        }

        self.length -= 1; // decrement!

        Some(MultiLineStringRef::new_unchecked(line_coordinate_slices))
    }
}

impl<'l> ExactSizeIterator for MultiLineStringIterator<'l> {}

impl<'l> IntoParallelIterator for MultiLineStringIterator<'l> {
    type Item = MultiLineStringRef<'l>;
    type Iter = MultiLineStringParIterator<'l>;

    fn into_par_iter(self) -> Self::Iter {
        MultiLineStringParIterator(self)
    }
}

impl<'l> ParallelIterator for MultiLineStringParIterator<'l> {
    type Item = MultiLineStringRef<'l>;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: rayon::iter::plumbing::UnindexedConsumer<Self::Item>,
    {
        rayon::iter::plumbing::bridge(self, consumer)
    }
}

impl<'l> Producer for MultiLineStringParIterator<'l> {
    type Item = MultiLineStringRef<'l>;

    type IntoIter = MultiLineStringIterator<'l>;

    fn into_iter(self) -> Self::IntoIter {
        self.0
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        // Example:
        //   Index: 0, Length 3
        //   Split at 1
        //   Left: Index: 0, Length: 1 -> Elements: 0
        //   Right: Index: 1, Length: 3 -> Elements: 1, 2

        // The index is between self.0.index and self.0.length,
        // so we have to transform it to a global index
        let global_index = self.0.index + index;

        let left = Self(Self::IntoIter {
            geometry_column: self.0.geometry_column,
            index: self.0.index,
            length: global_index,
        });

        let right = Self(Self::IntoIter {
            geometry_column: self.0.geometry_column,
            index: global_index,
            length: self.0.length,
        });

        (left, right)
    }
}

impl<'l> IndexedParallelIterator for MultiLineStringParIterator<'l> {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn drive<C: rayon::iter::plumbing::Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        rayon::iter::plumbing::bridge(self, consumer)
    }

    fn with_producer<CB: rayon::iter::plumbing::ProducerCallback<Self::Item>>(
        self,
        callback: CB,
    ) -> CB::Output {
        callback.callback(self)
    }
}

impl<'l> GeometryRandomAccess<'l> for MultiLineStringCollection {
    type GeometryType = MultiLineStringRef<'l>;

    fn geometry_at(&'l self, index: usize) -> Option<Self::GeometryType> {
        let geometry_column: &ListArray = downcast_array(
            self.table
                .column_by_name(MultiLineStringCollection::GEOMETRY_COLUMN_NAME)
                .expect("Column must exist since it is in the metadata"),
        );

        if index >= self.len() {
            return None;
        }

        let line_array_ref = geometry_column.value(index);
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
                    float_array.values().as_ptr().cast::<Coordinate2D>(),
                    number_of_coordinates,
                )
            });
        }

        Some(MultiLineStringRef::new_unchecked(line_coordinate_slices))
    }
}

impl GeoFeatureCollectionRowBuilder<MultiLineString>
    for FeatureCollectionRowBuilder<MultiLineString>
{
    fn push_geometry(&mut self, geometry: MultiLineString) {
        let line_builder = self.geometries_builder.values();

        for line in geometry.lines() {
            let coordinate_builder = line_builder.values();

            for coordinate in line {
                coordinate_builder
                    .values()
                    .append_slice(coordinate.as_ref());

                coordinate_builder.append(true);
            }

            line_builder.append(true);
        }

        self.geometries_builder.append(true);
    }
}

impl ReplaceRawArrayCoords for MultiLineStringCollection {
    fn replace_raw_coords(array_ref: &Arc<dyn Array>, new_coords: Buffer) -> Result<ArrayData> {
        let geometries: &ListArray = downcast_array(array_ref);

        let num_features = geometries.len();
        let feature_offsets = geometries.offsets();

        let lines: &ListArray = downcast_array(geometries.values());
        let num_lines = lines.len();
        let line_offsets = lines.offsets();

        let num_coords = new_coords.len() / std::mem::size_of::<Coordinate2D>();
        let num_floats = num_coords * 2;

        Ok(ArrayData::builder(MultiLineString::arrow_data_type())
            .len(num_features)
            .add_buffer(feature_offsets.inner().inner().clone())
            .add_child_data(
                ArrayData::builder(Coordinate2D::arrow_list_data_type())
                    .len(num_lines)
                    .add_buffer(line_offsets.inner().inner().clone())
                    .add_child_data(
                        ArrayData::builder(Coordinate2D::arrow_data_type())
                            .len(num_coords)
                            .add_child_data(
                                ArrayData::builder(DataType::Float64)
                                    .len(num_floats)
                                    .add_buffer(new_coords)
                                    .build()?,
                            )
                            .build()?,
                    )
                    .build()?,
            )
            .build()?)
    }
}

#[cfg(test)]
mod tests {
    use float_cmp::approx_eq;

    use super::*;

    use crate::collections::{BuilderProvider, FeatureCollectionModifications};
    use crate::primitives::{FeatureData, FeatureDataRef, TimeInterval};

    #[test]
    fn single_line() {
        let mut builder = MultiLineStringCollection::builder().finish_header();

        builder.push_geometry(
            MultiLineString::new(vec![vec![(0.0, 0.1).into(), (1.0, 1.1).into()]]).unwrap(),
        );
        builder.push_geometry(
            MultiLineString::new(vec![vec![
                (4.0, 4.1).into(),
                (5.0, 5.1).into(),
                (6.0, 6.1).into(),
            ]])
            .unwrap(),
        );

        for _ in 0..2 {
            builder.push_time_interval(TimeInterval::default());

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

        builder.push_geometry(
            MultiLineString::new(vec![vec![(0.0, 0.1).into(), (1.0, 1.1).into()]]).unwrap(),
        );
        builder.push_geometry(
            MultiLineString::new(vec![
                vec![(4.0, 4.1).into(), (5.0, 5.1).into(), (6.0, 6.1).into()],
                vec![(7.0, 7.1).into(), (8.0, 8.1).into(), (9.0, 9.1).into()],
            ])
            .unwrap(),
        );

        for _ in 0..2 {
            builder.push_time_interval(TimeInterval::default());

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
    fn equals() {
        let mut builder = MultiLineStringCollection::builder().finish_header();

        builder.push_geometry(
            MultiLineString::new(vec![vec![(0.0, 0.1).into(), (1.0, 1.1).into()]]).unwrap(),
        );
        builder.push_geometry(
            MultiLineString::new(vec![
                vec![(4.0, 4.1).into(), (5.0, 5.1).into(), (6.0, 6.1).into()],
                vec![(7.0, 7.1).into(), (8.0, 8.1).into(), (9.0, 9.1).into()],
            ])
            .unwrap(),
        );

        for _ in 0..2 {
            builder.push_time_interval(TimeInterval::default());

            builder.finish_row();
        }

        let collection = builder.build().unwrap();

        assert_eq!(collection, collection);

        assert_ne!(collection, collection.filter(vec![true, false]).unwrap());
    }

    #[test]
    fn filter() {
        let mut builder = MultiLineStringCollection::builder().finish_header();

        builder.push_geometry(
            MultiLineString::new(vec![vec![(0.0, 0.1).into(), (1.0, 1.1).into()]]).unwrap(),
        );
        builder.push_geometry(
            MultiLineString::new(vec![
                vec![(4.0, 4.1).into(), (5.0, 5.1).into(), (6.0, 6.1).into()],
                vec![(7.0, 7.1).into(), (8.0, 8.1).into(), (9.0, 9.1).into()],
            ])
            .unwrap(),
        );
        builder.push_geometry(
            MultiLineString::new(vec![
                vec![(10.0, 10.1).into(), (11.0, 11.1).into()],
                vec![
                    (12.0, 12.1).into(),
                    (13.0, 13.1).into(),
                    (14.0, 14.1).into(),
                ],
            ])
            .unwrap(),
        );

        for _ in 0..3 {
            builder.push_time_interval(TimeInterval::default());

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

        builder.push_geometry(
            MultiLineString::new(vec![vec![(0.0, 0.1).into(), (1.0, 1.1).into()]]).unwrap(),
        );
        builder.push_time_interval(TimeInterval::default());
        builder.finish_row();

        let collection_a = builder.build().unwrap();

        let mut builder = MultiLineStringCollection::builder().finish_header();

        builder.push_geometry(
            MultiLineString::new(vec![
                vec![(4.0, 4.1).into(), (5.0, 5.1).into(), (6.0, 6.1).into()],
                vec![(7.0, 7.1).into(), (8.0, 8.1).into(), (9.0, 9.1).into()],
            ])
            .unwrap(),
        );
        builder.push_time_interval(TimeInterval::default());
        builder.finish_row();

        let collection_b = builder.build().unwrap();

        let collection_c = collection_a.append(&collection_b).unwrap();

        assert_eq!(collection_a.len(), 1);
        assert_eq!(collection_b.len(), 1);
        assert_eq!(collection_c.len(), 2);

        assert!(collection_a.is_simple());
        assert!(!collection_b.is_simple());
        assert!(!collection_c.is_simple());

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

    #[test]
    fn reproject_multi_lines_epsg4326_epsg900913_collection() {
        use crate::operations::reproject::{CoordinateProjection, CoordinateProjector, Reproject};
        use crate::primitives::FeatureData;
        use crate::spatial_reference::{SpatialReference, SpatialReferenceAuthority};

        use crate::util::well_known_data::{
            COLOGNE_EPSG_4326, COLOGNE_EPSG_900_913, HAMBURG_EPSG_4326, HAMBURG_EPSG_900_913,
            MARBURG_EPSG_4326, MARBURG_EPSG_900_913,
        };

        let from = SpatialReference::epsg_4326();
        let to = SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);
        let projector = CoordinateProjector::from_known_srs(from, to).unwrap();

        let collection = MultiLineStringCollection::from_slices(
            &[
                MultiLineString::new(vec![vec![MARBURG_EPSG_4326, HAMBURG_EPSG_4326]]).unwrap(),
                MultiLineString::new(vec![
                    vec![COLOGNE_EPSG_4326, MARBURG_EPSG_4326, HAMBURG_EPSG_4326],
                    vec![HAMBURG_EPSG_4326, COLOGNE_EPSG_4326],
                ])
                .unwrap(),
            ],
            &[TimeInterval::default(), TimeInterval::default()],
            &[("A", FeatureData::Int(vec![1, 2]))],
        )
        .unwrap();

        let expected = [
            MultiLineString::new(vec![vec![MARBURG_EPSG_900_913, HAMBURG_EPSG_900_913]]).unwrap(),
            MultiLineString::new(vec![
                vec![
                    COLOGNE_EPSG_900_913,
                    MARBURG_EPSG_900_913,
                    HAMBURG_EPSG_900_913,
                ],
                vec![HAMBURG_EPSG_900_913, COLOGNE_EPSG_900_913],
            ])
            .unwrap(),
        ];

        let proj_collection = collection.reproject(&projector).unwrap();

        // Assert geometrys are approx equal
        proj_collection
            .geometries()
            .zip(expected.iter())
            .for_each(|(a, e)| {
                assert!(approx_eq!(
                    &MultiLineString,
                    &a.into(),
                    e,
                    epsilon = 0.00001
                ));
            });

        // Assert that feature time intervals did not move around
        assert_eq!(proj_collection.time_intervals().len(), 2);
        assert_eq!(
            proj_collection.time_intervals(),
            &[TimeInterval::default(), TimeInterval::default()]
        );

        // Assert that feature data did not magicaly disappear
        if let FeatureDataRef::Int(numbers) = proj_collection.data("A").unwrap() {
            assert_eq!(numbers.as_ref(), &[1, 2]);
        } else {
            unreachable!();
        }
    }

    #[test]
    fn test_geo_iter() {
        use crate::util::well_known_data::{
            COLOGNE_EPSG_4326, HAMBURG_EPSG_4326, MARBURG_EPSG_4326,
        };

        let collection = MultiLineStringCollection::from_slices(
            &[
                MultiLineString::new(vec![vec![MARBURG_EPSG_4326, HAMBURG_EPSG_4326]]).unwrap(),
                MultiLineString::new(vec![
                    vec![COLOGNE_EPSG_4326, MARBURG_EPSG_4326, HAMBURG_EPSG_4326],
                    vec![HAMBURG_EPSG_4326, COLOGNE_EPSG_4326],
                ])
                .unwrap(),
            ],
            &[TimeInterval::default(), TimeInterval::default()],
            &[] as &[(&str, FeatureData)],
        )
        .unwrap();
        let mut iter = collection.geometries();

        assert_eq!(iter.len(), 2);

        let geometry = iter.next().unwrap();
        assert_eq!(
            MultiLineString::new(vec![vec![MARBURG_EPSG_4326, HAMBURG_EPSG_4326]]).unwrap(),
            geometry.into()
        );

        assert_eq!(iter.len(), 1);

        let geometry = iter.next().unwrap();
        assert_eq!(
            MultiLineString::new(vec![
                vec![COLOGNE_EPSG_4326, MARBURG_EPSG_4326, HAMBURG_EPSG_4326],
                vec![HAMBURG_EPSG_4326, COLOGNE_EPSG_4326],
            ])
            .unwrap(),
            geometry.into()
        );

        assert_eq!(iter.len(), 0);

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_geo_iter_reverse() {
        use crate::util::well_known_data::{
            COLOGNE_EPSG_4326, HAMBURG_EPSG_4326, MARBURG_EPSG_4326,
        };

        let collection = MultiLineStringCollection::from_slices(
            &[
                MultiLineString::new(vec![vec![MARBURG_EPSG_4326, HAMBURG_EPSG_4326]]).unwrap(),
                MultiLineString::new(vec![
                    vec![COLOGNE_EPSG_4326, MARBURG_EPSG_4326, HAMBURG_EPSG_4326],
                    vec![HAMBURG_EPSG_4326, COLOGNE_EPSG_4326],
                ])
                .unwrap(),
            ],
            &[TimeInterval::default(), TimeInterval::default()],
            &[] as &[(&str, FeatureData)],
        )
        .unwrap();
        let mut iter = collection.geometries();

        assert_eq!(iter.len(), 2);

        let geometry = iter.next_back().unwrap();
        assert_eq!(
            MultiLineString::new(vec![
                vec![COLOGNE_EPSG_4326, MARBURG_EPSG_4326, HAMBURG_EPSG_4326],
                vec![HAMBURG_EPSG_4326, COLOGNE_EPSG_4326],
            ])
            .unwrap(),
            geometry.into()
        );

        assert_eq!(iter.len(), 1);

        let geometry = iter.next_back().unwrap();
        assert_eq!(
            MultiLineString::new(vec![vec![MARBURG_EPSG_4326, HAMBURG_EPSG_4326]]).unwrap(),
            geometry.into()
        );

        assert_eq!(iter.len(), 0);

        assert!(iter.next_back().is_none());
    }

    #[test]
    fn test_par_iter() {
        use crate::util::well_known_data::{
            COLOGNE_EPSG_4326, HAMBURG_EPSG_4326, MARBURG_EPSG_4326,
        };

        let collection = MultiLineStringCollection::from_slices(
            &[
                MultiLineString::new(vec![vec![MARBURG_EPSG_4326, HAMBURG_EPSG_4326]]).unwrap(),
                MultiLineString::new(vec![
                    vec![COLOGNE_EPSG_4326, MARBURG_EPSG_4326, HAMBURG_EPSG_4326],
                    vec![HAMBURG_EPSG_4326, COLOGNE_EPSG_4326],
                ])
                .unwrap(),
            ],
            &[TimeInterval::default(), TimeInterval::default()],
            &[] as &[(&str, FeatureData)],
        )
        .unwrap();

        //  check splitting
        let iter = collection.geometries().into_par_iter();

        let (iter_left, iter_right) = iter.split_at(1);

        assert_eq!(iter_left.count(), 1);
        assert_eq!(iter_right.count(), 1);

        // new iter
        let iter = collection.geometries().into_par_iter();

        let result = iter
            .map(|geometry| geometry.lines().len())
            .collect::<Vec<_>>();

        assert_eq!(result, vec![1, 2]);
    }
}
