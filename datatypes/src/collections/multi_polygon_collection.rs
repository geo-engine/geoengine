use crate::collections::{
    BuilderProvider, FeatureCollection, FeatureCollectionInfos, FeatureCollectionIterator,
    FeatureCollectionRow, FeatureCollectionRowBuilder, GeoFeatureCollectionRowBuilder,
    GeometryCollection, GeometryRandomAccess, IntoGeometryIterator,
};
use crate::primitives::{Coordinate2D, MultiPolygon, MultiPolygonAccess, MultiPolygonRef};
use crate::util::Result;
use crate::{
    primitives::MultiLineString,
    util::arrow::{downcast_array, ArrowTyped},
};
use arrow::datatypes::ToByteSlice;
use arrow::{
    array::{Array, ArrayData, FixedSizeListArray, Float64Array, ListArray},
    buffer::Buffer,
    datatypes::DataType,
};
use rayon::iter::plumbing::Producer;
use rayon::prelude::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use std::{slice, sync::Arc};

use super::geo_feature_collection::ReplaceRawArrayCoords;

/// This collection contains temporal multi polygons and miscellaneous data.
pub type MultiPolygonCollection = FeatureCollection<MultiPolygon>;

impl GeometryCollection for MultiPolygonCollection {
    fn coordinates(&self) -> &[Coordinate2D] {
        let geometries_ref = self
            .table
            .column_by_name(Self::GEOMETRY_COLUMN_NAME)
            .expect("There should exist a geometry column because it is added during creation of the collection");
        let geometries: &ListArray = downcast_array(geometries_ref);

        let polygons_ref = geometries.values();
        let polygons: &ListArray = downcast_array(polygons_ref);

        let rings_ref = polygons.values();
        let rings: &ListArray = downcast_array(rings_ref);

        let coordinates_ref = rings.values();
        let coordinates: &FixedSizeListArray = downcast_array(coordinates_ref);

        let floats: &Float64Array = downcast_array(coordinates.values());
        let floats: &[f64] = floats.values().as_ref();

        unsafe { slice::from_raw_parts(floats.as_ptr().cast::<Coordinate2D>(), coordinates.len()) }
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

impl MultiPolygonCollection {
    #[allow(clippy::missing_panics_doc)]
    pub fn polygon_offsets(&self) -> &[i32] {
        let geometries_ref = self
            .table
            .column_by_name(Self::GEOMETRY_COLUMN_NAME)
            .expect("There should exist a geometry column because it is added during creation of the collection");
        let geometries: &ListArray = downcast_array(geometries_ref);

        let polygons_ref = geometries.values();
        let polygons: &ListArray = downcast_array(polygons_ref);

        polygons.offsets()
    }

    #[allow(clippy::missing_panics_doc)]
    pub fn ring_offsets(&self) -> &[i32] {
        let geometries_ref = self
            .table
            .column_by_name(Self::GEOMETRY_COLUMN_NAME)
            .expect("There should exist a geometry column because it is added during creation of the collection");
        let geometries: &ListArray = downcast_array(geometries_ref);

        let polygons_ref = geometries.values();
        let polygons: &ListArray = downcast_array(polygons_ref);

        let rings_ref = polygons.values();
        let rings: &ListArray = downcast_array(rings_ref);

        rings.offsets()
    }
}

impl<'l> IntoGeometryIterator<'l> for MultiPolygonCollection {
    type GeometryIterator = MultiPolygonIterator<'l>;
    type GeometryType = MultiPolygonRef<'l>;

    fn geometries(&'l self) -> Self::GeometryIterator {
        let geometry_column: &ListArray = downcast_array(
            self.table
                .column_by_name(Self::GEOMETRY_COLUMN_NAME)
                .expect("Column must exist since it is in the metadata"),
        );

        Self::GeometryIterator::new(geometry_column, self.len())
    }
}

impl<'a> IntoIterator for &'a MultiPolygonCollection {
    type Item = FeatureCollectionRow<'a, MultiPolygonRef<'a>>;
    type IntoIter = FeatureCollectionIterator<'a, MultiPolygonIterator<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        FeatureCollectionIterator::new::<MultiPolygon>(self, self.geometries())
    }
}

/// A collection iterator for [`MultiPolygon`]s.
pub struct MultiPolygonIterator<'l> {
    geometry_column: &'l ListArray,
    index: usize,
    length: usize,
}

/// A parallel collection iterator for [`MultiPolygon`]s
pub struct MultiPolygonParIterator<'l>(MultiPolygonIterator<'l>);

impl<'l> MultiPolygonIterator<'l> {
    pub fn new(geometry_column: &'l ListArray, length: usize) -> Self {
        Self {
            geometry_column,
            index: 0,
            length,
        }
    }
}

impl<'l> Iterator for MultiPolygonIterator<'l> {
    type Item = MultiPolygonRef<'l>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.length {
            return None;
        }

        let polygon_array_ref = self.geometry_column.value(self.index);
        let polygon_array: &ListArray = downcast_array(&polygon_array_ref);

        let number_of_polygons = polygon_array.len();
        let mut polygon_refs = Vec::with_capacity(number_of_polygons);

        for polygon_index in 0..number_of_polygons {
            let ring_array_ref = polygon_array.value(polygon_index);
            let ring_array: &ListArray = downcast_array(&ring_array_ref);

            let number_of_rings = ring_array.len();
            let mut ring_refs = Vec::with_capacity(number_of_rings);

            for ring_index in 0..number_of_rings {
                let coordinate_array_ref = ring_array.value(ring_index);
                let coordinate_array: &FixedSizeListArray = downcast_array(&coordinate_array_ref);

                let number_of_coordinates = coordinate_array.len();

                let float_array_ref = coordinate_array.value(0);
                let float_array: &Float64Array = downcast_array(&float_array_ref);

                ring_refs.push(unsafe {
                    #[allow(clippy::cast_ptr_alignment)]
                    slice::from_raw_parts(
                        float_array.values().as_ptr().cast::<Coordinate2D>(),
                        number_of_coordinates,
                    )
                });
            }

            polygon_refs.push(ring_refs);
        }

        self.index += 1; // increment!

        Some(MultiPolygonRef::new_unchecked(polygon_refs))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.length - self.index;
        (remaining, Some(remaining))
    }

    fn count(self) -> usize {
        self.length - self.index
    }
}

impl<'l> DoubleEndedIterator for MultiPolygonIterator<'l> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index >= self.length {
            return None;
        }

        let polygon_array_ref = self.geometry_column.value(self.length - 1);
        let polygon_array: &ListArray = downcast_array(&polygon_array_ref);

        let number_of_polygons = polygon_array.len();
        let mut polygon_refs = Vec::with_capacity(number_of_polygons);

        for polygon_index in 0..number_of_polygons {
            let ring_array_ref = polygon_array.value(polygon_index);
            let ring_array: &ListArray = downcast_array(&ring_array_ref);

            let number_of_rings = ring_array.len();
            let mut ring_refs = Vec::with_capacity(number_of_rings);

            for ring_index in 0..number_of_rings {
                let coordinate_array_ref = ring_array.value(ring_index);
                let coordinate_array: &FixedSizeListArray = downcast_array(&coordinate_array_ref);

                let number_of_coordinates = coordinate_array.len();

                let float_array_ref = coordinate_array.value(0);
                let float_array: &Float64Array = downcast_array(&float_array_ref);

                ring_refs.push(unsafe {
                    #[allow(clippy::cast_ptr_alignment)]
                    slice::from_raw_parts(
                        float_array.values().as_ptr().cast::<Coordinate2D>(),
                        number_of_coordinates,
                    )
                });
            }

            polygon_refs.push(ring_refs);
        }

        self.length -= 1; // decrement!

        Some(MultiPolygonRef::new_unchecked(polygon_refs))
    }
}

impl<'l> ExactSizeIterator for MultiPolygonIterator<'l> {}

impl<'l> IntoParallelIterator for MultiPolygonIterator<'l> {
    type Item = MultiPolygonRef<'l>;
    type Iter = MultiPolygonParIterator<'l>;

    fn into_par_iter(self) -> Self::Iter {
        MultiPolygonParIterator(self)
    }
}

impl<'l> ParallelIterator for MultiPolygonParIterator<'l> {
    type Item = MultiPolygonRef<'l>;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: rayon::iter::plumbing::UnindexedConsumer<Self::Item>,
    {
        rayon::iter::plumbing::bridge(self, consumer)
    }
}

impl<'l> Producer for MultiPolygonParIterator<'l> {
    type Item = MultiPolygonRef<'l>;

    type IntoIter = MultiPolygonIterator<'l>;

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

impl<'l> IndexedParallelIterator for MultiPolygonParIterator<'l> {
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

impl<'l> GeometryRandomAccess<'l> for MultiPolygonCollection {
    type GeometryType = MultiPolygonRef<'l>;

    fn geometry_at(&'l self, index: usize) -> Option<Self::GeometryType> {
        let geometry_column: &ListArray = downcast_array(
            self.table
                .column_by_name(MultiPolygonCollection::GEOMETRY_COLUMN_NAME)
                .expect("Column must exist since it is in the metadata"),
        );

        if index >= self.len() {
            return None;
        }

        let polygon_array_ref = geometry_column.value(index);
        let polygon_array: &ListArray = downcast_array(&polygon_array_ref);

        let number_of_polygons = polygon_array.len();
        let mut polygon_refs = Vec::with_capacity(number_of_polygons);

        for polygon_index in 0..number_of_polygons {
            let ring_array_ref = polygon_array.value(polygon_index);
            let ring_array: &ListArray = downcast_array(&ring_array_ref);

            let number_of_rings = ring_array.len();
            let mut ring_refs = Vec::with_capacity(number_of_rings);

            for ring_index in 0..number_of_rings {
                let coordinate_array_ref = ring_array.value(ring_index);
                let coordinate_array: &FixedSizeListArray = downcast_array(&coordinate_array_ref);

                let number_of_coordinates = coordinate_array.len();

                let float_array_ref = coordinate_array.value(0);
                let float_array: &Float64Array = downcast_array(&float_array_ref);

                ring_refs.push(unsafe {
                    #[allow(clippy::cast_ptr_alignment)]
                    slice::from_raw_parts(
                        float_array.values().as_ptr().cast::<Coordinate2D>(),
                        number_of_coordinates,
                    )
                });
            }

            polygon_refs.push(ring_refs);
        }

        Some(MultiPolygonRef::new_unchecked(polygon_refs))
    }
}

impl GeoFeatureCollectionRowBuilder<MultiPolygon> for FeatureCollectionRowBuilder<MultiPolygon> {
    fn push_geometry(&mut self, geometry: MultiPolygon) {
        let polygon_builder = self.geometries_builder.values();

        for polygon in geometry.polygons() {
            let ring_builder = polygon_builder.values();

            for ring in polygon {
                let coordinate_builder = ring_builder.values();

                for coordinate in ring {
                    coordinate_builder
                        .values()
                        .append_slice(coordinate.as_ref());

                    coordinate_builder.append(true);
                }

                ring_builder.append(true);
            }

            polygon_builder.append(true);
        }

        self.geometries_builder.append(true);
    }
}

impl ReplaceRawArrayCoords for MultiPolygonCollection {
    fn replace_raw_coords(array_ref: &Arc<dyn Array>, new_coords: Buffer) -> Result<ArrayData> {
        let geometries: &ListArray = downcast_array(array_ref);

        let num_features = geometries.len();
        let feature_offsets = geometries.offsets();

        let polygons: &ListArray = downcast_array(geometries.values());
        let num_polygons = polygons.len();
        let polygon_offsets = polygons.offsets();

        let rings: &ListArray = downcast_array(polygons.values());
        let num_rings = rings.len();
        let ring_offsets = rings.offsets();

        let num_coords = new_coords.len() / std::mem::size_of::<Coordinate2D>();
        let num_floats = num_coords * 2;

        Ok(ArrayData::builder(MultiPolygon::arrow_data_type())
            .len(num_features)
            .add_buffer(feature_offsets.inner().inner().clone())
            .add_child_data(
                ArrayData::builder(MultiLineString::arrow_data_type())
                    .len(num_polygons)
                    .add_buffer(polygon_offsets.inner().inner().clone())
                    .add_child_data(
                        ArrayData::builder(Coordinate2D::arrow_list_data_type())
                            .len(num_rings)
                            .add_buffer(ring_offsets.inner().inner().clone())
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
                    .build()?,
            )
            .build()?)
    }
}

impl From<Vec<geo::MultiPolygon<f64>>> for MultiPolygonCollection {
    fn from(geo_polygons: Vec<geo::MultiPolygon<f64>>) -> Self {
        let mut features = vec![];
        let mut polygons = vec![];
        let mut rings = vec![];
        let mut coordinates = vec![];

        for multi_polygon in geo_polygons {
            features.push(polygons.len() as i32);
            for polygon in multi_polygon {
                polygons.push(rings.len() as i32);
                rings.push(coordinates.len() as i32);

                let mut outer_coords: Vec<Coordinate2D> =
                    polygon.exterior().points().map(From::from).collect();

                coordinates.append(&mut outer_coords);

                for inner_ring in polygon.interiors() {
                    rings.push(coordinates.len() as i32);
                    let mut inner_coords: Vec<Coordinate2D> =
                        inner_ring.points().map(From::from).collect();

                    coordinates.append(&mut inner_coords);
                }
            }
        }

        features.push(polygons.len() as i32);
        polygons.push(rings.len() as i32);
        rings.push(coordinates.len() as i32);

        let mut builder = FeatureCollection::<MultiPolygon>::builder()
            .batch_builder(features.len() - 1, coordinates.len() - 1);

        let feature_offsets = Buffer::from(features.to_byte_slice());
        let polygon_offsets = Buffer::from(polygons.to_byte_slice());
        let ring_offsets = Buffer::from(rings.to_byte_slice());
        let coordinates = unsafe {
            let coord_bytes: &[u8] = std::slice::from_raw_parts(
                coordinates.as_ptr().cast::<u8>(),
                coordinates.len() * std::mem::size_of::<Coordinate2D>(),
            );
            Buffer::from(coord_bytes)
        };

        builder
            .set_polygons(coordinates, ring_offsets, polygon_offsets, feature_offsets)
            .expect("setting polygons always works");

        builder.set_default_time_intervals();

        builder
            .finish()
            .expect("geo polygons are valid and default time is set");

        builder
            .output
            .expect("builder is finished")
            .try_into_polygons()
            .expect("builder builds polygons")
    }
}

#[cfg(test)]
mod tests {
    use float_cmp::approx_eq;
    use geo::polygon;

    use super::*;

    use crate::collections::feature_collection::ChunksEqualIgnoringCacheHint;
    use crate::collections::{BuilderProvider, FeatureCollectionModifications};
    use crate::primitives::CacheHint;
    use crate::primitives::{FeatureData, FeatureDataRef, TimeInterval};

    #[test]
    fn single_polygons() {
        let mut builder = MultiPolygonCollection::builder().finish_header();

        builder.push_geometry(
            MultiPolygon::new(vec![vec![vec![
                (0.0, 0.1).into(),
                (0.0, 1.1).into(),
                (1.0, 0.1).into(),
                (0.0, 0.1).into(),
            ]]])
            .unwrap(),
        );
        builder.push_geometry(
            MultiPolygon::new(vec![vec![vec![
                (2.0, 2.1).into(),
                (2.0, 3.1).into(),
                (3.0, 3.1).into(),
                (3.0, 2.1).into(),
                (2.0, 2.1).into(),
            ]]])
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
            geometry_iter.next().unwrap().polygons(),
            &[&[&[
                (0.0, 0.1).into(),
                (0.0, 1.1).into(),
                (1.0, 0.1).into(),
                (0.0, 0.1).into(),
            ]]]
        );
        assert_eq!(
            geometry_iter.next().unwrap().polygons(),
            &[&[&[
                (2.0, 2.1).into(),
                (2.0, 3.1).into(),
                (3.0, 3.1).into(),
                (3.0, 2.1).into(),
                (2.0, 2.1).into(),
            ]]]
        );
        assert!(geometry_iter.next().is_none());
    }

    #[test]
    fn multi_polygons() {
        let mut builder = MultiPolygonCollection::builder().finish_header();

        builder.push_geometry(
            MultiPolygon::new(vec![
                vec![vec![
                    (0.0, 0.1).into(),
                    (0.0, 1.1).into(),
                    (1.0, 0.1).into(),
                    (0.0, 0.1).into(),
                ]],
                vec![vec![
                    (2.0, 2.1).into(),
                    (2.0, 3.1).into(),
                    (3.0, 2.1).into(),
                    (2.0, 2.1).into(),
                ]],
            ])
            .unwrap(),
        );
        builder.push_geometry(
            MultiPolygon::new(vec![vec![
                vec![
                    (4.0, 4.1).into(),
                    (4.0, 8.1).into(),
                    (8.0, 8.1).into(),
                    (8.0, 4.1).into(),
                    (4.0, 4.1).into(),
                ],
                vec![
                    (5.0, 5.1).into(),
                    (5.0, 7.1).into(),
                    (7.0, 7.1).into(),
                    (7.0, 5.1).into(),
                    (5.0, 5.1).into(),
                ],
            ]])
            .unwrap(),
        );

        for _ in 0..2 {
            builder.push_time_interval(TimeInterval::default());

            builder.finish_row();
        }

        let collection = builder.build().unwrap();

        assert_eq!(collection.len(), 2);
        assert!(!collection.is_simple());

        assert_eq!(
            collection.time_intervals(),
            &[TimeInterval::default(), TimeInterval::default()]
        );

        assert!(!collection.is_simple());

        let mut geometry_iter = collection.geometries();
        assert_eq!(
            geometry_iter.next().unwrap().polygons(),
            &[
                &[&[
                    (0.0, 0.1).into(),
                    (0.0, 1.1).into(),
                    (1.0, 0.1).into(),
                    (0.0, 0.1).into(),
                ]],
                &[&[
                    (2.0, 2.1).into(),
                    (2.0, 3.1).into(),
                    (3.0, 2.1).into(),
                    (2.0, 2.1).into(),
                ]],
            ]
        );
        assert_eq!(
            geometry_iter.next().unwrap().polygons(),
            &[&[
                &[
                    (4.0, 4.1).into(),
                    (4.0, 8.1).into(),
                    (8.0, 8.1).into(),
                    (8.0, 4.1).into(),
                    (4.0, 4.1).into(),
                ],
                &[
                    (5.0, 5.1).into(),
                    (5.0, 7.1).into(),
                    (7.0, 7.1).into(),
                    (7.0, 5.1).into(),
                    (5.0, 5.1).into(),
                ],
            ]]
        );
        assert!(geometry_iter.next().is_none());
    }

    #[test]
    fn equals() {
        let mut builder = MultiPolygonCollection::builder().finish_header();

        builder.push_geometry(
            MultiPolygon::new(vec![
                vec![vec![
                    (0.0, 0.1).into(),
                    (0.0, 1.1).into(),
                    (1.0, 0.1).into(),
                    (0.0, 0.1).into(),
                ]],
                vec![vec![
                    (2.0, 2.1).into(),
                    (2.0, 3.1).into(),
                    (3.0, 2.1).into(),
                    (2.0, 2.1).into(),
                ]],
            ])
            .unwrap(),
        );
        builder.push_geometry(
            MultiPolygon::new(vec![vec![
                vec![
                    (4.0, 4.1).into(),
                    (4.0, 8.1).into(),
                    (8.0, 8.1).into(),
                    (8.0, 4.1).into(),
                    (4.0, 4.1).into(),
                ],
                vec![
                    (5.0, 5.1).into(),
                    (5.0, 7.1).into(),
                    (7.0, 7.1).into(),
                    (7.0, 5.1).into(),
                    (5.0, 5.1).into(),
                ],
            ]])
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
        let mut builder = MultiPolygonCollection::builder().finish_header();

        builder.push_geometry(
            MultiPolygon::new(vec![vec![vec![
                (0.0, 0.1).into(),
                (0.0, 1.1).into(),
                (1.0, 0.1).into(),
                (0.0, 0.1).into(),
            ]]])
            .unwrap(),
        );
        builder.push_geometry(
            MultiPolygon::new(vec![
                vec![vec![
                    (0.0, 0.1).into(),
                    (0.0, 1.1).into(),
                    (1.0, 0.1).into(),
                    (0.0, 0.1).into(),
                ]],
                vec![vec![
                    (2.0, 2.1).into(),
                    (2.0, 3.1).into(),
                    (3.0, 2.1).into(),
                    (2.0, 2.1).into(),
                ]],
            ])
            .unwrap(),
        );
        builder.push_geometry(
            MultiPolygon::new(vec![vec![
                vec![
                    (4.0, 4.1).into(),
                    (4.0, 8.1).into(),
                    (8.0, 8.1).into(),
                    (8.0, 4.1).into(),
                    (4.0, 4.1).into(),
                ],
                vec![
                    (5.0, 5.1).into(),
                    (5.0, 7.1).into(),
                    (7.0, 7.1).into(),
                    (7.0, 5.1).into(),
                    (5.0, 5.1).into(),
                ],
            ]])
            .unwrap(),
        );

        for _ in 0..3 {
            builder.push_time_interval(TimeInterval::default());

            builder.finish_row();
        }

        let collection = builder.build().unwrap();

        assert_eq!(collection.len(), 3);

        let collection = collection.filter(vec![false, false, true]).unwrap();

        assert_eq!(collection.len(), 1);

        let mut geometry_iter = collection.geometries();
        assert_eq!(
            geometry_iter.next().unwrap().polygons(),
            &[&[
                &[
                    (4.0, 4.1).into(),
                    (4.0, 8.1).into(),
                    (8.0, 8.1).into(),
                    (8.0, 4.1).into(),
                    (4.0, 4.1).into(),
                ],
                &[
                    (5.0, 5.1).into(),
                    (5.0, 7.1).into(),
                    (7.0, 7.1).into(),
                    (7.0, 5.1).into(),
                    (5.0, 5.1).into(),
                ],
            ]]
        );
        assert!(geometry_iter.next().is_none());
    }

    #[test]
    fn append() {
        let mut builder = MultiPolygonCollection::builder().finish_header();

        builder.push_geometry(
            MultiPolygon::new(vec![vec![vec![
                (0.0, 0.1).into(),
                (0.0, 1.1).into(),
                (1.0, 0.1).into(),
                (0.0, 0.1).into(),
            ]]])
            .unwrap(),
        );
        builder.push_time_interval(TimeInterval::default());
        builder.finish_row();

        let collection_a = builder.build().unwrap();

        let mut builder = MultiPolygonCollection::builder().finish_header();

        builder.push_geometry(
            MultiPolygon::new(vec![vec![
                vec![
                    (4.0, 4.1).into(),
                    (4.0, 8.1).into(),
                    (8.0, 8.1).into(),
                    (8.0, 4.1).into(),
                    (4.0, 4.1).into(),
                ],
                vec![
                    (5.0, 5.1).into(),
                    (5.0, 7.1).into(),
                    (7.0, 7.1).into(),
                    (7.0, 5.1).into(),
                    (5.0, 5.1).into(),
                ],
            ]])
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
        assert!(collection_b.is_simple());
        assert!(collection_c.is_simple());

        let mut geometry_iter = collection_c.geometries();
        assert_eq!(
            geometry_iter.next().unwrap().polygons(),
            &[&[&[
                (0.0, 0.1).into(),
                (0.0, 1.1).into(),
                (1.0, 0.1).into(),
                (0.0, 0.1).into(),
            ]]]
        );
        assert_eq!(
            geometry_iter.next().unwrap().polygons(),
            &[&[
                &[
                    (4.0, 4.1).into(),
                    (4.0, 8.1).into(),
                    (8.0, 8.1).into(),
                    (8.0, 4.1).into(),
                    (4.0, 4.1).into(),
                ],
                &[
                    (5.0, 5.1).into(),
                    (5.0, 7.1).into(),
                    (7.0, 7.1).into(),
                    (7.0, 5.1).into(),
                    (5.0, 5.1).into(),
                ],
            ]]
        );
        assert!(geometry_iter.next().is_none());
    }

    #[test]
    fn reproject_multi_polygons_epsg4326_epsg900913_collection() {
        use crate::operations::reproject::Reproject;
        use crate::operations::reproject::{CoordinateProjection, CoordinateProjector};
        use crate::primitives::FeatureData;
        use crate::spatial_reference::{SpatialReference, SpatialReferenceAuthority};

        use crate::util::well_known_data::{
            COLOGNE_EPSG_4326, COLOGNE_EPSG_900_913, HAMBURG_EPSG_4326, HAMBURG_EPSG_900_913,
            MARBURG_EPSG_4326, MARBURG_EPSG_900_913,
        };

        let from = SpatialReference::epsg_4326();
        let to = SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);
        let projector = CoordinateProjector::from_known_srs(from, to).unwrap();

        let collection = MultiPolygonCollection::from_slices(
            &[
                MultiPolygon::new(vec![
                    vec![vec![
                        HAMBURG_EPSG_4326,
                        MARBURG_EPSG_4326,
                        COLOGNE_EPSG_4326,
                        HAMBURG_EPSG_4326,
                    ]],
                    vec![vec![
                        COLOGNE_EPSG_4326,
                        HAMBURG_EPSG_4326,
                        MARBURG_EPSG_4326,
                        COLOGNE_EPSG_4326,
                    ]],
                ])
                .unwrap(),
                MultiPolygon::new(vec![vec![vec![
                    MARBURG_EPSG_4326,
                    COLOGNE_EPSG_4326,
                    HAMBURG_EPSG_4326,
                    MARBURG_EPSG_4326,
                ]]])
                .unwrap(),
            ],
            &[TimeInterval::default(), TimeInterval::default()],
            &[("A", FeatureData::Int(vec![1, 2]))],
        )
        .unwrap();

        let expected = [
            MultiPolygon::new(vec![
                vec![vec![
                    HAMBURG_EPSG_900_913,
                    MARBURG_EPSG_900_913,
                    COLOGNE_EPSG_900_913,
                    HAMBURG_EPSG_900_913,
                ]],
                vec![vec![
                    COLOGNE_EPSG_900_913,
                    HAMBURG_EPSG_900_913,
                    MARBURG_EPSG_900_913,
                    COLOGNE_EPSG_900_913,
                ]],
            ])
            .unwrap(),
            MultiPolygon::new(vec![vec![vec![
                MARBURG_EPSG_900_913,
                COLOGNE_EPSG_900_913,
                HAMBURG_EPSG_900_913,
                MARBURG_EPSG_900_913,
            ]]])
            .unwrap(),
        ];

        assert_eq!(collection.geometries().len(), 2);

        let proj_collection = collection.reproject(&projector).unwrap();

        // Assert geometrys are approx equal
        proj_collection
            .geometries()
            .zip(expected.iter())
            .for_each(|(a, e)| {
                assert!(approx_eq!(&MultiPolygon, &a.into(), e, epsilon = 0.00001));
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
    fn polygons_from_geo() {
        let poly1 = polygon!(
            exterior: [
                (x: -111., y: 45.),
                (x: -111., y: 41.),
                (x: -104., y: 41.),
                (x: -104., y: 45.),
            ],
            interiors: [
                [
                    (x: -110., y: 44.),
                    (x: -110., y: 42.),
                    (x: -105., y: 42.),
                    (x: -105., y: 44.),
                ],
            ],
        );

        let poly2 = polygon!(
            exterior: [
                (x: -111., y: 45.),
                (x: -111., y: 41.),
                (x: -104., y: 41.),
                (x: -104., y: 45.),
            ],
            interiors: [
                [
                    (x: -110., y: 44.),
                    (x: -110., y: 42.),
                    (x: -105., y: 42.),
                    (x: -105., y: 44.),
                ],
            ],
        );

        let multi1 = geo::MultiPolygon(vec![poly1, poly2]);

        let geometries = vec![multi1];

        let from_geo = MultiPolygonCollection::from(geometries);

        let collection = MultiPolygonCollection::from_data(
            vec![MultiPolygon::new(vec![
                vec![
                    vec![
                        Coordinate2D::new(-111., 45.),
                        Coordinate2D::new(-111., 41.),
                        Coordinate2D::new(-104., 41.),
                        Coordinate2D::new(-104., 45.),
                        Coordinate2D::new(-111., 45.),
                    ],
                    vec![
                        Coordinate2D::new(-110., 44.),
                        Coordinate2D::new(-110., 42.),
                        Coordinate2D::new(-105., 42.),
                        Coordinate2D::new(-105., 44.),
                        Coordinate2D::new(-110., 44.),
                    ],
                ],
                vec![
                    vec![
                        Coordinate2D::new(-111., 45.),
                        Coordinate2D::new(-111., 41.),
                        Coordinate2D::new(-104., 41.),
                        Coordinate2D::new(-104., 45.),
                        Coordinate2D::new(-111., 45.),
                    ],
                    vec![
                        Coordinate2D::new(-110., 44.),
                        Coordinate2D::new(-110., 42.),
                        Coordinate2D::new(-105., 42.),
                        Coordinate2D::new(-105., 44.),
                        Coordinate2D::new(-110., 44.),
                    ],
                ],
            ])
            .unwrap()],
            vec![Default::default(); 1],
            Default::default(),
            CacheHint::default(),
        )
        .unwrap();

        assert!(collection.chunks_equal_ignoring_cache_hint(&from_geo));
    }

    #[test]
    fn test_geo_iter() {
        use crate::util::well_known_data::{
            COLOGNE_EPSG_4326, HAMBURG_EPSG_4326, MARBURG_EPSG_4326,
        };

        let collection = MultiPolygonCollection::from_slices(
            &[
                MultiPolygon::new(vec![
                    vec![vec![
                        HAMBURG_EPSG_4326,
                        MARBURG_EPSG_4326,
                        COLOGNE_EPSG_4326,
                        HAMBURG_EPSG_4326,
                    ]],
                    vec![vec![
                        COLOGNE_EPSG_4326,
                        HAMBURG_EPSG_4326,
                        MARBURG_EPSG_4326,
                        COLOGNE_EPSG_4326,
                    ]],
                ])
                .unwrap(),
                MultiPolygon::new(vec![vec![vec![
                    MARBURG_EPSG_4326,
                    COLOGNE_EPSG_4326,
                    HAMBURG_EPSG_4326,
                    MARBURG_EPSG_4326,
                ]]])
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
            MultiPolygon::new(vec![
                vec![vec![
                    HAMBURG_EPSG_4326,
                    MARBURG_EPSG_4326,
                    COLOGNE_EPSG_4326,
                    HAMBURG_EPSG_4326,
                ]],
                vec![vec![
                    COLOGNE_EPSG_4326,
                    HAMBURG_EPSG_4326,
                    MARBURG_EPSG_4326,
                    COLOGNE_EPSG_4326,
                ]],
            ])
            .unwrap(),
            geometry.into()
        );

        assert_eq!(iter.len(), 1);

        let geometry = iter.next().unwrap();
        assert_eq!(
            MultiPolygon::new(vec![vec![vec![
                MARBURG_EPSG_4326,
                COLOGNE_EPSG_4326,
                HAMBURG_EPSG_4326,
                MARBURG_EPSG_4326,
            ]]])
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

        let collection = MultiPolygonCollection::from_slices(
            &[
                MultiPolygon::new(vec![
                    vec![vec![
                        HAMBURG_EPSG_4326,
                        MARBURG_EPSG_4326,
                        COLOGNE_EPSG_4326,
                        HAMBURG_EPSG_4326,
                    ]],
                    vec![vec![
                        COLOGNE_EPSG_4326,
                        HAMBURG_EPSG_4326,
                        MARBURG_EPSG_4326,
                        COLOGNE_EPSG_4326,
                    ]],
                ])
                .unwrap(),
                MultiPolygon::new(vec![vec![vec![
                    MARBURG_EPSG_4326,
                    COLOGNE_EPSG_4326,
                    HAMBURG_EPSG_4326,
                    MARBURG_EPSG_4326,
                ]]])
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
            MultiPolygon::new(vec![vec![vec![
                MARBURG_EPSG_4326,
                COLOGNE_EPSG_4326,
                HAMBURG_EPSG_4326,
                MARBURG_EPSG_4326,
            ]]])
            .unwrap(),
            geometry.into()
        );

        assert_eq!(iter.len(), 1);

        let geometry = iter.next_back().unwrap();

        assert_eq!(
            MultiPolygon::new(vec![
                vec![vec![
                    HAMBURG_EPSG_4326,
                    MARBURG_EPSG_4326,
                    COLOGNE_EPSG_4326,
                    HAMBURG_EPSG_4326,
                ]],
                vec![vec![
                    COLOGNE_EPSG_4326,
                    HAMBURG_EPSG_4326,
                    MARBURG_EPSG_4326,
                    COLOGNE_EPSG_4326,
                ]],
            ])
            .unwrap(),
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

        let collection = MultiPolygonCollection::from_slices(
            &[
                MultiPolygon::new(vec![
                    vec![vec![
                        HAMBURG_EPSG_4326,
                        MARBURG_EPSG_4326,
                        COLOGNE_EPSG_4326,
                        HAMBURG_EPSG_4326,
                    ]],
                    vec![vec![
                        COLOGNE_EPSG_4326,
                        HAMBURG_EPSG_4326,
                        MARBURG_EPSG_4326,
                        COLOGNE_EPSG_4326,
                    ]],
                ])
                .unwrap(),
                MultiPolygon::new(vec![vec![vec![
                    MARBURG_EPSG_4326,
                    COLOGNE_EPSG_4326,
                    HAMBURG_EPSG_4326,
                    MARBURG_EPSG_4326,
                ]]])
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
            .map(|geometry| geometry.polygons().len())
            .collect::<Vec<_>>();

        assert_eq!(result, vec![2, 1]);
    }
}
