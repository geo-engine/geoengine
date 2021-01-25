use crate::collections::{
    FeatureCollection, FeatureCollectionInfos, FeatureCollectionRowBuilder,
    GeoFeatureCollectionRowBuilder, GeometryCollection, GeometryRandomAccess, IntoGeometryIterator,
};
use crate::primitives::{Coordinate2D, MultiPolygon, MultiPolygonAccess, MultiPolygonRef};
use crate::util::arrow::downcast_array;
use crate::util::Result;
use arrow::array::{Array, FixedSizeListArray, Float64Array, ListArray};
use std::slice;

/// This collection contains temporal multi polygons and miscellaneous data.
pub type MultiPolygonCollection = FeatureCollection<MultiPolygon>;

impl GeometryCollection for MultiPolygonCollection {
    fn coordinates(&self) -> &[Coordinate2D] {
        let geometries_ref = self
            .table
            .column_by_name(Self::GEOMETRY_COLUMN_NAME)
            .expect("There must exist a geometry column");
        let geometries: &ListArray = downcast_array(geometries_ref);

        let polygons_ref = geometries.values();
        let polygons: &ListArray = downcast_array(&polygons_ref);

        let rings_ref = polygons.values();
        let rings: &ListArray = downcast_array(&rings_ref);

        let coordinates_ref = rings.values();
        let coordinates: &FixedSizeListArray = downcast_array(&coordinates_ref);

        let number_of_coordinates = coordinates.data().len();

        let floats_ref = coordinates.values();
        let floats: &Float64Array = downcast_array(&floats_ref);

        unsafe {
            slice::from_raw_parts(
                floats.raw_values().cast::<Coordinate2D>(),
                number_of_coordinates,
            )
        }
    }

    #[allow(clippy::cast_ptr_alignment)]
    fn feature_offsets(&self) -> &[i32] {
        let geometries_ref = self
            .table
            .column_by_name(Self::GEOMETRY_COLUMN_NAME)
            .expect("There must exist a geometry column");
        let geometries: &ListArray = downcast_array(geometries_ref);

        let data = geometries.data();
        let buffer = &data.buffers()[0];

        unsafe { slice::from_raw_parts(buffer.raw_data().cast::<i32>(), geometries.len() + 1) }
    }
}

impl MultiPolygonCollection {
    #[allow(clippy::cast_ptr_alignment)]
    pub fn polygon_offsets(&self) -> &[i32] {
        let geometries_ref = self
            .table
            .column_by_name(Self::GEOMETRY_COLUMN_NAME)
            .expect("There must exist a geometry column");
        let geometries: &ListArray = downcast_array(geometries_ref);

        let polygons_ref = geometries.values();
        let polygons: &ListArray = downcast_array(&polygons_ref);

        let data = polygons.data();
        let buffer = &data.buffers()[0];

        unsafe { slice::from_raw_parts(buffer.raw_data().cast::<i32>(), polygons.len() + 1) }
    }

    #[allow(clippy::cast_ptr_alignment)]
    pub fn ring_offsets(&self) -> &[i32] {
        let geometries_ref = self
            .table
            .column_by_name(Self::GEOMETRY_COLUMN_NAME)
            .expect("There must exist a geometry column");
        let geometries: &ListArray = downcast_array(geometries_ref);

        let polygons_ref = geometries.values();
        let polygons: &ListArray = downcast_array(&polygons_ref);

        let rings_ref = polygons.values();
        let rings: &ListArray = downcast_array(&rings_ref);

        let data = rings.data();
        let buffer = &data.buffers()[0];

        unsafe { slice::from_raw_parts(buffer.raw_data().cast::<i32>(), rings.len() + 1) }
    }
}

impl<'l> IntoGeometryIterator<'l> for MultiPolygonCollection {
    type GeometryIterator = MultiPolygonIterator<'l>;
    type GeometryType = MultiPolygonRef<'l>;

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

/// A collection iterator for multi points
pub struct MultiPolygonIterator<'l> {
    geometry_column: &'l ListArray,
    index: usize,
    length: usize,
}

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
                        float_array.raw_values().cast::<Coordinate2D>(),
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

impl<'l> GeometryRandomAccess<'l> for MultiPolygonCollection {
    type GeometryType = MultiPolygonRef<'l>;

    fn geometry_at(&'l self, index: usize) -> Option<Self::GeometryType> {
        let geometry_column: &ListArray = downcast_array(
            &self
                .table
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
                        float_array.raw_values().cast::<Coordinate2D>(),
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
    fn push_geometry(&mut self, geometry: MultiPolygon) -> Result<()> {
        let polygon_builder = self.geometries_builder.values();

        for polygon in geometry.polygons() {
            let ring_builder = polygon_builder.values();

            for ring in polygon {
                let coordinate_builder = ring_builder.values();

                for coordinate in ring {
                    coordinate_builder
                        .values()
                        .append_slice(coordinate.as_ref())?;

                    coordinate_builder.append(true)?;
                }

                ring_builder.append(true)?;
            }

            polygon_builder.append(true)?;
        }

        self.geometries_builder.append(true)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::collections::{BuilderProvider, FeatureCollectionModifications};
    use crate::primitives::TimeInterval;

    #[test]
    fn single_polygons() {
        let mut builder = MultiPolygonCollection::builder().finish_header();

        builder
            .push_geometry(
                MultiPolygon::new(vec![vec![vec![
                    (0.0, 0.1).into(),
                    (0.0, 1.1).into(),
                    (1.0, 0.1).into(),
                    (0.0, 0.1).into(),
                ]]])
                .unwrap(),
            )
            .unwrap();
        builder
            .push_geometry(
                MultiPolygon::new(vec![vec![vec![
                    (2.0, 2.1).into(),
                    (2.0, 3.1).into(),
                    (3.0, 3.1).into(),
                    (3.0, 2.1).into(),
                    (2.0, 2.1).into(),
                ]]])
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

        builder
            .push_geometry(
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
            )
            .unwrap();
        builder
            .push_geometry(
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
            )
            .unwrap();

        for _ in 0..2 {
            builder.push_time_interval(TimeInterval::default()).unwrap();

            builder.finish_row();
        }

        let collection = builder.build().unwrap();

        assert_eq!(collection.len(), 2);
        assert_eq!(collection.is_simple(), false);

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
    #[allow(clippy::eq_op)]
    fn equals() {
        let mut builder = MultiPolygonCollection::builder().finish_header();

        builder
            .push_geometry(
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
            )
            .unwrap();
        builder
            .push_geometry(
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
            )
            .unwrap();

        for _ in 0..2 {
            builder.push_time_interval(TimeInterval::default()).unwrap();

            builder.finish_row();
        }

        let collection = builder.build().unwrap();

        assert_eq!(collection, collection);

        assert_ne!(collection, collection.filter(vec![true, false]).unwrap());
    }

    #[test]
    fn filter() {
        let mut builder = MultiPolygonCollection::builder().finish_header();

        builder
            .push_geometry(
                MultiPolygon::new(vec![vec![vec![
                    (0.0, 0.1).into(),
                    (0.0, 1.1).into(),
                    (1.0, 0.1).into(),
                    (0.0, 0.1).into(),
                ]]])
                .unwrap(),
            )
            .unwrap();
        builder
            .push_geometry(
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
            )
            .unwrap();
        builder
            .push_geometry(
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
            )
            .unwrap();

        for _ in 0..3 {
            builder.push_time_interval(TimeInterval::default()).unwrap();

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

        builder
            .push_geometry(
                MultiPolygon::new(vec![vec![vec![
                    (0.0, 0.1).into(),
                    (0.0, 1.1).into(),
                    (1.0, 0.1).into(),
                    (0.0, 0.1).into(),
                ]]])
                .unwrap(),
            )
            .unwrap();
        builder.push_time_interval(TimeInterval::default()).unwrap();
        builder.finish_row();

        let collection_a = builder.build().unwrap();

        let mut builder = MultiPolygonCollection::builder().finish_header();

        builder
            .push_geometry(
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
        assert_eq!(collection_b.is_simple(), true);
        assert_eq!(collection_c.is_simple(), true);

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
}
