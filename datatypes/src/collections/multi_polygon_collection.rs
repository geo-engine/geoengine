use crate::{
    collections::{
        FeatureCollection, FeatureCollectionInfos, FeatureCollectionRowBuilder,
        GeoFeatureCollectionRowBuilder, GeometryCollection, GeometryRandomAccess,
        IntoGeometryIterator,
    },
    operations::reproject::Reproject,
};
use crate::{
    operations::reproject::CoordinateProjection,
    primitives::{Coordinate2D, MultiPolygon, MultiPolygonAccess, MultiPolygonRef},
};
use crate::{
    primitives::MultiLineString,
    util::arrow::{downcast_array, ArrowTyped},
};
use crate::{primitives::TimeInterval, util::Result};
use arrow::{
    array::{Array, ArrayData, FixedSizeListArray, Float64Array, ListArray},
    buffer::Buffer,
    datatypes::DataType,
};
use std::{slice, sync::Arc};

use super::feature_collection::struct_array_from_data;

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
                floats.values().as_ptr().cast::<Coordinate2D>(),
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

        unsafe { slice::from_raw_parts(buffer.as_ptr().cast::<i32>(), geometries.len() + 1) }
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

        unsafe { slice::from_raw_parts(buffer.as_ptr().cast::<i32>(), polygons.len() + 1) }
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

        unsafe { slice::from_raw_parts(buffer.as_ptr().cast::<i32>(), rings.len() + 1) }
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

impl<P> Reproject<P> for MultiPolygonCollection
where
    P: CoordinateProjection,
{
    type Out = MultiPolygonCollection;

    fn reproject(&self, projector: &P) -> Result<Self::Out> {
        // get the coordinates
        let coords_ref = self.coordinates();
        // reproject them...
        let projected_coords = projector.project_coordinate_slice_copy(coords_ref)?;

        // transform the coordinates into a byte slice and create a Buffer from it.
        let coords_buffer = unsafe {
            let coord_bytes: &[u8] = std::slice::from_raw_parts(
                projected_coords.as_ptr().cast::<u8>(),
                projected_coords.len() * std::mem::size_of::<Coordinate2D>(),
            );
            Buffer::from(coord_bytes)
        };

        // get the offsets (reuse)
        let geometries_ref = self
            .table
            .column_by_name(Self::GEOMETRY_COLUMN_NAME)
            .expect("There must exist a geometry column");
        let geometries: &ListArray = downcast_array(geometries_ref);

        let feature_offset_array = geometries.data();
        let feature_offsets_buffer = &feature_offset_array.buffers()[0];
        let num_features = (feature_offsets_buffer.len() / std::mem::size_of::<i32>()) - 1;

        let polygon_offsets_array = &feature_offset_array.child_data()[0];
        let polygon_offsets_buffer = &polygon_offsets_array.buffers()[0];
        let num_polygons = (polygon_offsets_buffer.len() / std::mem::size_of::<i32>()) - 1;

        let ring_offsets_array = &polygon_offsets_array.child_data()[0];
        let ring_offsets_buffer = &ring_offsets_array.buffers()[0];
        let num_rings = (ring_offsets_buffer.len() / std::mem::size_of::<i32>()) - 1;

        let num_coords = coords_buffer.len() / std::mem::size_of::<Coordinate2D>();
        let num_floats = num_coords * 2;

        let multi_polygon_array = ArrayData::builder(MultiPolygon::arrow_data_type())
            .len(num_features)
            .add_buffer(feature_offsets_buffer.clone())
            .add_child_data(
                ArrayData::builder(MultiLineString::arrow_data_type())
                    .len(num_polygons)
                    .add_buffer(polygon_offsets_buffer.clone())
                    .add_child_data(
                        ArrayData::builder(Coordinate2D::arrow_list_data_type())
                            .len(num_rings)
                            .add_buffer(ring_offsets_buffer.clone())
                            .add_child_data(
                                ArrayData::builder(Coordinate2D::arrow_data_type())
                                    .len(num_coords)
                                    .add_child_data(
                                        ArrayData::builder(DataType::Float64)
                                            .len(num_floats)
                                            .add_buffer(coords_buffer)
                                            .build(),
                                    )
                                    .build(),
                            )
                            .build(),
                    )
                    .build(),
            )
            .build();

        let mut columns = Vec::<arrow::datatypes::Field>::with_capacity(self.table.num_columns());
        let mut column_values =
            Vec::<arrow::array::ArrayRef>::with_capacity(self.table.num_columns());

        // copy geometry data if feature collection is geo collection
        // if CollectionType::IS_GEOMETRY {
        columns.push(arrow::datatypes::Field::new(
            Self::GEOMETRY_COLUMN_NAME,
            MultiLineString::arrow_data_type(),
            false,
        ));
        column_values.push(Arc::new(ListArray::from(multi_polygon_array)));
        // }

        // copy time data
        columns.push(arrow::datatypes::Field::new(
            Self::TIME_COLUMN_NAME,
            TimeInterval::arrow_data_type(),
            false,
        ));
        column_values.push(
            self.table
                .column_by_name(Self::TIME_COLUMN_NAME)
                .expect("The time column must exist")
                .clone(),
        );

        // copy remaining attribute data
        for (column_name, column_type) in &self.types {
            columns.push(arrow::datatypes::Field::new(
                &column_name,
                column_type.arrow_data_type(),
                column_type.nullable(),
            ));
            column_values.push(
                self.table
                    .column_by_name(&column_name)
                    .expect("The attribute column must exist")
                    .clone(),
            );
        }

        Ok(Self::new_from_internals(
            struct_array_from_data(columns, column_values, self.table.len()),
            self.types.clone(),
        ))
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

    #[test]
    fn reproject_multi_lines_epsg4326_epsg900913_collection() {
        use crate::operations::reproject::{CoordinateProjection, CoordinateProjector};
        use crate::spatial_reference::{SpatialReference, SpatialReferenceAuthority};

        use crate::operations::reproject::tests::{
            COLOGNE_EPSG_4326, COLOGNE_EPSG_900_913, HAMBURG_EPSG_4326, HAMBURG_EPSG_900_913,
            MARBURG_EPSG_4326, MARBURG_EPSG_900_913,
        };

        let from = SpatialReference::epsg_4326();
        let to = SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);
        let projector = CoordinateProjector::from_known_srs(from, to).unwrap();

        let mut builder = MultiPolygonCollection::builder().finish_header();

        builder
            .push_geometry(
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
            )
            .unwrap();
        builder
            .push_geometry(
                MultiPolygon::new(vec![vec![vec![
                    MARBURG_EPSG_4326,
                    COLOGNE_EPSG_4326,
                    HAMBURG_EPSG_4326,
                    MARBURG_EPSG_4326,
                ]]])
                .unwrap(),
            )
            .unwrap();

        for _ in 0..2 {
            builder.push_time_interval(TimeInterval::default()).unwrap();

            builder.finish_row();
        }

        let collection = builder.build().unwrap();

        let mut builder = MultiPolygonCollection::builder().finish_header();

        builder
            .push_geometry(
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
            )
            .unwrap();
        builder
            .push_geometry(
                MultiPolygon::new(vec![vec![vec![
                    MARBURG_EPSG_900_913,
                    COLOGNE_EPSG_900_913,
                    HAMBURG_EPSG_900_913,
                    MARBURG_EPSG_900_913,
                ]]])
                .unwrap(),
            )
            .unwrap();

        for _ in 0..2 {
            builder.push_time_interval(TimeInterval::default()).unwrap();

            builder.finish_row();
        }

        let expected_collection = builder.build().unwrap();

        let proj_collection = collection.reproject(&projector).unwrap();

        assert_eq!(proj_collection, expected_collection)
    }
}
