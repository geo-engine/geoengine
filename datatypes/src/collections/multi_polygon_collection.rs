use crate::collections::{
    BuilderProvider, FeatureCollection, FeatureCollectionBuilderImplHelpers,
    FeatureCollectionImplHelpers, GeoFeatureCollectionRowBuilder, IntoGeometryIterator,
    SimpleFeatureCollectionBuilder, SimpleFeatureCollectionRowBuilder,
};
use crate::primitives::{
    Coordinate2D, FeatureDataType, MultiPolygon, MultiPolygonAccess, MultiPolygonRef,
};
use crate::util::arrow::{downcast_array, ArrowTyped};
use crate::util::Result;
use arrow::array::{Array, BooleanArray, FixedSizeListArray, Float64Array, ListArray, StructArray};
use arrow::datatypes::DataType;
use std::collections::HashMap;
use std::slice;

/// This collection contains temporal multi-polygons miscellaneous data.
#[derive(Debug)]
pub struct MultiPolygonCollection {
    table: StructArray,
    types: HashMap<String, FeatureDataType>,
}

impl FeatureCollectionImplHelpers for MultiPolygonCollection {
    fn new_from_internals(table: StructArray, types: HashMap<String, FeatureDataType>) -> Self {
        Self { table, types }
    }

    fn table(&self) -> &StructArray {
        &self.table
    }

    fn types(&self) -> &HashMap<String, FeatureDataType> {
        &self.types
    }

    fn geometry_arrow_data_type() -> DataType {
        MultiPolygon::arrow_data_type()
    }

    fn filtered_geometries(
        multi_polygons: &ListArray,
        filter_array: &BooleanArray,
    ) -> Result<ListArray> {
        let mut multi_polygon_builder = MultiPolygon::arrow_builder(0);

        for multi_polygon_index in 0..multi_polygons.len() {
            if !filter_array.value(multi_polygon_index) {
                continue;
            }

            let polygon_builder = multi_polygon_builder.values();

            let polygons_ref = multi_polygons.value(multi_polygon_index);
            let polygons = downcast_array::<ListArray>(&polygons_ref);

            for polygon_index in 0..polygons.len() {
                let ring_builder = polygon_builder.values();

                let rings_ref = polygons.value(polygon_index);
                let rings = downcast_array::<ListArray>(&rings_ref);

                for ring_index in 0..rings.len() {
                    let coordinate_builder = ring_builder.values();

                    let coordinates_ref = rings.value(ring_index);
                    let coordinates = downcast_array::<FixedSizeListArray>(&coordinates_ref);

                    for coordinate_index in 0..(coordinates.len() as usize) {
                        let floats_ref = coordinates.value(coordinate_index);
                        let floats: &Float64Array = downcast_array(&floats_ref);

                        coordinate_builder
                            .values()
                            .append_slice(floats.value_slice(0, 2))?;

                        coordinate_builder.append(true)?;
                    }

                    ring_builder.append(true)?;
                }

                polygon_builder.append(true)?;
            }

            multi_polygon_builder.append(true)?;
        }

        Ok(multi_polygon_builder.finish())
    }

    fn concat_geometries(geometries_a: &ListArray, geometries_b: &ListArray) -> Result<ListArray> {
        let mut multi_polygon_builder =
            MultiPolygon::arrow_builder(geometries_a.len() + geometries_b.len());

        for multi_polygons in &[geometries_a, geometries_b] {
            for multi_polygon_index in 0..multi_polygons.len() {
                let polygon_builder = multi_polygon_builder.values();

                let polygons_ref = multi_polygons.value(multi_polygon_index);
                let polygons = downcast_array::<ListArray>(&polygons_ref);

                for polygon_index in 0..polygons.len() {
                    let ring_builder = polygon_builder.values();

                    let rings_ref = polygons.value(polygon_index);
                    let rings = downcast_array::<ListArray>(&rings_ref);

                    for ring_index in 0..rings.len() {
                        let coordinate_builder = ring_builder.values();

                        let coordinates_ref = rings.value(ring_index);
                        let coordinates = downcast_array::<FixedSizeListArray>(&coordinates_ref);

                        for coordinate_index in 0..(coordinates.len() as usize) {
                            let floats_ref = coordinates.value(coordinate_index);
                            let floats: &Float64Array = downcast_array(&floats_ref);

                            coordinate_builder
                                .values()
                                .append_slice(floats.value_slice(0, 2))?;

                            coordinate_builder.append(true)?;
                        }

                        ring_builder.append(true)?;
                    }

                    polygon_builder.append(true)?;
                }

                multi_polygon_builder.append(true)?;
            }
        }

        Ok(multi_polygon_builder.finish())
    }

    fn _is_simple(&self) -> bool {
        let multi_polygon_array: &ListArray = downcast_array(
            &self
                .table
                .column_by_name(MultiPolygonCollection::GEOMETRY_COLUMN_NAME)
                .expect("Column must exist since it is in the metadata"),
        );

        let polygon_array_ref = multi_polygon_array.values();
        let polygon_array: &ListArray = downcast_array(&polygon_array_ref);

        multi_polygon_array.len() == polygon_array.len()
    }
}

impl<'l> IntoGeometryIterator<'l> for MultiPolygonCollection {
    type GeometryIterator = MultiPolygonIterator<'l>;
    type GeometryType = MultiPolygonRef<'l>;

    fn geometries(&'l self) -> Self::GeometryIterator {
        let geometry_column: &ListArray = downcast_array(
            &self
                .table
                .column_by_name(MultiPolygonCollection::GEOMETRY_COLUMN_NAME)
                .expect("Column must exist since it is in the metadata"),
        );

        MultiPolygonIterator {
            geometry_column,
            index: 0,
            length: self.len(),
        }
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
                        float_array.raw_values() as *const Coordinate2D,
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

into_geometry_options_impl!(MultiPolygonCollection);
feature_collection_impl!(MultiPolygonCollection, true);

impl GeoFeatureCollectionRowBuilder<MultiPolygon>
    for SimpleFeatureCollectionRowBuilder<MultiPolygonCollection>
{
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

impl FeatureCollectionBuilderImplHelpers for MultiPolygonCollection {
    type GeometriesBuilder = <MultiPolygon as ArrowTyped>::ArrowBuilder;

    const HAS_GEOMETRIES: bool = true;

    fn geometries_builder() -> Self::GeometriesBuilder {
        MultiPolygon::arrow_builder(0)
    }
}

impl BuilderProvider for MultiPolygonCollection {
    type Builder = SimpleFeatureCollectionBuilder<Self>;
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::collections::{FeatureCollectionBuilder, FeatureCollectionRowBuilder};
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
