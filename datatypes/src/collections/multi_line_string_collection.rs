use crate::collections::{
    BuilderProvider, FeatureCollection, FeatureCollectionBuilderImplHelpers,
    FeatureCollectionImplHelpers, GeoFeatureCollectionRowBuilder, IntoGeometryIterator,
    SimpleFeatureCollectionBuilder, SimpleFeatureCollectionRowBuilder,
};
use crate::primitives::{
    Coordinate2D, FeatureDataType, MultiLineString, MultiLineStringAccess, MultiLineStringRef,
};
use crate::util::arrow::{downcast_array, ArrowTyped};
use crate::util::Result;
use arrow::array::{Array, BooleanArray, FixedSizeListArray, Float64Array, ListArray, StructArray};
use arrow::datatypes::DataType;
use std::collections::HashMap;
use std::slice;

/// This collection contains temporal multi-lines and miscellaneous data.
#[derive(Debug)]
pub struct MultiLineStringCollection {
    table: StructArray,
    types: HashMap<String, FeatureDataType>,
}

impl FeatureCollectionImplHelpers for MultiLineStringCollection {
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
        MultiLineString::arrow_data_type()
    }

    fn filtered_geometries(
        multi_lines: &ListArray,
        filter_array: &BooleanArray,
    ) -> Result<ListArray> {
        let mut multi_line_builder = MultiLineString::arrow_builder(0);

        for multi_line_index in 0..multi_lines.len() {
            if !filter_array.value(multi_line_index) {
                continue;
            }

            let line_builder = multi_line_builder.values();

            let lines_ref = multi_lines.value(multi_line_index);
            let lines = downcast_array::<ListArray>(&lines_ref);

            for line_index in 0..lines.len() {
                let coordinate_builder = line_builder.values();

                let coordinates_ref = lines.value(line_index);
                let coordinates = downcast_array::<FixedSizeListArray>(&coordinates_ref);

                for coordinate_index in 0..(coordinates.len() as usize) {
                    let floats_ref = coordinates.value(coordinate_index);
                    let floats: &Float64Array = downcast_array(&floats_ref);

                    coordinate_builder
                        .values()
                        .append_slice(floats.value_slice(0, 2))?;

                    coordinate_builder.append(true)?;
                }

                line_builder.append(true)?;
            }

            multi_line_builder.append(true)?;
        }

        Ok(multi_line_builder.finish())
    }

    fn concat_geometries(geometries_a: &ListArray, geometries_b: &ListArray) -> Result<ListArray> {
        let mut multi_line_builder =
            MultiLineString::arrow_builder(geometries_a.len() + geometries_b.len());

        for multi_lines in &[geometries_a, geometries_b] {
            for multi_line_index in 0..multi_lines.len() {
                let line_builder = multi_line_builder.values();

                let lines_ref = multi_lines.value(multi_line_index);
                let lines = downcast_array::<ListArray>(&lines_ref);

                for line_index in 0..lines.len() {
                    let coordinate_builder = line_builder.values();

                    let coordinates_ref = lines.value(line_index);
                    let coordinates = downcast_array::<FixedSizeListArray>(&coordinates_ref);

                    for coordinate_index in 0..(coordinates.len() as usize) {
                        let floats_ref = coordinates.value(coordinate_index);
                        let floats: &Float64Array = downcast_array(&floats_ref);

                        coordinate_builder
                            .values()
                            .append_slice(floats.value_slice(0, 2))?;

                        coordinate_builder.append(true)?;
                    }

                    line_builder.append(true)?;
                }

                multi_line_builder.append(true)?;
            }
        }

        Ok(multi_line_builder.finish())
    }

    fn _is_simple(&self) -> bool {
        let multi_line_array: &ListArray = downcast_array(
            &self
                .table
                .column_by_name(MultiLineStringCollection::GEOMETRY_COLUMN_NAME)
                .expect("Column must exist since it is in the metadata"),
        );

        let line_array_ref = multi_line_array.values();
        let line_array: &ListArray = downcast_array(&line_array_ref);

        multi_line_array.len() == line_array.len()
    }
}

impl<'l> IntoGeometryIterator<'l> for MultiLineStringCollection {
    type GeometryIterator = MultiLineIterator<'l>;
    type GeometryType = MultiLineStringRef<'l>;

    fn geometries(&'l self) -> Self::GeometryIterator {
        let geometry_column: &ListArray = downcast_array(
            &self
                .table
                .column_by_name(MultiLineStringCollection::GEOMETRY_COLUMN_NAME)
                .expect("Column must exist since it is in the metadata"),
        );

        MultiLineIterator {
            geometry_column,
            index: 0,
            length: self.len(),
        }
    }
}

/// A collection iterator for multi points
pub struct MultiLineIterator<'l> {
    geometry_column: &'l ListArray,
    index: usize,
    length: usize,
}

impl<'l> Iterator for MultiLineIterator<'l> {
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

into_geometry_options_impl!(MultiLineStringCollection);
feature_collection_impl!(MultiLineStringCollection, true);

impl GeoFeatureCollectionRowBuilder<MultiLineString>
    for SimpleFeatureCollectionRowBuilder<MultiLineStringCollection>
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

impl FeatureCollectionBuilderImplHelpers for MultiLineStringCollection {
    type GeometriesBuilder = <MultiLineString as ArrowTyped>::ArrowBuilder;

    const HAS_GEOMETRIES: bool = true;

    fn geometries_builder() -> Self::GeometriesBuilder {
        MultiLineString::arrow_builder(0)
    }
}

impl BuilderProvider for MultiLineStringCollection {
    type Builder = SimpleFeatureCollectionBuilder<Self>;
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::collections::{FeatureCollectionBuilder, FeatureCollectionRowBuilder};
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
