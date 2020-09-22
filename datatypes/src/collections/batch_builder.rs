use crate::collections::error;
use crate::collections::feature_collection::struct_array_from_data;
use crate::collections::FeatureCollection;
use crate::primitives::{Coordinate2D, FeatureDataType, Geometry, MultiPoint, TimeInterval};
use crate::util::arrow::ArrowTyped;
use crate::util::Result;
use arrow::array::{ArrayData, ArrayRef, FixedSizeListArray, ListArray, PrimitiveArray};
use arrow::buffer::Buffer;
use arrow::datatypes::{ArrowPrimitiveType, DataType, Field};
use snafu::ensure;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct FeatureCollectionBatchBuilder<CollectionType>
where
    CollectionType: Geometry + ArrowTyped,
{
    types: HashMap<String, FeatureDataType>,
    column_arrays: HashMap<String, ArrayRef>,
    time_array: Option<ArrayRef>,
    geo_array: Option<ArrayRef>,
    feature_count: usize,
    pub output: Option<FeatureCollection<CollectionType>>,
}

impl<CollectionType> FeatureCollectionBatchBuilder<CollectionType>
where
    CollectionType: Geometry + ArrowTyped,
{
    pub fn new(types: HashMap<String, FeatureDataType>, feature_count: usize) -> Self {
        Self {
            types,
            column_arrays: HashMap::new(),
            time_array: None,
            geo_array: None,
            feature_count,
            output: None,
        }
    }

    pub fn set_time_intervals(&mut self, values_buffer: Buffer) -> Result<()> {
        let data = ArrayData::builder(TimeInterval::arrow_data_type())
            .len(self.feature_count)
            .add_buffer(values_buffer)
            .build();

        let array = Arc::new(FixedSizeListArray::from(data)) as ArrayRef;

        self.time_array = Some(array);

        Ok(())
    }

    pub fn set_default_time_intervals(&mut self) -> Result<()> {
        let mut time_intervals_builder = TimeInterval::arrow_builder(self.feature_count);

        let default = TimeInterval::default();
        for _ in 0..self.feature_count {
            let date_builder = time_intervals_builder.values();
            date_builder.append_value(default.start().inner())?;
            date_builder.append_value(default.end().inner())?;

            time_intervals_builder.append(true)?;
        }

        let array = Arc::new(time_intervals_builder.finish()) as ArrayRef;

        self.time_array = Some(array);

        Ok(())
    }

    /// Set the column values for the given column from the given buffers.
    /// `values_buffer` buffer with data of values for construction of primitive array
    /// `nulls_buffer` optional buffer with info about nulls in the data as memset
    /// `T` the `ArrowPrimitiveType` of the column
    pub fn set_column<T: ArrowPrimitiveType>(
        &mut self,
        column: &str,
        values_buffer: Buffer,
        nulls_buffer: Option<Buffer>,
    ) -> Result<()> {
        ensure!(
            self.types.get(column).is_some(),
            error::ColumnDoesNotExist { name: column }
        );

        // TODO: check buffers are valid?

        // TODO: check if length corresponds to feature_count

        // TODO: check if type T corresponds to column type

        let builder = ArrayData::builder(DataType::Float64)
            .len(self.feature_count)
            .add_buffer(values_buffer);

        let data = if let Some(nulls) = nulls_buffer {
            builder.null_bit_buffer(nulls).build()
        } else {
            builder.build()
        };

        let array = Arc::new(PrimitiveArray::<T>::from(data)) as ArrayRef;

        self.column_arrays.entry(column.to_owned()).or_insert(array);

        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        ensure!(
            self.types
                .keys()
                .all(|k| self.column_arrays.contains_key(k)),
            error::MissingColumnArray
        );
        ensure!(self.time_array.is_some(), error::MissingTime);
        ensure!(
            !CollectionType::IS_GEOMETRY || self.geo_array.is_some(),
            error::MissingGeo
        );

        let mut columns = Vec::with_capacity(self.types.len() + 2);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.types.len() + 2);

        for (column_name, array) in self.column_arrays.drain() {
            let column_type = self.types.get(&column_name).unwrap(); // column must exist
            columns.push(Field::new(
                &column_name,
                column_type.arrow_data_type(),
                column_type.nullable(),
            ));
            arrays.push(array)
        }

        if CollectionType::IS_GEOMETRY {
            columns.push(Field::new(
                FeatureCollection::<CollectionType>::GEOMETRY_COLUMN_NAME,
                CollectionType::arrow_data_type(),
                false,
            ));

            let geo = std::mem::replace(&mut self.geo_array, None);
            arrays.push(geo.expect("checked"));
        }

        columns.push(Field::new(
            FeatureCollection::<CollectionType>::TIME_COLUMN_NAME,
            TimeInterval::arrow_data_type(),
            false,
        ));
        let time = std::mem::replace(&mut self.time_array, None);
        arrays.push(time.expect("checked"));

        let collection = FeatureCollection::<CollectionType>::new_from_internals(
            struct_array_from_data(columns, arrays, self.feature_count),
            self.types.clone(),
        );

        self.output = Some(collection);

        Ok(())
    }
}

pub struct MultiPointBuffers {
    pub offsets: Buffer,
    pub coords: Buffer,
}

// struct MultiLineStringBuffers {
//     line_offsets: Buffer,
//     coords_offsets: Buffer,
//     coords: Buffer,
// }
//
// struct MultiPolygonBuffers {
//     polygon_offsets: Buffer,
//     rings_offsets: Buffer,
//     coords_offsets: Buffer,
//     coords: Buffer,
// }

pub trait GeoFromBuffers<CollectionType>
where
    CollectionType: Geometry + ArrowTyped,
{
    type Buffers;
    fn set_geo(&mut self, buffers: Self::Buffers) -> Result<()>;
}

impl GeoFromBuffers<MultiPoint> for FeatureCollectionBatchBuilder<MultiPoint> {
    type Buffers = MultiPointBuffers;
    fn set_geo(&mut self, buffers: MultiPointBuffers) -> Result<()> {
        // TODO: check buffers validity / size

        let MultiPointBuffers { offsets, coords } = buffers;

        let num_multi_points = offsets.len() / std::mem::size_of::<i32>() - 1;
        let num_coords = coords.len() / std::mem::size_of::<f64>();
        let num_points = num_coords / 2;
        let data = ArrayData::builder(MultiPoint::arrow_data_type())
            .len(num_multi_points)
            .add_buffer(offsets)
            .add_child_data(
                ArrayData::builder(Coordinate2D::arrow_data_type())
                    .len(num_points)
                    .add_child_data(
                        ArrayData::builder(DataType::Float64)
                            .len(num_coords)
                            .add_buffer(coords)
                            .build(),
                    )
                    .build(),
            )
            .build();

        let array = Arc::new(ListArray::from(data)) as ArrayRef;

        self.geo_array = Some(array);

        Ok(())
    }
}

// TODO: GeoFromBuffers for lines and polygons

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::{BuilderProvider, DataCollection, MultiPointCollection};
    use crate::primitives::{FeatureDataRef, NullableDataRef};
    use arrow::buffer::MutableBuffer;
    use arrow::datatypes::{Float64Type, ToByteSlice};
    use arrow::util::bit_util;
    use serde_json::json;

    #[test]
    #[allow(clippy::float_cmp)]
    fn no_geo() {
        let mut builder = DataCollection::builder();
        builder
            .add_column("foo".into(), FeatureDataType::NullableNumber)
            .unwrap();
        let mut builder = builder.batch_builder(4);
        builder.set_default_time_intervals().unwrap();

        let numbers = vec![1., 2., 3., 4.];
        let nulls = vec![true, false, true, true];

        let value_buffer = Buffer::from(numbers.as_slice().to_byte_slice());

        let num_bytes = bit_util::ceil(numbers.len(), 8);
        let mut null_buffer = MutableBuffer::new(num_bytes).with_bitset(num_bytes, false);
        let null_slice = null_buffer.data_mut();

        for (i, null) in nulls.iter().enumerate() {
            if *null {
                bit_util::set_bit(null_slice, i);
            }
        }

        // nulls
        builder
            .set_column::<Float64Type>("foo", value_buffer, Some(null_buffer.freeze()))
            .unwrap();

        builder.finish().unwrap();

        let collection = builder.output.unwrap();

        assert_eq!(collection.len(), 4);

        match collection.data("foo").unwrap() {
            FeatureDataRef::NullableNumber(n) => {
                assert_eq!(n.as_ref()[0], 1.);
                assert_eq!(n.as_ref()[1], 2.);
                assert_eq!(n.as_ref()[2], 3.);
                assert_eq!(n.as_ref()[3], 4.);

                assert_eq!(n.nulls()[0], !true);
                assert_eq!(n.nulls()[1], !false);
                assert_eq!(n.nulls()[2], !true);
                assert_eq!(n.nulls()[3], !true);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn point_coords_to_array() {
        let mut builder = MultiPointCollection::builder().batch_builder(3);
        builder.set_default_time_intervals().unwrap();

        let coords: Vec<f64> = vec![0.0, 0.0, 1.1, 1.1, 2.2, 2.2, 3.3, 3.3];
        let offsets: Vec<i32> = vec![0, 1, 3, 4];

        let coords_buffer = Buffer::from(coords.as_slice().to_byte_slice());
        let offsets_buffer = Buffer::from(offsets.to_byte_slice());

        builder
            .set_geo(MultiPointBuffers {
                offsets: offsets_buffer,
                coords: coords_buffer,
            })
            .unwrap();

        builder.finish().unwrap();

        let collection = builder.output.unwrap();

        assert_eq!(
            collection.to_geo_json(),
            json!({
                "type": "FeatureCollection",
                "features": [{
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [0.0, 0.0]
                    },
                    "properties": {},
                    "when": {
                        "start": "-262144-01-01T00:00:00+00:00",
                        "end": "+262143-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }, {
                    "type": "Feature",
                    "geometry": {
                        "type": "MultiPoint",
                        "coordinates": [
                            [1.1, 1.1],
                            [2.2, 2.2]
                        ]
                    },
                    "properties": {},
                    "when": {
                        "start": "-262144-01-01T00:00:00+00:00",
                        "end": "+262143-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }, {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [3.3, 3.3]
                    },
                    "properties": {},
                    "when": {
                        "start": "-262144-01-01T00:00:00+00:00",
                        "end": "+262143-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }]
            })
            .to_string()
        );
    }
}
