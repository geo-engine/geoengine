use crate::collections::{error, FeatureCollectionError, TypedFeatureCollection};
use crate::collections::{FeatureCollection, VectorDataType};
use crate::primitives::CacheHint;
use crate::primitives::{
    Coordinate2D, FeatureDataType, Geometry, MultiLineString, MultiPoint, MultiPolygon, NoGeometry,
    TimeInterval,
};
use crate::util::arrow::ArrowTyped;
use crate::util::Result;
use arrow::array::{
    ArrayData, ArrayRef, FixedSizeListArray, ListArray, PrimitiveArray, StructArray,
};
use arrow::buffer::Buffer;
use arrow::datatypes::{ArrowPrimitiveType, DataType, Field};
use snafu::ensure;
use std::collections::HashMap;
use std::sync::Arc;

/// A feature collection builder that accepts arrow `Buffer`s filled with data. This allows to build
/// a collection without copying data e.g. when the feature data is read from an Open CL device
/// directly into an arrow buffer
#[derive(Debug)]
pub struct RawFeatureCollectionBuilder {
    types: HashMap<String, FeatureDataType>,
    column_arrays: HashMap<String, ArrayRef>,
    time_array: Option<ArrayRef>,
    geo_array: Option<ArrayRef>,
    num_features: usize,
    num_coords: usize,
    num_lines: Option<usize>,
    num_polygons: Option<usize>,
    num_rings: Option<usize>,
    pub output: Option<TypedFeatureCollection>,
    pub output_type: VectorDataType,
    cache_hint: CacheHint,
}

impl RawFeatureCollectionBuilder {
    pub fn new(
        output_type: VectorDataType,
        types: HashMap<String, FeatureDataType>,
        num_features: usize,
        num_coords: usize,
    ) -> Self {
        Self {
            types,
            column_arrays: HashMap::new(),
            time_array: None,
            geo_array: None,
            num_features,
            num_coords,
            num_lines: None,
            num_polygons: None,
            num_rings: None,
            output: None,
            output_type,
            cache_hint: CacheHint::default(),
        }
    }

    pub fn points(
        types: HashMap<String, FeatureDataType>,
        num_features: usize,
        num_coords: usize,
    ) -> Self {
        Self {
            types,
            column_arrays: HashMap::new(),
            time_array: None,
            geo_array: None,
            num_features,
            num_coords,
            num_lines: None,
            num_polygons: None,
            num_rings: None,
            output: None,
            output_type: VectorDataType::MultiPoint,
            cache_hint: CacheHint::default(),
        }
    }

    pub fn lines(
        types: HashMap<String, FeatureDataType>,
        num_features: usize,
        num_lines: usize,
        num_coords: usize,
    ) -> Self {
        Self {
            types,
            column_arrays: HashMap::new(),
            time_array: None,
            geo_array: None,
            num_features,
            num_coords,
            num_lines: Some(num_lines),
            num_polygons: None,
            num_rings: None,
            output: None,
            output_type: VectorDataType::MultiLineString,
            cache_hint: CacheHint::default(),
        }
    }

    pub fn polygons(
        types: HashMap<String, FeatureDataType>,
        num_features: usize,
        num_polygons: usize,
        num_rings: usize,
        num_coords: usize,
    ) -> Self {
        Self {
            types,
            column_arrays: HashMap::new(),
            time_array: None,
            geo_array: None,
            num_features,
            num_coords,
            num_lines: None,
            num_polygons: Some(num_polygons),
            num_rings: Some(num_rings),
            output: None,
            output_type: VectorDataType::MultiPolygon,
            cache_hint: CacheHint::default(),
        }
    }

    pub fn num_features(&self) -> usize {
        self.num_features
    }

    pub fn num_coords(&self) -> usize {
        self.num_coords
    }

    pub fn num_lines(&self) -> Result<usize> {
        self.num_lines
            .ok_or_else(|| FeatureCollectionError::WrongDataType.into())
    }

    pub fn num_polygons(&self) -> Result<usize> {
        self.num_polygons
            .ok_or_else(|| FeatureCollectionError::WrongDataType.into())
    }

    pub fn num_rings(&self) -> Result<usize> {
        self.num_rings
            .ok_or_else(|| FeatureCollectionError::WrongDataType.into())
    }

    pub fn column_types(&self) -> HashMap<String, FeatureDataType> {
        self.types.clone()
    }

    #[allow(clippy::unnecessary_wraps)]
    pub fn set_time_intervals(&mut self, values_buffer: Buffer) -> Result<()> {
        let data = ArrayData::builder(TimeInterval::arrow_data_type())
            .len(self.num_features)
            .add_child_data(
                ArrayData::builder(arrow::datatypes::DataType::Int64)
                    .len(self.num_features * 2)
                    .add_buffer(values_buffer)
                    .build()?,
            )
            .build()?;

        let array = Arc::new(FixedSizeListArray::from(data)) as ArrayRef;

        self.time_array = Some(array);

        Ok(())
    }

    pub fn set_default_time_intervals(&mut self) {
        let mut time_intervals_builder = TimeInterval::arrow_builder(self.num_features);

        let default = TimeInterval::default();
        let start = default.start().inner();
        let end = default.end().inner();
        for _ in 0..self.num_features {
            let date_builder = time_intervals_builder.values();
            date_builder.append_value(start);
            date_builder.append_value(end);
            time_intervals_builder.append(true);
        }

        let array = Arc::new(time_intervals_builder.finish()) as ArrayRef;

        self.time_array = Some(array);
    }

    #[allow(clippy::unnecessary_wraps)]
    pub fn set_points(&mut self, coords: Buffer, offsets: Buffer) -> Result<()> {
        // TODO: check buffers validity / size
        // TODO: check offsets start at zero and are valid

        let num_features = offsets.len() / std::mem::size_of::<i32>() - 1;
        let num_coords = coords.len() / (2 * std::mem::size_of::<f64>());
        let num_floats = num_coords * 2;
        let data = ArrayData::builder(MultiPoint::arrow_data_type())
            .len(num_features)
            .add_buffer(offsets)
            .add_child_data(
                ArrayData::builder(Coordinate2D::arrow_data_type())
                    .len(num_coords)
                    .add_child_data(
                        ArrayData::builder(DataType::Float64)
                            .len(num_floats)
                            .add_buffer(coords)
                            .build()?,
                    )
                    .build()?,
            )
            .build()?;

        let array = Arc::new(ListArray::from(data)) as ArrayRef;

        self.geo_array = Some(array);

        Ok(())
    }

    #[allow(clippy::unnecessary_wraps)]
    pub fn set_lines(
        &mut self,
        coords: Buffer,
        line_offsets: Buffer,
        feature_offsets: Buffer,
    ) -> Result<()> {
        let num_features = (feature_offsets.len() / std::mem::size_of::<i32>()) - 1;
        let num_lines = (line_offsets.len() / std::mem::size_of::<i32>()) - 1;
        let num_coords = coords.len() / std::mem::size_of::<Coordinate2D>();
        let num_floats = num_coords * 2;
        let data = ArrayData::builder(MultiLineString::arrow_data_type())
            .len(num_features)
            .add_buffer(feature_offsets)
            .add_child_data(
                ArrayData::builder(Coordinate2D::arrow_list_data_type())
                    .len(num_lines)
                    .add_buffer(line_offsets)
                    .add_child_data(
                        ArrayData::builder(Coordinate2D::arrow_data_type())
                            .len(num_coords)
                            .add_child_data(
                                ArrayData::builder(DataType::Float64)
                                    .len(num_floats)
                                    .add_buffer(coords)
                                    .build()?,
                            )
                            .build()?,
                    )
                    .build()?,
            )
            .build()?;

        let array = Arc::new(ListArray::from(data)) as ArrayRef;

        self.geo_array = Some(array);

        Ok(())
    }

    #[allow(clippy::unnecessary_wraps)]
    pub fn set_polygons(
        &mut self,
        coords: Buffer,
        ring_offsets: Buffer,
        polygon_offsets: Buffer,
        feature_offsets: Buffer,
    ) -> Result<()> {
        let num_features = feature_offsets.len() / std::mem::size_of::<i32>() - 1;
        let num_polygons = polygon_offsets.len() / std::mem::size_of::<i32>() - 1;
        let num_rings = ring_offsets.len() / std::mem::size_of::<i32>() - 1;
        let num_coords = coords.len() / std::mem::size_of::<Coordinate2D>();
        let num_floats = num_coords * 2;
        let data = ArrayData::builder(MultiPolygon::arrow_data_type())
            .len(num_features)
            .add_buffer(feature_offsets)
            .add_child_data(
                ArrayData::builder(MultiLineString::arrow_data_type())
                    .len(num_polygons)
                    .add_buffer(polygon_offsets)
                    .add_child_data(
                        ArrayData::builder(Coordinate2D::arrow_list_data_type())
                            .len(num_rings)
                            .add_buffer(ring_offsets)
                            .add_child_data(
                                ArrayData::builder(Coordinate2D::arrow_data_type())
                                    .len(num_coords)
                                    .add_child_data(
                                        ArrayData::builder(DataType::Float64)
                                            .len(num_floats)
                                            .add_buffer(coords)
                                            .build()?,
                                    )
                                    .build()?,
                            )
                            .build()?,
                    )
                    .build()?,
            )
            .build()?;

        let array = Arc::new(ListArray::from(data)) as ArrayRef;

        self.geo_array = Some(array);

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
            .len(self.num_features)
            .add_buffer(values_buffer);

        let data = builder.null_bit_buffer(nulls_buffer).build()?;

        let array = Arc::new(PrimitiveArray::<T>::from(data)) as ArrayRef;

        self.column_arrays.entry(column.to_owned()).or_insert(array);

        Ok(())
    }

    pub fn set_cache_hint(&mut self, cache_hint: CacheHint) {
        self.cache_hint = cache_hint;
    }

    pub fn finish(&mut self) -> Result<()> {
        match self.output_type {
            VectorDataType::Data => self.finish_data(),
            VectorDataType::MultiPoint => self.finish_points(),
            VectorDataType::MultiLineString => self.finish_lines(),
            VectorDataType::MultiPolygon => self.finish_polygons(),
        }
    }

    pub fn finish_points(&mut self) -> Result<()> {
        self.output = Some(TypedFeatureCollection::MultiPoint(
            self.finish_collection::<MultiPoint>()?,
        ));
        Ok(())
    }

    pub fn finish_lines(&mut self) -> Result<()> {
        self.output = Some(TypedFeatureCollection::MultiLineString(
            self.finish_collection::<MultiLineString>()?,
        ));
        Ok(())
    }

    pub fn finish_polygons(&mut self) -> Result<()> {
        self.output = Some(TypedFeatureCollection::MultiPolygon(
            self.finish_collection::<MultiPolygon>()?,
        ));
        Ok(())
    }

    pub fn finish_data(&mut self) -> Result<()> {
        self.output = Some(TypedFeatureCollection::Data(
            self.finish_collection::<NoGeometry>()?,
        ));
        Ok(())
    }

    fn finish_collection<CollectionType: Geometry + ArrowTyped>(
        &mut self,
    ) -> Result<FeatureCollection<CollectionType>> {
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
            arrays.push(array);
        }

        if CollectionType::IS_GEOMETRY {
            columns.push(Field::new(
                FeatureCollection::<CollectionType>::GEOMETRY_COLUMN_NAME,
                CollectionType::arrow_data_type(),
                false,
            ));

            let geo = self.geo_array.take();
            arrays.push(geo.expect("checked"));
        }

        columns.push(Field::new(
            FeatureCollection::<CollectionType>::TIME_COLUMN_NAME,
            TimeInterval::arrow_data_type(),
            false,
        ));
        let time = self.time_array.take();
        arrays.push(time.expect("checked"));

        Ok(FeatureCollection::<CollectionType>::new_from_internals(
            StructArray::try_new(columns.into(), arrays, None)?,
            self.types.clone(),
            self.cache_hint,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::{
        BuilderProvider, DataCollection, FeatureCollectionInfos, MultiPointCollection, ToGeoJson,
    };
    use crate::primitives::{DataRef, FeatureDataRef};
    use arrow::buffer::MutableBuffer;
    use arrow::datatypes::{Float64Type, ToByteSlice};
    use arrow::util::bit_util;
    use serde_json::json;

    #[test]
    #[allow(clippy::float_cmp)]
    fn no_geo() {
        let mut builder = DataCollection::builder();
        builder
            .add_column("foo".into(), FeatureDataType::Float)
            .unwrap();
        let mut builder = builder.batch_builder(4, 0);
        builder.set_default_time_intervals();

        let numbers = vec![1., 2., 3., 4.];
        let nulls = [true, false, true, true];

        let value_buffer = Buffer::from(numbers.as_slice().to_byte_slice());

        let num_bytes = bit_util::ceil(numbers.len(), 8);
        let mut null_buffer = MutableBuffer::new(num_bytes).with_bitset(num_bytes, false);
        let null_slice = null_buffer.as_slice_mut();

        for (i, null) in nulls.iter().enumerate() {
            if *null {
                bit_util::set_bit(null_slice, i);
            }
        }

        // nulls
        builder
            .set_column::<Float64Type>("foo", value_buffer, Some(null_buffer.into()))
            .unwrap();

        builder.finish().unwrap();

        let collection = builder.output.unwrap().try_into_data().unwrap();

        assert_eq!(collection.len(), 4);

        match collection.data("foo").unwrap() {
            FeatureDataRef::Float(n) => {
                assert_eq!(n.as_ref(), &[1., 2., 3., 4.]);
                assert_eq!(n.nulls(), &[false, true, false, false]);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn point_coords_to_array() {
        let mut builder = MultiPointCollection::builder().batch_builder(3, 8);
        builder.set_default_time_intervals();

        let coords: Vec<f64> = vec![0.0, 0.0, 1.1, 1.1, 2.2, 2.2, 3.3, 3.3];
        let offsets: Vec<i32> = vec![0, 1, 3, 4];

        let coords_buffer = Buffer::from(coords.as_slice().to_byte_slice());
        let offsets_buffer = Buffer::from(offsets.to_byte_slice());

        builder.set_points(coords_buffer, offsets_buffer).unwrap();

        builder.finish().unwrap();

        let collection = builder.output.unwrap().try_into_points().unwrap();

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

    #[test]
    fn points_with_time() {
        let mut builder = MultiPointCollection::builder().batch_builder(3, 8);
        builder.set_default_time_intervals();

        let coords: Vec<f64> = vec![0.0, 0.0, 1.1, 1.1, 2.2, 2.2, 3.3, 3.3];
        let offsets: Vec<i32> = vec![0, 1, 3, 4];
        let time: Vec<i64> = vec![
            0,
            1_000,
            1_577_836_800 * 1000,
            1_609_459_199 * 1000 + 999,
            1_546_300_800 * 1000,
            1_577_836_799 * 1000 + 999,
        ];

        let coords_buffer = Buffer::from(coords.to_byte_slice());
        let offsets_buffer = Buffer::from(offsets.to_byte_slice());
        let time_buffer = Buffer::from(time.to_byte_slice());

        builder.set_points(coords_buffer, offsets_buffer).unwrap();
        builder.set_time_intervals(time_buffer).unwrap();

        builder.finish().unwrap();

        let collection = builder.output.unwrap().try_into_points().unwrap();

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
                        "start": "1970-01-01T00:00:00+00:00",
                        "end": "1970-01-01T00:00:01+00:00",
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
                        "start": "2020-01-01T00:00:00+00:00",
                        "end": "2020-12-31T23:59:59.999+00:00",
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
                        "start": "2019-01-01T00:00:00+00:00",
                        "end": "2019-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }]
            })
            .to_string()
        );
    }

    #[test]
    fn line_builder() {
        let mut builder = RawFeatureCollectionBuilder::lines(Default::default(), 2, 3, 7);
        builder.set_default_time_intervals();

        let coords: Vec<f64> = vec![
            0.0, 0.1, 1.0, 1.1, 2.0, 2.1, 3.0, 3.1, 4.0, 4.1, 5.0, 5.1, 6.0, 6.1, 7.0, 7.1,
        ];
        let line_offsets: Vec<i32> = vec![0, 2, 5, 8];
        let feature_offsets: Vec<i32> = vec![0, 2, 3];

        let coords_buffer = Buffer::from(coords.as_slice().to_byte_slice());
        let line_offsets_buffer = Buffer::from(line_offsets.to_byte_slice());
        let feature_offsets_buffer = Buffer::from(feature_offsets.to_byte_slice());

        builder
            .set_lines(coords_buffer, line_offsets_buffer, feature_offsets_buffer)
            .unwrap();

        builder.finish().unwrap();

        let collection = builder.output.unwrap().try_into_lines().unwrap();

        assert_eq!(
            collection.to_geo_json(),
            json!({
                "type": "FeatureCollection",
                "features": [{
                    "type": "Feature",
                    "geometry": {
                        "type": "MultiLineString",
                        "coordinates": [
                            [
                                [0.0, 0.1],
                                [1.0, 1.1]
                            ],
                            [
                                [2.0, 2.1],
                                [3.0, 3.1],
                                [4.0, 4.1]
                            ]
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
                        "type": "LineString",
                        "coordinates": [
                            [5.0, 5.1],
                            [6.0, 6.1],
                            [7.0, 7.1]
                        ]
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

    #[test]
    fn polygon_builder() {
        let mut builder = RawFeatureCollectionBuilder::polygons(Default::default(), 2, 3, 4, 16);
        builder.set_default_time_intervals();

        let ring0_coords = [0.0, 0.1, 10.0, 10.1, 0.0, 10.1, 0.0, 0.1];
        let ring1_coords = [2.0, 2.1, 3.0, 3.1, 2.0, 3.1, 2.0, 2.1];
        let ring3_coords = [4.0, 4.1, 6.0, 6.1, 4.0, 6.1, 4.0, 4.1];
        let ring4_coords = [5.0, 5.1, 6.0, 6.1, 5.0, 6.1, 5.0, 5.1];
        let coords: Vec<f64> = [ring0_coords, ring1_coords, ring3_coords, ring4_coords].concat();
        let ring_offsets: Vec<i32> = vec![0, 4, 8, 12, 16];
        let polygon_offsets: Vec<i32> = vec![0, 2, 3, 4];
        let feature_offsets: Vec<i32> = vec![0, 2, 3];

        let coords_buffer = Buffer::from(coords.as_slice().to_byte_slice());
        let ring_offsets_buffer = Buffer::from(ring_offsets.to_byte_slice());
        let polygon_offsets_buffer = Buffer::from(polygon_offsets.to_byte_slice());
        let feature_offsets_buffer = Buffer::from(feature_offsets.to_byte_slice());

        builder
            .set_polygons(
                coords_buffer,
                ring_offsets_buffer,
                polygon_offsets_buffer,
                feature_offsets_buffer,
            )
            .unwrap();

        builder.finish().unwrap();

        let collection = builder.output.unwrap().try_into_polygons().unwrap();

        assert_eq!(
            collection.to_geo_json(),
            json!({
                "type": "FeatureCollection",
                "features": [{
                    "type": "Feature",
                    "geometry": {
                        "type": "MultiPolygon",
                        "coordinates": [
                            [
                                [
                                    [0.0, 0.1],
                                    [10.0, 10.1],
                                    [0.0, 10.1],
                                    [0.0, 0.1]
                                ],
                                [
                                    [2.0, 2.1],
                                    [3.0, 3.1],
                                    [2.0, 3.1],
                                    [2.0, 2.1]
                                ]
                            ],
                            [
                                [
                                    [4.0, 4.1],
                                    [6.0, 6.1],
                                    [4.0, 6.1],
                                    [4.0, 4.1]
                                ]
                            ]
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
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [5.0, 5.1],
                                [6.0, 6.1],
                                [5.0, 6.1],
                                [5.0, 5.1]
                            ]
                        ]
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
