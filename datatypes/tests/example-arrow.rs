use arrow::array::{
    Array, ArrayData, BooleanArray, Date64Array, Date64Builder, FixedSizeBinaryBuilder,
    FixedSizeListArray, FixedSizeListBuilder, Float64Array, Float64Builder, Int32Array,
    Int32Builder, ListArray, ListBuilder, StringArray, StringBuilder, StructBuilder, UInt64Array,
    UInt64Builder,
};
use arrow::buffer::Buffer;
use arrow::compute::gt_eq_scalar;
use arrow::compute::kernels::filter::filter;
use arrow::datatypes::{DataType, Field};
use geoengine_datatypes::primitives::{Coordinate2D, TimeInterval};
use std::{mem, slice};

#[test]
fn simple() {
    let mut primitive_array_builder = Int32Builder::with_capacity(5);
    primitive_array_builder.append_value(1);
    primitive_array_builder.append_value(2);
    primitive_array_builder.append_slice(&(3..=5).collect::<Vec<i32>>());

    let primitive_array = primitive_array_builder.finish();

    assert_eq!(primitive_array.len(), 5);
    assert_eq!(primitive_array.null_count(), 0);

    let mask = vec![true, false, true, false, true].into();

    let filtered_array = filter(&primitive_array, &mask).unwrap();

    assert_eq!(filtered_array.len(), 3);
    assert_eq!(filtered_array.null_count(), 0);

    assert!(primitive_array.data().null_bitmap().is_none());
}

#[test]
fn null_values() {
    let mut primitive_array_builder = Int32Builder::with_capacity(5);
    primitive_array_builder.append_value(1);
    primitive_array_builder.append_null();
    primitive_array_builder.append_slice(&[3, 4, 5]);

    let primitive_array = primitive_array_builder.finish();

    assert_eq!(primitive_array.len(), 5);
    assert_eq!(primitive_array.null_count(), 1);

    let data = primitive_array.values();

    assert_eq!(data.len(), 5);

    assert_eq!(&data[0..1], &[1]);
    assert_eq!(&data[2..5], &[3, 4, 5]);
}

#[test]
fn null_bytes() {
    let mut primitive_array_builder = Int32Builder::with_capacity(2);
    primitive_array_builder.append_value(1);
    primitive_array_builder.append_null();
    primitive_array_builder.append_option(None);
    primitive_array_builder.append_option(Some(4));
    primitive_array_builder.append_null();

    let primitive_array = primitive_array_builder.finish();

    assert_eq!(primitive_array.len(), 5);
    assert_eq!(primitive_array.null_count(), 3);

    if let Some(null_bitmap) = primitive_array.data().null_bitmap() {
        assert_eq!(null_bitmap.bit_len(), 8); // bit_len returns number of bits

        assert_eq!(
            null_bitmap.clone().into_buffer().as_slice(), // must clone bitmap because there is no way to get a reference to the data
            &[0b0000_1001] // right most bit is first element, 1 = valid value, 0 = null or unset
        );
    }
}

#[test]
#[allow(clippy::float_cmp)]
fn offset() {
    let array = {
        let mut array_builder = Float64Builder::with_capacity(5);
        array_builder.append_slice(&[2e10, 4e40, 20., 9.4, 0.]);
        array_builder.finish()
    };

    assert_eq!(array.len(), 5);
    assert_eq!(array.offset(), 0);

    let subarray = array.slice(2, 2);
    let typed_subarray: &Float64Array = subarray.as_any().downcast_ref().unwrap();

    assert_eq!(subarray.len(), 2);
    assert_eq!(subarray.offset(), 2);
    assert_eq!(typed_subarray.values().len(), 2);

    assert_eq!(typed_subarray.values(), &[20., 9.4]);
}

#[test]
fn strings() {
    use arrow::datatypes::ToByteSlice;

    let array = {
        let mut strings = String::new();
        let mut offsets: Vec<i32> = Vec::new();

        for string in &["hello", "from", "the", "other", "side"] {
            offsets.push(strings.len() as i32);
            strings.push_str(string);
        }
        offsets.push(strings.len() as i32);

        let data = ArrayData::builder(DataType::Utf8)
            .len(offsets.len() - 1) // number of strings
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_buffer(Buffer::from(strings.as_bytes()))
            .build()
            .unwrap();

        StringArray::from(data)
    };

    assert_eq!(array.len(), 5);
    assert_eq!(array.null_count(), 0);

    assert_eq!(array.value_offsets(), &[0, 5, 9, 12, 17, 21]);

    assert_eq!(array.value(0), "hello");
    assert_eq!(array.value(1), "from");
    assert_eq!(array.value(2), "the");
    assert_eq!(array.value(3), "other");
    assert_eq!(array.value(4), "side");
}

#[test]
fn strings2() {
    let array = {
        let mut builder = StringBuilder::with_capacity(5, 5 * 3);

        for string in &["hello", "from", "the", "other", "side"] {
            builder.append_value(string);
        }

        builder.finish()
    };

    assert_eq!(array.len(), 5);
    assert_eq!(array.null_count(), 0);

    assert_eq!(array.value_offsets(), &[0, 5, 9, 12, 17, 21]);

    assert_eq!(array.value_length(0), 5);
    assert_eq!(array.value_length(1), "from".len() as i32);

    assert_eq!(array.value(0), "hello");
    assert_eq!(array.value(1), "from");
    assert_eq!(array.value(2), "the");
    assert_eq!(array.value(3), "other");
    assert_eq!(array.value(4), "side");

    assert_eq!(array.value_data().as_slice(), b"hellofromtheotherside");
    assert_eq!(array.value_offsets(), &[0, 5, 9, 12, 17, 21]);
}

#[test]
fn list() {
    let array = {
        let mut builder = ListBuilder::new(Int32Builder::with_capacity(0));

        builder.values().append_value(0);
        builder.values().append_value(1);
        builder.append(true);
        builder.values().append_value(2);
        builder.values().append_value(3);
        builder.values().append_value(4);
        builder.append(true);

        builder.finish()
    };

    assert_eq!(array.len(), 2);
    assert_eq!(array.value_offsets(), &[0, 2, 5]);
    assert_eq!(array.value_length(0), 2);
    assert_eq!(array.value_length(1), 3);

    assert_eq!(
        array
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[0, 1, 2, 3, 4],
    );
    assert_eq!(array.data().buffers()[0].typed_data::<i32>(), &[0, 2, 5]); // its in buffer 0... kind of unstable...
}

#[test]
fn fixed_size_list() {
    let array = {
        let mut builder = FixedSizeListBuilder::with_capacity(Int32Builder::with_capacity(0), 2, 2);

        builder.values().append_value(0);
        builder.values().append_value(1);
        builder.append(true);
        builder.values().append_value(2);
        builder.values().append_value(3);
        builder.append(true);
        builder.values().append_value(4);
        builder.values().append_value(5);
        builder.append(true);

        builder.finish()
    };

    assert_eq!(array.len(), 3);
    assert_eq!(array.value_offset(0), 0);
    assert_eq!(array.value_offset(1), 2);
    assert_eq!(array.value_offset(2), 4);
    assert_eq!(array.value_length(), 2);

    assert_eq!(
        array
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[0, 1, 2, 3, 4, 5],
    );
}

#[test]
#[allow(clippy::cast_ptr_alignment, clippy::identity_op)]
fn binary() {
    let t1 = TimeInterval::new(0, 1).unwrap();
    let t2_bytes: [u8; 16] = unsafe { mem::transmute(t1) };
    let t2: TimeInterval = unsafe { mem::transmute(t2_bytes) };
    assert_eq!(t1, t2);

    let array = {
        let mut builder =
            FixedSizeBinaryBuilder::with_capacity(3, mem::size_of::<TimeInterval>() as i32);

        for &t in &[
            TimeInterval::new(0, 1).unwrap(),
            TimeInterval::new(1, 2).unwrap(),
            TimeInterval::new(2, 3).unwrap(),
        ] {
            let t_bytes: [u8; 16] = unsafe { mem::transmute(t) };
            builder.append_value(&t_bytes).unwrap();
        }

        builder.finish()
    };

    assert_eq!(array.len(), 3);
    assert_eq!(array.value_offset(0), 0);
    assert_eq!(
        array.value_offset(1),
        1 * mem::size_of::<TimeInterval>() as i32
    );
    assert_eq!(
        array.value_offset(2),
        2 * mem::size_of::<TimeInterval>() as i32
    );
    assert_eq!(array.value_length(), mem::size_of::<TimeInterval>() as i32);

    assert_eq!(
        unsafe { &*(array.value(0).as_ptr() as *const TimeInterval) },
        &TimeInterval::new(0, 1).unwrap(),
    );

    assert_eq!(
        unsafe {
            slice::from_raw_parts(
                array.value_data().as_ptr() as *const TimeInterval,
                array.len(),
            )
        },
        &[
            TimeInterval::new(0, 1).unwrap(),
            TimeInterval::new(1, 2).unwrap(),
            TimeInterval::new(2, 3).unwrap(),
        ]
    );
}

#[test]
fn serialize() {
    let array = {
        let mut builder = Int32Builder::with_capacity(5);
        builder.append_slice(&(1..=5).collect::<Vec<i32>>());

        builder.finish()
    };

    assert_eq!(array.len(), 5);

    // no serialization of arrays by now
    let json = serde_json::to_string(array.values()).unwrap();

    assert_eq!(json, "[1,2,3,4,5]");
}

#[test]
fn table() {
    let schema = vec![
        Field::new("feature_start", DataType::UInt64, false),
        Field::new("time_start", DataType::Date64, false),
    ];

    let array = {
        let mut builder = StructBuilder::from_fields(schema, 5);

        for &(feature_start, time) in &[(0_u64, 0_i64), (1, 10), (2, 20), (3, 30), (4, 40)] {
            builder
                .field_builder::<UInt64Builder>(0)
                .unwrap()
                .append_value(feature_start);
            builder
                .field_builder::<Date64Builder>(1)
                .unwrap()
                .append_value(time);
            builder.append(true);
        }

        builder.finish()
    };

    assert_eq!(array.len(), 5);
    assert_eq!(array.num_columns(), 2);
    assert_eq!(array.null_count(), 0);

    assert_eq!(
        array
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values(),
        &[0, 1, 2, 3, 4]
    );
    assert_eq!(
        array
            .column_by_name("time_start")
            .unwrap()
            .as_any()
            .downcast_ref::<Date64Array>()
            .unwrap()
            .values(),
        &[0, 10, 20, 30, 40]
    );
}

#[test]
fn nested_lists() {
    let array = {
        let mut builder = ListBuilder::new(ListBuilder::new(Int32Builder::with_capacity(0)));

        // [[[10, 11, 12], [20, 21]], [[30]]
        builder.values().values().append_slice(&[10, 11, 12]);
        builder.values().append(true);
        builder.values().values().append_slice(&[20, 21]);
        builder.values().append(true);
        builder.append(true);

        builder.values().values().append_slice(&[30]);
        builder.values().append(true);
        builder.append(true);

        builder.finish()
    };

    assert_eq!(array.len(), 2);
    assert_eq!(array.value_length(0), 2);
    assert_eq!(array.value_length(1), 1);
    assert_eq!(
        array
            .value(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value_length(0),
        3
    );
    assert_eq!(
        array
            .value(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value_length(1),
        2
    );
    assert_eq!(
        array
            .value(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value_length(0),
        1
    );

    assert_eq!(array.data().buffers().len(), 1);
    assert_eq!(
        array.data().buffers()[0].typed_data::<i32>(),
        &[0, 2, 3], // indices of first level arrays in second level structure
    );

    assert_eq!(array.data().child_data().len(), 1);
    assert_eq!(array.data().child_data()[0].buffers().len(), 1);
    assert_eq!(
        array.data().child_data()[0].buffers()[0].typed_data::<i32>(),
        &[0, 3, 5, 6], // indices of second level arrays in actual data
    );

    assert_eq!(array.data().child_data()[0].child_data().len(), 1);
    assert_eq!(
        array.data().child_data()[0].child_data()[0].buffers().len(),
        1,
    );
    assert_eq!(
        array.data().child_data()[0].child_data()[0].buffers()[0].typed_data::<i32>(),
        &[10, 11, 12, 20, 21, 30], // data
    );
}

#[test]
fn multipoints() {
    use arrow::datatypes::ToByteSlice;

    let array = {
        let data = ArrayData::builder(DataType::List(Box::new(Field::new(
            "",
            DataType::FixedSizeList(Box::new(Field::new("", DataType::Float64, false)), 2),
            false,
        ))))
        .len(2) // number of multipoints
        .add_buffer(Buffer::from(&[0_i32, 2, 5].to_byte_slice()))
        .add_child_data(
            ArrayData::builder(DataType::FixedSizeList(
                Box::new(Field::new("", DataType::Float64, false)),
                2,
            ))
            .len(5) // number of coordinates
            .add_child_data(
                ArrayData::builder(DataType::Float64)
                    .len(10) // number of floats
                    .add_buffer(Buffer::from(
                        &[
                            1_f64, 2., 11., 12., 21., 22., 31., 32., 41., 42., 51., 52., 61., 62.,
                            71., 72., 81., 82., 91., 92.,
                        ]
                        .to_byte_slice(),
                    ))
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap(),
        )
        .build()
        .unwrap();

        ListArray::from(data)
    };

    assert_eq!(array.len(), 2);
    assert_eq!(array.value_length(0), 2);
    assert_eq!(array.value_length(1), 3);

    let values = array.values();
    let subarray = values
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap()
        .values();
    let floats = subarray
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .values();
    assert_eq!(floats.len(), 10);
    let coordinates: &[Coordinate2D] =
        unsafe { slice::from_raw_parts(floats.as_ptr() as *const Coordinate2D, floats.len()) };

    assert_eq!(coordinates[4], Coordinate2D::new(41., 42.));
}

#[test]
#[allow(clippy::float_cmp)]
fn multipoint_builder() {
    let float_builder = arrow::array::Float64Builder::with_capacity(0);
    let coordinate_builder = arrow::array::FixedSizeListBuilder::new(float_builder, 2);
    let mut multi_point_builder = arrow::array::ListBuilder::new(coordinate_builder);

    multi_point_builder
        .values()
        .values()
        .append_slice(&[0.0, 0.1]);
    multi_point_builder.values().append(true);
    multi_point_builder
        .values()
        .values()
        .append_slice(&[1.0, 1.1]);
    multi_point_builder.values().append(true);

    multi_point_builder.append(true); // first multi point

    multi_point_builder
        .values()
        .values()
        .append_slice(&[2.0, 2.1]);
    multi_point_builder.values().append(true);
    multi_point_builder
        .values()
        .values()
        .append_slice(&[3.0, 3.1]);
    multi_point_builder.values().append(true);
    multi_point_builder
        .values()
        .values()
        .append_slice(&[4.0, 4.1]);
    multi_point_builder.values().append(true);

    multi_point_builder.append(true); // second multi point

    let multi_point = multi_point_builder.finish();

    let first_multi_point_ref = multi_point.value(0);
    let first_multi_point: &arrow::array::FixedSizeListArray =
        first_multi_point_ref.as_any().downcast_ref().unwrap();
    let coordinates_ref = first_multi_point.values();
    let coordinates: &Float64Array = coordinates_ref.as_any().downcast_ref().unwrap();

    assert_eq!(&coordinates.values()[0..2 * 2], &[0.0, 0.1, 1.0, 1.1]);

    let second_multi_point_ref = multi_point.value(1);
    let second_multi_point: &arrow::array::FixedSizeListArray =
        second_multi_point_ref.as_any().downcast_ref().unwrap();
    let coordinates_ref = second_multi_point.values();
    let _coordinates: &Float64Array = coordinates_ref.as_any().downcast_ref().unwrap();

    // assert_eq!(
    //     coordinates.value_slice(0, 2 * 3),
    //     &[2.0, 2.1, 3.0, 3.1, 4.0, 4.1]
    // );
}

#[test]
#[allow(clippy::cast_ptr_alignment)]
fn multipoint_builder_bytes() {
    use arrow::datatypes::ToByteSlice;

    let coordinate_builder = arrow::array::FixedSizeBinaryBuilder::with_capacity(
        0,
        std::mem::size_of::<[f64; 2]>() as i32,
    );
    let mut multi_point_builder = arrow::array::ListBuilder::new(coordinate_builder);

    multi_point_builder
        .values()
        .append_value(&[0.0, 0.1].to_byte_slice())
        .unwrap();
    multi_point_builder
        .values()
        .append_value(&[1.0, 1.1].to_byte_slice())
        .unwrap();

    multi_point_builder.append(true); // first multi point

    multi_point_builder
        .values()
        .append_value(&[2.0, 2.1].to_byte_slice())
        .unwrap();
    multi_point_builder
        .values()
        .append_value(&[3.0, 3.1].to_byte_slice())
        .unwrap();
    multi_point_builder
        .values()
        .append_value(&[4.0, 4.1].to_byte_slice())
        .unwrap();

    multi_point_builder.append(true); // second multi point

    let multi_point = multi_point_builder.finish();

    let first_multi_point_ref = multi_point.value(0);
    let first_multi_point: &arrow::array::FixedSizeBinaryArray =
        first_multi_point_ref.as_any().downcast_ref().unwrap();

    let floats: &[Coordinate2D] = unsafe {
        std::slice::from_raw_parts(
            first_multi_point.value(0).as_ptr() as *const _,
            first_multi_point.len(),
        )
    };
    assert_eq!(floats, &[(0.0, 0.1).into(), (1.0, 1.1).into()]);

    let second_multi_point_ref = multi_point.value(1);
    let second_multi_point: &arrow::array::FixedSizeBinaryArray =
        second_multi_point_ref.as_any().downcast_ref().unwrap();

    let floats: &[Coordinate2D] = unsafe {
        std::slice::from_raw_parts(
            second_multi_point.value(0).as_ptr() as *const _,
            second_multi_point.len(),
        )
    };
    assert_eq!(
        floats,
        &[(2.0, 2.1).into(), (3.0, 3.1).into(), (4.0, 4.1).into()]
    );
}

#[test]
fn float_equality() {
    let mut floats = Float64Builder::with_capacity(3);
    floats.append_value(4.0);
    floats.append_null();
    floats.append_value(f64::NAN);

    let floats = floats.finish();

    assert_eq!(floats, floats);

    let mut floats2 = Float64Builder::with_capacity(3);
    floats2.append_value(4.0);
    floats2.append_null();
    floats2.append_value(f64::NAN);

    let floats2 = floats2.finish();

    assert_eq!(floats, floats2);

    let mut floats3 = Float64Builder::with_capacity(3);
    floats3.append_value(f64::NAN);
    floats3.append_null();
    floats3.append_value(4.0);

    let floats3 = floats3.finish();

    assert_ne!(floats, floats3);
    assert_ne!(floats2, floats3);
}

#[test]
fn filter_example() {
    let a = Int32Array::from(vec![Some(1), Some(2), Some(3)]);

    // dbg!(&a);

    let b = filter(
        &a,
        &BooleanArray::from(vec![Some(true), Some(false), Some(true)]),
    )
    .unwrap();

    // dbg!(&b);

    assert_eq!(
        b.as_any().downcast_ref::<Int32Array>().unwrap(),
        &Int32Array::from(vec![Some(1), Some(3)])
    );

    let c = Int32Array::from(vec![Some(1), Some(2), None]);

    // dbg!(&c);

    let d = filter(
        &c,
        &BooleanArray::from(vec![Some(true), Some(false), Some(true)]),
    )
    .unwrap();

    // dbg!(&d);

    assert_eq!(
        d.as_any().downcast_ref::<Int32Array>().unwrap(),
        &Int32Array::from(vec![Some(1), None])
    );
}

#[test]
fn gt_eq_example() {
    let a = Int32Array::from(vec![Some(1), Some(2), None]);

    // dbg!(&a);

    let b = gt_eq_scalar(&a, 2).unwrap();

    // dbg!(&b);

    assert_eq!(&b, &BooleanArray::from(vec![Some(false), Some(true), None]));
}
