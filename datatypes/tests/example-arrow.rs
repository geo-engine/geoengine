use arrow::array::{
    Array, ArrayData, Date64Array, Date64Builder, FixedSizeBinaryBuilder, FixedSizeListArray,
    FixedSizeListBuilder, Float64Array, Float64Builder, Int32Array, Int32Builder, ListArray,
    ListBuilder, StringArray, StringBuilder, StructBuilder, UInt64Array, UInt64Builder,
};
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::compute::kernels::filter::filter;
use arrow::datatypes::{DataType, DateUnit, Field, Schema, ToByteSlice};
use geoengine_datatypes::primitives::{Coordinate2D, TimeInterval};
use ocl::ProQue;
use std::{mem, slice};

#[test]
fn simple() {
    let mut primitive_array_builder = Int32Builder::new(5);
    primitive_array_builder.append_value(1).unwrap();
    primitive_array_builder.append_value(2).unwrap();
    primitive_array_builder
        .append_slice(&(3..=5).collect::<Vec<i32>>())
        .unwrap();

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
    let mut primitive_array_builder = Int32Builder::new(5);
    primitive_array_builder.append_value(1).unwrap();
    primitive_array_builder.append_null().unwrap();
    primitive_array_builder.append_slice(&[3, 4, 5]).unwrap();

    let primitive_array = primitive_array_builder.finish();

    assert_eq!(primitive_array.len(), 5);
    assert_eq!(primitive_array.null_count(), 1);

    //    eprintln!("{:?}", primitive_array);

    let buffer = primitive_array.values();

    let underlying_data = buffer.data();
    assert_eq!(underlying_data.len(), 5 * 4);

    //    eprintln!("{:?}", underlying_data);

    let casted_data = buffer.typed_data::<i32>();

    assert_eq!(casted_data.len(), 5);

    //    eprintln!("{:?}", casted_data);

    assert_eq!(&casted_data[0..1], &[1]);
    assert_eq!(&casted_data[2..5], &[3, 4, 5]);
}

#[test]
fn null_bytes() {
    let mut primitive_array_builder = Int32Builder::new(2);
    primitive_array_builder.append_value(1).unwrap();
    primitive_array_builder.append_null().unwrap();
    primitive_array_builder.append_option(None).unwrap();
    primitive_array_builder.append_option(Some(4)).unwrap();
    primitive_array_builder.append_null().unwrap();

    let primitive_array = primitive_array_builder.finish();

    assert_eq!(primitive_array.len(), 5);
    assert_eq!(primitive_array.null_count(), 3);

    if let Some(null_bitmap) = primitive_array.data().null_bitmap() {
        assert_eq!(null_bitmap.len(), 1);

        assert_eq!(
            null_bitmap.clone().to_buffer().data(), // must clone bitmap because there is no way to get a reference to the data
            &[0b00001001] // right most bit is first element, 1 = valid value, 0 = null or unset
        );
    }
}

#[test]
fn offset() {
    let array = {
        let mut array_builder = Float64Builder::new(5);
        array_builder
            .append_slice(&[2e10, 4e40, 20., 9.4, 0.])
            .unwrap();
        array_builder.finish()
    };

    assert_eq!(array.len(), 5);
    assert_eq!(array.offset(), 0);

    let subarray = array.slice(2, 2);
    let typed_subarray: &Float64Array = subarray.as_any().downcast_ref().unwrap();

    assert_eq!(subarray.len(), 2);
    assert_eq!(subarray.offset(), 2);
    assert_eq!(typed_subarray.values().typed_data::<f64>().len(), 5); // does NOT point to sub-slice

    assert_eq!(array.value_slice(2, 2), &[20., 9.4]);
}

#[test]
fn strings() {
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
            .build();

        StringArray::from(data)
    };

    assert_eq!(array.len(), 5);
    assert_eq!(array.null_count(), 0);

    assert_eq!(array.value_offset(0), 0);
    assert_eq!(array.value_offset(1), 5);
    assert_eq!(array.value_offset(2), "hellofrom".len() as i32);

    assert_eq!(array.value(0), "hello");
    assert_eq!(array.value(1), "from");
    assert_eq!(array.value(2), "the");
    assert_eq!(array.value(3), "other");
    assert_eq!(array.value(4), "side");
}

#[test]
fn strings2() {
    let array = {
        let mut builder = StringBuilder::new(5);

        for string in &["hello", "from", "the", "other", "side"] {
            builder.append_value(string).unwrap();
        }

        builder.finish()
    };

    assert_eq!(array.len(), 5);
    assert_eq!(array.null_count(), 0);

    assert_eq!(array.value_offset(0), 0);
    assert_eq!(array.value_offset(1), 5);
    assert_eq!(array.value_offset(2), "hellofrom".len() as i32);

    assert_eq!(array.value_length(0), 5);
    assert_eq!(array.value_length(1), "from".len() as i32);

    assert_eq!(array.value(0), "hello");
    assert_eq!(array.value(1), "from");
    assert_eq!(array.value(2), "the");
    assert_eq!(array.value(3), "other");
    assert_eq!(array.value(4), "side");

    assert_eq!(
        array.value_data().data(),
        "hellofromtheotherside".as_bytes()
    );
    assert_eq!(
        array.value_offsets().typed_data::<i32>(),
        &[0, 5, 9, 12, 17, 21]
    );
}

#[test]
fn list() {
    let array = {
        let mut builder = ListBuilder::new(Int32Builder::new(0));

        builder.values().append_value(0).unwrap();
        builder.values().append_value(1).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(2).unwrap();
        builder.values().append_value(3).unwrap();
        builder.values().append_value(4).unwrap();
        builder.append(true).unwrap();

        builder.finish()
    };

    assert_eq!(array.len(), 2);
    assert_eq!(array.value_offset(0), 0);
    assert_eq!(array.value_length(0), 2);
    assert_eq!(array.value_offset(1), 2);
    assert_eq!(array.value_length(1), 3);

    assert_eq!(
        array
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value_slice(0, 5),
        &[0, 1, 2, 3, 4],
    );
    assert_eq!(array.data().buffers()[0].typed_data::<i32>(), &[0, 2, 5]); // its in buffer 0... kind of unstable...
}

#[test]
fn fixed_size_list() {
    let array = {
        let mut builder = FixedSizeListBuilder::new(Int32Builder::new(0), 2);

        builder.values().append_value(0).unwrap();
        builder.values().append_value(1).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(2).unwrap();
        builder.values().append_value(3).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(4).unwrap();
        builder.values().append_value(5).unwrap();
        builder.append(true).unwrap();

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
            .value_slice(0, array.len() * array.value_length() as usize),
        &[0, 1, 2, 3, 4, 5],
    );
}

#[test]
fn binary() {
    let t1 = TimeInterval::new(0, 1).unwrap();
    let t2_bytes: [u8; 16] = unsafe { mem::transmute(t1.clone()) };
    let t2: TimeInterval = unsafe { mem::transmute(t2_bytes) };
    assert_eq!(t1, t2);

    let array = {
        let mut builder = FixedSizeBinaryBuilder::new(3, mem::size_of::<TimeInterval>() as i32);

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
                mem::transmute::<*const u8, *const TimeInterval>(array.value_data().raw_data()),
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
fn ocl() {
    let array = {
        let mut builder = Int32Builder::new(5);
        builder
            .append_slice(&(1..=5).collect::<Vec<i32>>())
            .unwrap();

        builder.finish()
    };

    assert_eq!(array.len(), 5);

    let src = r#"
        __kernel void add(__global int* buffer, int scalar) {
            buffer[get_global_id(0)] += scalar;
        }
    "#;

    let pro_que = ProQue::builder()
        .src(src)
        .dims(array.len())
        .build()
        .unwrap();

    let ocl_buffer = pro_que
        .buffer_builder()
        .copy_host_slice(array.value_slice(0, array.len()))
        .build()
        .unwrap();

    let kernel = pro_que
        .kernel_builder("add")
        .arg(&ocl_buffer)
        .arg(10_i32)
        .build()
        .unwrap();

    unsafe {
        kernel.enq().unwrap();
    }

    assert_eq!(ocl_buffer.len(), 5);

    let result = {
        let buffer = MutableBuffer::new(ocl_buffer.len() * mem::size_of::<i32>());
        let buffer_raw: &mut [i32] =
            unsafe { slice::from_raw_parts_mut(buffer.raw_data() as *mut i32, ocl_buffer.len()) };
        ocl_buffer.read(buffer_raw).enq().unwrap();

        let data = ArrayData::builder(DataType::Int32)
            .len(ocl_buffer.len())
            .add_buffer(buffer.freeze())
            .build();

        Int32Array::from(data)
    };

    assert_eq!(result.value_slice(0, result.len()), &[11, 12, 13, 14, 15]);
}

#[test]
fn serialize() {
    let array = {
        let mut builder = Int32Builder::new(5);
        builder
            .append_slice(&(1..=5).collect::<Vec<i32>>())
            .unwrap();

        builder.finish()
    };

    assert_eq!(array.len(), 5);

    // no serialization of arrays by now
    let json = serde_json::to_string(array.value_slice(0, array.len())).unwrap();

    assert_eq!(json, "[1,2,3,4,5]");
}

#[test]
fn table() {
    let schema = Schema::new(vec![
        Field::new("feature_start", DataType::UInt64, false),
        Field::new("time_start", DataType::Date64(DateUnit::Millisecond), false),
    ]);

    let array = {
        let mut builder = StructBuilder::from_schema(schema, 5);

        for &(feature_start, time) in &[(0_u64, 0_i64), (1, 10), (2, 20), (3, 30), (4, 40)] {
            builder
                .field_builder(0)
                .and_then(|builder: &mut UInt64Builder| builder.append_value(feature_start).ok())
                .unwrap();
            builder
                .field_builder(1)
                .and_then(|builder: &mut Date64Builder| builder.append_value(time).ok())
                .unwrap();
            builder.append(true).unwrap();
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
            .value_slice(0, array.len()),
        &[0, 1, 2, 3, 4]
    );
    assert_eq!(
        array
            .column_by_name("time_start")
            .unwrap()
            .as_any()
            .downcast_ref::<Date64Array>()
            .unwrap()
            .value_slice(0, array.len()),
        &[0, 10, 20, 30, 40]
    );
}

#[test]
fn nested_lists() {
    let array = {
        let mut builder = ListBuilder::new(ListBuilder::new(Int32Builder::new(0)));

        // [[[10, 11, 12], [20, 21]], [[30]]
        builder
            .values()
            .values()
            .append_slice(&[10, 11, 12])
            .unwrap();
        builder.values().append(true).unwrap();
        builder.values().values().append_slice(&[20, 21]).unwrap();
        builder.values().append(true).unwrap();
        builder.append(true).unwrap();

        builder.values().values().append_slice(&[30]).unwrap();
        builder.values().append(true).unwrap();
        builder.append(true).unwrap();

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
    let array = {
        let data = ArrayData::builder(DataType::List(
            DataType::FixedSizeList(DataType::Float64.into(), 2).into(),
        ))
        .len(2) // number of multipoints
        .add_buffer(Buffer::from(&[0_i32, 2, 5].to_byte_slice()))
        .add_child_data(
            ArrayData::builder(DataType::FixedSizeList(DataType::Float64.into(), 2))
                .len(5) // number of coordinates
                .add_child_data(
                    ArrayData::builder(DataType::Float64)
                        .len(10) // number of floats
                        .add_buffer(Buffer::from(
                            &[
                                1_f64, 2., 11., 12., 21., 22., 31., 32., 41., 42., 51., 52., 61.,
                                62., 71., 72., 81., 82., 91., 92.,
                            ]
                            .to_byte_slice(),
                        ))
                        .build(),
                )
                .build(),
        )
        .build();

        ListArray::from(data)
    };

    //    eprintln!("{:?}", array);

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
        .value_slice(0, 10);
    assert_eq!(floats.len(), 10);
    let coordinates: &[Coordinate2D] =
        unsafe { slice::from_raw_parts(floats.as_ptr() as *const Coordinate2D, floats.len()) };

    assert_eq!(coordinates[4], Coordinate2D::new(41., 42.));
}
