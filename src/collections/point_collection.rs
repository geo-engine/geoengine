use std::collections::HashMap;

use arrow::array::{
    Array, ArrayBuilder, ArrayData, ArrayRef, BooleanArray, Date64Array, Date64Builder,
    FixedSizeListArray, FixedSizeListBuilder, Float64Array, Float64Builder, ListArray, ListBuilder,
    StructArray, StructBuilder,
};
use arrow::compute::kernels::filter::filter;
use arrow::datatypes::DataType::Struct;
use arrow::datatypes::{DataType, DateUnit, Field};
use snafu::ensure;

use crate::collections::FeatureCollection;
use crate::error;
use crate::operations::Filterable;
use crate::primitives::{Coordinate, FeatureData, TimeInterval};
use crate::util::Result;
use std::slice;
use std::sync::Arc;

#[derive(Debug)]
pub struct PointCollection {
    data: StructArray,
}

impl Clone for PointCollection {
    /// Clone the PointCollection
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{PointCollection, FeatureCollection};
    ///
    /// let pc = PointCollection::empty();
    /// let cloned = pc.clone();
    ///
    /// assert_eq!(pc.len(), 0);
    /// assert_eq!(cloned.len(), 0);
    /// ```
    fn clone(&self) -> Self {
        Self {
            data: StructArray::from(self.data.data()),
        }
    }
}

impl PointCollection {
    /// Retrieve the composite arrow data type for multi points
    #[inline]
    fn multi_points_data_type() -> DataType {
        // TODO: use fixed size list with two floats instead of binary for representing `Coordinate`
        DataType::List(DataType::FixedSizeList((DataType::Float64.into(), 2)).into())
        // DataType::List(DataType::FixedSizeBinary(mem::size_of::<Coordinate>() as i32).into())
    }

    /// Retrieve the composite arrow data type for multi points
    #[inline]
    fn time_data_type() -> DataType {
        DataType::FixedSizeList((DataType::Date64(DateUnit::Millisecond).into(), 2))
    }

    /// Create an empty PointCollection.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{PointCollection, FeatureCollection};
    ///
    /// let pc = PointCollection::empty();
    ///
    /// assert_eq!(pc.len(), 0);
    /// ```
    pub fn empty() -> Self {
        Self {
            data: {
                let fields = vec![
                    Field::new(Self::FEATURE_FIELD, Self::multi_points_data_type(), false),
                    Field::new(Self::TIME_FIELD, Self::time_data_type(), false),
                ];

                StructArray::from(ArrayData::builder(DataType::Struct(fields)).len(0).build())
            },
        }
    }

    /// Create a point collection from data
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{PointCollection, FeatureCollection};
    /// use geoengine_datatypes::primitives::{Coordinate, TimeInterval, FeatureData};
    /// use std::collections::HashMap;
    ///
    /// let pc = PointCollection::from_data(
    ///     vec![vec![(0., 0.).into()], vec![(1., 1.).into()]],
    ///     vec![TimeInterval::new_unchecked(0, 1), TimeInterval::new_unchecked(0, 1)],
    ///     {
    ///         let mut map = HashMap::new();
    ///         map.insert("number".into(), FeatureData::Number(vec![0., 1.]));
    ///         map
    ///     },
    /// ).unwrap();
    ///
    /// assert_eq!(pc.len(), 2);
    /// ```
    pub fn from_data(
        coordinates: Vec<Vec<Coordinate>>,
        time_intervals: Vec<TimeInterval>,
        data: HashMap<String, FeatureData>,
    ) -> Result<Self> {
        let capacity = coordinates.len();

        let mut fields = vec![
            Field::new(Self::FEATURE_FIELD, Self::multi_points_data_type(), false),
            Field::new(Self::TIME_FIELD, Self::time_data_type(), false),
        ];

        let mut builders: Vec<Box<dyn ArrayBuilder>> = vec![
            Box::new({
                let mut builder =
                    ListBuilder::new(FixedSizeListBuilder::new(Float64Builder::new(2), 2));
                for multi_point in coordinates {
                    let coordinate_builder = builder.values();
                    for coordinate in multi_point {
                        let float_builder = coordinate_builder.values();
                        float_builder.append_value(coordinate.x)?;
                        float_builder.append_value(coordinate.y)?;
                        coordinate_builder.append(true)?;
                    }
                    builder.append(true)?;
                }

                builder
            }),
            Box::new({
                let mut builder = FixedSizeListBuilder::new(Date64Builder::new(capacity), 2);
                for time_interval in time_intervals {
                    let date_builder = builder.values();
                    date_builder.append_value(time_interval.start())?;
                    date_builder.append_value(time_interval.end())?;
                    builder.append(true)?;
                }

                builder
            }),
        ];

        for (name, feature_data) in data {
            ensure!(
                !Self::is_reserved_name(&name),
                error::FieldNameConflict { name }
            );

            let field = Field::new(
                &name,
                feature_data.arrow_data_type(),
                feature_data.nullable(),
            );

            fields.push(field);
            builders.push(feature_data.arrow_builder()?);
        }

        let mut struct_builder = StructBuilder::new(fields, builders);
        for _ in 0..capacity {
            struct_builder.append(true)?;
        }

        // TODO: performance improvements by creating the buffers directly and not using so many loops

        Ok(Self {
            data: struct_builder.finish(),
        })
    }

    /// Retrieves the coordinates of this point collection
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{PointCollection, FeatureCollection};
    /// use geoengine_datatypes::primitives::{Coordinate, TimeInterval, FeatureData};
    /// use std::collections::HashMap;
    ///
    /// let pc = PointCollection::from_data(
    ///     vec![vec![(0., 0.).into()], vec![(1., 1.).into()], vec![(2., 2.).into()]],
    ///     vec![TimeInterval::new_unchecked(0, 1), TimeInterval::new_unchecked(1, 2), TimeInterval::new_unchecked(2, 3)],
    ///     HashMap::new(),
    /// ).unwrap();
    ///
    /// assert_eq!(pc.len(), 3);
    ///
    /// let coords = pc.coordinates();
    ///
    /// assert_eq!(coords.len(), 3);
    /// assert_eq!(coords, &[(0., 0.).into(), (1., 1.).into(), (2., 2.).into()]);
    /// ```
    ///
    pub fn coordinates(&self) -> &[Coordinate] {
        let features_ref = self
            .data
            .column_by_name(Self::FEATURE_FIELD)
            .expect("There must exist a feature field");
        let features: &ListArray = features_ref.as_any().downcast_ref().unwrap();

        let feature_coordinates_ref = features.values();
        let feature_coordinates: &FixedSizeListArray =
            feature_coordinates_ref.as_any().downcast_ref().unwrap();

        let number_of_coordinates = feature_coordinates.data().len();

        let floats_ref = feature_coordinates.values();
        let floats: &Float64Array = floats_ref.as_any().downcast_ref().unwrap();

        unsafe {
            slice::from_raw_parts(
                floats.raw_values() as *const Coordinate,
                number_of_coordinates,
            )
        }
    }

    /// Retrieves the time intervals of this point collection
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{PointCollection, FeatureCollection};
    /// use geoengine_datatypes::primitives::{Coordinate, TimeInterval, FeatureData};
    /// use std::collections::HashMap;
    ///
    /// let pc = PointCollection::from_data(
    ///     vec![vec![(0., 0.).into()], vec![(1., 1.).into()], vec![(2., 2.).into()]],
    ///     vec![TimeInterval::new_unchecked(0, 1), TimeInterval::new_unchecked(1, 2), TimeInterval::new_unchecked(2, 3)],
    ///     HashMap::new(),
    /// ).unwrap();
    ///
    /// assert_eq!(pc.len(), 3);
    ///
    /// let time_intervals = pc.time_intervals();
    ///
    /// assert_eq!(time_intervals.len(), 3);
    /// assert_eq!(
    ///     time_intervals,
    ///     &[TimeInterval::new_unchecked(0, 1), TimeInterval::new_unchecked(1, 2), TimeInterval::new_unchecked(2, 3)]
    /// );
    /// ```
    ///
    pub fn time_intervals(&self) -> &[TimeInterval] {
        let features_ref = self
            .data
            .column_by_name(Self::TIME_FIELD)
            .expect("There must exist a time interval field");
        let features: &FixedSizeListArray = features_ref.as_any().downcast_ref().unwrap();

        let number_of_time_intervals = self.len();

        let timestamps_ref = features.values();
        let timestamps: &Date64Array = timestamps_ref.as_any().downcast_ref().unwrap();

        unsafe {
            slice::from_raw_parts(
                timestamps.raw_values() as *const TimeInterval,
                number_of_time_intervals,
            )
        }
    }
}

impl FeatureCollection for PointCollection {
    fn len(&self) -> usize {
        self.data.len()
    }

    fn is_simple(&self) -> bool {
        self.len() == self.coordinates().len()
    }
}

impl Filterable for PointCollection {
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{PointCollection, FeatureCollection};
    /// use geoengine_datatypes::primitives::{Coordinate, TimeInterval, FeatureData};
    /// use geoengine_datatypes::operations::Filterable;
    /// use std::collections::HashMap;
    ///
    /// let pc = PointCollection::from_data(
    ///     vec![vec![(0., 0.).into()], vec![(1., 1.).into()], vec![(2., 2.).into()]],
    ///     vec![TimeInterval::new_unchecked(0, 1), TimeInterval::new_unchecked(1, 2), TimeInterval::new_unchecked(2, 3)],
    ///     HashMap::new(),
    /// ).unwrap();
    ///
    /// assert_eq!(pc.len(), 3);
    ///
    /// let filtered = pc.filter(vec![false, true, false]).unwrap();
    ///
    /// assert_eq!(filtered.len(), 1);
    /// ```
    fn filter(&self, mask: Vec<bool>) -> Result<Self> {
        ensure!(
            mask.len() == self.data.len(),
            error::MaskLengthDoesNotMatchCollectionLength {
                mask_length: mask.len(),
                collection_length: self.data.len(),
            }
        );

        let filter_array: BooleanArray = mask.into();

        // TODO: use filter directly on struct array when it is implemented

        let filtered_data: Vec<(Field, ArrayRef)> =
            if let Struct(fields) = self.data.data().data_type() {
                let mut filtered_data: Vec<(Field, ArrayRef)> = Vec::with_capacity(fields.len());
                for (field, array) in fields.iter().zip(self.data.columns()) {
                    match field.name().as_str() {
                        Self::FEATURE_FIELD => filtered_data.push((
                            field.clone(),
                            Arc::new(coordinates_filter(
                                array.as_any().downcast_ref().unwrap(),
                                &filter_array,
                            )?),
                        )),
                        Self::TIME_FIELD => filtered_data.push((
                            field.clone(),
                            Arc::new(time_interval_filter(
                                array.as_any().downcast_ref().unwrap(),
                                &filter_array,
                            )?),
                        )),
                        _ => filtered_data
                            .push((field.clone(), filter(array.as_ref(), &filter_array)?)),
                    }
                }
                filtered_data
            } else {
                unreachable!("data field must be a struct")
            };

        Ok(Self {
            data: filtered_data.into(),
        })
    }
}

fn coordinates_filter(features: &ListArray, filter_array: &BooleanArray) -> Result<ListArray> {
    let mut new_features = ListBuilder::new(FixedSizeListBuilder::new(Float64Builder::new(2), 2));

    for feature_index in 0..features.len() {
        if filter_array.value(feature_index) {
            let coordinate_builder = new_features.values();

            let old_coordinates = features.value(feature_index);

            for coordinate_index in 0..features.value_length(feature_index) {
                let old_floats_array = old_coordinates
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .unwrap()
                    .value(coordinate_index as usize);

                let old_floats: &Float64Array = old_floats_array.as_any().downcast_ref().unwrap();

                let float_builder = coordinate_builder.values();
                float_builder.append_slice(old_floats.value_slice(0, 2))?;

                coordinate_builder.append(true)?;
            }

            new_features.append(true)?;
        }
    }

    Ok(new_features.finish())
}

fn time_interval_filter(
    time_intervals: &FixedSizeListArray,
    filter_array: &BooleanArray,
) -> Result<FixedSizeListArray> {
    let mut new_time_intervals = FixedSizeListBuilder::new(Date64Builder::new(2), 2);

    for feature_index in 0..time_intervals.len() {
        if !filter_array.value(feature_index) {
            continue;
        }

        let old_timestamps_ref = time_intervals.value(feature_index);
        let old_timestamps: &Date64Array = old_timestamps_ref.as_any().downcast_ref().unwrap();

        let date_builder = new_time_intervals.values();
        date_builder.append_slice(old_timestamps.value_slice(0, 2))?;

        new_time_intervals.append(true)?;
    }

    Ok(new_time_intervals.finish())
}
