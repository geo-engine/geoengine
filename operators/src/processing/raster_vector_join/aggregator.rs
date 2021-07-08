use geoengine_datatypes::primitives::FeatureDataType;
use geoengine_datatypes::raster::{Pixel, RasterDataType};
use num_traits::AsPrimitive;

use super::FeatureAggregationMethod;

/// Aggregating raster pixel values
pub trait Aggregator {
    type Output: Pixel;

    fn new() -> Self;

    fn add_value<P>(&mut self, pixel: P, weight: u64)
    where
        P: Pixel + AsPrimitive<Self::Output>;

    fn add_null(&mut self);

    fn feature_data_type() -> FeatureDataType;

    fn data(&self) -> Self::Output;

    fn null(&self) -> bool;

    fn into_data(self) -> Option<Self::Output>;

    fn into_typed(self) -> TypedAggregator;

    /// Whether an aggregator needs no more values for producing the outcome
    fn is_satisfied(&self) -> bool;
}

pub fn create_aggregator<P: Pixel>(aggregation: FeatureAggregationMethod) -> TypedAggregator {
    match aggregation {
        FeatureAggregationMethod::First => match P::TYPE {
            RasterDataType::U8
            | RasterDataType::U16
            | RasterDataType::U32
            | RasterDataType::U64
            | RasterDataType::I8
            | RasterDataType::I16
            | RasterDataType::I32
            | RasterDataType::I64 => FirstValueIntAggregator::new().into_typed(),
            RasterDataType::F32 | RasterDataType::F64 => {
                FirstValueFloatAggregator::new().into_typed()
            }
        },
        FeatureAggregationMethod::Mean => MeanValueAggregator::new().into_typed(),
    }
}

/// An aggregator wrapper for different return types
pub enum TypedAggregator {
    FirstValueFloat(FirstValueFloatAggregator),
    FirstValueInt(FirstValueIntAggregator),
    MeanNumber(MeanValueAggregator),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggregationResult {
    Int(i64),
    Float(f64),
}

impl TypedAggregator {
    pub fn add_value<P>(&mut self, pixel: P, weight: u64)
    where
        P: Pixel + AsPrimitive<f64> + AsPrimitive<i64>,
    {
        match self {
            TypedAggregator::FirstValueFloat(aggregator) => aggregator.add_value(pixel, weight),
            TypedAggregator::FirstValueInt(aggregator) => aggregator.add_value(pixel, weight),
            TypedAggregator::MeanNumber(aggregator) => aggregator.add_value(pixel, weight),
        }
    }

    pub fn add_null(&mut self) {
        match self {
            TypedAggregator::FirstValueFloat(aggregator) => aggregator.add_null(),
            TypedAggregator::FirstValueInt(aggregator) => aggregator.add_null(),
            TypedAggregator::MeanNumber(aggregator) => aggregator.add_null(),
        }
    }

    pub fn result(self) -> Option<AggregationResult> {
        match self {
            TypedAggregator::FirstValueFloat(aggregator) => {
                aggregator.into_data().map(AggregationResult::Float)
            }
            TypedAggregator::FirstValueInt(aggregator) => {
                aggregator.into_data().map(AggregationResult::Int)
            }
            TypedAggregator::MeanNumber(aggregator) => {
                aggregator.into_data().map(AggregationResult::Float)
            }
        }
    }

    #[allow(dead_code)]
    pub fn null(&self) -> bool {
        match self {
            TypedAggregator::FirstValueFloat(aggregator) => aggregator.null(),
            TypedAggregator::FirstValueInt(aggregator) => aggregator.null(),
            TypedAggregator::MeanNumber(aggregator) => aggregator.null(),
        }
    }

    /// Whether an aggregator needs no more values for producing the outcome
    pub fn is_satisfied(&self) -> bool {
        match self {
            TypedAggregator::FirstValueFloat(aggregator) => aggregator.is_satisfied(),
            TypedAggregator::FirstValueInt(aggregator) => aggregator.is_satisfied(),
            TypedAggregator::MeanNumber(aggregator) => aggregator.is_satisfied(),
        }
    }
}

pub type FirstValueFloatAggregator = FirstValueAggregator<f64>;
pub type FirstValueIntAggregator = FirstValueAggregator<i64>;

/// Aggregation function that uses only the first value occurrence
pub struct FirstValueAggregator<T> {
    value: T,
    pristine: bool,
    null: bool,
}

impl<T> Aggregator for FirstValueAggregator<T>
where
    T: Pixel + FirstValueOutputType,
{
    type Output = T;

    fn new() -> Self {
        Self {
            value: T::zero(),
            pristine: true,
            null: false,
        }
    }

    fn add_value<P>(&mut self, pixel: P, _weight: u64)
    where
        P: Pixel + AsPrimitive<Self::Output>,
    {
        if self.null {
            return;
        }

        if self.pristine {
            self.value = pixel.as_();
            self.pristine = false;
        }
    }

    fn add_null(&mut self) {
        if !self.null {
            self.null = true;

            self.pristine = false;
        }
    }

    fn feature_data_type() -> FeatureDataType {
        T::feature_data_type()
    }

    fn data(&self) -> Self::Output {
        self.value
    }

    fn null(&self) -> bool {
        self.null
    }

    fn into_data(self) -> Option<Self::Output> {
        if self.null {
            None
        } else {
            Some(self.value)
        }
    }

    fn into_typed(self) -> TypedAggregator {
        T::typed_aggregator(self)
    }

    fn is_satisfied(&self) -> bool {
        !self.pristine || self.null
    }
}

pub trait FirstValueOutputType {
    fn feature_data_type() -> FeatureDataType;
    fn typed_aggregator(aggregator: FirstValueAggregator<Self>) -> TypedAggregator
    where
        Self: Sized;
}

impl FirstValueOutputType for i64 {
    fn feature_data_type() -> FeatureDataType {
        FeatureDataType::Int
    }

    fn typed_aggregator(aggregator: FirstValueAggregator<Self>) -> TypedAggregator {
        TypedAggregator::FirstValueInt(aggregator)
    }
}

impl FirstValueOutputType for f64 {
    fn feature_data_type() -> FeatureDataType {
        FeatureDataType::Float
    }

    fn typed_aggregator(aggregator: FirstValueAggregator<Self>) -> TypedAggregator {
        TypedAggregator::FirstValueFloat(aggregator)
    }
}

/// Aggregation function that calculates the weighted mean
pub struct MeanValueAggregator {
    mean: f64,
    sum_weight: f64,
    null: bool,
}

impl Aggregator for MeanValueAggregator {
    type Output = f64;

    fn new() -> Self {
        Self {
            mean: 0.,
            sum_weight: 0.,
            null: false,
        }
    }

    fn add_value<P>(&mut self, pixel: P, weight: u64)
    where
        P: Pixel + AsPrimitive<Self::Output>,
    {
        debug_assert!(weight > 0, "weights must be positive and non-zero");

        if self.null {
            return;
        }

        let value: f64 = pixel.as_();
        let weight: f64 = weight.as_();

        let old_mean = self.mean;
        let old_normalized_weight = self.sum_weight / weight;

        self.sum_weight += weight;
        self.mean += (value - old_mean) / (old_normalized_weight + 1.);
    }

    fn add_null(&mut self) {
        if !self.null {
            self.null = true;
        }
    }

    fn feature_data_type() -> FeatureDataType {
        FeatureDataType::Float
    }

    fn data(&self) -> Self::Output {
        self.mean
    }

    fn null(&self) -> bool {
        self.null
    }

    fn into_data(self) -> Option<Self::Output> {
        if self.null {
            None
        } else {
            Some(self.mean)
        }
    }

    fn into_typed(self) -> TypedAggregator {
        TypedAggregator::MeanNumber(self)
    }

    fn is_satisfied(&self) -> bool {
        !self.null
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::float_cmp)]
    fn fist_value_f64() {
        let mut aggregator = FirstValueFloatAggregator::new();

        aggregator.add_value(1, 1);
        aggregator.add_value(2, 1);

        assert_eq!(aggregator.data(), 1.);
    }

    #[test]
    fn fist_value_i64() {
        let mut aggregator = FirstValueIntAggregator::new();

        aggregator.add_value(2., 1);
        aggregator.add_value(0., 1);

        assert_eq!(aggregator.data(), 2);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn mean() {
        let mut aggregator = MeanValueAggregator::new();

        for i in 1..=10 {
            aggregator.add_value(i, 1);
        }

        assert_eq!(aggregator.data(), 5.5);
    }

    #[test]
    fn typed() {
        let mut aggregator = FirstValueIntAggregator::new().into_typed();

        aggregator.add_value(2., 1);
        aggregator.add_value(0., 1);

        if let TypedAggregator::FirstValueInt(ref aggregator) = aggregator {
            assert_eq!(aggregator.data(), 2);
        } else {
            unreachable!();
        }

        assert_eq!(aggregator.result(), Some(AggregationResult::Int(2)));
    }

    #[test]
    fn satisfaction() {
        let mut aggregator = FirstValueIntAggregator::new().into_typed();

        assert!(!aggregator.is_satisfied());

        aggregator.add_value(2., 1);

        assert!(aggregator.is_satisfied());
    }

    #[test]
    fn nulls() {
        let mut aggregator = FirstValueIntAggregator::new().into_typed();

        assert!(!aggregator.is_satisfied());

        aggregator.add_null();

        assert!(aggregator.is_satisfied());

        assert!(aggregator.null());
    }
}
