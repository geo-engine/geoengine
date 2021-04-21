use geoengine_datatypes::primitives::{FeatureData, FeatureDataType};
use geoengine_datatypes::raster::Pixel;
use num_traits::AsPrimitive;

/// Aggregating raster pixel values for features
pub trait Aggregator {
    type Output: Pixel;

    fn new(number_of_features: usize) -> Self;

    // TODO: add values for slice
    fn add_value<P>(&mut self, feature_idx: usize, pixel: P, weight: u64)
    where
        P: Pixel + AsPrimitive<Self::Output>;

    fn add_null(&mut self, feature_idx: usize);

    fn feature_data_type() -> FeatureDataType;

    fn data(&self) -> &[Self::Output];

    fn nulls(&self) -> &[bool];

    fn into_data(self) -> Vec<Option<Self::Output>>;

    fn into_typed(self) -> TypedAggregator;

    /// Whether an aggregator needs no more values for producing the outcome
    fn is_satisfied(&self) -> bool;
}

/// An aggregator wrapper for different return types
pub enum TypedAggregator {
    FirstValueFloat(FirstValueFloatAggregator),
    FirstValueInt(FirstValueIntAggregator),
    MeanNumber(MeanValueAggregator),
}

impl TypedAggregator {
    pub fn add_value<P>(&mut self, feature_idx: usize, pixel: P, weight: u64)
    where
        P: Pixel + AsPrimitive<f64> + AsPrimitive<i64>,
    {
        match self {
            TypedAggregator::FirstValueFloat(aggregator) => {
                aggregator.add_value(feature_idx, pixel, weight)
            }
            TypedAggregator::FirstValueInt(aggregator) => {
                aggregator.add_value(feature_idx, pixel, weight)
            }
            TypedAggregator::MeanNumber(aggregator) => {
                aggregator.add_value(feature_idx, pixel, weight)
            }
        }
    }

    pub fn add_null(&mut self, feature_idx: usize) {
        match self {
            TypedAggregator::FirstValueFloat(aggregator) => aggregator.add_null(feature_idx),
            TypedAggregator::FirstValueInt(aggregator) => aggregator.add_null(feature_idx),
            TypedAggregator::MeanNumber(aggregator) => aggregator.add_null(feature_idx),
        }
    }

    pub fn into_data(self) -> FeatureData {
        match self {
            TypedAggregator::FirstValueFloat(aggregator) => {
                FeatureData::NullableFloat(aggregator.into_data())
            }
            TypedAggregator::FirstValueInt(aggregator) => {
                FeatureData::NullableInt(aggregator.into_data())
            }
            TypedAggregator::MeanNumber(aggregator) => {
                FeatureData::NullableFloat(aggregator.into_data())
            }
        }
    }

    #[allow(dead_code)]
    pub fn nulls(&self) -> &[bool] {
        match self {
            TypedAggregator::FirstValueFloat(aggregator) => aggregator.nulls(),
            TypedAggregator::FirstValueInt(aggregator) => aggregator.nulls(),
            TypedAggregator::MeanNumber(aggregator) => aggregator.nulls(),
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
    values: Vec<T>,
    pristine: Vec<bool>,
    null: Vec<bool>,
    number_of_pristine_values: usize,
    number_of_non_null_values: usize,
}

impl<T> Aggregator for FirstValueAggregator<T>
where
    T: Pixel + FirstValueOutputType,
{
    type Output = T;

    fn new(number_of_features: usize) -> Self {
        Self {
            values: vec![T::zero(); number_of_features],
            pristine: vec![true; number_of_features],
            null: vec![false; number_of_features],
            number_of_pristine_values: number_of_features,
            number_of_non_null_values: number_of_features,
        }
    }

    fn add_value<P>(&mut self, feature_idx: usize, pixel: P, _weight: u64)
    where
        P: Pixel + AsPrimitive<Self::Output>,
    {
        if self.null[feature_idx] {
            return;
        }

        if self.pristine[feature_idx] {
            self.values[feature_idx] = pixel.as_();
            self.pristine[feature_idx] = false;
            self.number_of_pristine_values -= 1;
        }
    }

    fn add_null(&mut self, feature_idx: usize) {
        if !self.null[feature_idx] {
            self.null[feature_idx] = true;
            self.number_of_non_null_values -= 1;

            self.pristine[feature_idx] = false;
            self.number_of_pristine_values -= 1;
        }
    }

    fn feature_data_type() -> FeatureDataType {
        T::feature_data_type()
    }

    fn data(&self) -> &[Self::Output] {
        &self.values
    }

    fn nulls(&self) -> &[bool] {
        &self.null
    }

    fn into_data(self) -> Vec<Option<Self::Output>> {
        self.values
            .into_iter()
            .zip(self.null)
            .map(|(value, is_null)| if is_null { None } else { Some(value) })
            .collect()
    }

    fn into_typed(self) -> TypedAggregator {
        T::typed_aggregator(self)
    }

    fn is_satisfied(&self) -> bool {
        self.number_of_pristine_values == 0 || self.number_of_non_null_values == 0
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
    means: Vec<f64>,
    sum_weights: Vec<f64>,
    null: Vec<bool>,
    number_of_non_null_values: usize,
}

impl Aggregator for MeanValueAggregator {
    type Output = f64;

    fn new(number_of_features: usize) -> Self {
        Self {
            means: vec![0.; number_of_features],
            sum_weights: vec![0.; number_of_features],
            null: vec![false; number_of_features],
            number_of_non_null_values: number_of_features,
        }
    }

    fn add_value<P>(&mut self, feature_idx: usize, pixel: P, weight: u64)
    where
        P: Pixel + AsPrimitive<Self::Output>,
    {
        debug_assert!(weight > 0, "weights must be positive and non-zero");

        if self.null[feature_idx] {
            return;
        }

        let value: f64 = pixel.as_();
        let weight: f64 = weight.as_();

        let old_mean = self.means[feature_idx];
        let old_normalized_weight = self.sum_weights[feature_idx] / weight;

        self.sum_weights[feature_idx] += weight;
        self.means[feature_idx] += (value - old_mean) / (old_normalized_weight + 1.);
    }

    fn add_null(&mut self, feature_idx: usize) {
        if !self.null[feature_idx] {
            self.null[feature_idx] = true;
            self.number_of_non_null_values -= 1;
        }
    }

    fn feature_data_type() -> FeatureDataType {
        FeatureDataType::Float
    }

    fn data(&self) -> &[Self::Output] {
        &self.means
    }

    fn nulls(&self) -> &[bool] {
        &self.null
    }

    fn into_data(self) -> Vec<Option<Self::Output>> {
        self.means
            .into_iter()
            .zip(self.null)
            .map(|(value, is_null)| if is_null { None } else { Some(value) })
            .collect()
    }

    fn into_typed(self) -> TypedAggregator {
        TypedAggregator::MeanNumber(self)
    }

    fn is_satisfied(&self) -> bool {
        self.number_of_non_null_values == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::float_cmp)]
    fn fist_value_f64() {
        let mut aggregator = FirstValueFloatAggregator::new(2);

        aggregator.add_value(0, 1, 1);
        aggregator.add_value(0, 2, 1);

        aggregator.add_value(1, 10, 1);

        assert_eq!(aggregator.data(), &[1., 10.]);
    }

    #[test]
    fn fist_value_i64() {
        let mut aggregator = FirstValueIntAggregator::new(2);

        aggregator.add_value(0, 2., 1);
        aggregator.add_value(0, 0., 1);

        aggregator.add_value(1, 4., 1);

        assert_eq!(aggregator.data(), &[2, 4]);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn mean() {
        let mut aggregator = MeanValueAggregator::new(2);

        for i in 1..=10 {
            aggregator.add_value(0, i, 1);
            aggregator.add_value(1, i, i);
        }

        assert_eq!(aggregator.data(), &[5.5, 385. / 55.]);
    }

    #[test]
    fn typed() {
        let mut aggregator = FirstValueIntAggregator::new(2).into_typed();

        aggregator.add_value(0, 2., 1);
        aggregator.add_value(0, 0., 1);

        aggregator.add_value(1, 4., 1);

        if let TypedAggregator::FirstValueInt(ref aggregator) = aggregator {
            assert_eq!(aggregator.data(), &[2, 4]);
        } else {
            unreachable!();
        }

        assert_eq!(
            aggregator.into_data(),
            FeatureData::NullableInt(vec![Some(2), Some(4)])
        );
    }

    #[test]
    fn satisfaction() {
        let mut aggregator = FirstValueIntAggregator::new(2).into_typed();

        assert!(!aggregator.is_satisfied());

        aggregator.add_value(0, 2., 1);

        assert!(!aggregator.is_satisfied());

        aggregator.add_value(1, 0., 1);

        assert!(aggregator.is_satisfied());

        aggregator.add_value(1, 4., 1);

        assert!(aggregator.is_satisfied());
    }

    #[test]
    fn nulls() {
        let mut aggregator = FirstValueIntAggregator::new(2).into_typed();

        assert!(!aggregator.is_satisfied());

        aggregator.add_null(0);
        aggregator.add_null(1);

        assert!(aggregator.is_satisfied());

        assert_eq!(aggregator.nulls(), &[true, true]);
    }
}
