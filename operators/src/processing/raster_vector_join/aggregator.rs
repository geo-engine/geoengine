use geoengine_datatypes::primitives::{FeatureData, FeatureDataType};
use geoengine_datatypes::raster::Pixel;
use num_traits::AsPrimitive;

/// Aggregating raster pixel values for features
pub trait Aggregator {
    type Output: Pixel;

    fn new(number_of_features: usize) -> Self;

    // TODO: add values for slice
    // TODO: think about NODATA / nulls
    fn add_value<P>(&mut self, feature_idx: usize, pixel: P, weight: u64)
    where
        P: Pixel + AsPrimitive<Self::Output>;

    fn feature_data_type() -> FeatureDataType;

    fn data(&self) -> &[Self::Output];

    fn into_data(self) -> Vec<Self::Output>;

    fn into_typed(self) -> TypedAggregator;
}

/// An aggregator wrapper for different return types
pub enum TypedAggregator {
    FirstValueNumber(FirstValueNumberAggregator),
    FirstValueDecimal(FirstValueDecimalAggregator),
    MeanNumber(MeanValueAggregator),
}

impl TypedAggregator {
    pub fn add_value<P>(&mut self, feature_idx: usize, pixel: P, weight: u64)
    where
        P: Pixel + AsPrimitive<f64> + AsPrimitive<i64>,
    {
        match self {
            TypedAggregator::FirstValueNumber(aggregator) => {
                aggregator.add_value(feature_idx, pixel, weight)
            }
            TypedAggregator::FirstValueDecimal(aggregator) => {
                aggregator.add_value(feature_idx, pixel, weight)
            }
            TypedAggregator::MeanNumber(aggregator) => {
                aggregator.add_value(feature_idx, pixel, weight)
            }
        }
    }

    // pub fn feature_data_type(&self) -> FeatureDataType {
    //     match self {
    //         TypedAggregator::FirstValueNumber(_) => FirstValueNumberAggregator::feature_data_type(),
    //         TypedAggregator::FirstValueDecimal(_) => {
    //             FirstValueDecimalAggregator::feature_data_type()
    //         }
    //         TypedAggregator::MeanNumber(_) => MeanValueAggregator::feature_data_type(),
    //     }
    // }

    pub fn into_data(self) -> FeatureData {
        match self {
            TypedAggregator::FirstValueNumber(aggregator) => {
                FeatureData::Number(aggregator.into_data())
            }
            TypedAggregator::FirstValueDecimal(aggregator) => {
                FeatureData::Decimal(aggregator.into_data())
            }
            TypedAggregator::MeanNumber(aggregator) => FeatureData::Number(aggregator.into_data()),
        }
    }
}

pub type FirstValueNumberAggregator = FirstValueAggregator<f64>;
pub type FirstValueDecimalAggregator = FirstValueAggregator<i64>;

/// Aggregation function that uses only the first value occurrence
pub struct FirstValueAggregator<T> {
    values: Vec<T>,
    pristine: Vec<bool>,
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
        }
    }

    fn add_value<P>(&mut self, feature_idx: usize, pixel: P, _weight: u64)
    where
        P: Pixel + AsPrimitive<Self::Output>,
    {
        if self.pristine[feature_idx] {
            self.values[feature_idx] = pixel.as_();
            self.pristine[feature_idx] = false;
        }
    }

    fn feature_data_type() -> FeatureDataType {
        T::feature_data_type()
    }

    fn data(&self) -> &[Self::Output] {
        &self.values
    }

    fn into_data(self) -> Vec<Self::Output> {
        self.values
    }

    fn into_typed(self) -> TypedAggregator {
        T::typed_aggregator(self)
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
        FeatureDataType::Decimal
    }

    fn typed_aggregator(aggregator: FirstValueAggregator<Self>) -> TypedAggregator {
        TypedAggregator::FirstValueDecimal(aggregator)
    }
}

impl FirstValueOutputType for f64 {
    fn feature_data_type() -> FeatureDataType {
        FeatureDataType::Number
    }

    fn typed_aggregator(aggregator: FirstValueAggregator<Self>) -> TypedAggregator {
        TypedAggregator::FirstValueNumber(aggregator)
    }
}

/// Aggregation function that calculates the weighted mean
pub struct MeanValueAggregator {
    means: Vec<f64>,
    sum_weights: Vec<f64>,
}

impl Aggregator for MeanValueAggregator {
    type Output = f64;

    fn new(number_of_features: usize) -> Self {
        Self {
            means: vec![0.; number_of_features],
            sum_weights: vec![0.; number_of_features],
        }
    }

    fn add_value<P>(&mut self, feature_idx: usize, pixel: P, weight: u64)
    where
        P: Pixel + AsPrimitive<Self::Output>,
    {
        debug_assert!(weight > 0, "weights must be positive and non-zero");

        let value: f64 = pixel.as_();
        let weight: f64 = weight.as_();

        let old_mean = self.means[feature_idx];
        let old_normalized_weight = self.sum_weights[feature_idx] / weight;

        self.sum_weights[feature_idx] += weight;
        self.means[feature_idx] += (value - old_mean) / (old_normalized_weight + 1.);
    }

    fn feature_data_type() -> FeatureDataType {
        FeatureDataType::Number
    }

    fn data(&self) -> &[Self::Output] {
        &self.means
    }

    fn into_data(self) -> Vec<Self::Output> {
        self.means
    }

    fn into_typed(self) -> TypedAggregator {
        TypedAggregator::MeanNumber(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::float_cmp)]
    fn fist_value_f64() {
        let mut aggregator = FirstValueNumberAggregator::new(2);

        aggregator.add_value(0, 1, 1);
        aggregator.add_value(0, 2, 1);

        aggregator.add_value(1, 10, 1);

        assert_eq!(aggregator.data(), &[1., 10.]);
    }

    #[test]
    fn fist_value_i64() {
        let mut aggregator = FirstValueDecimalAggregator::new(2);

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
        let mut aggregator = FirstValueDecimalAggregator::new(2).into_typed();

        aggregator.add_value(0, 2., 1);
        aggregator.add_value(0, 0., 1);

        aggregator.add_value(1, 4., 1);

        if let TypedAggregator::FirstValueDecimal(ref aggregator) = aggregator {
            assert_eq!(aggregator.data(), &[2, 4]);
        } else {
            unreachable!();
        }

        assert_eq!(aggregator.into_data(), FeatureData::Decimal(vec![2, 4]));
    }
}
