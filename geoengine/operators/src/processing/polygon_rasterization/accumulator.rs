//! Per-pixel states that combine the values of all polygons covering a pixel.

use crate::util::statistics::{SafePSquareQuantileEstimator, incremental_mean};

/// Combines the values of all features that cover a pixel.
///
/// A pixel whose state is `Default` was not covered by any feature and becomes no-data.
pub trait PixelAccumulator: Send + Sync {
    type State: Default + Send;

    /// Adds the `value` of a feature that covers the pixel.
    fn add(&self, state: &mut Self::State, value: f64);

    /// The output value of the pixel, `None` if it is no-data.
    fn finish(&self, state: &Self::State) -> Option<f64>;
}

/// Burns a constant value into every covered pixel.
pub struct ConstantAccumulator(pub f64);

impl PixelAccumulator for ConstantAccumulator {
    type State = bool;

    fn add(&self, state: &mut bool, _value: f64) {
        *state = true;
    }

    fn finish(&self, state: &bool) -> Option<f64> {
        state.then_some(self.0)
    }
}

/// Counts the features that cover a pixel.
pub struct CountAccumulator;

impl PixelAccumulator for CountAccumulator {
    type State = u32;

    fn add(&self, state: &mut u32, _value: f64) {
        *state = state.saturating_add(1);
    }

    fn finish(&self, state: &u32) -> Option<f64> {
        (*state > 0).then_some(f64::from(*state))
    }
}

/// Combines the values of the features by a binary operation on `f64`.
pub struct FoldAccumulator(pub fn(f64, f64) -> f64);

impl FoldAccumulator {
    pub fn first() -> Self {
        Self(|first, _| first)
    }

    pub fn last() -> Self {
        Self(|_, last| last)
    }

    pub fn min() -> Self {
        Self(f64::min)
    }

    pub fn max() -> Self {
        Self(f64::max)
    }

    pub fn sum() -> Self {
        Self(|a, b| a + b)
    }
}

impl PixelAccumulator for FoldAccumulator {
    type State = Option<f64>;

    fn add(&self, state: &mut Option<f64>, value: f64) {
        *state = Some(match *state {
            Some(current) => (self.0)(current, value),
            None => value,
        });
    }

    fn finish(&self, state: &Option<f64>) -> Option<f64> {
        *state
    }
}

/// The numerically stable incremental mean of the feature values.
pub struct MeanAccumulator;

impl PixelAccumulator for MeanAccumulator {
    type State = Option<(f64, usize)>;

    fn add(&self, state: &mut Self::State, value: f64) {
        *state = Some(incremental_mean(state.unwrap_or((0., 0)), value));
    }

    fn finish(&self, state: &Self::State) -> Option<f64> {
        state.map(|(mean, _count)| mean)
    }
}

/// The median of the feature values.
///
/// It is exact for up to five values and estimated by the P² algorithm for more.
/// The estimator is only allocated for covered pixels, because it is much larger than a value.
pub struct MedianAccumulator;

impl PixelAccumulator for MedianAccumulator {
    type State = Option<Box<SafePSquareQuantileEstimator<f64>>>;

    fn add(&self, state: &mut Self::State, value: f64) {
        match state {
            Some(estimator) => estimator.update(value),
            None => {
                // only fails for non-finite values, which are skipped before
                *state = SafePSquareQuantileEstimator::new(0.5, value)
                    .ok()
                    .map(Box::new);
            }
        }
    }

    fn finish(&self, state: &Self::State) -> Option<f64> {
        state
            .as_ref()
            .map(|estimator| estimator.quantile_estimate())
    }
}

/// Makes a pixel no-data as soon as a feature with a no-data value, i.e., `NaN`, covers it.
pub struct NoDataPropagatingAccumulator<A>(pub A);

impl<A: PixelAccumulator> PixelAccumulator for NoDataPropagatingAccumulator<A> {
    /// The state of the inner accumulator and whether the pixel is no-data
    type State = (A::State, bool);

    fn add(&self, (state, is_no_data): &mut Self::State, value: f64) {
        if value.is_nan() {
            *is_no_data = true;
        } else if !*is_no_data {
            self.0.add(state, value);
        }
    }

    fn finish(&self, (state, is_no_data): &Self::State) -> Option<f64> {
        if *is_no_data {
            None
        } else {
            self.0.finish(state)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aggregate<A: PixelAccumulator>(accumulator: &A, values: &[f64]) -> Option<f64> {
        let mut state = A::State::default();
        for &value in values {
            accumulator.add(&mut state, value);
        }
        accumulator.finish(&state)
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn it_aggregates_values() {
        let values = [3., 1., 4., 1., 5., 9., 2.];

        assert_eq!(aggregate(&ConstantAccumulator(7.), &values), Some(7.));
        assert_eq!(aggregate(&CountAccumulator, &values), Some(7.));
        assert_eq!(aggregate(&FoldAccumulator::first(), &values), Some(3.));
        assert_eq!(aggregate(&FoldAccumulator::last(), &values), Some(2.));
        assert_eq!(aggregate(&FoldAccumulator::min(), &values), Some(1.));
        assert_eq!(aggregate(&FoldAccumulator::max(), &values), Some(9.));
        assert_eq!(aggregate(&FoldAccumulator::sum(), &values), Some(25.));
        let mean = aggregate(&MeanAccumulator, &values).unwrap();
        assert!((mean - 25. / 7.).abs() < 1e-12, "{mean}");
        assert_eq!(aggregate(&MedianAccumulator, &[3., 1., 4.]), Some(3.));
        assert_eq!(aggregate(&MedianAccumulator, &[3., 1., 4., 2.]), Some(2.5));
        assert!(aggregate(&MedianAccumulator, &values).is_some());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn it_propagates_no_data() {
        let accumulator = NoDataPropagatingAccumulator(FoldAccumulator::sum());

        assert_eq!(aggregate(&accumulator, &[1., 2.]), Some(3.));
        assert_eq!(aggregate(&accumulator, &[1., f64::NAN, 2.]), None);
        assert_eq!(aggregate(&accumulator, &[f64::NAN]), None);
    }

    #[test]
    fn it_outputs_no_data_without_values() {
        assert_eq!(aggregate(&ConstantAccumulator(7.), &[]), None);
        assert_eq!(aggregate(&CountAccumulator, &[]), None);
        assert_eq!(aggregate(&FoldAccumulator::sum(), &[]), None);
        assert_eq!(aggregate(&MeanAccumulator, &[]), None);
        assert_eq!(aggregate(&MedianAccumulator, &[]), None);
    }
}
