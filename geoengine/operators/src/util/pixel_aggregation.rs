//! Per-pixel aggregation of a sequence of values, e.g., of a time series of tiles or of the
//! polygons that cover a pixel.

use crate::util::{
    Result,
    statistics::{SafePSquareQuantileEstimator, incremental_mean},
};
use geoengine_datatypes::raster::{Grid2D, GridOrEmpty2D, GridShape2D, MaskedGrid2D, Pixel};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::marker::PhantomData;

/// Aggregates the values of a pixel into a state and computes the output value from it.
///
/// The aggregators ignore no-data values (`None`), unless they are wrapped in [`PropagateNoData`].
pub trait PixelAggregator<P: Pixel>: Send + Sync {
    /// The state of a pixel.
    /// The `Default` state means that no value was added yet, and it results in no-data.
    type State: Default + Clone + Send + Sync;

    /// Adds a value to the state of a pixel.
    fn add(&self, state: &mut Self::State, value: Option<P>);

    /// The output value of a pixel, `None` if it is no-data.
    fn finish(&self, state: &Self::State) -> Option<P>;

    /// Whether adding no-data leaves every state unchanged, i.e., no-data can be skipped.
    fn ignores_no_data(&self) -> bool {
        true
    }
}

/// Combines two values into one.
pub trait BinaryOperation<P: Pixel>: Send + Sync {
    /// The state of the first value
    fn unit(value: P) -> P {
        value
    }

    fn op(state: P, value: P) -> P;
}

/// Folds the values of a pixel with a [`BinaryOperation`].
pub struct Fold<Op>(PhantomData<Op>);

impl<Op> Default for Fold<Op> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<P: Pixel, Op: BinaryOperation<P>> PixelAggregator<P> for Fold<Op> {
    type State = Option<P>;

    fn add(&self, state: &mut Option<P>, value: Option<P>) {
        let Some(value) = value else {
            return;
        };

        *state = Some(match *state {
            Some(state) => Op::op(state, value),
            None => Op::unit(value),
        });
    }

    fn finish(&self, state: &Option<P>) -> Option<P> {
        *state
    }
}

/// The saturating sum of the values
pub struct Sum;

impl<P: Pixel> BinaryOperation<P> for Sum {
    fn op(state: P, value: P) -> P {
        state.saturating_add(value)
    }
}

/// The (saturating) number of values
pub struct Count;

impl<P: Pixel> BinaryOperation<P> for Count {
    fn unit(_value: P) -> P {
        P::one()
    }

    fn op(state: P, _value: P) -> P {
        state.saturating_add(P::one())
    }
}

/// The minimum of the values
pub struct Min;

impl<P: Pixel> BinaryOperation<P> for Min {
    fn op(state: P, value: P) -> P {
        if state < value { state } else { value }
    }
}

/// The maximum of the values
pub struct Max;

impl<P: Pixel> BinaryOperation<P> for Max {
    fn op(state: P, value: P) -> P {
        if state > value { state } else { value }
    }
}

/// The first value
pub struct First;

impl<P: Pixel> BinaryOperation<P> for First {
    fn op(state: P, _value: P) -> P {
        state
    }
}

/// The last value
pub struct Last;

impl<P: Pixel> BinaryOperation<P> for Last {
    fn op(_state: P, value: P) -> P {
        value
    }
}

pub type SumAggregator = Fold<Sum>;
pub type CountAggregator = Fold<Count>;
pub type MinAggregator = Fold<Min>;
pub type MaxAggregator = Fold<Max>;
pub type FirstAggregator = Fold<First>;
pub type LastAggregator = Fold<Last>;

/// Outputs a constant value for every pixel that got a value, regardless of the values
pub struct ConstantAggregator<P>(pub P);

impl<P: Pixel> PixelAggregator<P> for ConstantAggregator<P> {
    /// Whether the pixel got a value
    type State = bool;

    fn add(&self, state: &mut bool, value: Option<P>) {
        *state |= value.is_some();
    }

    fn finish(&self, state: &bool) -> Option<P> {
        state.then_some(self.0)
    }
}

/// The numerically stable incremental mean of the values
#[derive(Default)]
pub struct MeanAggregator;

impl<P: Pixel> PixelAggregator<P> for MeanAggregator {
    type State = Option<(f64, usize)>;

    fn add(&self, state: &mut Self::State, value: Option<P>) {
        if let Some(value) = value {
            *state = Some(incremental_mean(state.unwrap_or((0., 0)), value.as_()));
        }
    }

    fn finish(&self, state: &Self::State) -> Option<P> {
        state.map(|(mean, _count)| P::from_(mean))
    }
}

/// A percentile of the values.
///
/// It is exact for up to five values and estimated by the P² algorithm for more.
pub struct PercentileEstimateAggregator {
    percentile: f64,
}

impl PercentileEstimateAggregator {
    pub fn new(percentile: f64) -> Self {
        Self { percentile }
    }
}

impl<P: Pixel> PixelAggregator<P> for PercentileEstimateAggregator {
    type State = Option<SafePSquareQuantileEstimator<P>>;

    fn add(&self, state: &mut Self::State, value: Option<P>) {
        let Some(value) = value else {
            return;
        };

        match state {
            Some(estimator) => estimator.update(value),
            // only fails for an invalid percentile or a non-finite value, which are skipped
            None => *state = SafePSquareQuantileEstimator::new(self.percentile, value).ok(),
        }
    }

    fn finish(&self, state: &Self::State) -> Option<P> {
        state
            .as_ref()
            .map(|estimator| P::from_(estimator.quantile_estimate()))
    }
}

/// Makes a pixel no-data as soon as a no-data value is added to it.
pub struct PropagateNoData<A>(pub A);

impl<P: Pixel, A: PixelAggregator<P>> PixelAggregator<P> for PropagateNoData<A> {
    /// The state of the inner aggregator and whether the pixel is no-data
    type State = (A::State, bool);

    fn add(&self, (state, is_no_data): &mut Self::State, value: Option<P>) {
        if *is_no_data {
            return;
        }

        if value.is_some() {
            self.0.add(state, value);
        } else {
            *is_no_data = true;
        }
    }

    fn finish(&self, (state, is_no_data): &Self::State) -> Option<P> {
        if *is_no_data {
            None
        } else {
            self.0.finish(state)
        }
    }

    fn ignores_no_data(&self) -> bool {
        false
    }
}

/// Allocates the state of the inner aggregator on the heap when the first value is added.
///
/// This saves memory if the state is large and most pixels get no value at all, e.g., when
/// rasterizing polygons that cover only a few pixels of a tile.
/// If most pixels get a value, it only adds allocations and indirection.
pub struct Boxed<A>(pub A);

impl<P: Pixel, A: PixelAggregator<P>> PixelAggregator<P> for Boxed<A> {
    type State = Option<Box<A::State>>;

    fn add(&self, state: &mut Self::State, value: Option<P>) {
        if value.is_none() && self.0.ignores_no_data() {
            return;
        }

        self.0.add(state.get_or_insert_default(), value);
    }

    fn finish(&self, state: &Self::State) -> Option<P> {
        state.as_ref().and_then(|state| self.0.finish(state))
    }

    fn ignores_no_data(&self) -> bool {
        self.0.ignores_no_data()
    }
}

/// Computes the output values of all `states` in parallel on the current thread pool.
pub fn finish_states<P: Pixel, A: PixelAggregator<P>>(
    aggregator: &A,
    states: &[A::State],
) -> Vec<Option<P>> {
    states
        .par_iter()
        .map(|state| aggregator.finish(state))
        .collect()
}

/// Converts pixel values into a grid of `shape`, masking `None` as no-data.
pub fn values_to_grid<T: Pixel>(
    shape: GridShape2D,
    values: &[Option<T>],
) -> Result<GridOrEmpty2D<T>> {
    if values.iter().all(Option::is_none) {
        return Ok(GridOrEmpty2D::new_empty_shape(shape));
    }

    let data = values
        .iter()
        .map(|value| value.unwrap_or_else(T::zero))
        .collect();
    let mask = values.iter().map(Option::is_some).collect();

    Ok(MaskedGrid2D::new(Grid2D::new(shape, data)?, Grid2D::new(shape, mask)?)?.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aggregate<P: Pixel, A: PixelAggregator<P>>(
        aggregator: &A,
        values: &[Option<P>],
    ) -> Option<P> {
        let mut state = A::State::default();
        for &value in values {
            aggregator.add(&mut state, value);
        }
        aggregator.finish(&state)
    }

    fn some<P: Pixel>(values: &[P]) -> Vec<Option<P>> {
        values.iter().copied().map(Some).collect()
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn it_aggregates_values() {
        let values = some(&[3_f64, 1., 4., 1., 5., 9., 2.]);

        assert_eq!(aggregate(&ConstantAggregator(42.), &values), Some(42.));
        assert_eq!(aggregate(&CountAggregator::default(), &values), Some(7.));
        assert_eq!(aggregate(&FirstAggregator::default(), &values), Some(3.));
        assert_eq!(aggregate(&LastAggregator::default(), &values), Some(2.));
        assert_eq!(aggregate(&MinAggregator::default(), &values), Some(1.));
        assert_eq!(aggregate(&MaxAggregator::default(), &values), Some(9.));
        assert_eq!(aggregate(&SumAggregator::default(), &values), Some(25.));
        let mean = aggregate(&MeanAggregator, &values).unwrap();
        assert!((mean - 25. / 7.).abs() < 1e-12, "{mean}");

        let median = PercentileEstimateAggregator::new(0.5);
        assert_eq!(aggregate(&median, &some(&[3., 1., 4.])), Some(3.));
        assert_eq!(aggregate(&median, &some(&[3., 1., 4., 2.])), Some(2.5));
        assert!(aggregate(&median, &values).is_some());
        assert_eq!(
            aggregate(&Boxed(median), &some(&[3., 1., 4., 2.])),
            Some(2.5)
        );
    }

    #[test]
    fn it_saturates_integers() {
        assert_eq!(
            aggregate(&SumAggregator::default(), &some(&[200_u8, 100])),
            Some(255)
        );

        let counted = vec![Some(1_u8); 300];
        assert_eq!(aggregate(&CountAggregator::default(), &counted), Some(255));
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn it_ignores_no_data() {
        let values = [Some(1.), None, Some(2.)];

        assert_eq!(aggregate(&ConstantAggregator(42.), &values), Some(42.));
        assert_eq!(aggregate(&ConstantAggregator(42.), &[None]), None);
        assert_eq!(aggregate(&SumAggregator::default(), &values), Some(3.));
        assert_eq!(aggregate(&CountAggregator::default(), &values), Some(2.));
        assert_eq!(aggregate(&FirstAggregator::default(), &values), Some(1.));
        assert_eq!(aggregate(&LastAggregator::default(), &values), Some(2.));
        assert_eq!(
            aggregate::<f64, _>(&SumAggregator::default(), &[None, None]),
            None
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn it_propagates_no_data() {
        let aggregator = PropagateNoData(SumAggregator::default());

        assert_eq!(aggregate(&aggregator, &some(&[1., 2.])), Some(3.));
        assert_eq!(aggregate(&aggregator, &[Some(1.), None, Some(2.)]), None);
        assert_eq!(aggregate::<f64, _>(&aggregator, &[None]), None);
        assert!(!PixelAggregator::<f64>::ignores_no_data(&aggregator));

        let boxed = Boxed(PropagateNoData(PercentileEstimateAggregator::new(0.5)));
        assert_eq!(aggregate(&boxed, &[Some(1.), None, Some(2.)]), None);
        assert!(!PixelAggregator::<f64>::ignores_no_data(&boxed));
    }

    #[test]
    fn it_outputs_no_data_without_values() {
        let no_values: [Option<f64>; 0] = [];

        assert_eq!(aggregate(&ConstantAggregator(42.), &no_values), None);

        assert_eq!(aggregate(&CountAggregator::default(), &no_values), None);
        assert_eq!(aggregate(&SumAggregator::default(), &no_values), None);
        assert_eq!(aggregate(&MeanAggregator, &no_values), None);
        assert_eq!(
            aggregate(&PercentileEstimateAggregator::new(0.5), &no_values),
            None
        );
        assert_eq!(
            aggregate(&Boxed(PercentileEstimateAggregator::new(0.5)), &no_values),
            None
        );
        assert_eq!(
            aggregate(&PropagateNoData(MeanAggregator), &no_values),
            None
        );
    }

    #[test]
    fn it_converts_values_to_a_grid() {
        let shape = GridShape2D::new([1, 3]);

        let grid = values_to_grid(shape, &[Some(1_u8), None, Some(3)]).unwrap();
        let GridOrEmpty2D::Grid(grid) = grid else {
            panic!("expected a grid");
        };
        assert_eq!(grid.inner_grid.data, vec![1, 0, 3]);
        assert_eq!(grid.validity_mask.data, vec![true, false, true]);

        assert!(
            values_to_grid::<u8>(shape, &[None, None, None])
                .unwrap()
                .is_empty()
        );
    }
}
