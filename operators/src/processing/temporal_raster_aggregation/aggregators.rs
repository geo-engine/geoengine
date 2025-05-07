use crate::util::{Result, statistics::SafePSquareQuantileEstimator};
use geoengine_datatypes::raster::{GridOrEmpty2D, MapIndexedElements, Pixel};
use std::marker::PhantomData;

/// An aggregator that uses input values to produce an inner state that can be used to produce an output aggregate value.
pub trait TemporalRasterPixelAggregator<P: Pixel>: Send + Clone {
    type PixelState: Send + Sync + Copy + Clone + Default;

    /// Tell whether the aggregator ignores incoming no data values
    const IGNORE_NO_DATA: bool;

    /// Initialize the state from the first value
    fn initialize(value: Option<P>) -> Option<Self::PixelState>;

    /// Produce a new state from the current state and new value
    fn aggregate(state: Option<Self::PixelState>, value: Option<P>) -> Option<Self::PixelState>;

    /// Produce a tile from the state container
    fn into_grid(state: GridOrEmpty2D<Self::PixelState>) -> Result<GridOrEmpty2D<P>>;
}

pub trait GlobalStateTemporalRasterPixelAggregator<P: Pixel>: Send + Sync + Clone {
    type PixelState: Send + Sync + Copy + Clone + /* required by [`MapIndexElements`] */ Default;

    /// Tell whether the aggregator ignores incoming no data values
    const IGNORE_NO_DATA: bool;

    /// Initialize the state from the first value
    fn initialize(&self, value: Option<P>) -> Option<Self::PixelState>;

    /// Produce a new state from the current state and new value
    fn aggregate(
        &self,
        state: Option<Self::PixelState>,
        value: Option<P>,
    ) -> Option<Self::PixelState>;

    /// Produce a tile from the state container
    fn to_grid(&self, state: GridOrEmpty2D<Self::PixelState>) -> Result<GridOrEmpty2D<P>>;
}

/// A method to process to pixel values inside the aggregator.
trait BinaryOperation<P: Pixel>: Send + Clone {
    fn unit(value: P) -> P {
        value
    }

    fn op(state: P, value: P) -> P;
}

#[derive(Clone)]
pub struct OpPixelAggregator<Op> {
    op: PhantomData<Op>,
}

impl<P: Pixel, Op: BinaryOperation<P>> TemporalRasterPixelAggregator<P> for OpPixelAggregator<Op> {
    type PixelState = P;

    const IGNORE_NO_DATA: bool = false;

    fn initialize(value: Option<P>) -> Option<Self::PixelState> {
        value.map(Op::unit)
    }

    fn aggregate(state: Option<Self::PixelState>, value: Option<P>) -> Option<Self::PixelState> {
        match (state, value) {
            (Some(state), Some(value)) => Some(Op::op(state, value)),
            _ => None,
        }
    }

    fn into_grid(state: GridOrEmpty2D<Self::PixelState>) -> Result<GridOrEmpty2D<P>> {
        Ok(state)
    }
}

#[derive(Clone)]
pub struct OpPixelAggregatorIngoringNoData<Op> {
    op: PhantomData<Op>,
}

impl<P: Pixel, Op: BinaryOperation<P>> TemporalRasterPixelAggregator<P>
    for OpPixelAggregatorIngoringNoData<Op>
{
    type PixelState = P;

    const IGNORE_NO_DATA: bool = true;

    fn initialize(value: Option<P>) -> Option<Self::PixelState> {
        value.map(Op::unit)
    }

    fn aggregate(state: Option<Self::PixelState>, value: Option<P>) -> Option<Self::PixelState> {
        match (state, value) {
            (Some(state), Some(value)) => Some(Op::op(state, value)),
            (Some(state), None) => Some(state),
            (None, Some(value)) => Some(Op::unit(value)),
            _ => None,
        }
    }

    fn into_grid(state: GridOrEmpty2D<Self::PixelState>) -> Result<GridOrEmpty2D<P>> {
        Ok(state)
    }
}

#[derive(Clone)]
pub struct Sum;

impl<P: Pixel> BinaryOperation<P> for Sum {
    fn op(state: P, value: P) -> P {
        state.saturating_add(value)
    }
}

pub type SumPixelAggregator = OpPixelAggregator<Sum>;
pub type SumPixelAggregatorIngoringNoData = OpPixelAggregatorIngoringNoData<Sum>;

#[derive(Clone)]
pub struct Count;

impl<P: Pixel> BinaryOperation<P> for Count {
    fn unit(_value: P) -> P {
        P::one()
    }

    fn op(state: P, _value: P) -> P {
        state.saturating_add(P::one())
    }
}

pub type CountPixelAggregator = OpPixelAggregator<Count>;
pub type CountPixelAggregatorIngoringNoData = OpPixelAggregatorIngoringNoData<Count>;

#[derive(Clone)]
pub struct Min;

impl<P: Pixel> BinaryOperation<P> for Min {
    fn op(state: P, value: P) -> P {
        if state < value { state } else { value }
    }
}

pub type MinPixelAggregator = OpPixelAggregator<Min>;
pub type MinPixelAggregatorIngoringNoData = OpPixelAggregatorIngoringNoData<Min>;

#[derive(Clone)]
pub struct Max;

impl<P: Pixel> BinaryOperation<P> for Max {
    fn op(state: P, value: P) -> P {
        if state > value { state } else { value }
    }
}

pub type MaxPixelAggregator = OpPixelAggregator<Max>;
pub type MaxPixelAggregatorIngoringNoData = OpPixelAggregatorIngoringNoData<Max>;

#[derive(Clone)]
pub struct First;

impl<P: Pixel> BinaryOperation<P> for First {
    fn op(state: P, _value: P) -> P {
        state
    }
}

pub type FirstPixelAggregatorIngoringNoData = OpPixelAggregatorIngoringNoData<First>;

#[derive(Clone)]
pub struct Last;

impl<P: Pixel> BinaryOperation<P> for Last {
    fn op(_state: P, value: P) -> P {
        value
    }
}

pub type LastPixelAggregatorIngoringNoData = OpPixelAggregatorIngoringNoData<Last>;

#[derive(Clone)]
pub struct MeanPixelAggregator<const IGNORE_NO_DATA: bool>;

impl<P: Pixel, const IGNORE_NO_DATA: bool> TemporalRasterPixelAggregator<P>
    for MeanPixelAggregator<IGNORE_NO_DATA>
{
    type PixelState = (f64, usize);

    const IGNORE_NO_DATA: bool = IGNORE_NO_DATA;

    fn initialize(value: Option<P>) -> Option<Self::PixelState> {
        value.map(|v| (v.as_(), 1))
    }

    fn aggregate(state: Option<Self::PixelState>, value: Option<P>) -> Option<Self::PixelState> {
        if IGNORE_NO_DATA {
            match (state, value) {
                (Some(state), Some(value)) => Some(mean_of_state_and_value(state, value)),
                (Some(state), None) => Some(state),
                (None, Some(value)) => Self::initialize(Some(value)),
                _ => None,
            }
        } else {
            match (state, value) {
                (Some(state), Some(value)) => Some(mean_of_state_and_value(state, value)),
                _ => None,
            }
        }
    }

    fn into_grid(state: GridOrEmpty2D<Self::PixelState>) -> Result<GridOrEmpty2D<P>> {
        Ok(state.map_indexed_elements(|_index: usize, (mean, _count)| P::from_(mean)))
    }
}

fn mean_of_state_and_value<P: Pixel>(
    (mean_value, count): (f64, usize),
    new_value: P,
) -> (f64, usize) {
    let new_value: f64 = new_value.as_();
    let new_count = count + 1;
    let delta: f64 = new_value - mean_value;
    let new_state_value = mean_value + delta / (new_count as f64);
    (new_state_value, new_count)
}

#[derive(Clone)]
pub struct PercentileEstimateAggregator<const IGNORE_NO_DATA: bool> {
    percentile: f64,
}

impl<const IGNORE_NO_DATA: bool> PercentileEstimateAggregator<IGNORE_NO_DATA> {
    pub fn new(percentile: f64) -> Self {
        Self { percentile }
    }
}

impl<P: Pixel, const IGNORE_NO_DATA: bool> GlobalStateTemporalRasterPixelAggregator<P>
    for PercentileEstimateAggregator<IGNORE_NO_DATA>
{
    // we need a default value, so we wrap it again into an `Option`
    type PixelState = Option<SafePSquareQuantileEstimator<P>>;

    const IGNORE_NO_DATA: bool = IGNORE_NO_DATA;

    fn initialize(&self, value: Option<P>) -> Option<Self::PixelState> {
        Some(SafePSquareQuantileEstimator::new(self.percentile, value?).ok())
    }

    fn aggregate(
        &self,
        state: Option<Self::PixelState>,
        value: Option<P>,
    ) -> Option<Self::PixelState> {
        if IGNORE_NO_DATA {
            match (state, value) {
                (Some(Some(mut state)), Some(value)) => {
                    state.update(value);
                    Some(Some(state))
                }
                (Some(state), None) => Some(state),
                (None, Some(value)) => self.initialize(Some(value)),
                _ => None,
            }
        } else {
            match (state, value) {
                (Some(Some(mut state)), Some(value)) => {
                    state.update(value);
                    Some(Some(state))
                }
                _ => None,
            }
        }
    }

    fn to_grid(&self, state: GridOrEmpty2D<Self::PixelState>) -> Result<GridOrEmpty2D<P>> {
        Ok(
            state.map_indexed_elements(|_index: usize, estimator_opt: Option<Self::PixelState>| {
                let estimator = estimator_opt??;
                let value = P::from_(estimator.quantile_estimate());
                Some(value)
            }),
        )
    }
}
