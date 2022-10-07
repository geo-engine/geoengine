use super::{error, NeighborhoodAggregateError};
use crate::util::number_statistics::NumberStatistics;
use geoengine_datatypes::raster::{Grid2D, GridShape2D, GridSize, Pixel};
use num::Integer;
use num_traits::AsPrimitive;
use snafu::ensure;

/// A weight matrix that is applied for the neighborhood of pixels before aggregating them.
#[derive(Debug, Clone)]
pub struct Neighborhood {
    matrix: Grid2D<f64>,
}

impl Neighborhood {
    pub fn new(matrix: Grid2D<f64>) -> Result<Self, NeighborhoodAggregateError> {
        ensure!(
            matrix.axis_size_x().is_odd() && matrix.axis_size_y().is_odd(),
            error::DimensionsNotOdd {
                actual: GridShape2D::new(matrix.axis_size()),
            }
        );

        Ok(Self { matrix })
    }

    /// Apply the weight matrix to the given pixel neighborhood and return the neighborhood.
    ///
    // TODO: Think about returning only the f64 values and omitting NODATA values.
    //       We need more aggregate functions first to see if this would suffice.
    pub fn apply(&self, mut values: Vec<Option<f64>>) -> Vec<Option<f64>> {
        // enforce NODATA when neighborhood is incomplete
        if values.len() != self.matrix.number_of_elements() {
            return Vec::new();
        }

        for (value, weight) in values.iter_mut().zip(self.matrix.data.iter()) {
            if let Some(value) = value {
                *value *= weight;
            }
        }

        values
    }

    pub fn matrix(&self) -> &Grid2D<f64> {
        &self.matrix
    }

    /// Specifies the x extent beginning from the center pixel
    pub fn x_radius(&self) -> usize {
        self.matrix.axis_size_x() / 2
    }

    pub fn x_width(&self) -> usize {
        self.matrix.axis_size_x()
    }

    /// Specifies the y extent beginning from the center pixel
    pub fn y_width(&self) -> usize {
        self.matrix.axis_size_y()
    }

    /// Specifies the x extent right of one pixel
    pub fn y_radius(&self) -> usize {
        self.matrix.axis_size_y() / 2
    }
}

/// A function that aggregates a neighborhood of pixels to a single pixel value.
pub trait AggregateFunction: Sync + Send + Clone {
    fn apply<P>(&self, values: &[Option<f64>]) -> Option<P>
    where
        P: Pixel,
        f64: AsPrimitive<P>;
}

/// An aggregate function that computes the standard deviation of a set of pixels.
#[derive(Debug, Clone, Copy)]
pub struct StandardDeviation;

impl StandardDeviation {
    pub fn new() -> Self {
        Self
    }
}

impl AggregateFunction for StandardDeviation {
    fn apply<P>(&self, values: &[Option<f64>]) -> Option<P>
    where
        P: Pixel,
        f64: AsPrimitive<P>,
    {
        let mut aggregator = NumberStatistics::default();
        for value in values {
            match value {
                Some(v) => aggregator.add(*v),
                // TODO: Decide if the result should be NODATA on the first NODATA or if it should be ignored
                None => aggregator.add_no_data(),
            }
        }

        let std_dev = aggregator.std_dev();

        if std_dev.is_finite() {
            Some(std_dev.as_())
        } else {
            None
        }
    }
}

/// An aggregate function that computes the sum of a set of pixels.
#[derive(Debug, Clone)]
pub struct Sum;

impl Sum {
    pub fn new() -> Self {
        Self
    }
}

impl AggregateFunction for Sum {
    fn apply<P>(&self, value_options: &[Option<f64>]) -> Option<P>
    where
        P: Pixel,
        f64: AsPrimitive<P>,
    {
        if value_options.is_empty() {
            return None;
        }

        let mut sum = 0.;

        for value in value_options {
            if let Some(v) = value {
                sum += v;
            } else {
                return None;
            }
        }

        Some(sum.as_())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_standard_deviation() {
        let aggregate_function = StandardDeviation::new();

        let result: Option<f64> = aggregate_function.apply(&[
            Some(1.),
            Some(2.),
            Some(3.),
            Some(4.),
            Some(5.),
            Some(6.),
            Some(7.),
            Some(8.),
            Some(9.),
        ]);
        assert_eq!(result.unwrap(), 2.581_988_897_471_611);

        let result: Option<f64> = aggregate_function.apply(&[
            Some(1.),
            Some(2.),
            Some(3.),
            Some(4.),
            Some(5.),
            Some(6.),
            Some(7.),
            Some(8.),
            None,
        ]);
        assert_eq!(result.unwrap(), 2.291_287_847_477_92);

        assert!(aggregate_function
            .apply::<f64>(&[] as &[Option<f64>])
            .is_none());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_sum_fn() {
        let kernel = Sum::new();

        let result = kernel.apply::<f64>(&[
            Some(1.),
            Some(2.),
            Some(3.),
            Some(4.),
            Some(5.),
            Some(6.),
            Some(7.),
            Some(8.),
            Some(9.),
        ]);
        assert_eq!(result.unwrap(), 45.);

        let result = kernel.apply::<f64>(&[
            Some(1.),
            Some(2.),
            Some(3.),
            Some(4.),
            Some(5.),
            Some(6.),
            Some(7.),
            Some(8.),
            None,
        ]);
        assert!(result.is_none());

        assert!(kernel.apply::<f64>(&[] as &[Option<f64>]).is_none());
    }

    #[test]
    fn test_applying_weights() {
        let ones = Neighborhood::new(Grid2D::new([3, 3].into(), vec![1.; 9]).unwrap()).unwrap();

        assert_eq!(
            ones.apply(vec![
                Some(1.),
                Some(10.),
                Some(100.),
                Some(1000.),
                Some(10000.),
                Some(100_000.),
                Some(1_000_000.),
                Some(10_000_000.),
                Some(100_000_000.)
            ]),
            vec![
                Some(1.),
                Some(10.),
                Some(100.),
                Some(1000.),
                Some(10000.),
                Some(100_000.),
                Some(1_000_000.),
                Some(10_000_000.),
                Some(100_000_000.)
            ]
        );

        assert_eq!(
            ones.apply(vec![
                Some(1.),
                Some(10.),
                Some(100.),
                Some(1000.),
                Some(10000.),
                Some(100_000.),
                Some(1_000_000.),
                Some(10_000_000.),
                None
            ]),
            vec![
                Some(1.),
                Some(10.),
                Some(100.),
                Some(1000.),
                Some(10000.),
                Some(100_000.),
                Some(1_000_000.),
                Some(10_000_000.),
                None
            ]
        );

        assert_eq!(ones.apply(vec![Some(1.),]), vec![]);
        assert_eq!(ones.apply(vec![]), vec![]);
        assert_eq!(ones.apply(vec![Some(0.); 10]), vec![]);

        let one_to_nine = Neighborhood::new(
            Grid2D::new([3, 3].into(), vec![1., 2., 3., 4., 5., 6., 7., 8., 9.]).unwrap(),
        )
        .unwrap();

        assert_eq!(
            one_to_nine.apply(vec![
                Some(1.),
                Some(10.),
                Some(100.),
                Some(1000.),
                Some(10000.),
                Some(100_000.),
                Some(1_000_000.),
                Some(10_000_000.),
                Some(100_000_000.)
            ]),
            vec![
                Some(1.),
                Some(20.),
                Some(300.),
                Some(4000.),
                Some(50000.),
                Some(600_000.),
                Some(7_000_000.),
                Some(80_000_000.),
                Some(900_000_000.)
            ]
        );
    }
}
