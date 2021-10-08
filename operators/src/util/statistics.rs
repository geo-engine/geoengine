use num_traits::AsPrimitive;
use snafu::Snafu;
use std::marker::PhantomData;

/// Enum for any errors encountered while creating statistics.
#[derive(Debug, Snafu)]
pub enum StatisticsError {
    #[snafu(display("Error initializing statistics. Reason: {}", reason))]
    Initialization { reason: String },
    #[snafu(display("Cannot compute statistics on empty sample."))]
    Empty,
}

/// Single quantile estimation with the P^2 algorithm
///
/// The P^2 algorithm estimates a quantile dynamically without storing samples. Instead of
/// storing the whole sample cumulative distribution, only five points (markers) are stored. The heights
/// of these markers are the minimum and the maximum of the samples and the current estimates of the
/// (p/2)-, p- and (1+p)/2-quantiles. Their positions are equal to the number
/// of samples that are smaller or equal to the markers. Each time a new samples is recorded, the
/// positions of the markers are updated and if necessary their heights are adjusted using a piecewise-
/// parabolic formula.
///
/// For further details, see
///
/// R. Jain and I. Chlamtac, The P^2 algorithm for dynamic calculation of quantiles and
/// histograms without storing observations, Communications of the ACM,
/// Volume 28 (October), Number 10, 1985, p. 1076-1085.
/// <https://www.cse.wustl.edu/~jain/papers/ftp/psqr.pdf>
///
#[derive(Debug)]
pub struct PSquareQuantileEstimator<T>
where
    T: AsPrimitive<f64>,
{
    quantile: f64,
    positions: [i64; 5],
    markers: [f64; 5],
    desired: [f64; 5],
    increment: [f64; 5],
    sample_count: u64,
    _phantom: PhantomData<T>,
}

impl<T> PSquareQuantileEstimator<T>
where
    T: AsPrimitive<f64>,
{
    /// Creates a new estimator for the given quantile. The estimator is
    /// update with all valid values from the given `initial_samples`.
    /// The initial samples must at least contain 5 valid elements
    /// (i.e., 5 elements that do not translate to `f64::NAN`).
    ///
    /// # Panics
    /// If the given quantile is not within the interval (0,1).
    ///
    /// # Errors
    /// If the `initial_samples` contain less than 5 valid elements.
    pub fn new(
        quantile: f64,
        initial_samples: &[T],
    ) -> Result<PSquareQuantileEstimator<T>, StatisticsError> {
        assert!(
            quantile > 0.0 && quantile < 1.0,
            "The desired quantile must be in the interval (0,1)"
        );

        // Initialize marker positions
        let positions = [1, 2, 3, 4, 5];

        // Initialize desired marker positions
        let desired = [
            1.0,
            1.0 + 2.0 * quantile,
            1.0 + 4.0 * quantile,
            3.0 + 2.0 * quantile,
            5.0,
        ];
        // Initialize marker increment
        let increment = [0.0, quantile / 2.0, quantile, (1.0 + quantile) / 2.0, 1.0];

        // Initialize marker values
        let mut markers = [0.0; 5];
        let mut iter = initial_samples.iter();
        let mut sample_count = 0;

        for v in &mut iter {
            let v: f64 = v.as_();
            if !v.is_nan() {
                markers[sample_count] = v;
                sample_count += 1;
                if sample_count > 4 {
                    break;
                }
            }
        }

        // We require at least 5 valid initial samples
        if sample_count < 5 {
            return Err(StatisticsError::Initialization {
                reason: "Insufficient valid samples.".to_owned(),
            });
        }

        markers.sort_unstable_by(|a, b| a.partial_cmp(b).expect("impossible"));

        let mut result = PSquareQuantileEstimator {
            quantile,
            positions,
            markers,
            desired,
            increment,
            sample_count: sample_count as u64,
            _phantom: PhantomData {},
        };

        // Add remaining values

        for v in &mut iter {
            result.update(*v)
        }
        Ok(result)
    }

    fn marker_value(&self, idx: usize) -> f64 {
        self.markers[idx]
    }

    /// Returns the quantile to estimate
    pub fn quantile(&self) -> f64 {
        self.quantile
    }

    /// Returns the number of samples seen so far
    pub fn sample_count(&self) -> u64 {
        self.sample_count
    }

    /// Returns the minimum of the samples seen so far
    pub fn min(&self) -> f64 {
        self.marker_value(0)
    }

    /// Returns the minimum of the samples seen so far
    pub fn max(&self) -> f64 {
        self.marker_value(4)
    }

    /// Returns the quantile estimate based on the samples seen so far
    pub fn quantile_estimate(&self) -> f64 {
        self.marker_value(2)
    }

    /// Returns the value of the second marker (i.e., an estimate of the p/2 quantile).
    pub fn marker2(&self) -> f64 {
        self.marker_value(1)
    }

    /// Returns the value of the fourth marker (i.e., an estimate of the (1+p)/2 quantile).
    pub fn marker4(&self) -> f64 {
        self.marker_value(3)
    }

    /// Updates the estimator with the given sample.
    pub fn update(&mut self, sample: T) {
        let val: f64 = sample.as_();

        // Ignore NANs
        if val.is_nan() {
            return;
        }

        self.sample_count += 1;

        // Find bucket
        let idx = if val < self.markers[0] {
            self.markers[0] = val;
            0
        } else if val < self.markers[1] {
            0
        } else if val < self.markers[2] {
            1
        } else if val < self.markers[3] {
            2
        } else if val < self.markers[4] {
            3
        } else {
            self.markers[4] = val;
            3
        };

        // Shift marker positions
        for i in (idx + 1)..5 {
            self.positions[i] += 1;
        }

        // Update desired positions
        for i in 0..5 {
            self.desired[i] += self.increment[i];
        }

        // Adjust marker height
        for i in 1..4 {
            let delta = self.desired[i] - self.positions[i] as f64;

            // Check if an adjustment is required
            if delta >= 1.0 && self.positions[i + 1] - self.positions[i] > 1
                || delta <= -1.0 && self.positions[i - 1] - self.positions[i] < -1
            {
                let delta = delta.signum();

                // Apply p^2 formula
                let mut val = self.estimate_marker_psquare(i, delta);

                // Apply linear estimation if value is invalid
                if self.markers[i - 1] >= val || val >= self.markers[i + 1] {
                    val = self.estimate_marker_linear(i, delta);
                }
                self.markers[i] = val;
                self.positions[i] += delta as i64;
            }
        }
    }

    /// Estimates the new value of the `i-th` marker linearly
    fn estimate_marker_linear(&self, i: usize, delta: f64) -> f64 {
        let neighbor_idx = if delta < 0.0 { i - 1 } else { i + 1 };
        self.markers[i]
            + delta
                * ((self.markers[neighbor_idx] - self.markers[i])
                    / (self.positions[neighbor_idx] - self.positions[i]) as f64)
    }

    /// Estimates the new value of the `i-th` marker using the p^2 method
    fn estimate_marker_psquare(&self, i: usize, delta: f64) -> f64 {
        let pos = self.positions[i] as f64;
        let pos_prev = self.positions[i - 1] as f64;
        let pos_next = self.positions[i + 1] as f64;

        let base = delta / (pos_next - pos_prev);
        let left =
            (pos - pos_prev + delta) * ((self.markers[i + 1] - self.markers[i]) / (pos_next - pos));
        let right =
            (pos_next - pos - delta) * ((self.markers[i] - self.markers[i - 1]) / (pos - pos_prev));
        self.markers[i] + base * (left + right) //* pos
    }
}

#[cfg(test)]
mod tests {
    use crate::util::statistics::PSquareQuantileEstimator;
    use rand::seq::SliceRandom;

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_paper_example() {
        let data = vec![
            0.02, 0.15, 0.74, 3.39, 0.83, 22.37, 10.15, 15.43, 38.62, 15.92, 34.60, 10.28, 1.47,
            0.40, 0.05, 11.39, 0.27, 0.42, 0.09, 11.37,
        ];

        let expected_markers = [
            [0.02, 0.15, 0.74, 0.83, 22.37],
            [0.02, 0.15, 0.74, 4.465, 22.37],
            [
                0.02,
                0.15,
                2.178_333_333_333_333,
                8.592_500_000_000_001,
                22.37,
            ],
            [
                0.02,
                0.869_444_444_444_444_2,
                4.752_685_185_185_185,
                15.516_990_740_740_741,
                38.62,
            ],
            [
                0.02,
                0.869_444_444_444_444_2,
                4.752_685_185_185_185,
                15.516_990_740_740_741,
                38.62,
            ],
            [
                0.02,
                0.869_444_444_444_444_2,
                9.274_704_861_111_111,
                21.572_663_194_444_445,
                38.62,
            ],
            [
                0.02,
                0.869_444_444_444_444_2,
                9.274_704_861_111_111,
                21.572_663_194_444_445,
                38.62,
            ],
            [
                0.02,
                2.132_463_107_638_888_5,
                9.274_704_861_111_111,
                21.572_663_194_444_445,
                38.62,
            ],
            [
                0.02,
                2.132_463_107_638_888_5,
                9.274_704_861_111_111,
                21.572_663_194_444_445,
                38.62,
            ],
            [
                0.02,
                0.730_843_171_296_295_7,
                6.297_302_000_661_376,
                21.572_663_194_444_445,
                38.62,
            ],
            [
                0.02,
                0.730_843_171_296_295_7,
                6.297_302_000_661_376,
                21.572_663_194_444_445,
                38.62,
            ],
            [
                0.02,
                0.588_674_537_037_036_5,
                6.297_302_000_661_376,
                17.203_904_274_140_214,
                38.62,
            ],
            [
                0.02,
                0.588_674_537_037_036_5,
                6.297_302_000_661_376,
                17.203_904_274_140_214,
                38.62,
            ],
            [
                0.02,
                0.493_895_447_530_863_8,
                4.440_634_353_260_337,
                17.203_904_274_140_214,
                38.62,
            ],
            [
                0.02,
                0.493_895_447_530_863_8,
                4.440_634_353_260_337,
                17.203_904_274_140_214,
                38.62,
            ],
        ];

        let expected_positions: [[i64; 5]; 15] = [
            [1, 2, 3, 4, 6],
            [1, 2, 3, 5, 7],
            [1, 2, 4, 6, 8],
            [1, 3, 5, 7, 9],
            [1, 3, 5, 7, 10],
            [1, 3, 6, 8, 11],
            [1, 3, 6, 9, 12],
            [1, 4, 7, 10, 13],
            [1, 5, 8, 11, 14],
            [1, 5, 8, 12, 15],
            [1, 5, 8, 13, 16],
            [1, 5, 9, 13, 17],
            [1, 6, 10, 14, 18],
            [1, 6, 10, 15, 19],
            [1, 6, 10, 16, 20],
        ];

        let mut estimator = PSquareQuantileEstimator::new(0.5, &data.as_slice()[0..5]).unwrap();

        for (idx, &v) in data.as_slice()[5..].iter().enumerate() {
            estimator.update(v);
            assert_eq!(expected_positions[idx], estimator.positions);
            for i in 0..5 {
                float_cmp::assert_approx_eq!(f64, expected_markers[idx][i], estimator.markers[i]);
            }
        }
    }

    #[test]
    fn test_all_markers() {
        let mut data = Vec::<i32>::with_capacity(100_000);

        for v in 1..=100_000 {
            data.push(v);
        }

        let mut rng = rand::thread_rng();
        data.shuffle(&mut rng);

        let estimator = PSquareQuantileEstimator::new(0.5, data.as_slice()).unwrap();

        float_cmp::assert_approx_eq!(f64, 1.0, estimator.min());
        float_cmp::assert_approx_eq!(f64, 100_000.0, estimator.max());
        assert!((50000.0 - estimator.quantile_estimate()).abs() < 50.0);
        assert!((25000.0 - estimator.marker2()).abs() < 50.0);
        assert!((75000.0 - estimator.marker4()).abs() < 50.0);
    }

    #[test]
    fn test_bad_initial_value() {
        let initial = vec![0.02, 0.15, f64::NAN, 3.39, 0.83];
        let estimator = PSquareQuantileEstimator::new(0.5, initial.as_slice());
        assert!(estimator.is_err());
    }

    #[test]
    fn test_bad_value() {
        let initial = vec![0.02, 0.15, 0.74, 3.39, 0.83];

        let mut estimator = PSquareQuantileEstimator::new(0.5, initial.as_slice()).unwrap();

        let samples = estimator.sample_count();

        estimator.update(f64::NAN);

        assert_eq!(samples, estimator.sample_count());

        estimator.update(42.0);

        assert_eq!(samples + 1, estimator.sample_count());
    }

    #[test]
    #[should_panic]
    fn test_bad_quantile() {
        let initial = vec![0.02, 0.15, 0.74, 3.39, 0.83];
        PSquareQuantileEstimator::new(1.2, initial.as_slice()).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_bad_quantile2() {
        let initial = vec![0.02, 0.15, 0.74, 3.39, 0.83];
        PSquareQuantileEstimator::new(-1.0, initial.as_slice()).unwrap();
    }
}
