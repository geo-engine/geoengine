use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};

/// This struct provides some basic number statistics.
///
/// All operations run in constant time.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct NumberStatistics {
    min_value: f64,
    max_value: f64,
    value_count: usize,
    value_nan_count: usize,
    mean_value: f64,
    m2: f64,
}

impl Default for NumberStatistics {
    fn default() -> Self {
        Self {
            min_value: f64::MAX,
            max_value: f64::MIN,
            value_count: 0,
            value_nan_count: 0,
            mean_value: 0.0,
            m2: 0.0,
        }
    }
}

impl NumberStatistics {
    #[inline]
    pub fn add<V>(&mut self, value: V)
    where
        V: AsPrimitive<f64>,
    {
        let value = value.as_();

        if value.is_nan() {
            self.value_nan_count += 1;
            return;
        }

        self.min_value = f64::min(self.min_value, value);
        self.max_value = f64::max(self.max_value, value);

        // Welford's algorithm
        self.value_count += 1;
        let delta = value - self.mean_value;
        self.mean_value += delta / (self.value_count as f64);
        let delta2 = value - self.mean_value;
        self.m2 += delta * delta2;
    }

    #[inline]
    pub fn add_no_data(&mut self) {
        self.value_nan_count += 1;
    }

    pub fn count(&self) -> usize {
        self.value_count
    }

    pub fn nan_count(&self) -> usize {
        self.value_nan_count
    }

    pub fn min(&self) -> f64 {
        if self.value_count > 0 {
            self.min_value
        } else {
            f64::NAN
        }
    }

    pub fn max(&self) -> f64 {
        if self.value_count > 0 {
            self.max_value
        } else {
            f64::NAN
        }
    }

    pub fn mean(&self) -> f64 {
        if self.value_count > 0 {
            self.mean_value
        } else {
            f64::NAN
        }
    }

    pub fn var(&self) -> f64 {
        if self.value_count > 0 {
            self.m2 / (self.value_count as f64)
        } else {
            f64::NAN
        }
    }

    pub fn std_dev(&self) -> f64 {
        if self.value_count > 1 {
            f64::sqrt(self.m2 / (self.value_count as f64))
        } else {
            f64::NAN
        }
    }

    pub fn sample_std_dev(&self) -> f64 {
        if self.value_count > 1 {
            f64::sqrt(self.m2 / ((self.value_count - 1) as f64))
        } else {
            f64::NAN
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::float_cmp)]
    fn example_data() {
        let mut number_statistics = NumberStatistics::default();

        for &v in &[2, 4, 4, 4, 5, 5, 7, 9] {
            number_statistics.add(v);
        }

        assert_eq!(number_statistics.count(), 8);
        assert_eq!(number_statistics.nan_count(), 0);
        assert_eq!(number_statistics.min(), 2.);
        assert_eq!(number_statistics.max(), 9.);
        assert_eq!(number_statistics.mean(), 5.);
        assert_eq!(number_statistics.var(), 4.);
        assert_eq!(number_statistics.std_dev(), 2.);
        assert_eq!(number_statistics.sample_std_dev(), 2.138_089_935_299_395);
    }

    #[test]
    fn nan_data() {
        let mut number_statistics = NumberStatistics::default();

        number_statistics.add(1);
        number_statistics.add(f64::NAN);
        number_statistics.add_no_data();

        assert_eq!(number_statistics.count(), 1);
        assert_eq!(number_statistics.nan_count(), 2);
    }
}
