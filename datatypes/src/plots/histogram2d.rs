use std::cmp;
use std::collections::HashMap;

use float_cmp::*;
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::error;
use crate::plots::{Plot, PlotData, PlotMetaData};
use crate::primitives::Coordinate2D;
use crate::util::Result;

/// Describes one dimension of the 2D histogram
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistogramDimension {
    /// The name of the attribute
    column: String,
    /// The minimum value
    min: f64,
    /// The maximum value
    max: f64,
    /// The size of a bucket
    bucket_size: f64,
    /// The number of buckets to use
    bucket_count: usize,
}

impl HistogramDimension {
    /// Creates a new description of a histogram dimension
    ///
    /// # Errors
    /// This method fails in the following three cases:
    /// - The `bucket_count` is 0
    /// - `min` >= `max`
    /// - `min` or `max` is not a finite value
    pub fn new(
        column: String,
        min: f64,
        max: f64,
        bucket_count: usize,
    ) -> Result<HistogramDimension> {
        ensure!(
            bucket_count > 0,
            error::Plot {
                details: format!(
                    "HistogramDimension {} must have at least one bucket.",
                    &column
                )
            }
        );
        ensure!(
            min.is_finite() && max.is_finite(),
            error::Plot {
                details: format!(
                    "HistogramDimension {} must have finite min/max values.",
                    &column
                )
            }
        );
        ensure!(
            min < max || (bucket_count == 1 && approx_eq!(f64, min, max)),
            error::Plot {
                details: format!(
                    "HistogramDimension {}: max value must be larger than its min value",
                    &column
                )
            }
        );

        Ok(HistogramDimension {
            column,
            min,
            max,
            bucket_size: (max - min) / bucket_count as f64,
            bucket_count,
        })
    }

    /// Computes the bucket index for the given value.
    /// This method returns `None` if the given value is
    /// not within the domain `[min, max]`.
    fn bucket_idx(&self, value: f64) -> Option<usize> {
        if !value.is_finite() || !(self.min..=self.max).contains(&value) {
            None
        } else {
            let idx = ((value - self.min) / self.bucket_size) as usize;
            Some(cmp::min(idx, self.bucket_count - 1))
        }
    }
}

/// A 2-dimensional equi-distant histogram with a configurable number
/// of buckets per dimension.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Histogram2D {
    counts: Vec<HashMap<usize, u64>>,
    x: HistogramDimension,
    y: HistogramDimension,
    total_count: u64,
    max_count: u64,
}

impl Histogram2D {
    /// Creates a new empty `Histogram2D` with the given dimensions
    pub fn new(x: HistogramDimension, y: HistogramDimension) -> Self {
        let counts = vec![HashMap::new(); x.bucket_count];
        Self {
            counts,
            x,
            y,
            total_count: 0,
            max_count: 0,
        }
    }

    /// Returns the maximum number of samples contained
    /// in a single bucket.
    pub fn max_count(&self) -> u64 {
        self.max_count
    }

    /// Returns the total number of samples seen so far.
    pub fn total_count(&self) -> u64 {
        self.total_count
    }

    /// Adds the given samples to this histogram
    pub fn update_batch(&mut self, values: impl Iterator<Item = Coordinate2D>) {
        for i in values {
            self.update(i);
        }
    }

    /// Adds the given sample to this histogram
    pub fn update(&mut self, value: Coordinate2D) {
        let idx = self.x.bucket_idx(value.x).zip(self.y.bucket_idx(value.y));
        if let Some((x, y)) = idx {
            let v = self.counts[x].entry(y).or_insert(0);
            *v += 1;
            self.total_count += 1;
            self.max_count = cmp::max(self.max_count, *v);
        }
    }
}

impl Plot for Histogram2D {
    fn to_vega_embeddable(&self, _allow_interactions: bool) -> Result<PlotData> {
        let mut values = Vec::with_capacity(self.counts.len());
        for (idx_x, value) in self.counts.iter().enumerate() {
            let x = self.x.min + (idx_x as f64 + 0.5) * self.x.bucket_size;
            for (idx_y, count) in value {
                let y = self.y.min + (*idx_y as f64 + 0.5) * self.y.bucket_size;
                values.push(serde_json::json!({
                    "x": x,
                    "y": y,
                    "frequency": count,
                }));
            }
        }

        let vega_spec = serde_json::json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container",
            "height": "container",
            "data": {
                "values": values,
            },
            "mark": {
                "type": "point",
                "filled": true,
            },
            "encoding": {
                "x": {
                    "field": "x",
                    "type": "quantitative",
                    "axis": {
                        "title": "x"
                    }
                },
                "y": {
                    "field": "y",
                    "type": "quantitative",
                    "axis": {
                        "title": "y"
                    }
                },
                "size": {
                    "field": "frequency",
                    "bin": true
                },
                // "color": {
                //     "bin": true,
                //     "field": "frequency",
                //     "scale": {"scheme": "rainbow"}
                // },
            }
        });

        Ok(PlotData {
            vega_string: vega_spec.to_string(),
            metadata: PlotMetaData::None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dim_ok() {
        assert!(HistogramDimension::new("x".to_string(), 0.0, 10.0, 10).is_ok());
    }

    #[test]
    fn test_dim_min_gt_max() {
        assert!(HistogramDimension::new("x".to_string(), 1.0, 0.0, 10).is_err());
    }

    #[test]
    fn test_dim_min_eq_max() {
        assert!(HistogramDimension::new("x".to_string(), 0.0, 0.0, 10).is_err());
    }

    #[test]
    fn test_dim_min_eq_max_single_bucket() {
        assert!(HistogramDimension::new("x".to_string(), 0.0, 0.0, 1).is_ok());
    }

    #[test]
    fn test_dim_bucket_count() {
        assert!(HistogramDimension::new("x".to_string(), 0.0, 10.0, 0).is_err());
    }

    #[test]
    fn test_update() {
        let dim_x = HistogramDimension::new("x".to_string(), 0.0, 10.0, 10).unwrap();
        let dim_y = HistogramDimension::new("y".to_string(), 0.0, 10.0, 10).unwrap();

        let mut hist = Histogram2D::new(dim_x, dim_y);

        for i in 0..10 {
            for _ in 0..=i {
                hist.update(Coordinate2D::new(f64::from(i), f64::from(i)));
            }
        }

        assert_eq!(10, hist.max_count());
        assert_eq!(55, hist.total_count());
    }

    #[test]
    fn test_batch_update() {
        let dim_x = HistogramDimension::new("x".to_string(), 0.0, 10.0, 10).unwrap();
        let dim_y = HistogramDimension::new("y".to_string(), 0.0, 10.0, 10).unwrap();

        let mut hist = Histogram2D::new(dim_x, dim_y);

        let data = vec![Coordinate2D::new(5.5, 7.0), Coordinate2D::new(1.0, 9.0)];

        hist.update_batch(data.into_iter());

        assert_eq!(2, hist.total_count());
        assert_eq!(1, hist.max_count());
    }

    #[test]
    fn test_out_of_range() {
        let dim_x = HistogramDimension::new("x".to_string(), 0.0, 10.0, 10).unwrap();
        let dim_y = HistogramDimension::new("y".to_string(), 0.0, 10.0, 10).unwrap();

        let mut hist = Histogram2D::new(dim_x, dim_y);

        let data = vec![Coordinate2D::new(5.5, 12.0), Coordinate2D::new(1.0, 9.0)];

        hist.update_batch(data.into_iter());

        assert_eq!(1, hist.total_count());
        assert_eq!(1, hist.max_count());
    }

    #[test]
    fn test_out_nan() {
        let dim_x = HistogramDimension::new("x".to_string(), 0.0, 10.0, 10).unwrap();
        let dim_y = HistogramDimension::new("y".to_string(), 0.0, 10.0, 10).unwrap();

        let mut hist = Histogram2D::new(dim_x, dim_y);

        let data = vec![
            Coordinate2D::new(5.5, f64::NAN),
            Coordinate2D::new(1.0, 9.0),
        ];

        hist.update_batch(data.into_iter());

        assert_eq!(1, hist.total_count());
        assert_eq!(1, hist.max_count());
    }
}
