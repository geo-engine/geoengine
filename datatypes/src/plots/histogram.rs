use std::cmp;

use float_cmp::*;
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::error;
use crate::plots::{Plot, PlotData, PlotMetaData};
use crate::primitives::{DataRef, FeatureDataRef, Measurement};
use crate::raster::Pixel;
use crate::util::Result;

#[derive(Debug, Deserialize, Serialize)]
pub struct Histogram {
    counts: Vec<u64>,
    labels: Option<Vec<String>>,
    nodata_count: u64,
    min: f64,
    max: f64,
    measurement: Measurement,
}

impl Histogram {
    fn new(
        number_of_buckets: usize,
        min: f64,
        max: f64,
        measurement: Measurement,
        labels: Option<Vec<String>>,
        counts: Option<Vec<u64>>,
    ) -> Result<Self> {
        ensure!(
            number_of_buckets > 0,
            error::Plot {
                details: "Histograms must have at least one bucket"
            }
        );
        ensure!(
            min.is_finite() && max.is_finite(),
            error::Plot {
                details: "Histograms must have finite min/max values"
            }
        );
        ensure!(
            min < max || (approx_eq!(f64, min, max) && number_of_buckets == 1),
            error::Plot {
                details: "Histograms max value must be larger than its min value"
            }
        );
        if let Some(labels) = &labels {
            ensure!(
                labels.len() == number_of_buckets,
                error::Plot {
                    details: "Histogram must have as many labels as buckets"
                }
            );
        }

        let counts = if let Some(counts) = counts {
            ensure!(
                counts.len() == number_of_buckets,
                error::Plot {
                    details: "The `counts` must be of length `number_of_buckets`"
                }
            );
            counts
        } else {
            vec![0; number_of_buckets]
        };

        Ok(Self {
            counts,
            labels,
            nodata_count: 0,
            min,
            max,
            measurement,
        })
    }

    /// Creates a new empty histogram
    ///
    /// # Examples
    /// ```rust
    /// use geoengine_datatypes::plots::Histogram;
    /// use geoengine_datatypes::primitives::Measurement;
    ///
    /// Histogram::builder(2, 0., 1., Measurement::Unitless).build().unwrap();
    ///
    /// Histogram::builder(0, f64::NAN, f64::INFINITY, Measurement::Unitless).build().unwrap_err();
    /// ```
    ///
    /// ```rust
    /// use geoengine_datatypes::plots::Histogram;
    /// use geoengine_datatypes::primitives::Measurement;
    ///
    /// Histogram::builder(2, 0., 1., Measurement::Unitless).labels(vec!["foo".into(), "bar".into()])
    ///     .build()
    ///     .unwrap();
    ///
    /// Histogram::builder(0, f64::NAN, f64::INFINITY, Measurement::Unitless)
    ///     .labels(vec!["foo".into(), "bar".into()])
    ///     .build()
    ///     .unwrap_err();
    /// Histogram::builder(2, 0., 1., Measurement::Unitless)
    ///     .labels(vec!["foo".into()])
    ///     .build()
    ///     .unwrap_err();
    /// ```
    pub fn builder(
        number_of_buckets: usize,
        min: f64,
        max: f64,
        measurement: Measurement,
    ) -> HistogramBuilder {
        HistogramBuilder::new(number_of_buckets, min, max, measurement)
    }

    /// Add feature data to the histogram
    ///
    /// # Errors
    ///
    /// This method fails if the feature is not numeric.
    ///
    pub fn add_feature_data(&mut self, data: FeatureDataRef) -> Result<()> {
        // TODO: implement efficiently OpenCL version
        match data {
            FeatureDataRef::Float(value_ref) if !value_ref.has_nulls() => {
                for &value in value_ref.as_ref() {
                    self.handle_data_item(value, false);
                }
            }
            FeatureDataRef::Float(value_ref) => {
                for (&value, is_null) in value_ref.as_ref().iter().zip(value_ref.nulls()) {
                    self.handle_data_item(value, is_null);
                }
            }
            FeatureDataRef::Int(value_ref) if !value_ref.has_nulls() => {
                for value in value_ref.as_ref().iter().map(|&v| v as f64) {
                    self.handle_data_item(value, false);
                }
            }
            FeatureDataRef::Int(value_ref) => {
                for (value, is_null) in value_ref
                    .as_ref()
                    .iter()
                    .map(|&v| v as f64)
                    .zip(value_ref.nulls())
                {
                    self.handle_data_item(value, is_null);
                }
            }
            FeatureDataRef::Categorical(categorical_ref) if !categorical_ref.has_nulls() => {
                for value in categorical_ref.as_ref().iter().map(|&v| f64::from(v)) {
                    self.handle_data_item(value, false);
                }
            }
            FeatureDataRef::Categorical(categorical_ref) => {
                for (value, is_null) in categorical_ref
                    .as_ref()
                    .iter()
                    .map(|&v| f64::from(v))
                    .zip(categorical_ref.nulls())
                {
                    self.handle_data_item(value, is_null);
                }
            }
            FeatureDataRef::Text(..) => {
                return error::Plot {
                    details: "Cannot add non-numerical data to the histogram.",
                }
                .fail();
            }
        }

        Ok(())
    }

    /// Add raster data to the histogram
    pub fn add_raster_data<P: Pixel>(&mut self, data: &[P], no_data_value: Option<P>) {
        if let Some(no_data_value) = no_data_value {
            for &value in data {
                self.handle_data_item(value.as_(), value == no_data_value);
            }
        } else {
            for &value in data {
                self.handle_data_item(value.as_(), false);
            }
        }
    }

    fn handle_data_item(&mut self, value: f64, is_null: bool) {
        if is_null || !value.is_finite() {
            self.nodata_count += 1;
        } else if self.min <= value && value <= self.max {
            let bucket = self.bucket_for_value(value);
            self.counts[bucket] += 1;
        }
        // ignore out-of-range values
    }

    fn bucket_for_value(&self, value: f64) -> usize {
        if self.counts.len() == 1 {
            return 0;
        }

        let fraction = (value - self.min) / (self.max - self.min);
        let bucket = (fraction * (self.counts.len() as f64)) as usize;

        cmp::min(bucket, self.counts.len() - 1)
    }
}

impl Plot for Histogram {
    fn to_vega_embeddable(&self, allow_interactions: bool) -> Result<PlotData> {
        let step = (self.max - self.min) / (self.counts.len() as f64);

        let mut values = Vec::with_capacity(self.counts.len());
        let mut bin_start = self.min;
        for &count in &self.counts {
            let bin_end = bin_start + step;
            values.push(serde_json::json!({
                "bin_start": bin_start,
                "bin_end": bin_end,
                "Frequency": count,
            }));
            bin_start = bin_end;
        }

        let mut vega_spec = serde_json::json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
            "data": {
                "values": values,
            },
            "mark": "bar",
            "encoding": {
                "x": {
                    "field": "bin_start",
                    "bin": {
                        "binned": true,
                        "step": step,
                    },
                    "axis": {
                        "title": self.measurement.to_string(),
                    },
                },
                "x2": {
                    "field": "bin_end",
                },
                "y": {
                    "field": "Frequency",
                    "type": "quantitative",
                },
            },
        });

        let selection_name = if allow_interactions {
            let name = "range_selection".to_string();

            vega_spec.as_object_mut().expect("as defined").insert(
                "selection".to_owned(),
                serde_json::json!({
                    "range_selection": {
                        "encodings": ["x"],
                        "type": "interval"
                    }
                }),
            );

            Some(name)
        } else {
            None
        };

        Ok(PlotData {
            vega_string: vega_spec.to_string(),
            metadata: selection_name.map_or(PlotMetaData::None, |selection_name| {
                PlotMetaData::Selection { selection_name }
            }),
        })
    }
}

pub struct HistogramBuilder {
    number_of_buckets: usize,
    min: f64,
    max: f64,
    measurement: Measurement,
    labels: Option<Vec<String>>,
    counts: Option<Vec<u64>>,
}

impl HistogramBuilder {
    /// Builder with required values
    fn new(number_of_buckets: usize, min: f64, max: f64, measurement: Measurement) -> Self {
        Self {
            number_of_buckets,
            min,
            max,
            measurement,
            labels: None,
            counts: None,
        }
    }

    /// Adds labels to the histogram
    ///
    /// # Examples
    /// ```rust
    /// use geoengine_datatypes::plots::Histogram;
    /// use geoengine_datatypes::primitives::Measurement;
    /// use std::f64;
    ///
    /// Histogram::builder(2, 0., 1., Measurement::Unitless).labels(vec!["foo".into(), "bar".into()])
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn labels(mut self, labels: Vec<String>) -> Self {
        self.labels = Some(labels);
        self
    }

    /// Add counts to the histogram
    pub fn counts(mut self, counts: Vec<u64>) -> Self {
        self.counts = Some(counts);
        self
    }

    /// Builds a histogram out of the collected parameters
    ///
    /// # Examples
    /// ```rust
    /// use geoengine_datatypes::plots::Histogram;
    /// use geoengine_datatypes::primitives::Measurement;
    ///
    /// Histogram::builder(2, 0., 1., Measurement::Unitless).build().unwrap();
    ///
    /// Histogram::builder(0, f64::NAN, f64::INFINITY, Measurement::Unitless).build().unwrap_err();
    /// ```
    ///
    /// # Errors
    ///
    /// This method fails if the `Histogram`'s preconditions are not met
    ///
    pub fn build(self) -> Result<Histogram> {
        Histogram::new(
            self.number_of_buckets,
            self.min,
            self.max,
            self.measurement,
            self.labels,
            self.counts,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::primitives::{CategoricalDataRef, FloatDataRef, IntDataRef};
    use arrow::array::{Array, Float64Builder, Int64Builder, UInt8Builder};
    use num_traits::AsPrimitive;

    #[test]
    fn bucket_for_value() {
        let histogram = Histogram::builder(2, 0., 1., Measurement::Unitless)
            .build()
            .unwrap();

        assert_eq!(histogram.bucket_for_value(0.), 0);
        assert_eq!(histogram.bucket_for_value(1.), 1);
        assert_eq!(histogram.bucket_for_value(0.5), 1);
        assert_eq!(histogram.bucket_for_value(0.49), 0);
    }

    #[test]
    fn add_feature_data_number() {
        let mut histogram = Histogram::builder(2, 0., 1., Measurement::Unitless)
            .build()
            .unwrap();

        let data = {
            let mut builder = Float64Builder::new(4);
            builder.append_slice(&[0., 0.49, 0.5, 1.0]).unwrap();
            builder.finish()
        };

        histogram
            .add_feature_data(FeatureDataRef::Float(FloatDataRef::new(
                data.values(),
                data.data().null_bitmap(),
            )))
            .unwrap();

        assert_eq!(histogram.counts[0], 2);
        assert_eq!(histogram.counts[1], 2);
    }

    #[test]
    fn add_feature_data_nullable_number() {
        let mut histogram = Histogram::builder(2, 0., 1., Measurement::Unitless)
            .build()
            .unwrap();

        let data = {
            let mut builder = Float64Builder::new(4);
            builder.append_value(0.).unwrap();
            builder.append_null().unwrap();
            builder.append_value(0.5).unwrap();
            builder.append_value(1.).unwrap();
            builder.finish()
        };

        histogram
            .add_feature_data(FeatureDataRef::Float(FloatDataRef::new(
                data.values(),
                data.data_ref().null_bitmap(),
            )))
            .unwrap();

        assert_eq!(histogram.counts[0], 1);
        assert_eq!(histogram.counts[1], 2);
        assert_eq!(histogram.nodata_count, 1);
    }

    #[test]
    fn add_feature_data_int() {
        let mut histogram = Histogram::builder(2, 0., 3., Measurement::Unitless)
            .build()
            .unwrap();

        let data = {
            let mut builder = Int64Builder::new(4);
            builder.append_slice(&[0, 1, 2, 3]).unwrap();
            builder.finish()
        };

        histogram
            .add_feature_data(FeatureDataRef::Int(IntDataRef::new(
                data.values(),
                data.data().null_bitmap(),
            )))
            .unwrap();

        assert_eq!(histogram.counts[0], 2);
        assert_eq!(histogram.counts[1], 2);
    }

    #[test]
    fn add_feature_data_categorical() {
        let mut histogram = Histogram::builder(2, 0., 1., Measurement::Unitless)
            .build()
            .unwrap();

        let data = {
            let mut builder = UInt8Builder::new(4);
            builder.append_slice(&[0, 1, 0, 0, 1]).unwrap();
            builder.finish()
        };

        histogram
            .add_feature_data(FeatureDataRef::Categorical(CategoricalDataRef::new(
                data.values(),
                data.data().null_bitmap(),
            )))
            .unwrap();

        assert_eq!(histogram.counts[0], 3);
        assert_eq!(histogram.counts[1], 2);
    }

    #[test]
    fn values_less_than_min() {
        let mut histogram = Histogram::builder(2, 0., 1., Measurement::Unitless)
            .build()
            .unwrap();

        let data = {
            let mut builder = Int64Builder::new(6);
            builder.append_slice(&[-1, -1, -1, 0, 1, 1]).unwrap();
            builder.finish()
        };

        histogram
            .add_feature_data(FeatureDataRef::Int(IntDataRef::new(
                data.values(),
                data.data().null_bitmap(),
            )))
            .unwrap();

        assert_eq!(histogram.nodata_count, 0);
        assert_eq!(histogram.counts[0], 1);
        assert_eq!(histogram.counts[1], 2);
    }

    #[test]
    fn to_vega_embeddable() {
        let mut histogram = Histogram::builder(2, 0., 1., Measurement::Unitless)
            .build()
            .unwrap();

        let data = {
            let mut builder = Float64Builder::new(4);
            builder.append_slice(&[0., 0.49, 0.5, 1.0]).unwrap();
            builder.finish()
        };

        histogram
            .add_feature_data(FeatureDataRef::Float(FloatDataRef::new(
                data.values(),
                data.data_ref().null_bitmap(),
            )))
            .unwrap();

        assert_eq!(
           histogram.to_vega_embeddable(false).unwrap(),
            PlotData {
                vega_string: r#"{"$schema":"https://vega.github.io/schema/vega-lite/v4.json","data":{"values":[{"bin_start":0.0,"bin_end":0.5,"Frequency":2},{"bin_start":0.5,"bin_end":1.0,"Frequency":2}]},"mark":"bar","encoding":{"x":{"field":"bin_start","bin":{"binned":true,"step":0.5},"axis":{"title":""}},"x2":{"field":"bin_end"},"y":{"field":"Frequency","type":"quantitative"}}}"#.to_owned(),
                metadata: PlotMetaData::None
            }
        );
        assert_eq!(
            histogram.to_vega_embeddable(true).unwrap(),
            PlotData {
                vega_string: r#"{"$schema":"https://vega.github.io/schema/vega-lite/v4.json","data":{"values":[{"bin_start":0.0,"bin_end":0.5,"Frequency":2},{"bin_start":0.5,"bin_end":1.0,"Frequency":2}]},"mark":"bar","encoding":{"x":{"field":"bin_start","bin":{"binned":true,"step":0.5},"axis":{"title":""}},"x2":{"field":"bin_end"},"y":{"field":"Frequency","type":"quantitative"}},"selection":{"range_selection":{"encodings":["x"],"type":"interval"}}}"#.to_owned(),
                metadata: PlotMetaData::Selection {
                    selection_name: "range_selection".to_string(),
                }
            }
        );
    }

    #[test]
    fn vega_many_buckets() {
        let number_of_buckets = 100;
        let counts = (0..number_of_buckets)
            .map(|v| {
                let v: f64 = v.as_();
                (v - 50.).powi(2).as_()
            })
            .collect::<Vec<u64>>();

        let histogram = Histogram::builder(
            number_of_buckets,
            0.,
            (number_of_buckets - 1).as_(),
            Measurement::Unitless,
        )
        .counts(counts)
        .build()
        .unwrap();

        assert_eq!(
            histogram.to_vega_embeddable(false).unwrap(),
            PlotData {
                vega_string: r#"{"$schema":"https://vega.github.io/schema/vega-lite/v4.json","data":{"values":[{"bin_start":0.0,"bin_end":0.99,"Frequency":2500},{"bin_start":0.99,"bin_end":1.98,"Frequency":2401},{"bin_start":1.98,"bin_end":2.9699999999999998,"Frequency":2304},{"bin_start":2.9699999999999998,"bin_end":3.96,"Frequency":2209},{"bin_start":3.96,"bin_end":4.95,"Frequency":2116},{"bin_start":4.95,"bin_end":5.94,"Frequency":2025},{"bin_start":5.94,"bin_end":6.930000000000001,"Frequency":1936},{"bin_start":6.930000000000001,"bin_end":7.920000000000001,"Frequency":1849},{"bin_start":7.920000000000001,"bin_end":8.91,"Frequency":1764},{"bin_start":8.91,"bin_end":9.9,"Frequency":1681},{"bin_start":9.9,"bin_end":10.89,"Frequency":1600},{"bin_start":10.89,"bin_end":11.88,"Frequency":1521},{"bin_start":11.88,"bin_end":12.870000000000001,"Frequency":1444},{"bin_start":12.870000000000001,"bin_end":13.860000000000001,"Frequency":1369},{"bin_start":13.860000000000001,"bin_end":14.850000000000001,"Frequency":1296},{"bin_start":14.850000000000001,"bin_end":15.840000000000002,"Frequency":1225},{"bin_start":15.840000000000002,"bin_end":16.830000000000002,"Frequency":1156},{"bin_start":16.830000000000002,"bin_end":17.82,"Frequency":1089},{"bin_start":17.82,"bin_end":18.81,"Frequency":1024},{"bin_start":18.81,"bin_end":19.799999999999997,"Frequency":961},{"bin_start":19.799999999999997,"bin_end":20.789999999999996,"Frequency":900},{"bin_start":20.789999999999996,"bin_end":21.779999999999994,"Frequency":841},{"bin_start":21.779999999999994,"bin_end":22.769999999999992,"Frequency":784},{"bin_start":22.769999999999992,"bin_end":23.75999999999999,"Frequency":729},{"bin_start":23.75999999999999,"bin_end":24.74999999999999,"Frequency":676},{"bin_start":24.74999999999999,"bin_end":25.739999999999988,"Frequency":625},{"bin_start":25.739999999999988,"bin_end":26.729999999999986,"Frequency":576},{"bin_start":26.729999999999986,"bin_end":27.719999999999985,"Frequency":529},{"bin_start":27.719999999999985,"bin_end":28.709999999999983,"Frequency":484},{"bin_start":28.709999999999983,"bin_end":29.69999999999998,"Frequency":441},{"bin_start":29.69999999999998,"bin_end":30.68999999999998,"Frequency":400},{"bin_start":30.68999999999998,"bin_end":31.67999999999998,"Frequency":361},{"bin_start":31.67999999999998,"bin_end":32.66999999999998,"Frequency":324},{"bin_start":32.66999999999998,"bin_end":33.65999999999998,"Frequency":289},{"bin_start":33.65999999999998,"bin_end":34.649999999999984,"Frequency":256},{"bin_start":34.649999999999984,"bin_end":35.639999999999986,"Frequency":225},{"bin_start":35.639999999999986,"bin_end":36.62999999999999,"Frequency":196},{"bin_start":36.62999999999999,"bin_end":37.61999999999999,"Frequency":169},{"bin_start":37.61999999999999,"bin_end":38.60999999999999,"Frequency":144},{"bin_start":38.60999999999999,"bin_end":39.599999999999994,"Frequency":121},{"bin_start":39.599999999999994,"bin_end":40.589999999999996,"Frequency":100},{"bin_start":40.589999999999996,"bin_end":41.58,"Frequency":81},{"bin_start":41.58,"bin_end":42.57,"Frequency":64},{"bin_start":42.57,"bin_end":43.56,"Frequency":49},{"bin_start":43.56,"bin_end":44.550000000000004,"Frequency":36},{"bin_start":44.550000000000004,"bin_end":45.540000000000006,"Frequency":25},{"bin_start":45.540000000000006,"bin_end":46.53000000000001,"Frequency":16},{"bin_start":46.53000000000001,"bin_end":47.52000000000001,"Frequency":9},{"bin_start":47.52000000000001,"bin_end":48.51000000000001,"Frequency":4},{"bin_start":48.51000000000001,"bin_end":49.500000000000014,"Frequency":1},{"bin_start":49.500000000000014,"bin_end":50.490000000000016,"Frequency":0},{"bin_start":50.490000000000016,"bin_end":51.48000000000002,"Frequency":1},{"bin_start":51.48000000000002,"bin_end":52.47000000000002,"Frequency":4},{"bin_start":52.47000000000002,"bin_end":53.46000000000002,"Frequency":9},{"bin_start":53.46000000000002,"bin_end":54.450000000000024,"Frequency":16},{"bin_start":54.450000000000024,"bin_end":55.440000000000026,"Frequency":25},{"bin_start":55.440000000000026,"bin_end":56.43000000000003,"Frequency":36},{"bin_start":56.43000000000003,"bin_end":57.42000000000003,"Frequency":49},{"bin_start":57.42000000000003,"bin_end":58.41000000000003,"Frequency":64},{"bin_start":58.41000000000003,"bin_end":59.400000000000034,"Frequency":81},{"bin_start":59.400000000000034,"bin_end":60.390000000000036,"Frequency":100},{"bin_start":60.390000000000036,"bin_end":61.38000000000004,"Frequency":121},{"bin_start":61.38000000000004,"bin_end":62.37000000000004,"Frequency":144},{"bin_start":62.37000000000004,"bin_end":63.36000000000004,"Frequency":169},{"bin_start":63.36000000000004,"bin_end":64.35000000000004,"Frequency":196},{"bin_start":64.35000000000004,"bin_end":65.34000000000003,"Frequency":225},{"bin_start":65.34000000000003,"bin_end":66.33000000000003,"Frequency":256},{"bin_start":66.33000000000003,"bin_end":67.32000000000002,"Frequency":289},{"bin_start":67.32000000000002,"bin_end":68.31000000000002,"Frequency":324},{"bin_start":68.31000000000002,"bin_end":69.30000000000001,"Frequency":361},{"bin_start":69.30000000000001,"bin_end":70.29,"Frequency":400},{"bin_start":70.29,"bin_end":71.28,"Frequency":441},{"bin_start":71.28,"bin_end":72.27,"Frequency":484},{"bin_start":72.27,"bin_end":73.25999999999999,"Frequency":529},{"bin_start":73.25999999999999,"bin_end":74.24999999999999,"Frequency":576},{"bin_start":74.24999999999999,"bin_end":75.23999999999998,"Frequency":625},{"bin_start":75.23999999999998,"bin_end":76.22999999999998,"Frequency":676},{"bin_start":76.22999999999998,"bin_end":77.21999999999997,"Frequency":729},{"bin_start":77.21999999999997,"bin_end":78.20999999999997,"Frequency":784},{"bin_start":78.20999999999997,"bin_end":79.19999999999996,"Frequency":841},{"bin_start":79.19999999999996,"bin_end":80.18999999999996,"Frequency":900},{"bin_start":80.18999999999996,"bin_end":81.17999999999995,"Frequency":961},{"bin_start":81.17999999999995,"bin_end":82.16999999999994,"Frequency":1024},{"bin_start":82.16999999999994,"bin_end":83.15999999999994,"Frequency":1089},{"bin_start":83.15999999999994,"bin_end":84.14999999999993,"Frequency":1156},{"bin_start":84.14999999999993,"bin_end":85.13999999999993,"Frequency":1225},{"bin_start":85.13999999999993,"bin_end":86.12999999999992,"Frequency":1296},{"bin_start":86.12999999999992,"bin_end":87.11999999999992,"Frequency":1369},{"bin_start":87.11999999999992,"bin_end":88.10999999999991,"Frequency":1444},{"bin_start":88.10999999999991,"bin_end":89.09999999999991,"Frequency":1521},{"bin_start":89.09999999999991,"bin_end":90.0899999999999,"Frequency":1600},{"bin_start":90.0899999999999,"bin_end":91.0799999999999,"Frequency":1681},{"bin_start":91.0799999999999,"bin_end":92.0699999999999,"Frequency":1764},{"bin_start":92.0699999999999,"bin_end":93.05999999999989,"Frequency":1849},{"bin_start":93.05999999999989,"bin_end":94.04999999999988,"Frequency":1936},{"bin_start":94.04999999999988,"bin_end":95.03999999999988,"Frequency":2025},{"bin_start":95.03999999999988,"bin_end":96.02999999999987,"Frequency":2116},{"bin_start":96.02999999999987,"bin_end":97.01999999999987,"Frequency":2209},{"bin_start":97.01999999999987,"bin_end":98.00999999999986,"Frequency":2304},{"bin_start":98.00999999999986,"bin_end":98.99999999999986,"Frequency":2401}]},"mark":"bar","encoding":{"x":{"field":"bin_start","bin":{"binned":true,"step":0.99},"axis":{"title":""}},"x2":{"field":"bin_end"},"y":{"field":"Frequency","type":"quantitative"}}}"#.to_owned(),
                metadata: PlotMetaData::None
            }
        );
    }
}
