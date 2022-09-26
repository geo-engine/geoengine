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
#[serde(rename_all = "camelCase")]
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
        // TODO: implement efficiently multi-threaded version
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
            FeatureDataRef::Category(category_ref) if !category_ref.has_nulls() => {
                for value in category_ref.as_ref().iter().map(|&v| f64::from(v)) {
                    self.handle_data_item(value, false);
                }
            }
            FeatureDataRef::Category(category_ref) => {
                for (value, is_null) in category_ref
                    .as_ref()
                    .iter()
                    .map(|&v| f64::from(v))
                    .zip(category_ref.nulls())
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
            FeatureDataRef::Bool(value_ref) if !value_ref.has_nulls() => {
                for value in value_ref.as_ref().iter().map(|&v| f64::from(u8::from(v))) {
                    self.handle_data_item(value, false);
                }
            }
            FeatureDataRef::Bool(value_ref) => {
                for (value, is_null) in value_ref
                    .as_ref()
                    .iter()
                    .map(|&v| f64::from(u8::from(v)))
                    .zip(value_ref.nulls())
                {
                    self.handle_data_item(value, is_null);
                }
            }
            FeatureDataRef::DateTime(value_ref) if !value_ref.has_nulls() => {
                for value in value_ref.as_ref().iter().map(|&v| v.inner() as f64) {
                    self.handle_data_item(value, false);
                }
            }
            FeatureDataRef::DateTime(value_ref) => {
                for (value, is_null) in value_ref
                    .as_ref()
                    .iter()
                    .map(|&v| v.inner() as f64)
                    .zip(value_ref.nulls())
                {
                    self.handle_data_item(value, is_null);
                }
            }
        }

        Ok(())
    }

    /// Add raster data to the histogram
    pub fn add_raster_data<P: Pixel, I: Iterator<Item = Option<P>>>(&mut self, data: I) {
        data.for_each(|pixel_option| {
            if let Some(p) = pixel_option {
                self.handle_data_item(p.as_(), false);
            } else {
                self.inc_nodata_count();
            }
        });
    }

    pub fn add_nodata_batch(&mut self, nodata_count: u64) {
        self.nodata_count += nodata_count;
    }

    pub fn inc_nodata_count(&mut self) {
        self.nodata_count += 1;
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
        let mut step = (self.max - self.min) / (self.counts.len() as f64);

        let mut values = Vec::with_capacity(self.counts.len());
        let mut bin_start = self.min;
        for &count in &self.counts {
            let bin_end = bin_start + step;
            values.push(serde_json::json!({
                "binStart": bin_start,
                "binEnd": bin_end,
                "Frequency": count,
            }));
            bin_start = bin_end;
        }

        // step in spec must not be 0, so add a fake step
        if step == 0. {
            step = 1.;
        }

        let mut vega_spec = serde_json::json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
            "data": {
                "values": values,
            },
            "mark": "bar",
            "encoding": {
                "x": {
                    "field": "binStart",
                    "bin": {
                        "binned": true,
                        "step": step,
                    },
                    "axis": {
                        "title": self.measurement.to_string(),
                    },
                },
                "x2": {
                    "field": "binEnd",
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
    #[must_use]
    pub fn labels(mut self, labels: Vec<String>) -> Self {
        self.labels = Some(labels);
        self
    }

    /// Add counts to the histogram
    #[must_use]
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

    use crate::primitives::{CategoryDataRef, FloatDataRef, IntDataRef};
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
            let mut builder = Float64Builder::with_capacity(4);
            builder.append_slice(&[0., 0.49, 0.5, 1.0]);
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
            let mut builder = Float64Builder::with_capacity(4);
            builder.append_value(0.);
            builder.append_null();
            builder.append_value(0.5);
            builder.append_value(1.);
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
            let mut builder = Int64Builder::with_capacity(4);
            builder.append_slice(&[0, 1, 2, 3]);
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
    fn add_feature_data_category() {
        let mut histogram = Histogram::builder(2, 0., 1., Measurement::Unitless)
            .build()
            .unwrap();

        let data = {
            let mut builder = UInt8Builder::with_capacity(4);
            builder.append_slice(&[0, 1, 0, 0, 1]);
            builder.finish()
        };

        histogram
            .add_feature_data(FeatureDataRef::Category(CategoryDataRef::new(
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
            let mut builder = Int64Builder::with_capacity(6);
            builder.append_slice(&[-1, -1, -1, 0, 1, 1]);
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
            let mut builder = Float64Builder::with_capacity(4);
            builder.append_slice(&[0., 0.49, 0.5, 1.0]);
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
                vega_string: r#"{"$schema":"https://vega.github.io/schema/vega-lite/v4.json","data":{"values":[{"Frequency":2,"binEnd":0.5,"binStart":0.0},{"Frequency":2,"binEnd":1.0,"binStart":0.5}]},"encoding":{"x":{"axis":{"title":""},"bin":{"binned":true,"step":0.5},"field":"binStart"},"x2":{"field":"binEnd"},"y":{"field":"Frequency","type":"quantitative"}},"mark":"bar"}"#.to_owned(),
                metadata: PlotMetaData::None
            }
        );
        assert_eq!(
            histogram.to_vega_embeddable(true).unwrap(),
            PlotData {
                vega_string: r#"{"$schema":"https://vega.github.io/schema/vega-lite/v4.json","data":{"values":[{"Frequency":2,"binEnd":0.5,"binStart":0.0},{"Frequency":2,"binEnd":1.0,"binStart":0.5}]},"encoding":{"x":{"axis":{"title":""},"bin":{"binned":true,"step":0.5},"field":"binStart"},"x2":{"field":"binEnd"},"y":{"field":"Frequency","type":"quantitative"}},"mark":"bar","selection":{"range_selection":{"encodings":["x"],"type":"interval"}}}"#.to_owned(),
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
                vega_string: r#"{"$schema":"https://vega.github.io/schema/vega-lite/v4.json","data":{"values":[{"Frequency":2500,"binEnd":0.99,"binStart":0.0},{"Frequency":2401,"binEnd":1.98,"binStart":0.99},{"Frequency":2304,"binEnd":2.9699999999999998,"binStart":1.98},{"Frequency":2209,"binEnd":3.96,"binStart":2.9699999999999998},{"Frequency":2116,"binEnd":4.95,"binStart":3.96},{"Frequency":2025,"binEnd":5.94,"binStart":4.95},{"Frequency":1936,"binEnd":6.930000000000001,"binStart":5.94},{"Frequency":1849,"binEnd":7.920000000000001,"binStart":6.930000000000001},{"Frequency":1764,"binEnd":8.91,"binStart":7.920000000000001},{"Frequency":1681,"binEnd":9.9,"binStart":8.91},{"Frequency":1600,"binEnd":10.89,"binStart":9.9},{"Frequency":1521,"binEnd":11.88,"binStart":10.89},{"Frequency":1444,"binEnd":12.870000000000001,"binStart":11.88},{"Frequency":1369,"binEnd":13.860000000000001,"binStart":12.870000000000001},{"Frequency":1296,"binEnd":14.850000000000001,"binStart":13.860000000000001},{"Frequency":1225,"binEnd":15.840000000000002,"binStart":14.850000000000001},{"Frequency":1156,"binEnd":16.830000000000002,"binStart":15.840000000000002},{"Frequency":1089,"binEnd":17.82,"binStart":16.830000000000002},{"Frequency":1024,"binEnd":18.81,"binStart":17.82},{"Frequency":961,"binEnd":19.799999999999997,"binStart":18.81},{"Frequency":900,"binEnd":20.789999999999996,"binStart":19.799999999999997},{"Frequency":841,"binEnd":21.779999999999994,"binStart":20.789999999999996},{"Frequency":784,"binEnd":22.769999999999992,"binStart":21.779999999999994},{"Frequency":729,"binEnd":23.75999999999999,"binStart":22.769999999999992},{"Frequency":676,"binEnd":24.74999999999999,"binStart":23.75999999999999},{"Frequency":625,"binEnd":25.739999999999988,"binStart":24.74999999999999},{"Frequency":576,"binEnd":26.729999999999986,"binStart":25.739999999999988},{"Frequency":529,"binEnd":27.719999999999985,"binStart":26.729999999999986},{"Frequency":484,"binEnd":28.709999999999983,"binStart":27.719999999999985},{"Frequency":441,"binEnd":29.69999999999998,"binStart":28.709999999999983},{"Frequency":400,"binEnd":30.68999999999998,"binStart":29.69999999999998},{"Frequency":361,"binEnd":31.67999999999998,"binStart":30.68999999999998},{"Frequency":324,"binEnd":32.66999999999998,"binStart":31.67999999999998},{"Frequency":289,"binEnd":33.65999999999998,"binStart":32.66999999999998},{"Frequency":256,"binEnd":34.649999999999984,"binStart":33.65999999999998},{"Frequency":225,"binEnd":35.639999999999986,"binStart":34.649999999999984},{"Frequency":196,"binEnd":36.62999999999999,"binStart":35.639999999999986},{"Frequency":169,"binEnd":37.61999999999999,"binStart":36.62999999999999},{"Frequency":144,"binEnd":38.60999999999999,"binStart":37.61999999999999},{"Frequency":121,"binEnd":39.599999999999994,"binStart":38.60999999999999},{"Frequency":100,"binEnd":40.589999999999996,"binStart":39.599999999999994},{"Frequency":81,"binEnd":41.58,"binStart":40.589999999999996},{"Frequency":64,"binEnd":42.57,"binStart":41.58},{"Frequency":49,"binEnd":43.56,"binStart":42.57},{"Frequency":36,"binEnd":44.550000000000004,"binStart":43.56},{"Frequency":25,"binEnd":45.540000000000006,"binStart":44.550000000000004},{"Frequency":16,"binEnd":46.53000000000001,"binStart":45.540000000000006},{"Frequency":9,"binEnd":47.52000000000001,"binStart":46.53000000000001},{"Frequency":4,"binEnd":48.51000000000001,"binStart":47.52000000000001},{"Frequency":1,"binEnd":49.500000000000014,"binStart":48.51000000000001},{"Frequency":0,"binEnd":50.490000000000016,"binStart":49.500000000000014},{"Frequency":1,"binEnd":51.48000000000002,"binStart":50.490000000000016},{"Frequency":4,"binEnd":52.47000000000002,"binStart":51.48000000000002},{"Frequency":9,"binEnd":53.46000000000002,"binStart":52.47000000000002},{"Frequency":16,"binEnd":54.450000000000024,"binStart":53.46000000000002},{"Frequency":25,"binEnd":55.440000000000026,"binStart":54.450000000000024},{"Frequency":36,"binEnd":56.43000000000003,"binStart":55.440000000000026},{"Frequency":49,"binEnd":57.42000000000003,"binStart":56.43000000000003},{"Frequency":64,"binEnd":58.41000000000003,"binStart":57.42000000000003},{"Frequency":81,"binEnd":59.400000000000034,"binStart":58.41000000000003},{"Frequency":100,"binEnd":60.390000000000036,"binStart":59.400000000000034},{"Frequency":121,"binEnd":61.38000000000004,"binStart":60.390000000000036},{"Frequency":144,"binEnd":62.37000000000004,"binStart":61.38000000000004},{"Frequency":169,"binEnd":63.36000000000004,"binStart":62.37000000000004},{"Frequency":196,"binEnd":64.35000000000004,"binStart":63.36000000000004},{"Frequency":225,"binEnd":65.34000000000003,"binStart":64.35000000000004},{"Frequency":256,"binEnd":66.33000000000003,"binStart":65.34000000000003},{"Frequency":289,"binEnd":67.32000000000002,"binStart":66.33000000000003},{"Frequency":324,"binEnd":68.31000000000002,"binStart":67.32000000000002},{"Frequency":361,"binEnd":69.30000000000001,"binStart":68.31000000000002},{"Frequency":400,"binEnd":70.29,"binStart":69.30000000000001},{"Frequency":441,"binEnd":71.28,"binStart":70.29},{"Frequency":484,"binEnd":72.27,"binStart":71.28},{"Frequency":529,"binEnd":73.25999999999999,"binStart":72.27},{"Frequency":576,"binEnd":74.24999999999999,"binStart":73.25999999999999},{"Frequency":625,"binEnd":75.23999999999998,"binStart":74.24999999999999},{"Frequency":676,"binEnd":76.22999999999998,"binStart":75.23999999999998},{"Frequency":729,"binEnd":77.21999999999997,"binStart":76.22999999999998},{"Frequency":784,"binEnd":78.20999999999997,"binStart":77.21999999999997},{"Frequency":841,"binEnd":79.19999999999996,"binStart":78.20999999999997},{"Frequency":900,"binEnd":80.18999999999996,"binStart":79.19999999999996},{"Frequency":961,"binEnd":81.17999999999995,"binStart":80.18999999999996},{"Frequency":1024,"binEnd":82.16999999999994,"binStart":81.17999999999995},{"Frequency":1089,"binEnd":83.15999999999994,"binStart":82.16999999999994},{"Frequency":1156,"binEnd":84.14999999999993,"binStart":83.15999999999994},{"Frequency":1225,"binEnd":85.13999999999993,"binStart":84.14999999999993},{"Frequency":1296,"binEnd":86.12999999999992,"binStart":85.13999999999993},{"Frequency":1369,"binEnd":87.11999999999992,"binStart":86.12999999999992},{"Frequency":1444,"binEnd":88.10999999999991,"binStart":87.11999999999992},{"Frequency":1521,"binEnd":89.09999999999991,"binStart":88.10999999999991},{"Frequency":1600,"binEnd":90.0899999999999,"binStart":89.09999999999991},{"Frequency":1681,"binEnd":91.0799999999999,"binStart":90.0899999999999},{"Frequency":1764,"binEnd":92.0699999999999,"binStart":91.0799999999999},{"Frequency":1849,"binEnd":93.05999999999989,"binStart":92.0699999999999},{"Frequency":1936,"binEnd":94.04999999999988,"binStart":93.05999999999989},{"Frequency":2025,"binEnd":95.03999999999988,"binStart":94.04999999999988},{"Frequency":2116,"binEnd":96.02999999999987,"binStart":95.03999999999988},{"Frequency":2209,"binEnd":97.01999999999987,"binStart":96.02999999999987},{"Frequency":2304,"binEnd":98.00999999999986,"binStart":97.01999999999987},{"Frequency":2401,"binEnd":98.99999999999986,"binStart":98.00999999999986}]},"encoding":{"x":{"axis":{"title":""},"bin":{"binned":true,"step":0.99},"field":"binStart"},"x2":{"field":"binEnd"},"y":{"field":"Frequency","type":"quantitative"}},"mark":"bar"}"#.to_owned(),
                metadata: PlotMetaData::None
            }
        );
    }

    #[test]
    fn empty_histogram() {
        assert_eq!(
            HistogramBuilder::new(1, 0., 0., Measurement::continuous("foo".to_string(), Some("bar".to_string()))).build().unwrap().to_vega_embeddable(false).unwrap(),
            PlotData {
                vega_string: r#"{"$schema":"https://vega.github.io/schema/vega-lite/v4.json","data":{"values":[{"Frequency":0,"binEnd":0.0,"binStart":0.0}]},"encoding":{"x":{"axis":{"title":"foo in bar"},"bin":{"binned":true,"step":1.0},"field":"binStart"},"x2":{"field":"binEnd"},"y":{"field":"Frequency","type":"quantitative"}},"mark":"bar"}"#.to_owned(),
                metadata: PlotMetaData::None
            }
        );
    }
}
