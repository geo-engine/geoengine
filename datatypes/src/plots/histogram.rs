use std::cmp;
use std::collections::HashMap;

use float_cmp::*;
use ndarray::{stack, Array, Array1, Axis};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use vega_lite_4::{
    BinEnum, EdEncodingBuilder, Mark, Padding, SelectionDefBuilder, SelectionDefType,
    SingleDefUnitChannel, Type, VegaliteBuilder, X2ClassBuilder, XClassBuilder, YClassBuilder,
};

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
    /// use std::f64;
    ///
    /// Histogram::builder(2, 0., 1., Measurement::Unitless).build().unwrap();
    ///
    /// Histogram::builder(0, f64::NAN, f64::INFINITY, Measurement::Unitless).build().unwrap_err();
    /// ```
    ///
    /// ```rust
    /// use geoengine_datatypes::plots::Histogram;
    /// use geoengine_datatypes::primitives::Measurement;
    /// use std::f64;
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
            FeatureDataRef::Number(number_ref) if !number_ref.has_nulls() => {
                for &value in number_ref.as_ref() {
                    self.handle_data_item(value, false);
                }
            }
            FeatureDataRef::Number(number_ref) => {
                for (&value, is_null) in number_ref.as_ref().iter().zip(number_ref.nulls()) {
                    self.handle_data_item(value, is_null);
                }
            }
            FeatureDataRef::Decimal(decimal_ref) if !decimal_ref.has_nulls() => {
                for value in decimal_ref.as_ref().iter().map(|&v| v as f64) {
                    self.handle_data_item(value, false);
                }
            }
            FeatureDataRef::Decimal(decimal_ref) => {
                for (value, is_null) in decimal_ref
                    .as_ref()
                    .iter()
                    .map(|&v| v as f64)
                    .zip(decimal_ref.nulls())
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
        let bucket_counts: Array1<f64> = self.counts.iter().map(|&v| v as f64).collect();

        let step = (self.max - self.min) / (self.counts.len() as f64);

        let bucket_starts = Array::linspace(self.min, self.max - step, self.counts.len());
        let bucket_ends = Array::linspace(self.min + step, self.max, self.counts.len());

        let values = stack(
            Axis(0),
            &[
                bucket_starts.view(),
                bucket_ends.view(),
                bucket_counts.view(),
            ],
        )
        .unwrap()
        .into_shape((3, self.counts.len())) // requires transpose in next step
        .unwrap();

        let mut builder = VegaliteBuilder::default();
        builder
            .padding(Padding::Double(5.0))
            .data(values.t())
            .mark(Mark::Bar)
            .encoding(
                EdEncodingBuilder::default()
                    .x(XClassBuilder::default()
                        .field("data.0")
                        .title(self.measurement.to_string().as_str())
                        .position_def_type(Type::Quantitative)
                        .bin(BinEnum::Binned)
                        .build()
                        .unwrap())
                    .x2(X2ClassBuilder::default().field("data.1").build().unwrap())
                    .y(YClassBuilder::default()
                        .field("data.2")
                        .title("Frequency")
                        .position_def_type(Type::Quantitative)
                        .build()
                        .unwrap())
                    .build()
                    .unwrap(),
            );

        let selection_name = if allow_interactions {
            let name = "range_selection".to_string();

            let mut selector = HashMap::new();
            selector.insert(
                name.clone(),
                SelectionDefBuilder::default()
                    .encodings(vec![SingleDefUnitChannel::X])
                    .selection_def_type(SelectionDefType::Interval)
                    .build()
                    .unwrap(),
            );

            builder.selection(selector);

            Some(name)
        } else {
            None
        };

        let chart = builder.build().unwrap();

        Ok(PlotData {
            vega_string: chart.to_string().unwrap(),
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
    /// use std::f64;
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
    use arrow::array::{Array, Float64Builder, Int64Builder, UInt8Builder};

    use crate::primitives::{CategoricalDataRef, DecimalDataRef, NumberDataRef};

    use super::*;

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
            .add_feature_data(FeatureDataRef::Number(NumberDataRef::new(
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
            .add_feature_data(FeatureDataRef::Number(NumberDataRef::new(
                data.values(),
                data.data_ref().null_bitmap(),
            )))
            .unwrap();

        assert_eq!(histogram.counts[0], 1);
        assert_eq!(histogram.counts[1], 2);
        assert_eq!(histogram.nodata_count, 1);
    }

    #[test]
    fn add_feature_data_decimal() {
        let mut histogram = Histogram::builder(2, 0., 3., Measurement::Unitless)
            .build()
            .unwrap();

        let data = {
            let mut builder = Int64Builder::new(4);
            builder.append_slice(&[0, 1, 2, 3]).unwrap();
            builder.finish()
        };

        histogram
            .add_feature_data(FeatureDataRef::Decimal(DecimalDataRef::new(
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
            .add_feature_data(FeatureDataRef::Decimal(DecimalDataRef::new(
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
            .add_feature_data(FeatureDataRef::Number(NumberDataRef::new(
                data.values(),
                data.data_ref().null_bitmap(),
            )))
            .unwrap();

        assert_eq!(
        histogram.to_vega_embeddable(false).unwrap(),
        PlotData {
            vega_string: r#"{"$schema":"https://vega.github.io/schema/vega-lite/v4.17.0.json","data":{"values":[{"v":1,"dim":[3],"data":[0.0,0.5,2.0]},{"v":1,"dim":[3],"data":[0.5,1.0,2.0]}]},"encoding":{"x":{"bin":"binned","field":"data.0","title":"","type":"quantitative"},"x2":{"field":"data.1"},"y":{"field":"data.2","title":"Frequency","type":"quantitative"}},"mark":"bar","padding":5.0}"#.to_string(),
            metadata: PlotMetaData::None
        }
    );
        assert_eq!(
        histogram.to_vega_embeddable(true).unwrap(),
        PlotData {
            vega_string: r#"{"$schema":"https://vega.github.io/schema/vega-lite/v4.17.0.json","data":{"values":[{"v":1,"dim":[3],"data":[0.0,0.5,2.0]},{"v":1,"dim":[3],"data":[0.5,1.0,2.0]}]},"encoding":{"x":{"bin":"binned","field":"data.0","title":"","type":"quantitative"},"x2":{"field":"data.1"},"y":{"field":"data.2","title":"Frequency","type":"quantitative"}},"mark":"bar","padding":5.0,"selection":{"range_selection":{"encodings":["x"],"type":"interval"}}}"#.to_string(),
            metadata: PlotMetaData::Selection {
                selection_name: "range_selection".to_string(),
            }
        }
    );
    }
}
