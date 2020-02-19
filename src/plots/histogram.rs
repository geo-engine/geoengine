use crate::error;
use crate::plots::{Plot, PlotData};
use crate::primitives::{DataRef, FeatureDataRef, Measurement, NullableDataRef};
use crate::util::Result;
use float_cmp::*;
use ndarray::{stack, Array, Array1, Axis};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::collections::HashMap;
use std::iter::FromIterator;
use vega_lite_3::{
    BinEnum, EncodingBuilder, Mark, Padding, SelectionDefBuilder, SelectionDefType,
    SingleDefUnitChannel, StandardType, VegaliteBuilder, X2ClassBuilder, XClassBuilder,
    YClassBuilder,
};

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
    ) -> Result<Self> {
        ensure!(
            number_of_buckets > 0,
            error::PlotError {
                details: "Histograms must have at least one bucket"
            }
        );
        ensure!(
            min.is_finite() && max.is_finite(),
            error::PlotError {
                details: "Histograms must have finite min/max values"
            }
        );
        ensure!(
            min < max || (approx_eq!(f64, min, max) && number_of_buckets == 1),
            error::PlotError {
                details: "Histograms max value must be larger than its min value"
            }
        );
        if let Some(labels) = &labels {
            ensure!(
                labels.len() == number_of_buckets,
                error::PlotError {
                    details: "Histogram must have as many labels as buckets"
                }
            );
        }

        Ok(Self {
            counts: vec![0; number_of_buckets],
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

    pub fn add_feature_data(&mut self, data: FeatureDataRef) -> Result<()> {
        // TODO: implement efficiently OpenCL version
        match data {
            FeatureDataRef::Number(number_ref) => {
                for &value in number_ref.data() {
                    self.handle_data_item(value);
                }
            }
            FeatureDataRef::NullableNumber(number_ref) => {
                for (&value, is_null) in number_ref.data().iter().zip(number_ref.nulls()) {
                    self.handle_nullable_data_item(value, is_null);
                }
            }
            FeatureDataRef::Decimal(decimal_ref) => {
                for value in decimal_ref.data().iter().map(|&v| v as f64) {
                    self.handle_data_item(value);
                }
            }
            FeatureDataRef::NullableDecimal(decimal_ref) => {
                for (value, is_null) in decimal_ref
                    .data()
                    .iter()
                    .map(|&v| v as f64)
                    .zip(decimal_ref.nulls())
                {
                    self.handle_nullable_data_item(value, is_null);
                }
            }
            FeatureDataRef::Categorical(categorical_ref) => {
                for value in categorical_ref.data().iter().map(|&v| v as f64) {
                    self.handle_data_item(value);
                }
            }
            FeatureDataRef::NullableCategorical(categorical_ref) => {
                for (value, is_null) in categorical_ref
                    .data()
                    .iter()
                    .map(|&v| v as f64)
                    .zip(categorical_ref.nulls())
                {
                    self.handle_nullable_data_item(value, is_null);
                }
            }
            _ => {
                return error::PlotError {
                    details: "Cannot add non-numerical data to the histogram.",
                }
                .fail();
            }
        }

        Ok(())
    }

    fn handle_data_item(&mut self, value: f64) {
        if value < self.min || value > self.max {
            self.nodata_count += 1;
        } else {
            let bucket = self.bucket_for_value(value);
            self.counts[bucket] += 1;
        }
    }

    fn handle_nullable_data_item(&mut self, value: f64, is_null: bool) {
        if is_null || value < self.min || value > self.max {
            self.nodata_count += 1;
        } else {
            let bucket = self.bucket_for_value(value);
            self.counts[bucket] += 1;
        }
    }

    fn bucket_for_value(&self, value: f64) -> usize {
        if self.counts.len() == 1 {
            return 0;
        }

        let fraction = (value - self.min) / (self.max - self.min);
        let bucket = (fraction * (self.counts.len() as f64)) as usize;

        bucket.min(self.counts.len() - 1)
    }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct EmbeddingMetaData {
    pub selection_name: Option<String>,
}

impl Plot for Histogram {
    type PlotDataMetadataType = EmbeddingMetaData;

    /// Embeddable histogram
    ///
    /// ```rust
    /// use geoengine_datatypes::plots::{Histogram, Plot, PlotData};
    /// use geoengine_datatypes::plots::histogram::EmbeddingMetaData;
    /// use geoengine_datatypes::primitives::{Measurement, FeatureDataRef, NumberDataRef};
    /// use arrow::array::Float64Builder;
    ///
    /// let mut histogram = Histogram::builder(2, 0., 1., Measurement::Unitless).build().unwrap();
    ///
    /// let data = {
    ///     let mut builder = Float64Builder::new(4);
    ///     builder.append_slice(&[0., 0.49, 0.5, 1.0]).unwrap();
    ///     builder.finish()
    /// };
    ///
    /// histogram
    ///     .add_feature_data(FeatureDataRef::Number(NumberDataRef::new(data.values())))
    ///     .unwrap();
    ///
    /// assert_eq!(
    ///     histogram.to_vega_embeddable(false).unwrap(),
    ///     PlotData {
    ///         vega_string: r#"{"$schema":"https://vega.github.io/schema/vega-lite/v3.4.0.json","data":{"values":[{"v":1,"dim":[3],"data":[2.0,0.0,0.5]},{"v":1,"dim":[3],"data":[2.0,0.5,1.0]}]},"encoding":{"x":{"bin":"binned","field":"data.0","title":"","type":"quantitative"},"x2":{"field":"data.1"},"y":{"field":"data.2","title":"Frequency","type":"quantitative"}},"mark":"bar","padding":5.0}"#.to_string(),
    ///         metadata: EmbeddingMetaData {
    ///             selection_name: None,
    ///         }
    ///     }
    /// );
    /// assert_eq!(
    ///     histogram.to_vega_embeddable(true).unwrap(),
    ///     PlotData {
    ///         vega_string: r#"{"$schema":"https://vega.github.io/schema/vega-lite/v3.4.0.json","data":{"values":[{"v":1,"dim":[3],"data":[2.0,0.0,0.5]},{"v":1,"dim":[3],"data":[2.0,0.5,1.0]}]},"encoding":{"x":{"bin":"binned","field":"data.0","title":"","type":"quantitative"},"x2":{"field":"data.1"},"y":{"field":"data.2","title":"Frequency","type":"quantitative"}},"mark":"bar","padding":5.0,"selection":{"range_selection":{"encodings":["x"],"type":"interval"}}}"#.to_string(),
    ///         metadata: EmbeddingMetaData {
    ///             selection_name: Some("range_selection".to_string()),
    ///         }
    ///     }
    /// );
    /// ```
    fn to_vega_embeddable(
        &self,
        allow_interactions: bool,
    ) -> Result<PlotData<Self::PlotDataMetadataType>> {
        let bucket_counts = Array1::<f64>::from_iter(self.counts.iter().map(|&v| v as f64));

        let step = (self.max - self.min) / (self.counts.len() as f64);

        let bucket_starts = Array::linspace(self.min, self.max - step, self.counts.len());
        let bucket_ends = Array::linspace(self.min + step, self.max, self.counts.len());

        let values = stack(
            Axis(0),
            &[
                bucket_counts.view(),
                bucket_starts.view(),
                bucket_ends.view(),
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
                EncodingBuilder::default()
                    .x(XClassBuilder::default()
                        .field("data.0")
                        .title(self.measurement.to_string())
                        .def_type(StandardType::Quantitative)
                        .bin(BinEnum::Binned)
                        .build()
                        .unwrap())
                    .x2(X2ClassBuilder::default().field("data.1").build().unwrap())
                    .y(YClassBuilder::default()
                        .field("data.2")
                        .title("Frequency")
                        .def_type(StandardType::Quantitative)
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
            metadata: EmbeddingMetaData { selection_name },
        })
    }

    fn to_png(&self, _width_px: u16, _height_px: u16) -> Vec<u8> {
        todo!("keep track of https://github.com/procyon-rs/vega_lite_3.rs/issues/18")
    }
}

pub struct HistogramBuilder {
    number_of_buckets: usize,
    min: f64,
    max: f64,
    measurement: Measurement,
    labels: Option<Vec<String>>,
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
    pub fn build(self) -> Result<Histogram> {
        Histogram::new(
            self.number_of_buckets,
            self.min,
            self.max,
            self.measurement,
            self.labels,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::{
        CategoricalDataRef, DecimalDataRef, NullableNumberDataRef, NumberDataRef,
    };
    use arrow::array::{Array, Float64Builder, Int64Builder, UInt8Builder};

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
            .add_feature_data(FeatureDataRef::Number(NumberDataRef::new(data.values())))
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
            .add_feature_data(FeatureDataRef::NullableNumber(NullableNumberDataRef::new(
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
            .add_feature_data(FeatureDataRef::Decimal(DecimalDataRef::new(data.values())))
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
            )))
            .unwrap();

        assert_eq!(histogram.counts[0], 3);
        assert_eq!(histogram.counts[1], 2);
    }
}
