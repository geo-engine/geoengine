use crate::error;
use crate::plots::{Plot, PlotData, PlotMetaData};
use crate::primitives::{Measurement, TimeInstance};
use crate::util::Result;
use snafu::ensure;

pub struct AreaLineChart {
    timestamps: Vec<TimeInstance>,
    values: Vec<f64>,
    measurement: Measurement,
    draw_area: bool,
}

impl AreaLineChart {
    pub fn new(
        timestamps: Vec<TimeInstance>,
        values: Vec<f64>,
        measurement: Measurement,
        draw_area: bool,
    ) -> Result<Self> {
        ensure!(
            timestamps.len() == values.len(),
            error::Plot {
                details: "`timestamps` length must equal `values` length"
            }
        );

        Ok(Self {
            timestamps,
            values,
            measurement,
            draw_area,
        })
    }
}

impl Plot for AreaLineChart {
    fn to_vega_embeddable(&self, _allow_interactions: bool) -> Result<PlotData> {
        let data = self
            .timestamps
            .iter()
            .zip(&self.values)
            .map(|(timestamp, value)| {
                serde_json::json!({
                    "x": timestamp.as_rfc3339(),
                    "y": value,
                })
            })
            .collect::<Vec<_>>();

        let x_axis_label = "Time";
        let y_axis_label = self.measurement.to_string();

        let mark_type = if self.draw_area { "area" } else { "line" };

        let vega_string = serde_json::json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v4.17.0.json",
            "data": {
                "values": data
            },
            "description": "Area Plot",
            "encoding": {
                "x": {
                    "field": "x",
                    "title": x_axis_label,
                    "type": "temporal"
                },
                "y": {
                    "field": "y",
                    "title": y_axis_label,
                    "type": "quantitative"
                }
            },
            "mark": {
                "type": mark_type,
                "line": true,
                "point": true
            }
        })
        .to_string();

        Ok(PlotData {
            vega_string,
            metadata: PlotMetaData::None,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::primitives::{ContinuousMeasurement, DateTime};

    use super::*;

    #[test]
    fn serialization() {
        let chart = AreaLineChart::new(
            vec![
                TimeInstance::from(DateTime::new_utc(2010, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2011, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2012, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2013, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2014, 1, 1, 0, 0, 0)),
            ],
            vec![0., 1., 4., 9., 7.],
            Measurement::Unitless,
            true,
        )
        .unwrap();

        assert_eq!(
            chart.to_vega_embeddable(false).unwrap(),
            PlotData {
                vega_string: r#"{"$schema":"https://vega.github.io/schema/vega-lite/v4.17.0.json","data":{"values":[{"x":"2010-01-01T00:00:00+00:00","y":0.0},{"x":"2011-01-01T00:00:00+00:00","y":1.0},{"x":"2012-01-01T00:00:00+00:00","y":4.0},{"x":"2013-01-01T00:00:00+00:00","y":9.0},{"x":"2014-01-01T00:00:00+00:00","y":7.0}]},"description":"Area Plot","encoding":{"x":{"field":"x","title":"Time","type":"temporal"},"y":{"field":"y","title":"","type":"quantitative"}},"mark":{"type":"area","line":true,"point":true}}"#.to_owned(),
                metadata: PlotMetaData::None,
            }
        );
    }

    #[test]
    fn without_area() {
        let chart = AreaLineChart::new(
            vec![
                TimeInstance::from(DateTime::new_utc(2010, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2011, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2012, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2013, 1, 1, 0, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2014, 1, 1, 0, 0, 0)),
            ],
            vec![0., 1., 4., 9., 7.],
            Measurement::Continuous(ContinuousMeasurement {
                measurement: "Joy".to_owned(),
                unit: Some("Pct".to_owned()),
            }),
            false,
        )
        .unwrap();

        assert_eq!(
            chart.to_vega_embeddable(false).unwrap(),
            PlotData {
                vega_string: r#"{"$schema":"https://vega.github.io/schema/vega-lite/v4.17.0.json","data":{"values":[{"x":"2010-01-01T00:00:00+00:00","y":0.0},{"x":"2011-01-01T00:00:00+00:00","y":1.0},{"x":"2012-01-01T00:00:00+00:00","y":4.0},{"x":"2013-01-01T00:00:00+00:00","y":9.0},{"x":"2014-01-01T00:00:00+00:00","y":7.0}]},"description":"Area Plot","encoding":{"x":{"field":"x","title":"Time","type":"temporal"},"y":{"field":"y","title":"Joy in Pct","type":"quantitative"}},"mark":{"type":"line","line":true,"point":true}}"#.to_owned(),
                metadata: PlotMetaData::None,
            }
        );
    }
}
