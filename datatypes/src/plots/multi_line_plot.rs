use crate::plots::{Plot, PlotData, PlotMetaData};
use crate::primitives::{Measurement, TimeInstance};
use crate::util::Result;

pub struct DataPoint {
    pub series: String,
    pub time: TimeInstance,
    pub value: f64,
}

impl From<(String, TimeInstance, f64)> for DataPoint {
    fn from(p: (String, TimeInstance, f64)) -> Self {
        Self {
            series: p.0,
            time: p.1,
            value: p.2,
        }
    }
}

/// A plot that produces a chart over time (x-axis) with multiple (colored) lines, one for each
/// series defined by the corresponding field `series` of the given `DataPoint`s.
pub struct MultiLineChart {
    data: Vec<DataPoint>,
    measurement: Measurement,
}

impl MultiLineChart {
    pub fn new(data: Vec<DataPoint>, measurement: Measurement) -> Self {
        Self { data, measurement }
    }
}

impl Plot for MultiLineChart {
    fn to_vega_embeddable(&self, _allow_interactions: bool) -> Result<PlotData> {
        let data = self
            .data
            .iter()
            .map(|d| {
                serde_json::json!({
                    "x": d.time.as_datetime_string(),
                    "y": d.value,
                    "series": d.series,
                })
            })
            .collect::<Vec<_>>();

        let x_axis_label = "Time";
        let y_axis_label = self.measurement.to_string();

        let vega_string = serde_json::json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v4.17.0.json",
            "data": {
                "values": data
            },
            "description": "Multi Line Chart",
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
                },
                "color": {
                    "field": "series",
                    "scale": {
                        "scheme": "category20"
                    }
                }
            },
            "mark": {
                "type": "line",
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
    use super::*;

    #[test]
    fn serialization() {
        let chart = MultiLineChart::new(
            vec![
                ("S0".to_owned(), TimeInstance::from_millis_unchecked(0), 0.).into(),
                ("S1".to_owned(), TimeInstance::from_millis_unchecked(0), 2.).into(),
                (
                    "S0".to_owned(),
                    TimeInstance::from_millis_unchecked(1000),
                    1.,
                )
                    .into(),
            ],
            Measurement::Unitless,
        );
        assert_eq!(
            chart.to_vega_embeddable(false).unwrap(),
            PlotData {
                vega_string: r#"{"$schema":"https://vega.github.io/schema/vega-lite/v4.17.0.json","data":{"values":[{"series":"S0","x":"1970-01-01T00:00:00+00:00","y":0.0},{"series":"S1","x":"1970-01-01T00:00:00+00:00","y":2.0},{"series":"S0","x":"1970-01-01T00:00:01+00:00","y":1.0}]},"description":"Multi Line Chart","encoding":{"color":{"field":"series","scale":{"scheme":"category20"}},"x":{"field":"x","title":"Time","type":"temporal"},"y":{"field":"y","title":"","type":"quantitative"}},"mark":{"line":true,"point":true,"type":"line"}}"#.to_owned(),
                metadata: PlotMetaData::None,
            }
        );
    }
}
