use crate::error;
use crate::plots::{Plot, PlotData, PlotMetaData};
use crate::util::Result;
use serde::{Deserialize, Serialize};
use snafu::ensure;

/// A box plot consists of multiple boxes (`BoxPlotAttribute`)
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BoxPlot {
    values: Vec<BoxPlotAttribute>,
}

impl BoxPlot {
    /// Creates a new box plot without any boxes.
    pub fn new() -> BoxPlot {
        BoxPlot { values: vec![] }
    }

    /// Adds a new box/attribute to this box plot.
    pub fn add_attribute(&mut self, value: BoxPlotAttribute) {
        self.values.push(value);
    }
}

impl Default for BoxPlot {
    fn default() -> Self {
        BoxPlot::new()
    }
}

/// Represents a single box of a box plot including whiskers
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BoxPlotAttribute {
    pub name: String,
    pub min: f64,
    pub max: f64,
    pub median: f64,
    pub q1: f64,
    pub q3: f64,
    pub is_exact: bool,
}

impl BoxPlotAttribute {
    pub fn new(
        name: String,
        min: f64,
        max: f64,
        median: f64,
        q1: f64,
        q3: f64,
        is_exact: bool,
    ) -> Result<BoxPlotAttribute> {
        ensure!(
            !min.is_nan() && !max.is_nan() && !median.is_nan() && !q1.is_nan() && !q3.is_nan(),
            error::Plot {
                details: "NaN values not allowed in box plots."
            }
        );

        ensure!(
            min <= q1 && q1 <= median && median <= q3 && q3 <= max,
            error::Plot {
                details: format!(
                    "Illegal box plot values. min: {}, q1: {}, median: {}, q3: {}, max: {}",
                    min, q1, median, q3, max
                )
            }
        );

        Ok(BoxPlotAttribute {
            name,
            min,
            max,
            median,
            q1,
            q3,
            is_exact,
        })
    }
}

impl Plot for BoxPlot {
    fn to_vega_embeddable(&self, _allow_interactions: bool) -> Result<PlotData> {
        let vega_spec = serde_json::json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container",
            "height": "container",
            "data": self,
            "encoding": {"x": {"field": "name", "type": "nominal" }},
            "layer": [
              {
                "mark": {"type": "rule"},
                "encoding": {
                  "y": {"field": "min", "type": "quantitative", "scale": {"zero": false} },
                  "y2": {"field": "max"}
                }
              },
              {
                "mark": {"type": "bar", "cornerRadius": 5, "width": {"band": 0.75} },
                "encoding": {
                  "y": {"field": "q1", "type": "quantitative"},
                  "y2": {"field": "q3"},
                  "color": {"field": "name", "type": "nominal"}
                }
              },
              {
                "mark": {"type": "rect", "color": "white", "width": { "band": 0.75}, "height": 1  },
                "encoding": {"y": {"field": "median", "type": "quantitative"} }
              }
            ],
            "config": {
              "axisXDiscrete": { "title": null },
              "axisYQuantitative": { "title": null },
              "legend": {
                "disable": true
              }
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
    use crate::plots::box_plot::BoxPlotAttribute;
    use crate::plots::BoxPlot;

    #[test]
    fn test_ser() {
        let mut bp = BoxPlot::new();
        bp.add_attribute(
            BoxPlotAttribute::new("A1".to_string(), 12.0, 83.0, 35.0, 20.0, 55.0, true).unwrap(),
        );

        let ser = serde_json::to_string(&bp).unwrap();

        let expected = serde_json::json!({
            "values": [
                { "name": "A1", "min": 12.0, "max": 83.0, "median" : 35.0, "q1": 20.0, "q3": 55.0, "isExact": true }
            ]
        });

        assert_eq!(expected.to_string(), ser);
    }

    #[test]
    fn ok() {
        assert!(
            BoxPlotAttribute::new("A1".to_string(), 12.0, 83.0, 35.0, 20.0, 55.0, true).is_ok()
        );
        assert!(
            BoxPlotAttribute::new("A2".to_string(), 35.0, 112.0, 76.0, 55.0, 99.0, false).is_ok()
        );
    }

    #[test]
    fn nan() {
        assert!(
            BoxPlotAttribute::new("A1".to_string(), f64::NAN, 83.0, 35.0, 20.0, 55.0, true)
                .is_err()
        );
    }

    #[test]
    fn illegal_order() {
        assert!(
            BoxPlotAttribute::new("A1".to_string(), 12.0, 35.0, 83.0, 20.0, 55.0, true).is_err()
        );
    }
}
