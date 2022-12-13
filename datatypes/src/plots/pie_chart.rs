use crate::error;
use crate::plots::{Plot, PlotData, PlotMetaData};
use crate::util::Result;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::collections::BTreeMap;

/// A pie chart
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PieChart {
    slices: BTreeMap<String, f64>,
    legend_label: String,
    donut: bool,
}

impl PieChart {
    pub fn new(slices: BTreeMap<String, f64>, legend_label: String, donut: bool) -> Result<Self> {
        ensure!(
            !legend_label.is_empty(),
            error::Plot {
                details: "Legend label in the pie chart must not be empty".to_string()
            }
        );
        ensure!(
            slices.values().all(|&v| v > 0.0),
            error::Plot {
                details: "All slices of the pie chart must have a positive value".to_string()
            }
        );

        Ok(Self {
            slices,
            legend_label,
            donut,
        })
    }
}

#[derive(Debug)]
struct PieChartSlice {
    legend_label: String,
    label: String,
    value: f64,
}

impl Serialize for PieChartSlice {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let mut map_serializer = serializer.serialize_map(Some(2))?;
        map_serializer.serialize_entry(&self.legend_label, &self.label)?;
        map_serializer.serialize_entry("value", &self.value)?;
        map_serializer.end()
    }
}

impl Plot for PieChart {
    fn to_vega_embeddable(&self, _allow_interactions: bool) -> Result<PlotData> {
        let measurement = self.legend_label.clone();

        let mut values = Vec::new();
        for (label, value) in &self.slices {
            values.push(PieChartSlice {
                legend_label: measurement.clone(),
                label: label.clone(),
                value: *value,
            });
        }

        let radius = if self.donut { 50 } else { 0 };

        let vega_spec = serde_json::json!({
          "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
          "width": "container",
          "data": {
            "values": values,
          },
          "mark": {"type": "arc", "innerRadius": radius, "tooltip": true},
          "encoding": {
            "theta": {"field": "value", "type": "quantitative"},
            "color": {"field": measurement, "type": "nominal"}
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
    fn it_is_a_pie() {
        let chart = PieChart::new(
            vec![("a".to_string(), 1.), ("b".to_string(), 2.)]
                .into_iter()
                .collect(),
            "Label".to_string(),
            false,
        )
        .unwrap();

        assert_eq!(
            chart.to_vega_embeddable(false).unwrap(),
            PlotData {
                vega_string: serde_json::json!({
                  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                  "width": "container",
                  "data": {
                    "values": [
                        {"Label": "a", "value": 1.},
                        {"Label": "b", "value": 2.},
                    ]
                  },
                  "mark": {"type": "arc", "innerRadius": 0, "tooltip": true},
                  "encoding": {
                    "theta": {"field": "value", "type": "quantitative"},
                    "color": {"field": "Label", "type": "nominal"}
                  }
                })
                .to_string(),
                metadata: PlotMetaData::None,
            }
        );
    }

    #[test]
    fn it_is_a_donut() {
        let chart = PieChart::new(
            vec![("a".to_string(), 1.), ("b".to_string(), 2.)]
                .into_iter()
                .collect(),
            "Rain (in cm)".to_string(),
            true,
        )
        .unwrap();

        assert_eq!(
            chart.to_vega_embeddable(false).unwrap(),
            PlotData {
                vega_string: serde_json::json!({
                  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                  "width": "container",
                  "data": {
                    "values": [
                        {"Rain (in cm)": "a", "value": 1.},
                        {"Rain (in cm)": "b", "value": 2.},
                    ]
                  },
                  "mark": {"type": "arc", "innerRadius": 50, "tooltip": true},
                  "encoding": {
                    "theta": {"field": "value", "type": "quantitative"},
                    "color": {"field": "Rain (in cm)", "type": "nominal"}
                  }
                })
                .to_string(),
                metadata: PlotMetaData::None,
            }
        );
    }

    #[test]
    fn it_is_empty() {
        let chart = PieChart::new(
            vec![].into_iter().collect(),
            "Rain (in cm)".to_string(),
            true,
        )
        .unwrap();

        assert_eq!(
            chart.to_vega_embeddable(false).unwrap(),
            PlotData {
                vega_string: serde_json::json!({
                  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                  "width": "container",
                  "data": {
                    "values": []
                  },
                  "mark": {"type": "arc", "innerRadius": 50, "tooltip": true},
                  "encoding": {
                    "theta": {"field": "value", "type": "quantitative"},
                    "color": {"field": "Rain (in cm)", "type": "nominal"}
                  }
                })
                .to_string(),
                metadata: PlotMetaData::None,
            }
        );
    }
}
