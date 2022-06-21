use super::{Plot, PlotData, PlotMetaData};
use crate::util::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BarChart {
    bars: BTreeMap<String, u64>,
    x_label: String,
    y_label: String,
}

impl BarChart {
    pub fn new(bars: BTreeMap<String, u64>, x_label: String, y_label: String) -> Self {
        BarChart {
            bars,
            x_label,
            y_label,
        }
    }
}

impl Plot for BarChart {
    fn to_vega_embeddable(&self, _allow_interactions: bool) -> Result<PlotData> {
        // TODO: add interactive mode

        let mut values = Vec::with_capacity(self.bars.len());
        for (tick_label, bar_height) in &self.bars {
            values.push(serde_json::json!({
                "label": tick_label,
                self.y_label.clone(): bar_height,
            }));
        }

        let vega_spec = serde_json::json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "data": {
                "values": values,
            },
            "mark": "bar",
            "encoding": {
                "x": {
                    "field": "label",
                    "axis": {
                        "title": self.y_label,
                    },
                },
                "y": {
                    "field": self.y_label,
                    "type": "quantitative",
                },
            },
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
    fn test_to_vega_embeddable() {
        let bar_chart = BarChart::new(
            [
                ("foo".to_string(), 1),
                ("bar".to_string(), 3),
                ("baz".to_string(), 10),
            ]
            .into_iter()
            .collect(),
            "foobar".to_string(),
            "Frequency".to_string(),
        );

        assert_eq!(
            bar_chart.to_vega_embeddable(false).unwrap(),
            PlotData {
                vega_string: serde_json::json!({
                  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                  "data": {
                    "values": [
                      {
                        "label": "bar",
                        "Frequency": 3
                      },
                      {
                        "label": "baz",
                        "Frequency": 10
                      },
                      {
                        "label": "foo",
                        "Frequency": 1
                      }
                    ]
                  },
                  "mark": "bar",
                  "encoding": {
                    "x": {
                      "field": "label",
                      "axis": {
                        "title": "Frequency"
                      }
                    },
                    "y": {
                      "field": "Frequency",
                      "type": "quantitative"
                    }
                  }
                })
                .to_string(),
                metadata: PlotMetaData::None
            }
        );
    }
}
