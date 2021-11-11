use crate::plots::{Plot, PlotData, PlotMetaData};
use crate::primitives::Coordinate2D;
use crate::util::Result;
use serde::{Deserialize, Serialize};

/// A scatter plot consists of a series of `Coordinate`s
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScatterPlot {
    title_x: String,
    title_y: String,
    values: Vec<Coordinate2D>,
}

impl ScatterPlot {
    /// Creates a new scatter plot without points.
    pub fn new(title_x: String, title_y: String) -> ScatterPlot {
        Self::new_with_data(title_x, title_y, vec![])
    }

    /// Creates a new scatter plot with the given data points.
    pub fn new_with_data(
        title_x: String,
        title_y: String,
        values: Vec<Coordinate2D>,
    ) -> ScatterPlot {
        ScatterPlot {
            title_x,
            title_y,
            values,
        }
    }

    /// Adds the given points to this scatter plot
    pub fn update_batch(&mut self, values: impl Iterator<Item = Coordinate2D>) {
        for i in values {
            self.update(i);
        }
    }

    /// Adds a new point to this scatter plot.
    pub fn update(&mut self, value: Coordinate2D) {
        self.values.push(value);
    }
}

impl Plot for ScatterPlot {
    fn to_vega_embeddable(&self, _allow_interactions: bool) -> Result<PlotData> {
        let vega_spec = serde_json::json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container",
            "height": "container",
            "data": { "values": self.values },
            "mark": "point",
             "encoding": {
                "x": {"field": "x", "type": "quantitative", "title": self.title_x},
                "y": {"field": "y", "type": "quantitative", "title": self.title_y}
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
    use crate::plots::scatter_plot::ScatterPlot;
    use crate::plots::Plot;
    use crate::primitives::Coordinate2D;

    #[test]
    fn test_ser() {
        let mut sp = ScatterPlot::new("X-Axis".to_string(), "Y-Axis".to_string());
        for i in 1..=5 {
            sp.update(Coordinate2D::new(f64::from(i), f64::from(i)));
        }

        let ser = serde_json::to_string(&sp).unwrap();

        let expected = serde_json::json!({
            "titleX": "X-Axis",
            "titleY": "Y-Axis",
            "values": [
                { "x": 1.0, "y": 1.0 }, { "x": 2.0, "y": 2.0 }, { "x": 3.0, "y": 3.0 }, { "x": 4.0, "y": 4.0 }, { "x": 5.0, "y": 5.0 }
            ]
        });

        assert_eq!(expected.to_string(), ser);
    }

    #[test]
    fn test_vega() {
        let mut sp = ScatterPlot::new("X-Axis".to_string(), "Y-Axis".to_string());
        for i in 1..=5 {
            sp.update(Coordinate2D::new(f64::from(i), f64::from(i)));
        }

        let ser = sp.to_vega_embeddable(false).unwrap().vega_string;

        let expected = serde_json::json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container",
            "height": "container",
            "data": { "values": [
                { "x": 1.0, "y": 1.0 }, { "x": 2.0, "y": 2.0 }, { "x": 3.0, "y": 3.0 }, { "x": 4.0, "y": 4.0 }, { "x": 5.0, "y": 5.0 }
            ] },
            "mark": "point",
             "encoding": {
                "x": {"field": "x", "type": "quantitative", "title": "X-Axis"},
                "y": {"field": "y", "type": "quantitative", "title": "Y-Axis"}
             }
        });

        assert_eq!(expected.to_string(), ser);
    }
}
