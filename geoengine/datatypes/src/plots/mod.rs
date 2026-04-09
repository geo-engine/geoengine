mod area_line_plot;
mod bar_chart;
mod box_plot;
mod histogram;
mod histogram2d;
mod multi_line_plot;
mod pie_chart;
mod scatter_plot;

pub use area_line_plot::AreaLineChart;
pub use bar_chart::BarChart;
pub use box_plot::{BoxPlot, BoxPlotAttribute};
pub use histogram::{Histogram, HistogramBuilder};
pub use histogram2d::{Histogram2D, HistogramDimension};
pub use multi_line_plot::{DataPoint, MultiLineChart};
pub use pie_chart::PieChart;
pub use scatter_plot::ScatterPlot;

use crate::util::Result;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub trait Plot {
    /// Creates a Vega string for embedding it into a Html page
    ///
    /// # Errors
    ///
    /// This method fails on internal errors of the plot.
    ///
    fn to_vega_embeddable(&self, allow_interactions: bool) -> Result<PlotData>;

    // TODO: create some PNG output, cf. https://github.com/procyon-rs/vega_lite_3.rs/issues/18
    // fn to_png(&self, width_px: u16, height_px: u16) -> Vec<u8>;
}

#[derive(Debug, Clone, Deserialize, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlotData {
    pub vega_string: String,
    pub metadata: PlotMetaData,
}

impl PartialEq for PlotData {
    fn eq(&self, other: &Self) -> bool {
        let vega_equals = match (
            serde_json::from_str::<serde_json::Value>(&self.vega_string),
            serde_json::from_str::<serde_json::Value>(&other.vega_string),
        ) {
            (Ok(v1), Ok(v2)) => v1 == v2, // if the vega_string is valid JSON, compare the JSON values to avoid formatting differences
            _ => self.vega_string == other.vega_string,
        };

        vega_equals && self.metadata == other.metadata
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize, Default)]
#[serde(untagged)]
pub enum PlotMetaData {
    #[default]
    None,
    #[serde(rename_all = "camelCase")]
    Selection { selection_name: String },
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
pub enum PlotOutputFormat {
    JsonPlain,
    JsonVega,
    ImagePng,
}
