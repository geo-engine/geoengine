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

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlotData {
    pub vega_string: String,
    pub metadata: PlotMetaData,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum PlotMetaData {
    None,
    #[serde(rename_all = "camelCase")]
    Selection {
        selection_name: String,
    },
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
pub enum PlotOutputFormat {
    JsonPlain,
    JsonVega,
    ImagePng,
}

impl Default for PlotMetaData {
    fn default() -> Self {
        PlotMetaData::None
    }
}
