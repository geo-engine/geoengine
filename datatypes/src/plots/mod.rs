mod area_line_plot;
mod histogram;

pub use area_line_plot::AreaLineChart;
pub use histogram::{Histogram, HistogramBuilder};

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

#[derive(Debug, Clone, Deserialize, PartialEq, Serialize)]
pub struct PlotData {
    pub vega_string: String,
    pub metadata: PlotMetaData,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum PlotMetaData {
    None,
    Selection { selection_name: String },
}

#[derive(Debug, Clone, Deserialize, PartialEq, Serialize)]
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
