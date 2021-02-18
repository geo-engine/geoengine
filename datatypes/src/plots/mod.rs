mod area_line_plot;
mod histogram;

pub use area_line_plot::AreaLineChart;
pub use histogram::{Histogram, HistogramBuilder};

use crate::util::Result;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub trait Plot {
    type PlotDataMetadataType: Debug + PartialEq + Serialize;

    /// Creates a Vega string for embedding it into a Html page
    ///
    /// # Errors
    ///
    /// This method fails on internal errors of the plot.
    ///
    fn to_vega_embeddable(
        &self,
        allow_interactions: bool,
    ) -> Result<PlotData<Self::PlotDataMetadataType>>;

    fn to_png(&self, width_px: u16, height_px: u16) -> Vec<u8>;
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct PlotData<M>
where
    M: Debug + PartialEq + Serialize,
{
    pub vega_string: String,
    pub metadata: M,
}
