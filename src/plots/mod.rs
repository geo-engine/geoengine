pub mod histogram;

use crate::util::Result;
pub use histogram::Histogram;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub trait Plot {
    type PlotDataMetadataType: Debug + PartialEq + Serialize;

    fn to_vega_embeddable(
        &self,
        allow_interactions: bool,
    ) -> Result<PlotData<Self::PlotDataMetadataType>>;

    fn to_png(&self, width_px: u16, height_px: u16) -> Vec<u8>;
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct PlotData<T>
where
    T: Serialize,
{
    pub vega_string: String,
    pub metadata: T,
}
