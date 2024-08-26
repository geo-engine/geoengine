use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::raster::RasterDataType;

// TODO: custom serialization as ns:name
#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub struct MlModelName {
    pub namespace: Option<String>,
    pub name: String,
}

// For now we assume all models are pixel-wise, i.e., they take a single pixel with multiple bands as input and produce a single output value.
// To support different inputs, we would need a more sophisticated logic to produce the inputs for the model.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub struct MlModelMetadata {
    pub file_path: PathBuf,
    pub input_type: RasterDataType,
    pub num_input_bands: u32, // number of features per sample (bands per pixel)
    pub output_type: RasterDataType, // TODO: support multiple outputs, e.g. one band for the probability of prediction
                                     // TODO: output measurement, e.g. classification or regression, label names for classification. This would have to be provided by the model creator along the model file as it cannot be extracted from the model file(?)
}
