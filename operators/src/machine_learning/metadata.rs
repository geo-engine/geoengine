use std::path::PathBuf;

use geoengine_datatypes::{machine_learning::MlTensorShape3D, raster::RasterDataType};
use postgres_types::{FromSql, ToSql};

/// Strategies to handle no-data in model inputs.
/// - `EncodedNoData`: If inputs have empty (no-data) pixels, pixels are mapped to a `no_data_value`. This is usefull if the model can handle missing data.
/// - `SkipIfNoData`: If any input pixel is empty (no-data), the output is also empty (no-data). This is usefull if the model can't handle missing data.
#[derive(PartialEq, Debug, Copy, Clone)]
pub enum MlModelInputNoDataHandling {
    EncodedNoData { no_data_value: f32 },
    SkipIfNoData,
}

impl MlModelInputNoDataHandling {
    pub fn no_data_value_encoding(self) -> Option<f32> {
        match self {
            MlModelInputNoDataHandling::EncodedNoData { no_data_value } => Some(no_data_value),
            MlModelInputNoDataHandling::SkipIfNoData => None,
        }
    }
}

/// Strategies to handle no-data in model outputs.
/// - `EncodedNoData`: If the model outputs a `no_data_value` pixel with the `no_data_value` are masked and are ignored by other operators.
/// - `NanIsNoData`: If the model produces NaN values, they are masked as as no data.
#[derive(PartialEq, Debug, Copy, Clone)]
pub enum MlModelOutputNoDataHandling {
    EncodedNoData { no_data_value: f32 },
    NanIsNoData,
}

impl MlModelOutputNoDataHandling {
    pub fn no_data_value_encoding(self) -> Option<f32> {
        match self {
            MlModelOutputNoDataHandling::EncodedNoData { no_data_value } => Some(no_data_value),
            MlModelOutputNoDataHandling::NanIsNoData => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MlModelLoadingInfo {
    pub storage_path: PathBuf,
    pub metadata: MlModelMetadata,
}

impl MlModelLoadingInfo {
    pub fn model_path_owned(&self) -> PathBuf {
        self.storage_path.clone()
    }
}

// For now we assume all models are pixel-wise, i.e., they take a single pixel with multiple bands as input and produce a single output value.
// To support different inputs, we would need a more sophisticated logic to produce the inputs for the model.
#[derive(Debug, Clone, PartialEq, ToSql, FromSql)]
pub struct MlModelMetadata {
    pub input_type: RasterDataType,
    pub output_type: RasterDataType,
    pub input_shape: MlTensorShape3D,
    pub output_shape: MlTensorShape3D, // TODO: output measurement, e.g. classification or regression, label names for classification. This would have to be provided by the model creator along the model file as it cannot be extracted from the model file(?)
    pub input_no_data_handling: MlModelInputNoDataHandling,
    pub output_no_data_handling: MlModelOutputNoDataHandling,
}

impl MlModelMetadata {
    pub fn num_input_bands(&self) -> u32 {
        self.input_shape.bands
    }

    pub fn num_output_bands(&self) -> u32 {
        self.output_shape.bands
    }

    pub fn input_is_single_pixel(&self) -> bool {
        self.input_shape.x == 1 && self.input_shape.y == 1
    }

    pub fn output_is_single_pixel(&self) -> bool {
        self.output_shape.x == 1 && self.output_shape.y == 1
    }

    pub fn output_is_single_attribute(&self) -> bool {
        self.num_output_bands() == 1
    }
}
