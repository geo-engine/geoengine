use std::path::PathBuf;

use geoengine_datatypes::{
    machine_learning::MlTensorShape3D,
    raster::{GridShape2D, GridSize, RasterDataType, TileOverlap},
};
use postgres_types::{FromSql, ToSql};

use crate::machine_learning::MachineLearningError;

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

    /// Derive the input and output overlap (halo) the model needs relative to `core`.
    ///
    /// `halo_in = (input.yx - core) / 2` and `halo_out = (output.yx - core) / 2`.
    /// Both must be even and non-negative per axis (symmetric halos only). The `core`
    /// is the shared central region both the input and output grids are centered on
    /// (typically the source core tile size).
    pub fn input_output_overlap(
        &self,
        core: &GridShape2D,
    ) -> Result<(TileOverlap, TileOverlap), MachineLearningError> {
        // a per-pixel model (1x1 in and out) is applied to every pixel and never consumes a halo
        if self.input_is_single_pixel() && self.output_is_single_pixel() {
            return Ok((TileOverlap::zero(), TileOverlap::zero()));
        }

        let halo = |shape: &MlTensorShape3D| {
            let dy = i64::from(shape.y) - i64::from(core.axis_size_y() as u32);
            let dx = i64::from(shape.x) - i64::from(core.axis_size_x() as u32);
            if dy < 0 || dx < 0 || dy % 2 != 0 || dx % 2 != 0 {
                Err(MachineLearningError::AsymmetricModelOverlap {
                    in_shape: self.input_shape,
                    out_shape: self.output_shape,
                    core: *core,
                })
            } else {
                Ok(TileOverlap::new((dy / 2) as u32, (dx / 2) as u32))
            }
        };
        let halo_in = halo(&self.input_shape)?;
        let halo_out = halo(&self.output_shape)?;
        Ok((halo_in, halo_out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(in_yx: (u32, u32), out_yx: (u32, u32)) -> MlModelMetadata {
        MlModelMetadata {
            input_type: RasterDataType::F32,
            output_type: RasterDataType::F32,
            input_shape: MlTensorShape3D::new_y_x_bands(in_yx.0, in_yx.1, 1),
            output_shape: MlTensorShape3D::new_y_x_bands(out_yx.0, out_yx.1, 1),
            input_no_data_handling: MlModelInputNoDataHandling::SkipIfNoData,
            output_no_data_handling: MlModelOutputNoDataHandling::NanIsNoData,
        }
    }

    #[test]
    fn equal_shapes_no_overlap() {
        let m = metadata((4, 4), (4, 4));
        let (hi, ho) = m.input_output_overlap(&GridShape2D::new_2d(4, 4)).unwrap();
        assert_eq!(hi, TileOverlap::zero());
        assert_eq!(ho, TileOverlap::zero());
    }

    #[test]
    fn input_larger_than_core() {
        let m = metadata((6, 6), (4, 4));
        let (hi, ho) = m.input_output_overlap(&GridShape2D::new_2d(4, 4)).unwrap();
        assert_eq!(hi, TileOverlap::new(1, 1));
        assert_eq!(ho, TileOverlap::zero());
    }

    #[test]
    fn both_input_and_output_halo() {
        let m = metadata((6, 8), (4, 6));
        let (hi, ho) = m.input_output_overlap(&GridShape2D::new_2d(4, 4)).unwrap();
        assert_eq!(hi, TileOverlap::new(1, 2));
        assert_eq!(ho, TileOverlap::new(0, 1));
    }

    #[test]
    fn asymmetric_odd_difference_errors() {
        let m = metadata((5, 4), (4, 4));
        assert!(matches!(
            m.input_output_overlap(&GridShape2D::new_2d(4, 4)),
            Err(MachineLearningError::AsymmetricModelOverlap { .. })
        ));
    }

    #[test]
    fn negative_difference_errors() {
        let m = metadata((4, 4), (2, 4));
        assert!(matches!(
            m.input_output_overlap(&GridShape2D::new_2d(4, 4)),
            Err(MachineLearningError::AsymmetricModelOverlap { .. })
        ));
    }

    #[test]
    fn single_pixel_model_ignores_core() {
        // a 1x1 in / 1x1 out model is per-pixel: no halo regardless of the (arbitrary) core size
        let m = metadata((1, 1), (1, 1));
        let (hi, ho) = m.input_output_overlap(&GridShape2D::new_2d(2, 2)).unwrap();
        assert_eq!(hi, TileOverlap::zero());
        assert_eq!(ho, TileOverlap::zero());
    }
}
