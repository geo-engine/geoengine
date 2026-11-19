//! The `Warp` operator puts a raster onto another grid in one step: another spatial reference,
//! another resolution (per axis, coarser or finer) and another origin, in any combination.
//!
//! By default, it looks up the input pixel under each output pixel's center, the same
//! nearest-neighbour lookup as `Reprojection`, `Downsampling` and `Interpolation`
//! (`NearestNeighbor`). Unlike a chain of these, it needs a single sub-query per output tile.
//! In the same spatial reference, it maps pixel indices directly, with the sub-query of
//! `Interpolation` for finer outputs and the one of `Downsampling` otherwise; with another spatial
//! reference, it uses the sub-query of `Reprojection`.
//!
//! [`WarpParams::downsampling`] (`average`, `mode`, `min`, `max`) and [`WarpParams::upsampling`]
//! (`bilinear`) compute the output pixels from several input pixels instead, as `gdalwarp -r` does
//! (see [`kernels`]), with one sub-query per output tile as well. With another spatial reference,
//! whether the output is coarser or finer is relative to the resolution of the reprojected input.
//! [`WarpParams::output_data_type`] converts the output, e.g. to keep the fractions of an average.
//!
//! `Reprojection` (raster), `Downsampling` and `Interpolation` (`NearestNeighbor`) keep their own
//! parameters, validation and output grid, but initialize to an [`InitializedWarp`] onto that grid,
//! so they share its processors and its `optimize`, which returns a `Warp`.

use crate::adapters::TileReprojectionSubqueryGridInfo;
use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedRasterOperator, InitializedSources, Operator,
    OperatorName, ProcessingGraphOperatorPath, RasterOperator, RasterQueryProcessor,
    RasterResultDescriptor, SingleRasterSource, SpatialGridDescriptor, TypedRasterOperator,
    TypedRasterQueryProcessor,
};
use crate::error::Error;
use crate::optimization::{OptimizableOperator, OptimizationError};
use crate::processing::downsample::DownsampleProcessor;
use crate::processing::interpolation::InterploationProcessor;
use crate::processing::reprojection::{RasterReprojectionProcessor, compute_output_spatial_grid};
use crate::processing::{DeriveOutRasterSpecsSource, ReprojectionParams};
use crate::util::Result;
use async_trait::async_trait;
use geoengine_datatypes::primitives::{
    Coordinate2D, SpatialResolution, find_next_best_overview_level_resolution,
};
use geoengine_datatypes::raster::{
    DynamicRasterDataType, NearestNeighbor, RasterDataType, TilingSpecification,
};
use geoengine_datatypes::spatial_reference::SpatialReference;
use serde::{Deserialize, Serialize};

pub mod kernels;

use kernels::{KernelProjection, RoundingTypeConversionProcessor, WarpKernel, WarpKernelProcessor};

/// The parameters of [`Warp`]: the grid to put the raster onto. Without any of them, the output
/// is the input.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct WarpParams {
    /// The spatial reference of the output, the input's if `None`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_spatial_reference: Option<SpatialReference>,
    /// The resolution of the output, the input's (or, if reprojected, the reprojected input's) if `None`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_resolution: Option<SpatialResolution>,
    /// A coordinate the output grid is aligned to, the input's (reprojected) origin if `None`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_origin_reference: Option<Coordinate2D>,
    /// How to compute an output pixel that is coarser than the input in at least one axis, from the
    /// input pixels under it. Nearest neighbour by default.
    #[serde(default, skip_serializing_if = "is_default")]
    pub downsampling: WarpDownsampling,
    /// How to compute an output pixel that is finer than the input in both axes. Nearest neighbour
    /// by default.
    #[serde(default, skip_serializing_if = "is_default")]
    pub upsampling: WarpUpsampling,
    /// The data type of the output, the input's if `None`. Integer types round half up and clip
    /// values to their range.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_data_type: Option<RasterDataType>,
    /// How the output grid is derived when reprojecting: from the area of use of the source's
    /// spatial reference (default) or from the data bounds, as in `Reprojection`
    #[serde(default, skip_serializing_if = "is_default")]
    pub derive_out_spec: DeriveOutRasterSpecsSource,
    /// The largest error in input pixels of the approximate coordinate transformation when
    /// reprojecting, as `gdalwarp -et`; 0 projects every pixel exactly. The server's
    /// `[query_context] reprojection_approximation_error_px` (0.125 by default) if `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approximation_error_threshold: Option<geoengine_datatypes::util::NonNegativeFiniteF64>,
}

/// How [`Warp`] computes an output pixel that is coarser than the input in at least one axis, with
/// the semantics of `gdalwarp -r <method>` (see [`kernels`]).
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum WarpDownsampling {
    /// the value of the input pixel under the output pixel's center
    #[default]
    Nearest,
    /// the average of the input pixels under the output pixel, weighted by the area they cover
    Average,
    /// the most common value of the input pixels under the output pixel, weighted by the area they cover
    Mode,
    /// the minimum of the input pixels under the output pixel
    Min,
    /// the maximum of the input pixels under the output pixel
    Max,
}

/// How [`Warp`] computes an output pixel that is finer than the input in both axes, with the
/// semantics of `gdalwarp -r <method>` (see [`kernels`]).
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum WarpUpsampling {
    /// the value of the input pixel under the output pixel's center
    #[default]
    Nearest,
    /// the bilinear interpolation of the four input pixel centers around the output pixel's center
    Bilinear,
}

/// Whether `value` is the default, which serialized operators leave out
fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

/// Puts a raster onto another grid in one step: another spatial reference, resolution and origin,
/// in any combination (see the module documentation).
pub type Warp = Operator<WarpParams, SingleRasterSource>;

impl OperatorName for Warp {
    const TYPE_NAME: &'static str = "Warp";
}

#[async_trait]
impl RasterOperator for Warp {
    async fn _initialize(
        self: Box<Self>,
        path: ProcessingGraphOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let name = CanonicOperatorName::from(&self);
        let initialized_source = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?;
        let warp =
            InitializedWarp::new(name, path, initialized_source.raster, self.params, context)?;
        Ok(warp.boxed())
    }

    span_fn!(Warp);
}

/// The grid that `Warp` with `params` puts a raster with `in_descriptor` on.
///
/// # Errors
/// If the input has no spatial reference, or does not intersect the area of use of the target
/// spatial reference.
pub fn warped_spatial_grid(
    in_descriptor: &RasterResultDescriptor,
    params: &WarpParams,
) -> Result<SpatialGridDescriptor> {
    let in_srs = source_srs(in_descriptor)?;
    let in_grid = *in_descriptor.spatial_grid_descriptor();

    let grid = match params.target_spatial_reference {
        Some(target_srs) if target_srs != in_srs => compute_output_spatial_grid(
            in_grid,
            in_descriptor.spatial_bounds(),
            in_srs,
            ReprojectionParams {
                target_spatial_reference: target_srs,
                derive_out_spec: params.derive_out_spec,
            },
        )?,
        _ => in_grid,
    };

    let resolution = params
        .output_resolution
        .unwrap_or_else(|| grid.spatial_resolution());
    let changes_resolution = resolution != grid.spatial_resolution();
    if !changes_resolution && params.output_origin_reference.is_none() {
        return Ok(grid);
    }

    let with_resolution = |grid: SpatialGridDescriptor| {
        if changes_resolution {
            grid.with_changed_resolution(resolution)
        } else {
            grid
        }
    };
    let coarser = resolution.x() >= grid.spatial_resolution().x()
        && resolution.y() >= grid.spatial_resolution().y();

    Ok(match params.output_origin_reference {
        // a new grid aligned to that point: move the origin on the finer of the two grids, so the
        // grid moves by less than half a fine pixel (as `Downsampling` and `Interpolation` do)
        Some(origin) if coarser => with_resolution(
            grid.with_moved_origin_to_nearest_grid_edge(origin)
                .replace_origin(origin),
        ),
        Some(origin) => with_resolution(grid)
            .with_moved_origin_to_nearest_grid_edge(origin)
            .replace_origin(origin),
        None => with_resolution(grid),
    }
    .as_derived())
}

/// The input of a `Warp` from `source` with `params`: `source` rewritten to read the coarsest overview
/// level that is still at least as fine as the output in both axes, e.g. for an output that is
/// coarser in one axis and finer in the other, the level of the finer axis.
/// `None` if no overview level is coarser than the source, so the source stays as it is.
///
/// # Errors
/// If the output grid cannot be computed or the source cannot be rewritten.
pub fn source_at_overview_level(
    source: &dyn InitializedRasterOperator,
    params: &WarpParams,
) -> Result<Option<TypedRasterOperator>> {
    let Some(output_resolution) = params.output_resolution else {
        return Ok(None);
    };
    let in_descriptor = source.result_descriptor();
    let native_resolution = warped_spatial_grid(
        in_descriptor,
        &WarpParams {
            output_resolution: None,
            output_origin_reference: None,
            ..*params
        },
    )?
    .spatial_resolution();

    // how much coarser the output is than the input, in the finer of the two axes
    let factor = (output_resolution.x() / native_resolution.x())
        .get()
        .min((output_resolution.y() / native_resolution.y()).get());
    let in_resolution = in_descriptor.spatial_grid.spatial_resolution();
    let Ok(needed_resolution) = in_resolution * factor else {
        return Ok(None);
    };
    if factor < 2. {
        return Ok(None);
    }
    let overview_resolution =
        find_next_best_overview_level_resolution(in_resolution, needed_resolution);
    if overview_resolution == in_resolution {
        return Ok(None);
    }

    source
        .optimize(overview_resolution)
        .map(Some)
        .map_err(|source| Error::Optimization { source })
}

fn source_srs(in_descriptor: &RasterResultDescriptor) -> Result<SpatialReference> {
    Option::<SpatialReference>::from(in_descriptor.spatial_reference)
        .ok_or(Error::SpatialReferenceMustNotBeUnreferenced)
}

/// A `Warp` with its source initialized and the output grid computed.
/// It produces the processor for the output grid: one sub-query per output tile.
///
/// `Reprojection` (raster), `Downsampling` and `Interpolation` (`NearestNeighbor`) initialize to it as
/// well (see [`InitializedWarp::with_output_grid`]), so they share its processors and `optimize`.
pub struct InitializedWarp<O: InitializedRasterOperator> {
    name: CanonicOperatorName,
    path: ProcessingGraphOperatorPath,
    /// the name of the operator this was initialized from, for statistics, logs and the cache config
    operator_name: &'static str,
    result_descriptor: RasterResultDescriptor,
    params: WarpParams,
    source: O,
    state: TileReprojectionSubqueryGridInfo,
    source_srs: SpatialReference,
    target_srs: SpatialReference,
    /// project the coordinates, also within the same spatial reference for `Reprojection`
    reproject: bool,
    /// the kernel for the output pixels, `None` for nearest neighbour
    kernel: Option<WarpKernel>,
    /// the resolution of the input that the kernel compares with the output's: if reprojected,
    /// the one of the reprojected input grid
    kernel_in_resolution: SpatialResolution,
    /// the limit for the memory of `mode` per output tile
    mode_memory_budget: usize,
    /// the largest error in input pixels of the approximate projection, 0 to project exactly
    approximation_error: geoengine_datatypes::util::NonNegativeFiniteF64,
    tiling_specification: TilingSpecification,
}

impl<O: InitializedRasterOperator> InitializedWarp<O> {
    /// Computes the output grid of `source` with `params` (see [`warped_spatial_grid`]).
    ///
    /// # Errors
    /// If the source has no spatial reference, or does not intersect the area of use of the
    /// target spatial reference, or as [`Self::with_output_grid`].
    pub fn new(
        name: CanonicOperatorName,
        path: ProcessingGraphOperatorPath,
        source: O,
        params: WarpParams,
        context: &dyn ExecutionContext,
    ) -> Result<Self> {
        let out_grid = warped_spatial_grid(source.result_descriptor(), &params)?;
        Self::with_output_grid(
            WarpOrigin {
                name,
                path,
                operator_name: Warp::TYPE_NAME,
                always_reproject: false,
            },
            source,
            params,
            out_grid,
            context,
        )
    }

    /// A `Warp` of `source` onto `out_grid`, which the caller computed from `params`, e.g. with the
    /// rules of the operator in `origin` that is mapped to a `Warp`. The target spatial reference is
    /// the one of `params`, or the source's. `context` gives the tiling, the limit for the memory
    /// of `mode` and the default of `params.approximation_error_threshold`.
    ///
    /// # Errors
    /// If the source has no spatial reference, or `mode` is used with a float input or exceeds its
    /// memory limit.
    pub fn with_output_grid(
        origin: WarpOrigin,
        source: O,
        params: WarpParams,
        out_grid: SpatialGridDescriptor,
        context: &dyn ExecutionContext,
    ) -> Result<Self> {
        let tiling_specification = context.tiling_specification();
        let approximation_error = params
            .approximation_error_threshold
            .unwrap_or_else(|| context.reprojection_approximation_error());
        let WarpOrigin {
            name,
            path,
            operator_name,
            always_reproject,
        } = origin;
        let in_descriptor = source.result_descriptor();
        let source_srs = source_srs(in_descriptor)?;
        let target_srs = params.target_spatial_reference.unwrap_or(source_srs);

        let state = TileReprojectionSubqueryGridInfo {
            in_spatial_grid: in_descriptor
                .spatial_grid_descriptor()
                .tiling_grid_definition(tiling_specification)
                .tiling_spatial_grid_definition(),
            out_spatial_grid: out_grid
                .tiling_grid_definition(tiling_specification)
                .tiling_spatial_grid_definition(),
        };

        let reproject = always_reproject || source_srs != target_srs;
        let uses_methods = params.downsampling != WarpDownsampling::Nearest
            || params.upsampling != WarpUpsampling::Nearest;
        // whether the output is coarser or finer: compared with the input's resolution in the
        // target spatial reference, as the one of the reprojected input grid
        let in_resolution = if uses_methods && source_srs != target_srs {
            warped_spatial_grid(
                in_descriptor,
                &WarpParams {
                    output_resolution: None,
                    output_origin_reference: None,
                    ..params
                },
            )?
            .spatial_resolution()
        } else {
            in_descriptor.spatial_grid.spatial_resolution()
        };
        let kernel = warp_kernel(&params, in_resolution, out_grid.spatial_resolution());
        // `mode` counts each distinct value, which floats rarely repeat
        if kernel == Some(WarpKernel::Mode)
            && matches!(
                in_descriptor.data_type,
                RasterDataType::F32 | RasterDataType::F64
            )
        {
            return Err(Error::WarpModeRequiresIntegerInput {
                data_type: in_descriptor.data_type,
            });
        }

        let result_descriptor = RasterResultDescriptor {
            spatial_reference: target_srs.into(),
            data_type: params.output_data_type.unwrap_or(in_descriptor.data_type),
            time: in_descriptor.time,
            spatial_grid: out_grid,
            bands: in_descriptor.bands.clone(),
        };

        let warp = Self {
            name,
            path,
            operator_name,
            result_descriptor,
            params,
            source,
            state,
            source_srs,
            target_srs,
            reproject,
            kernel,
            kernel_in_resolution: in_resolution,
            mode_memory_budget: context.warp_mode_max_memory_per_tile(),
            approximation_error,
            tiling_specification,
        };
        warp.ensure_mode_fits_memory_budget(context.thread_pool().current_num_threads())?;
        Ok(warp)
    }

    /// Checks that `mode` keeps at most `self.mode_memory_budget` bytes per output tile (see
    /// [`kernels::mode_memory_per_tile`]), with `threads` threads that compute output rows at the
    /// same time. With reprojection, the footprints also depend on the output tile, so the
    /// processor checks them again for each tile.
    ///
    /// # Errors
    /// If the estimate exceeds the budget.
    fn ensure_mode_fits_memory_budget(&self, threads: usize) -> Result<()> {
        let budget = self.mode_memory_budget;
        if self.kernel != Some(WarpKernel::Mode) {
            return Ok(());
        }
        let data_type = self.source.result_descriptor().data_type;
        let in_resolution = self.kernel_in_resolution;
        let out_resolution = self.result_descriptor.spatial_grid.spatial_resolution();
        let estimate = kernels::mode_memory_per_tile(
            data_type,
            (
                (out_resolution.y() / in_resolution.y()).get(),
                (out_resolution.x() / in_resolution.x()).get(),
            ),
            self.tiling_specification.tile_size_in_pixels,
            threads,
        );
        kernels::ensure_mode_fits_memory_budget(&estimate, budget, data_type)
    }
}

/// The kernel that computes the output pixels from the input pixels for an output with
/// `out_resolution` from an input with `in_resolution`, `None` for nearest neighbour. Upsampling is
/// only for outputs that are finer in both axes, as an output that is coarser in one axis covers
/// several input pixels.
fn warp_kernel(
    params: &WarpParams,
    in_resolution: SpatialResolution,
    out_resolution: SpatialResolution,
) -> Option<WarpKernel> {
    let finer = out_resolution.x() < in_resolution.x() && out_resolution.y() < in_resolution.y();
    if finer {
        match params.upsampling {
            WarpUpsampling::Nearest => None,
            WarpUpsampling::Bilinear => Some(WarpKernel::Bilinear),
        }
    } else {
        match params.downsampling {
            WarpDownsampling::Nearest => None,
            WarpDownsampling::Average => Some(WarpKernel::Average),
            WarpDownsampling::Mode => Some(WarpKernel::Mode),
            WarpDownsampling::Min => Some(WarpKernel::Min),
            WarpDownsampling::Max => Some(WarpKernel::Max),
        }
    }
}

/// `processor` with its pixels converted to `data_type` with [`RoundingTypeConversionProcessor`],
/// `descriptor` with `data_type` as its result descriptor.
fn converted_processor(
    processor: TypedRasterQueryProcessor,
    descriptor: &RasterResultDescriptor,
    data_type: RasterDataType,
) -> TypedRasterQueryProcessor {
    if processor.raster_data_type() == data_type {
        return processor;
    }
    let descriptor = RasterResultDescriptor {
        data_type,
        ..descriptor.clone()
    };
    call_on_generic_raster_processor!(processor, p => {
        call_generic_raster_processor!(
            data_type,
            RoundingTypeConversionProcessor::new(p, descriptor).boxed()
        )
    })
}

impl<O: InitializedRasterOperator> InitializedWarp<O> {
    /// The processor that looks up the input pixel under the center of each output pixel, in the
    /// input data type.
    fn nearest_neighbor_processor(
        &self,
        source: TypedRasterQueryProcessor,
    ) -> TypedRasterQueryProcessor {
        let descriptor = RasterResultDescriptor {
            data_type: self.source.result_descriptor().data_type,
            ..self.result_descriptor.clone()
        };
        let in_resolution = self
            .source
            .result_descriptor()
            .spatial_grid
            .spatial_resolution();
        let out_resolution = self.result_descriptor.spatial_grid.spatial_resolution();
        // finer or equal in both axes, so the input of an output tile is at most one tile
        let finer =
            out_resolution.x() <= in_resolution.x() && out_resolution.y() <= in_resolution.y();
        call_on_generic_raster_processor!(source, p => {
            if self.reproject {
                RasterReprojectionProcessor::new(
                    p,
                    descriptor,
                    self.source_srs,
                    self.target_srs,
                    self.state,
                    self.approximation_error,
                ).boxed().into()
            } else if finer {
                // the same spatial reference: look up the input pixel by its index, without
                // projecting the coordinates of all output pixels; `Interpolation` collects the
                // input of an output tile and resamples it in one pass, which is faster but keeps
                // the whole input of the tile, so only for finer outputs
                InterploationProcessor::<_, _, NearestNeighbor>::new(
                    p,
                    descriptor,
                    self.tiling_specification,
                ).boxed().into()
            } else {
                DownsampleProcessor::new(p, descriptor, self.tiling_specification).boxed().into()
            }
        })
    }
}

/// The operator an [`InitializedWarp`] was initialized from.
pub struct WarpOrigin {
    /// the canonic name of that operator
    pub name: CanonicOperatorName,
    /// its path in the processing graph
    pub path: ProcessingGraphOperatorPath,
    /// its type name, e.g. `Downsampling`
    pub operator_name: &'static str,
    /// project the coordinates even within the same spatial reference, as `Reprojection` does
    pub always_reproject: bool,
}

impl<O: InitializedRasterOperator> InitializedRasterOperator for InitializedWarp<O> {
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source = self.source.query_processor()?;
        let in_descriptor = self.source.result_descriptor();
        let out_data_type = self.result_descriptor.data_type;

        // the same grid: nothing to warp
        if !self.reproject
            && in_descriptor.spatial_grid.as_parts().1
                == self.result_descriptor.spatial_grid.as_parts().1
        {
            return Ok(converted_processor(
                source,
                &self.result_descriptor,
                out_data_type,
            ));
        }

        let Some(kernel) = self.kernel else {
            let warped = self.nearest_neighbor_processor(source);
            return Ok(converted_processor(
                warped,
                &self.result_descriptor,
                out_data_type,
            ));
        };

        // `mode` picks one of the input values, so it computes in the input type, which keeps the
        // histogram for 8-bit inputs (see `kernels::ModeState`); the others compute in the output
        // data type if it holds the input values, e.g. the average of `U8` values in `F32`; for a
        // float output that does not, e.g. `I32` → `F32`, in `F64`, so that the fractions are kept
        // until the end; otherwise in the input type, rounding once at the end
        let working_data_type = if kernel == WarpKernel::Mode {
            in_descriptor.data_type
        } else if out_data_type.represents_losslessly(in_descriptor.data_type) {
            out_data_type
        } else if matches!(out_data_type, RasterDataType::F32 | RasterDataType::F64) {
            RasterDataType::F64
        } else {
            in_descriptor.data_type
        };
        let source = converted_processor(source, in_descriptor, working_data_type);
        let descriptor = RasterResultDescriptor {
            data_type: working_data_type,
            ..self.result_descriptor.clone()
        };
        let projection = self.reproject.then_some(KernelProjection {
            in_srs: self.source_srs,
            out_srs: self.target_srs,
            grids: self.state,
            approximation_error: self.approximation_error,
        });
        let warped = call_on_generic_raster_processor!(source, p => WarpKernelProcessor::new(
            p,
            kernel,
            projection,
            descriptor,
            self.mode_memory_budget,
        ).boxed().into());
        Ok(converted_processor(
            warped,
            &self.result_descriptor,
            out_data_type,
        ))
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        self.operator_name
    }

    fn path(&self) -> ProcessingGraphOperatorPath {
        self.path.clone()
    }

    fn optimize(
        &self,
        target_resolution: SpatialResolution,
    ) -> Result<TypedRasterOperator, OptimizationError> {
        self.ensure_resolution_is_compatible_for_optimization(target_resolution)?;

        let params = WarpParams {
            output_resolution: Some(target_resolution),
            // keep the alignment of the output grid
            output_origin_reference: Some(
                self.result_descriptor
                    .spatial_grid
                    .geo_transform()
                    .origin_coordinate,
            ),
            ..self.params
        };

        let source = match source_at_overview_level(&self.source, &params) {
            Ok(Some(optimized)) => optimized,
            Ok(None) => {
                let in_grid = &self.source.result_descriptor().spatial_grid;
                let optimized_source = self.source.optimize(in_grid.spatial_resolution())?;

                // the source is already on the output grid: nothing to warp
                if self.source_srs == self.target_srs
                    && self.source.result_descriptor().data_type == self.result_descriptor.data_type
                    && in_grid.spatial_resolution() == target_resolution
                    && in_grid.geo_transform().origin_coordinate
                        == self
                            .result_descriptor
                            .spatial_grid
                            .geo_transform()
                            .origin_coordinate
                {
                    return Ok(optimized_source);
                }

                optimized_source
            }
            Err(Error::Optimization { source }) => return Err(source),
            // the output grid cannot be computed for the optimized source
            Err(error) => {
                return Err(OptimizationError::ProjectionOptimizationFailed {
                    source: Box::new(error),
                });
            }
        };

        Ok(Warp {
            params,
            sources: SingleRasterSource { raster: source },
        }
        .typed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::BoxRasterQueryProcessor;
    use crate::engine::{MockExecutionContext, SingleRasterOrVectorSource};
    use crate::processing::{
        Downsampling, DownsamplingMethod, DownsamplingParams, DownsamplingResolution,
        Interpolation, InterpolationMethod, InterpolationParams, InterpolationResolution,
        Reprojection,
    };
    use crate::source::GdalMetaDataStatic;
    use crate::source::{GdalSource, GdalSourceParameters};
    use crate::util::gdal::{
        add_ndvi_dataset, gdal_open_dataset, gdal_parameters_from_dataset,
        raster_descriptor_from_dataset,
    };
    use crate::util::test::AsciiRasterTilesExt;
    use futures::StreamExt;
    use geoengine_datatypes::dataset::{DatasetId, NamedData};
    use geoengine_datatypes::primitives::{BandSelection, RasterQueryRectangle, TimeInterval};
    use geoengine_datatypes::raster::{
        GeoTransform, GridBoundingBox2D, GridBounds, GridIndexAccess, GridShape2D, Pixel,
        RasterTile2D,
    };
    use geoengine_datatypes::raster::{GridContains, GridIdx2D};
    use geoengine_datatypes::test_data;
    use geoengine_datatypes::util::Identifier;
    use geoengine_datatypes::util::test::{
        AsciiRasterTiles, TestDefault, assert_eq_two_list_of_tiles, assert_raster_tiles_eq_ascii,
    };
    use indoc::indoc;
    use num_traits::AsPrimitive;

    /// 6 × 6 pixels of 1° in tiles of 3 × 3, the value is 10 × row + column
    const INPUT: &str = indoc! {"
        [0, 10)
        +----------+----------+
        |  0  1  2 |  3  4  5 |
        | 10 11 12 | 13 14 15 |
        | 20 21 22 | 23 24 25 |
        +----------+----------+
        | 30 31 32 | 33 34 35 |
        | 40 41 42 | 43 44 45 |
        | 50 51 52 | 53 54 55 |
        +----------+----------+
    "};

    fn input() -> AsciiRasterTiles<'static> {
        AsciiRasterTiles::new(INPUT).tile_origin([-2, 0])
    }

    fn mock_exe_ctx() -> MockExecutionContext {
        MockExecutionContext::new_with_tiling_spec(input().tiling_specification())
    }

    fn warp(source: TypedRasterOperator, params: WarpParams) -> TypedRasterOperator {
        Warp {
            params,
            sources: SingleRasterSource { raster: source },
        }
        .typed()
    }

    fn resample_params(resolution: SpatialResolution) -> WarpParams {
        WarpParams {
            target_spatial_reference: None,
            output_resolution: Some(resolution),
            output_origin_reference: None,
            ..WarpParams::default()
        }
    }

    /// Queries all bands of the whole result of `operator` at the time of the input.
    async fn query_all(
        operator: TypedRasterOperator,
        exe_ctx: &MockExecutionContext,
    ) -> (RasterResultDescriptor, Vec<RasterTile2D<u8>>) {
        query(operator, exe_ctx, TimeInterval::new_unchecked(0, 10), None).await
    }

    async fn query(
        operator: TypedRasterOperator,
        exe_ctx: &MockExecutionContext,
        time: TimeInterval,
        bounds: Option<GridBoundingBox2D>,
    ) -> (RasterResultDescriptor, Vec<RasterTile2D<u8>>) {
        query_typed(
            operator,
            exe_ctx,
            time,
            bounds,
            TypedRasterQueryProcessor::get_u8,
        )
        .await
    }

    async fn query_typed<T: Pixel>(
        operator: TypedRasterOperator,
        exe_ctx: &MockExecutionContext,
        time: TimeInterval,
        bounds: Option<GridBoundingBox2D>,
        get: fn(TypedRasterQueryProcessor) -> Option<BoxRasterQueryProcessor<T>>,
    ) -> (RasterResultDescriptor, Vec<RasterTile2D<T>>) {
        let initialized = operator
            .initialize(ProcessingGraphOperatorPath::initialize_root(), exe_ctx)
            .await
            .unwrap();
        let descriptor = initialized.result_descriptor().clone();
        let processor = get(initialized.query_processor().unwrap()).unwrap();
        let query_ctx = exe_ctx.mock_query_context(TestDefault::test_default());
        let bounds = bounds.unwrap_or_else(|| descriptor.spatial_grid.as_parts().1.grid_bounds());
        let tiles = processor
            .raster_query(
                RasterQueryRectangle::new(bounds, time, BandSelection::first()),
                &query_ctx,
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect()
            .await;
        (descriptor, tiles)
    }

    #[tokio::test]
    async fn it_keeps_the_name_of_the_operator_it_was_initialized_from() {
        let exe_ctx = mock_exe_ctx();
        let source = || input().mock_raster_source::<u8>().typed();

        let operators = [
            (
                warp(
                    source(),
                    resample_params(SpatialResolution::new_unchecked(2., 2.)),
                ),
                "Warp",
            ),
            (
                Downsampling {
                    params: DownsamplingParams {
                        sampling_method: DownsamplingMethod::NearestNeighbor,
                        output_resolution: DownsamplingResolution::Resolution(
                            SpatialResolution::new_unchecked(2., 2.),
                        ),
                        output_origin_reference: None,
                    },
                    sources: SingleRasterSource { raster: source() },
                }
                .typed(),
                "Downsampling",
            ),
            (
                Interpolation {
                    params: InterpolationParams {
                        interpolation: InterpolationMethod::NearestNeighbor,
                        output_resolution: InterpolationResolution::Resolution(
                            SpatialResolution::new_unchecked(0.5, 0.5),
                        ),
                        output_origin_reference: None,
                    },
                    sources: SingleRasterSource { raster: source() },
                }
                .typed(),
                "Interpolation",
            ),
            (
                Reprojection {
                    params: ReprojectionParams {
                        target_spatial_reference: SpatialReference::web_mercator(),
                        derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
                    },
                    sources: SingleRasterOrVectorSource {
                        source: source().into(),
                    },
                }
                .typed(),
                "Reprojection",
            ),
        ];

        for (operator, name) in operators {
            let initialized = operator
                .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
                .await
                .unwrap();
            assert_eq!(initialized.name(), name);
        }
    }

    #[tokio::test]
    async fn it_resamples_coarser_in_one_axis_and_finer_in_the_other() {
        let exe_ctx = mock_exe_ctx();
        let source = input().mock_raster_source::<u8>().typed();

        let (descriptor, tiles) = query_all(
            warp(
                source,
                resample_params(SpatialResolution::new_unchecked(3., 0.5)),
            ),
            &exe_ctx,
        )
        .await;

        assert_eq!(
            descriptor.spatial_grid.spatial_resolution(),
            SpatialResolution::new_unchecked(3., 0.5)
        );
        // the columns of the pixel centers at x = 1.5 and 4.5, every row twice
        assert_raster_tiles_eq_ascii(
            &tiles,
            &AsciiRasterTiles::new(indoc! {"
                [0, 10)
                +----------+
                |  1  4  _ |
                |  1  4  _ |
                | 11 14  _ |
                +----------+
                | 11 14  _ |
                | 21 24  _ |
                | 21 24  _ |
                +----------+
                | 31 34  _ |
                | 31 34  _ |
                | 41 44  _ |
                +----------+
                | 41 44  _ |
                | 51 54  _ |
                | 51 54  _ |
                +----------+
            "})
            .tile_origin([-4, 0])
            .geo_transform(GeoTransform::new_unchecked(
                Coordinate2D::new(0., 0.),
                3.,
                -0.5,
            )),
        );
    }

    #[tokio::test]
    async fn it_resamples_coarser_like_downsampling() {
        let exe_ctx = mock_exe_ctx();
        let source = input().mock_raster_source::<u8>().typed();
        let resolution = SpatialResolution::new_unchecked(3., 3.);

        let (_, warped) =
            query_all(warp(source.clone(), resample_params(resolution)), &exe_ctx).await;
        let (_, downsampled) = query_all(
            Downsampling {
                params: DownsamplingParams {
                    sampling_method: DownsamplingMethod::NearestNeighbor,
                    output_resolution: DownsamplingResolution::Resolution(resolution),
                    output_origin_reference: None,
                },
                sources: SingleRasterSource { raster: source },
            }
            .typed(),
            &exe_ctx,
        )
        .await;

        assert_raster_tiles_eq_ascii(
            &warped,
            &AsciiRasterTiles::new(indoc! {"
                [0, 10)
                +----------+
                |  _  _  _ |
                | 11 14  _ |
                | 41 44  _ |
                +----------+
            "})
            .tile_origin([-1, 0])
            .geo_transform(GeoTransform::new_unchecked(
                Coordinate2D::new(0., 0.),
                3.,
                -3.,
            )),
        );
        assert_eq_two_list_of_tiles(&warped, &downsampled, false);
    }

    #[tokio::test]
    async fn it_resamples_finer_like_interpolation() {
        let exe_ctx = mock_exe_ctx();
        let source = input().mock_raster_source::<u8>().typed();
        let resolution = SpatialResolution::new_unchecked(0.5, 0.5);

        let (_, warped) =
            query_all(warp(source.clone(), resample_params(resolution)), &exe_ctx).await;
        let (_, interpolated) = query_all(
            Interpolation {
                params: InterpolationParams {
                    interpolation: InterpolationMethod::NearestNeighbor,
                    output_resolution: InterpolationResolution::Resolution(resolution),
                    output_origin_reference: None,
                },
                sources: SingleRasterSource { raster: source },
            }
            .typed(),
            &exe_ctx,
        )
        .await;

        assert_eq!(warped.len(), 16);
        assert_eq_two_list_of_tiles(&warped, &interpolated, false);
    }

    #[tokio::test]
    async fn it_moves_the_origin() {
        let exe_ctx = mock_exe_ctx();
        let source = input().mock_raster_source::<u8>().typed();

        let (descriptor, tiles) = query_all(
            warp(
                source,
                WarpParams {
                    output_origin_reference: Some(Coordinate2D::new(0.5, 0.5)),
                    ..resample_params(SpatialResolution::new_unchecked(1., 1.))
                },
            ),
            &exe_ctx,
        )
        .await;

        assert_eq!(
            descriptor.spatial_grid.geo_transform().origin_coordinate,
            Coordinate2D::new(0.5, 0.5)
        );
        // each output pixel center lies on a corner of four input pixels and takes the one below and
        // to the right of it, so the values move up by one row
        assert_raster_tiles_eq_ascii(
            &tiles,
            &AsciiRasterTiles::new(indoc! {"
                [0, 10)
                +----------+----------+----------+
                |  _  _ 10 | 11 12 13 | 14 15  _ |
                |  _  _ 20 | 21 22 23 | 24 25  _ |
                |  _  _ 30 | 31 32 33 | 34 35  _ |
                +----------+----------+----------+
                |  _  _ 40 | 41 42 43 | 44 45  _ |
                |  _  _ 50 | 51 52 53 | 54 55  _ |
                |  _  _  _ |  _  _  _ |  _  _  _ |
                +----------+----------+----------+
            "})
            .tile_origin([-2, -1])
            .geo_transform(GeoTransform::new_unchecked(
                Coordinate2D::new(0.5, -0.5),
                1.,
                -1.,
            )),
        );
    }

    #[tokio::test]
    async fn it_forwards_the_source_on_the_same_grid() {
        let exe_ctx = mock_exe_ctx();
        let source = input().mock_raster_source::<u8>().typed();

        let (_, tiles) = query_all(
            warp(
                source,
                resample_params(SpatialResolution::new_unchecked(1., 1.)),
            ),
            &exe_ctx,
        )
        .await;

        assert_raster_tiles_eq_ascii(&tiles, &input());
    }

    /// 2014-04-01 of the NDVI test data
    fn ndvi_time() -> TimeInterval {
        TimeInterval::new_unchecked(1_396_303_200_000, 1_396_389_600_000)
    }

    #[tokio::test]
    async fn it_reprojects_like_reprojection() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let ndvi = GdalSource {
            params: GdalSourceParameters::new(add_ndvi_dataset(&mut exe_ctx)),
        }
        .typed();
        exe_ctx.tiling_specification = TilingSpecification::new(GridShape2D::new_2d(512, 512));

        let reprojection = Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: SpatialReference::web_mercator(),
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: SingleRasterOrVectorSource {
                source: ndvi.clone().into(),
            },
        }
        .typed();
        let (reprojected_descriptor, reprojected) =
            query(reprojection, &exe_ctx, ndvi_time(), None).await;

        let (warped_descriptor, warped) = query(
            warp(
                ndvi,
                WarpParams {
                    target_spatial_reference: Some(SpatialReference::web_mercator()),
                    output_resolution: None,
                    output_origin_reference: None,
                    ..WarpParams::default()
                },
            ),
            &exe_ctx,
            ndvi_time(),
            None,
        )
        .await;

        assert_eq!(warped_descriptor, reprojected_descriptor);
        assert_ne!(warped.len(), 0);
        assert_eq_two_list_of_tiles(&warped, &reprojected, false);
    }

    #[tokio::test]
    async fn it_reprojects_to_another_resolution() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let ndvi = GdalSource {
            params: GdalSourceParameters::new(add_ndvi_dataset(&mut exe_ctx)),
        }
        .typed();
        exe_ctx.tiling_specification = TilingSpecification::new(GridShape2D::new_2d(512, 512));
        let resolution = SpatialResolution::new_unchecked(40_000., 20_000.);

        let (descriptor, tiles) = query(
            warp(
                ndvi,
                WarpParams {
                    target_spatial_reference: Some(SpatialReference::web_mercator()),
                    output_resolution: Some(resolution),
                    output_origin_reference: None,
                    ..WarpParams::default()
                },
            ),
            &exe_ctx,
            ndvi_time(),
            None,
        )
        .await;

        assert_eq!(
            descriptor.spatial_reference,
            SpatialReference::web_mercator().into()
        );
        assert_eq!(descriptor.spatial_grid.spatial_resolution(), resolution);
        assert!(tiles.iter().any(|tile| !tile.grid_array.is_empty()));
    }

    #[tokio::test]
    async fn it_fills_whole_tiles_when_reprojecting() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let ndvi = GdalSource {
            params: GdalSourceParameters::new(add_ndvi_dataset(&mut exe_ctx)),
        }
        .typed();
        exe_ctx.tiling_specification = TilingSpecification::new(GridShape2D::new_2d(16, 16));
        let tile_size = 16;

        // coarser with `average`, finer with `bilinear`, and both with nearest neighbour
        let methods = [
            (WarpDownsampling::Average, WarpUpsampling::Bilinear),
            (WarpDownsampling::Nearest, WarpUpsampling::Nearest),
        ];
        for ((downsampling, upsampling), resolution) in methods
            .into_iter()
            .flat_map(|methods| [30_000., 5_000.].map(|resolution| (methods, resolution)))
        {
            let operator = warp(
                ndvi.clone(),
                WarpParams {
                    target_spatial_reference: Some(SpatialReference::web_mercator()),
                    output_resolution: Some(SpatialResolution::new_unchecked(
                        resolution, resolution,
                    )),
                    downsampling,
                    upsampling,
                    ..WarpParams::default()
                },
            );
            let initialized = operator
                .clone()
                .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
                .await
                .unwrap();
            // the tile over land in Central Europe (10° E, 50° N)
            let [y, x] = initialized
                .result_descriptor()
                .spatial_grid
                .tiling_grid_definition(exe_ctx.tiling_specification)
                .tiling_geo_transform()
                .coordinate_to_grid_idx_2d((1_113_195., 6_446_276.).into())
                .0;
            let [min_y, min_x] =
                [y.div_euclid(tile_size), x.div_euclid(tile_size)].map(|tile| tile * tile_size);
            let tile = GridBoundingBox2D::new_unchecked(
                [min_y, min_x],
                [min_y + tile_size - 1, min_x + tile_size - 1],
            );
            // only its upper left quarter
            let quarter = GridBoundingBox2D::new_unchecked(
                [min_y, min_x],
                [min_y + tile_size / 2 - 1, min_x + tile_size / 2 - 1],
            );

            let (_, whole) = query(operator.clone(), &exe_ctx, ndvi_time(), Some(tile)).await;
            let (_, part) = query(operator, &exe_ctx, ndvi_time(), Some(quarter)).await;

            // one tile per time step
            assert_ne!(whole.len(), 0);
            let outside_of_the_quarter = whole.iter().all(|tile| {
                (tile_size / 2..tile_size)
                    .any(|y| tile.get_at_grid_index_unchecked([y, y]).is_some())
            });
            assert!(
                outside_of_the_quarter,
                "{downsampling:?}, {upsampling:?} at {resolution} m: the tile should have data \
                 outside of the quarter"
            );
            assert_eq_two_list_of_tiles(&part, &whole, false);
        }
    }

    #[tokio::test]
    async fn it_checks_the_memory_of_mode_per_tile_when_reprojecting() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let ndvi = GdalSource {
            params: GdalSourceParameters::new(add_ndvi_dataset(&mut exe_ctx)),
        }
        .typed();
        exe_ctx.tiling_specification = TilingSpecification::new(GridShape2D::new_2d(512, 512));
        // towards the pole, the footprints of 10 km in the polar stereographic projection grow
        // from 5 × 5 pixels of 0.1° at the reference resolution to up to 1800 pixels. With them,
        // `mode` keeps ≈ 12.3 MB, while the estimate at initialization is ≈ 10 MB.
        let operator = warp(
            ndvi,
            WarpParams {
                target_spatial_reference: Some(SpatialReference::new(
                    geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
                    3413,
                )),
                output_resolution: Some(SpatialResolution::new_unchecked(10_000., 10_000.)),
                downsampling: WarpDownsampling::Mode,
                ..WarpParams::default()
            },
        );

        let (_, tiles) = query(operator.clone(), &exe_ctx, ndvi_time(), None).await;
        assert!(tiles.iter().any(|tile| !tile.is_empty()));

        exe_ctx.warp_mode_max_memory_per_tile = 12 * 1024 * 1024;
        let initialized = operator
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();
        let processor = initialized.query_processor().unwrap().get_u8().unwrap();
        let bounds = initialized
            .result_descriptor()
            .spatial_grid
            .as_parts()
            .1
            .grid_bounds();
        let query_ctx = exe_ctx.mock_query_context(TestDefault::test_default());
        let results: Vec<_> = processor
            .raster_query(
                RasterQueryRectangle::new(bounds, ndvi_time(), BandSelection::first()),
                &query_ctx,
            )
            .await
            .unwrap()
            .collect()
            .await;
        assert!(
            results.iter().any(|result| matches!(
                result,
                Err(Error::WarpModeExceedsMemoryBudget {
                    budget_mb: 12,
                    data_type: RasterDataType::U8,
                    ..
                })
            )),
            "{:?}",
            results
                .iter()
                .filter_map(|result| result.as_ref().err())
                .collect::<Vec<_>>()
        );
    }

    /// `approximationErrorThreshold` overrides the default of the execution context, and 0
    /// projects exactly
    #[tokio::test]
    async fn it_approximates_the_reprojection_with_the_threshold_of_the_parameter_or_context() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let ndvi = GdalSource {
            params: GdalSourceParameters::new(add_ndvi_dataset(&mut exe_ctx)),
        }
        .typed();
        exe_ctx.tiling_specification = TilingSpecification::new(GridShape2D::new_2d(512, 512));
        let threshold = |px: f64| geoengine_datatypes::util::NonNegativeFiniteF64::new(px).unwrap();
        // in the polar stereographic projection, a row of output pixels is a curve in the input,
        // so a large error lets the interpolation move the pixels, unlike in EPSG:3857
        let operator = |approximation_error_threshold| {
            warp(
                ndvi.clone(),
                WarpParams {
                    target_spatial_reference: Some(SpatialReference::new(
                        geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
                        3413,
                    )),
                    output_resolution: Some(SpatialResolution::new_unchecked(5_000., 5_000.)),
                    approximation_error_threshold,
                    ..WarpParams::default()
                },
            )
        };

        // a tile in the middle of the grid, whose rows lie within the area of use, as only those
        // are approximated
        let grid = operator(None)
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .result_descriptor()
            .spatial_grid
            .tiling_grid_definition(exe_ctx.tiling_specification)
            .tiling_grid_bounds();
        let [center_y, center_x] = [0, 1].map(|axis| {
            (grid.min_index().0[axis] + grid.max_index().0[axis]).div_euclid(2 * 512) * 512
        });
        let tile = Some(GridBoundingBox2D::new_unchecked(
            [center_y, center_x],
            [center_y + 511, center_x + 511],
        ));

        exe_ctx.reprojection_approximation_error = threshold(0.);
        let (_, exact) = query(operator(None), &exe_ctx, ndvi_time(), tile).await;
        exe_ctx.reprojection_approximation_error = threshold(50.);
        let (_, coarse) = query(operator(None), &exe_ctx, ndvi_time(), tile).await;
        let (_, exact_by_parameter) =
            query(operator(Some(threshold(0.))), &exe_ctx, ndvi_time(), tile).await;

        assert!(exact.iter().any(|tile| !tile.is_empty()));
        assert_eq_two_list_of_tiles(&exact_by_parameter, &exact, false);
        assert_ne!(
            coarse
                .iter()
                .map(|tile| &tile.grid_array)
                .collect::<Vec<_>>(),
            exact
                .iter()
                .map(|tile| &tile.grid_array)
                .collect::<Vec<_>>(),
            "an error of 50 input pixels must change the result"
        );
    }

    #[tokio::test]
    async fn it_reads_an_overview_level_for_a_coarser_output() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let ndvi = GdalSource {
            params: GdalSourceParameters::new(add_ndvi_dataset(&mut exe_ctx)),
        }
        .typed();
        let initialized = ndvi
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        // 0.1° NDVI: coarser by 5 in x and 3 in y, so the overview level of the finer axis, 2
        let optimized = source_at_overview_level(
            initialized.as_ref(),
            &resample_params(SpatialResolution::new_unchecked(0.5, 0.3)),
        )
        .unwrap();
        let Some(TypedRasterOperator::GdalSource(gdal_source)) = optimized else {
            panic!("the source should be optimized, was {optimized:?}");
        };
        assert_eq!(
            gdal_source.params.overview_level,
            std::num::NonZeroU32::new(2)
        );

        // finer in one axis: the source stays as it is
        assert!(
            source_at_overview_level(
                initialized.as_ref(),
                &resample_params(SpatialResolution::new_unchecked(0.5, 0.05)),
            )
            .unwrap()
            .is_none()
        );
    }

    #[tokio::test]
    async fn it_optimizes_to_a_coarser_warp() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let ndvi = GdalSource {
            params: GdalSourceParameters::new(add_ndvi_dataset(&mut exe_ctx)),
        }
        .typed();
        let initialized = warp(
            ndvi,
            resample_params(SpatialResolution::new_unchecked(0.2, 0.1)),
        )
        .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap();

        let optimized = initialized
            .optimize(SpatialResolution::new_unchecked(0.4, 0.2))
            .unwrap();

        let TypedRasterOperator::Warp(optimized) = optimized else {
            panic!("the optimized operator should be a warp, was {optimized:?}");
        };
        assert_eq!(
            optimized.params.output_resolution,
            Some(SpatialResolution::new_unchecked(0.4, 0.2))
        );
        let TypedRasterOperator::GdalSource(gdal_source) = &optimized.sources.raster else {
            panic!("the source should be a GDAL source");
        };
        assert_eq!(
            gdal_source.params.overview_level,
            std::num::NonZeroU32::new(2)
        );
    }

    #[test]
    fn it_deserializes_params_with_defaults() {
        let params: WarpParams = serde_json::from_str("{}").unwrap();

        assert_eq!(
            params,
            WarpParams {
                target_spatial_reference: None,
                output_resolution: None,
                output_origin_reference: None,
                ..WarpParams::default()
            }
        );
    }

    /// 6 × 6 classes of 1° in tiles of 3 × 3, with ties and no-data for the 2 × 2 blocks
    const CLASSES: &str = indoc! {"
        [0, 10)
        +-------+-------+
        | 1 1 2 | 2 3 4 |
        | 1 2 _ | 2 4 3 |
        | 5 5 6 | 6 _ _ |
        +-------+-------+
        | 5 6 7 | 6 _ _ |
        | 7 7 7 | 8 8 9 |
        | 7 7 7 | 9 9 8 |
        +-------+-------+
    "};

    fn classes() -> AsciiRasterTiles<'static> {
        AsciiRasterTiles::new(CLASSES).tile_origin([-2, 0])
    }

    fn downsampling_params(resolution: f64, downsampling: WarpDownsampling) -> WarpParams {
        WarpParams {
            downsampling,
            ..resample_params(SpatialResolution::new_unchecked(resolution, resolution))
        }
    }

    /// The 3 × 3 pixels of 2° of the result
    fn two_degrees(pixels: &str) -> AsciiRasterTiles<'_> {
        AsciiRasterTiles::new(pixels)
            .tile_origin([-1, 0])
            .geo_transform(GeoTransform::new_unchecked(
                Coordinate2D::new(0., 0.),
                2.,
                -2.,
            ))
    }

    /// The values of `tiles` by their global pixel index
    fn pixels<T: Pixel>(tiles: &[RasterTile2D<T>]) -> Vec<([isize; 2], Option<T>)> {
        tiles
            .iter()
            .flat_map(|tile| {
                let bounds = tile.tile_information().global_pixel_bounds();
                let [min_y, min_x] = bounds.min_index().0;
                let [max_y, max_x] = bounds.max_index().0;
                (min_y..=max_y).flat_map(move |y| {
                    (min_x..=max_x).map(move |x| {
                        let local = [y - min_y, x - min_x];
                        ([y, x], tile.get_at_grid_index_unchecked(local))
                    })
                })
            })
            .collect()
    }

    #[tokio::test]
    async fn it_aggregates_coarser_pixels_with_the_downsampling_method() {
        let exe_ctx = mock_exe_ctx();
        let cases = [
            (
                WarpDownsampling::Mode,
                // ties go to the value that reaches the maximum first in row-major order
                indoc! {"
                    [0, 10)
                    +-------+
                    | 1 2 4 |
                    | 5 6 _ |
                    | 7 7 9 |
                    +-------+
                "},
            ),
            (
                WarpDownsampling::Min,
                indoc! {"
                    [0, 10)
                    +-------+
                    | 1 2 3 |
                    | 5 6 _ |
                    | 7 7 8 |
                    +-------+
                "},
            ),
            (
                WarpDownsampling::Max,
                indoc! {"
                    [0, 10)
                    +-------+
                    | 2 2 4 |
                    | 6 7 _ |
                    | 7 9 9 |
                    +-------+
                "},
            ),
            (
                // 1.25, 2, 3.5 / 5.25, 6.25, _ / 7, 7.75, 8.5, rounded half up
                WarpDownsampling::Average,
                indoc! {"
                    [0, 10)
                    +-------+
                    | 1 2 4 |
                    | 5 6 _ |
                    | 7 8 9 |
                    +-------+
                "},
            ),
        ];

        for (method, expected) in cases {
            let source = classes().mock_raster_source::<u8>().typed();
            let (_, tiles) =
                query_all(warp(source, downsampling_params(2., method)), &exe_ctx).await;

            assert_raster_tiles_eq_ascii(&tiles, &two_degrees(expected));
        }
    }

    #[tokio::test]
    async fn it_weights_partly_covered_pixels() {
        let exe_ctx = mock_exe_ctx();
        let source = input().mock_raster_source::<u8>().typed();

        let (descriptor, tiles) = query_typed(
            warp(
                source,
                WarpParams {
                    output_data_type: Some(RasterDataType::F32),
                    ..downsampling_params(1.5, WarpDownsampling::Average)
                },
            ),
            &exe_ctx,
            TimeInterval::new_unchecked(0, 10),
            None,
            TypedRasterQueryProcessor::get_f32,
        )
        .await;

        assert_eq!(descriptor.data_type, RasterDataType::F32);
        // an output pixel covers one input pixel fully and the next by half, in each axis
        let means = [1. / 3., 5. / 3., 10. / 3., 14. / 3.];
        let pixels = pixels(&tiles);
        assert_eq!(
            pixels.iter().filter(|(_, value)| value.is_some()).count(),
            16
        );
        for ([y, x], value) in pixels {
            let Some(value) = value else { continue };
            // the output rows -4..=-1 cover the input rows 0..6 from the top
            let row = usize::try_from(y + 4).unwrap();
            let column = usize::try_from(x).unwrap();
            let expected = 10. * means[row] + means[column];
            assert!(
                (f64::from(value) - expected).abs() < 1e-5,
                "pixel {y}, {x}: {value} != {expected}"
            );
        }
    }

    #[tokio::test]
    async fn it_averages_coarser_in_one_axis_and_finer_in_the_other() {
        let exe_ctx = mock_exe_ctx();
        let source = input().mock_raster_source::<u8>().typed();

        let (_, tiles) = query_all(
            warp(
                source,
                WarpParams {
                    downsampling: WarpDownsampling::Average,
                    // the upsampling method is for outputs that are finer in both axes
                    upsampling: WarpUpsampling::Bilinear,
                    ..resample_params(SpatialResolution::new_unchecked(3., 0.5))
                },
            ),
            &exe_ctx,
        )
        .await;

        // the average of three columns, every row twice
        assert_raster_tiles_eq_ascii(
            &tiles,
            &AsciiRasterTiles::new(indoc! {"
                [0, 10)
                +----------+
                |  1  4  _ |
                |  1  4  _ |
                | 11 14  _ |
                +----------+
                | 11 14  _ |
                | 21 24  _ |
                | 21 24  _ |
                +----------+
                | 31 34  _ |
                | 31 34  _ |
                | 41 44  _ |
                +----------+
                | 41 44  _ |
                | 51 54  _ |
                | 51 54  _ |
                +----------+
            "})
            .tile_origin([-4, 0])
            .geo_transform(GeoTransform::new_unchecked(
                Coordinate2D::new(0., 0.),
                3.,
                -0.5,
            )),
        );
    }

    #[tokio::test]
    async fn it_interpolates_finer_pixels_bilinearly() {
        let exe_ctx = mock_exe_ctx();
        let source = input().mock_raster_source::<u8>().typed();

        let (_, tiles) = query_typed(
            warp(
                source,
                WarpParams {
                    upsampling: WarpUpsampling::Bilinear,
                    output_data_type: Some(RasterDataType::F32),
                    ..resample_params(SpatialResolution::new_unchecked(0.5, 0.5))
                },
            ),
            &exe_ctx,
            TimeInterval::new_unchecked(0, 10),
            None,
            TypedRasterQueryProcessor::get_f32,
        )
        .await;

        // the centers of the output pixels lie a quarter input pixel off the input centers; at the
        // edges, the missing neighbour outside the input is left out
        let along_axis = |idx: isize| -> f64 {
            // the output pixels are half as large as the input pixels
            let center = 0.25 + idx as f64 * 0.5;
            (center - 0.5).clamp(0., 5.)
        };
        let pixels = pixels(&tiles);
        assert_eq!(
            pixels.iter().filter(|(_, value)| value.is_some()).count(),
            144
        );
        for ([y, x], value) in pixels {
            let Some(value) = value else { continue };
            // the output rows -12..=-1 cover the input rows 0..6 from the top
            let expected = 10. * along_axis(y + 12) + along_axis(x);
            assert!(
                (f64::from(value) - expected).abs() < 1e-5,
                "pixel {y}, {x}: {value} != {expected}"
            );
        }
    }

    #[tokio::test]
    async fn it_keeps_the_fractions_of_an_integer_average_in_a_float_output() {
        let exe_ctx = mock_exe_ctx();
        // `F32` cannot hold every `I32`, so the kernel must not compute in `I32` and round there
        let source = input().mock_raster_source::<i32>().typed();

        let (descriptor, tiles) = query_typed(
            warp(
                source,
                WarpParams {
                    output_data_type: Some(RasterDataType::F32),
                    ..downsampling_params(2., WarpDownsampling::Average)
                },
            ),
            &exe_ctx,
            TimeInterval::new_unchecked(0, 10),
            None,
            TypedRasterQueryProcessor::get_f32,
        )
        .await;

        assert_eq!(descriptor.data_type, RasterDataType::F32);
        // the mean of 10 × row + column over 2 × 2 pixels, e.g. (0 + 1 + 10 + 11) / 4 = 5.5
        assert_raster_tiles_eq_ascii(
            &tiles,
            &two_degrees(indoc! {"
                [0, 10)
                +----------------+
                |  5.5  7.5  9.5 |
                | 25.5 27.5 29.5 |
                | 45.5 47.5 49.5 |
                +----------------+
            "}),
        );
    }

    #[tokio::test]
    async fn it_treats_nan_like_no_data_in_every_method() {
        let exe_ctx = mock_exe_ctx();
        let input = |nan: &str| {
            AsciiRasterTiles::new(
                &indoc! {"
                    [0, 10)
                    +-------------+-------------+
                    |  0  1  2    |  3  4  5    |
                    | 10  X 12    | 13 14 15    |
                    | 20 21 22    | 23  X 25    |
                    +-------------+-------------+
                    | 30 31 32    | 33 34 35    |
                    |  X 41 42    | 43 44 45    |
                    | 50 51 52    | 53 54  X    |
                    +-------------+-------------+
                "}
                .replace('X', nan),
            )
            .tile_origin([-2, 0])
            .mock_raster_source::<f32>()
            .typed()
        };
        let half_degree = SpatialResolution::new_unchecked(0.5, 0.5);
        let cases = [
            downsampling_params(2., WarpDownsampling::Average),
            downsampling_params(2., WarpDownsampling::Min),
            downsampling_params(2., WarpDownsampling::Max),
            downsampling_params(1.5, WarpDownsampling::Average),
            WarpParams {
                upsampling: WarpUpsampling::Bilinear,
                output_origin_reference: Some(Coordinate2D::new(0.25, 0.25)),
                ..resample_params(half_degree)
            },
            WarpParams {
                upsampling: WarpUpsampling::Bilinear,
                ..resample_params(half_degree)
            },
        ];

        for params in cases {
            let query = |source| async {
                let (_, tiles) = query_typed(
                    warp(source, params),
                    &exe_ctx,
                    TimeInterval::new_unchecked(0, 10),
                    None,
                    TypedRasterQueryProcessor::get_f32,
                )
                .await;
                pixels(&tiles)
            };
            let with_nan = query(input("NaN")).await;
            let with_no_data = query(input("_")).await;

            assert!(
                with_no_data.iter().any(|(_, value)| value.is_some()),
                "{params:?} should have data"
            );
            assert_eq!(with_nan, with_no_data, "{params:?}");
        }
    }

    #[tokio::test]
    async fn it_converts_nearest_neighbor_results_to_the_output_data_type() {
        let exe_ctx = mock_exe_ctx();
        let source = input().mock_raster_source::<u8>().typed();

        let (descriptor, tiles) = query_typed(
            warp(
                source,
                WarpParams {
                    output_data_type: Some(RasterDataType::F32),
                    ..resample_params(SpatialResolution::new_unchecked(2., 2.))
                },
            ),
            &exe_ctx,
            TimeInterval::new_unchecked(0, 10),
            None,
            TypedRasterQueryProcessor::get_f32,
        )
        .await;

        assert_eq!(descriptor.data_type, RasterDataType::F32);
        assert_raster_tiles_eq_ascii(
            &tiles,
            &two_degrees(indoc! {"
                [0, 10)
                +----------+
                | 11 13 15 |
                | 31 33 35 |
                | 51 53 55 |
                +----------+
            "}),
        );
    }

    #[tokio::test]
    async fn it_rejects_mode_for_float_inputs() {
        let exe_ctx = mock_exe_ctx();
        let source = input().mock_raster_source::<f32>().typed();

        let result = warp(source, downsampling_params(2., WarpDownsampling::Mode))
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
            .await;

        assert!(matches!(
            result,
            Err(Error::WarpModeRequiresIntegerInput {
                data_type: RasterDataType::F32
            })
        ));
    }

    #[tokio::test]
    async fn it_rejects_mode_beyond_the_memory_budget() {
        // at 16×, a footprint has 17 × 17 pixels: up to 289 values for `U16`, but only a histogram of
        // 256 values for `U8`, so that 9 open pixels need ≈ 54 KB and ≈ 18 KB
        let mut exe_ctx = mock_exe_ctx();
        exe_ctx.warp_mode_max_memory_per_tile = 32 * 1024;
        let initialize = |source: TypedRasterOperator| {
            warp(source, downsampling_params(16., WarpDownsampling::Mode))
                .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
        };

        let result = initialize(input().mock_raster_source::<u16>().typed()).await;
        assert!(
            matches!(
                result,
                Err(Error::WarpModeExceedsMemoryBudget {
                    footprint_pixels: 289,
                    data_type: RasterDataType::U16,
                    ..
                })
            ),
            "{:?}",
            result.err()
        );

        assert!(
            initialize(input().mock_raster_source::<u8>().typed())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn it_computes_the_mode_of_wider_integer_types() {
        let exe_ctx = mock_exe_ctx();
        let source = classes().mock_raster_source::<u16>().typed();

        let (_, tiles) = query_typed(
            warp(source, downsampling_params(2., WarpDownsampling::Mode)),
            &exe_ctx,
            TimeInterval::new_unchecked(0, 10),
            None,
            TypedRasterQueryProcessor::get_u16,
        )
        .await;

        // the same as for `U8`, see `it_aggregates_coarser_pixels_with_the_downsampling_method`
        assert_raster_tiles_eq_ascii(
            &tiles,
            &two_degrees(indoc! {"
                [0, 10)
                +-------+
                | 1 2 4 |
                | 5 6 _ |
                | 7 7 9 |
                +-------+
            "}),
        );
    }

    #[test]
    fn it_serializes_methods_with_gdal_names_and_leaves_out_defaults() {
        let params = WarpParams {
            downsampling: WarpDownsampling::Average,
            upsampling: WarpUpsampling::Bilinear,
            output_data_type: Some(RasterDataType::F32),
            ..WarpParams::default()
        };

        assert_eq!(
            serde_json::to_value(params).unwrap(),
            serde_json::json!({
                "downsampling": "average",
                "upsampling": "bilinear",
                "outputDataType": "F32",
            })
        );
        assert_eq!(
            serde_json::to_value(WarpParams::default()).unwrap(),
            serde_json::json!({})
        );
    }

    /// Reads the values of `test_data/raster/warp/expected/<name>.tif` row by row, `None` for no-data,
    /// and its geo transform and size.
    #[allow(clippy::float_cmp, reason = "GDAL writes the no-data value exactly")]
    fn read_expected(name: &str) -> (Vec<Option<f64>>, GeoTransform, (usize, usize)) {
        let path = test_data!(format!("raster/warp/expected/{name}.tif")).to_path_buf();
        let dataset = gdal_open_dataset(&path).unwrap();
        let geo_transform = GeoTransform::try_from(dataset.geo_transform().unwrap()).unwrap();
        let band = dataset.rasterband(1).unwrap();
        let no_data = band.no_data_value().unwrap();
        let size = dataset.raster_size();
        let (_, values) = band
            .read_as::<f64>((0, 0), size, size, None)
            .unwrap()
            .into_shape_and_vec();
        let values = values
            .into_iter()
            .map(|value| (value != no_data).then_some(value))
            .collect();
        (values, geo_transform, size)
    }

    /// An execution context with tiles of 16 × 16 pixels and `test_data/raster/warp/<input>.tif`
    /// as a dataset, and the name of the dataset.
    fn warp_input(input: &str) -> (MockExecutionContext, NamedData) {
        let input_path = test_data!(format!("raster/warp/{input}.tif")).to_path_buf();
        let input_dataset = gdal_open_dataset(&input_path).unwrap();
        let mut exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            GridShape2D::new_2d(16, 16),
        ));
        let name = NamedData::with_system_name(input);
        exe_ctx.add_meta_data(
            DatasetId::new().into(),
            name.clone(),
            Box::new(GdalMetaDataStatic {
                time: Some(TimeInterval::default()),
                params: gdal_parameters_from_dataset(&input_dataset, 1, &input_path, None, None)
                    .unwrap(),
                result_descriptor: raster_descriptor_from_dataset(&input_dataset, 1).unwrap(),
                cache_ttl: None,
            }),
        );
        (exe_ctx, name)
    }

    /// Warps `test_data/raster/warp/<input>.tif` onto the grid of `expected` with `params` and
    /// compares the result with GDAL's in `expected/<expected>.tif` (see the README there): the
    /// same no-data pixels and values that differ by at most `tolerance`. Where GDAL's float result
    /// in `halfway_reference` lies halfway between two integers, both are accepted.
    async fn assert_like_gdal<T: Pixel>(
        input: &str,
        expected: &str,
        params: WarpParams,
        tolerance: f64,
        halfway_reference: Option<&str>,
        get: fn(TypedRasterQueryProcessor) -> Option<BoxRasterQueryProcessor<T>>,
    ) {
        let (exe_ctx, name) = warp_input(input);

        let (expected_values, expected_geo_transform, (width, height)) = read_expected(expected);
        assert!(
            expected_values.iter().any(Option::is_some),
            "{expected}: GDAL's result has no valid pixels to compare with"
        );
        let halfway_values = halfway_reference.map(|name| read_expected(name).0);

        let operator = warp(
            GdalSource {
                params: GdalSourceParameters::new(name),
            }
            .typed(),
            WarpParams {
                output_resolution: Some(SpatialResolution::new_unchecked(
                    expected_geo_transform.x_pixel_size().get(),
                    -expected_geo_transform.y_pixel_size().get(),
                )),
                output_origin_reference: Some(expected_geo_transform.origin_coordinate()),
                output_data_type: Some(T::TYPE),
                ..params
            },
        );
        let initialized = operator
            .clone()
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();
        // the pixels of the output grid that `expected` covers, in the pixel space of the tiles
        let out_geo_transform = initialized
            .result_descriptor()
            .spatial_grid
            .tiling_grid_definition(exe_ctx.tiling_specification)
            .tiling_geo_transform();
        let [min_y, min_x] = out_geo_transform
            .coordinate_to_grid_idx_2d(
                expected_geo_transform.grid_idx_to_pixel_center_coordinate_2d([0, 0].into()),
            )
            .0;
        let bounds = GridBoundingBox2D::new_unchecked(
            [min_y, min_x],
            [min_y + height as isize - 1, min_x + width as isize - 1],
        );

        let (_, tiles) = query_typed(
            operator,
            &exe_ctx,
            TimeInterval::new_instant(0).unwrap(),
            Some(bounds),
            get,
        )
        .await;

        let mut compared = 0;
        let mut mismatches = Vec::new();
        for ([y, x], actual) in pixels(&tiles) {
            if !bounds.contains(&GridIdx2D::from([y, x])) {
                continue;
            }
            let idx = usize::try_from((y - min_y) * width as isize + (x - min_x)).unwrap();
            let expected_value = expected_values[idx];
            let halfway = halfway_values
                .as_ref()
                .and_then(|values| values[idx])
                .is_some_and(|value| ((value - value.floor()) - 0.5).abs() < 1e-4);
            let actual: Option<f64> = actual.map(AsPrimitive::as_);
            compared += 1;
            let equal = match (actual, expected_value) {
                (Some(actual), Some(expected)) => {
                    (actual - expected).abs() <= tolerance
                        || (halfway && (actual - expected).abs() <= 1.)
                }
                (None, None) => true,
                _ => false,
            };
            if !equal {
                mismatches.push(([y - min_y, x - min_x], actual, expected_value));
            }
        }

        assert_eq!(
            compared,
            width * height,
            "{expected}: not all pixels were compared"
        );
        assert!(
            mismatches.is_empty(),
            "{expected}: {} of {compared} pixels differ from GDAL, e.g. (row, column, actual, expected) {:?}",
            mismatches.len(),
            &mismatches[..mismatches.len().min(10)]
        );
    }

    #[tokio::test]
    async fn it_aggregates_like_gdal() {
        let cases = [
            ("ndvi", "3x", WarpDownsampling::Average),
            ("ndvi", "2x_shifted", WarpDownsampling::Average),
            ("ndvi", "mixed", WarpDownsampling::Average),
            ("ndvi", "2_5x", WarpDownsampling::Min),
            ("ndvi", "3x", WarpDownsampling::Max),
            ("landcover", "3x", WarpDownsampling::Mode),
            ("landcover", "2_5x", WarpDownsampling::Mode),
            ("landcover", "2x_shifted", WarpDownsampling::Mode),
            ("landcover", "mixed", WarpDownsampling::Mode),
        ];
        for (input, case, downsampling) in cases {
            let method = serde_json::to_value(downsampling).unwrap();
            let method = method.as_str().unwrap();
            let halfway_reference = format!("{input}_{method}_{case}_Float32");
            assert_like_gdal(
                input,
                &format!("{input}_{method}_{case}_Byte"),
                WarpParams {
                    downsampling,
                    ..WarpParams::default()
                },
                0.,
                (downsampling == WarpDownsampling::Average).then_some(halfway_reference.as_str()),
                TypedRasterQueryProcessor::get_u8,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn it_averages_into_float_like_gdal() {
        for case in ["3x", "2_5x", "2x_shifted", "mixed"] {
            assert_like_gdal(
                "ndvi",
                &format!("ndvi_average_{case}_Float32"),
                WarpParams {
                    downsampling: WarpDownsampling::Average,
                    ..WarpParams::default()
                },
                1e-3,
                None,
                TypedRasterQueryProcessor::get_f32,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn it_interpolates_bilinearly_like_gdal() {
        let params = WarpParams {
            upsampling: WarpUpsampling::Bilinear,
            ..WarpParams::default()
        };
        assert_like_gdal(
            "ndvi",
            "ndvi_bilinear_2x_finer_Byte",
            params,
            0.,
            Some("ndvi_bilinear_2x_finer_Float32"),
            TypedRasterQueryProcessor::get_u8,
        )
        .await;
        for case in ["2x_finer", "2_5x_finer_shifted"] {
            assert_like_gdal(
                "ndvi",
                &format!("ndvi_bilinear_{case}_Float32"),
                params,
                1e-3,
                None,
                TypedRasterQueryProcessor::get_f32,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn it_aggregates_with_reprojection_like_gdal() {
        let cases = [
            ("ndvi", WarpDownsampling::Average),
            ("ndvi", WarpDownsampling::Max),
            ("landcover", WarpDownsampling::Mode),
        ];
        for (input, downsampling) in cases {
            let method = serde_json::to_value(downsampling).unwrap();
            let method = method.as_str().unwrap();
            let halfway_reference = format!("{input}_{method}_3857_30km_Float32");
            assert_like_gdal(
                input,
                &format!("{input}_{method}_3857_30km_Byte"),
                WarpParams {
                    target_spatial_reference: Some(SpatialReference::web_mercator()),
                    downsampling,
                    ..WarpParams::default()
                },
                0.,
                (downsampling == WarpDownsampling::Average).then_some(halfway_reference.as_str()),
                TypedRasterQueryProcessor::get_u8,
            )
            .await;
        }
        assert_like_gdal(
            "ndvi",
            "ndvi_average_3857_30km_Float32",
            WarpParams {
                target_spatial_reference: Some(SpatialReference::web_mercator()),
                downsampling: WarpDownsampling::Average,
                ..WarpParams::default()
            },
            1e-3,
            None,
            TypedRasterQueryProcessor::get_f32,
        )
        .await;
    }

    #[tokio::test]
    async fn it_interpolates_bilinearly_with_reprojection_like_gdal() {
        let params = WarpParams {
            target_spatial_reference: Some(SpatialReference::web_mercator()),
            upsampling: WarpUpsampling::Bilinear,
            ..WarpParams::default()
        };
        // on land and on the coast, where water is no-data
        for case in ["3857_2km", "3857_2km_coast"] {
            assert_like_gdal(
                "ndvi",
                &format!("ndvi_bilinear_{case}_Byte"),
                params,
                0.,
                Some(&format!("ndvi_bilinear_{case}_Float32")),
                TypedRasterQueryProcessor::get_u8,
            )
            .await;
            assert_like_gdal(
                "ndvi",
                &format!("ndvi_bilinear_{case}_Float32"),
                params,
                1e-3,
                None,
                TypedRasterQueryProcessor::get_f32,
            )
            .await;
        }
    }

    /// `Warp` approximates the coordinate transformation as GDAL's default `--et 0.125`, with its
    /// own subdivision of the rows, so it is close to GDAL's approximate results as well
    #[tokio::test]
    async fn it_approximates_the_reprojection_like_gdal() {
        let reprojected = WarpParams {
            target_spatial_reference: Some(SpatialReference::web_mercator()),
            ..WarpParams::default()
        };
        assert_like_gdal(
            "ndvi",
            "ndvi_average_3857_30km_Float32_et0125",
            WarpParams {
                downsampling: WarpDownsampling::Average,
                ..reprojected
            },
            1e-3,
            None,
            TypedRasterQueryProcessor::get_f32,
        )
        .await;
        assert_like_gdal(
            "ndvi",
            "ndvi_bilinear_3857_2km_coast_Float32_et0125",
            WarpParams {
                upsampling: WarpUpsampling::Bilinear,
                ..reprojected
            },
            1e-3,
            None,
            TypedRasterQueryProcessor::get_f32,
        )
        .await;
    }
}
