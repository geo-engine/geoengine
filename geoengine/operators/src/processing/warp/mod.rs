//! The `Warp` operator puts a raster onto another grid in one step: another spatial reference,
//! another resolution (per axis, coarser or finer) and another origin, in any combination.
//!
//! For each output pixel, it looks up the input pixel under the output pixel's center, the same
//! nearest-neighbour lookup as `Reprojection`, `Downsampling` and `Interpolation`
//! (`NearestNeighbor`). Unlike a chain of these, it needs a single sub-query per output tile.
//! In the same spatial reference, it maps pixel indices directly, with the sub-query of
//! `Interpolation` for finer outputs and the one of `Downsampling` otherwise; with another spatial
//! reference, it uses the sub-query of `Reprojection`.
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
use geoengine_datatypes::raster::{NearestNeighbor, TilingSpecification};
use geoengine_datatypes::spatial_reference::SpatialReference;
use serde::{Deserialize, Serialize};

/// The parameters of [`Warp`]: the grid to put the raster onto. Without any of them, the output
/// is the input.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
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
    /// How to compute the output pixels, nearest neighbour by default
    #[serde(default, skip_serializing_if = "WarpMethod::is_default")]
    pub method: WarpMethod,
}

/// How [`Warp`] computes the value of an output pixel from the input.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum WarpMethod {
    /// the value of the input pixel under the output pixel's center
    #[default]
    NearestNeighbor,
}

impl WarpMethod {
    /// Whether this is the default method, which serialized operators leave out
    #[allow(clippy::trivially_copy_pass_by_ref)] // must adhere to serde's signature
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
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
        InitializedWarp::new(
            name,
            path,
            initialized_source.raster,
            self.params,
            context.tiling_specification(),
        )
        .map(InitializedRasterOperator::boxed)
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
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
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
    /// project the coordinates even within the same spatial reference, as `Reprojection` does
    always_reproject: bool,
    tiling_specification: TilingSpecification,
}

impl<O: InitializedRasterOperator> InitializedWarp<O> {
    /// Computes the output grid of `source` with `params` (see [`warped_spatial_grid`]).
    ///
    /// # Errors
    /// If the source has no spatial reference, or does not intersect the area of use of the
    /// target spatial reference.
    pub fn new(
        name: CanonicOperatorName,
        path: ProcessingGraphOperatorPath,
        source: O,
        params: WarpParams,
        tiling_specification: TilingSpecification,
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
            tiling_specification,
        )
    }

    /// A `Warp` of `source` onto `out_grid`, which the caller computed from `params`, e.g. with the
    /// rules of the operator in `origin` that is mapped to a `Warp`. The target spatial reference is
    /// the one of `params`, or the source's.
    ///
    /// # Errors
    /// If the source has no spatial reference.
    pub fn with_output_grid(
        origin: WarpOrigin,
        source: O,
        params: WarpParams,
        out_grid: SpatialGridDescriptor,
        tiling_specification: TilingSpecification,
    ) -> Result<Self> {
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

        let result_descriptor = RasterResultDescriptor {
            spatial_reference: target_srs.into(),
            data_type: in_descriptor.data_type,
            time: in_descriptor.time,
            spatial_grid: out_grid,
            bands: in_descriptor.bands.clone(),
        };

        Ok(Self {
            name,
            path,
            operator_name,
            result_descriptor,
            params,
            source,
            state,
            source_srs,
            target_srs,
            always_reproject,
            tiling_specification,
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

        let reproject = self.always_reproject || self.source_srs != self.target_srs;

        // the same grid: nothing to warp
        if !reproject
            && self.source.result_descriptor().spatial_grid.as_parts().1
                == self.result_descriptor.spatial_grid.as_parts().1
        {
            return Ok(source);
        }

        let in_resolution = self
            .source
            .result_descriptor()
            .spatial_grid
            .spatial_resolution();
        let out_resolution = self.result_descriptor.spatial_grid.spatial_resolution();
        // finer or equal in both axes, so the input of an output tile is at most one tile
        let finer =
            out_resolution.x() <= in_resolution.x() && out_resolution.y() <= in_resolution.y();
        Ok(call_on_generic_raster_processor!(
            source, p => match self.params.method {
                // the same spatial reference: look up the input pixel by its index, without
                // projecting the coordinates of all output pixels; `Interpolation` collects the
                // input of an output tile and resamples it in one pass, which is faster but keeps
                // the whole input of the tile, so only for finer outputs
                WarpMethod::NearestNeighbor if !reproject && finer => {
                    InterploationProcessor::<_, _, NearestNeighbor>::new(
                        p,
                        self.result_descriptor.clone(),
                        self.tiling_specification,
                    ).boxed().into()
                }
                WarpMethod::NearestNeighbor if !reproject => DownsampleProcessor::new(
                    p,
                    self.result_descriptor.clone(),
                    self.tiling_specification,
                ).boxed().into(),
                WarpMethod::NearestNeighbor => RasterReprojectionProcessor::new(
                    p,
                    self.result_descriptor.clone(),
                    self.source_srs,
                    self.target_srs,
                    self.state,
                ).boxed().into(),
            }
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
    use crate::engine::{MockExecutionContext, SingleRasterOrVectorSource};
    use crate::processing::{
        Downsampling, DownsamplingMethod, DownsamplingParams, DownsamplingResolution,
        Interpolation, InterpolationMethod, InterpolationParams, InterpolationResolution,
        Reprojection,
    };
    use crate::source::{GdalSource, GdalSourceParameters};
    use crate::util::gdal::add_ndvi_dataset;
    use crate::util::test::AsciiRasterTilesExt;
    use futures::StreamExt;
    use geoengine_datatypes::primitives::{BandSelection, RasterQueryRectangle, TimeInterval};
    use geoengine_datatypes::raster::{GeoTransform, GridBoundingBox2D, GridShape2D, RasterTile2D};
    use geoengine_datatypes::util::test::{
        AsciiRasterTiles, TestDefault, assert_eq_two_list_of_tiles, assert_raster_tiles_eq_ascii,
    };
    use indoc::indoc;

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
            method: WarpMethod::NearestNeighbor,
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
        let initialized = operator
            .initialize(ProcessingGraphOperatorPath::initialize_root(), exe_ctx)
            .await
            .unwrap();
        let descriptor = initialized.result_descriptor().clone();
        let processor = initialized.query_processor().unwrap().get_u8().unwrap();
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
                    method: WarpMethod::NearestNeighbor,
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
                    method: WarpMethod::NearestNeighbor,
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
        assert_eq!(gdal_source.params.overview_level, Some(2));

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
        assert_eq!(gdal_source.params.overview_level, Some(2));
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
                method: WarpMethod::NearestNeighbor,
            }
        );
    }
}
