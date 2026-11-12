use crate::engine::{
    ExecutionContext, InitializedRasterOperator, ProcessingGraphOperatorPath, RasterOperator,
    RasterResultDescriptor, ResultDescriptor, SingleRasterOrVectorSource, TypedRasterOperator,
};
use crate::error::{self, Optimization};
use crate::processing::{
    DeriveOutRasterSpecsSource, Downsampling, DownsamplingMethod, DownsamplingParams,
    DownsamplingResolution, Interpolation, InterpolationMethod, InterpolationParams,
    InterpolationResolution, Reprojection, ReprojectionParams,
};
use crate::util::Result;
use crate::util::input::RasterOrVectorOperator;
use geoengine_datatypes::primitives::{
    Coordinate2D, SpatialResolution, find_next_best_overview_level_resolution,
};
use geoengine_datatypes::spatial_reference::SpatialReference;
use snafu::ResultExt;

pub struct WrapWithProjectionAndResample {
    pub operator: TypedRasterOperator,
    pub initialized_operator: Box<dyn InitializedRasterOperator>,
    pub result_descriptor: RasterResultDescriptor,
}

impl WrapWithProjectionAndResample {
    pub fn new_create_result_descriptor(
        operator: TypedRasterOperator,
        initialized: Box<dyn InitializedRasterOperator>,
    ) -> Self {
        let result_descriptor = initialized.result_descriptor().clone();
        Self::new(operator, initialized, result_descriptor)
    }

    pub fn new(
        operator: TypedRasterOperator,
        initialized: Box<dyn InitializedRasterOperator>,
        result_descriptor: RasterResultDescriptor,
    ) -> Self {
        Self {
            operator,
            initialized_operator: initialized,
            result_descriptor,
        }
    }

    pub async fn wrap_with_projection(
        self,
        target_sref: SpatialReference,
        _target_origin_reference: Option<Coordinate2D>, // TODO: add resampling if origin does not match! Could also do that in projection and avoid extra operation?
        exe_ctx: &dyn ExecutionContext,
    ) -> Result<Self> {
        let result_sref = self
            .result_descriptor
            .spatial_reference()
            .as_option()
            .ok_or(error::Error::SpatialReferenceMustNotBeUnreferenced)?;

        // perform reprojection if necessary
        let res = if target_sref == result_sref {
            self
        } else {
            tracing::debug!(
                "Target srs: {target_sref}, processing graph srs: {result_sref} --> injecting reprojection"
            );

            let reprojection_params = ReprojectionParams {
                target_spatial_reference: target_sref,
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            };

            // create the reprojection operator in order to get the canonic operator name
            let reprojected_processing_graph = Reprojection {
                params: reprojection_params,
                sources: SingleRasterOrVectorSource {
                    source: RasterOrVectorOperator::Raster(self.operator),
                },
            }
            .typed();

            let irp = reprojected_processing_graph
                .clone()
                .initialize(ProcessingGraphOperatorPath::initialize_root(), exe_ctx)
                .await?;

            let rd = irp.result_descriptor().clone();
            Self::new(reprojected_processing_graph, irp.boxed(), rd)
        };
        Ok(res)
    }

    pub async fn wrap_with_resample(
        self,
        target_origin_reference: Option<Coordinate2D>,
        target_spatial_resolution: Option<SpatialResolution>,
        exe_ctx: &dyn ExecutionContext,
    ) -> Result<Self> {
        if target_origin_reference.is_none() && target_spatial_resolution.is_none() {
            return Ok(self);
        }

        let rd_resolution = self
            .result_descriptor
            .spatial_grid_descriptor()
            .spatial_resolution();
        let target_spatial_grid = if let Some(tsr) = target_spatial_resolution {
            self.result_descriptor
                .spatial_grid_descriptor()
                .with_changed_resolution(tsr)
        } else {
            *self.result_descriptor.spatial_grid_descriptor()
        };
        let target_spatial_grid = if let Some(tor) = target_origin_reference {
            // if the request is to move the origin of the query to a different point, we generate a new grid aligned to that point.
            target_spatial_grid
                .with_moved_origin_to_nearest_grid_edge(tor)
                .as_derived()
                .replace_origin(tor)
        } else {
            target_spatial_grid
        };

        if self
            .result_descriptor
            .spatial_grid_descriptor()
            .is_compatible_grid(&target_spatial_grid)
        {
            // TODO: resample if origin is not allgned to query? (maybe n
            return Ok(self);
        }

        let target_resolution = target_spatial_grid.spatial_resolution();

        // Query resolution is smaller than processing graph => compute on full resolution and append interpolation to decrease resolution
        //TODO: we should allow to use the "interpolation" as long as the fraction is > 0.5. This would require to keep 4 tiles which seems to be fine. The edge case of resampling with same resolution should also use the interpolation since bilieaner woudl make sense here?
        if target_resolution <= rd_resolution {
            self.interpolation(
                target_spatial_resolution,
                exe_ctx,
                rd_resolution,
                target_spatial_grid,
            )
            .await
        } else if target_resolution >= rd_resolution {
            // Query resolution is larger than processing graph => compute on overview level and append downsampling to increase resolution
            self.downsampling(
                target_spatial_resolution,
                exe_ctx,
                rd_resolution,
                target_spatial_grid,
            )
            .await
        } else {
            // Query resolution is larger in one axis and smaller in the other one => downsample the coarser axis and interpolate the finer one
            let downsampled_resolution = SpatialResolution::new_unchecked(
                target_resolution.x().get().max(rd_resolution.x().get()),
                target_resolution.y().get().max(rd_resolution.y().get()),
            );
            let downsampled_spatial_grid = self
                .result_descriptor
                .spatial_grid_descriptor()
                .with_changed_resolution(downsampled_resolution);

            self.downsampling(
                Some(downsampled_resolution),
                exe_ctx,
                rd_resolution,
                downsampled_spatial_grid,
            )
            .await?
            .interpolation(
                target_spatial_resolution,
                exe_ctx,
                downsampled_resolution,
                target_spatial_grid,
            )
            .await
        }
    }

    async fn downsampling(
        self,
        target_spatial_resolution: Option<SpatialResolution>,
        exe_ctx: &dyn ExecutionContext,
        rd_resolution: SpatialResolution,
        target_spatial_grid: crate::engine::SpatialGridDescriptor,
    ) -> std::result::Result<WrapWithProjectionAndResample, error::Error> {
        tracing::debug!(
            "Query res: {target_spatial_resolution:?}, processing graph res: {rd_resolution:?} --> optimize processing graph and push-down downsampling"
        );

        let snapped_resolution = find_next_best_overview_level_resolution(
            self.result_descriptor.spatial_grid.spatial_resolution(),
            target_spatial_grid.spatial_resolution(),
        );
        debug_assert!(snapped_resolution <= target_spatial_grid.spatial_resolution());

        let optimized_operator = self
            .initialized_operator
            .optimize(snapped_resolution)
            .context(Optimization)?;

        if snapped_resolution == target_spatial_grid.spatial_resolution() {
            // target resolution is an overview level, so we can use the optimized operator directly
            let initialized_raster_operator = optimized_operator
                .clone()
                .initialize(ProcessingGraphOperatorPath::initialize_root(), exe_ctx)
                .await?;

            let rd: RasterResultDescriptor =
                initialized_raster_operator.result_descriptor().clone();
            return Ok(Self::new(
                optimized_operator,
                initialized_raster_operator,
                rd,
            ));
        }

        // target resolution is not an overview level, so we need to downsample the optimized operator
        let downsample_params = DownsamplingParams {
            sampling_method: DownsamplingMethod::NearestNeighbor,
            output_resolution: DownsamplingResolution::Resolution(
                target_spatial_grid.spatial_resolution(),
            ),
            output_origin_reference: None,
        };
        let dop = Downsampling {
            params: downsample_params,
            sources: optimized_operator.into(),
        }
        .typed();

        let ido = dop
            .clone()
            .initialize(ProcessingGraphOperatorPath::initialize_root(), exe_ctx)
            .await?;

        let rd = ido.result_descriptor().clone();
        Ok(Self::new(dop, ido.boxed(), rd))
    }

    async fn interpolation(
        self,
        target_spatial_resolution: Option<SpatialResolution>,
        exe_ctx: &dyn ExecutionContext,
        rd_resolution: SpatialResolution,
        target_spatial_grid: crate::engine::SpatialGridDescriptor,
    ) -> std::result::Result<WrapWithProjectionAndResample, error::Error> {
        tracing::debug!(
            "Target res: {target_spatial_resolution:?}, processing graph res: {rd_resolution:?} --> injecting interpolation"
        );

        let interpolation_params = InterpolationParams {
            interpolation: InterpolationMethod::NearestNeighbor, // TODO: choose appropriate interpolation method
            output_resolution: InterpolationResolution::Resolution(
                target_spatial_grid.spatial_resolution(),
            ),
            output_origin_reference: None,
        };

        let iop = Interpolation {
            params: interpolation_params.clone(),
            sources: self.operator.into(),
        }
        .typed();

        let iip = iop
            .clone()
            .initialize(ProcessingGraphOperatorPath::initialize_root(), exe_ctx)
            .await?;

        let rd = iip.result_descriptor().clone();

        Ok(Self::new(iop, iip.boxed(), rd))
    }

    pub async fn wrap_with_projection_and_resample(
        self,
        target_origin_reference: Option<Coordinate2D>,
        target_spatial_resolution: Option<SpatialResolution>,
        target_sref: SpatialReference,
        exe_ctx: &dyn ExecutionContext,
    ) -> Result<Self> {
        self.wrap_with_projection(target_sref, target_origin_reference, exe_ctx)
            .await?
            .wrap_with_resample(target_origin_reference, target_spatial_resolution, exe_ctx)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        MockExecutionContext, RasterBandDescriptors, SpatialGridDescriptor, TimeDescriptor,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use geoengine_datatypes::primitives::TimeInterval;
    use geoengine_datatypes::raster::{GeoTransform, GridBoundingBox2D, RasterDataType, TileSize};
    use geoengine_datatypes::util::test::TestDefault;

    async fn wrap_with_resample(target_resolution: SpatialResolution) -> TypedRasterOperator {
        let exe_ctx = MockExecutionContext::test_default();
        let source = MockRasterSource::<u8> {
            params: MockRasterSourceParams {
                data: vec![],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: TimeDescriptor::new_irregular(Some(TimeInterval::default())),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.).unwrap(),
                        GridBoundingBox2D::new_min_max(0, 7, 0, 7).unwrap(),
                        TileSize::default_512(),
                    ),
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .typed();
        let initialized = source
            .clone()
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let wrapped =
            WrapWithProjectionAndResample::new_create_result_descriptor(source, initialized)
                .wrap_with_resample(None, Some(target_resolution), &exe_ctx)
                .await
                .unwrap();

        assert_eq!(
            wrapped
                .result_descriptor
                .spatial_grid_descriptor()
                .spatial_resolution(),
            target_resolution
        );

        wrapped.operator
    }

    #[tokio::test]
    async fn it_resamples_to_a_resolution_that_is_coarser_in_one_axis_and_finer_in_the_other() {
        let operator = wrap_with_resample(SpatialResolution::new_unchecked(2., 0.5)).await;

        let TypedRasterOperator::Interpolation(interpolation) = operator else {
            panic!("the finer axis should be interpolated");
        };
        assert!(
            matches!(
                interpolation.sources.raster,
                TypedRasterOperator::Downsampling(_)
            ),
            "the coarser axis should be downsampled"
        );
    }

    #[tokio::test]
    async fn it_resamples_to_a_resolution_that_is_coarser_in_one_axis() {
        let operator = wrap_with_resample(SpatialResolution::new_unchecked(3., 1.)).await;

        assert!(matches!(operator, TypedRasterOperator::Downsampling(_)));
    }
}
