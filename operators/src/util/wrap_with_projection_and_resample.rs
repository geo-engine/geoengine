use crate::engine::{
    CanonicOperatorName, InitializedRasterOperator, RasterOperator, RasterResultDescriptor,
    ResultDescriptor, SingleRasterOrVectorSource, WorkflowOperatorPath,
};
use crate::error;
use crate::processing::{
    DeriveOutRasterSpecsSource, Downsampling, DownsamplingMethod, DownsamplingParams,
    DownsamplingResolution, InitializedDownsampling, InitializedInterpolation,
    InitializedRasterReprojection, Interpolation, InterpolationMethod, InterpolationParams,
    InterpolationResolution, Reprojection, ReprojectionParams,
};
use crate::util::Result;
use crate::util::input::RasterOrVectorOperator;
use geoengine_datatypes::primitives::{Coordinate2D, SpatialResolution};
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_datatypes::spatial_reference::SpatialReference;

pub struct WrapWithProjectionAndResample {
    pub operator: Box<dyn RasterOperator>,
    pub initialized_operator: Box<dyn InitializedRasterOperator>,
    pub result_descriptor: RasterResultDescriptor,
}

impl WrapWithProjectionAndResample {
    pub fn new_create_result_descriptor(
        operator: Box<dyn RasterOperator>,
        initialized: Box<dyn InitializedRasterOperator>,
    ) -> Self {
        let result_descriptor = initialized.result_descriptor().clone();
        Self::new(operator, initialized, result_descriptor)
    }

    pub fn new(
        operator: Box<dyn RasterOperator>,
        initialized: Box<dyn InitializedRasterOperator>,
        result_descriptor: RasterResultDescriptor,
    ) -> Self {
        Self {
            operator,
            initialized_operator: initialized,
            result_descriptor,
        }
    }

    pub fn wrap_with_projection(
        self,
        target_sref: SpatialReference,
        _target_origin_reference: Option<Coordinate2D>, // TODO: add resampling if origin does not match! Could also do that in projection and avoid extra operation?
        tiling_spec: TilingSpecification,
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
            log::debug!(
                "Target srs: {target_sref}, workflow srs: {result_sref} --> injecting reprojection"
            );

            let reprojection_params = ReprojectionParams {
                target_spatial_reference: target_sref,
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            };

            // create the reprojection operator in order to get the canonic operator name
            let reprojected_workflow = Reprojection {
                params: reprojection_params,
                sources: SingleRasterOrVectorSource {
                    source: RasterOrVectorOperator::Raster(self.operator),
                },
            };

            // create the inititalized operator directly, to avoid re-initializing everything
            let irp = InitializedRasterReprojection::try_new_with_input(
                CanonicOperatorName::from(&reprojected_workflow),
                WorkflowOperatorPath::initialize_root(), // FIXME: this is not correct since the root is the child operator
                reprojection_params,
                self.initialized_operator,
                tiling_spec,
            )?;
            let rd = irp.result_descriptor().clone();

            Self::new(reprojected_workflow.boxed(), irp.boxed(), rd)
        };
        Ok(res)
    }

    pub fn wrap_with_resample(
        self,
        target_origin_reference: Option<Coordinate2D>,
        target_spatial_resolution: Option<SpatialResolution>,
        tiling_spec: TilingSpecification,
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

        let res = if self
            .result_descriptor
            .spatial_grid_descriptor()
            .is_compatible_grid(&target_spatial_grid)
        {
            // TODO: resample if origin is not allgned to query? (maybe n
            self
        }
        // Query resolution is smaller then workdlow
        else if target_spatial_grid.spatial_resolution().x <= rd_resolution.x
            && target_spatial_grid.spatial_resolution().y <= rd_resolution.y
        //TODO: we should allow to use the "interpolation" as long as the fraction is > 0.5. This would require to keep 4 tiles which seems to be fine. The edge case of resampling with same resolution should also use the interpolation since bilieaner woudl make sense here?
        {
            log::debug!(
                "Target res: {target_spatial_resolution:?}, workflow res: {rd_resolution:?} --> injecting interpolation"
            );
            /*
            let interpolation_method = if self
                .result_descriptor
                .bands
                .bands()
                .iter()
                .all(|b| b.measurement.is_continuous())
            {
                InterpolationMethod::BiLinear
            } else {
                InterpolationMethod::NearestNeighbor
            };
            */

            let interpolation_params = InterpolationParams {
                interpolation: InterpolationMethod::NearestNeighbor,
                output_resolution: InterpolationResolution::Resolution(
                    target_spatial_grid.spatial_resolution(),
                ),
                output_origin_reference: None,
            };

            let iop = Interpolation {
                params: interpolation_params.clone(),
                sources: self.operator.into(),
            };

            let iip = InitializedInterpolation::new_with_source_and_params(
                CanonicOperatorName::from(&iop),
                WorkflowOperatorPath::initialize_root(), // FIXME: this is not correct since the root is the child operator
                self.initialized_operator,
                &interpolation_params,
                tiling_spec,
            )?;
            let rd = iip.result_descriptor().clone();
            Self::new(iop.boxed(), iip.boxed(), rd)
        } else {
            log::debug!(
                "Query res: {target_spatial_resolution:?}, workflow res: {rd_resolution:?} --> injecting downsampling"
            );

            let downsample_params = DownsamplingParams {
                sampling_method: DownsamplingMethod::NearestNeighbor,
                output_resolution: DownsamplingResolution::Resolution(
                    target_spatial_grid.spatial_resolution(),
                ),
                output_origin_reference: None,
            };
            let dop = Downsampling {
                params: downsample_params,
                sources: self.operator.into(),
            };

            let ido = InitializedDownsampling::new_with_source_and_params(
                CanonicOperatorName::from(&dop),
                WorkflowOperatorPath::initialize_root(), // FIXME: this is not correct since the root is the child operator
                self.initialized_operator,
                downsample_params,
                tiling_spec,
            )?;
            let rd = ido.result_descriptor().clone();
            Self::new(dop.boxed(), ido.boxed(), rd)
        };
        Ok(res)
    }

    pub fn wrap_with_projection_and_resample(
        self,
        target_origin_reference: Option<Coordinate2D>,
        target_spatial_resolution: Option<SpatialResolution>,
        target_sref: SpatialReference,
        tiling_spec: TilingSpecification,
    ) -> Result<Self> {
        self.wrap_with_projection(target_sref, target_origin_reference, tiling_spec)?
            .wrap_with_resample(
                target_origin_reference,
                target_spatial_resolution,
                tiling_spec,
            )
    }
}
