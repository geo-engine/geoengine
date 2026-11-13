use crate::engine::{
    ExecutionContext, InitializedRasterOperator, ProcessingGraphOperatorPath, RasterOperator,
    RasterResultDescriptor, ResultDescriptor, TypedRasterOperator,
};
use crate::error;
use crate::processing::{
    RemoveTileOverlap, RemoveTileOverlapParams, Warp, WarpMethod, WarpParams,
    source_at_overview_level, warped_spatial_grid,
};
use crate::util::Result;
use geoengine_datatypes::primitives::{Coordinate2D, SpatialResolution};
use geoengine_datatypes::spatial_reference::SpatialReference;

/// Wraps `operator` in a `RemoveTileOverlap` if its tiles carry an overlap halo.
///
/// Sinks must never receive overlapping tiles: halo pixels would be written
/// or rendered in addition to the cores.
pub fn strip_tile_overlap(
    operator: TypedRasterOperator,
    result_descriptor: &RasterResultDescriptor,
) -> Option<TypedRasterOperator> {
    result_descriptor
        .spatial_grid_descriptor()
        .has_tile_overlap()
        .then(|| {
            RemoveTileOverlap {
                params: RemoveTileOverlapParams { amount: None },
                sources: crate::engine::SingleRasterSource { raster: operator },
            }
            .typed()
        })
}

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

    /// Puts the operator onto the query's grid: `target_sref`, and, if given, the resolution and
    /// the origin, in one `Warp`.
    /// If the output is coarser than the operator, the operator is optimized to an overview level
    /// first, so that it computes no more pixels than the output needs.
    /// Overlapping tiles are stripped to their cores first.
    pub async fn wrap_with_projection_and_resample(
        self,
        target_origin_reference: Option<Coordinate2D>,
        target_spatial_resolution: Option<SpatialResolution>,
        target_sref: SpatialReference,
        exe_ctx: &dyn ExecutionContext,
    ) -> Result<Self> {
        self.wrap_with_overlap_removal(exe_ctx)
            .await?
            .wrap_with_warp(
                target_origin_reference,
                target_spatial_resolution,
                target_sref,
                exe_ctx,
            )
            .await
    }

    /// Strips the tile overlap halo before any output processing.
    /// Projection and resampling also expect non-overlapping inputs.
    pub async fn wrap_with_overlap_removal(self, exe_ctx: &dyn ExecutionContext) -> Result<Self> {
        let Some(remove_overlap) =
            strip_tile_overlap(self.operator.clone(), &self.result_descriptor)
        else {
            return Ok(self);
        };

        let initialized = remove_overlap
            .clone()
            .initialize(ProcessingGraphOperatorPath::initialize_root(), exe_ctx)
            .await?;

        Ok(Self::new_create_result_descriptor(remove_overlap, initialized))
    }

    async fn wrap_with_warp(
        self,
        target_origin_reference: Option<Coordinate2D>,
        target_spatial_resolution: Option<SpatialResolution>,
        target_sref: SpatialReference,
        exe_ctx: &dyn ExecutionContext,
    ) -> Result<Self> {
        let result_sref = self
            .result_descriptor
            .spatial_reference()
            .as_option()
            .ok_or(error::Error::SpatialReferenceMustNotBeUnreferenced)?;

        let params = WarpParams {
            target_spatial_reference: (target_sref != result_sref).then_some(target_sref),
            output_resolution: target_spatial_resolution,
            output_origin_reference: target_origin_reference,
            method: WarpMethod::NearestNeighbor,
        };

        let target_spatial_grid = warped_spatial_grid(&self.result_descriptor, &params)?;
        if target_sref == result_sref
            && self
                .result_descriptor
                .spatial_grid_descriptor()
                .is_compatible_grid(&target_spatial_grid)
        {
            // TODO: resample if origin is not aligned to query?
            return Ok(self);
        }

        tracing::debug!(
            "Target srs: {target_sref}, res: {target_spatial_resolution:?}, origin: {target_origin_reference:?}, processing graph srs: {result_sref}, res: {:?} --> injecting warp",
            self.result_descriptor
                .spatial_grid_descriptor()
                .spatial_resolution()
        );

        // compute on an overview level if the output is coarser
        let source = match source_at_overview_level(self.initialized_operator.as_ref(), &params)? {
            Some(optimized) => {
                let initialized = optimized
                    .clone()
                    .initialize(ProcessingGraphOperatorPath::initialize_root(), exe_ctx)
                    .await?;
                let result_descriptor = initialized.result_descriptor().clone();
                let optimized = Self::new(optimized, initialized, result_descriptor);

                // the target resolution is an overview level, so the optimized operator is the result
                if target_sref == result_sref
                    && optimized
                        .result_descriptor
                        .spatial_grid_descriptor()
                        .is_compatible_grid(&target_spatial_grid)
                {
                    return Ok(optimized);
                }
                optimized.operator
            }
            None => self.operator,
        };

        let warp = Warp {
            params,
            sources: source.into(),
        }
        .typed();
        let initialized = warp
            .clone()
            .initialize(ProcessingGraphOperatorPath::initialize_root(), exe_ctx)
            .await?;
        let result_descriptor = initialized.result_descriptor().clone();
        Ok(Self::new(warp, initialized, result_descriptor))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        MockExecutionContext, RasterBandDescriptors, SpatialGridDescriptor, TimeDescriptor,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use crate::processing::{Expression, ExpressionParams};
    use geoengine_datatypes::primitives::TimeInterval;
    use geoengine_datatypes::raster::TileOverlap;
    use geoengine_datatypes::raster::{GeoTransform, GridBoundingBox2D, RasterDataType, TileSize};
    use geoengine_datatypes::util::test::TestDefault;

    #[test]
    fn it_strips_only_overlapping_tiles() {
        let operator = Expression {
            params: ExpressionParams {
                expression: "A".to_string(),
                output_type: RasterDataType::U8,
                output_band: None,
                map_no_data: false,
            },
            sources: crate::engine::SingleRasterSource {
                raster: crate::source::GdalSource {
                    params: crate::source::GdalSourceParameters::new(
                        geoengine_datatypes::dataset::NamedData::with_system_name("test"),
                    ),
                }
                .typed(),
            },
        }
        .typed();
        let mut descriptor = RasterResultDescriptor::with_datatype_and_num_bands(
            RasterDataType::U8,
            1,
            geoengine_datatypes::raster::GridBoundingBox2D::new([0, 0], [1, 1]).unwrap(),
            geoengine_datatypes::util::test::TestDefault::test_default(),
            crate::engine::TimeDescriptor::new_irregular(None),
        );

        assert!(strip_tile_overlap(operator.clone(), &descriptor).is_none());

        descriptor.spatial_grid = descriptor
            .spatial_grid
            .with_tile_overlap(TileOverlap::new(1, 1));
        assert!(matches!(
            strip_tile_overlap(operator, &descriptor),
            Some(TypedRasterOperator::RemoveTileOverlap(_))
        ));
    }

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
                .wrap_with_projection_and_resample(
                    None,
                    Some(target_resolution),
                    SpatialReference::epsg_4326(),
                    &exe_ctx,
                )
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

    /// one `Warp` directly on the source, without an overview level of the source
    fn assert_single_warp(operator: &TypedRasterOperator) {
        let TypedRasterOperator::Warp(warp) = operator else {
            panic!("the operator should be warped, was {operator:?}");
        };
        assert!(matches!(
            warp.sources.raster,
            TypedRasterOperator::MockRasterSourceu8(_)
        ));
    }

    #[tokio::test]
    async fn it_resamples_to_a_resolution_that_is_coarser_in_one_axis_and_finer_in_the_other() {
        let operator = wrap_with_resample(SpatialResolution::new_unchecked(2., 0.5)).await;

        assert_single_warp(&operator);
    }

    #[tokio::test]
    async fn it_resamples_to_a_resolution_that_is_coarser_in_one_axis() {
        let operator = wrap_with_resample(SpatialResolution::new_unchecked(3., 1.)).await;

        assert_single_warp(&operator);
    }

    #[tokio::test]
    async fn it_resamples_to_a_finer_resolution() {
        let operator = wrap_with_resample(SpatialResolution::new_unchecked(0.5, 0.5)).await;

        assert_single_warp(&operator);
    }
}
