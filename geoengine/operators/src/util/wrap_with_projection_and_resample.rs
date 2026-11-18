use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedRasterOperator, ProcessingGraphOperatorPath,
    RasterOperator, RasterResultDescriptor, ResultDescriptor, TypedRasterOperator,
};
use crate::error;
use crate::processing::{
    InitializedWarp, Warp, WarpDownsampling, WarpParams, WarpUpsampling, source_at_overview_level,
    warped_spatial_grid,
};
use crate::util::Result;
use geoengine_datatypes::operations::image::{Colorizer, RasterColorizer};
use geoengine_datatypes::primitives::{Coordinate2D, SpatialResolution};
use geoengine_datatypes::spatial_reference::SpatialReference;

/// How a `Warp` computes the output pixels: [`WarpParams::downsampling`] and
/// [`WarpParams::upsampling`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WarpMethods {
    pub downsampling: WarpDownsampling,
    pub upsampling: WarpUpsampling,
}

impl WarpMethods {
    /// Nearest neighbour, e.g. for data that is downloaded rather than rendered
    pub const NEAREST: Self = Self {
        downsampling: WarpDownsampling::Nearest,
        upsampling: WarpUpsampling::Nearest,
    };

    /// For classes: the most common class of the covered pixels, and no interpolation between classes
    pub const CLASSES: Self = Self {
        downsampling: WarpDownsampling::Mode,
        upsampling: WarpUpsampling::Nearest,
    };

    /// For continuous values: the average of the covered pixels, and bilinear interpolation
    pub const CONTINUOUS: Self = Self {
        downsampling: WarpDownsampling::Average,
        upsampling: WarpUpsampling::Bilinear,
    };

    /// The methods for rendering a raster with `colorizer`: classes for a palette, continuous
    /// values for gradients and RGB composites. A raster without a given colorizer is rendered with
    /// [`default_raster_colorizer`](crate::util::raster_stream_to_png::default_raster_colorizer),
    /// a palette for a classification.
    pub fn for_symbology(colorizer: &RasterColorizer) -> Self {
        match colorizer {
            RasterColorizer::SingleBand {
                band_colorizer: Colorizer::Palette { .. },
                ..
            } => Self::CLASSES,
            RasterColorizer::SingleBand {
                band_colorizer:
                    Colorizer::LinearGradient { .. } | Colorizer::LogarithmicGradient { .. },
                ..
            }
            | RasterColorizer::MultiBand { .. } => Self::CONTINUOUS,
        }
    }
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
    /// the origin, in one `Warp` that computes the output pixels with `methods`.
    /// If the output is coarser than the operator, the operator is optimized to an overview level
    /// first, so that it computes no more pixels than the output needs.
    pub async fn wrap_with_projection_and_resample(
        self,
        target_origin_reference: Option<Coordinate2D>,
        target_spatial_resolution: Option<SpatialResolution>,
        target_sref: SpatialReference,
        methods: WarpMethods,
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
            downsampling: methods.downsampling,
            upsampling: methods.upsampling,
            ..WarpParams::default()
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
                optimized
            }
            None => self,
        };

        // the source is initialized already, so the `Warp` is built around it instead of initializing the graph again
        let warp = Warp {
            params,
            sources: source.operator.into(),
        };
        let span = warp.span();
        let initialized = exe_ctx.wrap_initialized_raster_operator(
            InitializedWarp::new(
                CanonicOperatorName::from(&warp),
                ProcessingGraphOperatorPath::initialize_root(),
                source.initialized_operator,
                params,
                exe_ctx.tiling_specification(),
            )?
            .boxed(),
            span,
        );
        let result_descriptor = initialized.result_descriptor().clone();
        Ok(Self::new(warp.typed(), initialized, result_descriptor))
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
    use geoengine_datatypes::raster::{GeoTransform, GridBoundingBox2D, RasterDataType};
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
                    WarpMethods::NEAREST,
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

    fn single_band(band_colorizer: Colorizer) -> RasterColorizer {
        RasterColorizer::SingleBand {
            band: 0,
            band_colorizer,
        }
    }

    #[test]
    fn it_chooses_the_methods_for_the_symbology() {
        use geoengine_datatypes::operations::image::{RgbParams, RgbaColor};
        use geoengine_datatypes::util::test::TestDefault;

        let palette = single_band(
            Colorizer::palette(
                [(1.0.try_into().unwrap(), RgbaColor::white())].into(),
                RgbaColor::transparent(),
                RgbaColor::black(),
            )
            .unwrap(),
        );
        let linear = single_band(Colorizer::test_default());
        let logarithmic = single_band(
            Colorizer::logarithmic_gradient(
                vec![
                    (1.0, RgbaColor::black()).try_into().unwrap(),
                    (100.0, RgbaColor::white()).try_into().unwrap(),
                ],
                RgbaColor::transparent(),
                RgbaColor::white(),
                RgbaColor::black(),
            )
            .unwrap(),
        );
        let rgb = RasterColorizer::MultiBand {
            red_band: 0,
            green_band: 0,
            blue_band: 0,
            rgb_params: RgbParams {
                red_min: 0.,
                red_max: 255.,
                red_scale: 1.,
                green_min: 0.,
                green_max: 255.,
                green_scale: 1.,
                blue_min: 0.,
                blue_max: 255.,
                blue_scale: 1.,
                no_data_color: RgbaColor::transparent(),
            },
        };

        assert_eq!(WarpMethods::for_symbology(&palette), WarpMethods::CLASSES);
        for colorizer in [&linear, &logarithmic, &rgb] {
            assert_eq!(
                WarpMethods::for_symbology(colorizer),
                WarpMethods::CONTINUOUS
            );
        }
    }

    #[tokio::test]
    async fn it_warps_with_the_methods() {
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
                    Some(SpatialResolution::new_unchecked(1.5, 1.5)),
                    SpatialReference::epsg_4326(),
                    WarpMethods::CLASSES,
                    &exe_ctx,
                )
                .await
                .unwrap();

        let TypedRasterOperator::Warp(warp) = wrapped.operator else {
            panic!("the operator should be warped");
        };
        assert_eq!(warp.params.downsampling, WarpDownsampling::Mode);
        assert_eq!(warp.params.upsampling, WarpUpsampling::Nearest);
    }
}
