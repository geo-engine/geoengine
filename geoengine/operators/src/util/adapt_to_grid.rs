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

/// The grid that [`AdaptToGrid::adapt_to_grid`] puts an operator onto
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TargetGrid {
    pub spatial_reference: SpatialReference,
    /// `None` keeps the resolution of the operator, reprojected if `spatial_reference` differs
    pub resolution: Option<SpatialResolution>,
    /// `None` keeps the origin of the operator's grid, reprojected if `spatial_reference` differs
    pub origin: Option<Coordinate2D>,
}

pub struct AdaptToGrid {
    pub operator: TypedRasterOperator,
    pub initialized_operator: Box<dyn InitializedRasterOperator>,
    pub result_descriptor: RasterResultDescriptor,
}

impl AdaptToGrid {
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

    /// Puts the operator onto the `target` grid, in one `Warp` that computes the output pixels
    /// with `methods`.
    /// If the output is coarser than the operator, the operator is optimized to an overview level
    /// first, so that it computes no more pixels than the output needs.
    pub async fn adapt_to_grid(
        self,
        target: TargetGrid,
        methods: WarpMethods,
        exe_ctx: &dyn ExecutionContext,
    ) -> Result<Self> {
        let TargetGrid {
            spatial_reference: target_sref,
            resolution: target_spatial_resolution,
            origin: target_origin_reference,
        } = target;

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
                exe_ctx,
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
    use crate::engine::SingleRasterSource;
    use crate::engine::{
        MockExecutionContext, RasterBandDescriptors, SpatialGridDescriptor, TimeDescriptor,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use crate::processing::{Expression, ExpressionParams};
    use crate::source::{GdalSource, GdalSourceParameters};
    use crate::util::gdal::add_ndvi_dataset;
    use geoengine_datatypes::primitives::TimeInterval;
    use geoengine_datatypes::raster::{GeoTransform, GridBoundingBox2D, RasterDataType};
    use geoengine_datatypes::util::test::TestDefault;

    async fn adapt_to_resolution(target_resolution: SpatialResolution) -> TypedRasterOperator {
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

        let wrapped = AdaptToGrid::new_create_result_descriptor(source, initialized)
            .adapt_to_grid(
                TargetGrid {
                    spatial_reference: SpatialReference::epsg_4326(),
                    resolution: Some(target_resolution),
                    origin: None,
                },
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
        let operator = adapt_to_resolution(SpatialResolution::new_unchecked(2., 0.5)).await;

        assert_single_warp(&operator);
    }

    #[tokio::test]
    async fn it_resamples_to_a_resolution_that_is_coarser_in_one_axis() {
        let operator = adapt_to_resolution(SpatialResolution::new_unchecked(3., 1.)).await;

        assert_single_warp(&operator);
    }

    #[tokio::test]
    async fn it_resamples_to_a_finer_resolution() {
        let operator = adapt_to_resolution(SpatialResolution::new_unchecked(0.5, 0.5)).await;

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

        let wrapped = AdaptToGrid::new_create_result_descriptor(source, initialized)
            .adapt_to_grid(
                TargetGrid {
                    spatial_reference: SpatialReference::epsg_4326(),
                    resolution: Some(SpatialResolution::new_unchecked(1.5, 1.5)),
                    origin: None,
                },
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

    /// The NDVI of 0.1° in EPSG:4326 with overviews
    fn ndvi(exe_ctx: &mut MockExecutionContext) -> TypedRasterOperator {
        GdalSource {
            params: GdalSourceParameters::new(add_ndvi_dataset(exe_ctx)),
        }
        .typed()
    }

    fn warp(source: TypedRasterOperator, params: WarpParams) -> TypedRasterOperator {
        Warp {
            params,
            sources: SingleRasterSource { raster: source },
        }
        .typed()
    }

    fn resample(resolution: f64) -> WarpParams {
        WarpParams {
            output_resolution: Some(SpatialResolution::new_unchecked(resolution, resolution)),
            ..WarpParams::default()
        }
    }

    fn identity(source: TypedRasterOperator) -> TypedRasterOperator {
        Expression {
            params: ExpressionParams {
                expression: "A".to_string(),
                output_type: RasterDataType::U8,
                output_band: None,
                map_no_data: false,
            },
            sources: SingleRasterSource { raster: source },
        }
        .typed()
    }

    /// Wraps `operator` as WMS and OGC API Tiles do, for an output of `resolution` in `srs`.
    async fn adapt_graph(
        operator: TypedRasterOperator,
        resolution: SpatialResolution,
        srs: SpatialReference,
        exe_ctx: &MockExecutionContext,
    ) -> AdaptToGrid {
        let initialized = operator
            .clone()
            .initialize(ProcessingGraphOperatorPath::initialize_root(), exe_ctx)
            .await
            .unwrap();
        AdaptToGrid::new_create_result_descriptor(operator, initialized)
            .adapt_to_grid(
                TargetGrid {
                    spatial_reference: srs,
                    resolution: Some(resolution),
                    origin: None,
                },
                WarpMethods::CONTINUOUS,
                exe_ctx,
            )
            .await
            .unwrap()
    }

    /// The single child of `operator`, for the operators of these tests
    fn child(operator: &TypedRasterOperator) -> Option<&TypedRasterOperator> {
        match operator {
            TypedRasterOperator::Warp(warp) => Some(&warp.sources.raster),
            TypedRasterOperator::Expression(expression) => Some(&expression.sources.raster),
            _ => None,
        }
    }

    /// The operators from `operator` down to its leaf
    fn path(operator: &TypedRasterOperator) -> Vec<&TypedRasterOperator> {
        std::iter::successors(Some(operator), |operator| child(operator)).collect()
    }

    fn warps(operator: &TypedRasterOperator) -> Vec<&WarpParams> {
        path(operator)
            .into_iter()
            .filter_map(|operator| match operator {
                TypedRasterOperator::Warp(warp) => Some(&warp.params),
                _ => None,
            })
            .collect()
    }

    fn overview_level(operator: &TypedRasterOperator) -> Option<u32> {
        match path(operator).last() {
            Some(TypedRasterOperator::GdalSource(gdal)) => {
                gdal.params.overview_level.map(std::num::NonZeroU32::get)
            }
            leaf => panic!("the leaf should be a GDAL source, was {leaf:?}"),
        }
    }

    /// Queries the first tile of `wrapped` at the first time step of the NDVI.
    async fn query_first_tile(
        wrapped: AdaptToGrid,
        exe_ctx: &MockExecutionContext,
    ) -> geoengine_datatypes::raster::RasterTile2D<u8> {
        use futures::StreamExt;
        use geoengine_datatypes::primitives::{BandSelection, RasterQueryRectangle};

        let tiling = wrapped
            .result_descriptor
            .tiling_grid_definition(exe_ctx.tiling_specification());
        let first_tile = tiling
            .generate_data_tiling_strategy()
            .tile_information_iterator_from_pixel_bounds(tiling.tiling_grid_bounds())
            .next()
            .unwrap();
        let processor = wrapped
            .initialized_operator
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let tiles: Vec<_> = processor
            .query(
                RasterQueryRectangle::new(
                    first_tile.global_pixel_bounds(),
                    TimeInterval::new_unchecked(1_388_534_400_000, 1_388_534_400_001),
                    BandSelection::first(),
                ),
                &query_ctx,
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect()
            .await;
        assert_eq!(tiles.len(), 1);
        tiles.into_iter().next().unwrap()
    }

    #[tokio::test]
    async fn it_rewrites_the_resolution_of_a_warp_in_the_graph_for_overviews() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let ndvi = ndvi(&mut exe_ctx);
        let graph = identity(warp(ndvi, resample(0.2)));

        let wrapped = adapt_graph(
            graph,
            SpatialResolution::new_unchecked(0.8, 0.8),
            SpatialReference::epsg_4326(),
            &exe_ctx,
        )
        .await;

        assert_eq!(
            wrapped.result_descriptor.spatial_grid.spatial_resolution(),
            SpatialResolution::new_unchecked(0.8, 0.8)
        );
        assert!(warps(&wrapped.operator).iter().all(|params| {
            params.output_resolution == Some(SpatialResolution::new_unchecked(0.8, 0.8))
        }));
        assert_eq!(overview_level(&wrapped.operator), Some(8));

        let tile = query_first_tile(wrapped, &exe_ctx).await;
        assert!(!tile.is_empty());
    }

    fn max_warp(source: TypedRasterOperator, resolution: f64) -> TypedRasterOperator {
        warp(
            source,
            WarpParams {
                downsampling: WarpDownsampling::Max,
                ..resample(resolution)
            },
        )
    }

    #[tokio::test]
    async fn it_keeps_the_kernel_of_a_warp_between_overview_levels() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let ndvi = ndvi(&mut exe_ctx);

        // 0.3° × 4 = 1.2° lies between the overview levels of 0.8° and 1.6°
        let wrapped = adapt_graph(
            max_warp(ndvi, 0.3),
            SpatialResolution::new_unchecked(1.2, 1.2),
            SpatialReference::epsg_4326(),
            &exe_ctx,
        )
        .await;

        let warps = warps(&wrapped.operator);
        assert_eq!(warps.len(), 1, "the warp of the graph stays, no other one");
        assert_eq!(warps[0].downsampling, WarpDownsampling::Max);
        assert_eq!(
            warps[0].output_resolution,
            Some(SpatialResolution::new_unchecked(1.2, 1.2))
        );
        assert_eq!(overview_level(&wrapped.operator), Some(8));
    }

    #[tokio::test]
    async fn it_drops_a_warp_with_a_kernel_on_an_overview_level() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let ndvi = ndvi(&mut exe_ctx);

        let wrapped = adapt_graph(
            max_warp(ndvi, 0.2),
            SpatialResolution::new_unchecked(0.8, 0.8),
            SpatialReference::epsg_4326(),
            &exe_ctx,
        )
        .await;

        // the overview level of 0.8° is the output: always overviews, regardless of the kernel
        assert!(
            warps(&wrapped.operator).is_empty(),
            "nothing to warp, was {:?}",
            wrapped.operator
        );
        assert_eq!(overview_level(&wrapped.operator), Some(8));
    }

    #[tokio::test]
    async fn it_rewrites_the_resolution_of_nested_warps_for_overviews() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let ndvi = ndvi(&mut exe_ctx);
        let graph = warp(warp(ndvi, resample(0.2)), resample(0.4));

        let wrapped = adapt_graph(
            graph,
            SpatialResolution::new_unchecked(1.6, 1.6),
            SpatialReference::epsg_4326(),
            &exe_ctx,
        )
        .await;

        assert_eq!(
            wrapped.result_descriptor.spatial_grid.spatial_resolution(),
            SpatialResolution::new_unchecked(1.6, 1.6)
        );
        assert_eq!(overview_level(&wrapped.operator), Some(16));

        let tile = query_first_tile(wrapped, &exe_ctx).await;
        assert!(!tile.is_empty());
    }

    #[tokio::test]
    async fn it_rewrites_the_resolution_of_a_reprojecting_warp_in_the_graph_for_overviews() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let ndvi = GdalSource {
            params: GdalSourceParameters::new(
                crate::util::gdal::add_ndvi_dataset_cropped_to_valid_webmercator_bounds(
                    &mut exe_ctx,
                ),
            ),
        }
        .typed();
        let web_mercator = SpatialReference::new(
            geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
            3857,
        );
        let graph = identity(warp(
            ndvi,
            WarpParams {
                target_spatial_reference: Some(web_mercator),
                ..WarpParams::default()
            },
        ));
        let initialized = graph
            .clone()
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();
        let native = initialized
            .result_descriptor()
            .spatial_grid
            .spatial_resolution();
        let origin = initialized
            .result_descriptor()
            .spatial_grid
            .geo_transform()
            .origin_coordinate;
        let target = (native * 8.).unwrap();

        let wrapped = adapt_graph(graph, target, web_mercator, &exe_ctx).await;

        assert_eq!(
            wrapped.result_descriptor.spatial_grid.spatial_resolution(),
            target
        );
        assert_eq!(
            wrapped
                .result_descriptor
                .spatial_grid
                .geo_transform()
                .origin_coordinate,
            origin,
            "the output keeps the alignment of the warp"
        );
        assert!(
            overview_level(&wrapped.operator).is_some_and(|level| level >= 4),
            "the source should read an overview, was {:?}",
            wrapped.operator
        );

        let tile = query_first_tile(wrapped, &exe_ctx).await;
        assert!(!tile.is_empty());
    }
}
