use crate::{
    adapters::{QueryWrapper, RasterArrayTimeAdapter},
    engine::{
        BoxRasterQueryProcessor, CanonicOperatorName, ExecutionContext, InitializedRasterOperator,
        InitializedSources, Operator, OperatorData, OperatorName, QueryContext, QueryProcessor,
        RasterBandDescriptors, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
        TypedRasterQueryProcessor, WorkflowOperatorPath,
    },
    util::Result,
};
use async_trait::async_trait;
use futures::{stream::BoxStream, try_join, StreamExt};
use geoengine_datatypes::{
    dataset::NamedData,
    primitives::{
        partitions_extent, time_interval_extent, BandSelection, RasterQueryRectangle,
        SpatialPartition2D, SpatialResolution,
    },
    raster::{
        FromIndexFn, GridIndexAccess, GridOrEmpty, GridShapeAccess, RasterDataType, RasterTile2D,
    },
    spatial_reference::SpatialReferenceOption,
};
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};
use snafu::{ensure, Snafu};

/// The `Rgb` operator combines three raster sources into a single raster output.
/// The output it of type `U32` and contains the red, green and blue values as the first, second and third byte.
/// The forth byte (alpha) is always 255.
pub type Rgb = Operator<RgbParams, RgbSources>;

/// Parameters for the `Rgb` operator.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub struct RgbParams {
    /// The minimum value for the red channel.
    pub red_min: f64,
    /// The maximum value for the red channel.
    pub red_max: f64,
    /// A scaling factor for the red channel between 0 and 1.
    #[serde(default = "num_traits::One::one")]
    pub red_scale: f64,

    /// The minimum value for the red channel.
    pub green_min: f64,
    /// The maximum value for the red channel.
    pub green_max: f64,
    /// A scaling factor for the green channel between 0 and 1.
    #[serde(default = "num_traits::One::one")]
    pub green_scale: f64,

    /// The minimum value for the red channel.
    pub blue_min: f64,
    /// The maximum value for the red channel.
    pub blue_max: f64,
    /// A scaling factor for the blue channel between 0 and 1.
    #[serde(default = "num_traits::One::one")]
    pub blue_scale: f64,
}

impl RgbParams {
    fn check_valid_user_input(&self) -> Result<()> {
        ensure!(
            self.red_min < self.red_max,
            error::MinGreaterThanMax {
                color: "red",
                min: self.red_min,
                max: self.red_max,
            }
        );
        ensure!(
            self.green_min < self.green_max,
            error::MinGreaterThanMax {
                color: "green",
                min: self.green_min,
                max: self.green_max,
            }
        );
        ensure!(
            self.blue_min < self.blue_max,
            error::MinGreaterThanMax {
                color: "blue",
                min: self.blue_min,
                max: self.blue_max,
            }
        );

        ensure!(
            (0.0..=1.0).contains(&self.red_scale)
                && (0.0..=1.0).contains(&self.green_scale)
                && (0.0..=1.0).contains(&self.blue_scale),
            error::RgbScaleOutOfBounds {
                red: self.red_scale,
                green: self.green_scale,
                blue: self.blue_scale
            }
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RgbSources {
    red: Box<dyn RasterOperator>,
    green: Box<dyn RasterOperator>,
    blue: Box<dyn RasterOperator>,
}

impl OperatorData for RgbSources {
    fn data_names_collect(&self, data_names: &mut Vec<NamedData>) {
        for source in self.iter() {
            source.data_names_collect(data_names);
        }
    }
}

impl RgbSources {
    pub fn new(
        red: Box<dyn RasterOperator>,
        green: Box<dyn RasterOperator>,
        blue: Box<dyn RasterOperator>,
    ) -> Self {
        Self { red, green, blue }
    }

    fn iter(&self) -> impl Iterator<Item = &Box<dyn RasterOperator>> {
        [&self.red, &self.green, &self.blue].into_iter()
    }
}

#[async_trait]
impl InitializedSources<RgbInitializedSources> for RgbSources {
    async fn initialize_sources(
        self,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<RgbInitializedSources> {
        let (red, green, blue) = try_join!(
            self.red.initialize(path.clone_and_append(0), context),
            self.green.initialize(path.clone_and_append(1), context),
            self.blue.initialize(path.clone_and_append(2), context),
        )?;

        Ok(RgbInitializedSources { red, green, blue })
    }
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for Rgb {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        self.params.check_valid_user_input()?;

        let name = CanonicOperatorName::from(&self);

        let sources = self.sources.initialize_sources(path, context).await?;

        // TODO: implement multi-band functionality and remove this check
        ensure!(
            sources
                .iter()
                .all(|r| r.result_descriptor().bands.len() == 1),
            crate::error::OperatorDoesNotSupportMultiBandsSourcesYet {
                operator: Rgb::TYPE_NAME
            }
        );

        let spatial_reference = sources.red.result_descriptor().spatial_reference;

        ensure!(
            sources
                .iter()
                .skip(1)
                .all(|rd| rd.result_descriptor().spatial_reference == spatial_reference),
            error::DifferentSpatialReferences {
                red: sources.red.result_descriptor().spatial_reference,
                green: sources.green.result_descriptor().spatial_reference,
                blue: sources.blue.result_descriptor().spatial_reference,
            }
        );

        let time =
            time_interval_extent(sources.iter().map(|source| source.result_descriptor().time));
        let bbox = partitions_extent(sources.iter().map(|source| source.result_descriptor().bbox));

        let resolution = sources
            .iter()
            .map(|source| source.result_descriptor().resolution)
            .reduce(|a, b| match (a, b) {
                (Some(a), Some(b)) => {
                    Some(SpatialResolution::new_unchecked(a.x.min(b.x), a.y.min(b.y)))
                }
                // we can only compute the minimum resolution if all sources have a resolution
                _ => None,
            })
            .flatten();

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U32,
            spatial_reference,
            time,
            bbox,
            resolution,
            bands: RasterBandDescriptors::new_single_band(),
        };

        let initialized_operator = InitializedRgb {
            name,
            result_descriptor,
            sources,
            params: self.params,
        };

        Ok(initialized_operator.boxed())
    }

    span_fn!(Rgb);
}

impl OperatorName for Rgb {
    const TYPE_NAME: &'static str = "Rgb";
}

pub struct InitializedRgb {
    name: CanonicOperatorName,
    result_descriptor: RasterResultDescriptor,
    params: RgbParams,
    sources: RgbInitializedSources,
}

pub struct RgbInitializedSources {
    red: Box<dyn InitializedRasterOperator>,
    green: Box<dyn InitializedRasterOperator>,
    blue: Box<dyn InitializedRasterOperator>,
}

impl RgbInitializedSources {
    fn iter(&self) -> impl Iterator<Item = &Box<dyn InitializedRasterOperator>> {
        [&self.red, &self.green, &self.blue].into_iter()
    }
}

impl InitializedRasterOperator for InitializedRgb {
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        Ok(RgbQueryProcessor::new(
            self.sources.red.query_processor()?.into_f64(),
            self.sources.green.query_processor()?.into_f64(),
            self.sources.blue.query_processor()?.into_f64(),
            self.params,
        )
        .boxed()
        .into())
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }
}

pub struct RgbQueryProcessor {
    red: BoxRasterQueryProcessor<f64>,
    green: BoxRasterQueryProcessor<f64>,
    blue: BoxRasterQueryProcessor<f64>,
    params: RgbParams,
}

impl RgbQueryProcessor {
    pub fn new(
        red: BoxRasterQueryProcessor<f64>,
        green: BoxRasterQueryProcessor<f64>,
        blue: BoxRasterQueryProcessor<f64>,
        params: RgbParams,
    ) -> Self {
        Self {
            red,
            green,
            blue,
            params,
        }
    }
}

#[async_trait]
impl QueryProcessor for RgbQueryProcessor {
    type Output = RasterTile2D<u32>;
    type SpatialBounds = SpatialPartition2D;
    type Selection = BandSelection;

    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let red = QueryWrapper { p: &self.red, ctx };
        let green = QueryWrapper {
            p: &self.green,
            ctx,
        };
        let blue = QueryWrapper { p: &self.blue, ctx };

        let params = self.params;

        let stream = RasterArrayTimeAdapter::new([red, green, blue], query)
            .map(move |tiles| Ok(compute_tile(tiles?, &params)));

        Ok(Box::pin(stream))
    }
}

fn compute_tile(
    [red, green, blue]: [RasterTile2D<f64>; 3],
    params: &RgbParams,
) -> RasterTile2D<u32> {
    fn fit_to_interval_0_255(value: f64, min: f64, max: f64, scale: f64) -> u32 {
        let mut result = value - min; // shift towards zero
        result /= max - min; // normalize to [0, 1]
        result *= scale; // after scaling with scale ∈ [0, 1], value stays in [0, 1]
        result = (255. * result).round().clamp(0., 255.); // bring value to integer range [0, 255]
        result.as_()
    }

    let map_fn = |lin_idx: usize| -> Option<u32> {
        let (Some(red_value), Some(green_value), Some(blue_value)) = (
            red.get_at_grid_index_unchecked(lin_idx),
            green.get_at_grid_index_unchecked(lin_idx),
            blue.get_at_grid_index_unchecked(lin_idx),
        ) else {
            return None;
        };

        let red =
            fit_to_interval_0_255(red_value, params.red_min, params.red_max, params.red_scale);
        let green = fit_to_interval_0_255(
            green_value,
            params.green_min,
            params.green_max,
            params.green_scale,
        );
        let blue = fit_to_interval_0_255(
            blue_value,
            params.blue_min,
            params.blue_max,
            params.blue_scale,
        );
        let alpha: u32 = 255;

        let rgba = (red << 24) | (green << 16) | (blue << 8) | (alpha);

        Some(rgba)
    };

    // all tiles have the same shape, time, position, etc.
    // so we use red for reference

    let grid_shape = red.grid_shape();
    // TODO: check if parallelism brings any speed-up – might no be faster since we only do simple arithmetic
    let out_grid = GridOrEmpty::from_index_fn(&grid_shape, map_fn);

    RasterTile2D::new(
        red.time,
        red.tile_position,
        0, // TODO
        red.global_geo_transform,
        out_grid,
        red.cache_hint
            .merged(&green.cache_hint)
            .merged(&blue.cache_hint),
    )
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)), module(error))]
pub enum RgbOperatorError {
    #[snafu(display(
        "The scale values for r, g and b must be in the range [0, 1]. Got: red: {red}, green: {green}, blue: {blue}",
    ))]
    RgbScaleOutOfBounds { red: f64, green: f64, blue: f64 },

    #[snafu(display(
        "Min value for `{color}` must be smaller than max value. Got: min: {min}, max: {max}",
    ))]
    MinGreaterThanMax {
        color: &'static str,
        min: f64,
        max: f64,
    },

    #[snafu(display(
        "The sources must have the same spatial reference. Got: red: {red}, green: {green}, blue: {blue}",
    ))]
    DifferentSpatialReferences {
        red: SpatialReferenceOption,
        green: SpatialReferenceOption,
        blue: SpatialReferenceOption,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext, QueryProcessor};
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use futures::StreamExt;
    use geoengine_datatypes::operations::image::{Colorizer, RgbaColor};
    use geoengine_datatypes::primitives::{
        CacheHint, RasterQueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::raster::{
        Grid2D, GridOrEmpty, MapElements, MaskedGrid2D, RasterTile2D, TileInformation,
        TilingSpecification,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;
    use itertools::Itertools;

    #[test]
    fn deserialize_params() {
        assert_eq!(
            serde_json::from_value::<RgbParams>(serde_json::json!({
                "redMin": 0.,
                "redMax": 1.,
                "redScale": 1.,
                "greenMin": -5.,
                "greenMax": 6.,
                "greenScale": 0.5,
                "blueMin": 0.,
                "blueMax": 10.,
                "blueScale": 0.,
            }))
            .unwrap(),
            RgbParams {
                red_min: 0.,
                red_max: 1.,
                red_scale: 1.,
                green_min: -5.,
                green_max: 6.,
                green_scale: 0.5,
                blue_min: 0.,
                blue_max: 10.,
                blue_scale: 0.,
            }
        );
    }

    #[test]
    fn serialize_params() {
        assert_eq!(
            serde_json::json!({
                "redMin": 0.,
                "redMax": 1.,
                "redScale": 1.,
                "greenMin": -5.,
                "greenMax": 6.,
                "greenScale": 0.5,
                "blueMin": 0.,
                "blueMax": 10.,
                "blueScale": 0.,
            }),
            serde_json::to_value(RgbParams {
                red_min: 0.,
                red_max: 1.,
                red_scale: 1.,
                green_min: -5.,
                green_max: 6.,
                green_scale: 0.5,
                blue_min: 0.,
                blue_max: 10.,
                blue_scale: 0.,
            })
            .unwrap()
        );
    }

    #[tokio::test]
    async fn computation() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let o = Rgb {
            params: RgbParams {
                red_min: 1.,
                red_max: 6.,
                red_scale: 1.,
                green_min: 1.,
                green_max: 6.,
                green_scale: 0.5,
                blue_min: 1.,
                blue_max: 6.,
                blue_scale: 0.1,
            },
            sources: RgbSources {
                red: make_raster(None),
                green: make_raster(Some(3)),
                blue: make_raster(Some(4)),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &ctx)
        .await
        .unwrap();

        let processor = o.query_processor().unwrap().get_u32().unwrap();

        let ctx = MockQueryContext::new(1.into());
        let result_stream = processor
            .query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 3.).into(),
                        (2., 0.).into(),
                    ),
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::one(),
                    selection: Default::default(),
                },
                &ctx,
            )
            .await
            .unwrap();

        let result: Vec<Result<RasterTile2D<u32>>> = result_stream.collect().await;

        assert_eq!(result.len(), 1);

        let first_result = result[0].as_ref().unwrap();

        assert!(!first_result.is_empty());

        let grid = match &first_result.grid_array {
            GridOrEmpty::Grid(g) => g,
            GridOrEmpty::Empty(_) => panic!(),
        };

        let res: Vec<Option<u32>> = grid.masked_element_deref_iterator().collect();

        let expected: [Option<u32>; 6] = [
            Some(0x00_00_00_FF),
            Some(0x33_1A_05_FF),
            None, // 3 is NODATA at green
            None, // 4 is NODATA at blue
            Some(0xCC_66_14_FF),
            Some(0xFF_80_1A_FF),
        ];

        assert_eq!(
            res,
            expected,
            "values must be equal:\n {actual} !=\n {expected}",
            actual = res
                .iter()
                .map(|v| v.map_or("_".to_string(), |v| format!("{v:X}")))
                .join(", "),
            expected = expected
                .iter()
                .map(|v| v.map_or("_".to_string(), |v| format!("{v:X}")))
                .join(", "),
        );

        let colorizer = Colorizer::rgba();
        let color_mapper = colorizer.create_color_mapper();

        let colors: Vec<RgbaColor> = res
            .iter()
            .map(|v| v.map_or(RgbaColor::transparent(), |v| color_mapper.call(v)))
            .collect();

        assert_eq!(
            colors,
            [
                RgbaColor::new(0x00, 0x00, 0x00, 0xFF),
                RgbaColor::new(0x33, 0x1A, 0x05, 0xFF),
                RgbaColor::transparent(), // 3 is NODATA at green
                RgbaColor::transparent(), // 4 is NODATA at blue
                RgbaColor::new(0xCC, 0x66, 0x14, 0xFF),
                RgbaColor::new(0xFF, 0x80, 0x1A, 0xFF),
            ]
        );
    }

    fn make_raster(no_data_value: Option<i8>) -> Box<dyn RasterOperator> {
        let raster = Grid2D::<i8>::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap();

        let real_raster = if let Some(no_data_value) = no_data_value {
            MaskedGrid2D::from(raster)
                .map_elements(|e: Option<i8>| e.filter(|v| *v != no_data_value))
                .into()
        } else {
            GridOrEmpty::from(raster)
        };

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_tile_position: [-1, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
                global_geo_transform: TestDefault::test_default(),
            },
            0,
            real_raster,
            CacheHint::no_cache(),
        );

        MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::I8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed()
    }
}
