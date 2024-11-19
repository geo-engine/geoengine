use super::abortable_query_execution;
use crate::engine::{QueryAbortTrigger, QueryContext, QueryProcessor, RasterQueryProcessor};
use crate::util::Result;
use futures::TryStreamExt;
use futures::{future::BoxFuture, StreamExt};
use geoengine_datatypes::error::{BoxedResultExt, ErrorSource};
use geoengine_datatypes::operations::image::{ColorMapper, RgbParams};
use geoengine_datatypes::raster::{FromIndexFn, GridIndexAccess, GridShapeAccess};
use geoengine_datatypes::{
    operations::image::{Colorizer, RasterColorizer, RgbaColor, ToPng},
    primitives::{AxisAlignedRectangle, CacheHint, RasterQueryRectangle, TimeInterval},
    raster::{Blit, ConvertDataType, EmptyGrid2D, GeoTransform, GridOrEmpty, Pixel, RasterTile2D},
};
use num_traits::AsPrimitive;
use snafu::Snafu;
use std::convert::TryInto;
use tracing::{span, Level};

/// # Panics
/// Panics if not three bands were queried.
#[allow(clippy::too_many_arguments)]
pub async fn raster_stream_to_png_bytes<T: Pixel, C: QueryContext + 'static>(
    processor: Box<dyn RasterQueryProcessor<RasterType = T>>,
    query_rect: RasterQueryRectangle,
    mut query_ctx: C,
    width: u32,
    height: u32,
    time: Option<TimeInterval>,
    raster_colorizer: Option<RasterColorizer>,
    conn_closed: BoxFuture<'_, ()>,
) -> Result<(Vec<u8>, CacheHint)> {
    debug_assert!(
        query_rect.attributes.count() <= 3
            || query_rect
                .attributes
                .as_slice()
                .windows(2)
                .all(|w| w[0] < w[1]), // TODO: replace with `is_sorted` once it is stable
        "bands must be sorted and at most three bands can be queried"
    );

    let span = span!(Level::TRACE, "raster_stream_to_png_bytes");
    let _enter = span.enter();

    let raster_colorizer = match raster_colorizer {
        Some(colorizer) => colorizer,
        None => RasterColorizer::SingleBand {
            band: 0,
            band_colorizer: default_colorizer_gradient::<T>(),
        },
    };

    let required_bands = match raster_colorizer {
        RasterColorizer::MultiBand {
            red_band,
            green_band,
            blue_band,
            ..
        } => vec![red_band, green_band, blue_band],
        RasterColorizer::SingleBand { band, .. } => vec![band],
    };

    let band_positions = required_bands
        .iter()
        .filter_map(|band| {
            query_rect
                .attributes
                .as_slice()
                .iter()
                .position(|b| b == band)
        })
        .collect::<Vec<usize>>();

    if band_positions.len() != required_bands.len() {
        return Err(PngCreationError::ColorizerBandsMustBePresentInQuery {
            bands_present: query_rect.attributes.as_vec(),
            required_bands,
        })?;
    }

    let query_abort_trigger = query_ctx.abort_trigger()?;

    let x_query_resolution = query_rect.spatial_bounds.size_x() / f64::from(width);
    let y_query_resolution = query_rect.spatial_bounds.size_y() / f64::from(height);

    // build png
    let dim = [height as usize, width as usize];
    let query_geo_transform = GeoTransform::new(
        query_rect.spatial_bounds.upper_left(),
        x_query_resolution,
        -y_query_resolution, // TODO: negative, s.t. geo transform fits...
    );

    let tile_template: RasterTile2D<T> = RasterTile2D::new_without_offset(
        time.unwrap_or_default(),
        query_geo_transform,
        GridOrEmpty::from(EmptyGrid2D::new(dim.into())),
        CacheHint::max_duration(),
    );

    match raster_colorizer {
        RasterColorizer::SingleBand { band_colorizer, .. } => {
            single_band_colorizer_to_png_bytes(
                processor,
                query_rect,
                query_ctx,
                tile_template,
                width,
                height,
                band_colorizer,
                conn_closed,
                query_abort_trigger,
            )
            .await
        }
        RasterColorizer::MultiBand {
            rgb_params: rgba_params,
            ..
        } => {
            multi_band_colorizer_to_png_bytes(
                processor,
                query_rect,
                query_ctx,
                tile_template,
                width,
                height,
                rgba_params,
                band_positions,
                conn_closed,
                query_abort_trigger,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn single_band_colorizer_to_png_bytes<T: Pixel, C: QueryContext + 'static>(
    processor: Box<dyn RasterQueryProcessor<RasterType = T>>,
    query_rect: RasterQueryRectangle,
    query_ctx: C,
    tile_template: RasterTile2D<T>,
    width: u32,
    height: u32,
    colorizer: Colorizer,
    conn_closed: BoxFuture<'_, ()>,
    query_abort_trigger: QueryAbortTrigger,
) -> Result<(Vec<u8>, CacheHint)> {
    debug_assert_eq!(query_rect.attributes.count(), 1);

    let tile_stream = processor.query(query_rect.clone(), &query_ctx).await?;

    let output_tile = Box::pin(
        tile_stream.fold(Ok(tile_template), |raster, tile| async move {
            blit_tile(raster, tile)
        }),
    );

    let result = abortable_query_execution(output_tile, conn_closed, query_abort_trigger).await?;
    Ok((
        result.grid_array.to_png(width, height, &colorizer)?,
        result.cache_hint,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn multi_band_colorizer_to_png_bytes<T: Pixel, C: QueryContext + 'static>(
    processor: Box<dyn RasterQueryProcessor<RasterType = T>>,
    query_rect: RasterQueryRectangle,
    query_ctx: C,
    tile_template: RasterTile2D<T>,
    width: u32,
    height: u32,
    rgb_params: RgbParams,
    band_positions: Vec<usize>,
    conn_closed: BoxFuture<'_, ()>,
    query_abort_trigger: QueryAbortTrigger,
) -> Result<(Vec<u8>, CacheHint)> {
    let rgb_channel_count = query_rect.attributes.count() as usize;
    let no_data_color = rgb_params.no_data_color;
    let tile_template: RasterTile2D<u32> = tile_template.convert_data_type();
    let red_band_index = band_positions[0];
    let green_band_index = band_positions[1];
    let blue_band_index = band_positions[2];

    let tile_stream = processor.query(query_rect.clone(), &query_ctx).await?;

    let output_tile = Box::pin(tile_stream.try_chunks(rgb_channel_count).fold(
        Ok(tile_template),
        |raster2d, chunk| async move {
            let chunk = chunk.boxed_context(error::QueryDidNotProduceNextChunk)?;

            if chunk.len() != rgb_channel_count {
                return Err(PngCreationError::RgbChunkIsNotEnoughBands)?;
            }

            // TODO: spawn blocking task
            let rgb_tile = crate::util::spawn_blocking(move || {
                compute_rgb_tile(
                    [
                        &chunk[red_band_index],
                        &chunk[green_band_index],
                        &chunk[blue_band_index],
                    ],
                    &rgb_params,
                )
            })
            .await
            .boxed_context(error::UnexpectedComputational)?;

            blit_tile(raster2d, Ok(rgb_tile))
        },
    ));

    let result = abortable_query_execution(output_tile, conn_closed, query_abort_trigger).await?;
    Ok((
        result
            .grid_array
            .to_png_with_mapper(width, height, ColorMapper::Rgba, no_data_color)?,
        result.cache_hint,
    ))
}

fn blit_tile<T>(
    raster2d: Result<RasterTile2D<T>>,
    tile: Result<RasterTile2D<T>>,
) -> Result<RasterTile2D<T>>
where
    T: Pixel,
{
    let result: Result<RasterTile2D<T>> = match (raster2d, tile) {
        (Ok(mut raster2d), Ok(tile)) if tile.is_empty() => {
            raster2d.cache_hint.merge_with(&tile.cache_hint);
            Ok(raster2d)
        }
        (Ok(mut raster2d), Ok(tile)) => match raster2d.blit(tile) {
            Ok(()) => Ok(raster2d),
            Err(error) => Err(error.into()),
        },
        (Err(error), _) | (_, Err(error)) => Err(error),
    };

    result
}

/// Method to generate a default `Colorizer`.
///
/// # Panics
/// If T has no min max value
pub fn default_colorizer_gradient<T: Pixel>() -> Colorizer {
    Colorizer::linear_gradient(
        vec![
            (AsPrimitive::<f64>::as_(T::min_value()), RgbaColor::black())
                .try_into()
                .expect("a `Pixel` type's min value should not be NaN"),
            (AsPrimitive::<f64>::as_(T::max_value()), RgbaColor::white())
                .try_into()
                .expect("a `Pixel` type's max value should not be NaN"),
        ],
        RgbaColor::transparent(),
        RgbaColor::white(),
        RgbaColor::black(),
    )
    .expect("we know the min and max value are valid")
}

pub fn compute_rgb_tile<P: Pixel>(
    [red, green, blue]: [&RasterTile2D<P>; 3],
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

        let red = fit_to_interval_0_255(
            red_value.as_(),
            params.red_min,
            params.red_max,
            params.red_scale,
        );
        let green = fit_to_interval_0_255(
            green_value.as_(),
            params.green_min,
            params.green_max,
            params.green_scale,
        );
        let blue = fit_to_interval_0_255(
            blue_value.as_(),
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
        0, // always single band
        red.global_geo_transform,
        out_grid,
        red.cache_hint
            .merged(&green.cache_hint)
            .merged(&blue.cache_hint),
    )
}

#[derive(Debug, Snafu)]
#[snafu(context(suffix(false)), visibility(pub(crate)), module(error))]
pub enum PngCreationError {
    #[snafu(display(
        "Colorizer bands must be present in query: {required_bands:?} not in {bands_present:?}"
    ))]
    ColorizerBandsMustBePresentInQuery {
        bands_present: Vec<u32>,
        required_bands: Vec<u32>,
    },
    #[snafu(display("Query did not produce next chunk: {source}"))]
    QueryDidNotProduceNextChunk { source: Box<dyn ErrorSource> },
    #[snafu(display("RGB chunk is not enough bands"))]
    RgbChunkIsNotEnoughBands,
    #[snafu(display("Unexpected computational error"))]
    UnexpectedComputationalError { source: Box<dyn ErrorSource> },
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use geoengine_datatypes::{
        primitives::{BandSelection, Coordinate2D, SpatialPartition2D, SpatialResolution},
        raster::{RasterDataType, TilingSpecification},
        test_data,
        util::{assert_image_equals, test::TestDefault},
    };

    use crate::{
        engine::{MockQueryContext, RasterResultDescriptor},
        source::GdalSourceProcessor,
        util::gdal::create_ndvi_meta_data,
    };

    use super::*;

    #[test]
    fn fallback_colorizer_works() {
        // check that this does not panic
        default_colorizer_gradient::<u8>();
        default_colorizer_gradient::<u16>();
        default_colorizer_gradient::<u32>();
        default_colorizer_gradient::<u64>();
        default_colorizer_gradient::<i8>();
        default_colorizer_gradient::<i16>();
        default_colorizer_gradient::<i32>();
        default_colorizer_gradient::<i64>();
        default_colorizer_gradient::<f32>();
        default_colorizer_gradient::<f64>();
    }

    #[tokio::test]
    async fn png_from_stream() {
        let ctx = MockQueryContext::test_default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

        let gdal_source = GdalSourceProcessor::<u8> {
            result_descriptor: RasterResultDescriptor::with_datatype_and_num_bands(
                RasterDataType::U8,
                1,
            ),
            tiling_specification,
            meta_data: Box::new(create_ndvi_meta_data()),
            _phantom_data: PhantomData,
        };

        let query_partition =
            SpatialPartition2D::new((-10., 80.).into(), (50., 20.).into()).unwrap();

        let (image_bytes, _) = raster_stream_to_png_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_partition,
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::zero_point_one(),
                attributes: BandSelection::first(),
            },
            ctx,
            600,
            600,
            None,
            None,
            Box::pin(futures::future::pending()),
        )
        .await
        .unwrap();

        // geoengine_datatypes::util::test::save_test_bytes(&image_bytes, "png_from_stream.png");

        assert_image_equals(test_data!("raster/png/png_from_stream.png"), &image_bytes);
    }
}
