use super::abortable_query_execution;
use crate::engine::{QueryAbortTrigger, QueryContext, QueryProcessor, RasterQueryProcessor};
use crate::util::Result;
use futures::TryStreamExt;
use futures::{StreamExt, future::BoxFuture};
use geoengine_datatypes::error::{BoxedResultExt, ErrorSource};
use geoengine_datatypes::operations::image::{ColorMapper, RasterColorizer, RgbParams};
use geoengine_datatypes::raster::{FromIndexFn, GridIndexAccess, GridShapeAccess, RasterTile2D};
use geoengine_datatypes::{
    operations::image::{Colorizer, RgbaColor, ToPng},
    primitives::{CacheHint, RasterQueryRectangle, TimeInterval},
    raster::{ChangeGridBounds, GridBlit, GridBoundingBox2D, GridOrEmpty, Pixel},
};
use num_traits::AsPrimitive;
use snafu::Snafu;
use std::convert::TryInto;
use tracing::{Level, span};

/// # Panics
/// Panics if not three bands were queried.
#[allow(clippy::too_many_arguments)]
pub async fn raster_stream_to_png_bytes<T: Pixel, C: QueryContext + 'static>(
    processor: Box<dyn RasterQueryProcessor<RasterType = T>>,
    query_rect: RasterQueryRectangle,
    mut query_ctx: C,
    width: u32,
    height: u32,
    _time: Option<TimeInterval>,
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

    match raster_colorizer {
        RasterColorizer::SingleBand { band_colorizer, .. } => {
            single_band_colorizer_to_png_bytes(
                processor,
                query_rect,
                query_ctx,
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
    width: u32,
    height: u32,
    colorizer: Colorizer,
    conn_closed: BoxFuture<'_, ()>,
    query_abort_trigger: QueryAbortTrigger,
) -> Result<(Vec<u8>, CacheHint)> {
    debug_assert_eq!(query_rect.attributes.count(), 1);

    // the tile stream will allways produce tiles aligned to the tiling origin
    let tile_stream = processor.query(query_rect.clone(), &query_ctx).await?;
    let output_cache_hint = CacheHint::max_duration();

    let output_grid =
        GridOrEmpty::<GridBoundingBox2D, T>::new_empty_shape(query_rect.grid_bounds());

    let accu = Ok((output_grid, output_cache_hint));

    let output_tile: BoxFuture<Result<(GridOrEmpty<GridBoundingBox2D, T>, CacheHint)>> =
        Box::pin(tile_stream.fold(accu, |accu, tile| {
            let result: Result<(GridOrEmpty<GridBoundingBox2D, T>, CacheHint)> =
                blit_tile(accu, tile);

            match result {
                Ok(updated_raster2d) => futures::future::ok(updated_raster2d),
                Err(error) => futures::future::err(error),
            }
        }));

    let (result, ch) =
        abortable_query_execution(output_tile, conn_closed, query_abort_trigger).await?;
    Ok((result.unbounded().to_png(width, height, &colorizer)?, ch))
}

#[allow(clippy::too_many_arguments)]
async fn multi_band_colorizer_to_png_bytes<T: Pixel, C: QueryContext + 'static>(
    processor: Box<dyn RasterQueryProcessor<RasterType = T>>,
    query_rect: RasterQueryRectangle,
    query_ctx: C,
    width: u32,
    height: u32,
    rgb_params: RgbParams,
    band_positions: Vec<usize>,
    conn_closed: BoxFuture<'_, ()>,
    query_abort_trigger: QueryAbortTrigger,
) -> Result<(Vec<u8>, CacheHint)> {
    let rgb_channel_count = query_rect.attributes.count() as usize;
    let no_data_color = rgb_params.no_data_color;
    let tile_template: GridOrEmpty<GridBoundingBox2D, u32> =
        GridOrEmpty::new_empty_shape(query_rect.grid_bounds());
    let output_cache_hint = CacheHint::max_duration();
    let red_band_index = band_positions[0];
    let green_band_index = band_positions[1];
    let blue_band_index = band_positions[2];

    let tile_stream = processor.query(query_rect.clone(), &query_ctx).await?;
    let accu = Ok((tile_template, output_cache_hint));

    let output_tile = Box::pin(tile_stream.try_chunks(rgb_channel_count).fold(
        accu,
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

    let (result, ch) =
        abortable_query_execution(output_tile, conn_closed, query_abort_trigger).await?;
    Ok((
        result
            .unbounded()
            .to_png_with_mapper(width, height, ColorMapper::Rgba, no_data_color)?,
        ch,
    ))
}

fn blit_tile<T>(
    accu: Result<(GridOrEmpty<GridBoundingBox2D, T>, CacheHint)>,
    tile: Result<RasterTile2D<T>>,
) -> Result<(GridOrEmpty<GridBoundingBox2D, T>, CacheHint)>
where
    T: Pixel,
{
    let result: Result<(GridOrEmpty<GridBoundingBox2D, T>, CacheHint)> = match (accu, tile) {
        (Ok((empty_grid, ch)), Ok(tile)) if tile.is_empty() => Ok((empty_grid, ch)),
        (Ok((mut grid, mut ch)), Ok(tile)) => {
            ch.merge_with(&tile.cache_hint);
            grid.grid_blit_from(&tile.into_inner_positioned_grid());
            Ok((grid, ch))
        }
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

    use crate::{
        engine::MockQueryContext, source::GdalSourceProcessor, util::gdal::create_ndvi_meta_data,
    };
    use geoengine_datatypes::primitives::{DateTime, TimeInstance};
    use geoengine_datatypes::{
        primitives::BandSelection,
        raster::TilingSpecification,
        test_data,
        util::{assert_image_equals, test::TestDefault},
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
        let tiling_specification = TilingSpecification::new([600, 600].into());

        let meta_data = create_ndvi_meta_data();

        let gdal_source = GdalSourceProcessor::<u8> {
            produced_result_descriptor: meta_data.result_descriptor.clone(),
            tiling_specification,
            overview_level: 0,
            meta_data: Box::new(meta_data),
            original_resolution_spatial_grid: None,
            _phantom_data: PhantomData,
        };

        let query = RasterQueryRectangle::new_with_grid_bounds(
            GridBoundingBox2D::new([-800, -100], [-199, 499]).unwrap(),
            TimeInstance::from(DateTime::new_utc(2014, 1, 1, 0, 0, 0)).into(),
            BandSelection::first(),
        );

        let (image_bytes, _) = raster_stream_to_png_bytes(
            gdal_source.boxed(),
            query,
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
