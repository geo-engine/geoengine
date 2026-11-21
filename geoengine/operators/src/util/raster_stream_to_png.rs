use super::abortable_query_execution;
use crate::engine::RasterResultDescriptor;
use crate::engine::{BoxRasterQueryProcessor, QueryContext, QueryProcessorExt};
use crate::util::{Result, spawn_query_task};
use futures::TryStreamExt;
use futures::{StreamExt, future::BoxFuture};
use geoengine_datatypes::error::{BoxedResultExt, ErrorSource};
use geoengine_datatypes::operations::image::{ColorMapper, RasterColorizer, RgbParams};
use geoengine_datatypes::primitives::Measurement;
use geoengine_datatypes::raster::{
    FromIndexFn, GridIndexAccess, GridShapeAccess, RasterDataType, RasterTile2D,
};
use geoengine_datatypes::{
    operations::image::{Colorizer, RgbaColor, ToPng},
    primitives::{CacheHint, RasterQueryRectangle, TimeInterval},
    raster::{ChangeGridBounds, GridBlit, GridBoundingBox2D, GridOrEmpty, Pixel},
};
use num_traits::AsPrimitive;
use snafu::Snafu;
use std::convert::TryInto;
use tracing::{Instrument, Level, span};
use typed_floats::tf64::NonNaN;

/// # Panics
/// Panics if not three bands were queried.
#[allow(clippy::too_many_arguments)]
pub async fn raster_stream_to_png_bytes<T: Pixel, C: QueryContext + 'static>(
    processor: BoxRasterQueryProcessor<T>,
    query_rect: RasterQueryRectangle,
    mut query_ctx: C,
    width: u32,
    height: u32,
    _time: Option<TimeInterval>,
    raster_colorizer: Option<RasterColorizer>,
    conn_closed: BoxFuture<'_, ()>,
) -> Result<(Vec<u8>, CacheHint)> {
    // the tiles of a position arrive in ascending band order, because the band selection is always sorted
    debug_assert!(
        query_rect.attributes().count() <= 3 && query_rect.attributes().as_slice().is_sorted(),
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
                .attributes()
                .as_slice()
                .iter()
                .position(|b| b == band)
        })
        .collect::<Vec<usize>>();

    if band_positions.len() != required_bands.len() {
        Err(PngCreationError::ColorizerBandsMustBePresentInQuery {
            bands_present: query_rect.attributes().as_vec(),
            required_bands,
        })?;
    }

    let query_abort_trigger = query_ctx.abort_trigger()?;
    let runtime = query_ctx.processing_runtime();

    // query, blit and encode run as one task, so the caller's runtime only awaits the bytes
    let work = async move {
        match raster_colorizer {
            RasterColorizer::SingleBand { band_colorizer, .. } => {
                single_band_colorizer_to_png_bytes(
                    processor,
                    query_rect,
                    query_ctx,
                    width,
                    height,
                    band_colorizer,
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
                )
                .await
            }
        }
    };

    abortable_query_execution(
        spawn_query_task(runtime, work),
        conn_closed,
        query_abort_trigger,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn single_band_colorizer_to_png_bytes<T: Pixel, C: QueryContext + 'static>(
    processor: BoxRasterQueryProcessor<T>,
    query_rect: RasterQueryRectangle,
    query_ctx: C,
    width: u32,
    height: u32,
    colorizer: Colorizer,
) -> Result<(Vec<u8>, CacheHint)> {
    debug_assert_eq!(query_rect.attributes().count(), 1);

    // the tile stream will allways produce tiles aligned to the tiling origin
    // with TRACE spans (debug builds only), the stream's spans keep `query_plan` open until it ends
    let tile_stream = processor
        .query_into_owned_boxed_stream(query_rect.clone(), Box::new(query_ctx))
        .instrument(tracing::debug_span!("query_plan"))
        .await?;
    let output_cache_hint = CacheHint::max_duration();

    let output_grid =
        GridOrEmpty::<GridBoundingBox2D, T>::new_empty_shape(query_rect.spatial_bounds());

    let accu = Ok((output_grid, output_cache_hint));

    let output_tile: BoxFuture<Result<(GridOrEmpty<GridBoundingBox2D, T>, CacheHint)>> =
        Box::pin(tile_stream.fold(accu, |accu, tile| {
            #[cfg(debug_assertions)]
            if let Err(er) = tile.as_ref() {
                tracing::debug!("Error tile passed to single_[..]_to_png: {er}");
            }

            let result: Result<(GridOrEmpty<GridBoundingBox2D, T>, CacheHint)> =
                blit_tile(accu, tile);

            match result {
                Ok(updated_raster2d) => futures::future::ok(updated_raster2d),
                Err(error) => futures::future::err(error),
            }
        }));

    let (result, ch) = output_tile.await?;
    let png = crate::util::spawn_blocking(move || {
        let _span = tracing::debug_span!("png_encode").entered();
        result.unbounded().to_png(width, height, &colorizer)
    })
    .await
    .boxed_context(error::UnexpectedComputational)??;
    Ok((png, ch))
}

#[allow(clippy::too_many_arguments)]
async fn multi_band_colorizer_to_png_bytes<T: Pixel, C: QueryContext + 'static>(
    processor: BoxRasterQueryProcessor<T>,
    query_rect: RasterQueryRectangle,
    query_ctx: C,
    width: u32,
    height: u32,
    rgb_params: RgbParams,
    band_positions: Vec<usize>,
) -> Result<(Vec<u8>, CacheHint)> {
    let rgb_channel_count = query_rect.attributes().count() as usize;
    let no_data_color = rgb_params.no_data_color;
    let tile_template: GridOrEmpty<GridBoundingBox2D, u32> =
        GridOrEmpty::new_empty_shape(query_rect.spatial_bounds());
    let output_cache_hint = CacheHint::max_duration();
    let red_band_index = band_positions[0];
    let green_band_index = band_positions[1];
    let blue_band_index = band_positions[2];

    let tile_stream = processor
        .query_into_owned_boxed_stream(query_rect.clone(), Box::new(query_ctx))
        .instrument(tracing::debug_span!("query_plan"))
        .await?;
    let accu = Ok((tile_template, output_cache_hint));

    let output_tile = Box::pin(tile_stream.try_chunks(rgb_channel_count).fold(
        accu,
        |raster2d, chunk| async move {
            #[cfg(debug_assertions)]
            if let Err(er) = chunk.as_ref() {
                tracing::debug!("Error chunk passed to multi_[..]_to_png: {er}");
            }

            let chunk = chunk.boxed_context(error::QueryDidNotProduceNextChunk)?;

            if chunk.len() != rgb_channel_count {
                Err(PngCreationError::RgbChunkIsNotEnoughBands)?;
            }

            let rgb_tile = crate::util::spawn_blocking(move || {
                let _span = tracing::debug_span!("rgb_tile").entered();
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

    let (result, ch) = output_tile.await?;
    let png = crate::util::spawn_blocking(move || {
        let _span = tracing::debug_span!("png_encode").entered();
        result
            .unbounded()
            .to_png_with_mapper(width, height, ColorMapper::Rgba, no_data_color)
    })
    .await
    .boxed_context(error::UnexpectedComputational)??;
    Ok((png, ch))
}

fn blit_tile<T>(
    accu: Result<(GridOrEmpty<GridBoundingBox2D, T>, CacheHint)>,
    tile: Result<RasterTile2D<T>>,
) -> Result<(GridOrEmpty<GridBoundingBox2D, T>, CacheHint)>
where
    T: Pixel,
{
    let _span = tracing::debug_span!("blit").entered();
    let result: Result<(GridOrEmpty<GridBoundingBox2D, T>, CacheHint)> = match (accu, tile) {
        (Ok((empty_grid, mut ch)), Ok(tile)) if tile.is_empty() => {
            ch.merge_with(&tile.cache_hint);
            Ok((empty_grid, ch))
        }
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

/// The colorizer for rendering the first band of a raster with `result_descriptor` without a
/// given one: a palette with one color per class for a classification, otherwise a gradient from
/// black to white over the range of the data type (see [`default_colorizer_gradient`]).
pub fn default_raster_colorizer(result_descriptor: &RasterResultDescriptor) -> RasterColorizer {
    let measurement = result_descriptor
        .bands
        .iter()
        .next()
        .map(|band| &band.measurement);
    let band_colorizer = match measurement {
        Some(Measurement::Classification(classification)) if !classification.classes.is_empty() => {
            class_palette(classification.classes.keys().copied())
        }
        _ => match result_descriptor.data_type {
            RasterDataType::U8 => default_colorizer_gradient::<u8>(),
            RasterDataType::U16 => default_colorizer_gradient::<u16>(),
            RasterDataType::U32 => default_colorizer_gradient::<u32>(),
            RasterDataType::U64 => default_colorizer_gradient::<u64>(),
            RasterDataType::I8 => default_colorizer_gradient::<i8>(),
            RasterDataType::I16 => default_colorizer_gradient::<i16>(),
            RasterDataType::I32 => default_colorizer_gradient::<i32>(),
            RasterDataType::I64 => default_colorizer_gradient::<i64>(),
            RasterDataType::F32 => default_colorizer_gradient::<f32>(),
            RasterDataType::F64 => default_colorizer_gradient::<f64>(),
        },
    };
    RasterColorizer::SingleBand {
        band: 0,
        band_colorizer,
    }
}

/// A palette with a distinct color for each of `classes`: hues a golden angle apart, so that
/// neighbouring classes differ clearly
fn class_palette(classes: impl Iterator<Item = u8>) -> Colorizer {
    const GOLDEN_ANGLE: f64 = 137.507_764;
    let colors = classes
        .enumerate()
        .map(|(index, class)| {
            let hue = (index as f64 * GOLDEN_ANGLE) % 360.;
            let [red, green, blue] = hsv_to_rgb(hue, 0.65, 0.9);
            (
                NonNaN::new(f64::from(class)).expect("a class is a number"),
                RgbaColor::new(red, green, blue, 255),
            )
        })
        .collect();
    Colorizer::palette(colors, RgbaColor::transparent(), RgbaColor::black())
        .expect("the classes of a classification are at most 256 colors")
}

/// The RGB color of `hue` in degrees, `saturation` and `value` in `[0, 1]`
fn hsv_to_rgb(hue: f64, saturation: f64, value: f64) -> [u8; 3] {
    let chroma = value * saturation;
    let sector = hue / 60.;
    let x = chroma * (1. - (sector % 2. - 1.).abs());
    let (red, green, blue) = match sector as u8 {
        0 => (chroma, x, 0.),
        1 => (x, chroma, 0.),
        2 => (0., chroma, x),
        3 => (0., x, chroma),
        4 => (x, 0., chroma),
        _ => (chroma, 0., x),
    };
    let to_u8 = |component: f64| ((component + value - chroma) * 255.).round() as u8;
    [to_u8(red), to_u8(green), to_u8(blue)]
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

    use crate::engine::{
        MockExecutionContext, QueryProcessor, RasterQueryProcessor, RasterResultDescriptor,
    };
    use crate::{source::GdalSourceProcessor, util::gdal::create_ndvi_meta_data};
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use geoengine_datatypes::primitives::{DateTime, TimeInstance};
    use geoengine_datatypes::{
        primitives::BandSelection,
        raster::TilingSpecification,
        test_data,
        util::{assert_image_equals, test::TestDefault},
    };
    use std::sync::{Arc, Mutex};

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
        let exe_ctx = MockExecutionContext::test_default();
        let ctx = exe_ctx.mock_query_context_test_default();
        let tiling_specification = TilingSpecification::new([600, 600].into());

        let meta_data = create_ndvi_meta_data();

        let gdal_source = GdalSourceProcessor::<u8>::new_no_overview(
            meta_data.result_descriptor.clone(),
            tiling_specification,
            Box::new(meta_data),
        );

        let query = RasterQueryRectangle::new(
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

    /// Records the name of the thread that creates the query and polls each of its tiles.
    struct ThreadRecording {
        inner: BoxRasterQueryProcessor<u8>,
        threads: Arc<Mutex<Vec<Option<String>>>>,
    }

    fn record_thread(threads: &Mutex<Vec<Option<String>>>) {
        threads
            .lock()
            .unwrap()
            .push(std::thread::current().name().map(ToOwned::to_owned));
    }

    #[async_trait]
    impl QueryProcessor for ThreadRecording {
        type Output = RasterTile2D<u8>;
        type SpatialBounds = GridBoundingBox2D;
        type Selection = BandSelection;
        type ResultDescription = RasterResultDescriptor;

        async fn _query<'a>(
            &'a self,
            query: RasterQueryRectangle,
            ctx: &'a dyn QueryContext,
        ) -> Result<BoxStream<'a, Result<RasterTile2D<u8>>>> {
            record_thread(&self.threads);
            let threads = self.threads.clone();
            Ok(self
                .inner
                .query(query, ctx)
                .await?
                .inspect(move |_| record_thread(&threads))
                .boxed())
        }

        fn result_descriptor(&self) -> &RasterResultDescriptor {
            self.inner.result_descriptor()
        }
    }

    #[async_trait]
    impl RasterQueryProcessor for ThreadRecording {
        type RasterType = u8;

        async fn _time_query<'a>(
            &'a self,
            query: TimeInterval,
            ctx: &'a dyn QueryContext,
        ) -> Result<BoxStream<'a, Result<TimeInterval>>> {
            self.inner.time_query(query, ctx).await
        }
    }

    #[tokio::test]
    async fn png_from_stream_runs_on_the_processing_runtime() {
        let processing = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("processing-test")
            .enable_all()
            .build()
            .unwrap();

        let exe_ctx = MockExecutionContext::test_default();
        let mut ctx = exe_ctx.mock_query_context_test_default();
        ctx.processing_runtime = processing.handle().clone();
        let tiling_specification = TilingSpecification::new([600, 600].into());

        let meta_data = create_ndvi_meta_data();

        let gdal_source = GdalSourceProcessor::<u8>::new_no_overview(
            meta_data.result_descriptor.clone(),
            tiling_specification,
            Box::new(meta_data),
        );

        let threads = Arc::new(Mutex::new(Vec::new()));
        let processor = ThreadRecording {
            inner: gdal_source.boxed(),
            threads: threads.clone(),
        };

        let query = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-800, -100], [-199, 499]).unwrap(),
            TimeInstance::from(DateTime::new_utc(2014, 1, 1, 0, 0, 0)).into(),
            BandSelection::first(),
        );

        let (image_bytes, _) = raster_stream_to_png_bytes(
            processor.boxed(),
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

        assert_image_equals(test_data!("raster/png/png_from_stream.png"), &image_bytes);

        let threads = threads.lock().unwrap().clone();
        assert!(
            threads.len() > 1,
            "the query and at least one tile: {threads:?}"
        );
        assert!(
            threads
                .iter()
                .all(|thread| thread.as_deref() == Some("processing-test")),
            "{threads:?}"
        );

        processing.shutdown_background();
    }

    #[test]
    fn it_renders_classes_with_a_palette_and_values_with_a_gradient() {
        use crate::engine::{RasterBandDescriptor, RasterBandDescriptors};
        use crate::util::raster_stream_to_png::default_raster_colorizer;
        use geoengine_datatypes::operations::image::{Colorizer, RasterColorizer};
        use geoengine_datatypes::primitives::Measurement;
        use geoengine_datatypes::raster::RasterDataType;

        let descriptor = |data_type, measurement| RasterResultDescriptor {
            data_type,
            bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                "band".into(),
                measurement,
            )])
            .unwrap(),
            ..create_ndvi_meta_data().result_descriptor
        };

        let classes = default_raster_colorizer(&descriptor(
            RasterDataType::U8,
            Measurement::classification(
                "land cover".into(),
                [
                    (1, "forest".into()),
                    (2, "water".into()),
                    (7, "city".into()),
                ]
                .into(),
            ),
        ));
        let RasterColorizer::SingleBand {
            band: 0,
            band_colorizer: Colorizer::Palette { colors, .. },
        } = classes
        else {
            panic!("a classification should get a palette, got {classes:?}");
        };
        let mut palette_classes: Vec<f64> = colors
            .into_inner()
            .keys()
            .map(typed_floats::tf64::NonNaN::get)
            .collect();
        palette_classes.sort_by(f64::total_cmp);
        assert_eq!(palette_classes, vec![1., 2., 7.]);

        let values = default_raster_colorizer(&descriptor(
            RasterDataType::U16,
            Measurement::continuous("ndvi".into(), None),
        ));
        assert_eq!(
            values,
            RasterColorizer::SingleBand {
                band: 0,
                band_colorizer: super::default_colorizer_gradient::<u16>(),
            }
        );
    }
}
