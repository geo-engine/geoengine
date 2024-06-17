use futures::{future::BoxFuture, StreamExt};
use geoengine_datatypes::{
    operations::image::{Colorizer, RasterColorizer, RgbaColor, ToPng},
    primitives::{AxisAlignedRectangle, CacheHint, RasterQueryRectangle, TimeInterval},
    raster::{Blit, ConvertDataType, EmptyGrid2D, GeoTransform, GridOrEmpty, Pixel, RasterTile2D},
};
use num_traits::AsPrimitive;
use snafu::ensure;
use std::convert::TryInto;
use tracing::{span, Level};

use crate::{
    engine::{QueryContext, QueryProcessor, RasterQueryProcessor},
    processing::{compute_rgb_tile, RgbParams},
};
use crate::{error, util::Result};

use super::abortable_query_execution;

#[allow(clippy::too_many_arguments)]
pub async fn raster_stream_to_png_bytes<T, C: QueryContext + 'static>(
    processor: Box<dyn RasterQueryProcessor<RasterType = T>>,
    query_rect: RasterQueryRectangle,
    mut query_ctx: C,
    width: u32,
    height: u32,
    time: Option<TimeInterval>,
    raster_colorizer: Option<RasterColorizer>,
    conn_closed: BoxFuture<'_, ()>,
) -> Result<(Vec<u8>, CacheHint)>
where
    T: Pixel,
{
    // TODO: support multi band colorizers
    ensure!(
        query_rect.attributes.count() == 1,
        crate::error::OperationDoesNotSupportMultiBandQueriesYet {
            operation: "raster_stream_to_png_bytes"
        }
    );

    let span = span!(Level::TRACE, "raster_stream_to_png_bytes");
    let _enter = span.enter();

    let query_abort_trigger = query_ctx.abort_trigger()?;

    let tile_stream = processor.query(query_rect.clone(), &query_ctx).await?;

    let x_query_resolution = query_rect.spatial_bounds.size_x() / f64::from(width);
    let y_query_resolution = query_rect.spatial_bounds.size_y() / f64::from(height);

    // build png
    let dim = [height as usize, width as usize];
    let query_geo_transform = GeoTransform::new(
        query_rect.spatial_bounds.upper_left(),
        x_query_resolution,
        -y_query_resolution, // TODO: negative, s.t. geo transform fits...
    );

    let tile_template: Result<RasterTile2D<T>> = Ok(RasterTile2D::new_without_offset(
        time.unwrap_or_default(),
        query_geo_transform,
        GridOrEmpty::from(EmptyGrid2D::new(dim.into())),
        CacheHint::max_duration(),
    ));

    let colorizer = match raster_colorizer {
        Some(RasterColorizer::MultiBand { .. }) => Colorizer::rgba(),
        Some(RasterColorizer::SingleBand {
            ref band_colorizer, ..
        }) => band_colorizer.clone(),
        None => default_colorizer_gradient::<T>()?,
    };

    match raster_colorizer {
        Some(RasterColorizer::MultiBand {
            red_min,
            red_max,
            red_scale,
            green_min,
            green_max,
            green_scale,
            blue_min,
            blue_max,
            blue_scale,
            ..
        }) => {
            let rgb_params = RgbParams {
                red_min,
                red_max,
                red_scale,
                green_min,
                green_max,
                green_scale,
                blue_min,
                blue_max,
                blue_scale,
            };
            const RGB_CHANNEL_COUNT: usize = 3;
            let tile_template: Result<RasterTile2D<u32>> =
                Ok(tile_template.unwrap().convert_data_type());
            let output_tile = Box::pin(tile_stream.chunks(RGB_CHANNEL_COUNT).fold(
                tile_template,
                |raster2d, chunk| {
                    if chunk.len() != RGB_CHANNEL_COUNT {
                        // if there are not exactly N tiles, it should mean the last tile was an error and the chunker ended prematurely
                        if let Some(Err(e)) = chunk.into_iter().last() {
                            return futures::future::err(e);
                        }
                        // if there is no error, the source did not produce all bands, which likely means a bug in an operator
                        unreachable!("the source did not produce all bands");
                    }

                    let mut ok_tiles: Vec<RasterTile2D<f64>> =
                        Vec::with_capacity(RGB_CHANNEL_COUNT);

                    for tile in chunk {
                        match tile {
                            Ok(tile) => ok_tiles.push(tile.convert_data_type()),
                            Err(e) => return futures::future::err(e),
                        }
                    }

                    let tuple: [RasterTile2D<f64>; RGB_CHANNEL_COUNT] = ok_tiles.try_into().expect(
                        "all chunks should be of the expected length because it was checked above",
                    );

                    let rgb_tile = compute_rgb_tile(tuple, &rgb_params);

                    blit_tile(raster2d, Ok(rgb_tile))
                },
            ));

            let result =
                abortable_query_execution(output_tile, conn_closed, query_abort_trigger).await?;
            Ok((
                result.grid_array.to_png(width, height, &colorizer)?,
                result.cache_hint,
            ))
        }
        _ => {
            let output_tile = Box::pin(tile_stream.fold(tile_template, blit_tile));

            let result =
                abortable_query_execution(output_tile, conn_closed, query_abort_trigger).await?;
            Ok((
                result.grid_array.to_png(width, height, &colorizer)?,
                result.cache_hint,
            ))
        }
    }
}

fn blit_tile<T>(
    raster2d: Result<RasterTile2D<T>>,
    tile: Result<RasterTile2D<T>>,
) -> futures::future::Ready<Result<RasterTile2D<T>>>
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

    match result {
        Ok(updated_raster2d) => futures::future::ok(updated_raster2d),
        Err(error) => futures::future::err(error),
    }
}

/// Method to generate a default `Colorizer`.
///
/// # Panics
/// If T has no min max value
pub fn default_colorizer_gradient<T: Pixel>() -> Result<Colorizer> {
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
    .map_err(error::Error::from)
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use geoengine_datatypes::{
        primitives::{BandSelection, Coordinate2D, SpatialPartition2D, SpatialResolution},
        raster::{RasterDataType, TilingSpecification},
        util::test::TestDefault,
    };

    use crate::{
        engine::{MockQueryContext, RasterResultDescriptor},
        source::GdalSourceProcessor,
        util::gdal::create_ndvi_meta_data,
    };

    use super::*;

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

        assert_eq!(
            include_bytes!("../../../test_data/raster/png/png_from_stream.png") as &[u8],
            image_bytes.as_slice()
        );
    }
}
