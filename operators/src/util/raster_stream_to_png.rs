use futures::{future::BoxFuture, StreamExt};
use geoengine_datatypes::{
    operations::image::{Colorizer, RgbaColor, ToPng},
    primitives::{
        AxisAlignedRectangle, RasterQueryRectangle, SpatialPartitioned, SpatialQuery, TimeInterval,
    },
    raster::{Blit, EmptyGrid2D, GeoTransform, GridOrEmpty, Pixel, RasterTile2D},
};
use num_traits::AsPrimitive;
use std::convert::TryInto;
use tracing::{span, Level};

use crate::engine::{QueryContext, QueryProcessor, RasterQueryProcessor};
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
    colorizer: Option<Colorizer>,
    conn_closed: BoxFuture<'_, ()>,
) -> Result<Vec<u8>>
where
    T: Pixel,
{
    let span = span!(Level::TRACE, "raster_stream_to_png_bytes");
    let _enter = span.enter();

    let query_abort_trigger = query_ctx.abort_trigger()?;

    let tile_stream = processor.query(query_rect, &query_ctx).await?;

    // build png
    let dim = [height as usize, width as usize];
    let query_geo_transform = query_rect.spatial_query().geo_transform;

    let output_geo_transform = GeoTransform::new(
        query_rect.spatial_query().spatial_partition().upper_left(),
        query_geo_transform.x_pixel_size(),
        query_geo_transform.y_pixel_size(),
    );

    let output_tile = Ok(RasterTile2D::new_without_offset(
        time.unwrap_or_default(),
        output_geo_transform,
        GridOrEmpty::from(EmptyGrid2D::new(dim.into())),
    ));

    let output_tile: BoxFuture<Result<RasterTile2D<T>>> =
        Box::pin(tile_stream.fold(output_tile, |raster2d, tile| {
            let result: Result<RasterTile2D<T>> = match (raster2d, tile) {
                (Ok(raster2d), Ok(tile)) if tile.is_empty() => Ok(raster2d),
                (Ok(mut raster2d), Ok(tile)) => match raster2d.blit(tile) {
                    Ok(_) => Ok(raster2d),
                    Err(error) => Err(error.into()),
                },
                (Err(error), _) | (_, Err(error)) => Err(error),
            };

            match result {
                Ok(updated_raster2d) => futures::future::ok(updated_raster2d),
                Err(error) => futures::future::err(error),
            }
        }));

    let result = abortable_query_execution(output_tile, conn_closed, query_abort_trigger).await?;

    let colorizer = colorizer.unwrap_or(default_colorizer_gradient::<T>()?);
    Ok(result.grid_array.to_png(width, height, &colorizer)?)
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
                .unwrap(),
            (AsPrimitive::<f64>::as_(T::max_value()), RgbaColor::white())
                .try_into()
                .unwrap(),
        ],
        RgbaColor::transparent(),
        RgbaColor::pink(),
    )
    .map_err(error::Error::from)
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use geoengine_datatypes::{
        primitives::{Coordinate2D, SpatialPartition2D, SpatialResolution},
        raster::TilingSpecification,
        util::test::TestDefault,
    };

    use crate::{
        engine::MockQueryContext, source::GdalSourceProcessor, util::gdal::create_ndvi_meta_data,
    };

    use super::*;

    #[tokio::test]
    async fn png_from_stream() {
        let ctx = MockQueryContext::test_default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

        let gdal_source = GdalSourceProcessor::<u8> {
            tiling_specification,
            meta_data: Box::new(create_ndvi_meta_data()),
            _phantom_data: PhantomData,
        };

        let query_partition =
            SpatialPartition2D::new((-10., 80.).into(), (50., 20.).into()).unwrap();

        let image_bytes = raster_stream_to_png_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                query_partition,
                SpatialResolution::zero_point_one(),
                Coordinate2D::new(0., 0.), // FIXME: check if this is correct here.
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            ),
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
