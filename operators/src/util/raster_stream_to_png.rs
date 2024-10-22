use futures::{future::BoxFuture, StreamExt};
use geoengine_datatypes::{
    operations::image::{Colorizer, RgbaColor, ToPng},
    primitives::{CacheHint, RasterQueryRectangle, TimeInterval},
    raster::{ChangeGridBounds, GridBlit, GridBoundingBox2D, GridOrEmpty, Pixel},
};
use num_traits::AsPrimitive;
use snafu::ensure;
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
    _time: Option<TimeInterval>,
    colorizer: Option<Colorizer>,
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

    // the tile stream will allways produce tiles aligned to the tiling origin
    let tile_stream = processor.query(query_rect.clone(), &query_ctx).await?;

    let output_grid = GridOrEmpty::<GridBoundingBox2D, T>::new_empty_shape(
        query_rect.spatial_query.grid_bounds(),
    );
    let output_cache_hint = CacheHint::max_duration();
    let accu = Ok((output_grid, output_cache_hint));

    let output_tile: BoxFuture<Result<(GridOrEmpty<GridBoundingBox2D, T>, CacheHint)>> =
        Box::pin(tile_stream.fold(accu, |accu, tile| {
            let result: Result<(GridOrEmpty<GridBoundingBox2D, T>, CacheHint)> = match (accu, tile)
            {
                (Ok((empty_grid, ch)), Ok(tile)) if tile.is_empty() => Ok((empty_grid, ch)),
                (Ok((mut grid, mut ch)), Ok(tile)) => {
                    ch.merge_with(&tile.cache_hint);
                    grid.grid_blit_from(&tile.into_inner_positioned_grid());
                    Ok((grid, ch))
                }
                (Err(error), _) | (_, Err(error)) => Err(error),
            };

            match result {
                Ok(updated_raster2d) => futures::future::ok(updated_raster2d),
                Err(error) => futures::future::err(error),
            }
        }));

    let (result, cache_hint) =
        abortable_query_execution(output_tile, conn_closed, query_abort_trigger).await?;

    let colorizer = colorizer.unwrap_or(default_colorizer_gradient::<T>()?);
    Ok((
        result.unbounded().to_png(width, height, &colorizer)?,
        cache_hint,
    ))
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

    use crate::{
        engine::MockQueryContext, source::GdalSourceProcessor, util::gdal::create_ndvi_meta_data,
    };
    use geoengine_datatypes::{
        primitives::{BandSelection, DateTime, TimeInstance},
        raster::TilingSpecification,
        util::test::TestDefault,
    };

    use super::*;

    #[tokio::test]
    async fn png_from_stream() {
        let ctx = MockQueryContext::test_default();
        let tiling_specification = TilingSpecification::new([600, 600].into());

        let meta_data = create_ndvi_meta_data();

        let gdal_source = GdalSourceProcessor::<u8> {
            result_descriptor: meta_data.result_descriptor.clone(),
            tiling_specification,
            overview_level: 0,
            meta_data: Box::new(meta_data),
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

        assert_eq!(
            include_bytes!("../../../test_data/raster/png/png_from_stream.png") as &[u8],
            image_bytes.as_slice()
        );
    }
}
