use futures::StreamExt;
use geoengine_datatypes::{
    operations::image::{Colorizer, RgbaColor, ToPng},
    primitives::{AxisAlignedRectangle, TimeInterval},
    raster::{Blit, EmptyGrid2D, GeoTransform, Grid2D, Pixel, RasterTile2D},
};
use num_traits::AsPrimitive;
use std::convert::TryInto;

use crate::engine::{QueryContext, QueryProcessor, RasterQueryProcessor, RasterQueryRectangle};
use crate::{error, util::Result};

#[allow(clippy::too_many_arguments)]
pub async fn raster_stream_to_png_bytes<T, C: QueryContext>(
    processor: Box<dyn RasterQueryProcessor<RasterType = T>>,
    query_rect: RasterQueryRectangle,
    query_ctx: C,
    width: u32,
    height: u32,
    time: Option<TimeInterval>,
    colorizer: Option<Colorizer>,
    no_data_value: Option<T>,
) -> Result<Vec<u8>>
where
    T: Pixel,
{
    let colorizer = colorizer.unwrap_or(default_colorizer_gradient::<T>()?);

    let tile_stream = processor.query(query_rect, &query_ctx).await?;

    let x_query_resolution = query_rect.spatial_bounds.size_x() / f64::from(width);
    let y_query_resolution = query_rect.spatial_bounds.size_y() / f64::from(height);

    // build png
    let dim = [height as usize, width as usize];
    let query_geo_transform = GeoTransform::new(
        query_rect.spatial_bounds.upper_left(),
        x_query_resolution,
        -y_query_resolution, // TODO: negative, s.t. geo transform fits...
    );

    let output_grid = if let Some(no_data) = no_data_value {
        EmptyGrid2D::new(dim.into(), no_data).into()
    } else {
        Grid2D::new_filled(
            dim.into(),
            no_data_value.unwrap_or_else(T::zero),
            no_data_value,
        )
    };
    let output_tile = Ok(RasterTile2D::new_without_offset(
        time.unwrap_or_default(),
        query_geo_transform,
        output_grid,
    ));

    let output_tile = tile_stream
        .fold(output_tile, |raster2d, tile| {
            let result: Result<RasterTile2D<T>> = match (raster2d, tile) {
                (Ok(raster2d), Ok(tile)) if tile.is_empty() => Ok(raster2d),
                (Ok(raster2d), Ok(tile)) => {
                    let mut mat_raster2d = raster2d.into_materialized_tile();
                    match mat_raster2d.blit(tile) {
                        Ok(_) => Ok(mat_raster2d.into()),
                        Err(error) => Err(error.into()),
                    }
                }
                (Err(error), _) | (_, Err(error)) => Err(error),
            };

            match result {
                Ok(updated_raster2d) => futures::future::ok(updated_raster2d),
                Err(error) => futures::future::err(error),
            }
        })
        .await?;

    Ok(output_tile.grid_array.to_png(width, height, &colorizer)?)
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
    use geoengine_datatypes::{
        primitives::{Coordinate2D, SpatialPartition2D, SpatialResolution},
        raster::TilingSpecification,
    };

    use crate::{
        engine::MockQueryContext, source::GdalSourceProcessor, util::gdal::create_ndvi_meta_data,
    };

    use super::*;

    #[tokio::test]
    async fn png_from_stream() {
        let ctx = MockQueryContext::default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

        let gdal_source = GdalSourceProcessor::<u8> {
            tiling_specification,
            meta_data: Box::new(create_ndvi_meta_data()),
            phantom_data: Default::default(),
        };

        let query_partition =
            SpatialPartition2D::new((-10., 80.).into(), (50., 20.).into()).unwrap();

        let image_bytes = raster_stream_to_png_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_partition,
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::zero_point_one(),
            },
            ctx,
            600,
            600,
            None,
            None,
            Some(0),
        )
        .await
        .unwrap();

        assert_eq!(
            include_bytes!("../../../test_data/raster/png/png_from_stream.png") as &[u8],
            image_bytes.as_slice()
        );
    }
}
