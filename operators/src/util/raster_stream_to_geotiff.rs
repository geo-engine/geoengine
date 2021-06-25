use core::slice;
use futures::StreamExt;
use gdal::{
    raster::{Buffer, GdalType},
    Driver,
};
use gdal_sys::{VSIFree, VSIGetMemFileBuffer};
use geoengine_datatypes::{
    primitives::{SpatialBounded, TimeInterval},
    raster::{Blit, GeoTransform, Grid2D, GridShape2D, GridSize, Pixel, RasterTile2D},
    spatial_reference::SpatialReference,
};
use std::{
    convert::TryInto,
    sync::mpsc::{Receiver, Sender},
};
use std::{ffi::CString, sync::mpsc};

use crate::util::Result;
use crate::{
    engine::{QueryContext, QueryRectangle, RasterQueryProcessor},
    error::Error,
};

pub async fn raster_stream_to_geotiff_bytes<T, C: QueryContext + 'static>(
    processor: Box<dyn RasterQueryProcessor<RasterType = T>>,
    query_rect: QueryRectangle,
    query_ctx: C,
    width: u32,
    height: u32,
    no_data_value: Option<f64>,
    spatial_reference: SpatialReference,
) -> Result<Vec<u8>>
where
    T: Pixel + GdalType,
{
    let file_name = format!("/vsimem/{}.tiff", uuid::Uuid::new_v4());
    let (tx, rx): (Sender<RasterTile2D<T>>, Receiver<RasterTile2D<T>>) = mpsc::channel();

    let file_name_clone = file_name.clone();
    let writer = tokio::task::spawn_blocking(move || {
        gdal_writer(
            &rx,
            &file_name_clone,
            query_rect,
            width,
            height,
            no_data_value,
            spatial_reference,
        )
    });

    let mut tile_stream = processor.raster_query(query_rect, &query_ctx).await?;

    while let Some(tile) = tile_stream.next().await {
        tx.send(tile?).map_err(|_| Error::ChannelSend)?;
    }

    drop(tx);

    writer.await??;

    // TODO: use higher level rust-gdal method when it is mapped
    let bytes = get_vsi_mem_file_bytes_and_free(&file_name);

    Ok(bytes)
}

fn gdal_writer<T: Pixel + GdalType>(
    rx: &Receiver<RasterTile2D<T>>,
    file_name: &str,
    query_rect: QueryRectangle,
    width: u32,
    height: u32,
    no_data_value: Option<f64>,
    spatial_reference: SpatialReference,
) -> Result<()> {
    let x_pixel_size = query_rect.bbox.size_x() / f64::from(width);
    let y_pixel_size = query_rect.bbox.size_y() / f64::from(height);

    let output_geo_transform =
        GeoTransform::new(query_rect.bbox.upper_left(), x_pixel_size, -y_pixel_size);
    let output_bounds = query_rect.bbox;

    let driver = Driver::get("GTiff")?;
    // TODO: "COMPRESS, DEFLATE" flags but rust-gdal doesn't support setting this yet(?)
    let mut dataset =
        driver.create_with_band_type::<T>(&file_name, width as isize, height as isize, 1)?;

    dataset.set_spatial_ref(&spatial_reference.try_into()?)?;
    dataset.set_geo_transform(&output_geo_transform.into())?;
    let mut band = dataset.rasterband(1)?;

    if let Some(no_data) = no_data_value {
        band.set_no_data_value(no_data)?;
    }

    while let Ok(tile) = rx.recv() {
        let tile_info = tile.tile_information();

        let tile_bounds = tile_info.spatial_bounds();

        let mat_tile = if output_bounds.contains_bbox(&tile_bounds) {
            tile.into_materialized_tile()
        } else {
            // extract relevant data from tile (intersection with output_bounds)

            // TODO: snap intersection to pixels?
            let intersection = output_bounds
                .intersection(&tile_bounds)
                .expect("tile must intersect with query");

            let shape: GridShape2D = [
                (intersection.size_y() / y_pixel_size).ceil() as usize,
                (intersection.size_x() / x_pixel_size).ceil() as usize,
            ]
            .into();

            let output_grid = Grid2D::new_filled(
                shape,
                no_data_value.map_or_else(T::zero, T::from_),
                no_data_value.map(T::from_),
            );

            let output_geo_transform = GeoTransform {
                origin_coordinate: intersection.upper_left(),
                x_pixel_size,
                y_pixel_size: -y_pixel_size,
            };

            let mut output_tile = RasterTile2D::new_without_offset(
                TimeInterval::default(),
                output_geo_transform,
                output_grid,
            )
            .into_materialized_tile();

            output_tile.blit(tile)?;

            output_tile
        };

        let upper_left = mat_tile.spatial_bounds().upper_left();

        let upper_left_pixel_x = ((upper_left.x - output_geo_transform.origin_coordinate.x)
            / x_pixel_size)
            .floor() as isize;
        let upper_left_pixel_y = ((output_geo_transform.origin_coordinate.y - upper_left.y)
            / y_pixel_size)
            .floor() as isize;
        let window = (upper_left_pixel_x, upper_left_pixel_y);

        let shape = mat_tile.grid_array.axis_size();
        let window_size = (shape[1], shape[0]);

        let buffer = Buffer::new(window_size, mat_tile.grid_array.data);

        band.write(window, window_size, &buffer)?;
    }

    Ok(())
}

/// copies the bytes of the vsi in-memory file with given `file_name` and frees the memory
fn get_vsi_mem_file_bytes_and_free(file_name: &str) -> Vec<u8> {
    let bytes = unsafe {
        let mut length: u64 = 0;
        let file_name_c = CString::new(file_name).expect("contains no 0 byte");
        let bytes = VSIGetMemFileBuffer(file_name_c.as_ptr(), &mut length, 1);

        let slice = slice::from_raw_parts(bytes, length as usize);
        let vec = slice.to_vec();

        VSIFree(bytes.cast::<std::ffi::c_void>());

        vec
    };
    bytes
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{BoundingBox2D, Coordinate2D, SpatialResolution, TimeInterval},
        raster::TilingSpecification,
    };

    use crate::{
        engine::MockQueryContext, source::GdalSourceProcessor, util::gdal::create_ndvi_meta_data,
    };

    use super::*;

    #[tokio::test]
    async fn geotiff_from_stream() {
        let ctx = MockQueryContext::default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

        let gdal_source = GdalSourceProcessor::<u8> {
            tiling_specification,
            meta_data: Box::new(create_ndvi_meta_data()),
            phantom_data: Default::default(),
        };

        let query_bbox = BoundingBox2D::new((-10., 20.).into(), (50., 80.).into()).unwrap();

        let bytes = raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            QueryRectangle {
                bbox: query_bbox,
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::zero_point_one(),
            },
            ctx,
            600,
            600,
            Some(0.),
            SpatialReference::epsg_4326(),
        )
        .await
        .unwrap();

        assert_eq!(
            include_bytes!("../../../operators/test-data/raster/geotiff_from_stream.tiff")
                as &[u8],
            bytes.as_slice()
        );
    }
}
