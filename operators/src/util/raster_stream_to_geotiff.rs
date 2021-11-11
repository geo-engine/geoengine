use futures::StreamExt;
use gdal::raster::{Buffer, GdalType, RasterCreationOption};
use gdal::Driver;
use geoengine_datatypes::primitives::{AxisAlignedRectangle, SpatialPartitioned};
use geoengine_datatypes::raster::{
    ChangeGridBounds, GeoTransform, Grid2D, GridBlit, GridIdx, GridSize, Pixel, RasterTile2D,
};
use geoengine_datatypes::spatial_reference::SpatialReference;
use std::convert::TryInto;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender};
use std::{path::Path, sync::mpsc};

use crate::{engine::RasterQueryRectangle, util::Result};
use crate::{
    engine::{QueryContext, RasterQueryProcessor},
    error::Error,
};

pub async fn raster_stream_to_geotiff_bytes<T, C: QueryContext + 'static>(
    processor: Box<dyn RasterQueryProcessor<RasterType = T>>,
    query_rect: RasterQueryRectangle,
    query_ctx: C,
    no_data_value: Option<f64>,
    spatial_reference: SpatialReference,
    tile_limit: Option<usize>,
    as_cog: bool,
) -> Result<Vec<u8>>
where
    T: Pixel + GdalType,
{
    let file_path = PathBuf::from(format!("/vsimem/{}.tiff", uuid::Uuid::new_v4()));

    raster_stream_to_geotiff(
        &file_path,
        processor,
        query_rect,
        query_ctx,
        no_data_value,
        spatial_reference,
        tile_limit,
        as_cog,
    )
    .await?;

    let bytes = gdal::vsi::get_vsi_mem_file_bytes_owned(file_path)?;

    Ok(bytes)
}

#[allow(clippy::too_many_arguments)] // TODO: refactor parameters
pub async fn raster_stream_to_geotiff<T, C: QueryContext + 'static>(
    file_path: &Path,
    processor: Box<dyn RasterQueryProcessor<RasterType = T>>,
    query_rect: RasterQueryRectangle,
    query_ctx: C,
    no_data_value: Option<f64>,
    spatial_reference: SpatialReference,
    tile_limit: Option<usize>,
    as_cog: bool,
) -> Result<()>
where
    T: Pixel + GdalType,
{
    // TODO: create file path if it doesn't exist
    // TODO: handle streams with multiple time steps correctly

    let (tx, rx): (Sender<RasterTile2D<T>>, Receiver<RasterTile2D<T>>) = mpsc::channel();

    let file_path_clone = file_path.to_owned();
    let writer = tokio::task::spawn_blocking(move || {
        gdal_writer(
            &rx,
            &file_path_clone,
            query_rect,
            no_data_value,
            spatial_reference,
            as_cog,
        )
    });

    let mut tile_stream = processor.raster_query(query_rect, &query_ctx).await?;

    let mut tile_count = 0;
    while let Some(tile) = tile_stream.next().await {
        // TODO: more descriptive error. This error occured when a file could not be created...
        tx.send(tile?).map_err(|_| Error::ChannelSend)?;

        tile_count += 1;

        if tile_limit.map_or_else(|| false, |limit| tile_count > limit) {
            return Err(Error::TileLimitExceeded {
                limit: tile_limit.expect("limit exist because it is exceeded"),
            });
        }
    }

    drop(tx);

    writer.await??;

    Ok(())
}

#[allow(clippy::too_many_lines)]
fn gdal_writer<T: Pixel + GdalType>(
    rx: &Receiver<RasterTile2D<T>>,
    file_path: &Path,
    query_rect: RasterQueryRectangle,
    no_data_value: Option<f64>,
    spatial_reference: SpatialReference,
    as_cog: bool,
) -> Result<()> {
    const INTERMEDIATE_FILE_SUFFIX: &str = "GEO-ENGINE-TMP";

    let intermediate_file_path = file_path.with_extension(INTERMEDIATE_FILE_SUFFIX);
    let output_file_path = file_path;

    let x_pixel_size = query_rect.spatial_resolution.x;
    let y_pixel_size = query_rect.spatial_resolution.y;
    let width = (query_rect.spatial_bounds.size_x() / x_pixel_size).ceil() as u32;
    let height = (query_rect.spatial_bounds.size_y() / y_pixel_size).ceil() as u32;

    let output_geo_transform = GeoTransform::new(
        query_rect.spatial_bounds.upper_left(),
        x_pixel_size,
        -y_pixel_size,
    );
    let output_bounds = query_rect.spatial_bounds;

    let driver = Driver::get("GTiff")?;
    let mut options = vec![
        RasterCreationOption {
            key: "COMPRESS",
            value: "LZW",
        },
        RasterCreationOption {
            key: "TILED",
            value: "YES",
        },
    ];
    if as_cog {
        // COGs require a block size of 512x512, so we enforce it now so that we do the work only once.
        options.push(RasterCreationOption {
            key: "BLOCKXSIZE",
            value: "512",
        });
        options.push(RasterCreationOption {
            key: "BLOCKYSIZE",
            value: "512",
        });
    }

    let mut dataset = driver.create_with_band_type_with_options::<T, _>(
        if as_cog {
            &intermediate_file_path
        } else {
            output_file_path
        },
        width as isize,
        height as isize,
        1,
        &options,
    )?;

    dataset.set_spatial_ref(&spatial_reference.try_into()?)?;
    dataset.set_geo_transform(&output_geo_transform.into())?;
    let mut band = dataset.rasterband(1)?;

    if let Some(no_data) = no_data_value {
        band.set_no_data_value(no_data)?;
    }

    while let Ok(tile) = rx.recv() {
        let tile_info = tile.tile_information();

        let tile_bounds = tile_info.spatial_partition();

        let (upper_left, grid_array) = if output_bounds.contains(&tile_bounds) {
            (
                tile_bounds.upper_left(),
                tile.into_materialized_tile().grid_array,
            )
        } else {
            // extract relevant data from tile (intersection with output_bounds)

            let intersection = output_bounds
                .intersection(&tile_bounds)
                .expect("tile must intersect with query");

            let mut output_grid = Grid2D::new_filled(
                intersection.grid_shape(
                    output_geo_transform.origin_coordinate,
                    output_geo_transform.spatial_resolution(),
                ),
                no_data_value.map_or_else(T::zero, T::from_),
                no_data_value.map(T::from_),
            );

            let offset = tile
                .tile_geo_transform()
                .coordinate_to_grid_idx_2d(intersection.upper_left());

            let shifted_source = tile.grid_array.shift_by_offset(GridIdx([-1, -1]) * offset);

            output_grid.grid_blit_from(shifted_source);

            (intersection.upper_left(), output_grid)
        };

        let upper_left_pixel_x = ((upper_left.x - output_geo_transform.origin_coordinate.x)
            / x_pixel_size)
            .floor() as isize;
        let upper_left_pixel_y = ((output_geo_transform.origin_coordinate.y - upper_left.y)
            / y_pixel_size)
            .floor() as isize;
        let window = (upper_left_pixel_x, upper_left_pixel_y);

        let shape = grid_array.axis_size();
        let window_size = (shape[1], shape[0]);

        let buffer = Buffer::new(window_size, grid_array.data);

        band.write(window, window_size, &buffer)?;
    }

    if as_cog {
        // Override file with COG driver.

        // Since COGs are written "overviews first, data second", we cannot generate a GeoTiff (where we first have to
        // write the data in order to generate overviews) that fulfills this property. So, we have to do it as a
        // separate step.

        // TODO: use different options - with compression but without unsupported options TILED, BLOCKXSIZE, BLOCKYSIZE
        dataset.create_copy(&Driver::get("COG")?, output_file_path, &options)?;

        // TODO: remove intermediate file
        drop(dataset);
        // driver.delete(intermediate_file_path)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{Coordinate2D, SpatialPartition2D, SpatialResolution, TimeInterval},
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

        let query_bbox = SpatialPartition2D::new((-10., 80.).into(), (50., 20.).into()).unwrap();

        let bytes = raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_bbox,
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
            },
            ctx,
            Some(0.),
            SpatialReference::epsg_4326(),
            None,
            false,
        )
        .await
        .unwrap();

        assert_eq!(
            include_bytes!("../../../test_data/raster/geotiff_from_stream_compressed.tiff")
                as &[u8],
            bytes.as_slice()
        );
    }

    #[tokio::test]
    async fn cloud_optimized_geotiff_from_stream() {
        let ctx = MockQueryContext::default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

        let gdal_source = GdalSourceProcessor::<u8> {
            tiling_specification,
            meta_data: Box::new(create_ndvi_meta_data()),
            phantom_data: Default::default(),
        };

        let query_bbox = SpatialPartition2D::new((-10., 80.).into(), (50., 20.).into()).unwrap();

        let bytes = raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_bbox,
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
            },
            ctx,
            Some(0.),
            SpatialReference::epsg_4326(),
            None,
            true,
        )
        .await
        .unwrap();

        assert_eq!(
            include_bytes!(
                "../../../test_data/raster/cloud_optimized_geotiff_from_stream_compressed.tiff"
            ) as &[u8],
            bytes.as_slice()
        );
    }

    #[tokio::test]
    async fn geotiff_from_stream_limit() {
        let ctx = MockQueryContext::default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

        let gdal_source = GdalSourceProcessor::<u8> {
            tiling_specification,
            meta_data: Box::new(create_ndvi_meta_data()),
            phantom_data: Default::default(),
        };

        let query_bbox = SpatialPartition2D::new((-10., 80.).into(), (50., 20.).into()).unwrap();

        let bytes = raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_bbox,
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
            },
            ctx,
            Some(0.),
            SpatialReference::epsg_4326(),
            Some(1),
            false,
        )
        .await;

        assert!(bytes.is_err());
    }

    #[tokio::test]
    async fn geotiff_from_stream_in_range_of_window() {
        let ctx = MockQueryContext::default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

        let gdal_source = GdalSourceProcessor::<u8> {
            tiling_specification,
            meta_data: Box::new(create_ndvi_meta_data()),
            phantom_data: Default::default(),
        };

        let query_bbox =
            SpatialPartition2D::new((-180., -66.227_224_576_271_84).into(), (180., -90.).into())
                .unwrap();

        let bytes = raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_bbox,
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    0.228_716_645_489_199_48,
                    0.226_407_384_987_887_26,
                ),
            },
            ctx,
            Some(0.),
            SpatialReference::epsg_4326(),
            None,
            false,
        )
        .await;

        assert!(bytes.is_ok())
    }
}
