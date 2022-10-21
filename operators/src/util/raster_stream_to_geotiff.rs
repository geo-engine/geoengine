use crate::util::{Result, TemporaryGdalThreadLocalConfigOptions};
use crate::{
    engine::{QueryContext, RasterQueryProcessor},
    error::Error,
};
use futures::StreamExt;
use gdal::raster::{Buffer, GdalType, RasterCreationOption};
use gdal::{Dataset, Driver};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, RasterQueryRectangle, SpatialPartition2D, SpatialPartitioned,
};
use geoengine_datatypes::raster::{
    ChangeGridBounds, EmptyGrid2D, GeoTransform, GridBlit, GridIdx, GridSize, MapElements,
    MaskedGrid2D, NoDataValueGrid, Pixel, RasterTile2D,
};
use geoengine_datatypes::spatial_reference::SpatialReference;
use log::debug;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::path::Path;
use std::path::PathBuf;

pub async fn raster_stream_to_geotiff_bytes<T, C: QueryContext + 'static>(
    processor: Box<dyn RasterQueryProcessor<RasterType = T>>,
    query_rect: RasterQueryRectangle,
    query_ctx: C,
    gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
    gdal_tiff_options: GdalGeoTiffOptions,
    tile_limit: Option<usize>,
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
        gdal_tiff_metadata,
        gdal_tiff_options,
        tile_limit,
    )
    .await?;

    let bytes = gdal::vsi::get_vsi_mem_file_bytes_owned(file_path)?;

    Ok(bytes)
}

#[derive(Debug, Clone, Copy)]
pub struct GdalGeoTiffDatasetMetadata {
    pub no_data_value: Option<f64>,
    pub spatial_reference: SpatialReference,
}

pub async fn raster_stream_to_geotiff<P, C: QueryContext + 'static>(
    file_path: &Path,
    processor: Box<dyn RasterQueryProcessor<RasterType = P>>,
    query_rect: RasterQueryRectangle,
    query_ctx: C,
    gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
    gdal_tiff_options: GdalGeoTiffOptions,
    tile_limit: Option<usize>,
) -> Result<()>
where
    P: Pixel + GdalType,
{
    // TODO: create file path if it doesn't exist
    // TODO: handle streams with multiple time steps correctly

    let file_path = file_path.to_owned();

    let gdal_config_options = if gdal_tiff_metadata.no_data_value.is_none() {
        // If we want to write a mask into the geotiff we need to do that internaly because of vismem.
        Some(vec![(
            "GDAL_TIFF_INTERNAL_MASK".to_string(),
            "YES".to_string(),
        )])
    } else {
        None
    };

    let dataset_writer = crate::util::spawn_blocking(move || {
        let gdal_config_options = gdal_config_options.as_deref();

        GdalDatasetWriter::new(
            &file_path,
            query_rect,
            gdal_tiff_metadata,
            gdal_tiff_options,
            gdal_config_options,
        )
    })
    .await?;

    let tile_stream = processor.raster_query(query_rect, &query_ctx).await?;

    let dataset_writer = tile_stream
        .enumerate()
        .fold(
            dataset_writer,
            move |dataset_writer, (tile_index, tile)| async move {
                if tile_limit.map_or_else(|| false, |limit| tile_index >= limit) {
                    return Err(Error::TileLimitExceeded {
                        limit: tile_limit.expect("limit exist because it is exceeded"),
                    });
                }

                // TODO: more descriptive error. This error occured when a file could not be created...
                let dataset_writer = dataset_writer?;
                let tile = tile?;

                crate::util::spawn_blocking(move || -> Result<GdalDatasetWriter<P>> {
                    dataset_writer.write_tile(tile)?;
                    Ok(dataset_writer)
                })
                .await?
            },
        )
        .await?;

    crate::util::spawn_blocking(move || dataset_writer.finish()).await?
}

const COG_BLOCK_SIZE: &str = "512";
const COMPRESSION_FORMAT: &str = "LZW";
const COMPRESSION_LEVEL: &str = "9"; // maximum compression
const BIG_TIFF_BYTE_THRESHOLD: usize = 2_000_000_000; // ~ 2GB + 2GB for overviews + buffer for headers

#[derive(Debug)]
struct GdalDatasetWriter<P: Pixel + GdalType> {
    dataset: Dataset,
    rasterband_index: isize,
    intermediate_file_path: PathBuf,
    output_file_path: PathBuf,
    gdal_tiff_options: GdalGeoTiffOptions,
    gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
    output_bounds: SpatialPartition2D,
    output_geo_transform: GeoTransform,
    x_pixel_size: f64,
    y_pixel_size: f64,
    _type: std::marker::PhantomData<P>,
    use_big_tiff: bool,
}

impl<P: Pixel + GdalType> GdalDatasetWriter<P> {
    fn new(
        file_path: &Path,
        query_rect: RasterQueryRectangle,
        gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
        gdal_tiff_options: GdalGeoTiffOptions,
        gdal_config_options: Option<&[(String, String)]>,
    ) -> Result<Self> {
        const INTERMEDIATE_FILE_SUFFIX: &str = "GEO-ENGINE-TMP";
        let intermediate_file_path = file_path.with_extension(INTERMEDIATE_FILE_SUFFIX);
        let output_file_path = file_path.to_path_buf();

        let compression_num_threads = gdal_tiff_options.compression_num_threads.to_string();

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

        let uncompressed_byte_size = width as usize * height as usize * std::mem::size_of::<P>();
        let use_big_tiff =
            gdal_tiff_options.force_big_tiff || uncompressed_byte_size >= BIG_TIFF_BYTE_THRESHOLD;

        debug!(
            "use_big_tiff: {}, forced: {}",
            use_big_tiff, gdal_tiff_options.force_big_tiff
        );

        // reverts the thread local configs on drop
        let thread_local_configs =
            gdal_config_options.map(TemporaryGdalThreadLocalConfigOptions::new);

        let driver = Driver::get_by_name("GTiff")?;
        let options = create_gdal_tiff_options(
            &compression_num_threads,
            gdal_tiff_options.as_cog,
            use_big_tiff,
        );

        let mut dataset = driver.create_with_band_type_with_options::<P, _>(
            &intermediate_file_path,
            width as isize,
            height as isize,
            1,
            &options,
        )?;

        dataset.set_spatial_ref(&gdal_tiff_metadata.spatial_reference.try_into()?)?;
        dataset.set_geo_transform(&output_geo_transform.into())?;
        let rasterband_index = 1;
        let mut band = dataset.rasterband(rasterband_index)?;

        // Check if the gdal_tiff_metadata no-data value is set.
        // If it is set, set the no-data value for the output geotiff.
        // Otherwise add a mask band to the output geotiff.
        if let Some(no_data) = gdal_tiff_metadata.no_data_value {
            band.set_no_data_value(no_data)?;
        } else {
            band.create_mask_band(true)?;
        }

        drop(thread_local_configs); // ensure that we drop here

        Ok(Self {
            dataset,
            rasterband_index,
            intermediate_file_path,
            output_file_path,
            gdal_tiff_options,
            gdal_tiff_metadata,
            output_bounds,
            output_geo_transform,
            x_pixel_size,
            y_pixel_size,
            _type: Default::default(),
            use_big_tiff,
        })
    }

    fn write_tile(&self, tile: RasterTile2D<P>) -> Result<()> {
        let tile_info = tile.tile_information();

        let tile_bounds = tile_info.spatial_partition();

        let (upper_left, grid_array) = if self.output_bounds.contains(&tile_bounds) {
            (
                tile_bounds.upper_left(),
                tile.into_materialized_tile().grid_array,
            )
        } else {
            // extract relevant data from tile (intersection with output_bounds)

            let intersection = self
                .output_bounds
                .intersection(&tile_bounds)
                .expect("tile must intersect with query");

            let mut output_grid = MaskedGrid2D::from(EmptyGrid2D::new(intersection.grid_shape(
                self.output_geo_transform.origin_coordinate,
                self.output_geo_transform.spatial_resolution(),
            )));

            let offset = tile
                .tile_geo_transform()
                .coordinate_to_grid_idx_2d(intersection.upper_left());

            let shifted_source = tile.grid_array.shift_by_offset(GridIdx([-1, -1]) * offset);

            output_grid.grid_blit_from(&shifted_source);

            (intersection.upper_left(), output_grid)
        };

        let upper_left_pixel_x = ((upper_left.x - self.output_geo_transform.origin_coordinate.x)
            / self.x_pixel_size)
            .floor() as isize;
        let upper_left_pixel_y = ((self.output_geo_transform.origin_coordinate.y - upper_left.y)
            / self.y_pixel_size)
            .floor() as isize;
        let window = (upper_left_pixel_x, upper_left_pixel_y);

        let shape = grid_array.axis_size();
        let window_size = (shape[1], shape[0]);

        // Check if the gdal_tiff_metadata no-data value is set.
        // If it is set write a geotiff with no-data values.
        // Otherwise write a geotiff with a mask band.
        if let Some(out_no_data_value) = self.gdal_tiff_metadata.no_data_value {
            self.write_no_data_value_grid(&grid_array, out_no_data_value, window, window_size)?;
        } else {
            self.write_masked_data_grid(grid_array, window, window_size)?;
        }
        Ok(())
    }

    fn write_no_data_value_grid(
        &self,
        grid_array: &MaskedGrid2D<P>,
        no_data_value: f64,
        window: (isize, isize),
        window_size: (usize, usize),
    ) -> Result<()> {
        let out_no_data_value_p: P = P::from_(no_data_value);
        let no_data_value_grid = NoDataValueGrid::from_masked_grid(grid_array, out_no_data_value_p);

        let buffer = Buffer::new(window_size, no_data_value_grid.inner_grid.data); // TODO: also write mask!

        self.dataset
            .rasterband(self.rasterband_index)?
            .write(window, window_size, &buffer)?;
        Ok(())
    }

    fn write_masked_data_grid(
        &self,
        masked_grid: MaskedGrid2D<P>,
        window: (isize, isize),
        window_size: (usize, usize),
    ) -> Result<()> {
        // Write the MaskedGrid data and mask if no no-data value is set.
        let data_buffer = Buffer::new(window_size, masked_grid.inner_grid.data);

        let mut raster_band = self.dataset.rasterband(self.rasterband_index)?;
        raster_band.write(window, window_size, &data_buffer)?;

        // No-data masks are described by the rasterio docs as:
        // "One is the the valid data mask from GDAL, an unsigned byte array with the same number of rows and columns as the dataset in which non-zero elements (typically 255) indicate that the corresponding data elements are valid. Other elements are invalid, or nodata elements."

        let mask_grid_gdal_values =
            masked_grid
                .validity_mask
                .map_elements(|is_valid| if is_valid { 255_u8 } else { 0 }); // TODO: investigate if we can transmute the vec of bool to u8.
        let mask_buffer = Buffer::new(window_size, mask_grid_gdal_values.data);

        let mut mask_band = raster_band.open_mask_band()?;
        mask_band.write(window, window_size, &mask_buffer)?;

        Ok(())
    }

    fn finish(self) -> Result<()> {
        if self.gdal_tiff_options.as_cog {
            geotiff_to_cog(
                self.dataset,
                &self.intermediate_file_path,
                &self.output_file_path,
                self.gdal_tiff_options.compression_num_threads,
                self.use_big_tiff,
            )
        } else {
            let driver = self.dataset.driver();

            // close file before renaming
            drop(self.dataset);

            driver.rename(&self.output_file_path, &self.intermediate_file_path)?;

            Ok(())
        }
    }
}

fn create_gdal_tiff_options(
    compression_num_threads: &str,
    as_cog: bool,
    as_big_tiff: bool,
) -> Vec<RasterCreationOption<'_>> {
    let mut options = vec![
        RasterCreationOption {
            key: "COMPRESS",
            value: COMPRESSION_FORMAT,
        },
        RasterCreationOption {
            key: "TILED",
            value: "YES",
        },
        RasterCreationOption {
            key: "ZLEVEL",
            value: COMPRESSION_LEVEL,
        },
        RasterCreationOption {
            key: "NUM_THREADS",
            value: compression_num_threads,
        },
        RasterCreationOption {
            key: "INTERLEAVE",
            value: "BAND",
        },
    ];
    if as_cog {
        // COGs require a block size of 512x512, so we enforce it now so that we do the work only once.
        options.push(RasterCreationOption {
            key: "BLOCKXSIZE",
            value: COG_BLOCK_SIZE,
        });
        options.push(RasterCreationOption {
            key: "BLOCKYSIZE",
            value: COG_BLOCK_SIZE,
        });
    }
    if as_big_tiff {
        options.push(RasterCreationOption {
            key: "BIGTIFF",
            value: "YES",
        });
    }
    options
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct GdalGeoTiffOptions {
    pub compression_num_threads: GdalCompressionNumThreads,
    pub as_cog: bool,
    pub force_big_tiff: bool,
}

/// Number of threads for GDAL to use when compressing files.
#[derive(Debug, Clone, Copy)]
pub enum GdalCompressionNumThreads {
    AllCpus,
    NumThreads(u16),
}

impl<'de> Deserialize<'de> for GdalCompressionNumThreads {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let number_of_threads = u16::deserialize(deserializer)?;

        if number_of_threads == 0 {
            Ok(GdalCompressionNumThreads::AllCpus)
        } else {
            Ok(GdalCompressionNumThreads::NumThreads(number_of_threads))
        }
    }
}

impl Serialize for GdalCompressionNumThreads {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::AllCpus => serializer.serialize_u16(0),
            Self::NumThreads(number_of_threads) => serializer.serialize_u16(*number_of_threads),
        }
    }
}

impl std::fmt::Display for GdalCompressionNumThreads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AllCpus => write!(f, "ALL_CPUS"),
            Self::NumThreads(n) => write!(f, "{}", n),
        }
    }
}

/// Override file with COG driver
///
/// Since COGs are written "overviews first, data second", we cannot generate a `GeoTiff` (where we first have to
/// write the data in order to generate overviews) that fulfills this property. So, we have to do it as a
/// separate step.
///
fn geotiff_to_cog(
    input_dataset: Dataset,
    input_file_path: &Path,
    output_file_path: &Path,
    compression_num_threads: GdalCompressionNumThreads,
    as_big_tiff: bool,
) -> Result<()> {
    let input_driver = input_dataset.driver();
    let output_driver = Driver::get_by_name("COG")?;
    let num_threads = &compression_num_threads.to_string();

    let mut options = vec![
        RasterCreationOption {
            key: "COMPRESS",
            value: COMPRESSION_FORMAT,
        },
        RasterCreationOption {
            key: "LEVEL",
            value: COMPRESSION_LEVEL,
        },
        RasterCreationOption {
            key: "NUM_THREADS",
            value: num_threads,
        },
        RasterCreationOption {
            key: "BLOCKSIZE",
            value: COG_BLOCK_SIZE,
        },
    ];

    if as_big_tiff {
        options.push(RasterCreationOption {
            key: "BIGTIFF",
            value: "YES",
        });
    }

    input_dataset.create_copy(&output_driver, output_file_path, &options)?;

    drop(input_dataset);

    input_driver.delete(input_file_path)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use geoengine_datatypes::{
        primitives::{Coordinate2D, SpatialPartition2D, SpatialResolution, TimeInterval},
        raster::TilingSpecification,
        util::test::TestDefault,
    };

    use crate::{
        engine::MockQueryContext, source::GdalSourceProcessor, util::gdal::create_ndvi_meta_data,
    };

    use super::*;

    #[tokio::test]
    async fn geotiff_with_no_data_from_stream() {
        let ctx = MockQueryContext::test_default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

        let metadata = create_ndvi_meta_data();

        let gdal_source = GdalSourceProcessor::<u8> {
            tiling_specification,
            meta_data: Box::new(metadata),
            _phantom_data: PhantomData,
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
            GdalGeoTiffDatasetMetadata {
                no_data_value: Some(0.),
                spatial_reference: SpatialReference::epsg_4326(),
            },
            GdalGeoTiffOptions {
                as_cog: false,
                compression_num_threads: GdalCompressionNumThreads::NumThreads(2),
                force_big_tiff: false,
            },
            None,
        )
        .await
        .unwrap();

        // geoengine_datatypes::util::test::save_test_bytes(
        //    &bytes,
        //    "../test_data/raster/geotiff_from_stream_compressed.tiff",
        // );

        assert_eq!(
            include_bytes!("../../../test_data/raster/geotiff_from_stream_compressed.tiff")
                as &[u8],
            bytes.as_slice()
        );
    }

    #[tokio::test]
    async fn geotiff_with_mask_from_stream() {
        let ctx = MockQueryContext::test_default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

        let metadata = create_ndvi_meta_data();

        let gdal_source = GdalSourceProcessor::<u8> {
            tiling_specification,
            meta_data: Box::new(metadata),
            _phantom_data: PhantomData,
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
            GdalGeoTiffDatasetMetadata {
                no_data_value: None,
                spatial_reference: SpatialReference::epsg_4326(),
            },
            GdalGeoTiffOptions {
                as_cog: false,
                compression_num_threads: GdalCompressionNumThreads::NumThreads(2),
                force_big_tiff: false,
            },
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            include_bytes!(
                "../../../test_data/raster/geotiff_with_mask_from_stream_compressed.tiff"
            ) as &[u8],
            bytes.as_slice()
        );
    }

    #[tokio::test]
    async fn geotiff_big_tiff_from_stream() {
        let ctx = MockQueryContext::test_default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

        let metadata = create_ndvi_meta_data();

        let gdal_source = GdalSourceProcessor::<u8> {
            tiling_specification,
            meta_data: Box::new(metadata),
            _phantom_data: PhantomData,
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
            GdalGeoTiffDatasetMetadata {
                no_data_value: Some(0.),
                spatial_reference: SpatialReference::epsg_4326(),
            },
            GdalGeoTiffOptions {
                as_cog: false,
                compression_num_threads: GdalCompressionNumThreads::NumThreads(2),
                force_big_tiff: true,
            },
            None,
        )
        .await
        .unwrap();

        // geoengine_datatypes::util::test::save_test_bytes(
        //    &bytes,
        //    "../test_data/raster/geotiff_big_tiff_from_stream_compressed.tiff",
        // );

        assert_eq!(
            include_bytes!("../../../test_data/raster/geotiff_big_tiff_from_stream_compressed.tiff")
                as &[u8],
            bytes.as_slice()
        );
    }

    #[tokio::test]
    async fn cloud_optimized_geotiff_big_tiff_from_stream() {
        let ctx = MockQueryContext::test_default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

        let metadata = create_ndvi_meta_data();

        let gdal_source = GdalSourceProcessor::<u8> {
            tiling_specification,
            meta_data: Box::new(metadata),
            _phantom_data: PhantomData,
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
            GdalGeoTiffDatasetMetadata {
                no_data_value: Some(0.),
                spatial_reference: SpatialReference::epsg_4326(),
            },
            GdalGeoTiffOptions {
                as_cog: true,
                compression_num_threads: GdalCompressionNumThreads::AllCpus,
                force_big_tiff: true,
            },
            None,
        )
        .await
        .unwrap();

        // geoengine_datatypes::util::test::save_test_bytes(
        //    &bytes,
        //    "../test_data/raster/cloud_optimized_geotiff_big_tiff_from_stream_compressed.tiff",
        //);

        assert_eq!(
            include_bytes!(
                "../../../test_data/raster/cloud_optimized_geotiff_big_tiff_from_stream_compressed.tiff"
            ) as &[u8],
            bytes.as_slice()
        );

        // TODO: check programmatically that intermediate file is gone
    }

    #[tokio::test]
    async fn cloud_optimized_geotiff_from_stream() {
        let ctx = MockQueryContext::test_default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

        let metadata = create_ndvi_meta_data();

        let gdal_source = GdalSourceProcessor::<u8> {
            tiling_specification,
            meta_data: Box::new(metadata),
            _phantom_data: PhantomData,
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
            GdalGeoTiffDatasetMetadata {
                no_data_value: Some(0.),
                spatial_reference: SpatialReference::epsg_4326(),
            },
            GdalGeoTiffOptions {
                as_cog: true,
                compression_num_threads: GdalCompressionNumThreads::AllCpus,
                force_big_tiff: false,
            },
            None,
        )
        .await
        .unwrap();

        // geoengine_datatypes::util::test::save_test_bytes(
        //     &bytes,
        //     "../test_data/raster/cloud_optimized_geotiff_from_stream_compressed.tiff",
        // );

        assert_eq!(
            include_bytes!(
                "../../../test_data/raster/cloud_optimized_geotiff_from_stream_compressed.tiff"
            ) as &[u8],
            bytes.as_slice()
        );

        // TODO: check programmatically that intermediate file is gone
    }

    #[tokio::test]
    async fn geotiff_from_stream_limit() {
        let ctx = MockQueryContext::test_default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

        let metadata = create_ndvi_meta_data();

        let gdal_source = GdalSourceProcessor::<u8> {
            tiling_specification,
            meta_data: Box::new(metadata),
            _phantom_data: PhantomData,
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
            GdalGeoTiffDatasetMetadata {
                no_data_value: Some(0.),
                spatial_reference: SpatialReference::epsg_4326(),
            },
            GdalGeoTiffOptions {
                as_cog: false,
                compression_num_threads: GdalCompressionNumThreads::NumThreads(1),
                force_big_tiff: false,
            },
            Some(1),
        )
        .await;

        assert!(bytes.is_err());
    }

    #[tokio::test]
    async fn geotiff_from_stream_in_range_of_window() {
        let ctx = MockQueryContext::test_default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

        let metadata = create_ndvi_meta_data();

        let gdal_source = GdalSourceProcessor::<u8> {
            tiling_specification,
            meta_data: Box::new(metadata),
            _phantom_data: PhantomData,
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
            GdalGeoTiffDatasetMetadata {
                no_data_value: Some(0.),
                spatial_reference: SpatialReference::epsg_4326(),
            },
            GdalGeoTiffOptions {
                as_cog: false,
                compression_num_threads: GdalCompressionNumThreads::AllCpus,
                force_big_tiff: false,
            },
            None,
        )
        .await;

        assert!(bytes.is_ok());
    }
}
