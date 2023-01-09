use crate::error;
use crate::source::{
    FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters,
    GdalLoadingInfoTemporalSlice, GdalSourceTimePlaceholder, TimeReference,
};
use crate::util::{Result, TemporaryGdalThreadLocalConfigOptions};
use crate::{
    engine::{QueryContext, RasterQueryProcessor},
    error::Error,
};
use futures::future::BoxFuture;
use futures::{StreamExt, TryFutureExt};
use gdal::raster::{Buffer, GdalType, RasterBand, RasterCreationOption};
use gdal::{Dataset, DriverManager};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, DateTimeParseFormat, RasterQueryRectangle, SpatialPartition2D,
    SpatialPartitioned, SpatialQuery, TimeInterval,
};
use geoengine_datatypes::raster::{
    ChangeGridBounds, EmptyGrid2D, GeoTransform, GridBlit, GridBounds, GridIdx, GridIdx2D,
    GridSize, MapElements, MaskedGrid2D, NoDataValueGrid, Pixel, RasterTile2D, TilingSpecification,
};
use geoengine_datatypes::spatial_reference::SpatialReference;
use log::debug;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::path::Path;
use std::path::PathBuf;

use super::abortable_query_execution;

#[allow(clippy::too_many_arguments)]
pub async fn single_timestep_raster_stream_to_geotiff_bytes<T, C: QueryContext + 'static>(
    processor: Box<dyn RasterQueryProcessor<RasterType = T>>,
    query_rect: RasterQueryRectangle,
    query_ctx: C,
    gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
    gdal_tiff_options: GdalGeoTiffOptions,
    tile_limit: Option<usize>,
    conn_closed: BoxFuture<'_, ()>,
    tiling_specification: TilingSpecification,
) -> Result<Vec<u8>>
where
    T: Pixel + GdalType,
{
    let mut timesteps = raster_stream_to_geotiff_bytes(
        processor,
        query_rect,
        query_ctx,
        gdal_tiff_metadata,
        gdal_tiff_options,
        tile_limit,
        conn_closed,
        tiling_specification,
    )
    .await?;

    if timesteps.len() == 1 {
        Ok(timesteps
            .pop()
            .expect("there should be exactly one timestep"))
    } else {
        Err(Error::InvalidNumberOfTimeSteps {
            expected: 1,
            found: timesteps.len(),
        })
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn raster_stream_to_geotiff_bytes<T, C: QueryContext + 'static>(
    processor: Box<dyn RasterQueryProcessor<RasterType = T>>,
    query_rect: RasterQueryRectangle,
    query_ctx: C,
    gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
    gdal_tiff_options: GdalGeoTiffOptions,
    tile_limit: Option<usize>,
    conn_closed: BoxFuture<'_, ()>,
    tiling_specification: TilingSpecification,
) -> Result<Vec<Vec<u8>>>
where
    T: Pixel + GdalType,
{
    let file_path = PathBuf::from(format!("/vsimem/{}/", uuid::Uuid::new_v4()));

    let result = raster_stream_to_geotiff(
        &file_path,
        processor,
        query_rect,
        query_ctx,
        gdal_tiff_metadata,
        gdal_tiff_options,
        tile_limit,
        conn_closed,
        tiling_specification,
    )
    .await?
    .into_iter()
    .map(|x| match x.params {
        Some(p) => gdal::vsi::get_vsi_mem_file_bytes_owned(p.file_path),
        None => Ok(vec![]),
    })
    .collect::<Result<Vec<_>, _>>()?;

    Ok(result)
}

#[allow(clippy::too_many_arguments)]
pub async fn raster_stream_to_geotiff<P, C: QueryContext + 'static>(
    file_path: &Path,
    processor: Box<dyn RasterQueryProcessor<RasterType = P>>,
    query_rect: RasterQueryRectangle,
    mut query_ctx: C,
    gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
    gdal_tiff_options: GdalGeoTiffOptions,
    tile_limit: Option<usize>,
    conn_closed: BoxFuture<'_, ()>,
    tiling_specification: TilingSpecification,
) -> Result<Vec<GdalLoadingInfoTemporalSlice>>
where
    P: Pixel + GdalType,
{
    let query_abort_trigger = query_ctx.abort_trigger()?;

    // TODO: create file path if it doesn't exist

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

    let dataset_holder: Result<GdalDatasetHolder<P>> = Ok(GdalDatasetHolder::new_with_tiling_spec(
        tiling_specification,
        &file_path,
        query_rect,
        gdal_tiff_metadata,
        gdal_tiff_options,
        gdal_config_options,
    ));

    let tile_stream = processor.raster_query(query_rect, &query_ctx).await?;

    let mut dataset_holder = tile_stream
        .enumerate()
        .fold(
            dataset_holder,
            move |dataset_holder, (tile_index, tile)| async move {
                if tile_limit.map_or_else(|| false, |limit| tile_index >= limit) {
                    return Err(Error::TileLimitExceeded {
                        limit: tile_limit.expect("limit exist because it is exceeded"),
                    });
                }

                let mut dataset_holder = dataset_holder?;
                let tile = tile?;

                let current_interval = tile.time;

                let dataset_holder =
                    crate::util::spawn_blocking(move || -> Result<GdalDatasetHolder<P>> {
                        dataset_holder
                            .update_intermediate_dataset_from_time_interval(current_interval)?;
                        Ok(dataset_holder)
                    })
                    .await??;

                crate::util::spawn_blocking(move || -> Result<GdalDatasetHolder<P>> {
                    let raster_band = dataset_holder
                        .intermediate_dataset
                        .as_ref()
                        .expect("dataset should exist after successfully creating/updating it based on the tile's time interval")
                        .dataset
                        .rasterband(dataset_holder.create_meta.raster_band_index)?;
                    dataset_holder
                        .dataset_writer
                        .write_tile_into_band(tile, raster_band)?;
                    Ok(dataset_holder)
                })
                .await?
            },
        )
        .await?;

    let intermediate_dataset = dataset_holder
        .intermediate_dataset
        .take()
        .expect("dataset should exist after writing all tiles");

    let result = dataset_holder.result.clone();

    let written = crate::util::spawn_blocking(move || {
        dataset_holder
            .dataset_writer
            .finish_dataset(intermediate_dataset)
    })
    .map_err(|e| error::Error::TokioJoin { source: e });

    abortable_query_execution(written, conn_closed, query_abort_trigger).await??;

    Ok(result)
}

const COG_BLOCK_SIZE: &str = "512";
const COMPRESSION_FORMAT: &str = "LZW";
const COMPRESSION_LEVEL: &str = "9"; // maximum compression
const BIG_TIFF_BYTE_THRESHOLD: usize = 2_000_000_000; // ~ 2GB + 2GB for overviews + buffer for headers

#[derive(Debug)]
pub struct PathWithPlaceholder {
    full_path: PathBuf,
    placeholder: String,
    time_placeholder: GdalSourceTimePlaceholder,
}

impl PathWithPlaceholder {
    fn translate_path_for_interval(&self, interval: TimeInterval) -> Result<PathBuf> {
        let time = match self.time_placeholder.reference {
            TimeReference::Start => interval.start(),
            TimeReference::End => interval.end(),
        };
        let time_string = time
            .as_date_time()
            .ok_or(Error::TimeInstanceNotDisplayable)?
            .format(&self.time_placeholder.format);

        Ok(PathBuf::from(
            // TODO: use more efficient algorithm for replacing multiple placeholders, e.g. aho-corasick
            self.full_path
                .to_string_lossy()
                .to_string()
                .replace(&self.placeholder, &time_string),
        ))
    }
}

#[derive(Debug)]
struct IntermediateDataset {
    dataset: Dataset,
    time_interval: TimeInterval,
    intermediate_path: PathBuf,
    destination_path: PathBuf,
}

#[derive(Debug)]
struct IntermediateDatasetMetadata {
    raster_band_index: isize,
    width: u32,
    height: u32,
    use_big_tiff: bool,
    path_with_placeholder: PathWithPlaceholder,
    gdal_config_options: Option<Vec<(String, String)>>,
    intermediate_dataset_parameters: GdalDatasetParameters,
}

struct GdalDatasetHolder<P: Pixel + GdalType> {
    intermediate_dataset: Option<IntermediateDataset>,
    create_meta: IntermediateDatasetMetadata,
    dataset_writer: GdalDatasetWriter<P>,
    result: Vec<GdalLoadingInfoTemporalSlice>,
}

impl<P: Pixel + GdalType> GdalDatasetHolder<P> {
    fn new(
        file_path: &Path,
        query_rect: RasterQueryRectangle,
        gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
        gdal_tiff_options: GdalGeoTiffOptions,
        gdal_config_options: Option<Vec<(String, String)>>,
        window_start: GridIdx2D,
        window_end: GridIdx2D,
    ) -> Self {
        const INTERMEDIATE_FILE_SUFFIX: &str = "GEO-ENGINE-TMP";

        //TODO: Consider making placeholder and time_placeholder format configurable
        let placeholder = "%_START_TIME_%";
        let placeholder_path = file_path.join(format!("raster_{placeholder}.tiff"));
        let path_with_placeholder = PathWithPlaceholder {
            full_path: placeholder_path,
            placeholder: placeholder.to_string(),
            time_placeholder: GdalSourceTimePlaceholder {
                format: DateTimeParseFormat::custom("%Y-%m-%dT%H-%M-%S".to_string()),
                reference: TimeReference::Start,
            },
        };

        let file_path = file_path.join("raster.tiff");
        let intermediate_file_path = file_path.with_extension(INTERMEDIATE_FILE_SUFFIX);

        let width = query_rect.spatial_query().grid_bounds.axis_size_x();
        let height = query_rect.spatial_query().grid_bounds.axis_size_y();

        let query_spatial_partition = query_rect.spatial_query().spatial_partition();

        let output_geo_transform = GeoTransform::new(
            query_spatial_partition.upper_left(),
            query_rect.spatial_query().geo_transform.x_pixel_size(),
            query_rect.spatial_query().geo_transform.y_pixel_size(),
        );

        let intermediate_dataset_parameters = GdalDatasetParameters {
            file_path: intermediate_file_path,
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: query_spatial_partition.upper_left(),
                x_pixel_size: query_rect.spatial_query().geo_transform.x_pixel_size(),
                y_pixel_size: query_rect.spatial_query().geo_transform.y_pixel_size(),
            },
            width: width as usize,
            height: height as usize,
            file_not_found_handling: FileNotFoundHandling::Error,
            no_data_value: None, // `None` will let the GdalSource detect the correct no-data value.
            properties_mapping: None, // TODO: add properties
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
        };

        let uncompressed_byte_size = intermediate_dataset_parameters.width
            * intermediate_dataset_parameters.height
            * std::mem::size_of::<P>();
        let use_big_tiff =
            gdal_tiff_options.force_big_tiff || uncompressed_byte_size >= BIG_TIFF_BYTE_THRESHOLD;

        debug!(
            "use_big_tiff: {}, forced: {}",
            use_big_tiff, gdal_tiff_options.force_big_tiff
        );

        let rasterband_index = 1;

        Self {
            intermediate_dataset: None,
            create_meta: IntermediateDatasetMetadata {
                raster_band_index: rasterband_index,
                width: width as u32,
                height: height as u32,
                use_big_tiff,
                path_with_placeholder,
                gdal_config_options,
                intermediate_dataset_parameters,
            },
            dataset_writer: GdalDatasetWriter {
                gdal_tiff_options,
                gdal_tiff_metadata,
                _output_bounds: query_spatial_partition,
                output_geo_transform,
                use_big_tiff,
                _type: Default::default(),
                window_start,
                window_end,
            },
            result: vec![],
        }
    }

    fn create_data_set(
        intermediate_dataset_metadata: &IntermediateDatasetMetadata,
        gdal_tiff_metadata: &GdalGeoTiffDatasetMetadata,
        gdal_tiff_options: GdalGeoTiffOptions,
        output_geo_transform: GeoTransform,
    ) -> Result<Dataset> {
        let compression_num_threads = gdal_tiff_options.compression_num_threads.to_string();

        // reverts the thread local configs on drop
        let thread_local_configs = intermediate_dataset_metadata
            .gdal_config_options
            .as_deref()
            .map(TemporaryGdalThreadLocalConfigOptions::new);

        let driver = DriverManager::get_driver_by_name("GTiff")?;
        let options = create_gdal_tiff_options(
            &compression_num_threads,
            gdal_tiff_options.as_cog,
            intermediate_dataset_metadata.use_big_tiff,
        );

        let mut dataset = driver.create_with_band_type_with_options::<P, _>(
            &intermediate_dataset_metadata
                .intermediate_dataset_parameters
                .file_path,
            intermediate_dataset_metadata.width as isize,
            intermediate_dataset_metadata.height as isize,
            1,
            &options,
        )?;

        dataset.set_spatial_ref(&gdal_tiff_metadata.spatial_reference.try_into()?)?;
        dataset.set_geo_transform(&output_geo_transform.into())?;
        let mut band = dataset.rasterband(intermediate_dataset_metadata.raster_band_index)?;

        // Check if the gdal_tiff_metadata no-data value is set.
        // If it is set, set the no-data value for the output geotiff.
        // Otherwise add a mask band to the output geotiff.
        if let Some(no_data) = gdal_tiff_metadata.no_data_value {
            band.set_no_data_value(Some(no_data))?;
        } else {
            band.create_mask_band(true)?;
        }

        drop(thread_local_configs); // ensure that we drop here

        Ok(dataset)
    }

    fn init_new_intermediate_dataset(
        &mut self,
        time_interval: TimeInterval,
        intermediate_path: PathBuf,
        destination_path: PathBuf,
    ) -> Result<()> {
        let dataset = Self::create_data_set(
            &self.create_meta,
            &self.dataset_writer.gdal_tiff_metadata,
            self.dataset_writer.gdal_tiff_options,
            self.dataset_writer.output_geo_transform,
        )?;
        self.intermediate_dataset = Some(IntermediateDataset {
            dataset,
            time_interval,
            intermediate_path,
            destination_path,
        });

        Ok(())
    }

    fn new_with_tiling_spec(
        tiling_specification: TilingSpecification,
        file_path: &Path,
        query_rect: RasterQueryRectangle,
        gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
        gdal_tiff_options: GdalGeoTiffOptions,
        gdal_config_options: Option<Vec<(String, String)>>,
    ) -> Self {
        // FIXME: The query origin must match the tiling strategy's origin for now. Also use grid bounds not spatial bounds.
        assert_eq!(
            query_rect.spatial_query().geo_transform.origin_coordinate(),
            tiling_specification.origin_coordinate,
            "The query origin coordinate must match the tiling strategy's origin for now."
        );

        let window_start = query_rect.spatial_query().grid_bounds.min_index();
        let window_end = query_rect.spatial_query().grid_bounds.max_index() + 1; // +1 since grid bounds is inclusiv.

        Self::new(
            file_path,
            query_rect,
            gdal_tiff_metadata,
            gdal_tiff_options,
            gdal_config_options,
            window_start,
            window_end,
        )
    }

    fn update_intermediate_dataset_from_time_interval(
        &mut self,
        time_interval: TimeInterval,
    ) -> Result<()> {
        if self
            .intermediate_dataset
            .as_ref()
            .map_or(true, |x| x.time_interval != time_interval)
        {
            if let Some(intermediate_dataset) = self.intermediate_dataset.take() {
                self.dataset_writer.finish_dataset(intermediate_dataset)?;
            }
            let dataset_path = self
                .create_meta
                .path_with_placeholder
                .translate_path_for_interval(time_interval)?;

            let mut dataset_parameters = self.create_meta.intermediate_dataset_parameters.clone();
            dataset_parameters.file_path = dataset_path.clone();
            self.result.push(GdalLoadingInfoTemporalSlice {
                time: time_interval,
                params: Some(dataset_parameters),
            });
            self.init_new_intermediate_dataset(
                time_interval,
                self.create_meta
                    .intermediate_dataset_parameters
                    .file_path
                    .clone(),
                dataset_path,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GdalGeoTiffDatasetMetadata {
    pub no_data_value: Option<f64>,
    pub spatial_reference: SpatialReference,
}

#[derive(Debug)]
struct GdalDatasetWriter<P: Pixel + GdalType> {
    gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
    gdal_tiff_options: GdalGeoTiffOptions,
    _output_bounds: SpatialPartition2D, // currently unused due to workaround for intersection and contained because of float precision
    output_geo_transform: GeoTransform,
    use_big_tiff: bool,
    _type: std::marker::PhantomData<P>,
    window_start: GridIdx2D,
    window_end: GridIdx2D,
}

impl<P: Pixel + GdalType> GdalDatasetWriter<P> {
    fn write_tile_into_band(&self, tile: RasterTile2D<P>, raster_band: RasterBand) -> Result<()> {
        let tile_info = tile.tile_information();

        let tile_start = tile_info.global_upper_left_pixel_idx();
        let [tile_height, tile_width] = tile_info.tile_size_in_pixels.shape_array;
        let tile_end = tile_start + GridIdx2D::from([tile_height as isize, tile_width as isize]);

        let GridIdx([tile_start_y, tile_start_x]) = tile_start;
        let GridIdx([tile_end_y, tile_end_x]) = tile_end;
        let GridIdx([window_start_y, window_start_x]) = self.window_start;
        let GridIdx([window_end_y, window_end_x]) = self.window_end;

        // compute the upper left pixel index in the output raster and extract the input data
        let (GridIdx([output_ul_y, output_ul_x]), grid_array) =
            // TODO: check contains on the `SpatialPartition2D`s once the float precision issue is fixed
            if tile_start_x >= window_start_x && tile_start_y >= window_start_y && tile_end_x <= window_end_x && tile_end_y <= window_end_y {
                // tile is completely inside the output raster
                (
                    tile_info.global_upper_left_pixel_idx() - self.window_start,
                    tile.into_materialized_tile().grid_array,
                )
            } else {
                // extract relevant data from tile (intersection with output_bounds)

                // TODO: compute the intersection on the `SpatialPartition2D`s once the float precision issue is fixed

                if tile_end_y < window_start_y
                    || tile_end_x < window_start_x
                    || tile_start_y >= window_end_y
                    || tile_start_x >= window_end_x
                {
                    // tile is outside of output bounds
                    return Ok(());
                }

                let intersection_start = GridIdx2D::from([
                    std::cmp::max(tile_start_y, window_start_y),
                    std::cmp::max(tile_start_x, window_start_x),
                ]);
                let GridIdx([intersection_start_y, intersection_start_x]) = intersection_start;

                let width = std::cmp::min(
                    tile_info.tile_size_in_pixels.axis_size_x() as isize,
                    window_end_x - intersection_start_x,
                );

                let height = std::cmp::min(
                    tile_info.tile_size_in_pixels.axis_size_y() as isize,
                    window_end_y - intersection_start_y,
                );

                let mut output_grid =
                    MaskedGrid2D::from(EmptyGrid2D::new([height as usize, width as usize].into()));

                let shift_offset = intersection_start - tile_start;
                let shifted_source = tile
                    .grid_array
                    .shift_by_offset(GridIdx([-1, -1]) * shift_offset);

                output_grid.grid_blit_from(&shifted_source);

                (intersection_start - self.window_start, output_grid)
            };

        let window = (output_ul_x, output_ul_y);
        let [shape_y, shape_x] = grid_array.axis_size();
        let window_size = (shape_x, shape_y);

        // Check if the gdal_tiff_metadata no-data value is set.
        // If it is set write a geotiff with no-data values.
        // Otherwise write a geotiff with a mask band.
        if let Some(out_no_data_value) = self.gdal_tiff_metadata.no_data_value {
            Self::write_no_data_value_grid(
                &grid_array,
                out_no_data_value,
                window,
                window_size,
                raster_band,
            )?;
        } else {
            Self::write_masked_data_grid(grid_array, window, window_size, raster_band)?;
        }
        Ok(())
    }

    fn write_no_data_value_grid(
        grid_array: &MaskedGrid2D<P>,
        no_data_value: f64,
        window: (isize, isize),
        window_size: (usize, usize),
        mut raster_band: RasterBand,
    ) -> Result<()> {
        let out_no_data_value_p: P = P::from_(no_data_value);
        let no_data_value_grid = NoDataValueGrid::from_masked_grid(grid_array, out_no_data_value_p);

        let buffer = Buffer::new(window_size, no_data_value_grid.inner_grid.data); // TODO: also write mask!

        raster_band.write(window, window_size, &buffer)?;
        Ok(())
    }

    fn write_masked_data_grid(
        masked_grid: MaskedGrid2D<P>,
        window: (isize, isize),
        window_size: (usize, usize),
        mut raster_band: RasterBand,
    ) -> Result<()> {
        // Write the MaskedGrid data and mask if no no-data value is set.
        let data_buffer = Buffer::new(window_size, masked_grid.inner_grid.data);

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

    fn finish_dataset(&self, intermediate_dataset: IntermediateDataset) -> Result<()> {
        let input_dataset = intermediate_dataset.dataset;
        let input_file_path = intermediate_dataset.intermediate_path;
        let output_file_path = intermediate_dataset.destination_path;
        if self.gdal_tiff_options.as_cog {
            geotiff_to_cog(
                input_dataset,
                &input_file_path,
                &output_file_path,
                self.gdal_tiff_options.compression_num_threads,
                self.use_big_tiff,
            )
        } else {
            let driver = input_dataset.driver();

            // close file before renaming
            drop(input_dataset);

            driver.rename(&output_file_path, &input_file_path)?;

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
            Self::NumThreads(n) => write!(f, "{n}"),
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
    let output_driver = DriverManager::get_driver_by_name("COG")?;
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

        let bytes = single_timestep_raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                query_bbox,
                SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
                Coordinate2D::default(), // this is the default tiling strategy origin
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            ),
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
            Box::pin(futures::future::pending()),
            tiling_specification,
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

        let bytes = single_timestep_raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                query_bbox,
                SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
                Coordinate2D::default(), // this is the default tiling strategy origin
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            ),
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
            Box::pin(futures::future::pending()),
            tiling_specification,
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

        let bytes = single_timestep_raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                query_bbox,
                SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
                Coordinate2D::default(), // this is the default tiling strategy origin
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            ),
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
            Box::pin(futures::future::pending()),
            tiling_specification,
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

        let bytes = single_timestep_raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                query_bbox,
                SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
                Coordinate2D::default(), // this is the default tiling strategy origin
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            ),
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
            Box::pin(futures::future::pending()),
            tiling_specification,
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

        let bytes = single_timestep_raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                query_bbox,
                SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
                Coordinate2D::default(), // this is the default tiling strategy origin
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            ),
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
            Box::pin(futures::future::pending()),
            tiling_specification,
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
    async fn cloud_optimized_geotiff_multiple_timesteps_from_stream() {
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

        let mut bytes = raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                query_bbox,
                SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
                Coordinate2D::default(), // this is the default tiling strategy origin
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 7_776_000_000).unwrap(),
            ),
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
            Box::pin(futures::future::pending()),
            tiling_specification,
        )
        .await
        .unwrap();

        assert_eq!(bytes.len(), 3);

        // for i in 0..3 {
        //     let file_name = format!(
        //         "../test_data/raster/cloud_optimized_geotiff_timestep_{}_from_stream_compressed.tiff",
        //         i
        //     );
        //     geoengine_datatypes::util::test::save_test_bytes(
        //         &bytes.pop().expect("bytes should have length 3"),
        //         file_name.as_str(),
        //     );
        // }

        assert_eq!(
            include_bytes!("../../../test_data/raster/cloud_optimized_geotiff_timestep_0_from_stream_compressed.tiff") as &[u8],
            bytes.pop().expect("bytes should have length 3").as_slice()
        );
        assert_eq!(
            include_bytes!("../../../test_data/raster/cloud_optimized_geotiff_timestep_1_from_stream_compressed.tiff") as &[u8],
            bytes.pop().expect("bytes should have length 3").as_slice()
        );
        assert_eq!(
            include_bytes!("../../../test_data/raster/cloud_optimized_geotiff_timestep_2_from_stream_compressed.tiff") as &[u8],
            bytes.pop().expect("bytes should have length 3").as_slice()
        );

        // TODO: check programmatically that intermediate file is gone
    }

    #[tokio::test]
    async fn cloud_optimized_geotiff_multiple_timesteps_from_stream_wrong_request() {
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

        let bytes = single_timestep_raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                query_bbox,
                SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
                Coordinate2D::default(), // this is the default tiling strategy origin
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 7_776_000_000).unwrap(),
            ),
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
            Box::pin(futures::future::pending()),
            tiling_specification,
        )
        .await;

        assert!(bytes.is_err());
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

        let bytes = single_timestep_raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                query_bbox,
                SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
                Coordinate2D::default(), // this is the default tiling strategy origin
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            ),
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
            Box::pin(futures::future::pending()),
            tiling_specification,
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

        let bytes = single_timestep_raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                query_bbox,
                SpatialResolution::new_unchecked(
                    0.228_716_645_489_199_48,
                    0.226_407_384_987_887_26,
                ),
                Coordinate2D::default(),
                TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000).unwrap(),
            ),
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
            Box::pin(futures::future::pending()),
            tiling_specification,
        )
        .await;

        assert!(bytes.is_ok());
    }
}
