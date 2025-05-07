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
use gdal::raster::{Buffer, GdalType, RasterBand, RasterCreationOptions};
use gdal::{Dataset, DriverManager, Metadata};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BandSelection, DateTimeParseFormat, QueryRectangle, RasterQueryRectangle,
    SpatialPartition2D, TimeInterval,
};
use geoengine_datatypes::primitives::{CacheHint, CacheTtlSeconds};
use geoengine_datatypes::raster::{
    ChangeGridBounds, EmptyGrid2D, GeoTransform, GridBlit, GridIdx, GridIdx2D, GridSize,
    MapElements, MaskedGrid2D, NoDataValueGrid, Pixel, RasterTile2D, TilingSpecification,
    TilingStrategy,
};
use geoengine_datatypes::spatial_reference::SpatialReference;
use log::debug;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::convert::TryInto;
use std::path::Path;
use std::path::PathBuf;

use super::{abortable_query_execution, spawn_blocking};

/// consume a raster stream and write it to a geotiff file, one band for each time step
/// Note: the entire process is done in memory, and will take 2x the size of the raster
///       time series
#[allow(clippy::too_many_arguments)]
pub async fn raster_stream_to_multiband_geotiff_bytes<T, C: QueryContext + 'static>(
    processor: Box<dyn RasterQueryProcessor<RasterType = T>>,
    query_rect: RasterQueryRectangle,
    mut query_ctx: C,
    gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
    gdal_tiff_options: GdalGeoTiffOptions,
    tile_limit: Option<usize>,
    conn_closed: BoxFuture<'_, ()>,
    tiling_specification: TilingSpecification,
) -> Result<(Vec<u8>, CacheHint)>
where
    T: Pixel + GdalType,
{
    let query_abort_trigger = query_ctx.abort_trigger()?;

    let tiles = abortable_query_execution(
        consume_stream_into_vec(processor, query_rect.clone(), query_ctx, tile_limit),
        conn_closed,
        query_abort_trigger,
    )
    .await?;

    let (initial_tile_time, file_path, dataset, writer) = create_multiband_dataset_and_writer(
        &tiles,
        &query_rect,
        tiling_specification,
        gdal_tiff_options,
        gdal_tiff_metadata,
    )?;

    let cache_hint = spawn_blocking(move || {
        let mut band_idx = 1;
        let mut time = initial_tile_time;

        let mut cache_hint = CacheHint::max_duration();

        for tile in tiles {
            if tile.time != time {
                // new time step => next band
                time = tile.time;
                band_idx += 1;

                let mut band = dataset.rasterband(band_idx)?;

                band.set_metadata_item(
                    "start",
                    &time.start().as_datetime_string_with_millis(),
                    "time",
                )?;
                band.set_metadata_item(
                    "end",
                    &time.end().as_datetime_string_with_millis(),
                    "time",
                )?;
            }

            cache_hint.merge_with(&tile.cache_hint);

            writer.write_tile_into_band(tile, dataset.rasterband(band_idx)?)?;
        }

        Result::<CacheHint, Error>::Ok(cache_hint)
    })
    .await??;

    Ok((
        gdal::vsi::get_vsi_mem_file_bytes_owned(file_path)?,
        cache_hint,
    ))
}

fn create_multiband_dataset_and_writer<T>(
    tiles: &[RasterTile2D<T>],
    query_rect: &QueryRectangle<SpatialPartition2D, BandSelection>,
    tiling_specification: TilingSpecification,
    gdal_tiff_options: GdalGeoTiffOptions,
    gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
) -> Result<(TimeInterval, PathBuf, Dataset, GdalDatasetWriter<T>), Error>
where
    T: Pixel + GdalType,
{
    let (initial_tile_info, initial_tile_time) = {
        let tile = &tiles
            .first()
            .expect("tiles should not be empty because query rectangle in not empty");
        (tile.tile_information(), tile.time)
    };

    let strat = TilingStrategy {
        tile_size_in_pixels: initial_tile_info.tile_size_in_pixels,
        geo_transform: initial_tile_info.global_geo_transform,
    };
    let num_tiles_per_timestep = strat
        .tile_grid_box(query_rect.spatial_bounds)
        .number_of_elements();
    let num_timesteps = tiles.len() / num_tiles_per_timestep;

    let x_pixel_size = query_rect.spatial_resolution.x;
    let y_pixel_size = query_rect.spatial_resolution.y;
    let width = (query_rect.spatial_bounds.size_x() / x_pixel_size).ceil() as usize;
    let height = (query_rect.spatial_bounds.size_y() / y_pixel_size).ceil() as usize;
    let output_geo_transform = GeoTransform::new(
        query_rect.spatial_bounds.upper_left(),
        x_pixel_size,
        -y_pixel_size,
    );

    let global_geo_transform = tiling_specification
        .strategy(x_pixel_size, -y_pixel_size)
        .geo_transform;
    let window_start =
        global_geo_transform.coordinate_to_grid_idx_2d(query_rect.spatial_bounds.upper_left());
    let window_end = window_start + GridIdx2D::from([height as isize, width as isize]);

    let uncompressed_byte_size = width * height * std::mem::size_of::<T>();

    let use_big_tiff =
        gdal_tiff_options.force_big_tiff || uncompressed_byte_size >= BIG_TIFF_BYTE_THRESHOLD;

    let gdal_compression_num_threads = gdal_tiff_options.compression_num_threads.to_string();

    let options = create_gdal_tiff_options(
        &gdal_compression_num_threads,
        gdal_tiff_options.as_cog,
        use_big_tiff,
    )?;

    let driver = DriverManager::get_driver_by_name("GTiff")?;

    let file_path = PathBuf::from(format!("/vsimem/{}/", uuid::Uuid::new_v4()));

    let gdal_config_options = if gdal_tiff_metadata.no_data_value.is_none() {
        // If we want to write a mask into the geotiff we need to do that internaly because of vismem.
        Some(vec![(
            "GDAL_TIFF_INTERNAL_MASK".to_string(),
            "YES".to_string(),
        )])
    } else {
        None
    };

    let option_vars = gdal_config_options
        .as_deref()
        .map(TemporaryGdalThreadLocalConfigOptions::new);

    let mut dataset = driver.create_with_band_type_with_options::<T, _>(
        &file_path,
        width,
        height,
        num_timesteps,
        &options,
    )?;
    dataset.set_spatial_ref(&gdal_tiff_metadata.spatial_reference.try_into()?)?;
    dataset.set_geo_transform(&output_geo_transform.into())?;

    for band_idx in 0..dataset.raster_count() {
        let mut band = dataset.rasterband(band_idx + 1)?;
        if let Some(no_data) = gdal_tiff_metadata.no_data_value {
            band.set_no_data_value(Some(no_data))?;
        } else {
            // only allowed option for internal masks
            band.create_mask_band(true)?;
            break;
        }
    }

    let writer = GdalDatasetWriter::<T> {
        gdal_tiff_options,
        gdal_tiff_metadata,
        _output_bounds: query_rect.spatial_bounds,
        output_geo_transform,
        use_big_tiff,
        _type: Default::default(),
        window_start,
        window_end,
    };

    drop(option_vars);

    Ok((initial_tile_time, file_path, dataset, writer))
}

async fn consume_stream_into_vec<T, C: QueryContext + 'static>(
    processor: Box<dyn RasterQueryProcessor<RasterType = T>>,
    query_rect: geoengine_datatypes::primitives::QueryRectangle<SpatialPartition2D, BandSelection>,
    query_ctx: C,
    tile_limit: Option<usize>,
) -> Result<Vec<RasterTile2D<T>>>
where
    T: Pixel + GdalType,
{
    let mut tile_stream = processor
        .raster_query(query_rect, &query_ctx)
        .await?
        .enumerate();
    let mut tiles = Vec::new();
    while let Some((tile_index, tile)) = tile_stream.next().await {
        if tile_limit.map_or_else(|| false, |limit| tile_index >= limit) {
            return Err(Error::TileLimitExceeded {
                limit: tile_limit.expect("limit exist because it is exceeded"),
            });
        }

        tiles.push(tile?);
    }
    Ok(tiles)
}

#[allow(clippy::too_many_arguments, clippy::missing_panics_doc)]
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

#[allow(clippy::too_many_arguments, clippy::missing_panics_doc)]
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
    // TODO: support multi band geotiffs
    ensure!(
        query_rect.attributes.count() == 1,
        crate::error::OperationDoesNotSupportMultiBandQueriesYet {
            operation: "raster_stream_to_geotiff"
        }
    );

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
        &query_rect,
        gdal_tiff_metadata,
        gdal_tiff_options,
        gdal_config_options,
    ));

    let tile_stream = processor
        .raster_query(query_rect.clone(), &query_ctx)
        .await?;

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
    raster_band_index: usize,
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
        query_rect: &RasterQueryRectangle,
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
                format: DateTimeParseFormat::custom("%Y-%m-%dT%H-%M-%S-%3f".to_string()),
                reference: TimeReference::Start,
            },
        };

        let file_path = file_path.join("raster.tiff");
        let intermediate_file_path = file_path.with_extension(INTERMEDIATE_FILE_SUFFIX);

        let x_pixel_size = query_rect.spatial_resolution.x;
        let y_pixel_size = query_rect.spatial_resolution.y;
        let width = (query_rect.spatial_bounds.size_x() / x_pixel_size).ceil() as u32;
        let height = (query_rect.spatial_bounds.size_y() / y_pixel_size).ceil() as u32;

        let output_geo_transform = GeoTransform::new(
            query_rect.spatial_bounds.upper_left(),
            x_pixel_size,
            -y_pixel_size,
        );

        let intermediate_dataset_parameters = GdalDatasetParameters {
            file_path: intermediate_file_path,
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: query_rect.spatial_bounds.upper_left(),
                x_pixel_size,
                y_pixel_size: -y_pixel_size,
            },
            width: width as usize,
            height: height as usize,
            file_not_found_handling: FileNotFoundHandling::Error,
            no_data_value: None, // `None` will let the GdalSource detect the correct no-data value.
            properties_mapping: None, // TODO: add properties
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
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
                width,
                height,
                use_big_tiff,
                path_with_placeholder,
                gdal_config_options,
                intermediate_dataset_parameters,
            },
            dataset_writer: GdalDatasetWriter {
                gdal_tiff_options,
                gdal_tiff_metadata,
                _output_bounds: query_rect.spatial_bounds,
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
        )?;

        let mut dataset = driver.create_with_band_type_with_options::<P, _>(
            &intermediate_dataset_metadata
                .intermediate_dataset_parameters
                .file_path,
            intermediate_dataset_metadata.width as usize,
            intermediate_dataset_metadata.height as usize,
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
        query_rect: &RasterQueryRectangle,
        gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
        gdal_tiff_options: GdalGeoTiffOptions,
        gdal_config_options: Option<Vec<(String, String)>>,
    ) -> Self {
        let x_pixel_size = query_rect.spatial_resolution.x;
        let y_pixel_size = query_rect.spatial_resolution.y;

        let width = (query_rect.spatial_bounds.size_x() / x_pixel_size).ceil() as u32;
        let height = (query_rect.spatial_bounds.size_y() / y_pixel_size).ceil() as u32;

        let global_geo_transform = tiling_specification
            .strategy(x_pixel_size, -y_pixel_size)
            .geo_transform;

        let window_start =
            global_geo_transform.coordinate_to_grid_idx_2d(query_rect.spatial_bounds.upper_left());

        let window_end = window_start + GridIdx2D::from([height as isize, width as isize]);

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
            .is_none_or(|x| x.time_interval != time_interval)
        {
            if let Some(intermediate_dataset) = self.intermediate_dataset.take() {
                self.dataset_writer.finish_dataset(intermediate_dataset)?;
            }
            let dataset_path = self
                .create_meta
                .path_with_placeholder
                .translate_path_for_interval(time_interval)?;

            let mut dataset_parameters = self.create_meta.intermediate_dataset_parameters.clone();
            dataset_parameters.file_path.clone_from(&dataset_path);
            self.result.push(GdalLoadingInfoTemporalSlice {
                time: time_interval,
                params: Some(dataset_parameters),
                cache_ttl: CacheTtlSeconds::max(), // not relevant for writing tiffs, but required for persistent datasets. Since persistent datasets are constant, we can set this to max (for now)
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

        let mut buffer = Buffer::new(window_size, no_data_value_grid.inner_grid.data); // TODO: also write mask!

        raster_band.write(window, window_size, &mut buffer)?;
        Ok(())
    }

    fn write_masked_data_grid(
        masked_grid: MaskedGrid2D<P>,
        window: (isize, isize),
        window_size: (usize, usize),
        mut raster_band: RasterBand,
    ) -> Result<()> {
        // Write the MaskedGrid data and mask if no no-data value is set.
        let mut data_buffer = Buffer::new(window_size, masked_grid.inner_grid.data);

        raster_band.write(window, window_size, &mut data_buffer)?;

        // No-data masks are described by the rasterio docs as:
        // "One is the the valid data mask from GDAL, an unsigned byte array with the same number of rows and columns as the dataset in which non-zero elements (typically 255) indicate that the corresponding data elements are valid. Other elements are invalid, or nodata elements."

        let mask_grid_gdal_values = masked_grid
            .validity_mask
            .map_elements(|is_valid| if is_valid { 255_u8 } else { 0 }); // TODO: investigate if we can transmute the vec of bool to u8.
        let mut mask_buffer = Buffer::new(window_size, mask_grid_gdal_values.data);

        let mut mask_band = raster_band.open_mask_band()?;
        mask_band.write(window, window_size, &mut mask_buffer)?;

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
) -> Result<RasterCreationOptions> {
    let mut options = RasterCreationOptions::new();
    options.add_name_value("COMPRESS", COMPRESSION_FORMAT)?;
    options.add_name_value("TILED", "YES")?;
    options.add_name_value("ZLEVEL", COMPRESSION_LEVEL)?;
    options.add_name_value("NUM_THREADS", compression_num_threads)?;
    options.add_name_value("INTERLEAVE", "BAND")?;

    if as_cog {
        // COGs require a block size of 512x512, so we enforce it now so that we do the work only once.
        options.add_name_value("BLOCKXSIZE", COG_BLOCK_SIZE)?;
        options.add_name_value("BLOCKYSIZE", COG_BLOCK_SIZE)?;
    }

    if as_big_tiff {
        options.add_name_value("BIGTIFF", "YES")?;
    }

    Ok(options)
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

    let mut options = RasterCreationOptions::new();
    options.add_name_value("COMPRESS", COMPRESSION_FORMAT)?;
    options.add_name_value("TILED", "YES")?;
    options.add_name_value("NUM_THREADS", num_threads)?;
    options.add_name_value("BLOCKSIZE", COG_BLOCK_SIZE)?;

    if as_big_tiff {
        options.add_name_value("BIGTIFF", "YES")?;
    }

    input_dataset.create_copy(&output_driver, output_file_path, &options)?;

    drop(input_dataset);

    input_driver.delete(input_file_path)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::RasterResultDescriptor;
    use crate::mock::MockRasterSourceProcessor;
    use crate::util::gdal::gdal_open_dataset;
    use crate::{
        engine::MockQueryContext, source::GdalSourceProcessor, util::gdal::create_ndvi_meta_data,
    };
    use geoengine_datatypes::primitives::CacheHint;
    use geoengine_datatypes::primitives::{DateTime, Duration};
    use geoengine_datatypes::raster::{Grid, RasterDataType};
    use geoengine_datatypes::test_data;
    use geoengine_datatypes::util::{
        ImageFormat, assert_image_equals, assert_image_equals_with_format,
    };
    use geoengine_datatypes::{
        primitives::{Coordinate2D, SpatialPartition2D, SpatialResolution, TimeInterval},
        raster::TilingSpecification,
        util::test::TestDefault,
    };
    use std::marker::PhantomData;
    use std::ops::Add;

    #[tokio::test]
    async fn geotiff_with_no_data_from_stream() {
        let ctx = MockQueryContext::test_default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

        let metadata = create_ndvi_meta_data();

        let gdal_source = GdalSourceProcessor::<u8> {
            result_descriptor: RasterResultDescriptor::with_datatype_and_num_bands(
                RasterDataType::U8,
                1,
            ),
            tiling_specification,
            meta_data: Box::new(metadata),
            _phantom_data: PhantomData,
        };

        let query_bbox = SpatialPartition2D::new((-10., 80.).into(), (50., 20.).into()).unwrap();

        let bytes = single_timestep_raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_bbox,
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
                attributes: BandSelection::first(),
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
            Box::pin(futures::future::pending()),
            tiling_specification,
        )
        .await
        .unwrap();

        // geoengine_datatypes::util::test::save_test_bytes(
        //    &bytes,
        //    "../test_data/raster/geotiff_from_stream_compressed.tiff",
        // );

        assert_image_equals(
            test_data!("raster/geotiff_from_stream_compressed.tiff"),
            &bytes,
        );
    }

    #[tokio::test]
    async fn geotiff_with_mask_from_stream() {
        let ctx = MockQueryContext::test_default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

        let metadata = create_ndvi_meta_data();

        let gdal_source = GdalSourceProcessor::<u8> {
            result_descriptor: RasterResultDescriptor::with_datatype_and_num_bands(
                RasterDataType::U8,
                1,
            ),
            tiling_specification,
            meta_data: Box::new(metadata),
            _phantom_data: PhantomData,
        };

        let query_bbox = SpatialPartition2D::new((-10., 80.).into(), (50., 20.).into()).unwrap();

        let bytes = single_timestep_raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_bbox,
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
                attributes: BandSelection::first(),
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
            Box::pin(futures::future::pending()),
            tiling_specification,
        )
        .await
        .unwrap();

        assert_image_equals(
            test_data!("raster/geotiff_with_mask_from_stream_compressed.tiff"),
            &bytes,
        );
    }

    #[tokio::test]
    async fn geotiff_big_tiff_from_stream() {
        let ctx = MockQueryContext::test_default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

        let metadata = create_ndvi_meta_data();

        let gdal_source = GdalSourceProcessor::<u8> {
            result_descriptor: RasterResultDescriptor::with_datatype_and_num_bands(
                RasterDataType::U8,
                1,
            ),
            tiling_specification,
            meta_data: Box::new(metadata),
            _phantom_data: PhantomData,
        };

        let query_bbox = SpatialPartition2D::new((-10., 80.).into(), (50., 20.).into()).unwrap();

        let bytes = single_timestep_raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_bbox,
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
                attributes: BandSelection::first(),
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
            Box::pin(futures::future::pending()),
            tiling_specification,
        )
        .await
        .unwrap();

        // geoengine_datatypes::util::test::save_test_bytes(
        //    &bytes,
        //    "../test_data/raster/geotiff_big_tiff_from_stream_compressed.tiff",
        // );

        assert_image_equals_with_format(
            test_data!("raster/geotiff_big_tiff_from_stream_compressed.tiff"),
            &bytes,
            ImageFormat::Tiff,
        );
    }

    #[tokio::test]
    async fn cloud_optimized_geotiff_big_tiff_from_stream() {
        let ctx = MockQueryContext::test_default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());

        let metadata = create_ndvi_meta_data();

        let gdal_source = GdalSourceProcessor::<u8> {
            result_descriptor: RasterResultDescriptor::with_datatype_and_num_bands(
                RasterDataType::U8,
                1,
            ),
            tiling_specification,
            meta_data: Box::new(metadata),
            _phantom_data: PhantomData,
        };

        let query_bbox = SpatialPartition2D::new((-10., 80.).into(), (50., 20.).into()).unwrap();

        let bytes = single_timestep_raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_bbox,
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
                attributes: BandSelection::first(),
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
            Box::pin(futures::future::pending()),
            tiling_specification,
        )
        .await
        .unwrap();

        // geoengine_datatypes::util::test::save_test_bytes(
        //     &bytes,
        //     "../test_data/raster/cloud_optimized_geotiff_big_tiff_from_stream_compressed.tiff",
        // );

        assert_image_equals_with_format(
            test_data!("raster/cloud_optimized_geotiff_big_tiff_from_stream_compressed.tiff"),
            &bytes,
            ImageFormat::Tiff,
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
            result_descriptor: RasterResultDescriptor::with_datatype_and_num_bands(
                RasterDataType::U8,
                1,
            ),
            tiling_specification,
            meta_data: Box::new(metadata),
            _phantom_data: PhantomData,
        };

        let query_bbox = SpatialPartition2D::new((-10., 80.).into(), (50., 20.).into()).unwrap();

        let bytes = single_timestep_raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_bbox,
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
                attributes: BandSelection::first(),
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
            Box::pin(futures::future::pending()),
            tiling_specification,
        )
        .await
        .unwrap();

        // geoengine_datatypes::util::test::save_test_bytes(
        //     &bytes,
        //     "../test_data/raster/cloud_optimized_geotiff_from_stream_compressed.tiff",
        // );

        assert_image_equals(
            test_data!("raster/cloud_optimized_geotiff_from_stream_compressed.tiff"),
            &bytes,
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
            result_descriptor: RasterResultDescriptor::with_datatype_and_num_bands(
                RasterDataType::U8,
                1,
            ),
            tiling_specification,
            meta_data: Box::new(metadata),
            _phantom_data: PhantomData,
        };

        let query_bbox = SpatialPartition2D::new((-10., 80.).into(), (50., 20.).into()).unwrap();

        let mut bytes = raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_bbox,
                time_interval: TimeInterval::new(
                    1_388_534_400_000,
                    1_388_534_400_000 + 7_776_000_000,
                )
                .unwrap(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
                attributes: BandSelection::first(),
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

        assert_image_equals(
            test_data!("raster/cloud_optimized_geotiff_timestep_0_from_stream_compressed.tiff"),
            bytes.pop().expect("bytes should have length 3").as_slice(),
        );

        assert_image_equals(
            test_data!("raster/cloud_optimized_geotiff_timestep_1_from_stream_compressed.tiff"),
            bytes.pop().expect("bytes should have length 3").as_slice(),
        );

        assert_image_equals(
            test_data!("raster/cloud_optimized_geotiff_timestep_2_from_stream_compressed.tiff"),
            bytes.pop().expect("bytes should have length 3").as_slice(),
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
            result_descriptor: RasterResultDescriptor::with_datatype_and_num_bands(
                RasterDataType::U8,
                1,
            ),
            tiling_specification,
            meta_data: Box::new(metadata),
            _phantom_data: PhantomData,
        };

        let query_bbox = SpatialPartition2D::new((-10., 80.).into(), (50., 20.).into()).unwrap();

        let bytes = single_timestep_raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_bbox,
                time_interval: TimeInterval::new(
                    1_388_534_400_000,
                    1_388_534_400_000 + 7_776_000_000,
                )
                .unwrap(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
                attributes: BandSelection::first(),
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
            result_descriptor: RasterResultDescriptor::with_datatype_and_num_bands(
                RasterDataType::U8,
                1,
            ),
            tiling_specification,
            meta_data: Box::new(metadata),
            _phantom_data: PhantomData,
        };

        let query_bbox = SpatialPartition2D::new((-10., 80.).into(), (50., 20.).into()).unwrap();

        let bytes = single_timestep_raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_bbox,
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    query_bbox.size_x() / 600.,
                    query_bbox.size_y() / 600.,
                ),
                attributes: BandSelection::first(),
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
            result_descriptor: RasterResultDescriptor::with_datatype_and_num_bands(
                RasterDataType::U8,
                1,
            ),
            tiling_specification,
            meta_data: Box::new(metadata),
            _phantom_data: PhantomData,
        };

        let query_bbox =
            SpatialPartition2D::new((-180., -66.227_224_576_271_84).into(), (180., -90.).into())
                .unwrap();

        let bytes = single_timestep_raster_stream_to_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_bbox,
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    0.228_716_645_489_199_48,
                    0.226_407_384_987_887_26,
                ),
                attributes: BandSelection::first(),
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
            Box::pin(futures::future::pending()),
            tiling_specification,
        )
        .await;

        assert!(bytes.is_ok());
    }

    fn generate_time_intervals(
        start_time: DateTime,
        time_step: Duration,
        num_time_step: usize,
    ) -> Result<Vec<TimeInterval>> {
        let mut result = Vec::new();
        let mut counter = num_time_step;
        let mut tmp_start_time = start_time;
        while counter > 0 {
            let end_time = tmp_start_time.add(time_step);
            let start_interval = TimeInterval::new(tmp_start_time, end_time)?;
            result.push(start_interval);
            counter -= 1;
            tmp_start_time = end_time;
        }
        Ok(result)
    }

    async fn test_output_for_time_interval(
        num_time_steps: usize,
        base_start_time: DateTime,
        time_step: Duration,
        file_suffixes: Vec<&str>,
    ) {
        let time_intervals =
            generate_time_intervals(base_start_time, time_step, num_time_steps).unwrap();
        let data = vec![
            RasterTile2D {
                time: *time_intervals.first().unwrap(),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: *time_intervals.get(1).unwrap(),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let query_time = TimeInterval::new(data[0].time.start(), data[1].time.end()).unwrap();

        let ctx = MockQueryContext::test_default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [600, 600].into());
        let processor = MockRasterSourceProcessor {
            result_descriptor: RasterResultDescriptor::with_datatype_and_num_bands(
                RasterDataType::U8,
                1,
            ),
            data,
            tiling_specification,
        }
        .boxed();

        let query_rectangle = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new((0., 2.).into(), (2., 0.).into()).unwrap(),
            time_interval: query_time,
            spatial_resolution: GeoTransform::test_default().spatial_resolution(),
            attributes: BandSelection::first(),
        };

        let file_path = PathBuf::from(format!("/vsimem/{}/", uuid::Uuid::new_v4()));
        let expected_paths = file_suffixes
            .iter()
            .map(|x| file_path.join(x))
            .collect::<Vec<_>>();
        let result = raster_stream_to_geotiff(
            &file_path,
            processor,
            query_rectangle,
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

        for x in result.iter().zip(expected_paths) {
            assert_eq!(x.0.params.as_ref().unwrap().file_path, x.1);
        }

        for x in result.iter().zip(time_intervals) {
            assert_eq!(x.0.time, x.1);
        }
    }

    #[tokio::test]
    async fn output_time_intervals_for_varying_time_step_granularity() {
        let num_time_steps = 2;

        let base_start_time = DateTime::new_utc(2020, 7, 30, 11, 50, 1);

        let time_steps = [
            Duration::days(365),
            Duration::days(31),
            Duration::days(1),
            Duration::hours(1),
            Duration::minutes(1),
            Duration::seconds(1),
            Duration::milliseconds(1),
        ];

        let all_expected_file_suffixes = vec![
            vec![
                "raster_2020-07-30T11-50-01-000.tiff", //1596109801000
                "raster_2021-07-30T11-50-01-000.tiff", //1627645801000
            ], //year
            vec![
                "raster_2020-07-30T11-50-01-000.tiff", //1596109801000
                "raster_2020-08-30T11-50-01-000.tiff", //1598788201000
            ], //month
            vec![
                "raster_2020-07-30T11-50-01-000.tiff",
                "raster_2020-07-31T11-50-01-000.tiff",
            ], //day
            vec![
                "raster_2020-07-30T11-50-01-000.tiff",
                "raster_2020-07-30T12-50-01-000.tiff",
            ], //hour
            vec![
                "raster_2020-07-30T11-50-01-000.tiff",
                "raster_2020-07-30T11-51-01-000.tiff",
            ], //minute
            vec![
                "raster_2020-07-30T11-50-01-000.tiff",
                "raster_2020-07-30T11-50-02-000.tiff",
            ], //second
            vec![
                "raster_2020-07-30T11-50-01-000.tiff",
                "raster_2020-07-30T11-50-01-001.tiff",
            ], //millisecond
        ];

        for (time_step, file_suffixes) in time_steps.iter().zip(all_expected_file_suffixes) {
            test_output_for_time_interval(
                num_time_steps,
                base_start_time,
                *time_step,
                file_suffixes,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn multi_band_geotriff() {
        let ctx = MockQueryContext::test_default();
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [512, 512].into());

        let metadata = create_ndvi_meta_data();

        let gdal_source = GdalSourceProcessor::<u8> {
            result_descriptor: RasterResultDescriptor::with_datatype_and_num_bands(
                RasterDataType::U8,
                1,
            ),
            tiling_specification,
            meta_data: Box::new(metadata),
            _phantom_data: PhantomData,
        };

        let query_bbox = SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap();

        let (mut bytes, _) = raster_stream_to_multiband_geotiff_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_bbox,
                // 1.1.2014 - 1.4.2014
                time_interval: TimeInterval::new(1_388_534_400_000, 1_396_306_800_000).unwrap(),
                spatial_resolution: SpatialResolution::new_unchecked(0.1, 0.1),
                attributes: BandSelection::first(),
            },
            ctx,
            GdalGeoTiffDatasetMetadata {
                no_data_value: None,
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
        .await
        .unwrap();

        let file_path = PathBuf::from(format!("/vsimem/{}/", uuid::Uuid::new_v4()));
        let _mem_file =
            gdal::vsi::create_mem_file_from_ref(&file_path, bytes.as_mut_slice()).unwrap();
        let ds = gdal_open_dataset(&file_path).unwrap();

        // three bands for Jan, Feb, Mar
        assert_eq!(ds.raster_count(), 3);

        // TODO: check that the time is encoded in the geotiff band metadata

        drop(ds);
    }
}
