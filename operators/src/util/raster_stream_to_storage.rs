use crate::engine::{BoxRasterQueryProcessor, QueryProcessor};
use crate::error::{self};
use crate::source::{
    FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters,
    GdalLoadingInfoTemporalSlice, GdalSourceTimePlaceholder, TimeReference,
};
use crate::util::raster_stream_to_geotiff::{GdalGeoTiffDatasetMetadata, GdalGeoTiffOptions};
use crate::util::{Result, TemporaryGdalThreadLocalConfigOptions};
use crate::{
    engine::{QueryContext, RasterQueryProcessor},
    error::Error,
};
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::{TryFutureExt, TryStreamExt};
use gdal::raster::{Buffer, GdalType, RasterBand, RasterCreationOptions};
use gdal::spatial_ref::SpatialRef;
use gdal::{Dataset, DatasetOptions, DriverManager, GdalOpenFlags};
use geoengine_datatypes::primitives::{
    CacheTtlSeconds, DateTimeParseFormat, RasterQueryRectangle, TimeInterval,
};
use geoengine_datatypes::raster::{
    ChangeGridBounds, GeoTransform, GridBlit, GridBoundingBox2D, GridBounds, GridIntersection,
    GridOrEmpty, GridSize, MapElements, MaskedGrid2D, NoDataValueGrid, Pixel, RasterTile2D,
    TilingStrategy,
};
use std::convert::TryInto;
use std::fmt::Display;
use std::path::Path;
use std::path::PathBuf;
use tokio::sync::mpsc;
use tracing::debug;

use super::abortable_query_execution;

pub struct GdalWriterState {
    current_dataset: Option<(TimeInterval, Dataset)>,
    result: Vec<TimeInterval>,
}

impl Default for GdalWriterState {
    fn default() -> Self {
        Self::new()
    }
}

impl GdalWriterState {
    pub fn new() -> Self {
        Self {
            current_dataset: None,
            result: vec![],
        }
    }

    pub fn set_current_dataset(&mut self, path: TimeInterval, dataset: Dataset) {
        self.current_dataset = Some((path, dataset));
    }

    pub fn take_current_dataset(&mut self) -> Option<(TimeInterval, Dataset)> {
        self.current_dataset.take()
    }

    pub fn push_result(&mut self, time: TimeInterval) {
        self.result.push(time);
    }

    pub fn get_dataset_with_path(&self, path: &TimeInterval) -> Option<&Dataset> {
        self.current_dataset
            .as_ref()
            .filter(|(current_path, _)| current_path == path)
            .map(|(_, dataset)| dataset)
    }

    /// # Panics
    // Only if the dataset is broken.
    pub fn get_or_create_dataset_for_time_interval<P: GdalType>(
        &mut self,
        time_interval: TimeInterval,
        gdal_wrapper: &GdalDatasetWriterWrapper,
    ) -> Result<&mut Dataset> {
        let is_current_time_interval = self
            .current_dataset
            .as_ref()
            .is_some_and(|(current_interval, _)| *current_interval == time_interval);

        if !is_current_time_interval {
            // create new dataset for the new time interval
            let dataset_path = gdal_wrapper
                .path_with_placeholder
                .translate_path_for_interval(time_interval)?;

            // if a dataset exists at the path, open it, otherwise create a new one
            let dataset = if dataset_path.exists() {
                GdalDatasetWriterWrapper::open_existing_file(&dataset_path)?
            } else {
                gdal_wrapper.create_file::<P>(&dataset_path)?
            };

            debug_assert!(
                dataset.raster_count() == 1,
                "dataset should have at one raster band"
            );

            debug_assert!(
                dataset
                    .rasterband(gdal_wrapper.raster_band_index)?
                    .band_type()
                    == P::datatype(),
                "dataset should have one raster band at index 1 and it should be of the correct type {}",
                P::datatype()
            );

            self.set_current_dataset(time_interval, dataset);
            self.push_result(time_interval);
        }

        let mut_dataset = self
            .current_dataset
            .as_mut()
            .expect("dataset should be set");
        Ok(&mut mut_dataset.1)
    }

    pub fn flush_current_dataset(&mut self) -> Result<()> {
        if let Some((path, dataset)) = self.current_dataset.as_mut() {
            debug!("Flushing dataset at path: {}", path);
            dataset.flush_cache()?;
        }
        Ok(())
    }

    pub fn close_current_dataset(&mut self) {
        let tmp = self.current_dataset.take();
        if let Some((path, dataset)) = tmp {
            debug!("Closing dataset at path: {}", path);
            drop(dataset);
        }
    }

    fn results(self, gdal_wrapper: &GdalDatasetWriterWrapper) -> Vec<GdalLoadingInfoTemporalSlice> {
        self.result
            .into_iter()
            .map(|time_interval| {
                let dataset_path = gdal_wrapper
                .path_with_placeholder
                .translate_path_for_interval(time_interval)
                .expect(
                    "translating path should work since it worked before when creating the dataset",
                );

                let mut params = gdal_wrapper.dataset_parameters.clone();
                params.file_path = dataset_path;

                GdalLoadingInfoTemporalSlice {
                    time: time_interval,
                    params: Some(params),
                    cache_ttl: CacheTtlSeconds::max(),
                }
            })
            .collect()
    }
}

pub struct GdalDatasetWriterWrapper {
    raster_band_index: usize,
    width: u32,
    height: u32,
    path_with_placeholder: PathWithPlaceholder,
    gdal_config_options: Option<Vec<(String, String)>>,
    dataset_parameters: GdalDatasetParameters,
    gdal_driver_code: Option<String>,
    // From GdalDatasetWriter
    gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
    gdal_tiff_options: GdalGeoTiffOptions,
    output_pixel_grid_bounds: GridBoundingBox2D,
    output_geo_transform: GeoTransform,
}

impl GdalDatasetWriterWrapper {
    fn new<P: Pixel>(
        file_path: &Path,
        query_rect: &RasterQueryRectangle,
        gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
        gdal_tiff_options: GdalGeoTiffOptions,
        gdal_config_options: Option<Vec<(String, String)>>,
        tiling_strategy: TilingStrategy,
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

        let width = query_rect.spatial_bounds().axis_size_x();
        let height = query_rect.spatial_bounds().axis_size_y();

        let output_pixel_grid_bounds = query_rect.spatial_bounds();

        let out_geo_transform_origin = tiling_strategy
            .geo_transform
            .grid_idx_to_pixel_upper_left_coordinate_2d(query_rect.spatial_bounds().min_index());

        let output_geo_transform = GeoTransform::new(
            out_geo_transform_origin,
            tiling_strategy.geo_transform.x_pixel_size(),
            tiling_strategy.geo_transform.y_pixel_size(),
        );

        let output_gdal_geo_transform = GdalDatasetGeoTransform {
            origin_coordinate: out_geo_transform_origin,
            x_pixel_size: tiling_strategy.geo_transform.x_pixel_size(),
            y_pixel_size: tiling_strategy.geo_transform.y_pixel_size(),
        };

        let intermediate_dataset_parameters = GdalDatasetParameters {
            file_path: intermediate_file_path,
            rasterband_channel: 1,
            geo_transform: output_gdal_geo_transform,
            width,
            height,
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

        let gdal_tiff_options = GdalGeoTiffOptions {
            force_big_tiff: use_big_tiff,
            ..gdal_tiff_options
        };

        let rasterband_index = 1;

        Self {
            raster_band_index: rasterband_index,
            width: width as u32,
            height: height as u32,
            path_with_placeholder,
            gdal_config_options,
            dataset_parameters: intermediate_dataset_parameters,
            gdal_driver_code: None,
            gdal_tiff_metadata,
            gdal_tiff_options,
            output_pixel_grid_bounds,
            output_geo_transform,
        }
    }

    fn create_file<P: GdalType>(&self, path: &Path) -> Result<Dataset> {
        let driver =
            DriverManager::get_driver_by_name(self.gdal_driver_code.as_deref().unwrap_or("GTiff"))?;

        let options = create_gdal_tiff_options(self.gdal_tiff_options)?;

        let mut dataset = driver.create_with_band_type_with_options::<P, _>(
            path,
            self.width as usize,
            self.height as usize,
            1,
            &options,
        )?;
        let spatial_ref: SpatialRef = self.gdal_tiff_metadata.spatial_reference.try_into()?;
        dataset.set_spatial_ref(&spatial_ref)?;
        dataset.set_geo_transform(&self.output_geo_transform.into())?;
        let mut band = dataset.rasterband(self.raster_band_index)?;

        // Check if the gdal_tiff_metadata no-data value is set.
        // If it is set, set the no-data value for the output geotiff.
        // Otherwise add a mask band to the output geotiff.
        if let Some(no_data) = self.gdal_tiff_metadata.no_data_value {
            band.set_no_data_value(Some(no_data))?;
        } else {
            band.create_mask_band(true)?;
        }

        Ok(dataset)
    }

    fn open_existing_file(path: &Path) -> Result<Dataset> {
        let dataset = Dataset::open_ex(
            path,
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_UPDATE | GdalOpenFlags::GDAL_OF_RASTER,
                ..Default::default()
            },
        )?;
        Ok(dataset)
    }

    fn write_tile_into_band<P: GdalType + Pixel>(
        &self,
        tile: RasterTile2D<P>,
        raster_band: RasterBand,
    ) -> Result<()> {
        let tile_info = tile.tile_information();
        let tile_grid_bounds = tile_info.global_pixel_bounds();
        let out_data_bounds = self
            .output_pixel_grid_bounds
            .intersection(&tile_grid_bounds);

        if out_data_bounds.is_none() {
            return Ok(());
        }

        let out_data_bounds = out_data_bounds.expect("was checked before");
        let mut write_buffer_grid = GridOrEmpty::<_, P>::new_empty_shape(out_data_bounds);
        write_buffer_grid.grid_blit_from(&tile.into_inner_positioned_grid());

        let window_start = out_data_bounds.min_index() - self.output_pixel_grid_bounds.min_index();
        let window = (window_start.x(), window_start.y());
        let window_size = (
            write_buffer_grid.shape_ref().axis_size_x(),
            write_buffer_grid.shape_ref().axis_size_y(),
        );
        let grid_array = write_buffer_grid
            .into_materialized_masked_grid()
            .unbounded();

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

    fn write_no_data_value_grid<P: Pixel + GdalType>(
        grid_array: &MaskedGrid2D<P>,
        no_data_value: f64,
        window: (isize, isize),
        window_size: (usize, usize),
        mut raster_band: RasterBand,
    ) -> Result<()> {
        let out_no_data_value_p: P = P::from_(no_data_value);
        let no_data_value_grid = NoDataValueGrid::from_masked_grid(grid_array, out_no_data_value_p);
        let mut buffer = Buffer::new(window_size, no_data_value_grid.inner_grid.data);
        raster_band.write(window, window_size, &mut buffer)?;
        Ok(())
    }

    fn write_masked_data_grid<P: GdalType + Pixel>(
        masked_grid: MaskedGrid2D<P>,
        window: (isize, isize),
        window_size: (usize, usize),
        mut raster_band: RasterBand,
    ) -> Result<()> {
        let mut data_buffer = Buffer::new(window_size, masked_grid.inner_grid.data);
        raster_band.write(window, window_size, &mut data_buffer)?;

        let mask_grid_gdal_values = masked_grid
            .validity_mask
            .map_elements(|is_valid| if is_valid { 255_u8 } else { 0 });
        let mut mask_buffer = Buffer::new(window_size, mask_grid_gdal_values.data);
        let mut mask_band = raster_band.open_mask_band()?;
        mask_band.write(window, window_size, &mut mask_buffer)?;

        Ok(())
    }
}

#[allow(clippy::large_enum_variant)]
pub enum TileMsg<P: Pixel + GdalType> {
    Tile(RasterTile2D<P>),
    ReOpen,
    Flush,
    Finish,
}

#[allow(
    clippy::too_many_arguments,
    clippy::missing_panics_doc,
    clippy::too_many_lines
)]
pub async fn raster_stream_to_geotiff<G: ToGeoTiffProgressConsumer, P, C: QueryContext + 'static>(
    file_path: &Path,
    processor: BoxRasterQueryProcessor<P>,
    query_rect: RasterQueryRectangle,
    mut query_ctx: C,
    gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
    gdal_tiff_options: GdalGeoTiffOptions,
    tile_limit: Option<usize>,
    conn_closed: BoxFuture<'_, ()>,
    progress_consumer: G,
) -> Result<Vec<GdalLoadingInfoTemporalSlice>>
where
    P: Pixel + GdalType,
{
    let query_rect_bands = processor.raster_result_descriptor().bands.count() as usize;

    if query_rect_bands != 1 {
        return Err(Error::OperationDoesNotSupportMultiBandQueriesYet {
            operation: "raster_stream_to_geotiff",
        });
    }

    let query_abort_trigger = query_ctx.abort_trigger()?;

    let tiling_grid = processor
        .result_descriptor()
        .spatial_grid_descriptor()
        .tiling_grid_definition(query_ctx.tiling_specification());

    let tiles_intersecting_qrect = tiling_grid
        .generate_data_tiling_strategy()
        .num_tiles_intersecting_grid_bounds(query_rect.spatial_bounds());

    let file_path = file_path.to_owned();

    let gdal_config_options = if gdal_tiff_metadata.no_data_value.is_none() {
        Some(vec![(
            "GDAL_TIFF_INTERNAL_MASK".to_string(),
            "YES".to_string(),
        )])
    } else {
        None
    };

    let tile_stream = processor
        .raster_query(query_rect.clone(), &query_ctx)
        .await?;

    let gdal_wrapper = GdalDatasetWriterWrapper::new::<P>(
        &file_path,
        &query_rect,
        gdal_tiff_metadata,
        gdal_tiff_options,
        gdal_config_options,
        tiling_grid.generate_data_tiling_strategy(),
    );

    let (tx, mut rx) = mpsc::channel::<TileMsg<P>>(CHANNEL_CAPACITY);

    // Worker thread - runs blocking operations on separate thread pool
    let worker = tokio::task::spawn_blocking(move || {
        tracing::debug!("Worker thread started for writing GeoTIFFs");

        let mut state = GdalWriterState::new();
        let mut tile_count = 0;
        let gdal_wrapper = gdal_wrapper;

        // reverts the thread local configs on drop
        let thread_local_configs = gdal_wrapper
            .gdal_config_options
            .as_deref()
            .map(TemporaryGdalThreadLocalConfigOptions::new)
            .transpose()?;

        while let Some(tile_msg) = rx.blocking_recv() {
            match tile_msg {
                TileMsg::Tile(tile) => {
                    // Process the tile (e.g., write to dataset)
                    // For demonstration, we just print the tile information
                    tile_count += 1;

                    tracing::trace!(
                        "Worker processing tile #{tile_count}: {:?}",
                        tile.tile_information()
                    );

                    let tile_time = tile.time;

                    let dataset = state
                        .get_or_create_dataset_for_time_interval::<P>(tile_time, &gdal_wrapper)?;
                    let raster_band = dataset.rasterband(gdal_wrapper.raster_band_index)?; // TODO: make this configurable

                    gdal_wrapper.write_tile_into_band(tile, raster_band)?;
                }

                TileMsg::ReOpen => {
                    tracing::debug!(
                        "Closing current dataset for re-opening in next iteration. Current tile count: {tile_count}."
                    );
                    state.close_current_dataset();
                }
                TileMsg::Flush => {
                    tracing::debug!(
                        "Flushing current dataset to storage. Current tile count: {tile_count}."
                    );
                    state.flush_current_dataset()?;
                }
                TileMsg::Finish => {
                    tracing::debug!(
                        "Finishing up, closing current dataset. Current tile count: {tile_count}."
                    );
                    break;
                }
            }
        }

        state.close_current_dataset();
        drop(thread_local_configs); // ensure that we drop here

        Ok(state.results(&gdal_wrapper))
    });

    tracing::debug!("Starting to process tile stream and send tiles to worker thread");

    // TODO: maybe replace process tracker with a channel...
    let (total_tiles, pc) = tile_stream
        .inspect_err(|e| tracing::warn!("Writer got Error instead of tile! {e}"))
        .try_fold((0, progress_consumer), |(tile_count, pc), tile| {
            let tx_c = tx.clone();
            let new_tile_count = tile_count + 1;

            tracing::trace!("Received tile #{new_tile_count} for time {:?}", tile.time);

            async move {
                if tile_limit.map_or_else(|| false, |limit| new_tile_count >= limit) {
                    tracing::debug!("TileLimitExceeded: Count: {new_tile_count}");
                    return Err(Error::TileLimitExceeded {
                        limit: tile_limit.expect("limit exist because it is exceeded"),
                    });
                }

                let current_interval = tile.time;
                // TODO: remove expect and handle error properly
                tx_c.send(TileMsg::Tile(tile))
                    .await
                    .map_err(|_| Error::ChannelSend)?;

                if new_tile_count % 64 == 0 {
                    tx_c.send(TileMsg::Flush)
                        .await
                        .map_err(|_| Error::ChannelSend)?;
                }

                if new_tile_count % 256 == 0 {
                    tx_c.send(TileMsg::ReOpen)
                        .await
                        .map_err(|_| Error::ChannelSend)?;
                }

                pc.consume_progress(ToGeoTiffProgress::Processing {
                    tiles_per_timestep: tiles_intersecting_qrect,
                    processed_tiles: tile_count,
                    current_time_step: current_interval,
                })
                .await;

                Ok((new_tile_count, pc))
            }
        })
        .await
        .inspect_err(|e| tracing::warn!("Error while sending tiles to writer: {e}"))?;

    tx.send(TileMsg::Finish).await.map_err(|e| {
        tracing::warn!("Error while sending tiles to writer: {e}");
        Error::ChannelSend
    })?;

    drop(tx); // close channel

    let written = worker.map_err(|e| error::Error::TokioJoin { source: e });

    let result = abortable_query_execution(written, conn_closed, query_abort_trigger).await?;

    pc.consume_progress(ToGeoTiffProgress::Done {
        tiles_per_timestep: tiles_intersecting_qrect,
        processed_tiles: total_tiles,
    })
    .await;

    result
}

#[derive(Copy, Clone, Debug)]
pub enum ToGeoTiffProgress {
    Processing {
        tiles_per_timestep: usize,
        current_time_step: TimeInterval,
        processed_tiles: usize,
    },
    Done {
        tiles_per_timestep: usize,
        processed_tiles: usize,
    },
}

impl ToGeoTiffProgress {
    pub fn percent_done(&self) -> f64 {
        match *self {
            Self::Done { .. } => 1.0,
            Self::Processing { .. } => 0.0,
        }
    }

    pub fn percent_done_current_timestep(&self) -> f64 {
        match *self {
            Self::Done { .. } => 1.0,
            Self::Processing {
                tiles_per_timestep,
                current_time_step: _,
                processed_tiles,
            } => (processed_tiles % tiles_per_timestep) as f64 / tiles_per_timestep as f64,
        }
    }
}

impl Display for ToGeoTiffProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Done {
                processed_tiles,
                tiles_per_timestep,
            } => write!(
                f,
                "Done: Total tiles processed: {processed_tiles}; Tiles per time step: {tiles_per_timestep}",
            ),
            Self::Processing {
                tiles_per_timestep,
                current_time_step,
                processed_tiles,
            } => write!(
                f,
                "Processing: Total tiles processed: {}; Tiles per time step: {}; Current {} @ {:.3}%",
                processed_tiles,
                tiles_per_timestep,
                current_time_step,
                (self.percent_done_current_timestep() * 100.)
            ),
        }
    }
}

#[async_trait]
pub trait ToGeoTiffProgressConsumer {
    async fn consume_progress(&self, status: ToGeoTiffProgress);
}

#[async_trait]
impl ToGeoTiffProgressConsumer for () {
    async fn consume_progress(&self, _status: ToGeoTiffProgress) {
        // No-op
    }
}

const CHANNEL_CAPACITY: usize = 16;
const COG_BLOCK_SIZE: &str = "512";
const COMPRESSION_FORMAT: &str = "ZSTD";
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

pub fn create_gdal_tiff_options(opts: GdalGeoTiffOptions) -> Result<RasterCreationOptions> {
    let mut options = RasterCreationOptions::new();
    options.add_name_value("COMPRESS", COMPRESSION_FORMAT)?;
    options.add_name_value("ZLEVEL", COMPRESSION_LEVEL)?;
    options.add_name_value("NUM_THREADS", &opts.compression_num_threads.to_string())?;
    options.add_name_value("INTERLEAVE", "BAND")?;

    if opts.as_cog {
        // COGs require a block size of 512x512, so we enforce it now so that we do the work only once.
        options.add_name_value("BLOCKXSIZE", COG_BLOCK_SIZE)?;
        options.add_name_value("BLOCKYSIZE", COG_BLOCK_SIZE)?;
    } else {
        options.add_name_value("TILED", "YES")?;
    }

    if opts.force_big_tiff {
        options.add_name_value("BIGTIFF", "YES")?;
    }

    Ok(options)
}
