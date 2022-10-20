use std::{sync::Arc, time::Instant};

use super::{GdalDatasetParameters, GdalLoadingInfoTemporalSlice};
use crate::{
    source::{
        gdal_source::{create_no_data_tile, read_raster_tile_with_properties},
        FileNotFoundHandling,
    },
    util::{gdal::gdal_open_dataset_ex, Result, TemporaryGdalThreadLocalConfigOptions},
};
use futures::{stream, Future, Stream, StreamExt, TryStreamExt};
use gdal::{raster::GdalType, DatasetOptions, GdalOpenFlags};
use geoengine_datatypes::{
    primitives::{RasterQueryRectangle, SpatialPartitioned, TimeInterval},
    raster::{Pixel, RasterTile2D, TileInformation, TilingStrategy},
};
use log::debug;
use num::FromPrimitive;
use snafu::ResultExt;

// TODO: this should probably be part of the query context
lazy_static::lazy_static! {
    pub(crate) static ref GDAL_RASTER_LOADER: Arc<GdalRasterLoader> = Arc::new(GdalRasterLoader::new());
}

pub(crate) struct GdalRasterLoader {}

impl GdalRasterLoader {
    fn new() -> Self {
        Self {}
    }

    ///
    /// A method to async load single tiles from a GDAL dataset.
    ///
    async fn load_tile_data_async<T: Pixel + GdalType + FromPrimitive>(
        self: Arc<Self>,
        dataset_params: GdalDatasetParameters,
        tile_information: TileInformation,
        tile_time: TimeInterval,
    ) -> Result<RasterTile2D<T>> {
        crate::util::spawn_blocking(move || {
            self.load_tile_data(&dataset_params, tile_information, tile_time)
        })
        .await
        .context(crate::error::TokioJoin)?
    }

    pub async fn load_tile_async<T: Pixel + GdalType + FromPrimitive>(
        self: Arc<Self>,
        dataset_params: Option<GdalDatasetParameters>,
        tile_information: TileInformation,
        tile_time: TimeInterval,
    ) -> Result<RasterTile2D<T>> {
        match dataset_params {
            // TODO: discuss if we need this check here. The metadata provider should only pass on loading infos if the query intersects the datasets bounds! And the tiling strategy should only generate tiles that intersect the querys bbox.
            Some(ds)
                if tile_information
                    .spatial_partition()
                    .intersects(&ds.spatial_partition()) =>
            {
                debug!(
                    "Loading tile {:?}, from {:?}, band: {}",
                    &tile_information, ds.file_path, ds.rasterband_channel
                );
                self.load_tile_data_async(ds, tile_information, tile_time)
                    .await
            }
            Some(_) => {
                debug!("Skipping tile not in query rect {:?}", &tile_information);

                Ok(create_no_data_tile(tile_information, tile_time))
            }

            _ => {
                debug!(
                    "Skipping tile without GdalDatasetParameters {:?}",
                    &tile_information
                );

                Ok(create_no_data_tile(tile_information, tile_time))
            }
        }
    }

    ///
    /// A method to load single tiles from a GDAL dataset.
    ///
    pub fn load_tile_data<T: Pixel + GdalType + FromPrimitive>(
        &self,
        dataset_params: &GdalDatasetParameters,
        tile_information: TileInformation,
        tile_time: TimeInterval,
    ) -> Result<RasterTile2D<T>> {
        let start = Instant::now();

        debug!(
            "GridOrEmpty2D<{:?}> requested for {:?}.",
            T::TYPE,
            &tile_information.spatial_partition()
        );

        let options = dataset_params
            .gdal_open_options
            .as_ref()
            .map(|o| o.iter().map(String::as_str).collect::<Vec<_>>());

        // reverts the thread local configs on drop
        let _thread_local_configs = dataset_params
            .gdal_config_options
            .as_ref()
            .map(|config_options| TemporaryGdalThreadLocalConfigOptions::new(config_options));

        let dataset_result = gdal_open_dataset_ex(
            &dataset_params.file_path,
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_RASTER,
                open_options: options.as_deref(),
                ..DatasetOptions::default()
            },
        );

        if dataset_result.is_err() {
            // TODO: check if Gdal error is actually file not found

            let err_result = match dataset_params.file_not_found_handling {
                FileNotFoundHandling::NoData => {
                    Ok(create_no_data_tile(tile_information, tile_time))
                }
                FileNotFoundHandling::Error => Err(crate::error::Error::CouldNotOpenGdalDataset {
                    file_path: dataset_params.file_path.to_string_lossy().to_string(),
                }),
            };
            let elapsed = start.elapsed();
            debug!(
                "file not found -> returning error = {}, took {:?}",
                err_result.is_err(),
                elapsed
            );
            return err_result;
        };

        let dataset = dataset_result.expect("checked");

        let result_tile = read_raster_tile_with_properties(
            &dataset,
            dataset_params,
            tile_information,
            tile_time,
        )?;

        let elapsed = start.elapsed();
        debug!("data loaded -> returning data grid, took {:?}", elapsed);

        Ok(result_tile)
    }

    ///
    /// A stream of futures producing `RasterTile2D` for a single slice in time
    ///
    fn temporal_slice_tile_future_stream<T: Pixel + GdalType + FromPrimitive>(
        self: Arc<Self>,
        query: RasterQueryRectangle,
        info: GdalLoadingInfoTemporalSlice,
        tiling_strategy: TilingStrategy,
    ) -> impl Stream<Item = impl Future<Output = Result<RasterTile2D<T>>>> {
        stream::iter(tiling_strategy.tile_information_iterator(query.spatial_bounds)).map(
            move |tile| {
                self.clone()
                    .load_tile_async(info.params.clone(), tile, info.time)
            },
        )
    }

    pub fn loading_info_to_tile_stream<
        T: Pixel + GdalType + FromPrimitive,
        S: Stream<Item = Result<GdalLoadingInfoTemporalSlice>>,
    >(
        self: Arc<Self>,
        loading_info_stream: S,
        query: RasterQueryRectangle,
        tiling_strategy: TilingStrategy,
    ) -> impl Stream<Item = Result<RasterTile2D<T>>> {
        loading_info_stream
            .map_ok(move |info| {
                self.clone()
                    .temporal_slice_tile_future_stream(query, info, tiling_strategy)
                    .map(Result::Ok)
            })
            .try_flatten()
            .try_buffered(16) // TODO: make this configurable
    }
}
