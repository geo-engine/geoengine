use super::{GdalDatasetParameters, GdalLoadingInfoTemporalSlice};
use crate::{
    source::{
        gdal_source::{create_no_data_tile, read_raster_tile_with_properties},
        FileNotFoundHandling,
    },
    util::{gdal::gdal_open_dataset_ex, Result, TemporaryGdalThreadLocalConfigOptions},
};
use futures::{
    lock::{Mutex, MutexGuard, OwnedMutexGuard},
    stream, Future, Stream, StreamExt, TryStreamExt,
};
use gdal::{raster::GdalType, Dataset, DatasetOptions, GdalOpenFlags};
use geoengine_datatypes::{
    primitives::{RasterQueryRectangle, SpatialPartitioned, TimeInterval},
    raster::{Pixel, RasterTile2D, TileInformation, TilingStrategy},
};
use log::debug;
use lru::LruCache;
use num::FromPrimitive;
use snafu::ResultExt;
use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

// TODO: this should probably be part of the query context
lazy_static::lazy_static! {
    pub(crate) static ref GDAL_RASTER_LOADER: Arc<GdalRasterLoader> = Arc::new(GdalRasterLoader::new());
}

pub(crate) struct GdalRasterLoader {
    // TODO: enhance to vector of datasets
    datasets_cache: Mutex<LruCache<GdalRasterLoaderKey, Arc<Mutex<Dataset>>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct GdalRasterLoaderKey {
    file_path: PathBuf,
    gdal_open_options: GdalDatasetOptions,
    gdal_config_options: Option<Vec<(String, String)>>,
}

// struct LendedDataset<'l> {
//     dataset: MutexGuard<'l, Dataset>,
// }

impl GdalRasterLoader {
    fn new() -> Self {
        let LRU_LIMIT: NonZeroUsize = NonZeroUsize::new(10).expect("LRU limit should be non-zero"); // TODO: config parameter
        Self {
            datasets_cache: Mutex::new(LruCache::new(LRU_LIMIT)),
        }
    }

    async fn open_dataset(
        &self,
        file_path: PathBuf,
        gdal_open_options: GdalDatasetOptions,
        gdal_config_options: Option<Vec<(String, String)>>,
    ) -> OwnedMutexGuard<Dataset> {
        let key = GdalRasterLoaderKey {
            file_path,
            gdal_open_options,
            gdal_config_options,
        };

        let mut datasets_cache = self.datasets_cache.lock().await;

        if let Some(dataset) = datasets_cache.get(&key) {
            if let Some(dataset_lock) = dataset.try_lock_owned() {
                return dataset_lock;
            }
        }

        // datasets_cache.push(k, v)

        // if datasets_cache.len() >= datasets_cache.cap().into() {
        //     // try to remove the oldest dataset
        //     datasets_cache.pu
        // }

        // optoins.op
        todo!("wait")
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

/// A variante of `gdal::DatasetOptions` without a lifetime and with `Hash` and `Eq` implemented.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GdalDatasetOptions {
    pub open_flags: GdalOpenFlags,
    pub allowed_drivers: Option<Vec<String>>,
    pub open_options: Option<Vec<String>>,
    pub sibling_files: Option<Vec<String>>,
}

impl From<DatasetOptions<'_>> for GdalDatasetOptions {
    fn from(options: DatasetOptions) -> Self {
        Self {
            open_flags: options.open_flags,
            allowed_drivers: options
                .allowed_drivers
                .map(|v| v.iter().map(|s| (*s).to_owned()).collect()),
            open_options: options
                .open_options
                .map(|v| v.iter().map(|s| (*s).to_owned()).collect()),
            sibling_files: options
                .sibling_files
                .map(|v| v.iter().map(|s| (*s).to_owned()).collect()),
        }
    }
}

impl GdalDatasetOptions {
    pub fn open(&self, path: &Path) -> Result<Dataset> {
        let allowed_drivers: Option<Vec<&str>> = self
            .allowed_drivers
            .as_ref()
            .map(|v| v.iter().map(String::as_str).collect());
        let open_options: Option<Vec<&str>> = self
            .open_options
            .as_ref()
            .map(|v| v.iter().map(String::as_str).collect());
        let sibling_files: Option<Vec<&str>> = self
            .sibling_files
            .as_ref()
            .map(|v| v.iter().map(String::as_str).collect());

        let dataset_options = DatasetOptions {
            open_flags: self.open_flags,
            allowed_drivers: allowed_drivers.as_deref(),
            open_options: open_options.as_deref(),
            sibling_files: sibling_files.as_deref(),
        };

        gdal_open_dataset_ex(path, dataset_options)
    }
}
