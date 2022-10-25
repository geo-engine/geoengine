use super::{GdalDatasetParameters, GdalLoadingInfoTemporalSlice};
use crate::{
    source::{
        gdal_source::{create_no_data_tile, read_raster_tile_with_properties},
        FileNotFoundHandling,
    },
    util::{gdal::gdal_open_dataset_ex, Result, TemporaryGdalThreadLocalConfigOptions},
};
use futures::{stream, Future, Stream, StreamExt, TryStreamExt};
use gdal::{raster::GdalType, Dataset, DatasetOptions, GdalOpenFlags};
use geoengine_datatypes::{
    primitives::{RasterQueryRectangle, SpatialPartitioned, TimeInterval},
    raster::{Pixel, RasterTile2D, TileInformation, TilingStrategy},
};
use log::debug;
use num::FromPrimitive;
use snafu::ResultExt;
use std::time::Instant;

// TODO: this should probably be part of the query context
// lazy_static::lazy_static! {
//     pub(crate) static ref GDAL_RASTER_LOADER: Arc<GdalRasterLoader> = Arc::new(GdalRasterLoader::new());
// }

pub(crate) struct GdalRasterLoader {
    // TODO: enhance to vector of datasets
    // datasets_cache: Mutex<LruCache<GdalRasterLoaderKey, Arc<Mutex<Dataset>>>>,
}

// #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
// struct GdalRasterLoaderKey {
//     file_path: PathBuf,
//     gdal_open_options: GdalDatasetOptions,
//     gdal_config_options: Option<Vec<(String, String)>>,
// }

// struct LendedDataset<'l> {
//     dataset: MutexGuard<'l, Dataset>,
// }

// #[derive(Debug, Clone)]
// pub struct GdalDatasetAndParams {
//     // has to be an `Arc<Mutex<…>>` because `spawn_blocking` requires stuff to be `Sync`
//     pub dataset: Arc<Mutex<Result<Dataset>>>,
//     pub params: GdalDatasetParameters,
// }

// impl GdalDatasetAndParams {
//     pub async fn lock(self) -> GdalDatasetAndParamsGuard {
//         GdalDatasetAndParamsGuard {
//             dataset: self.dataset.lock_owned().await,
//             params: self.params,
//         }
//     }
// }

// pub struct GdalDatasetAndParamsGuard {
//     // has to be an `Arc<Mutex<…>>` because `spawn_blocking` requires stuff to be `Sync`
//     pub dataset: OwnedMutexGuard<Result<Dataset>>,
//     pub params: GdalDatasetParameters,
// }

#[derive(Debug, Clone)]
pub struct DatasetChannelAndParams {
    pub receiver: async_channel::Receiver<Result<Dataset>>,
    pub sender: async_channel::Sender<Result<Dataset>>,
    pub params: GdalDatasetParameters,
}

impl GdalRasterLoader {
    // fn new() -> Self {
    // let LRU_LIMIT: NonZeroUsize = NonZeroUsize::new(10).expect("LRU limit should be non-zero"); // TODO: config parameter
    // Self {
    //     datasets_cache: Mutex::new(LruCache::new(LRU_LIMIT)),
    // }
    // }

    // async fn open_dataset(
    //     &self,
    //     file_path: PathBuf,
    //     gdal_open_options: GdalDatasetOptions,
    //     gdal_config_options: Option<Vec<(String, String)>>,
    // ) -> OwnedMutexGuard<Dataset> {
    //     let key = GdalRasterLoaderKey {
    //         file_path,
    //         gdal_open_options,
    //         gdal_config_options,
    //     };

    //     let mut datasets_cache = self.datasets_cache.lock().await;

    //     if let Some(dataset) = datasets_cache.get(&key) {
    //         if let Some(dataset_lock) = dataset.try_lock_owned() {
    //             return dataset_lock;
    //         }
    //     }

    //     // datasets_cache.push(k, v)

    //     // if datasets_cache.len() >= datasets_cache.cap().into() {
    //     //     // try to remove the oldest dataset
    //     //     datasets_cache.pu
    //     // }

    //     // optoins.op
    //     todo!("wait")
    // }

    ///
    /// A method to async load single tiles from a GDAL dataset.
    ///
    async fn load_tile_data_async<T: Pixel + GdalType + FromPrimitive>(
        // self: Arc<Self>,
        dataset_and_params: DatasetChannelAndParams,
        tile_information: TileInformation,
        tile_time: TimeInterval,
    ) -> Result<RasterTile2D<T>> {
        // try to get dataset from cache
        let dataset_option = dataset_and_params.receiver.try_recv().ok();

        let params = dataset_and_params.params;

        let (result, dataset) = crate::util::spawn_blocking(move || {
            let dataset = dataset_option.unwrap_or_else(|| Self::open_dataset(&params));

            let result = Self::load_tile_data(&params, &dataset, tile_information, tile_time);

            (result, dataset)
        })
        .await
        .context(crate::error::TokioJoin)?;

        dataset_and_params
            .sender
            .send(dataset)
            .await
            // error means channel is closed, so do nothing
            .unwrap_or_default();

        result
    }

    pub(super) async fn load_tile_async<T: Pixel + GdalType + FromPrimitive>(
        // self: Arc<Self>,
        dataset_and_params: Option<DatasetChannelAndParams>,
        tile_information: TileInformation,
        tile_time: TimeInterval,
    ) -> Result<RasterTile2D<T>> {
        match dataset_and_params {
            // TODO: discuss if we need this check here. The metadata provider should only pass on loading infos if the query intersects the datasets bounds! And the tiling strategy should only generate tiles that intersect the querys bbox.
            Some(dataset_and_params)
                if tile_information
                    .spatial_partition()
                    .intersects(&dataset_and_params.params.spatial_partition()) =>
            {
                debug!(
                    "Loading tile {:?}, from {:?}, band: {}",
                    &tile_information,
                    dataset_and_params.params.file_path,
                    dataset_and_params.params.rasterband_channel
                );
                Self::load_tile_data_async(dataset_and_params, tile_information, tile_time).await
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
    pub(super) fn load_tile_data<T: Pixel + GdalType + FromPrimitive>(
        dataset_params: &GdalDatasetParameters,
        dataset: &Result<Dataset>,
        tile_information: TileInformation,
        tile_time: TimeInterval,
    ) -> Result<RasterTile2D<T>> {
        // TODO: remove here and integrate in counting adapter
        let start = Instant::now();

        debug!(
            "GridOrEmpty2D<{:?}> requested for {:?}.",
            T::TYPE,
            &tile_information.spatial_partition()
        );

        // reverts the thread local configs on drop
        let _thread_local_configs = dataset_params
            .gdal_config_options
            .as_ref()
            .map(|config_options| TemporaryGdalThreadLocalConfigOptions::new(config_options));

        let dataset = match dataset {
            Ok(dataset) => dataset,
            Err(_e) => {
                // TODO: check if Gdal error is actually file not found

                let err_result = match dataset_params.file_not_found_handling {
                    FileNotFoundHandling::NoData => {
                        Ok(create_no_data_tile(tile_information, tile_time))
                    }
                    FileNotFoundHandling::Error => {
                        Err(crate::error::Error::CouldNotOpenGdalDataset {
                            file_path: dataset_params.file_path.to_string_lossy().to_string(),
                        })
                    }
                };
                let elapsed = start.elapsed();
                debug!(
                    "file not found -> returning error = {}, took {:?}",
                    err_result.is_err(),
                    elapsed
                );
                return err_result;
            }
        };

        let result_tile =
            read_raster_tile_with_properties(dataset, dataset_params, tile_information, tile_time)?;

        let elapsed = start.elapsed();
        debug!("data loaded -> returning data grid, took {:?}", elapsed);

        Ok(result_tile)
    }

    pub(super) fn open_dataset(dataset_params: &GdalDatasetParameters) -> Result<Dataset> {
        let options = dataset_params
            .gdal_open_options
            .as_ref()
            .map(|o| o.iter().map(String::as_str).collect::<Vec<_>>());

        // reverts the thread local configs on drop
        let _thread_local_configs = dataset_params
            .gdal_config_options
            .as_ref()
            .map(|config_options| TemporaryGdalThreadLocalConfigOptions::new(config_options));

        let dataset = gdal_open_dataset_ex(
            &dataset_params.file_path,
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_RASTER,
                open_options: options.as_deref(),
                ..DatasetOptions::default()
            },
        );

        // GdalDatasetAndParams {
        //     dataset: Arc::new(Mutex::new(dataset)),
        //     params: dataset_params,
        // }

        dataset
    }

    ///
    /// A stream of futures producing `RasterTile2D` for a single slice in time
    ///
    fn temporal_slice_tile_future_stream<T: Pixel + GdalType + FromPrimitive>(
        // self: Arc<Self>,
        query: RasterQueryRectangle,
        info: GdalLoadingInfoTemporalSlice,
        tiling_strategy: TilingStrategy,
    ) -> impl Stream<Item = impl Future<Output = Result<RasterTile2D<T>>>> {
        // since the dataset is the same for all tiles for this time slice, we open it once and pass it to the tile loading futures
        let dataset_and_params = info.params.map(|params| {
            let (sender, receiver) = async_channel::unbounded();

            DatasetChannelAndParams {
                receiver,
                sender,
                params,
            }
        });

        stream::iter(tiling_strategy.tile_information_iterator(query.spatial_bounds))
            .map(move |tile| Self::load_tile_async(dataset_and_params.clone(), tile, info.time))
    }

    pub fn loading_info_to_tile_stream<
        T: Pixel + GdalType + FromPrimitive,
        S: Stream<Item = Result<GdalLoadingInfoTemporalSlice>>,
    >(
        // self: Arc<Self>,
        loading_info_stream: S,
        query: RasterQueryRectangle,
        tiling_strategy: TilingStrategy,
    ) -> impl Stream<Item = Result<RasterTile2D<T>>> {
        loading_info_stream
            .map_ok(move |info| {
                Self::temporal_slice_tile_future_stream(query, info, tiling_strategy)
                    .map(Result::Ok)
            })
            .try_flatten()
            .try_buffered(*GEOENGINE_TRY_BUFFERED_SIZE)
        // TODO: make this configurable
    }
}

lazy_static::lazy_static! {
    // TODO: it was 16
    static ref GEOENGINE_TRY_BUFFERED_SIZE: usize = std::env::var("GEOENGINE_TRY_BUFFERED_SIZE")
    .unwrap()
    .parse()
    .unwrap();
}

// TODO: use these for re-using datasets

///// A variante of `gdal::DatasetOptions` without a lifetime and with `Hash` and `Eq` implemented.
// #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
// pub struct GdalDatasetOptions {
//     pub open_flags: GdalOpenFlags,
//     pub allowed_drivers: Option<Vec<String>>,
//     pub open_options: Option<Vec<String>>,
//     pub sibling_files: Option<Vec<String>>,
// }

// impl From<DatasetOptions<'_>> for GdalDatasetOptions {
//     fn from(options: DatasetOptions) -> Self {
//         Self {
//             open_flags: options.open_flags,
//             allowed_drivers: options
//                 .allowed_drivers
//                 .map(|v| v.iter().map(|s| (*s).to_owned()).collect()),
//             open_options: options
//                 .open_options
//                 .map(|v| v.iter().map(|s| (*s).to_owned()).collect()),
//             sibling_files: options
//                 .sibling_files
//                 .map(|v| v.iter().map(|s| (*s).to_owned()).collect()),
//         }
//     }
// }

// impl GdalDatasetOptions {
//     pub fn open(&self, path: &Path) -> Result<Dataset> {
//         let allowed_drivers: Option<Vec<&str>> = self
//             .allowed_drivers
//             .as_ref()
//             .map(|v| v.iter().map(String::as_str).collect());
//         let open_options: Option<Vec<&str>> = self
//             .open_options
//             .as_ref()
//             .map(|v| v.iter().map(String::as_str).collect());
//         let sibling_files: Option<Vec<&str>> = self
//             .sibling_files
//             .as_ref()
//             .map(|v| v.iter().map(String::as_str).collect());

//         let dataset_options = DatasetOptions {
//             open_flags: self.open_flags,
//             allowed_drivers: allowed_drivers.as_deref(),
//             open_options: open_options.as_deref(),
//             sibling_files: sibling_files.as_deref(),
//         };

//         gdal_open_dataset_ex(path, dataset_options)
//     }
// }
