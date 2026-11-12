//! The read path shared by the GDAL source operators: a tile plan names the files of one
//! output tile, `assemble_tile` reads them through the process pool and composes the tile,
//! and `tile_stream` drives the plans of a query in order with bounded read-ahead.

mod blocks;

pub use blocks::BlockIo;
pub use blocks::{UnitRead, UnitResult};

use crate::cache::new_raster_cache::SupportedRasterDataType;
use crate::engine::{RasterOperator, SingleRasterSource, TypedRasterOperator};
use crate::error::Error;
use crate::processing::{
    Downsampling, DownsamplingMethod, DownsamplingParams, DownsamplingResolution,
};
use crate::source::gdal_worker_process::process_common::{
    GdalReadAdvise, GdalReadWindow, IpcProcessError,
};
use crate::source::gdal_worker_process::reader::GdalPoolReader;
use crate::source::gdal_worker_process::{
    GdalDatasetParameters, GdalPoolDispatcher, GdalProcessPoolError, GdalProcessReadResult,
    GdalReaderMode,
};
use crate::util::Result;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use gdal::raster::GdalType;
use geoengine_datatypes::dataset::NamedData;
use geoengine_datatypes::primitives::{
    CacheHint, SpatialResolution, TimeInterval, find_next_best_overview_level,
};
use geoengine_datatypes::raster::{
    BoundedGrid, ChangeGridBounds, EmptyGrid, GridBlit, GridBoundingBox2D, GridIdx2D, GridOrEmpty,
    GridShapeAccess, Pixel, RasterTile2D, TileInformation,
};
use num::FromPrimitive;
use std::num::NonZeroUsize;
use tracing::trace;

/// Everything needed to produce one output tile.
#[derive(Debug, Clone)]
pub struct TilePlan {
    pub tile: TileInformation,
    pub time: TimeInterval,
    pub band: u32,
    /// the files covering the tile, highest z-index first
    pub files: Vec<GdalDatasetParameters>,
    pub cache_hint: CacheHint,
}

/// Reads the files of `plan` and composes its tile. Reads of a level the dataset declares go
/// through `blocks` if given, else they read the level directly; neither resamples. Only
/// overviews of datasets without declared levels are left to GDAL, which may resample.
///
/// The highest z-index wins at each valid pixel. A file is read only where the files above it
/// left pixels invalid, so files are read one after another. The properties come from the first
/// file that was read. A tile no file covers is empty.
///
/// # Errors
/// Returns an error if a pool read fails. A missing file counts as no data if its parameters
/// say so.
pub async fn assemble_tile<T: Pixel + GdalType + FromPrimitive + SupportedRasterDataType>(
    plan: TilePlan,
    reader_mode: GdalReaderMode,
    gdal_worker: &GdalPoolDispatcher,
    blocks: Option<&BlockIo>,
) -> Result<RasterTile2D<T>, GdalProcessPoolError> {
    let tile_bounds = plan.tile.global_pixel_bounds();
    let tile_spatial_grid = plan.tile.spatial_grid_definition();
    let mut raster: GridOrEmpty<GridBoundingBox2D, T> =
        GridOrEmpty::from(EmptyGrid::new(tile_bounds));
    let mut properties = None;
    let reader = GdalPoolReader::from(gdal_worker.clone());
    let level_factor = reader_mode.level_factor();
    let blocks = blocks.filter(|blocks| blocks.caches(plan.cache_hint));

    for params in plan.files {
        let Some(read_advise) = reader_mode
            .tiling_to_dataset_read_advise(&params.spatial_grid_definition(), &tile_spatial_grid)
        else {
            trace!(
                file = %params.file_path.display(),
                "file does not intersect tile {:?}",
                plan.tile.tile_position,
            );
            continue;
        };

        if raster
            .as_masked_grid()
            .is_some_and(|grid| grid.all_valid_in_bbox(&read_advise.read_window_bounds))
        {
            trace!(
                file = %params.file_path.display(),
                "skipping file whose read area is covered by higher-z valid pixels"
            );
            continue;
        }

        let read = match (level_factor, blocks.filter(|blocks| blocks.reads(&params))) {
            (Some(factor), Some(blocks)) => {
                blocks
                    .read::<T>(&params, read_advise, factor, plan.cache_hint)
                    .await
            }
            (Some(factor), None) if factor.get() > 1 => {
                let Some(level_advise) = level_read_advise(read_advise, factor) else {
                    return Err(misaligned_read(&params, factor));
                };
                reader
                    .read_level_data::<T>(params.clone(), factor, level_advise)
                    .await
            }
            _ => {
                reader
                    .read_tile_data::<T>(params.clone(), read_advise)
                    .await
            }
        };
        let read = match read {
            Err(GdalProcessPoolError::IpcProcessError {
                source: IpcProcessError::MissingOverviewLevel { file, factor },
            }) => {
                // the dataset declares the level, but one of its files lacks it: GDAL picks another
                tracing::warn!("{file} lacks the overview level {factor}, GDAL may resample it");
                reader.read_tile_data::<T>(params, read_advise).await?
            }
            read => read?,
        };
        let file_data = match read {
            GdalProcessReadResult::Grid(grid_and_properties) => *grid_and_properties,
            GdalProcessReadResult::FileNotFoundAsNoData => continue,
        };

        if raster.is_empty() && file_data.grid.bounding_box() == tile_bounds {
            // nothing to merge with: take the grid instead of copying it
            raster = file_data.grid;
        } else if raster.is_empty() {
            // nothing to merge with either: copy whole rows, the merge below checks each pixel
            raster.grid_blit_from(&file_data.grid);
        } else {
            raster.grid_blit_fill_no_data(&file_data.grid);
        }
        properties.get_or_insert(file_data.properties);
    }

    Ok(RasterTile2D::new_with_properties(
        plan.time,
        plan.tile.tile_position,
        plan.band,
        plan.tile.global_geo_transform,
        raster.unbounded(),
        properties.unwrap_or_default(),
        plan.cache_hint,
    ))
}

/// Checks the overview level a processing graph asks for against the levels its dataset
/// declares. Datasets without declared levels accept any level, GDAL then picks one.
///
/// # Errors
/// Returns an error if the dataset declares its levels and `overview_level` is not one of them.
pub(crate) fn ensure_overview_level_exists(
    data: &NamedData,
    overview_level: u32,
    max_overview_level: Option<u32>,
) -> Result<()> {
    match max_overview_level {
        Some(max_overview_level)
            if overview_level > 1
                && (!overview_level.is_power_of_two() || overview_level > max_overview_level) =>
        {
            Err(Error::OverviewLevelNotAvailable {
                data: data.to_string(),
                overview_level,
                max_overview_level,
            })
        }
        _ => Ok(()),
    }
}

/// The source for `target_resolution`: `source(level)` reads the coarsest power-of-two overview
/// level not coarser than the target. If the dataset declares fewer levels, the source reads its
/// coarsest one and a `Downsampling` produces the resolution of the missing level, so GDAL never
/// resamples.
pub(crate) fn overview_source(
    native_resolution: SpatialResolution,
    target_resolution: SpatialResolution,
    max_overview_level: Option<u32>,
    source: impl FnOnce(u32) -> TypedRasterOperator,
) -> TypedRasterOperator {
    let wanted = find_next_best_overview_level(native_resolution, target_resolution);
    let available = match max_overview_level {
        // the largest declared power of two; 0 reads the full resolution
        Some(max) if wanted > 1 => match wanted.min(1 << max.max(1).ilog2()) {
            1 => 0,
            level => level,
        },
        _ => wanted,
    };
    let source = source(available);
    if available >= wanted {
        return source;
    }

    let resolution = SpatialResolution::with_native_resolution_and_gdal_overview_level(
        native_resolution,
        wanted,
    )
    .unwrap_or(target_resolution);
    Downsampling {
        params: DownsamplingParams {
            sampling_method: DownsamplingMethod::NearestNeighbor,
            output_resolution: DownsamplingResolution::Resolution(resolution),
            output_origin_reference: None,
        },
        sources: SingleRasterSource { raster: source },
    }
    .typed()
}

/// `read_advise` in the pixel space of the level `factor` times coarser than the full
/// resolution, where its window has the size of its output. `None` if it does not start at a
/// pixel of the level.
pub(crate) fn level_read_advise(
    read_advise: GdalReadAdvise,
    factor: NonZeroUsize,
) -> Option<GdalReadAdvise> {
    let (start_x, start_y) = read_advise.gdal_read_widow.gdal_window_start();
    let (start_x, start_y) = (
        usize::try_from(start_x).ok()?,
        usize::try_from(start_y).ok()?,
    );
    if start_x % factor != 0 || start_y % factor != 0 {
        return None;
    }
    Some(GdalReadAdvise {
        gdal_read_widow: GdalReadWindow::new(
            GridIdx2D::new([(start_y / factor) as isize, (start_x / factor) as isize]),
            read_advise.read_window_bounds.grid_shape(),
        ),
        ..read_advise
    })
}

fn misaligned_read(params: &GdalDatasetParameters, factor: NonZeroUsize) -> GdalProcessPoolError {
    IpcProcessError::IpcOther {
        msg: format!(
            "a read of {} does not start at a pixel of its level {factor}",
            params.file_path.display()
        ),
    }
    .into()
}

/// Assembles the tiles of `plans` in their order, with up to `read_ahead` tiles in flight.
/// `into_error` tags pool errors with the calling source.
pub fn tile_stream<'a, T>(
    plans: impl Stream<Item = Result<TilePlan>> + Send + 'a,
    reader_mode: GdalReaderMode,
    gdal_worker: GdalPoolDispatcher,
    blocks: Option<BlockIo>,
    read_ahead: usize,
    into_error: fn(GdalProcessPoolError) -> Error,
) -> BoxStream<'a, Result<RasterTile2D<T>>>
where
    T: Pixel + GdalType + FromPrimitive + SupportedRasterDataType,
{
    plans
        .map(move |plan| {
            let gdal_worker = gdal_worker.clone();
            let blocks = blocks.clone();
            async move {
                assemble_tile(plan?, reader_mode, &gdal_worker, blocks.as_ref())
                    .await
                    .map_err(into_error)
            }
        })
        .buffered(read_ahead)
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::new_raster_cache::{BlockReadMode, NewRasterCacheEnum};
    use crate::source::gdal_worker_process::{
        DEFAULT_HEADER_CACHE_ENTRIES, FileNotFoundHandling, GdalDatasetGeoTransform,
        GdalMetadataMapping, GdalProcessPool, GdalProcessPoolAccess, OverviewReaderState,
        ReaderState, WorkerConfig,
    };
    use geoengine_datatypes::primitives::CacheTtlSeconds;
    use geoengine_datatypes::raster::{
        GeoTransform, RasterPropertiesEntry, RasterPropertiesEntryType, RasterPropertiesKey,
        TileSize,
    };
    use geoengine_datatypes::test_data;
    use geoengine_datatypes::util::NonZeroFiniteF64;
    use geoengine_datatypes::util::test::TestDefault;
    use std::sync::Arc;

    #[tokio::test]
    async fn it_reads_only_remote_files_through_blocks_in_auto_mode() {
        let remote = |path: &str| GdalDatasetParameters {
            file_path: path.into(),
            ..ndvi_jan_2014_params()
        };
        let local = ndvi_jan_2014_params();
        let remote_paths = [
            "https://example.com/a.tif",
            "s3://bucket/a.tif",
            "/vsicurl/https://example.com/a.tif",
            "/vsis3/bucket/a.tif",
            "SENTINEL2_L2A:/vsis3/eodata/MTD_MSIL2A.xml:10m:EPSG_32632",
            "/vsizip//vsicurl/https://example.com/a.zip/a.tif",
        ];
        let blocks = |mode| {
            let mut ctx = crate::engine::MockExecutionContext::test_default()
                .mock_query_context_test_default();
            ctx.new_raster_cache = Some(Arc::new(
                NewRasterCacheEnum::new_lru(1_000_000).with_block_read_mode(mode),
            ));
            BlockIo::from_query_context(&ctx)
        };

        assert!(blocks(BlockReadMode::Off).is_none());
        let auto = blocks(BlockReadMode::Auto).unwrap();
        assert!(!auto.reads(&local));
        assert!(!auto.reads(&remote("/vsizip/local.zip/a.tif")));
        for path in remote_paths {
            assert!(auto.reads(&remote(path)), "{path}");
        }
        let on = blocks(BlockReadMode::On).unwrap();
        assert!(on.reads(&local));
        assert!(on.reads(&remote(remote_paths[0])));
    }

    fn ndvi_jan_2014_params() -> GdalDatasetParameters {
        GdalDatasetParameters {
            file_path: test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF").into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (-180., 90.).into(),
                x_pixel_size: NonZeroFiniteF64::new_unchecked(0.1),
                y_pixel_size: NonZeroFiniteF64::new_unchecked(-0.1),
            },
            width: 3600,
            height: 1800,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: Some(0.),
            properties_mapping: Some(vec![
                GdalMetadataMapping {
                    source_key: RasterPropertiesKey {
                        domain: None,
                        key: "AREA_OR_POINT".to_string(),
                    },
                    target_type: RasterPropertiesEntryType::String,
                    target_key: RasterPropertiesKey {
                        domain: None,
                        key: "AREA_OR_POINT".to_string(),
                    },
                },
                GdalMetadataMapping {
                    source_key: RasterPropertiesKey {
                        domain: Some("IMAGE_STRUCTURE".to_string()),
                        key: "COMPRESSION".to_string(),
                    },
                    target_type: RasterPropertiesEntryType::String,
                    target_key: RasterPropertiesKey {
                        domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
                        key: "COMPRESSION".to_string(),
                    },
                },
            ]),
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
            tile_size: None,
        }
    }

    #[tokio::test]
    async fn it_assembles_a_tile_from_one_file() {
        let params = ndvi_jan_2014_params();
        let reader_mode = GdalReaderMode::OriginalResolution(ReaderState {
            dataset_spatial_grid: params.spatial_grid_definition(),
        });
        // the file's own grid: a pixel size computed from bounds is off by rounding and does not
        // align with the file
        let tile_information = TileInformation {
            tile_size: TileSize::new_unchecked(8, 8),
            tile_position: [0, 0].into(),
            global_geo_transform: GeoTransform::new_unchecked((-180., 90.).into(), 0.1, -0.1),
        };

        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );

        let tile = assemble_tile::<u8>(
            TilePlan {
                tile: tile_information,
                time: TimeInterval::default(),
                band: 0,
                files: vec![params],
                cache_hint: CacheHint::no_cache(),
            },
            reader_mode,
            &gpp.get_gdal_worker(),
            None,
        )
        .await
        .unwrap();

        assert!(!tile.is_empty());
        let grid = tile.grid_array.into_materialized_masked_grid();
        assert_eq!(grid.inner_grid.data, &[255; 64]);
        assert_eq!(grid.validity_mask.data, &[true; 64]);

        let properties = tile.properties;
        assert!(properties.scale_option().is_none());
        assert!(properties.offset_option().is_none());
        assert_eq!(
            properties.get_property(&RasterPropertiesKey {
                domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
                key: "COMPRESSION".to_string(),
            }),
            Some(&RasterPropertiesEntry::String("LZW".to_string()))
        );
        assert_eq!(
            properties.get_property(&RasterPropertiesKey {
                domain: None,
                key: "AREA_OR_POINT".to_string(),
            }),
            Some(&RasterPropertiesEntry::String("Area".to_string()))
        );
    }

    /// A 64x64 u16 `GeoTIFF` in 16x16 blocks with overviews 2 and 4, every pixel distinct.
    fn tiled_tiff_with_overviews(dir: &std::path::Path) -> GdalDatasetParameters {
        let file_path = dir.join("blocks.tif");
        let driver = gdal::DriverManager::get_driver_by_name("GTiff").unwrap();
        let options: gdal::cpl::CslStringList = ["TILED=YES", "BLOCKXSIZE=16", "BLOCKYSIZE=16"]
            .into_iter()
            .collect();
        let mut dataset = driver
            .create_with_band_type_with_options::<u16, _>(&file_path, 64, 64, 1, &options)
            .unwrap();
        dataset
            .set_spatial_ref(&gdal::spatial_ref::SpatialRef::from_epsg(4326).unwrap())
            .unwrap();
        dataset
            .set_geo_transform(&[0., 1., 0., 64., 0., -1.])
            .unwrap();
        let mut buffer =
            gdal::raster::Buffer::new((64, 64), (0..64 * 64).map(|v| v as u16).collect());
        dataset
            .rasterband(1)
            .unwrap()
            .write((0, 0), (64, 64), &mut buffer)
            .unwrap();
        dataset.build_overviews("NEAREST", &[2, 4], &[]).unwrap();
        dataset.close().unwrap();

        GdalDatasetParameters {
            file_path,
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (0., 64.).into(),
                x_pixel_size: NonZeroFiniteF64::new_unchecked(1.),
                y_pixel_size: NonZeroFiniteF64::new_unchecked(-1.),
            },
            width: 64,
            height: 64,
            file_not_found_handling: FileNotFoundHandling::Error,
            no_data_value: None,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
            tile_size: None,
        }
    }

    #[test]
    fn it_reads_only_declared_overviews_and_downsamples_the_rest() {
        let native = SpatialResolution::new_unchecked(10., 10.);
        let target = SpatialResolution::new_unchecked(80., 80.);
        let source = |level: u32| {
            crate::source::GdalSource {
                params: crate::source::GdalSourceParameters::new(NamedData::with_system_name(
                    "data",
                ))
                .with_overview_level(Some(level)),
            }
            .typed()
        };
        let json = |operator: &TypedRasterOperator| serde_json::to_value(operator).unwrap();

        assert_eq!(
            json(&overview_source(native, target, None, source)),
            json(&source(8)),
            "unknown levels: as before"
        );
        assert_eq!(
            json(&overview_source(native, target, Some(16), source)),
            json(&source(8))
        );

        let downsampled = json(&overview_source(native, target, Some(4), source));
        assert_eq!(downsampled["type"], "Downsampling");
        assert_eq!(downsampled["params"]["outputResolution"]["x"], 80.);
        assert_eq!(downsampled["sources"]["raster"], json(&source(4)));

        let full = json(&overview_source(native, target, Some(1), source));
        assert_eq!(full["sources"]["raster"], json(&source(0)));
    }

    #[test]
    fn it_rejects_undeclared_overview_levels() {
        let data = NamedData::with_system_name("data");
        assert!(ensure_overview_level_exists(&data, 8, None).is_ok());
        assert!(ensure_overview_level_exists(&data, 4, Some(4)).is_ok());
        assert!(ensure_overview_level_exists(&data, 0, Some(1)).is_ok());
        assert!(ensure_overview_level_exists(&data, 8, Some(4)).is_err());
        assert!(ensure_overview_level_exists(&data, 3, Some(4)).is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tiles_that_share_a_unit_read_and_store_it_once() {
        let dir = tempfile::tempdir().unwrap();
        let params = tiled_tiff_with_overviews(dir.path());
        let file_grid = params.spatial_grid_definition();
        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let gdal_worker = gpp.get_gdal_worker();
        let cache = Arc::new(NewRasterCacheEnum::new_lru(100_000_000));
        let blocks = BlockIo::new(cache.clone(), gdal_worker.clone(), CacheTtlSeconds::new(60));

        // the 64 px file is one unit, and one piece, of the 16 px blocks grouped to 512 px
        let tiles = (0..2)
            .flat_map(|y| (0..2).map(move |x| [y, x]))
            .map(|position| {
                let plan = TilePlan {
                    tile: TileInformation::new(
                        position.into(),
                        TileSize::new_unchecked(32, 32),
                        GeoTransform::new_unchecked((0., 64.).into(), 1., -1.),
                    ),
                    time: TimeInterval::default(),
                    band: 0,
                    files: vec![params.clone()],
                    cache_hint: CacheHint::max_duration(),
                };
                let reader_mode = GdalReaderMode::OriginalResolution(ReaderState {
                    dataset_spatial_grid: file_grid,
                });
                let (gdal_worker, blocks) = (gdal_worker.clone(), blocks.clone());
                tokio::spawn(async move {
                    assemble_tile::<u16>(plan, reader_mode, &gdal_worker, Some(&blocks)).await
                })
            });
        for tile in futures::future::join_all(tiles.collect::<Vec<_>>()).await {
            tile.unwrap().unwrap();
        }

        let stats = cache.stats();
        assert_eq!(stats.stored, 1);
        assert_eq!(stats.inserts_joined, 0);
        assert_eq!(
            gpp.request_stats().reads + gpp.request_stats().reads_joined,
            1
        );
    }

    #[tokio::test]
    async fn tiles_read_windows_when_blocks_would_not_stay_cached() {
        let dir = tempfile::tempdir().unwrap();
        let params = tiled_tiff_with_overviews(dir.path());
        let file_grid = params.spatial_grid_definition();
        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let gdal_worker = gpp.get_gdal_worker();
        let cache = Arc::new(NewRasterCacheEnum::new_lru(100_000_000));
        // no minimum lifetime and a tile that must not be cached, as in the mock query contexts
        let blocks = BlockIo::new(cache.clone(), gdal_worker.clone(), CacheTtlSeconds::new(0));

        let plan = TilePlan {
            tile: TileInformation::new(
                [0, 0].into(),
                TileSize::new_unchecked(32, 32),
                GeoTransform::new_unchecked((0., 64.).into(), 1., -1.),
            ),
            time: TimeInterval::default(),
            band: 0,
            files: vec![params.clone()],
            cache_hint: CacheHint::no_cache(),
        };
        let reader_mode = GdalReaderMode::OriginalResolution(ReaderState {
            dataset_spatial_grid: file_grid,
        });
        let from_blocks =
            assemble_tile::<u16>(plan.clone(), reader_mode, &gdal_worker, Some(&blocks))
                .await
                .unwrap();
        let from_window = assemble_tile::<u16>(plan, reader_mode, &gdal_worker, None)
            .await
            .unwrap();

        assert_eq!(from_blocks.grid_array, from_window.grid_array);
        assert_eq!(cache.stats().stored, 0, "no block is stored");
        assert_eq!(gpp.request_stats().headers, 0, "no header is read");
    }

    #[tokio::test]
    async fn block_reads_reuse_the_header_until_caches_are_cleared() {
        let dir = tempfile::tempdir().unwrap();
        let params = tiled_tiff_with_overviews(dir.path());
        let file_grid = params.spatial_grid_definition();
        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let gdal_worker = gpp.get_gdal_worker();
        let cache = Arc::new(NewRasterCacheEnum::new_lru(100_000_000));
        let blocks = BlockIo::new(cache, gdal_worker.clone(), CacheTtlSeconds::new(60));

        let read = || {
            let plan = TilePlan {
                tile: TileInformation::new(
                    [0, 0].into(),
                    TileSize::new_unchecked(32, 32),
                    GeoTransform::new_unchecked((0., 64.).into(), 1., -1.),
                ),
                time: TimeInterval::default(),
                band: 0,
                files: vec![params.clone()],
                cache_hint: CacheHint::max_duration(),
            };
            let reader_mode = GdalReaderMode::OriginalResolution(ReaderState {
                dataset_spatial_grid: file_grid,
            });
            assemble_tile::<u16>(plan, reader_mode, &gdal_worker, Some(&blocks))
        };

        read().await.unwrap();
        read().await.unwrap();
        assert_eq!(gpp.request_stats().headers, 1);

        gpp.clear_caches().await.unwrap();
        read().await.unwrap();
        assert_eq!(gpp.request_stats().headers, 2);
    }

    #[tokio::test]
    async fn reads_of_a_level_the_file_lacks_fall_back_to_gdal() {
        let dir = tempfile::tempdir().unwrap();
        let params = tiled_tiff_with_overviews(dir.path());
        let file_grid = params.spatial_grid_definition();
        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let gdal_worker = gpp.get_gdal_worker();
        let cache = Arc::new(NewRasterCacheEnum::new_lru(100_000_000));
        let blocks = BlockIo::new(cache, gdal_worker.clone(), CacheTtlSeconds::new(60));

        let plan = TilePlan {
            tile: TileInformation::new(
                [0, 0].into(),
                TileSize::new_unchecked(8, 8),
                GeoTransform::new_unchecked((0., 64.).into(), 8., -8.),
            ),
            time: TimeInterval::default(),
            band: 0,
            files: vec![params.clone()],
            cache_hint: CacheHint::max_duration(),
        };
        // the file has the overviews 2 and 4
        let reader_mode = |files_have_level| {
            GdalReaderMode::OverviewLevel(OverviewReaderState {
                original_dataset_grid: file_grid,
                overview_level: 8,
                files_have_level,
            })
        };

        let from_gdal = assemble_tile::<u16>(plan.clone(), reader_mode(false), &gdal_worker, None)
            .await
            .unwrap();
        for blocks in [None, Some(&blocks)] {
            let tile = assemble_tile::<u16>(plan.clone(), reader_mode(true), &gdal_worker, blocks)
                .await
                .unwrap();
            assert_eq!(tile, from_gdal);
        }
    }

    #[tokio::test]
    async fn block_reads_equal_window_reads() {
        let dir = tempfile::tempdir().unwrap();
        let params = tiled_tiff_with_overviews(dir.path());
        let file_grid = params.spatial_grid_definition();
        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let gdal_worker = gpp.get_gdal_worker();
        let cache = Arc::new(NewRasterCacheEnum::new_lru(100_000_000));
        let blocks = BlockIo::new(cache.clone(), gdal_worker.clone(), CacheTtlSeconds::new(60));

        for (overview_level, tile_size) in [(0_u32, 8_usize), (0, 12), (2, 8), (2, 12), (4, 12)] {
            let reader_mode = |files_have_level| {
                if overview_level == 0 {
                    GdalReaderMode::OriginalResolution(ReaderState {
                        dataset_spatial_grid: file_grid,
                    })
                } else {
                    GdalReaderMode::OverviewLevel(OverviewReaderState {
                        original_dataset_grid: file_grid,
                        overview_level,
                        files_have_level,
                    })
                }
            };
            let pixel_size = f64::from(overview_level.max(1));
            let tiles_per_axis = (64 / overview_level.max(1) as usize).div_ceil(tile_size);

            for tile_y in 0..tiles_per_axis {
                for tile_x in 0..tiles_per_axis {
                    let plan = TilePlan {
                        tile: TileInformation::new(
                            [tile_y as isize, tile_x as isize].into(),
                            TileSize::new_unchecked(tile_size, tile_size),
                            GeoTransform::new_unchecked((0., 64.).into(), pixel_size, -pixel_size),
                        ),
                        time: TimeInterval::default(),
                        band: 0,
                        files: vec![params.clone()],
                        cache_hint: CacheHint::max_duration(),
                    };

                    // GDAL picks the overview itself; the sizes divide, so it reads the same pixels
                    let from_gdal =
                        assemble_tile::<u16>(plan.clone(), reader_mode(false), &gdal_worker, None)
                            .await
                            .unwrap();
                    let from_level =
                        assemble_tile::<u16>(plan.clone(), reader_mode(true), &gdal_worker, None)
                            .await
                            .unwrap();
                    let from_blocks =
                        assemble_tile::<u16>(plan, reader_mode(true), &gdal_worker, Some(&blocks))
                            .await
                            .unwrap();

                    let case = format!(
                        "overview {overview_level}, tile size {tile_size}, tile [{tile_y}, {tile_x}]"
                    );
                    assert_eq!(from_level, from_gdal, "{case}");
                    assert_eq!(from_blocks, from_gdal, "{case}");
                }
            }
        }

        assert!(cache.byte_size() > 0, "the reads went through the blocks");
    }
}
