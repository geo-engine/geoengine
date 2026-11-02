//! The read path shared by the GDAL source operators: a tile plan names the files of one
//! output tile, `assemble_tile` reads them through the process pool and composes the tile,
//! and `tile_stream` drives the plans of a query in order with bounded read-ahead.

use crate::error::Error;
use crate::source::gdal_worker_process::reader::GdalPoolReader;
use crate::source::gdal_worker_process::{
    GdalDatasetParameters, GdalPoolDispatcher, GdalProcessPoolError, GdalProcessReadResult,
    GdalReaderMode,
};
use crate::util::Result;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use gdal::raster::GdalType;
use geoengine_datatypes::primitives::{CacheHint, TimeInterval};
use geoengine_datatypes::raster::{
    BoundedGrid, ChangeGridBounds, EmptyGrid, GridBoundingBox2D, GridOrEmpty, Pixel, RasterTile2D,
    TileInformation,
};
use num::FromPrimitive;
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

/// Reads the files of `plan` and composes its tile.
///
/// The highest z-index wins at each valid pixel. A file is read only where the files above it
/// left pixels invalid, so files are read one after another. The properties come from the first
/// file that was read. A tile no file covers is empty.
///
/// # Errors
/// Returns an error if a pool read fails. A missing file counts as no data if its parameters
/// say so.
pub async fn assemble_tile<T: Pixel + GdalType + FromPrimitive>(
    plan: TilePlan,
    reader_mode: GdalReaderMode,
    gdal_worker: &GdalPoolDispatcher,
) -> Result<RasterTile2D<T>, GdalProcessPoolError> {
    let tile_bounds = plan.tile.global_pixel_bounds();
    let tile_spatial_grid = plan.tile.spatial_grid_definition();
    let mut raster: GridOrEmpty<GridBoundingBox2D, T> =
        GridOrEmpty::from(EmptyGrid::new(tile_bounds));
    let mut properties = None;
    let reader = GdalPoolReader::from(gdal_worker.clone());

    for params in plan.files {
        let Some(read_advise) = reader_mode
            .tiling_to_dataset_read_advise(&params.spatial_grid_definition(), &tile_spatial_grid)
        else {
            trace!(
                file = %params.file_path.display(),
                "file does not intersect tile {:?}",
                plan.tile.global_tile_position,
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

        let read = reader.read_tile_data::<T>(params, read_advise).await?;
        let file_data = match read {
            GdalProcessReadResult::Grid(grid_and_properties) => *grid_and_properties,
            GdalProcessReadResult::FileNotFoundAsNoData => continue,
        };

        if raster.is_empty() && file_data.grid.bounding_box() == tile_bounds {
            // nothing to merge with: take the grid instead of copying it
            raster = file_data.grid;
        } else {
            raster.grid_blit_fill_no_data(&file_data.grid);
        }
        properties.get_or_insert(file_data.properties);
    }

    Ok(RasterTile2D::new_with_properties(
        plan.time,
        plan.tile.global_tile_position,
        plan.band,
        plan.tile.global_geo_transform,
        raster.unbounded(),
        properties.unwrap_or_default(),
        plan.cache_hint,
    ))
}

/// Assembles the tiles of `plans` in their order, with up to `read_ahead` tiles in flight.
/// `into_error` tags pool errors with the calling source.
pub fn tile_stream<'a, T>(
    plans: impl Stream<Item = Result<TilePlan>> + Send + 'a,
    reader_mode: GdalReaderMode,
    gdal_worker: GdalPoolDispatcher,
    read_ahead: usize,
    into_error: fn(GdalProcessPoolError) -> Error,
) -> BoxStream<'a, Result<RasterTile2D<T>>>
where
    T: Pixel + GdalType + FromPrimitive,
{
    plans
        .map(move |plan| {
            let gdal_worker = gdal_worker.clone();
            async move {
                assemble_tile(plan?, reader_mode, &gdal_worker)
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
    use crate::source::gdal_worker_process::{
        FileNotFoundHandling, GdalDatasetGeoTransform, GdalMetadataMapping, GdalProcessPool,
        GdalProcessPoolAccess, ReaderState, WorkerConfig,
    };
    use geoengine_datatypes::raster::{
        GeoTransform, RasterPropertiesEntry, RasterPropertiesEntryType, RasterPropertiesKey,
    };
    use geoengine_datatypes::test_data;
    use geoengine_datatypes::util::NonZeroFiniteF64;

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
            tile_size_in_pixels: [8, 8].into(),
            global_tile_position: [0, 0].into(),
            global_geo_transform: GeoTransform::new_unchecked((-180., 90.).into(), 0.1, -0.1),
        };

        let gpp = GdalProcessPool::new(2, 2, 2, WorkerConfig::default());

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
}
