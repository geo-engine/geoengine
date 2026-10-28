//! Writes raster streams to files in a separate `gdalwriter-process`, so that the server never calls GDAL.
//!
//! The server sends a [`WriterBegin`], then for every time step a [`WriterRequest::NextTimeStep`] followed by its
//! tiles, and finally [`WriterRequest::Finish`]. The writer answers once with a [`WriterResponse`].
//! Each file holds one band of one time step as a tiled `GTiff` (or COG) with an internal mask and overviews.
//! One file per band, because a `GTiff` holds only one internal mask for all of its bands.

pub mod writer;

use crate::engine::{BoxRasterQueryProcessor, QueryContext};
use crate::error::{self, Error};
use crate::source::gdal_worker_process::GdalDatasetGeoTransform;
use crate::source::gdal_worker_process::process_common::IpcProcessError;
use crate::source::gdal_worker_process::process_impl::{
    WorkerConfig, sibling_binary_path, spawn_ipc_process,
};
use crate::util::raster_stream_to_geotiff::GdalCompressionNumThreads;
use crate::util::{Result, abortable_query_execution, spawn_blocking};
use bytemuck::NoUninit;
use futures::StreamExt;
use futures::future::BoxFuture;
use geoengine_datatypes::primitives::{
    DateTimeParseFormat, Measurement, RasterQueryRectangle, TimeInterval,
};
use geoengine_datatypes::raster::{
    GridBoundingBox2D, GridBounds, GridIdx2D, GridOrEmpty, GridShapeAccess, GridSize, Pixel,
    RasterDataType, RasterTile2D,
};
use geoengine_datatypes::spatial_reference::SpatialReference;
use num::integer::div_floor;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Session parameters of one writer process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriterBegin {
    pub data_type: RasterDataType,
    pub width: usize,
    pub height: usize,
    pub geo_transform: GdalDatasetGeoTransform,
    pub spatial_reference: SpatialReference,
    /// block size `[x, y]`, equal to the tile size, so every tile is written as exactly one block
    pub block_size: [usize; 2],
    /// one entry per band, in band selection order
    pub band_resampling: Vec<OverviewResampling>,
    pub as_cog: bool,
    pub compression_num_threads: GdalCompressionNumThreads,
}

/// Resampling for the overviews of one band.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OverviewResampling {
    Nearest,
    Average,
}

impl From<&Measurement> for OverviewResampling {
    fn from(measurement: &Measurement) -> Self {
        match measurement {
            Measurement::Continuous(_) => Self::Average,
            // averaging classes or unknown values invents values that do not exist
            Measurement::Classification(_) | Measurement::Unitless => Self::Nearest,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WriterRequest {
    Begin(WriterBegin),
    /// Finish the current files (overviews, COG) and start new ones, one per band.
    NextTimeStep {
        paths: Vec<PathBuf>,
    },
    /// Pixels of one band (0-based, in band selection order) at a pixel offset into its file.
    Tile {
        band: usize,
        x_offset: usize,
        y_offset: usize,
        width: usize,
        height: usize,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
        /// validity mask, 255 = valid and 0 = invalid, `None` if all pixels are valid
        #[serde(with = "serde_bytes")]
        mask: Option<Vec<u8>>,
    },
    /// Finish the current files and end the session.
    Finish,
}

pub type WriterResponse = Result<(), IpcProcessError>;

/// Options for [`raster_stream_to_files`].
#[derive(Debug, Clone, Copy)]
pub struct GdalWriterOptions {
    pub as_cog: bool,
    pub compression_num_threads: GdalCompressionNumThreads,
}

/// The files written by [`raster_stream_to_files`]. All files share the same grid and use internal masks.
#[derive(Debug, Clone)]
pub struct WrittenRaster {
    /// per time step, in stream order, one single-band file per selected band
    pub files: Vec<(TimeInterval, Vec<PathBuf>)>,
    /// the pixel bounds of the files, snapped to the tiling grid
    pub grid_bounds: GridBoundingBox2D,
    pub geo_transform: GdalDatasetGeoTransform,
}

static GDALWRITER_PROCESS_PATH: OnceLock<PathBuf> = OnceLock::new();

fn gdalwriter_path() -> &'static Path {
    GDALWRITER_PROCESS_PATH
        .get_or_init(|| sibling_binary_path("gdalwriter-process", "GDALWRITER_PROCESS_PATH"))
}

/// Grows `bounds` to whole tiles of `tile_size` (`[y, x]`).
fn snap_to_tiles(bounds: GridBoundingBox2D, tile_size: [usize; 2]) -> GridBoundingBox2D {
    let [tile_y, tile_x] = tile_size.map(|s| s as isize);
    let min = bounds.min_index();
    let max = bounds.max_index();
    GridBoundingBox2D::new_min_max(
        div_floor(min.y(), tile_y) * tile_y,
        (div_floor(max.y(), tile_y) + 1) * tile_y - 1,
        div_floor(min.x(), tile_x) * tile_x,
        (div_floor(max.x(), tile_x) + 1) * tile_x - 1,
    )
    .expect("snapped bounds contain the original bounds")
}

fn file_name(time: TimeInterval, band: u32) -> Result<String> {
    let time_string = time
        .start()
        .as_date_time()
        .ok_or(Error::TimeInstanceNotDisplayable)?
        .format(&DateTimeParseFormat::custom(
            "%Y-%m-%dT%H-%M-%S-%3f".to_string(),
        ));
    Ok(format!("raster_{time_string}_b{band}.tif"))
}

/// Converts a tile into a writer request, or `None` for an empty tile (left sparse and invalid in the file).
fn tile_request<P: Pixel + NoUninit>(
    tile: RasterTile2D<P>,
    band: usize,
    file_bounds: GridBoundingBox2D,
) -> Option<WriterRequest> {
    let offset: GridIdx2D =
        tile.tile_information().global_pixel_bounds().min_index() - file_bounds.min_index();
    let (width, height) = (
        tile.grid_shape().axis_size_x(),
        tile.grid_shape().axis_size_y(),
    );

    let GridOrEmpty::Grid(grid) = tile.grid_array else {
        return None;
    };

    let mask = grid.validity_mask.data.iter().any(|valid| !valid).then(|| {
        grid.validity_mask
            .data
            .iter()
            .map(|valid| if *valid { 255 } else { 0 })
            .collect()
    });

    Some(WriterRequest::Tile {
        band,
        x_offset: offset.x() as usize,
        y_offset: offset.y() as usize,
        width,
        height,
        data: bytemuck::cast_slice(&grid.inner_grid.data).to_vec(),
        mask,
    })
}

/// Writes the raster stream of `query_rect` into one file per time step in `out_dir`,
/// using a `gdalwriter-process` spawned with `worker_config`.
///
/// The files cover the query bounds grown to whole tiles. Invalid pixels are masked, empty tiles are not written.
///
/// # Errors
/// Fails if the query, the IPC or the writer fails. Files of a failed write may remain in `out_dir`.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn raster_stream_to_files<P, C>(
    out_dir: &Path,
    processor: BoxRasterQueryProcessor<P>,
    query_rect: RasterQueryRectangle,
    mut query_ctx: C,
    spatial_reference: SpatialReference,
    options: GdalWriterOptions,
    worker_config: &WorkerConfig,
    conn_closed: BoxFuture<'_, ()>,
) -> Result<WrittenRaster>
where
    P: Pixel + NoUninit,
    C: QueryContext + 'static,
{
    let query_abort_trigger = query_ctx.abort_trigger()?;

    let result_descriptor = processor.raster_result_descriptor();
    let tile_size = query_ctx.tiling_specification().tile_size_in_pixels;
    let tile_size = [tile_size.axis_size_y(), tile_size.axis_size_x()];
    let tiling_geo_transform = result_descriptor
        .tiling_grid_definition(query_ctx.tiling_specification())
        .tiling_geo_transform();

    let file_bounds = snap_to_tiles(query_rect.spatial_bounds(), tile_size);
    let geo_transform = GdalDatasetGeoTransform {
        origin_coordinate: tiling_geo_transform
            .grid_idx_to_pixel_upper_left_coordinate_2d(file_bounds.min_index()),
        x_pixel_size: tiling_geo_transform.x_pixel_size().into(),
        y_pixel_size: tiling_geo_transform.y_pixel_size().into(),
    };

    let bands = query_rect.attributes().as_vec();
    let band_resampling = bands
        .iter()
        .map(|band| OverviewResampling::from(&result_descriptor.bands[*band as usize].measurement))
        .collect::<Vec<_>>();

    let begin = WriterBegin {
        data_type: result_descriptor.data_type,
        width: file_bounds.axis_size_x(),
        height: file_bounds.axis_size_y(),
        geo_transform,
        spatial_reference,
        // GTiff blocks must be multiples of 16; other tile sizes still work, just not block-aligned
        block_size: [tile_size[1].next_multiple_of(16), tile_size[0].next_multiple_of(16)],
        band_resampling,
        as_cog: options.as_cog,
        compression_num_threads: options.compression_num_threads,
    };

    let worker_config = worker_config.clone();
    // the guard kills the writer when this future is dropped, which also ends the bridge thread
    let (_writer_guard, sender, receiver) = spawn_blocking(move || {
        spawn_ipc_process::<WriterRequest, WriterResponse>(gdalwriter_path(), &worker_config)
    })
    .await
    .context(error::TokioJoin)?
    .context(error::GdalWriterProcess)?;

    let (tx, mut rx) = tokio::sync::mpsc::channel::<WriterRequest>(2 * bands.len());

    // one blocking thread forwards the requests, so the async runtime never blocks on IPC
    let bridge = spawn_blocking(move || {
        while let Some(request) = rx.blocking_recv() {
            if sender.send(request).is_err() {
                break; // the writer stopped; its reply says why
            }
        }
        receiver.recv()
    });

    let out_dir = out_dir.to_owned();
    let files =
        abortable_query_execution(
            async move {
                let mut files: Vec<(TimeInterval, Vec<PathBuf>)> = Vec::new();
                let mut tile_stream = processor
                    .raster_query(query_rect.clone(), &query_ctx)
                    .await?;

                // a send error means the writer stopped; the bridge result below says why
                let _ = tx.send(WriterRequest::Begin(begin)).await;

                while let Some(tile) = tile_stream.next().await {
                    let tile = tile?;

                    if files.last().is_none_or(|(time, _)| *time != tile.time) {
                        let paths = bands
                            .iter()
                            .map(|band| Ok(out_dir.join(file_name(tile.time, *band)?)))
                            .collect::<Result<Vec<_>>>()?;
                        files.push((tile.time, paths.clone()));
                        if tx
                            .send(WriterRequest::NextTimeStep { paths })
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }

                    let band = bands.iter().position(|b| *b == tile.band).ok_or(
                        Error::GdalWriterProcess {
                            source: IpcProcessError::IpcOther {
                                msg: format!("tile of unselected band {}", tile.band),
                            },
                        },
                    )?;

                    if let Some(request) = tile_request(tile, band, file_bounds)
                        && tx.send(request).await.is_err()
                    {
                        break;
                    }
                }

                let _ = tx.send(WriterRequest::Finish).await;
                drop(tx);

                bridge
                    .await
                    .context(error::TokioJoin)?
                    .map_err(|e| Error::GdalWriterProcess { source: e.into() })?
                    .context(error::GdalWriterProcess)?;

                Ok(files)
            },
            conn_closed,
            query_abort_trigger,
        )
        .await?;

    Ok(WrittenRaster {
        files,
        grid_bounds: file_bounds,
        geo_transform,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        MockExecutionContext, RasterBandDescriptor, RasterBandDescriptors, RasterOperator,
        RasterResultDescriptor, SpatialGridDescriptor, TimeDescriptor, WorkflowOperatorPath,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use gdal::{Dataset, Metadata};
    use geoengine_datatypes::primitives::{BandSelection, CacheHint, Coordinate2D};
    use geoengine_datatypes::raster::{
        BoundedGrid, GeoTransform, Grid2D, GridShape2D, MaskedGrid2D, TileInformation,
        TilingSpecification,
    };

    const SIZE: usize = 16; // the smallest block size GTiff accepts

    fn tile(time: TimeInterval, band: u32, grid: GridOrEmpty<GridShape2D, u8>) -> RasterTile2D<u8> {
        RasterTile2D::new_with_tile_info(
            time,
            TileInformation {
                global_geo_transform: GeoTransform::new_unchecked(
                    Coordinate2D::new(0., 0.),
                    1.,
                    -1.,
                ),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: GridShape2D::new_2d(SIZE, SIZE),
            },
            band,
            grid,
            CacheHint::no_cache(),
        )
    }

    /// Writes 2 time steps of a 2-band u8 raster: one pixel of t0/b0 is masked, t1/b1 is empty.
    async fn write(out_dir: &Path, as_cog: bool) -> WrittenRaster {
        let shape = GridShape2D::new_2d(SIZE, SIZE);
        let t0 = TimeInterval::new(0, 10).unwrap();
        let t1 = TimeInterval::new(10, 20).unwrap();

        let mut mask = vec![true; SIZE * SIZE];
        mask[0] = false;
        let masked: MaskedGrid2D<u8> = MaskedGrid2D::new(
            Grid2D::new(shape, vec![7; SIZE * SIZE]).unwrap(),
            Grid2D::new(shape, mask).unwrap(),
        )
        .unwrap();
        let full = |v: u8| -> GridOrEmpty<GridShape2D, u8> {
            MaskedGrid2D::from(Grid2D::new(shape, vec![v; SIZE * SIZE]).unwrap()).into()
        };

        let data = vec![
            tile(t0, 0, masked.into()),
            tile(t0, 1, full(1)),
            tile(t1, 0, full(2)),
            tile(t1, 1, GridOrEmpty::new_empty_shape(shape)),
        ];

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_irregular(Some(TimeInterval::new(0, 20).unwrap())),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 1., -1.),
                shape.bounding_box(),
            ),
            bands: RasterBandDescriptors::new(vec![
                RasterBandDescriptor::new_unitless("a".into()),
                RasterBandDescriptor::new_unitless("b".into()),
            ])
            .unwrap(),
        };

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(shape));
        let processor = MockRasterSource {
            params: MockRasterSourceParams {
                data,
                result_descriptor,
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .get_u8()
        .unwrap();

        raster_stream_to_files(
            out_dir,
            processor,
            RasterQueryRectangle::new(
                shape.bounding_box(),
                TimeInterval::new(0, 20).unwrap(),
                BandSelection::first_n(2),
            ),
            exe_ctx.mock_query_context_test_default(),
            SpatialReference::epsg_4326(),
            GdalWriterOptions {
                as_cog,
                compression_num_threads: GdalCompressionNumThreads::NumThreads(1),
            },
            &WorkerConfig::default(),
            Box::pin(futures::future::pending()),
        )
        .await
        .unwrap()
    }

    /// Reads a band file as `(values, mask)`.
    fn read(path: &Path) -> (Vec<u8>, Vec<u8>) {
        let dataset = Dataset::open(path).unwrap();
        let band = dataset.rasterband(1).unwrap();
        let read = |b: &gdal::raster::RasterBand| {
            b.read_as::<u8>((0, 0), (SIZE, SIZE), (SIZE, SIZE), None)
                .unwrap()
                .data()
                .to_vec()
        };
        (read(&band), read(&band.open_mask_band().unwrap()))
    }

    #[tokio::test]
    async fn it_writes_masked_gtiffs_per_time_step_and_band() {
        let dir = tempfile::tempdir().unwrap();
        let written = write(dir.path(), false).await;

        assert_eq!(written.files.len(), 2);
        assert!(written.files.iter().all(|(_, paths)| paths.len() == 2));

        let t0 = Dataset::open(&written.files[0].1[0]).unwrap();
        assert_eq!(t0.raster_count(), 1);
        assert_eq!(
            t0.metadata_item("COMPRESSION", "IMAGE_STRUCTURE")
                .as_deref(),
            Some("ZSTD")
        );

        let (values, mask) = read(&written.files[0].1[0]);
        assert_eq!(mask[0], 0, "the masked pixel is invalid");
        assert!(mask[1..].iter().all(|v| *v == 255));
        assert!(values[1..].iter().all(|v| *v == 7));

        let (values, mask) = read(&written.files[0].1[1]);
        assert!(values.iter().all(|v| *v == 1) && mask.iter().all(|v| *v == 255));

        let (values, _) = read(&written.files[1].1[0]);
        assert!(values.iter().all(|v| *v == 2));

        let (_, mask) = read(&written.files[1].1[1]);
        assert!(mask.iter().all(|v| *v == 0), "the empty tile is invalid");

        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            4,
            "masks are internal, no .msk files"
        );
    }

    #[tokio::test]
    async fn it_writes_cogs() {
        let dir = tempfile::tempdir().unwrap();
        let written = write(dir.path(), true).await;

        let t0 = Dataset::open(&written.files[0].1[0]).unwrap();
        assert_eq!(
            t0.metadata_item("LAYOUT", "IMAGE_STRUCTURE").as_deref(),
            Some("COG")
        );
        let (_, mask) = read(&written.files[0].1[0]);
        assert_eq!(mask[0], 0, "the COG keeps the mask");

        let file_count = std::fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(file_count, 4, "the temporary GTiffs are deleted");
    }

    #[test]
    fn it_snaps_bounds_to_tiles() {
        let bounds = GridBoundingBox2D::new([-3, 5], [20, 17]).unwrap();
        assert_eq!(
            snap_to_tiles(bounds, [16, 16]),
            GridBoundingBox2D::new([-16, 0], [31, 31]).unwrap()
        );
    }
}
