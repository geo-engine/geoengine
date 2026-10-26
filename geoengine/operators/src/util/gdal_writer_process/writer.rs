//! The GDAL side of the writer protocol. It runs inside the `gdalwriter-process` binary.

use super::{OverviewResampling, WriterBegin, WriterRequest, WriterResponse};
use crate::source::gdal_worker_process::process_common::IpcProcessError;
use gdal::{
    Dataset, DriverManager,
    raster::{Buffer, RasterCreationOptions},
    spatial_ref::SpatialRef,
};
use geoengine_datatypes::raster::{GeoTransform, RasterDataType};
use ipc_channel::ipc::{IpcReceiver, IpcSender};
use std::path::PathBuf;

/// Calls `$body` with `$t` bound to the Rust pixel type of `$data_type`.
macro_rules! with_pixel_type {
    ($data_type:expr, $t:ident => $body:expr) => {
        match $data_type {
            RasterDataType::U8 => {
                type $t = u8;
                $body
            }
            RasterDataType::U16 => {
                type $t = u16;
                $body
            }
            RasterDataType::U32 => {
                type $t = u32;
                $body
            }
            RasterDataType::U64 => {
                type $t = u64;
                $body
            }
            RasterDataType::I8 => {
                type $t = i8;
                $body
            }
            RasterDataType::I16 => {
                type $t = i16;
                $body
            }
            RasterDataType::I32 => {
                type $t = i32;
                $body
            }
            RasterDataType::I64 => {
                type $t = i64;
                $body
            }
            RasterDataType::F32 => {
                type $t = f32;
                $body
            }
            RasterDataType::F64 => {
                type $t = f64;
                $body
            }
        }
    };
}

/// Serves one writer session: receives requests until `Finish` or an error and sends one reply.
/// On error, the files that are being written are deleted.
pub fn run_writer(receiver: &IpcReceiver<WriterRequest>, sender: &IpcSender<WriterResponse>) {
    let mut writer = Writer::default();

    let result = writer.serve(receiver);

    if let Err(error) = &result {
        tracing::error!(?error, "gdal writer failed");
        writer.discard_current();
    }

    if let Err(error) = sender.send(result) {
        tracing::error!(?error, "gdal writer cannot send its result");
    }
}

/// A single-band file with an internal mask.
struct OpenFile {
    dataset: Dataset,
    /// the file GDAL writes to: `path` for `GTiff`, a temporary file for COG
    write_path: PathBuf,
    path: PathBuf,
}

#[derive(Default)]
struct Writer {
    begin: Option<WriterBegin>,
    /// the files of the current time step, one per band
    current: Vec<OpenFile>,
}

impl Writer {
    fn serve(&mut self, receiver: &IpcReceiver<WriterRequest>) -> Result<(), IpcProcessError> {
        loop {
            match receiver.recv()? {
                WriterRequest::Begin(begin) => {
                    // a mask in a separate .msk file would not be copied along with the dataset
                    gdal::config::set_config_option("GDAL_TIFF_INTERNAL_MASK", "YES")?;
                    self.begin = Some(begin);
                }
                WriterRequest::NextTimeStep { paths } => {
                    self.finish_current()?;
                    let begin = self.begin()?;
                    if paths.len() != begin.band_resampling.len() {
                        return Err(protocol_error("one path per band expected"));
                    }
                    for path in paths {
                        let file = create_file(self.begin()?, path)?;
                        self.current.push(file);
                    }
                }
                WriterRequest::Tile {
                    band,
                    x_offset,
                    y_offset,
                    width,
                    height,
                    data,
                    mask,
                } => {
                    let begin = self.begin()?;
                    let file = self
                        .current
                        .get(band)
                        .ok_or_else(|| protocol_error("tile without file"))?;
                    let window = (x_offset as isize, y_offset as isize);
                    let mut raster_band = file.dataset.rasterband(1)?;
                    with_pixel_type!(begin.data_type, T => {
                        let pixels: Vec<T> = bytemuck::pod_collect_to_vec(&data);
                        let mut buffer = Buffer::new((width, height), pixels);
                        raster_band.write(window, (width, height), &mut buffer)?;
                    });
                    // unwritten mask blocks read as invalid, so valid tiles need their mask, too
                    let mask = mask.unwrap_or_else(|| vec![255; width * height]);
                    let mut mask_buffer = Buffer::new((width, height), mask);
                    raster_band.open_mask_band()?.write(
                        window,
                        (width, height),
                        &mut mask_buffer,
                    )?;
                }
                WriterRequest::Finish => return self.finish_current(),
            }
        }
    }

    fn begin(&self) -> Result<&WriterBegin, IpcProcessError> {
        self.begin
            .as_ref()
            .ok_or_else(|| protocol_error("request before begin"))
    }

    fn finish_current(&mut self) -> Result<(), IpcProcessError> {
        let files = std::mem::take(&mut self.current);
        let begin = self.begin()?;
        for (file, resampling) in files.into_iter().zip(&begin.band_resampling) {
            finish_file(file, begin, *resampling)?;
        }
        Ok(())
    }

    fn discard_current(&mut self) {
        for file in self.current.drain(..) {
            drop(file.dataset);
            for path in [&file.write_path, &file.path] {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

fn protocol_error(msg: &str) -> IpcProcessError {
    IpcProcessError::IpcOther {
        msg: format!("gdal writer protocol: {msg}"),
    }
}

fn creation_options(pairs: &[(&str, String)]) -> Result<RasterCreationOptions, IpcProcessError> {
    let mut options = RasterCreationOptions::new();
    for (key, value) in pairs {
        options.add_name_value(key, value)?;
    }
    Ok(options)
}

fn predictor(data_type: RasterDataType) -> &'static str {
    match data_type {
        RasterDataType::F32 | RasterDataType::F64 => "3",
        _ => "2",
    }
}

fn create_file(begin: &WriterBegin, path: PathBuf) -> Result<OpenFile, IpcProcessError> {
    let write_path = if begin.as_cog {
        path.with_extension("tmp.tif")
    } else {
        path.clone()
    };

    let options = creation_options(&[
        ("COMPRESS", "ZSTD".to_string()),
        ("PREDICTOR", predictor(begin.data_type).to_string()),
        ("TILED", "YES".to_string()),
        ("BLOCKXSIZE", begin.block_size[0].to_string()),
        ("BLOCKYSIZE", begin.block_size[1].to_string()),
        ("SPARSE_OK", "TRUE".to_string()),
        ("BIGTIFF", "IF_SAFER".to_string()),
        ("NUM_THREADS", begin.compression_num_threads.to_string()),
    ])?;

    let driver = DriverManager::get_driver_by_name("GTiff")?;
    let mut dataset = with_pixel_type!(begin.data_type, T => driver
        .create_with_band_type_with_options::<T, _>(
            &write_path,
            begin.width,
            begin.height,
            1,
            &options,
        )?);

    dataset.set_geo_transform(&GeoTransform::from(begin.geo_transform).into())?;
    let spatial_ref = SpatialRef::try_from(begin.spatial_reference).map_err(|error| {
        IpcProcessError::IpcOther {
            msg: format!("invalid spatial reference: {error}"),
        }
    })?;
    dataset.set_spatial_ref(&spatial_ref)?;
    dataset.rasterband(1)?.create_mask_band(true)?;

    Ok(OpenFile {
        dataset,
        write_path,
        path,
    })
}

fn finish_file(
    file: OpenFile,
    begin: &WriterBegin,
    resampling: OverviewResampling,
) -> Result<(), IpcProcessError> {
    let OpenFile {
        mut dataset,
        write_path,
        path,
    } = file;

    let levels = overview_levels(begin);
    if !levels.is_empty() {
        // also builds the overviews of the internal mask
        dataset.build_overviews(resampling.as_gdal_str(), &levels, &[])?;
    }

    if !begin.as_cog {
        dataset.close()?;
        return Ok(());
    }

    let options = creation_options(&[
        ("COMPRESS", "ZSTD".to_string()),
        ("PREDICTOR", "YES".to_string()),
        // shortcut: COG blocks are square, so non-square tiling specs use the tile width
        ("BLOCKSIZE", begin.block_size[0].to_string()),
        ("OVERVIEWS", "FORCE_USE_EXISTING".to_string()),
        ("SPARSE_OK", "TRUE".to_string()),
        ("BIGTIFF", "IF_SAFER".to_string()),
        ("NUM_THREADS", begin.compression_num_threads.to_string()),
    ])?;
    let cog_driver = DriverManager::get_driver_by_name("COG")?;
    dataset.create_copy(&cog_driver, &path, &options)?.close()?;

    let gtiff_driver = dataset.driver();
    dataset.close()?;
    gtiff_driver.delete(&write_path)?;

    Ok(())
}

/// Overview factors 2, 4, 8, … until the coarsest level fits into one block.
fn overview_levels(begin: &WriterBegin) -> Vec<i32> {
    let max_size = begin.width.max(begin.height);
    let block_size = begin.block_size[0].min(begin.block_size[1]).max(1);

    let mut levels = Vec::new();
    let mut factor = 2;
    while max_size.div_ceil(factor / 2) > block_size {
        levels.push(factor as i32);
        factor *= 2;
    }
    levels
}

impl OverviewResampling {
    fn as_gdal_str(self) -> &'static str {
        match self {
            OverviewResampling::Nearest => "NEAREST",
            OverviewResampling::Average => "AVERAGE",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::gdal_worker_process::GdalDatasetGeoTransform;
    use crate::util::raster_stream_to_geotiff::GdalCompressionNumThreads;
    use geoengine_datatypes::primitives::Coordinate2D;
    use geoengine_datatypes::spatial_reference::SpatialReference;

    #[test]
    fn it_writes_masks_and_per_band_overviews() {
        let dir = tempfile::tempdir().unwrap();
        let begin = WriterBegin {
            data_type: RasterDataType::U8,
            width: 32,
            height: 32,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: Coordinate2D::new(0., 0.),
                x_pixel_size: 1.0.try_into().unwrap(),
                y_pixel_size: (-1.0).try_into().unwrap(),
            },
            spatial_reference: SpatialReference::epsg_4326(),
            block_size: [16, 16],
            band_resampling: vec![OverviewResampling::Nearest, OverviewResampling::Average],
            as_cog: false,
            compression_num_threads: GdalCompressionNumThreads::NumThreads(1),
        };
        assert_eq!(overview_levels(&begin), vec![2]);

        let paths: Vec<PathBuf> = (0..2)
            .map(|b| dir.path().join(format!("b{b}.tif")))
            .collect();
        // columns alternate 0 and 10: NEAREST keeps 0, AVERAGE gives 5
        let row: Vec<u8> = (0..16).map(|x| if x % 2 == 0 { 0 } else { 10 }).collect();
        let mut mask = vec![255_u8; 16 * 16];
        mask[0] = 0;

        let mut requests = vec![
            WriterRequest::Begin(begin),
            WriterRequest::NextTimeStep {
                paths: paths.clone(),
            },
        ];
        // only the upper left tile of each band is written, the others stay empty
        for band in 0..2 {
            requests.push(WriterRequest::Tile {
                band,
                x_offset: 0,
                y_offset: 0,
                width: 16,
                height: 16,
                data: row.repeat(16),
                mask: (band == 0).then(|| mask.clone()),
            });
        }
        requests.push(WriterRequest::Finish);

        let (request_tx, request_rx) = ipc_channel::ipc::channel().unwrap();
        let (response_tx, response_rx) = ipc_channel::ipc::channel().unwrap();
        for request in requests {
            request_tx.send(request).unwrap();
        }
        run_writer(&request_rx, &response_tx);
        response_rx.recv().unwrap().unwrap();

        let read_mask = |path: &PathBuf, (x, y): (isize, isize)| -> u8 {
            Dataset::open(path)
                .unwrap()
                .rasterband(1)
                .unwrap()
                .open_mask_band()
                .unwrap()
                .read_as::<u8>((x, y), (1, 1), (1, 1), None)
                .unwrap()
                .data()[0]
        };
        assert_eq!(read_mask(&paths[0], (0, 0)), 0, "masked pixel");
        assert_eq!(read_mask(&paths[0], (1, 0)), 255, "valid pixel");
        assert_eq!(read_mask(&paths[1], (0, 0)), 255, "unmasked tile");
        assert_eq!(read_mask(&paths[1], (20, 20)), 0, "empty tile");

        let overview_pixel = |path: &PathBuf| -> u8 {
            Dataset::open(path)
                .unwrap()
                .rasterband(1)
                .unwrap()
                .overview(0)
                .unwrap()
                .read_as::<u8>((1, 0), (1, 1), (1, 1), None)
                .unwrap()
                .data()[0]
        };
        assert_eq!(overview_pixel(&paths[0]), 0);
        assert_eq!(overview_pixel(&paths[1]), 5);

        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            2,
            "masks are internal, no .msk files"
        );
    }
}
