//! Writes raster streams as `GeoTIFF` without GDAL, with the pure-Rust `geotiff-writer` crate.
//!
//! This is a prototype next to the GDAL writer in [`super::raster_stream_to_geotiff`].
//! It writes the same layout (one `GeoTIFF` band per time step and selected band), but it cannot
//! write everything the GDAL writer can. In these cases, it falls back to the GDAL writer:
//!
//! - Spatial references other than EPSG codes, because they need user-defined `GeoKeys`.
//! - Integer rasters with masked pixels and without a no-data value, because `geotiff-writer`
//!   cannot write internal mask IFDs (`NewSubfileType = 4`). Float rasters use NaN instead.

use super::raster_stream_to_geotiff::{
    GdalGeoTiffDatasetMetadata, GdalGeoTiffOptions, band_names, consume_stream_into_vec,
    geotiff_band_positions, write_multiband_geotiff_bytes,
};
use super::{abortable_query_execution, spawn_blocking};
use crate::engine::{BoxRasterQueryProcessor, QueryContext, RasterQueryProcessor};
use crate::error::Error;
use crate::util::{Result, spawn_query_task};
use crs_constants::{CrsType, EpsgBounds};
use futures::future::BoxFuture;
use gdal::raster::GdalType;
use geoengine_datatypes::primitives::{CacheHint, RasterQueryRectangle, TimeInterval};
use geoengine_datatypes::raster::{
    GeoTransform, GridBlit, GridBoundingBox2D, GridBounds, GridOrEmpty, GridSize, Pixel,
    RasterDataType, RasterTile2D, StaticRasterDataType, TilingSpecification,
};
use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceAuthority};
use geotiff_writer::{
    Compression, GeoTiffBuilder, NumericSample, PlanarConfiguration, Predictor, RasterType, Tag,
    TagValue,
};
use ndarray::ArrayView3;
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;
use std::io::Cursor;
use tracing::debug;

/// The `GDAL_METADATA` TIFF tag: an XML document with band descriptions and metadata items
const TAG_GDAL_METADATA: u16 = 42112;

/// The library that writes `GeoTIFF` files
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GeoTiffWriter {
    /// GDAL's `GTiff` driver
    #[default]
    Gdal,
    /// The pure-Rust `geotiff-writer` crate, with a fallback to GDAL for unsupported inputs
    Rust,
}

/// Like [`super::raster_stream_to_geotiff::raster_stream_to_multiband_geotiff_bytes`], but writes
/// the `GeoTIFF` with `geotiff-writer` instead of GDAL where possible.
///
/// `gdal_tiff_options` only apply to the fallback to the GDAL writer. The query runs only once:
/// the fallback writes the tiles that were already collected.
#[allow(clippy::too_many_arguments)]
pub async fn raster_stream_to_multiband_geotiff_bytes_rust<T, C: QueryContext + 'static>(
    processor: BoxRasterQueryProcessor<T>,
    query_rect: RasterQueryRectangle,
    band_order: Option<Vec<u32>>,
    mut query_ctx: C,
    gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
    gdal_tiff_options: GdalGeoTiffOptions,
    tile_limit: Option<usize>,
    conn_closed: BoxFuture<'_, ()>,
    tiling_specification: TilingSpecification,
) -> Result<(Vec<u8>, CacheHint)>
where
    T: Pixel + GdalType + NumericSample,
{
    let result_descriptor = processor.raster_result_descriptor();
    let tiling_strategy = result_descriptor
        .tiling_grid_definition(tiling_specification)
        .generate_data_tiling_strategy();

    let band_order = band_order.unwrap_or_else(|| query_rect.attributes().as_vec());
    let geotiff_band_positions = geotiff_band_positions(query_rect.attributes(), &band_order)?;
    let band_names = band_names(result_descriptor, &band_order);
    let crs = EpsgCrs::try_from_spatial_reference(gdal_tiff_metadata.spatial_reference);
    let output_geo_transform = GeoTransform::new_typed(
        tiling_strategy
            .geo_transform
            .grid_idx_to_pixel_upper_left_coordinate_2d(query_rect.spatial_bounds().min_index()),
        tiling_strategy.geo_transform.x_pixel_size(),
        tiling_strategy.geo_transform.y_pixel_size(),
    );

    let query_abort_trigger = query_ctx.abort_trigger()?;
    let runtime = query_ctx.processing_runtime();

    // query, GeoTIFF writing and copying the bytes run as one task off the caller's runtime
    let work = async move {
        let tiles =
            consume_stream_into_vec(processor, query_rect.clone(), query_ctx, tile_limit).await?;

        let crs = match crs {
            Some(crs)
                if gdal_tiff_metadata.no_data_value.is_some()
                    || is_float::<T>()
                    || !tiles.iter().any(has_masked_pixels) =>
            {
                crs
            }
            _ => {
                debug!(
                    spatial_reference = %gdal_tiff_metadata.spatial_reference,
                    "falling back to the GDAL GeoTIFF writer: no EPSG code, or masked integer pixels without a no-data value"
                );
                return write_multiband_geotiff_bytes(
                    tiles,
                    &query_rect,
                    band_names,
                    geotiff_band_positions,
                    tiling_strategy,
                    gdal_tiff_options,
                    gdal_tiff_metadata,
                )
                .await;
            }
        };

        let input = GeoTiffInput {
            query_rect,
            band_names,
            geotiff_band_positions,
            output_geo_transform,
            no_data_value: gdal_tiff_metadata.no_data_value,
            crs,
        };
        spawn_blocking(move || encode_geotiff(tiles, &input)).await?
    };

    abortable_query_execution(
        spawn_query_task(runtime, work),
        conn_closed,
        query_abort_trigger,
    )
    .await
}

/// A spatial reference that `geotiff-writer` can encode: an EPSG code with a known CRS type
#[derive(Debug, Clone, Copy)]
struct EpsgCrs {
    code: u16,
    crs_type: CrsType,
}

impl EpsgCrs {
    fn try_from_spatial_reference(spatial_reference: SpatialReference) -> Option<Self> {
        if *spatial_reference.authority() != SpatialReferenceAuthority::Epsg {
            return None;
        }
        let code = u16::try_from(spatial_reference.code()).ok()?;
        let bounds = EpsgBounds::from_code(code)?;
        Some(Self {
            code,
            crs_type: bounds.crs_type,
        })
    }
}

/// Everything besides the tiles that [`encode_geotiff`] needs
struct GeoTiffInput {
    query_rect: RasterQueryRectangle,
    /// the names of the `GeoTIFF` bands of one time step
    band_names: Vec<String>,
    /// maps the `i`-th selected band, in stream order, to its `GeoTIFF` band of the time step
    geotiff_band_positions: Vec<usize>,
    output_geo_transform: GeoTransform,
    no_data_value: Option<f64>,
    crs: EpsgCrs,
}

fn is_float<T: StaticRasterDataType>() -> bool {
    matches!(T::TYPE, RasterDataType::F32 | RasterDataType::F64)
}

fn has_masked_pixels<T>(tile: &RasterTile2D<T>) -> bool {
    match &tile.grid_array {
        GridOrEmpty::Empty(_) => true,
        GridOrEmpty::Grid(grid) => grid.validity_mask.data.iter().any(|valid| !valid),
    }
}

/// Assembles the tiles into one plane per time step and band and encodes them as `GeoTIFF`.
///
/// The caller must make sure that the pixels can be written, i.e., integer rasters without a
/// no-data value must not have masked pixels.
fn encode_geotiff<T>(
    tiles: Vec<RasterTile2D<T>>,
    input: &GeoTiffInput,
) -> Result<(Vec<u8>, CacheHint)>
where
    T: Pixel + NumericSample,
{
    let bounds = input.query_rect.spatial_bounds();
    let (width, height) = (bounds.axis_size_x(), bounds.axis_size_y());
    let num_bands = input.band_names.len();

    let mut time_steps: Vec<TimeInterval> = Vec::new();
    let mut planes: Vec<GridOrEmpty<GridBoundingBox2D, T>> = Vec::new();
    let mut tile_idx_in_time_step = 0;
    let mut cache_hint = CacheHint::max_duration();

    for tile in tiles {
        if time_steps.last() != Some(&tile.time) {
            time_steps.push(tile.time);
            tile_idx_in_time_step = 0;
            planes.extend((0..num_bands).map(|_| GridOrEmpty::new_empty_shape(bounds)));
        }

        // The stream delivers the selected bands of each tile position in ascending order,
        // so the position of the tile in its tile position is the index of its band in the selection.
        let selection_idx = tile_idx_in_time_step % num_bands;
        tile_idx_in_time_step += 1;
        let plane_idx =
            (time_steps.len() - 1) * num_bands + input.geotiff_band_positions[selection_idx];

        cache_hint.merge_with(&tile.cache_hint);
        planes[plane_idx].grid_blit_from(&tile.into_inner_positioned_grid());
    }

    let (fill_value, no_data) = fill_value_and_no_data::<T>(input.no_data_value, &planes);

    let mut samples = Vec::with_capacity(planes.len() * width * height);
    for plane in planes {
        let masked = plane.into_materialized_masked_grid();
        match fill_value {
            Some(fill_value) => samples.extend(
                masked
                    .inner_grid
                    .data
                    .into_iter()
                    .zip(masked.validity_mask.data)
                    .map(|(value, valid)| if valid { value } else { fill_value }),
            ),
            None => samples.extend(masked.inner_grid.data),
        }
    }

    let num_geotiff_bands = time_steps.len() * num_bands;
    // [band, row, col] in memory, viewed as [row, col, band] as `geotiff-writer` expects
    let samples = ArrayView3::from_shape((num_geotiff_bands, height, width), &samples)
        .map_err(rust_geotiff_error)?
        .permuted_axes([1, 2, 0]);

    let mut builder = GeoTiffBuilder::new(u32_dimension(width)?, u32_dimension(height)?)
        .bands(u32_dimension(num_geotiff_bands)?)
        .planar_configuration(PlanarConfiguration::Planar)
        .compression(Compression::Lzw)
        .predictor(if is_float::<T>() {
            Predictor::FloatingPoint
        } else {
            Predictor::Horizontal
        })
        .raster_type(RasterType::PixelIsArea)
        .pixel_scale(
            input.output_geo_transform.x_pixel_size().get(),
            -input.output_geo_transform.y_pixel_size().get(),
        )
        .origin(
            input.output_geo_transform.origin_coordinate.x,
            input.output_geo_transform.origin_coordinate.y,
        );
    builder = match input.crs.crs_type {
        CrsType::Projected => builder.projected_epsg(input.crs.code),
        CrsType::Geographic2d => builder.geographic_epsg(input.crs.code),
    };
    if let Some(no_data) = no_data {
        builder = builder.nodata(&no_data);
    }
    if let Some(gdal_metadata) = gdal_metadata(&input.band_names, &time_steps) {
        builder = builder.tag(Tag::new(TAG_GDAL_METADATA, TagValue::Ascii(gdal_metadata)));
    }

    let mut sink = Cursor::new(Vec::new());
    builder
        .write_3d_to(&mut sink, samples)
        .map_err(rust_geotiff_error)?;

    Ok((sink.into_inner(), cache_hint))
}

/// The value for masked pixels and the `GDAL_NODATA` tag.
///
/// Without a no-data value, float rasters with masked pixels use NaN, and all other rasters
/// keep their pixels and get no tag.
fn fill_value_and_no_data<T: Pixel>(
    no_data_value: Option<f64>,
    planes: &[GridOrEmpty<GridBoundingBox2D, T>],
) -> (Option<T>, Option<String>) {
    match no_data_value {
        Some(no_data) => (Some(T::from_(no_data)), Some(format_no_data(no_data))),
        None if is_float::<T>() && planes.iter().any(plane_has_masked_pixels) => {
            (Some(T::from_(f64::NAN)), Some("nan".to_string()))
        }
        None => (None, None),
    }
}

fn plane_has_masked_pixels<T>(plane: &GridOrEmpty<GridBoundingBox2D, T>) -> bool {
    match plane {
        GridOrEmpty::Empty(_) => true,
        GridOrEmpty::Grid(grid) => grid.validity_mask.data.iter().any(|valid| !valid),
    }
}

fn format_no_data(no_data: f64) -> String {
    if no_data.is_nan() {
        "nan".to_string()
    } else {
        no_data.to_string()
    }
}

/// The `GDAL_METADATA` XML with the band descriptions and the time interval of each band,
/// as GDAL's `GTiff` driver writes them.
///
/// A single raster band keeps its `GeoTIFF` bands without description, like the GDAL writer.
fn gdal_metadata(band_names: &[String], time_steps: &[TimeInterval]) -> Option<String> {
    let mut xml = String::from("<GDALMetadata>");
    for (time_step_idx, time) in time_steps.iter().enumerate() {
        for (band_position, band_name) in band_names.iter().enumerate() {
            let sample = time_step_idx * band_names.len() + band_position;
            if band_names.len() > 1 {
                let _ = write!(
                    xml,
                    r#"<Item name="DESCRIPTION" sample="{sample}" role="description">{}</Item>"#,
                    // GDAL escapes the description and then the whole item, and unescapes twice when reading
                    escape_xml(&escape_xml(band_name))
                );
            }
            let _ = write!(
                xml,
                r#"<Item name="start" domain="time" sample="{sample}">{}</Item><Item name="end" domain="time" sample="{sample}">{}</Item>"#,
                time.start().as_datetime_string_with_millis(),
                time.end().as_datetime_string_with_millis()
            );
        }
    }
    xml.push_str("</GDALMetadata>");

    (!time_steps.is_empty()).then_some(xml)
}

fn escape_xml(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            c => escaped.push(c),
        }
    }
    escaped
}

fn u32_dimension(value: usize) -> Result<u32> {
    u32::try_from(value).map_err(rust_geotiff_error)
}

fn rust_geotiff_error(source: impl std::fmt::Display) -> Error {
    Error::RustGeoTiffWriter {
        message: source.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        MockExecutionContext, RasterBandDescriptor, RasterBandDescriptors, RasterResultDescriptor,
        TimeDescriptor,
    };
    use crate::mock::MockRasterSourceProcessor;
    use crate::util::gdal::gdal_open_dataset;
    use crate::util::raster_stream_to_geotiff::{
        GdalCompressionNumThreads, raster_stream_to_multiband_geotiff_bytes,
    };
    use gdal::Metadata;
    use geoengine_datatypes::primitives::{BandSelection, Measurement};
    use geoengine_datatypes::raster::{Grid, MaskedGrid2D};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;
    use std::path::PathBuf;

    const TIME_STEPS: [(i64, i64); 2] = [(0, 10), (10, 20)];

    /// A `GeoTIFF` as GDAL reads it
    #[derive(Debug, PartialEq)]
    struct GeoTiffContent {
        size: (usize, usize),
        geo_transform: [f64; 6],
        epsg: Option<String>,
        bands: Vec<BandContent>,
    }

    /// A band's description, its values (`None` for masked pixels) and its time metadata (start, end)
    type BandContent = (String, Vec<Option<f64>>, Option<(String, String)>);

    struct WrittenGeoTiffs {
        gdal: GeoTiffContent,
        rust: GeoTiffContent,
        rust_mask_flags: Vec<i32>,
    }

    /// Two time steps of 2×2 tiles with `num_bands` bands; each tile's values encode time step,
    /// tile position and band, and `masked` marks the pixel at index 1 of every tile as invalid
    fn tiles<T: Pixel>(num_bands: u32, masked: bool) -> Vec<RasterTile2D<T>> {
        let mut tiles = Vec::new();
        for (time_step, (start, end)) in TIME_STEPS.into_iter().enumerate() {
            for tile_y in [-1, 0] {
                for tile_x in [0, 1] {
                    for band in 0..num_bands {
                        let base = (time_step * 100) as i32 + (tile_y + 1) * 40 + tile_x * 20;
                        let values = (0..4)
                            .map(|i| T::from_(base + band as i32 * 5 + i))
                            .collect::<Vec<_>>();
                        let grid = Grid::new([2, 2].into(), values).unwrap();
                        let validity =
                            Grid::new([2, 2].into(), vec![true, !masked, true, true]).unwrap();
                        tiles.push(RasterTile2D {
                            time: TimeInterval::new(start, end).unwrap(),
                            tile_position: [tile_y as isize, tile_x as isize].into(),
                            band,
                            global_geo_transform: TestDefault::test_default(),
                            grid_array: MaskedGrid2D::new(grid, validity).unwrap().into(),
                            properties: Default::default(),
                            cache_hint: CacheHint::no_cache(),
                        });
                    }
                }
            }
        }
        tiles
    }

    fn processor<T: Pixel>(
        data: Vec<RasterTile2D<T>>,
        num_bands: u32,
    ) -> BoxRasterQueryProcessor<T> {
        let mut result_descriptor = RasterResultDescriptor::with_datatype_and_num_bands(
            T::TYPE,
            num_bands,
            GridBoundingBox2D::new([-4, 0], [-1, 3]).unwrap(),
            GeoTransform::test_default(),
            TimeDescriptor::new_irregular(Some(TimeInterval::new(0, 20).unwrap())),
        );
        result_descriptor.bands = RasterBandDescriptors::new(
            (0..num_bands)
                .map(|band| {
                    RasterBandDescriptor::new(format!("band <{band}> &lt;"), Measurement::Unitless)
                })
                .collect(),
        )
        .unwrap();

        MockRasterSourceProcessor {
            result_descriptor,
            data,
            tiling_specification: TilingSpecification::new([2, 2].into()),
        }
        .boxed()
    }

    /// Writes the tiles with both writers into a `GeoTIFF` with 3×3 pixels that cuts through the 2×2 tiles,
    /// inside the data, so that there are no empty tiles
    async fn write_with_both<T: Pixel + GdalType + NumericSample>(
        data: Vec<RasterTile2D<T>>,
        num_bands: u32,
        band_order: Option<Vec<u32>>,
        gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
    ) -> WrittenGeoTiffs {
        let tiling_specification = TilingSpecification::new([2, 2].into());
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-2, 0], [0, 2]).unwrap(),
            TimeInterval::new(0, 20).unwrap(),
            BandSelection::first_n(num_bands),
        );
        let gdal_tiff_options = GdalGeoTiffOptions {
            as_cog: false,
            compression_num_threads: GdalCompressionNumThreads::NumThreads(1),
            force_big_tiff: false,
        };
        let ctx = || {
            MockExecutionContext::new_with_tiling_spec(tiling_specification)
                .mock_query_context_test_default()
        };

        let (gdal, _) = raster_stream_to_multiband_geotiff_bytes(
            processor(data.clone(), num_bands),
            query_rect.clone(),
            band_order.clone(),
            ctx(),
            gdal_tiff_metadata,
            gdal_tiff_options,
            None,
            Box::pin(futures::future::pending()),
            tiling_specification,
        )
        .await
        .unwrap();
        let (rust, _) = raster_stream_to_multiband_geotiff_bytes_rust(
            processor(data, num_bands),
            query_rect,
            band_order,
            ctx(),
            gdal_tiff_metadata,
            gdal_tiff_options,
            None,
            Box::pin(futures::future::pending()),
            tiling_specification,
        )
        .await
        .unwrap();

        let (rust, rust_mask_flags) = read_geotiff(rust);
        WrittenGeoTiffs {
            gdal: read_geotiff(gdal).0,
            rust,
            rust_mask_flags,
        }
    }

    fn read_geotiff(mut bytes: Vec<u8>) -> (GeoTiffContent, Vec<i32>) {
        let file_path = PathBuf::from(format!("/vsimem/{}.tif", uuid::Uuid::new_v4()));
        let _mem_file =
            gdal::vsi::create_mem_file_from_ref(&file_path, bytes.as_mut_slice()).unwrap();
        let dataset = gdal_open_dataset(&file_path).unwrap();
        let (width, height) = dataset.raster_size();

        let mut mask_flags = Vec::new();
        let bands = (1..=dataset.raster_count())
            .map(|band_idx| {
                let band = dataset.rasterband(band_idx).unwrap();
                let values = band
                    .read_as::<f64>((0, 0), (width, height), (width, height), None)
                    .unwrap()
                    .data()
                    .to_vec();
                let mask = band
                    .open_mask_band()
                    .unwrap()
                    .read_as::<u8>((0, 0), (width, height), (width, height), None)
                    .unwrap()
                    .data()
                    .to_vec();
                let flags = band.mask_flags().unwrap();
                mask_flags.push(if flags.is_all_valid() {
                    GMF_ALL_VALID
                } else if flags.is_nodata() {
                    GMF_NODATA
                } else if flags.is_per_dataset() {
                    GMF_PER_DATASET
                } else {
                    0
                });
                let time = band
                    .metadata_item("start", "time")
                    .zip(band.metadata_item("end", "time"));
                (
                    band.description().unwrap(),
                    values
                        .into_iter()
                        .zip(mask)
                        .map(|(value, mask)| (mask > 0).then_some(value))
                        .collect(),
                    time,
                )
            })
            .collect();

        let content = GeoTiffContent {
            size: (width, height),
            geo_transform: dataset.geo_transform().unwrap(),
            epsg: dataset
                .spatial_ref()
                .ok()
                .and_then(|srs| srs.authority().ok()),
            bands,
        };
        (content, mask_flags)
    }

    /// The GDAL writer sets no time metadata on the bands of the first time step,
    /// the Rust writer does, so compare them only where GDAL has them
    fn assert_same_content(written: &WrittenGeoTiffs) {
        let WrittenGeoTiffs { gdal, rust, .. } = written;
        assert_eq!(rust.size, gdal.size);
        assert_eq!(rust.geo_transform, gdal.geo_transform);
        assert_eq!(rust.epsg, gdal.epsg);
        assert_eq!(rust.bands.len(), gdal.bands.len());
        for (rust_band, gdal_band) in rust.bands.iter().zip(&gdal.bands) {
            assert_eq!(rust_band.0, gdal_band.0, "band description");
            assert_eq!(rust_band.1, gdal_band.1, "band values");
            assert!(
                rust_band.2.is_some(),
                "the Rust writer sets the time of every band"
            );
            if gdal_band.2.is_some() {
                assert_eq!(rust_band.2, gdal_band.2, "band time");
            }
        }
    }

    const GMF_ALL_VALID: i32 = 0x01;
    const GMF_PER_DATASET: i32 = 0x02;
    const GMF_NODATA: i32 = 0x08;

    #[tokio::test]
    async fn it_writes_the_same_bands_as_gdal_in_the_requested_order() {
        let written = write_with_both(
            tiles::<u8>(2, false),
            2,
            Some(vec![1, 0]),
            GdalGeoTiffDatasetMetadata {
                no_data_value: None,
                spatial_reference: SpatialReference::epsg_4326(),
            },
        )
        .await;

        assert_same_content(&written);
        assert_eq!(written.rust.bands.len(), 4);
        assert_eq!(written.rust.bands[0].0, "band <1> &lt;");
        assert_eq!(written.rust.epsg.as_deref(), Some("EPSG:4326"));
        // written by the Rust writer: without masked pixels, it needs no mask
        assert!(written.rust_mask_flags.iter().all(|&f| f == GMF_ALL_VALID));
    }

    #[tokio::test]
    async fn it_writes_masked_float_pixels_as_nan() {
        let written = write_with_both(
            tiles::<f32>(1, true),
            1,
            None,
            GdalGeoTiffDatasetMetadata {
                no_data_value: None,
                spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857),
            },
        )
        .await;

        assert_same_content(&written);
        assert_eq!(written.rust.epsg.as_deref(), Some("EPSG:3857"));
        assert!(written.rust_mask_flags.iter().all(|&f| f == GMF_NODATA));
        assert!(written.rust.bands[0].1.contains(&None));
    }

    #[tokio::test]
    async fn it_writes_masked_integer_pixels_with_a_no_data_value() {
        let written = write_with_both(
            tiles::<u16>(3, true),
            3,
            None,
            GdalGeoTiffDatasetMetadata {
                no_data_value: Some(9999.),
                spatial_reference: SpatialReference::epsg_4326(),
            },
        )
        .await;

        assert_same_content(&written);
        assert!(written.rust_mask_flags.iter().all(|&f| f == GMF_NODATA));
    }

    #[tokio::test]
    async fn it_falls_back_to_gdal_for_masked_integer_pixels_without_no_data_value() {
        let written = write_with_both(
            tiles::<u8>(1, true),
            1,
            None,
            GdalGeoTiffDatasetMetadata {
                no_data_value: None,
                spatial_reference: SpatialReference::epsg_4326(),
            },
        )
        .await;

        assert_eq!(written.rust, written.gdal);
        assert!(
            written
                .rust_mask_flags
                .iter()
                .all(|&f| f == GMF_PER_DATASET)
        );
    }

    #[tokio::test]
    async fn it_falls_back_to_gdal_for_spatial_references_without_epsg_code() {
        let written = write_with_both(
            tiles::<f32>(1, false),
            1,
            None,
            GdalGeoTiffDatasetMetadata {
                no_data_value: None,
                spatial_reference: SpatialReference::new(SpatialReferenceAuthority::SrOrg, 81),
            },
        )
        .await;

        assert_eq!(written.rust, written.gdal);
        assert!(written.rust.epsg.is_none());
    }

    #[test]
    fn it_escapes_band_names_in_gdal_metadata() {
        let xml = gdal_metadata(
            &["a&b".to_string(), "<c>".to_string()],
            &[TimeInterval::new(0, 10).unwrap()],
        )
        .unwrap();

        assert!(xml.contains(r#"sample="0" role="description">a&amp;amp;b</Item>"#));
        assert!(xml.contains(r#"sample="1" role="description">&amp;lt;c&amp;gt;</Item>"#));
        assert!(xml.contains(r#"<Item name="start" domain="time" sample="1">"#));
    }
}
