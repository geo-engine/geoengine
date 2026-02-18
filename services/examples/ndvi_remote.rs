#![allow(clippy::print_stdout)]

use geoengine_datatypes::operations::image::{Colorizer, RgbaColor, ToPng};
use geoengine_datatypes::primitives::{CacheHint, TimeInterval};
use geoengine_datatypes::raster::{
    GeoTransform, Grid2D, GridIdx2D, GridOrEmpty2D, GridShape2D, MaskedGrid2D, RasterTile2D,
};
use geoengine_datatypes::util::gdal::gdal_open_dataset;
use std::time::{Duration, Instant};

struct BoundingBox {
    minx: f64,
    miny: f64,
    maxx: f64,
    maxy: f64,
}

struct BandData {
    b04: Vec<u16>,
    b08: Vec<u16>,
    scl: Vec<u8>,
}

struct NdviResult {
    data: Vec<f32>,
    valid_mask: Vec<bool>,
}

/// Workflow of reading remote Sentinel-2 data using vsicurl, calculating NDVI, and exporting as PNG.
/// Compare direct reads to tiled reads. This is a baseline to compare a Geo Engine workflow against.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Loading Sentinel-2 data from STAC...");

    let bbox = BoundingBox {
        minx: 419_960.0,
        miny: 5_610_200.0,
        maxx: 489_760.0,
        maxy: 5_680_000.0,
    };

    let pixel_size = 10.0; // Sentinel-2 10m resolution

    let sentinel_tile_pixel_size = 10980;
    // clip tile, to make it comparable to Geo Engine workflow where we omit the borders as to avoid also downloading overlapping neighboring tiles
    let pixel_border = 2000;

    let x_offset = pixel_border as isize;
    let y_offset = pixel_border as isize;

    let width = sentinel_tile_pixel_size - 2 * pixel_border;
    let height = sentinel_tile_pixel_size - 2 * pixel_border;
    let pixel_size_x = pixel_size;
    let pixel_size_y = -pixel_size; // negative because y decreases as we go down

    println!(
        "Bounding box: ({}, {}) to ({}, {})",
        bbox.minx, bbox.miny, bbox.maxx, bbox.maxy
    );
    println!("Output dimensions: {width}x{height}");

    // Process data
    let (bands, read_duration) = read_bands(&bbox, width, height, x_offset, y_offset)?;
    let (ndvi, ndvi_duration) = compute_ndvi(&bands, width, height);
    let _tile = create_raster_tile(&ndvi, &bbox, width, height, pixel_size_x, pixel_size_y)?;
    let png_duration = export_png(&ndvi, width, height, "ndvi_output.png")?;

    // Print timing summary
    println!("\n=== Timing Summary ===");
    println!("Reading data:      {:.2}s", read_duration.as_secs_f64());
    println!("Calculating NDVI:  {:.2}s", ndvi_duration.as_secs_f64());
    println!("Writing PNG:       {:.2}s", png_duration.as_secs_f64());
    println!(
        "Total:             {:.2}s",
        (read_duration + ndvi_duration + png_duration).as_secs_f64()
    );

    Ok(())
}

fn read_bands(
    _bbox: &BoundingBox,
    width: usize,
    height: usize,
    x_offset: isize,
    y_offset: isize,
) -> Result<(BandData, Duration), Box<dyn std::error::Error>> {
    println!("\nReading band data...");
    let start = Instant::now();

    let scl_url = "/vsicurl/https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/32/U/MB/2020/8/S2B_32UMB_20200807_1_L2A/SCL.tif";
    let b08_url = "/vsicurl/https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/32/U/MB/2020/8/S2B_32UMB_20200807_1_L2A/B08.tif";
    let b04_url = "/vsicurl/https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/32/U/MB/2020/8/S2B_32UMB_20200807_1_L2A/B04.tif";

    let scl_dataset = gdal_open_dataset(std::path::Path::new(scl_url))?;
    let b08_dataset = gdal_open_dataset(std::path::Path::new(b08_url))?;
    let b04_dataset = gdal_open_dataset(std::path::Path::new(b04_url))?;

    // Read B04 (10m resolution)
    let b04_band = b04_dataset.rasterband(1)?;
    let b04_buffer =
        b04_band.read_as::<u16>((x_offset, y_offset), (width, height), (width, height), None)?;
    let (_, b04_data) = b04_buffer.into_shape_and_vec();
    println!("B04 band read: {} pixels", b04_data.len());

    // Read B08 (10m resolution)
    let b08_band = b08_dataset.rasterband(1)?;
    let b08_buffer =
        b08_band.read_as::<u16>((x_offset, y_offset), (width, height), (width, height), None)?;
    let (_, b08_data) = b08_buffer.into_shape_and_vec();
    println!("B08 band read: {} pixels", b08_data.len());

    // Read SCL (20m resolution) - need to adjust offset and size
    let scl_width = width / 2;
    let scl_height = height / 2;
    let scl_x_offset = x_offset / 2;
    let scl_y_offset = y_offset / 2;

    let scl_band = scl_dataset.rasterband(1)?;
    let scl_buffer = scl_band.read_as::<u8>(
        (scl_x_offset, scl_y_offset),
        (scl_width, scl_height),
        (width, height),
        None,
    )?;
    let (_, scl_data) = scl_buffer.into_shape_and_vec();
    println!("SCL band read and resampled: {} pixels", scl_data.len());

    let duration = start.elapsed();
    println!("Time reading data: {:.2}s", duration.as_secs_f64());

    Ok((
        BandData {
            b04: b04_data,
            b08: b08_data,
            scl: scl_data,
        },
        duration,
    ))
}

fn compute_ndvi(bands: &BandData, width: usize, height: usize) -> (NdviResult, Duration) {
    println!("\nComputing NDVI...");
    let start = Instant::now();

    let mut ndvi_data = Vec::with_capacity(width * height);
    let mut valid_mask = Vec::with_capacity(width * height);

    for i in 0..width * height {
        let scl = bands.scl[i];
        let b04 = f32::from(bands.b04[i]);
        let b08 = f32::from(bands.b08[i]);

        // Check SCL mask: if (A == 3 || (A >= 7 && A <= 11)) { NODATA }
        if scl == 3 || (7..=11).contains(&scl) {
            ndvi_data.push(0.0);
            valid_mask.push(false);
        } else {
            // Compute NDVI: (B08 - B04) / (B08 + B04)
            let sum = b08 + b04;
            if sum == 0.0 {
                ndvi_data.push(0.0);
                valid_mask.push(false);
            } else {
                let ndvi = (b08 - b04) / sum;
                ndvi_data.push(ndvi);
                valid_mask.push(true);
            }
        }
    }

    let valid_count = valid_mask.iter().filter(|&&v| v).count();
    println!(
        "NDVI computed: {} valid pixels out of {}",
        valid_count,
        width * height
    );

    let duration = start.elapsed();
    println!("Time calculating NDVI: {:.2}s", duration.as_secs_f64());

    (
        NdviResult {
            data: ndvi_data,
            valid_mask,
        },
        duration,
    )
}

fn create_raster_tile(
    ndvi: &NdviResult,
    bbox: &BoundingBox,
    width: usize,
    height: usize,
    pixel_size_x: f64,
    pixel_size_y: f64,
) -> Result<RasterTile2D<f32>, Box<dyn std::error::Error>> {
    println!("\nCreating RasterTile2D...");

    let grid_shape = GridShape2D::new([height, width]);
    let validity_grid = Grid2D::new(grid_shape, ndvi.valid_mask.clone())?;
    let masked_grid =
        MaskedGrid2D::new(Grid2D::new(grid_shape, ndvi.data.clone())?, validity_grid)?;
    let grid_or_empty: GridOrEmpty2D<f32> = masked_grid.into();

    let geo_transform =
        GeoTransform::new((bbox.minx, bbox.maxy).into(), pixel_size_x, pixel_size_y);

    let datetime: geoengine_datatypes::primitives::DateTime =
        chrono::NaiveDate::from_ymd_opt(2020, 8, 7)
            .expect("valid date")
            .and_hms_opt(0, 0, 0)
            .expect("valid time")
            .and_utc()
            .into();

    let time = TimeInterval::new_instant(geoengine_datatypes::primitives::TimeInstance::from(
        datetime,
    ))?;

    Ok(RasterTile2D::new(
        time,
        GridIdx2D::new([0, 0]),
        0,
        geo_transform,
        grid_or_empty,
        CacheHint::default(),
    ))
}

fn export_png(
    ndvi: &NdviResult,
    width: usize,
    height: usize,
    output_path: &str,
) -> Result<Duration, Box<dyn std::error::Error>> {
    println!("\nExporting as PNG...");
    let start = Instant::now();

    // Create a MaskedGrid2D with the NDVI data
    let grid_shape = GridShape2D::new([height, width]);

    // Create colorizer: black (0,0,0, 255) to green (0,255,0, 255) for values 0 to 1
    let colorizer = Colorizer::linear_gradient(
        vec![
            (0.0, RgbaColor::new(0, 0, 0, 255)).try_into()?,
            (1.0, RgbaColor::new(0, 255, 0, 255)).try_into()?,
        ],
        RgbaColor::black(),             // no data color
        RgbaColor::new(0, 255, 0, 255), // over color
        RgbaColor::black(),             // under color
    )?;

    // Create grid and masked grid
    let data_grid = Grid2D::new(grid_shape, ndvi.data.clone())?;
    let validity_grid = Grid2D::new(grid_shape, ndvi.valid_mask.clone())?;
    let masked_grid = MaskedGrid2D::new(data_grid, validity_grid)?;

    // Use the ToPng trait to create PNG bytes
    let png_bytes = masked_grid.to_png(width as u32, height as u32, &colorizer)?;

    // Write to file
    std::fs::write(output_path, png_bytes)?;

    let duration = start.elapsed();
    println!("Time writing PNG: {:.2}s", duration.as_secs_f64());
    println!("PNG saved to: {output_path}");

    Ok(duration)
}
