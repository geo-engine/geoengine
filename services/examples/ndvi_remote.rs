#![allow(clippy::print_stdout)]

use geoengine_datatypes::operations::image::{Colorizer, RgbaColor, ToPng};
use geoengine_datatypes::primitives::{CacheHint, Coordinate2D, TimeInterval};
use geoengine_datatypes::raster::{
    GeoTransform, Grid2D, GridIdx2D, GridOrEmpty2D, GridShape2D, MaskedGrid2D, RasterTile2D,
};
use geoengine_datatypes::util::gdal::gdal_open_dataset;
use std::time::{Duration, Instant};

const B04_URL: &str = "/vsicurl/https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/32/U/MB/2020/8/S2B_32UMB_20200807_1_L2A/B04.tif";
const B08_URL: &str = "/vsicurl/https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/32/U/MB/2020/8/S2B_32UMB_20200807_1_L2A/B08.tif";
const SCL_URL: &str = "/vsicurl/https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/32/U/MB/2020/8/S2B_32UMB_20200807_1_L2A/SCL.tif";

struct BandData {
    b04: Vec<u16>,
    b08: Vec<u16>,
    scl: Vec<u8>,
}

struct NdviResult {
    data: Vec<f32>,
    valid_mask: Vec<bool>,
}

#[derive(Debug, Clone, Copy)]
enum ReadStrategy {
    /// Read the entire raster in one GDAL call
    FullRaster,
    /// Read in tiles, opening dataset once and reusing it
    TiledReusedDataset { tile_size: usize },
    /// Read in tiles, opening dataset for each tile
    TiledReopenDataset { tile_size: usize },
}

/// Workflow of reading remote Sentinel-2 data using vsicurl, calculating NDVI, and exporting as PNG.
/// Compare direct reads to tiled reads. This is a baseline to compare a Geo Engine workflow against.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    const N_RUNS: usize = 5;

    // minx: 419_960.0,
    // miny: 5_610_200.0,
    // maxx: 489_760.0,
    // maxy: 5_680_000.0,
    let origin = Coordinate2D::new(419_960.0, 5_680_000.0);

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

    // Test all three strategies
    let strategies = vec![
        ReadStrategy::FullRaster,
        ReadStrategy::TiledReusedDataset { tile_size: 512 },
        ReadStrategy::TiledReopenDataset { tile_size: 512 },
    ];

    // Store timings for each strategy
    let mut timing_results: Vec<(ReadStrategy, Vec<(Duration, Duration)>)> = Vec::new();
    let mut first_ndvi: Option<NdviResult> = None;
    let mut reference_ndvi: Option<&NdviResult> = None;

    for strategy in &strategies {
        println!("Running {strategy:?} {N_RUNS} times...");
        let mut timings = Vec::new();

        for run in 1..=N_RUNS {
            print!("  Run {run}/{N_RUNS}... ");
            std::io::Write::flush(&mut std::io::stdout())?;

            // Process data
            let (bands, read_duration) = read_bands(width, height, x_offset, y_offset, *strategy)?;
            let (ndvi, ndvi_duration) = compute_ndvi(&bands, width, height);
            let _tile =
                create_raster_tile(&ndvi, origin, width, height, pixel_size_x, pixel_size_y)?;

            timings.push((read_duration, ndvi_duration));

            // Store first result for verification and PNG export
            if first_ndvi.is_none() {
                first_ndvi = Some(ndvi);
                reference_ndvi = first_ndvi.as_ref();
            } else if run == 1 {
                // Verify this strategy matches the first strategy's result
                if let Some(reference) = reference_ndvi
                    && (ndvi.data != reference.data || ndvi.valid_mask != reference.valid_mask)
                {
                    println!("\n✗ {strategy:?} differs from reference!");
                }
            }

            let total = read_duration.as_secs_f64() + ndvi_duration.as_secs_f64();
            println!("{total:.2}s");
        }

        timing_results.push((*strategy, timings));
    }

    // Export PNG from first result
    let png_duration = export_png(
        first_ndvi.as_ref().expect("at least one run completed"),
        width,
        height,
        "ndvi_output.png",
    )?;

    // Print comparison table with mean values
    println!("\n=== Performance Comparison (mean of {N_RUNS} runs) ===");
    println!(
        "{:<35} {:>12} {:>12} {:>12}",
        "Strategy", "Read (s)", "NDVI (s)", "Total (s)"
    );
    println!("{:-<72}", "");

    for (strategy, timings) in &timing_results {
        let mean_read = timings.iter().map(|(r, _)| r.as_secs_f64()).sum::<f64>() / N_RUNS as f64;
        let mean_ndvi = timings.iter().map(|(_, n)| n.as_secs_f64()).sum::<f64>() / N_RUNS as f64;
        let mean_total = mean_read + mean_ndvi;

        println!(
            "{:<35} {:>12.2} {:>12.2} {:>12.2}",
            format!("{strategy:?}"),
            mean_read,
            mean_ndvi,
            mean_total
        );
    }
    println!("\nPNG export: {:.2}s", png_duration.as_secs_f64());

    Ok(())
}

fn read_bands(
    width: usize,
    height: usize,
    x_offset: isize,
    y_offset: isize,
    strategy: ReadStrategy,
) -> Result<(BandData, Duration), Box<dyn std::error::Error>> {
    let start = Instant::now();

    let data = match strategy {
        ReadStrategy::FullRaster => read_bands_full_raster(width, height, x_offset, y_offset)?.0,
        ReadStrategy::TiledReusedDataset { tile_size }
        | ReadStrategy::TiledReopenDataset { tile_size } => {
            let reopen_per_tile = matches!(strategy, ReadStrategy::TiledReopenDataset { .. });
            let scl_dataset = gdal_open_dataset(std::path::Path::new(SCL_URL))?;
            let b08_dataset = gdal_open_dataset(std::path::Path::new(B08_URL))?;
            let b04_dataset = gdal_open_dataset(std::path::Path::new(B04_URL))?;
            read_bands_tiled_internal(
                &b04_dataset,
                &b08_dataset,
                &scl_dataset,
                width,
                height,
                x_offset,
                y_offset,
                tile_size,
                reopen_per_tile,
            )?
            .0
        }
    };

    Ok((data, start.elapsed()))
}

fn read_bands_full_raster(
    width: usize,
    height: usize,
    x_offset: isize,
    y_offset: isize,
) -> Result<(BandData, Duration), Box<dyn std::error::Error>> {
    let start = Instant::now();

    let scl_dataset = gdal_open_dataset(std::path::Path::new(SCL_URL))?;
    let b08_dataset = gdal_open_dataset(std::path::Path::new(B08_URL))?;
    let b04_dataset = gdal_open_dataset(std::path::Path::new(B04_URL))?;

    // Read B04 (10m resolution)
    let b04_band = b04_dataset.rasterband(1)?;
    let b04_buffer =
        b04_band.read_as::<u16>((x_offset, y_offset), (width, height), (width, height), None)?;
    let (_, b04_data) = b04_buffer.into_shape_and_vec();

    // Read B08 (10m resolution)
    let b08_band = b08_dataset.rasterband(1)?;
    let b08_buffer =
        b08_band.read_as::<u16>((x_offset, y_offset), (width, height), (width, height), None)?;
    let (_, b08_data) = b08_buffer.into_shape_and_vec();

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

    let duration = start.elapsed();

    Ok((
        BandData {
            b04: b04_data,
            b08: b08_data,
            scl: scl_data,
        },
        duration,
    ))
}

#[allow(clippy::too_many_arguments)]
fn read_bands_tiled_internal(
    b04_dataset: &gdal::Dataset,
    b08_dataset: &gdal::Dataset,
    scl_dataset: &gdal::Dataset,
    width: usize,
    height: usize,
    x_offset: isize,
    y_offset: isize,
    tile_size: usize,
    reopen_per_tile: bool,
) -> Result<(BandData, usize), Box<dyn std::error::Error>> {
    // Allocate output buffers
    let mut b04_data = vec![0u16; width * height];
    let mut b08_data = vec![0u16; width * height];
    let mut scl_data = vec![0u8; width * height];

    let mut tile_count = 0;

    // Read in tiles
    for tile_y in (0..height).step_by(tile_size) {
        for tile_x in (0..width).step_by(tile_size) {
            let tile_width = tile_size.min(width - tile_x);
            let tile_height = tile_size.min(height - tile_y);

            // Get datasets for this tile (reopen if needed)
            let (b04_ds, b08_ds, scl_ds);
            let (b04_band, b08_band, scl_band) = if reopen_per_tile {
                b04_ds = gdal_open_dataset(std::path::Path::new(B04_URL))?;
                b08_ds = gdal_open_dataset(std::path::Path::new(B08_URL))?;
                scl_ds = gdal_open_dataset(std::path::Path::new(SCL_URL))?;
                (
                    b04_ds.rasterband(1)?,
                    b08_ds.rasterband(1)?,
                    scl_ds.rasterband(1)?,
                )
            } else {
                (
                    b04_dataset.rasterband(1)?,
                    b08_dataset.rasterband(1)?,
                    scl_dataset.rasterband(1)?,
                )
            };

            // Read B04 tile
            let tile_b04_buffer = b04_band.read_as::<u16>(
                (x_offset + tile_x as isize, y_offset + tile_y as isize),
                (tile_width, tile_height),
                (tile_width, tile_height),
                None,
            )?;
            let (_, tile_b04_data) = tile_b04_buffer.into_shape_and_vec();

            // Read B08 tile
            let tile_b08_buffer = b08_band.read_as::<u16>(
                (x_offset + tile_x as isize, y_offset + tile_y as isize),
                (tile_width, tile_height),
                (tile_width, tile_height),
                None,
            )?;
            let (_, tile_b08_data) = tile_b08_buffer.into_shape_and_vec();

            // Read SCL tile (20m resolution)
            let scl_tile_x = tile_x / 2;
            let scl_tile_y = tile_y / 2;
            let scl_tile_width = tile_width / 2;
            let scl_tile_height = tile_height / 2;
            let scl_x_offset = x_offset / 2;
            let scl_y_offset = y_offset / 2;

            let tile_scl_buffer = scl_band.read_as::<u8>(
                (
                    scl_x_offset + scl_tile_x as isize,
                    scl_y_offset + scl_tile_y as isize,
                ),
                (scl_tile_width, scl_tile_height),
                (tile_width, tile_height), // upsample to 10m
                None,
            )?;
            let (_, tile_scl_data) = tile_scl_buffer.into_shape_and_vec();

            // Copy tile data into output buffers
            for ty in 0..tile_height {
                let src_start = ty * tile_width;
                let dst_start = (tile_y + ty) * width + tile_x;
                b04_data[dst_start..dst_start + tile_width]
                    .copy_from_slice(&tile_b04_data[src_start..src_start + tile_width]);
                b08_data[dst_start..dst_start + tile_width]
                    .copy_from_slice(&tile_b08_data[src_start..src_start + tile_width]);
                scl_data[dst_start..dst_start + tile_width]
                    .copy_from_slice(&tile_scl_data[src_start..src_start + tile_width]);
            }

            tile_count += 1;
        }
    }

    Ok((
        BandData {
            b04: b04_data,
            b08: b08_data,
            scl: scl_data,
        },
        tile_count,
    ))
}

fn compute_ndvi(bands: &BandData, width: usize, height: usize) -> (NdviResult, Duration) {
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

    let duration = start.elapsed();

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
    origin: Coordinate2D,
    width: usize,
    height: usize,
    pixel_size_x: f64,
    pixel_size_y: f64,
) -> Result<RasterTile2D<f32>, Box<dyn std::error::Error>> {
    let grid_shape = GridShape2D::new([height, width]);
    let validity_grid = Grid2D::new(grid_shape, ndvi.valid_mask.clone())?;
    let masked_grid =
        MaskedGrid2D::new(Grid2D::new(grid_shape, ndvi.data.clone())?, validity_grid)?;
    let grid_or_empty: GridOrEmpty2D<f32> = masked_grid.into();

    let geo_transform = GeoTransform::new(origin, pixel_size_x, pixel_size_y);

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

    Ok(start.elapsed())
}
