//! Compares the GDAL `GeoTIFF` writer with the pure-Rust prototype (`geotiff-writer`) on the
//! output of coverage tiles and small WCS requests. It is not part of the CI benchmarks.
//!
//! The tiles come from a `MockRasterSourceProcessor`, so the benchmark measures collecting the
//! stream, assembling the pixels and encoding the `GeoTIFF`, but no data loading.
//! The pixels are pseudo-random values with some spatial correlation, so that the LZW
//! compression has realistic work to do. Masked cases mark every 7th pixel as invalid; integer
//! rasters then get a no-data value, because the Rust writer falls back to GDAL without one.
//!
//! Run with `cargo bench -p geoengine-operators --bench geotiff_writer`.

#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "okay in benchmarks"
)]

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use gdal::raster::GdalType;
use geoengine_datatypes::{
    primitives::{BandSelection, CacheHint, RasterQueryRectangle, TimeInterval},
    raster::{
        GeoTransform, Grid, GridBoundingBox2D, MaskedGrid2D, Pixel, RasterTile2D,
        TilingSpecification,
    },
    spatial_reference::SpatialReference,
    util::test::TestDefault,
};
use geoengine_operators::{
    engine::{
        BoxRasterQueryProcessor, MockExecutionContext, RasterQueryProcessor,
        RasterResultDescriptor, TimeDescriptor,
    },
    mock::MockRasterSourceProcessor,
    util::{
        raster_stream_to_geotiff::{
            GdalCompressionNumThreads, GdalGeoTiffDatasetMetadata, GdalGeoTiffOptions,
            raster_stream_to_multiband_geotiff_bytes,
        },
        raster_stream_to_geotiff_rust::{
            GeoTiffWriter, raster_stream_to_multiband_geotiff_bytes_rust,
        },
    },
};
use geotiff_writer::NumericSample;
use std::hint::black_box;

const TILE_SIZE: usize = 512;

struct Case {
    name: &'static str,
    /// width and height of the output in pixels
    size: isize,
    bands: u32,
    time_steps: i64,
    masked: bool,
}

const CASES: &[Case] = &[
    Case {
        name: "coverage_tile_256",
        size: 256,
        bands: 1,
        time_steps: 1,
        masked: false,
    },
    Case {
        name: "coverage_tile_256_3_bands",
        size: 256,
        bands: 3,
        time_steps: 1,
        masked: false,
    },
    Case {
        name: "coverage_tile_256_masked",
        size: 256,
        bands: 1,
        time_steps: 1,
        masked: true,
    },
    Case {
        name: "grid_1024_4_time_steps",
        size: 1024,
        bands: 1,
        time_steps: 4,
        masked: false,
    },
];

/// The tiles of all time steps and bands that intersect `bounds`
fn tiles<T: Pixel>(case: &Case, bounds: GridBoundingBox2D) -> Vec<RasterTile2D<T>> {
    let tiling = TilingSpecification::new([TILE_SIZE, TILE_SIZE].into());
    let strategy = geoengine_datatypes::raster::TilingStrategy {
        tile_size_in_pixels: tiling.tile_size_in_pixels,
        geo_transform: GeoTransform::test_default(),
    };

    let mut state: u32 = 42;
    let mut tiles = Vec::new();
    for time_step in 0..case.time_steps {
        let time = TimeInterval::new(time_step * 10, (time_step + 1) * 10).unwrap();
        for tile_position in strategy.tile_idx_iterator_from_grid_bounds(bounds) {
            for band in 0..case.bands {
                let mut value: f64 = 100.;
                let values = (0..TILE_SIZE * TILE_SIZE)
                    .map(|_| {
                        // a random walk, so neighboring pixels are similar as in real data
                        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                        value = (value + f64::from(state >> 28) - 7.5).clamp(0., 250.);
                        T::from_(value)
                    })
                    .collect::<Vec<_>>();
                let validity = (0..TILE_SIZE * TILE_SIZE)
                    .map(|i| !case.masked || i % 7 != 0)
                    .collect::<Vec<_>>();
                tiles.push(RasterTile2D {
                    time,
                    tile_position,
                    band,
                    global_geo_transform: GeoTransform::test_default(),
                    grid_array: MaskedGrid2D::new(
                        Grid::new([TILE_SIZE, TILE_SIZE].into(), values).unwrap(),
                        Grid::new([TILE_SIZE, TILE_SIZE].into(), validity).unwrap(),
                    )
                    .unwrap()
                    .into(),
                    properties: Default::default(),
                    cache_hint: CacheHint::no_cache(),
                });
            }
        }
    }
    tiles
}

fn processor<T: Pixel>(case: &Case, data: Vec<RasterTile2D<T>>) -> BoxRasterQueryProcessor<T> {
    MockRasterSourceProcessor {
        result_descriptor: RasterResultDescriptor::with_datatype_and_num_bands(
            T::TYPE,
            case.bands,
            GridBoundingBox2D::new([-4096, 0], [-1, 4095]).unwrap(),
            GeoTransform::test_default(),
            TimeDescriptor::new_irregular(Some(
                TimeInterval::new(0, case.time_steps * 10).unwrap(),
            )),
        ),
        data,
        tiling_specification: TilingSpecification::new([TILE_SIZE, TILE_SIZE].into()),
    }
    .boxed()
}

#[allow(clippy::too_many_lines)]
fn bench_type<T: Pixel + GdalType + NumericSample>(c: &mut Criterion, type_name: &str) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();
    let tiling = TilingSpecification::new([TILE_SIZE, TILE_SIZE].into());
    let execution_context =
        MockExecutionContext::new_with_tiling_spec_and_tokio_handle(tiling, runtime.handle());

    let mut group = c.benchmark_group(format!("geotiff_writer_{type_name}"));
    for case in CASES {
        // the output cuts through the tiles, as coverage tiles and grid queries usually do
        let offset = (TILE_SIZE / 2) as isize;
        let bounds = GridBoundingBox2D::new(
            [-offset - case.size, offset],
            [-offset - 1, offset + case.size - 1],
        )
        .unwrap();
        let data = tiles::<T>(case, bounds);
        let query_rect = RasterQueryRectangle::new(
            bounds,
            TimeInterval::new(0, case.time_steps * 10).unwrap(),
            BandSelection::first_n(case.bands),
        );
        let gdal_tiff_metadata = GdalGeoTiffDatasetMetadata {
            no_data_value: (case.masked
                && !matches!(
                    T::TYPE,
                    geoengine_datatypes::raster::RasterDataType::F32
                        | geoengine_datatypes::raster::RasterDataType::F64
                ))
            .then_some(0.),
            spatial_reference: SpatialReference::epsg_4326(),
        };
        let gdal_tiff_options = GdalGeoTiffOptions {
            as_cog: false,
            compression_num_threads: GdalCompressionNumThreads::NumThreads(1),
            force_big_tiff: false,
        };

        for writer in [GeoTiffWriter::Gdal, GeoTiffWriter::Rust] {
            // print the sizes once, to compare the compression
            let size = runtime.block_on(write(
                writer,
                processor(case, data.clone()),
                query_rect.clone(),
                &execution_context,
                gdal_tiff_metadata,
                gdal_tiff_options,
                tiling,
            ));
            println!("{type_name} {} {writer:?}: {size} bytes", case.name);

            group.bench_with_input(
                BenchmarkId::new(format!("{writer:?}"), case.name),
                &case,
                |b, case| {
                    b.to_async(&runtime).iter(|| {
                        write(
                            writer,
                            processor(case, data.clone()),
                            query_rect.clone(),
                            &execution_context,
                            gdal_tiff_metadata,
                            gdal_tiff_options,
                            tiling,
                        )
                    });
                },
            );
        }
    }
    group.finish();
}

/// Writes the `GeoTIFF` and returns its size in bytes
async fn write<T: Pixel + GdalType + NumericSample>(
    writer: GeoTiffWriter,
    processor: BoxRasterQueryProcessor<T>,
    query_rect: RasterQueryRectangle,
    execution_context: &MockExecutionContext,
    gdal_tiff_metadata: GdalGeoTiffDatasetMetadata,
    gdal_tiff_options: GdalGeoTiffOptions,
    tiling: TilingSpecification,
) -> usize {
    let query_ctx = execution_context.mock_query_context_test_default();
    let not_canceled = Box::pin(futures::future::pending());
    let (bytes, _) = match writer {
        GeoTiffWriter::Gdal => {
            raster_stream_to_multiband_geotiff_bytes(
                processor,
                query_rect,
                None,
                query_ctx,
                gdal_tiff_metadata,
                gdal_tiff_options,
                None,
                not_canceled,
                tiling,
            )
            .await
        }
        GeoTiffWriter::Rust => {
            raster_stream_to_multiband_geotiff_bytes_rust(
                processor,
                query_rect,
                None,
                query_ctx,
                gdal_tiff_metadata,
                gdal_tiff_options,
                None,
                not_canceled,
                tiling,
            )
            .await
        }
    }
    .unwrap();
    black_box(bytes.len())
}

fn geotiff_writer(c: &mut Criterion) {
    bench_type::<u16>(c, "u16");
    bench_type::<f32>(c, "f32");
}

criterion_group!(benches, geotiff_writer);
criterion_main!(benches);
