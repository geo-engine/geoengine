use std::path::Path;

use image::{DynamicImage, GenericImageView};

/// Compare two images
///
/// # Panics
/// - if the `expected` image cannot be loaded
/// - if the `found` bytes cannot be loaded as an image
/// - if the images differ
///
pub fn assert_image_equals(expected: &Path, found: &[u8]) {
    let left_buf = std::fs::read(expected).expect("Failed to read `expected` path");
    let left =
        image::load_from_memory(&left_buf).expect("Failed to make image from `expected` path");
    let right = image::load_from_memory(found).expect("Failed to make image from `found` bytes");

    assert_image_equals_helper(&left, &right, expected);
}

/// Compare two images
///
/// # Panics
/// - if the `expected` image cannot be loaded
/// - if the `found` bytes cannot be loaded as an image
/// - if the images differ
///
pub fn assert_image_equals_with_format(expected: &Path, found: &[u8], format: ImageFormat) {
    // `image` and `tiff` do not currently support planar tiffs, so we need to use `gdal` for that
    if format == ImageFormat::Tiff {
        return assert_image_equals_tiff(expected, found);
    }

    let left_buf = std::fs::read(expected).expect("Failed to read `expected` path");
    let left = image::load_from_memory_with_format(&left_buf, format.into())
        .expect("Failed to make image from `expected` path");
    let right = image::load_from_memory_with_format(found, format.into())
        .expect("Failed to make image from `found` bytes");

    assert_image_equals_helper(&left, &right, expected);
}

fn assert_image_equals_helper(left: &DynamicImage, right: &DynamicImage, expected: &Path) {
    assert!(
        left.dimensions() == right.dimensions(),
        "Images differ in dimensions: {} ({}x{}) vs ({}x{})",
        expected.display(),
        left.width(),
        left.height(),
        right.width(),
        right.height()
    );
    assert!(left == right, "Images differ: {}", expected.display());
}

/// Compare two tiff images using `gdal raster compare` (GDAL >= 3.12).
///
/// Binary content and metadata are ignored because they depend on the GDAL
/// version (e.g. overview `RESAMPLING` metadata). Pixels, overviews, spatial
/// reference and geotransform are still compared.
///
/// # Panics
/// - if the `found` bytes cannot be stored as a temporary file
/// - if the images differ
///
pub fn assert_image_equals_tiff(expected: &Path, found: &[u8]) {
    use std::io::Write;

    let mut tmp_file = tempfile::NamedTempFile::new().expect("Failed to create temporary file");
    tmp_file
        .write_all(found)
        .expect("Failed to write `found` bytes to temporary file");

    let output = std::process::Command::new("gdal")
        .args(["raster", "compare", "--skip-binary", "--skip-metadata"])
        .arg(expected)
        .arg(tmp_file.path())
        .output()
        .expect("Failed to execute gdal raster compare (requires GDAL >= 3.12)");

    assert!(
        output.status.success(),
        "Images differ: {}\n{}\n{}",
        expected.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Image format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]

pub enum ImageFormat {
    Png,
    Tiff,
}

impl From<ImageFormat> for image::ImageFormat {
    fn from(format: ImageFormat) -> Self {
        match format {
            ImageFormat::Png => image::ImageFormat::Png,
            ImageFormat::Tiff => image::ImageFormat::Tiff,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_data;
    use gdal::{Dataset, DatasetOptions, DriverManager, GdalOpenFlags, Metadata};
    use std::io::Cursor;

    fn tiff_with_overview_metadata(key: &str, value: &str) -> tempfile::NamedTempFile {
        let tmp_file = tempfile::NamedTempFile::new().unwrap();
        let expected = Dataset::open(test_data!(
            "raster/cloud_optimized_geotiff_from_stream_compressed.tiff"
        ))
        .unwrap();
        let found = expected
            .create_copy(
                &DriverManager::get_driver_by_name("GTiff").unwrap(),
                tmp_file.path(),
                &gdal::raster::RasterCreationOptions::from_iter(["COPY_SRC_OVERVIEWS=YES"]),
            )
            .unwrap();
        found
            .rasterband(1)
            .unwrap()
            .overview(0)
            .unwrap()
            .set_metadata_item(key, value, "")
            .unwrap();
        drop(found);
        tmp_file
    }

    #[test]
    fn it_allows_missing_tiff_overview_resampling_metadata() {
        let expected =
            test_data!("raster/cloud_optimized_geotiff_from_stream_compressed.tiff").to_path_buf();
        let found = tiff_with_overview_metadata("RESAMPLING", "NEAREST");
        assert_image_equals_tiff(&expected, &std::fs::read(found.path()).unwrap());
        assert_image_equals_tiff(found.path(), &std::fs::read(expected).unwrap());
    }

    #[test]
    fn it_detects_different_tiff_overview_pixels() {
        let found = tiff_with_overview_metadata("RESAMPLING", "NEAREST");
        let dataset = Dataset::open_ex(
            found.path(),
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_UPDATE,
                open_options: Some(&["IGNORE_COG_LAYOUT_BREAK=YES"]),
                ..Default::default()
            },
        )
        .unwrap();
        dataset
            .rasterband(1)
            .unwrap()
            .overview(0)
            .unwrap()
            .fill(255., None)
            .unwrap();
        drop(dataset);
        assert!(
            std::panic::catch_unwind(|| {
                assert_image_equals_tiff(
                    test_data!("raster/cloud_optimized_geotiff_from_stream_compressed.tiff"),
                    &std::fs::read(found.path()).unwrap(),
                );
            })
            .is_err()
        );
    }

    #[test]
    fn it_asserts_images() {
        const PIXELS_PER_AXIS: u32 = 8;

        let tmp_file = tempfile::NamedTempFile::new().unwrap();

        let mut img = image::RgbaImage::new(PIXELS_PER_AXIS, PIXELS_PER_AXIS);
        image::save_buffer_with_format(
            tmp_file.path(),
            &img,
            PIXELS_PER_AXIS,
            PIXELS_PER_AXIS,
            image::ExtendedColorType::Rgba8,
            image::ImageFormat::Png,
        )
        .unwrap();

        let mut bytes: Vec<u8> = Vec::new();
        img.write_to(&mut Cursor::new(&mut bytes), image::ImageFormat::Png)
            .unwrap();

        assert_image_equals(tmp_file.path(), &bytes);

        img.put_pixel(0, 0, image::Rgba([0, 1, 2, 3]));

        let mut bytes: Vec<u8> = Vec::new();
        img.write_to(&mut Cursor::new(&mut bytes), image::ImageFormat::Png)
            .unwrap();

        std::panic::catch_unwind(|| {
            assert_image_equals(tmp_file.path(), &bytes);
        })
        .unwrap_err();
    }
}
