use std::path::Path;

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

    assert_eq!(left, right, "Images differ: {}", expected.display());
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

    assert_eq!(left, right, "Images differ: {}", expected.display());
}

/// Compare two tiff images using GDAL compare
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

    // TODO: use GDALRasterCompareAlgorithm in `gdal 3.11`
    let output = std::process::Command::new("gdalcompare.py")
        .arg("-skip_binary") // skip comparison of exact binary content
        .arg(expected)
        .arg(tmp_file.path())
        .output()
        .expect("Failed to execute gdalcompare.py");

    assert!(
        output.status.success(),
        "Images differ: {}\n{}",
        expected.display(),
        String::from_utf8_lossy(&output.stdout) // somehow, the error messages are in stdout
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
    use std::io::Cursor;

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
