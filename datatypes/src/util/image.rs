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

    assert_eq!(left, right, "Images differ: {expected:?}");
}

/// Compare two images
///
/// # Panics
/// - if the `expected` image cannot be loaded
/// - if the `found` bytes cannot be loaded as an image
/// - if the images differ
///
pub fn assert_image_equals_with_format(expected: &Path, found: &[u8], format: ImageFormat) {
    let left_buf = std::fs::read(expected).expect("Failed to read `expected` path");
    let left = image::load_from_memory_with_format(&left_buf, format.into())
        .expect("Failed to make image from `expected` path");
    let right = image::load_from_memory_with_format(found, format.into())
        .expect("Failed to make image from `found` bytes");

    assert_eq!(left, right, "Images differ: {expected:?}");
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
