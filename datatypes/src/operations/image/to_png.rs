use std::io::Cursor;

use crate::raster::{
    Grid2D, GridIndexAccess, GridOrEmpty2D, MaskedGrid2D, Pixel, RasterTile2D, TypedRasterTile2D,
};
use crate::util::Result;
use crate::{error, raster::EmptyGrid2D};
use crate::{
    operations::image::{Colorizer, RgbaTransmutable},
    raster::GridOrEmpty,
};
use image::{DynamicImage, ImageBuffer, ImageFormat, RgbaImage};

pub trait ToPng {
    /// Outputs png bytes of an image of size width x height
    fn to_png(&self, width: u32, height: u32, colorizer: &Colorizer) -> Result<Vec<u8>>;
}

fn image_buffer_to_png_bytes(
    image_buffer: ImageBuffer<image::Rgba<u8>, Vec<u8>>,
) -> Result<Vec<u8>> {
    let mut buffer = Cursor::new(Vec::new());
    DynamicImage::ImageRgba8(image_buffer)
        .write_to(&mut buffer, ImageFormat::Png)
        .map_err(|error| error::Error::Colorizer {
            details: format!("encoding PNG failed: {error}"),
        })?;
    Ok(buffer.into_inner())
}

impl<P> ToPng for Grid2D<P>
where
    P: Pixel + RgbaTransmutable,
{
    fn to_png(&self, width: u32, height: u32, colorizer: &Colorizer) -> Result<Vec<u8>> {
        // TODO: use PNG color palette once it is available

        let [.., raster_y_size, raster_x_size] = self.shape.shape_array;
        let scale_x = (raster_x_size as f64) / f64::from(width);
        let scale_y = (raster_y_size as f64) / f64::from(height);

        let image_buffer =
            create_rgba_image_from_grid(self, width, height, colorizer, scale_x, scale_y);

        image_buffer_to_png_bytes(image_buffer)
    }
}

impl<P> ToPng for MaskedGrid2D<P>
where
    P: Pixel + RgbaTransmutable,
{
    fn to_png(&self, width: u32, height: u32, colorizer: &Colorizer) -> Result<Vec<u8>> {
        // TODO: use PNG color palette once it is available

        let [.., raster_y_size, raster_x_size] = self.shape().shape_array;
        let scale_x = (raster_x_size as f64) / f64::from(width);
        let scale_y = (raster_y_size as f64) / f64::from(height);

        let image_buffer =
            create_rgba_image_from_masked_grid(self, width, height, colorizer, scale_x, scale_y);

        image_buffer_to_png_bytes(image_buffer)
    }
}

impl<P> ToPng for EmptyGrid2D<P>
where
    P: Pixel + RgbaTransmutable,
{
    fn to_png(&self, width: u32, height: u32, colorizer: &Colorizer) -> Result<Vec<u8>> {
        // TODO: use PNG color palette once it is available

        let no_data_color: image::Rgba<u8> = colorizer.no_data_color().into();

        let image_buffer = ImageBuffer::from_pixel(width, height, no_data_color);

        image_buffer_to_png_bytes(image_buffer)
    }
}

impl<P> ToPng for GridOrEmpty2D<P>
where
    P: Pixel + RgbaTransmutable,
{
    fn to_png(&self, width: u32, height: u32, colorizer: &Colorizer) -> Result<Vec<u8>> {
        match self {
            GridOrEmpty::Grid(g) => g.to_png(width, height, colorizer),
            GridOrEmpty::Empty(n) => n.to_png(width, height, colorizer),
        }
    }
}

fn create_rgba_image_from_grid<P: Pixel + RgbaTransmutable>(
    raster_grid: &Grid2D<P>,
    width: u32,
    height: u32,
    colorizer: &Colorizer,
    scale_x: f64,
    scale_y: f64,
) -> RgbaImage {
    let color_mapper = colorizer.create_color_mapper();

    RgbaImage::from_fn(width, height, |x, y| {
        let (grid_pixel_x, grid_pixel_y) = image_pixel_to_raster_pixel(x, y, scale_x, scale_y);
        if let Ok(pixel_value) = raster_grid.get_at_grid_index([grid_pixel_y, grid_pixel_x]) {
            color_mapper.call(pixel_value)
        } else {
            colorizer.no_data_color()
        }
        .into()
    })
}

fn create_rgba_image_from_masked_grid<P: Pixel + RgbaTransmutable>(
    raster_grid: &MaskedGrid2D<P>,
    width: u32,
    height: u32,
    colorizer: &Colorizer,
    scale_x: f64,
    scale_y: f64,
) -> RgbaImage {
    let color_mapper = colorizer.create_color_mapper();

    RgbaImage::from_fn(width, height, |x, y| {
        let (grid_pixel_x, grid_pixel_y) = image_pixel_to_raster_pixel(x, y, scale_x, scale_y);
        if let Ok(Some(pixel_value)) = raster_grid.get_at_grid_index([grid_pixel_y, grid_pixel_x]) {
            color_mapper.call(pixel_value)
        } else {
            colorizer.no_data_color()
        }
        .into()
    })
}

impl<T: Pixel> ToPng for RasterTile2D<T> {
    fn to_png(&self, width: u32, height: u32, colorizer: &Colorizer) -> Result<Vec<u8>> {
        self.grid_array.to_png(width, height, colorizer)
    }
}

impl ToPng for TypedRasterTile2D {
    fn to_png(&self, width: u32, height: u32, colorizer: &Colorizer) -> Result<Vec<u8>> {
        match self {
            TypedRasterTile2D::U8(r) => r.to_png(width, height, colorizer),
            TypedRasterTile2D::U16(r) => r.to_png(width, height, colorizer),
            TypedRasterTile2D::U32(r) => r.to_png(width, height, colorizer),
            TypedRasterTile2D::U64(r) => r.to_png(width, height, colorizer),
            TypedRasterTile2D::I8(r) => r.to_png(width, height, colorizer),
            TypedRasterTile2D::I16(r) => r.to_png(width, height, colorizer),
            TypedRasterTile2D::I32(r) => r.to_png(width, height, colorizer),
            TypedRasterTile2D::I64(r) => r.to_png(width, height, colorizer),
            TypedRasterTile2D::F32(r) => r.to_png(width, height, colorizer),
            TypedRasterTile2D::F64(r) => r.to_png(width, height, colorizer),
        }
    }
}

// TODO: raster pixel access is currently modeled similar to numpy/ndarray with ..,z,y,x
// TODO: move these functions to base raster (?)
/// Map an image's (x, y) values to the grid cells of a raster.
fn image_pixel_to_raster_pixel<ImagePixelType>(
    x: ImagePixelType,
    y: ImagePixelType,
    scale_x: f64,
    scale_y: f64,
) -> (isize, isize)
where
    ImagePixelType: Into<f64>,
{
    debug_assert!(
        scale_x > 0. && scale_y > 0.,
        "scale values must be positive"
    );

    let cell_x = (((x.into() + 0.5) * scale_x) - 0.5).round();
    let cell_y = (((y.into() + 0.5) * scale_y) - 0.5).round();
    (cell_x as isize, cell_y as isize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::image::RgbaColor;
    use crate::raster::GridIndexAccessMut;
    use std::convert::TryInto;

    #[test]
    fn linear_gradient() {
        let mut raster = Grid2D::new([2, 2].into(), vec![0; 4]).unwrap();

        raster.set_at_grid_index([0, 0], 255).unwrap();
        raster.set_at_grid_index([1, 0], 100).unwrap();

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::new(0, 0, 0, 255)).try_into().unwrap(),
                (255.0, RgbaColor::new(255, 255, 255, 255))
                    .try_into()
                    .unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::pink(),
            None,
            None,
        )
        .unwrap();

        let image_bytes = raster.to_png(100, 100, &colorizer).unwrap();

        // crate::util::test::save_test_bytes(&image_bytes, "linear_gradient.png");

        assert_eq!(
            include_bytes!("../../../../test_data/colorizer/linear_gradient.png") as &[u8],
            image_bytes.as_slice()
        );
    }

    #[test]
    fn logarithmic_gradient() {
        let mut raster = Grid2D::new([2, 2].into(), vec![1; 4]).unwrap();

        raster.set_at_grid_index([0, 0], 10).unwrap();
        raster.set_at_grid_index([1, 0], 5).unwrap();

        let colorizer = Colorizer::logarithmic_gradient(
            vec![
                (1.0, RgbaColor::new(0, 0, 0, 255)).try_into().unwrap(),
                (10.0, RgbaColor::new(255, 255, 255, 255))
                    .try_into()
                    .unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::pink(),
            None,
            None,
        )
        .unwrap();

        let image_bytes = raster.to_png(100, 100, &colorizer).unwrap();

        // crate::util::test::save_test_bytes(&image_bytes, "logarithmic_gradient.png");

        assert_eq!(
            include_bytes!("../../../../test_data/colorizer/logarithmic_gradient.png") as &[u8],
            image_bytes.as_slice()
        );
    }

    #[test]
    fn palette() {
        let mut raster = Grid2D::new([2, 2].into(), vec![0; 4]).unwrap();

        raster.set_at_grid_index([0, 0], 2).unwrap();
        raster.set_at_grid_index([1, 0], 1).unwrap();

        let colorizer = Colorizer::palette(
            [
                (0.0.try_into().unwrap(), RgbaColor::new(0, 0, 0, 255)),
                (1.0.try_into().unwrap(), RgbaColor::new(255, 0, 0, 255)),
                (2.0.try_into().unwrap(), RgbaColor::new(255, 255, 255, 255)),
            ]
            .iter()
            .copied()
            .collect(),
            RgbaColor::transparent(),
            RgbaColor::transparent(),
            None,
            None,
        )
        .unwrap();

        let image_bytes = raster.to_png(100, 100, &colorizer).unwrap();

        // crate::util::test::save_test_bytes(&image_bytes, "palette.png");

        assert_eq!(
            include_bytes!("../../../../test_data/colorizer/palette.png") as &[u8],
            image_bytes.as_slice()
        );
    }

    #[test]
    fn rgba() {
        let mut raster = Grid2D::new([2, 2].into(), vec![0x0000_00FF_u32; 4]).unwrap();

        raster.set_at_grid_index([0, 0], 0xFF00_00FF_u32).unwrap();
        raster.set_at_grid_index([1, 0], 0x00FF_00FF_u32).unwrap();

        let colorizer = Colorizer::rgba();

        let image_bytes = raster.to_png(100, 100, &colorizer).unwrap();

        // crate::util::test::save_test_bytes(&image_bytes, "rgba.png");

        assert_eq!(
            include_bytes!("../../../../test_data/colorizer/rgba.png") as &[u8],
            image_bytes.as_slice()
        );
    }

    #[test]
    fn no_data() {
        let raster = MaskedGrid2D::new(
            Grid2D::new([2, 2].into(), vec![0, 100, 200, 255]).unwrap(),
            Grid2D::new([2, 2].into(), vec![false, true, true, true]).unwrap(),
        )
        .unwrap();

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::new(0, 0, 0, 255)).try_into().unwrap(),
                (255.0, RgbaColor::new(255, 255, 255, 255))
                    .try_into()
                    .unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::pink(),
            None,
            None,
        )
        .unwrap();

        let image_bytes = raster.to_png(100, 100, &colorizer).unwrap();

        // crate::util::test::save_test_bytes(&image_bytes, "no_data_2.png");

        assert_eq!(
            include_bytes!("../../../../test_data/colorizer/no_data.png") as &[u8],
            image_bytes.as_slice()
        );
    }

    #[test]
    fn no_data_tile() {
        let raster = EmptyGrid2D::<u8>::new([2, 2].into());

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::new(0, 0, 0, 255)).try_into().unwrap(),
                (255.0, RgbaColor::new(255, 255, 255, 255))
                    .try_into()
                    .unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::pink(),
            None,
            None,
        )
        .unwrap();

        let image_bytes = raster.to_png(100, 100, &colorizer).unwrap();

        // crate::util::test::save_test_bytes(&image_bytes, "empty.png");

        assert_eq!(
            include_bytes!("../../../../test_data/colorizer/empty.png") as &[u8],
            image_bytes.as_slice()
        );
    }
}
