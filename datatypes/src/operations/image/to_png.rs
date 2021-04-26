use crate::error;
use crate::operations::image::{Colorizer, RgbaTransmutable};
use crate::raster::{Grid2D, GridIndexAccess, Pixel, RasterTile2D, TypedRasterTile2D};
use crate::util::Result;
use image::{DynamicImage, ImageFormat, RgbaImage};

pub trait ToPng {
    /// Outputs png bytes of an image of size width x height
    fn to_png(&self, width: u32, height: u32, colorizer: &Colorizer) -> Result<Vec<u8>>;
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

        let image_buffer = if let Some(no_data_value) = self.no_data_value {
            let no_data_fn = move |p: P| p == no_data_value;
            create_rgba_image(self, width, height, colorizer, scale_x, scale_y, no_data_fn)
        } else {
            let no_data_fn = move |_| false;
            create_rgba_image(self, width, height, colorizer, scale_x, scale_y, no_data_fn)
        };

        let mut buffer = Vec::new();

        DynamicImage::ImageRgba8(image_buffer)
            .write_to(&mut buffer, ImageFormat::Png)
            .map_err(|error| error::Error::Colorizer {
                details: format!("encoding PNG failed: {}", error),
            })?;

        Ok(buffer)
    }
}

fn create_rgba_image<P: Pixel + RgbaTransmutable, N: Fn(P) -> bool>(
    raster_grid: &Grid2D<P>,
    width: u32,
    height: u32,
    colorizer: &Colorizer,
    scale_x: f64,
    scale_y: f64,
    is_no_data: N,
) -> RgbaImage {
    let color_mapper = colorizer.create_color_mapper();

    RgbaImage::from_fn(width, height, |x, y| {
        let (grid_pixel_x, grid_pixel_y) = image_pixel_to_raster_pixel(x, y, scale_x, scale_y);
        if let Ok(pixel_value) = raster_grid.get_at_grid_index([grid_pixel_y, grid_pixel_x]) {
            if is_no_data(pixel_value) {
                return colorizer.no_data_color().into();
            }

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
        let mut raster = Grid2D::new([2, 2].into(), vec![0; 4], None).unwrap();

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
        )
        .unwrap();

        let image_bytes = raster.to_png(100, 100, &colorizer).unwrap();

        assert_eq!(
            include_bytes!("../../../test-data/colorizer/linear_gradient.png") as &[u8],
            image_bytes.as_slice()
        );
    }

    #[test]
    fn logarithmic_gradient() {
        let mut raster = Grid2D::new([2, 2].into(), vec![1; 4], None).unwrap();

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
        )
        .unwrap();

        let image_bytes = raster.to_png(100, 100, &colorizer).unwrap();

        assert_eq!(
            include_bytes!("../../../test-data/colorizer/logarithmic_gradient.png") as &[u8],
            image_bytes.as_slice()
        );
    }

    #[test]
    fn palette() {
        let mut raster = Grid2D::new([2, 2].into(), vec![0; 4], None).unwrap();

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
        )
        .unwrap();

        let image_bytes = raster.to_png(100, 100, &colorizer).unwrap();

        assert_eq!(
            include_bytes!("../../../test-data/colorizer/palette.png") as &[u8],
            image_bytes.as_slice()
        );
    }

    #[test]
    fn rgba() {
        let mut raster = Grid2D::new([2, 2].into(), vec![0x0000_00FF_u32; 4], None).unwrap();

        raster.set_at_grid_index([0, 0], 0xFF00_00FF_u32).unwrap();
        raster.set_at_grid_index([1, 0], 0x00FF_00FF_u32).unwrap();

        let colorizer = Colorizer::rgba();

        let image_bytes = raster.to_png(100, 100, &colorizer).unwrap();

        assert_eq!(
            include_bytes!("../../../test-data/colorizer/rgba.png") as &[u8],
            image_bytes.as_slice()
        );
    }

    #[test]
    fn no_data() {
        let raster = Grid2D::new([2, 2].into(), vec![0, 100, 200, 255], Some(0)).unwrap();

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::new(0, 0, 0, 255)).try_into().unwrap(),
                (255.0, RgbaColor::new(255, 255, 255, 255))
                    .try_into()
                    .unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::pink(),
        )
        .unwrap();

        let image_bytes = raster.to_png(100, 100, &colorizer).unwrap();

        assert_eq!(
            include_bytes!("../../../test-data/colorizer/no_data.png") as &[u8],
            image_bytes.as_slice()
        );
    }
}
