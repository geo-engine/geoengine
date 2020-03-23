use crate::operations::image::{Colorizer, RgbaTransmutable};
use crate::raster::{GridPixelAccess, Raster, Raster2D};
use image::{DynamicImage, ImageFormat, RgbaImage};

pub trait ToPng {
    /// Outputs png bytes of an image of size width x height
    fn to_png(&self, width: u32, height: u32, colorizer: &Colorizer) -> Vec<u8>;
}

impl<T> ToPng for Raster2D<T>
where
    T: Into<f64> + Copy + RgbaTransmutable,
{
    fn to_png(&self, width: u32, height: u32, colorizer: &Colorizer) -> Vec<u8> {
        // TODO: use PNG color palette once it is available

        let scale_x = (self.dimension()[0] as f64) / f64::from(width);
        let scale_y = (self.dimension()[1] as f64) / f64::from(height);

        let color_mapper = colorizer.create_color_mapper();

        let image_buffer: RgbaImage = RgbaImage::from_fn(width, height, |x, y| {
            // TODO: move these functions to base raster (?)
            let cell_x = (((f64::from(x) + 0.5) * scale_x) - 0.5).round() as u32;
            let cell_y = (((f64::from(y) + 0.5) * scale_y) - 0.5).round() as u32;

            if let Ok(pixel_value) =
                self.pixel_value_at_grid_index(&(cell_x as usize, cell_y as usize))
            {
                color_mapper.call(pixel_value)
            } else {
                colorizer.no_data_color()
            }
            .into()
        });

        let mut buffer = Vec::new();

        let _ = DynamicImage::ImageRgba8(image_buffer).write_to(&mut buffer, ImageFormat::Png);

        buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::image::RgbaColor;
    use crate::raster::GridPixelAccessMut;

    #[test]
    fn linear_gradient() {
        let mut raster = Raster2D::new(
            [2, 2].into(),
            vec![0; 4],
            None,
            Default::default(),
            Default::default(),
        )
        .unwrap();

        raster.set_pixel_value_at_grid_index(&(0, 0), 255).unwrap();
        raster.set_pixel_value_at_grid_index(&(0, 1), 100).unwrap();

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0.into(), RgbaColor::new(0, 0, 0, 255)),
                (255.0.into(), RgbaColor::new(255, 255, 255, 255)),
            ],
            RgbaColor::transparent(),
            RgbaColor::pink(),
        )
        .unwrap();

        let image_bytes = raster.to_png(100, 100, &colorizer);

        assert_eq!(
            include_bytes!("../../../test-data/colorizer/linear_gradient.png") as &[u8],
            image_bytes.as_slice()
        );
    }

    #[test]
    fn logarithmic_gradient() {
        let mut raster = Raster2D::new(
            [2, 2].into(),
            vec![1; 4],
            None,
            Default::default(),
            Default::default(),
        )
        .unwrap();

        raster.set_pixel_value_at_grid_index(&(0, 0), 10).unwrap();
        raster.set_pixel_value_at_grid_index(&(0, 1), 5).unwrap();

        let colorizer = Colorizer::logarithmic_gradient(
            vec![
                (1.0.into(), RgbaColor::new(0, 0, 0, 255)),
                (10.0.into(), RgbaColor::new(255, 255, 255, 255)),
            ],
            RgbaColor::transparent(),
            RgbaColor::pink(),
        )
        .unwrap();

        let image_bytes = raster.to_png(100, 100, &colorizer);

        assert_eq!(
            include_bytes!("../../../test-data/colorizer/logarithmic_gradient.png") as &[u8],
            image_bytes.as_slice()
        );
    }

    #[test]
    fn palette() {
        let mut raster = Raster2D::new(
            [2, 2].into(),
            vec![0; 4],
            None,
            Default::default(),
            Default::default(),
        )
        .unwrap();

        raster.set_pixel_value_at_grid_index(&(0, 0), 2).unwrap();
        raster.set_pixel_value_at_grid_index(&(0, 1), 1).unwrap();

        let colorizer = Colorizer::palette(
            [
                (0.0.into(), RgbaColor::new(0, 0, 0, 255)),
                (1.0.into(), RgbaColor::new(255, 0, 0, 255)),
                (2.0.into(), RgbaColor::new(255, 255, 255, 255)),
            ]
            .iter()
            .cloned()
            .collect(),
            RgbaColor::transparent(),
        )
        .unwrap();

        let image_bytes = raster.to_png(100, 100, &colorizer);

        assert_eq!(
            include_bytes!("../../../test-data/colorizer/palette.png") as &[u8],
            image_bytes.as_slice()
        );
    }

    #[test]
    fn rgba() {
        let mut raster = Raster2D::new(
            [2, 2].into(),
            vec![0x000000FF_u32; 4],
            None,
            Default::default(),
            Default::default(),
        )
        .unwrap();

        raster
            .set_pixel_value_at_grid_index(&(0, 0), 0xFF0000FF_u32)
            .unwrap();
        raster
            .set_pixel_value_at_grid_index(&(0, 1), 0x00FF00FF_u32)
            .unwrap();

        let colorizer = Colorizer::rgba();

        let image_bytes = raster.to_png(100, 100, &colorizer);

        assert_eq!(
            include_bytes!("../../../test-data/colorizer/rgba.png") as &[u8],
            image_bytes.as_slice()
        );
    }
}
