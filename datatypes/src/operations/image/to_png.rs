use crate::operations::image::Colorizer;
use crate::raster::{GridPixelAccess, Raster, Raster2D};
use image::{DynamicImage, ImageFormat, RgbaImage};
use std::mem;

pub trait ToPng {
    /// Outputs png bytes of an image of size width x height
    fn to_png(&self, width: u32, height: u32, colorizer: &Colorizer) -> Vec<u8>;
}

impl<T> ToPng for Raster2D<T>
where
    T: Into<f64> + Copy + F64Transmutable,
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

            let pixel_value: f64 = self
                .pixel_value_at_grid_index(&(cell_x as usize, cell_y as usize))
                .map(|v| {
                    if mem::discriminant(&Colorizer::Rgba) == mem::discriminant(colorizer) {
                        v.transmute_to_f64()
                    } else {
                        v.into()
                    }
                })
                .unwrap_or(f64::NAN);

            color_mapper.call(pixel_value).into()
        });

        let mut buffer = Vec::new();

        let _ = DynamicImage::ImageRgba8(image_buffer).write_to(&mut buffer, ImageFormat::Png);

        buffer
    }
}

pub trait F64Transmutable {
    fn transmute_to_f64(self) -> f64;
}

impl F64Transmutable for u32 {
    fn transmute_to_f64(self) -> f64 {
        let [r, g, b, a] = self.to_le_bytes();
        f64::from_le_bytes([r, g, b, a, 0, 0, 0, 0])
    }
}

impl F64Transmutable for i32 {
    fn transmute_to_f64(self) -> f64 {
        let [r, g, b, a] = self.to_le_bytes();
        f64::from_le_bytes([r, g, b, a, 0, 0, 0, 0])
    }
}

impl F64Transmutable for f32 {
    fn transmute_to_f64(self) -> f64 {
        let [r, g, b, a] = self.to_le_bytes();
        f64::from_le_bytes([r, g, b, a, 0, 0, 0, 0])
    }
}

impl F64Transmutable for u64 {
    fn transmute_to_f64(self) -> f64 {
        f64::from_le_bytes(self.to_le_bytes())
    }
}

impl F64Transmutable for i64 {
    fn transmute_to_f64(self) -> f64 {
        f64::from_le_bytes(self.to_le_bytes())
    }
}

impl F64Transmutable for u16 {
    fn transmute_to_f64(self) -> f64 {
        let [gray, alpha] = self.to_le_bytes();
        f64::from_le_bytes([gray, gray, gray, alpha, 0, 0, 0, 0])
    }
}

impl F64Transmutable for i16 {
    fn transmute_to_f64(self) -> f64 {
        let [gray, alpha] = self.to_le_bytes();
        f64::from_le_bytes([gray, gray, gray, alpha, 0, 0, 0, 0])
    }
}

impl F64Transmutable for i8 {
    fn transmute_to_f64(self) -> f64 {
        let [gray] = self.to_le_bytes();
        f64::from_le_bytes([gray, gray, gray, 255, 0, 0, 0, 0])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::image::RgbaColor;
    use crate::raster::GridPixelAccessMut;
    use std::fs::File;
    use std::io::Write;

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

        File::create("linear_gradient.png")
            .unwrap()
            .write_all(&raster.to_png(100, 100, &colorizer))
            .unwrap();
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

        File::create("logarithmic_gradient.png")
            .unwrap()
            .write_all(&raster.to_png(100, 100, &colorizer))
            .unwrap();
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

        File::create("palette.png")
            .unwrap()
            .write_all(&raster.to_png(100, 100, &colorizer))
            .unwrap();
    }

    #[test]
    fn rgba() {
        let mut raster = Raster2D::new(
            [2, 2].into(),
            vec![u32::from_le_bytes([0, 0, 0, 255]); 4],
            None,
            Default::default(),
            Default::default(),
        )
        .unwrap();

        raster
            .set_pixel_value_at_grid_index(&(0, 0), u32::from_le_bytes([255, 0, 0, 255]))
            .unwrap();
        raster
            .set_pixel_value_at_grid_index(&(0, 1), u32::from_le_bytes([0, 255, 0, 255]))
            .unwrap();

        let colorizer = Colorizer::rgba();

        File::create("rgba.png")
            .unwrap()
            .write_all(&raster.to_png(100, 100, &colorizer))
            .unwrap();
    }
}
