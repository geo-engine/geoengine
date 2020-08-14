use crate::raster::{data_type::FromPrimitive, BaseRaster, GridDimension, Pixel, Raster};
use crate::util::Result;
use num_traits::AsPrimitive;

pub trait RasterClassification<D, C>
where
    D: GridDimension,
{
    type Output: Raster<D, u32, C>;
    fn classification(
        &self,
        bounds_class: &[(f64, f64, u32)],
        no_data_class: u32,
    ) -> Result<Self::Output>;
}

impl<D, T> RasterClassification<D, Vec<u32>> for BaseRaster<D, T, Vec<T>>
where
    D: GridDimension + Copy,
    T: Pixel + FromPrimitive<f64> + AsPrimitive<f64>,
{
    type Output = BaseRaster<D, u32, Vec<u32>>;
    fn classification(
        &self,
        bounds_class: &[(f64, f64, u32)],
        no_data_class: u32,
    ) -> Result<Self::Output> {
        let pixels: Vec<u32> = self
            .data_container
            .iter()
            .map(|&p| {
                if let Some(no_data_value) = self.no_data_value {
                    if p == no_data_value {
                        return no_data_class;
                    }
                }

                let class = bounds_class.iter().find_map(|&(lower, upper, class)| {
                    let p_f64: f64 = p.as_();
                    if (p_f64 >= lower) && (p_f64 <= upper) {
                        return Some(class);
                    }
                    None
                });
                class.unwrap_or(no_data_class)
            })
            .collect();

        BaseRaster::new(
            self.grid_dimension,
            pixels,
            Some(no_data_class),
            self.temporal_bounds,
            self.geo_transform,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{primitives::TimeInterval, raster::Raster2D};

    #[test]
    fn classifiy_raster_2d() {
        let dim = [3, 2];
        let data = vec![1, 2, 3, 4, 5, 6];
        let geo_transform = [1.0, 1.0, 0.0, 1.0, 0.0, 1.0];
        let temporal_bounds: TimeInterval = TimeInterval::default();
        let raster2d = Raster2D::new(
            dim.into(),
            data,
            None,
            temporal_bounds,
            geo_transform.into(),
        )
        .unwrap();

        let def = vec![(1.0, 2.0, 2), (4.0, 5.0, 3)];
        let class_raster = raster2d.classification(&def, 0).unwrap();

        assert_eq!(class_raster.data_container, vec![2, 2, 0, 3, 3, 0,])
    }
}
