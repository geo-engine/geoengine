use crate::util::number_statistics::NumberStatistics;
use geoengine_datatypes::raster::{Grid2D, GridShape2D, GridSize, Pixel};
use num_traits::AsPrimitive;

/// A function that calculates one pixel values from a neighborhood of pixels.
pub trait KernelFunction<P>: Sync + Send + Clone
where
    P: Pixel,
    f64: AsPrimitive<P>,
{
    fn kernel_size(&self) -> GridShape2D;

    // TODO: we must ensure that the size of `values` adhere to `kernel_size`
    fn apply(&self, values: &[Option<P>]) -> Option<P>;
}

/// A kernel that computes the standard deviation of a neighborhood of pixels.
#[derive(Debug, Clone, Copy)]
pub struct StandardDeviationKernel {
    dimensions: GridShape2D,
}

impl StandardDeviationKernel {
    pub fn new(dimensions: GridShape2D) -> Self {
        Self { dimensions }
    }
}

impl<P> KernelFunction<P> for StandardDeviationKernel
where
    P: Pixel,
    f64: AsPrimitive<P>,
{
    fn kernel_size(&self) -> GridShape2D {
        self.dimensions
    }

    fn apply(&self, values: &[Option<P>]) -> Option<P> {
        if values.len() != self.dimensions.number_of_elements() {
            return None;
        }

        let mut aggreagtor = NumberStatistics::default();
        for value in values {
            match value {
                Some(v) => aggreagtor.add(*v),
                // TODO: apply strategy here?
                None => aggreagtor.add_no_data(),
            }
        }

        Some(aggreagtor.std_dev().as_())
    }
}

/// A kernel that convolutes a neighborhood of pixels with a kernel matrix.
#[derive(Debug, Clone)]
pub struct ConvolutionKernel {
    kernel: Grid2D<f64>,
}

impl ConvolutionKernel {
    pub fn new(kernel: Grid2D<f64>) -> Self {
        Self { kernel }
    }
}

impl<P> KernelFunction<P> for ConvolutionKernel
where
    P: Pixel,
    f64: AsPrimitive<P>,
{
    fn kernel_size(&self) -> GridShape2D {
        self.kernel.axis_size().into()
    }

    fn apply(&self, value_options: &[Option<P>]) -> Option<P> {
        if value_options.len() != self.kernel.shape.number_of_elements() {
            return None;
        }

        let mut values = Vec::<f64>::with_capacity(value_options.len());
        for value in value_options {
            if let Some(v) = value {
                values.push(v.as_());
            } else {
                return None;
            }
        }

        // TODO: SIMD?
        let mut sum = 0.0;
        for (value, kernel_value) in values.iter().zip(self.kernel.data.iter()) {
            sum += value * kernel_value;
        }

        Some(sum.as_())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_standard_deviation_kernel() {
        let kernel = StandardDeviationKernel::new(GridShape2D::new([3, 3]));

        let result = kernel.apply(&[
            Some(1.),
            Some(2.),
            Some(3.),
            Some(4.),
            Some(5.),
            Some(6.),
            Some(7.),
            Some(8.),
            Some(9.),
        ]);
        assert_eq!(result.unwrap(), 2.581_988_897_471_611);

        let result = kernel.apply(&[
            Some(1.),
            Some(2.),
            Some(3.),
            Some(4.),
            Some(5.),
            Some(6.),
            Some(7.),
            Some(8.),
            None,
        ]);
        assert_eq!(result.unwrap(), 2.291_287_847_477_92);

        assert!(kernel.apply(&[] as &[Option<f64>]).is_none());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_convolution_kernel() {
        let kernel = ConvolutionKernel::new(
            Grid2D::new(GridShape2D::new([3, 3]), vec![1. / 9.; 9]).unwrap(),
        );

        let result = kernel.apply(&[
            Some(1.),
            Some(2.),
            Some(3.),
            Some(4.),
            Some(5.),
            Some(6.),
            Some(7.),
            Some(8.),
            Some(9.),
        ]);
        assert_eq!(result.unwrap(), 5.);

        let result = kernel.apply(&[
            Some(1.),
            Some(2.),
            Some(3.),
            Some(4.),
            Some(5.),
            Some(6.),
            Some(7.),
            Some(8.),
            None,
        ]);
        assert!(result.is_none());

        assert!(kernel.apply(&[] as &[Option<f64>]).is_none());
    }
}
