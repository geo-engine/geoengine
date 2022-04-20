use std::ops::{Add, Div, Mul, Sub};

use num_traits::AsPrimitive;

pub fn scale_pixel<In, Out>(pixel_value: In, scale_with: In, offset_by: In) -> Option<Out>
where
    In: AsPrimitive<Out> + Copy + 'static + Sub<Output = In> + Div<Output = In>,
    Out: Copy + 'static,
{
    Some(((pixel_value - offset_by) / scale_with).as_())
}

pub fn unscale_pixel<In, Out>(pixel_value: In, scale_with: Out, offset_by: Out) -> Option<Out>
where
    In: AsPrimitive<Out> + Copy + 'static,
    Out: Copy + 'static + Add<Output = Out> + Mul<Output = Out>,
{
    Some(((pixel_value).as_() * scale_with) + offset_by)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scale_pixels() {
        let pixel_value: u8 = 0;
        let scale_with: u8 = 2;
        let offset_by: u8 = 1;
        let scaled_pixel_value = scale_pixel(pixel_value, scale_with, offset_by);
        assert_eq!(scaled_pixel_value, Some(1));
    }
}
