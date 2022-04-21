use std::ops::{Add, Div, Mul, Sub};

use num_traits::{AsPrimitive, CheckedAdd, CheckedDiv, CheckedMul, CheckedSub};

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

pub fn scale_pixel_overflow_to_none<In, Out>(
    pixel_value: In,
    scale_with: In,
    offset_by: In,
) -> Option<Out>
where
    In: AsPrimitive<Out> + Copy + 'static + CheckedSub<Output = In> + CheckedDiv<Output = In>,
    Out: Copy + 'static,
{
    pixel_value
        .checked_sub(&offset_by)
        .and_then(|f| f.checked_div(&scale_with))
        .map(|r| r.as_())
}

pub fn unscale_pixel_overflow_to_none<In, Out>(
    pixel_value: In,
    scale_with: Out,
    offset_by: Out,
) -> Option<Out>
where
    In: AsPrimitive<Out> + Copy + 'static,
    Out: Copy + 'static + CheckedAdd<Output = Out> + CheckedMul<Output = Out>,
{
    ((pixel_value).as_())
        .checked_mul(&scale_with)
        .and_then(|f| f.checked_add(&offset_by))
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
