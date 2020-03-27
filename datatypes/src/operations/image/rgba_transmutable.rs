use crate::operations::image::RgbaColor;

/// This trait allows using raw bytes as RGBA colors
///
/// # Examples
///
/// ```
/// use geoengine_datatypes::operations::image::{RgbaColor, RgbaTransmutable};
///
/// assert_eq!(
///     0x01234567_u32.transmute_to_rgba(),
///     RgbaColor::new(0x01, 0x23, 0x45, 0x67)
/// );
/// ```
pub trait RgbaTransmutable {
    fn transmute_to_rgba(self) -> RgbaColor;
}

/// Implement `RgbaTransmutable` for types with at least four bytes
/// that implement the `to_be_bytes` function (which is not backed by a trait)
macro_rules! rbga_transmutable_impl {
    ($type:ty) => {
        impl RgbaTransmutable for $type {
            fn transmute_to_rgba(self) -> RgbaColor {
                let [r, g, b, a, ..] = self.to_be_bytes();
                RgbaColor::new(r, g, b, a)
            }
        }
    };
}

// floats

rbga_transmutable_impl!(f64);
rbga_transmutable_impl!(f32);

// integers

rbga_transmutable_impl!(u64);
rbga_transmutable_impl!(i64);
rbga_transmutable_impl!(u32);
rbga_transmutable_impl!(i32);

impl RgbaTransmutable for u16 {
    fn transmute_to_rgba(self) -> RgbaColor {
        let [gray, alpha] = self.to_be_bytes();
        RgbaColor::new(gray, gray, gray, alpha)
    }
}

impl RgbaTransmutable for i16 {
    fn transmute_to_rgba(self) -> RgbaColor {
        let [gray, alpha] = self.to_be_bytes();
        RgbaColor::new(gray, gray, gray, alpha)
    }
}

impl RgbaTransmutable for u8 {
    fn transmute_to_rgba(self) -> RgbaColor {
        let [gray] = self.to_be_bytes();
        RgbaColor::new(gray, gray, gray, std::u8::MAX)
    }
}

impl RgbaTransmutable for i8 {
    fn transmute_to_rgba(self) -> RgbaColor {
        let [gray] = self.to_be_bytes();
        RgbaColor::new(gray, gray, gray, std::u8::MAX)
    }
}

// others

impl RgbaTransmutable for bool {
    fn transmute_to_rgba(self) -> RgbaColor {
        if self {
            RgbaColor::white()
        } else {
            RgbaColor::black()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversions() {
        assert_eq!(
            0x01234567_u32.transmute_to_rgba(),
            RgbaColor::new(0x01, 0x23, 0x45, 0x67)
        );
        assert_eq!(
            0x01234567_i32.transmute_to_rgba(),
            RgbaColor::new(0x01, 0x23, 0x45, 0x67)
        );

        assert_eq!(
            0x0123_u16.transmute_to_rgba(),
            RgbaColor::new(0x01, 0x01, 0x01, 0x23)
        );
        assert_eq!(
            0x0123_i16.transmute_to_rgba(),
            RgbaColor::new(0x01, 0x01, 0x01, 0x23)
        );

        assert_eq!(
            0x01_u8.transmute_to_rgba(),
            RgbaColor::new(0x01, 0x01, 0x01, 0xFF)
        );
        assert_eq!(
            0x01_i8.transmute_to_rgba(),
            RgbaColor::new(0x01, 0x01, 0x01, 0xFF)
        );

        assert_eq!(
            0x0123456700000000_u64.transmute_to_rgba(),
            RgbaColor::new(0x01, 0x23, 0x45, 0x67)
        );
        assert_eq!(
            0x0123456700000000_i64.transmute_to_rgba(),
            RgbaColor::new(0x01, 0x23, 0x45, 0x67)
        );

        assert_eq!(
            f32::from_be_bytes([0x01, 0x23, 0x45, 0x67]).transmute_to_rgba(),
            RgbaColor::new(0x01, 0x23, 0x45, 0x67)
        );
        assert_eq!(
            f64::from_be_bytes([0x01, 0x23, 0x45, 0x67, 0, 0, 0, 0]).transmute_to_rgba(),
            RgbaColor::new(0x01, 0x23, 0x45, 0x67)
        );
    }
}
