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
    fn transmute_to_f64(self) -> f64;

    fn transmute_to_rgba(self) -> RgbaColor
    where
        Self: Sized,
    {
        let [r, g, b, a, ..] = self.transmute_to_f64().to_be_bytes();
        RgbaColor::new(r, g, b, a)
    }
}

// floats

impl RgbaTransmutable for f64 {
    fn transmute_to_f64(self) -> f64 {
        self
    }
}

impl RgbaTransmutable for f32 {
    fn transmute_to_f64(self) -> f64 {
        let [r, g, b, a] = self.to_be_bytes();
        f64::from_be_bytes([r, g, b, a, 0, 0, 0, 0])
    }
}

// integers

impl RgbaTransmutable for u32 {
    fn transmute_to_f64(self) -> f64 {
        let [r, g, b, a] = self.to_be_bytes();
        f64::from_be_bytes([r, g, b, a, 0, 0, 0, 0])
    }
}

impl RgbaTransmutable for i32 {
    fn transmute_to_f64(self) -> f64 {
        let [r, g, b, a] = self.to_be_bytes();
        f64::from_be_bytes([r, g, b, a, 0, 0, 0, 0])
    }
}

impl RgbaTransmutable for u64 {
    fn transmute_to_f64(self) -> f64 {
        f64::from_be_bytes(self.to_be_bytes())
    }
}

impl RgbaTransmutable for i64 {
    fn transmute_to_f64(self) -> f64 {
        f64::from_be_bytes(self.to_be_bytes())
    }
}

impl RgbaTransmutable for u16 {
    fn transmute_to_f64(self) -> f64 {
        let [gray, alpha] = self.to_be_bytes();
        f64::from_be_bytes([gray, gray, gray, alpha, 0, 0, 0, 0])
    }
}

impl RgbaTransmutable for i16 {
    fn transmute_to_f64(self) -> f64 {
        let [gray, alpha] = self.to_be_bytes();
        f64::from_be_bytes([gray, gray, gray, alpha, 0, 0, 0, 0])
    }
}

impl RgbaTransmutable for u8 {
    fn transmute_to_f64(self) -> f64 {
        let [gray] = self.to_be_bytes();
        f64::from_be_bytes([gray, gray, gray, 255, 0, 0, 0, 0])
    }
}

impl RgbaTransmutable for i8 {
    fn transmute_to_f64(self) -> f64 {
        let [gray] = self.to_be_bytes();
        f64::from_be_bytes([gray, gray, gray, 255, 0, 0, 0, 0])
    }
}

// others

impl RgbaTransmutable for bool {
    fn transmute_to_f64(self) -> f64 {
        f64::from_be_bytes(if self {
            [255, 255, 255, 255, 0, 0, 0, 0]
        } else {
            [0, 0, 0, 0, 0, 0, 0, 0]
        })
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
