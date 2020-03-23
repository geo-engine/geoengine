use crate::error;
use crate::operations::image::RgbaTransmutable;
use crate::util::Result;
use ordered_float::NotNan;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::collections::HashMap;

/// A colorizer specifies a mapping between raster values and an output image
/// There are different variants that perform different kinds of mapping.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Colorizer {
    LinearGradient {
        breakpoints: Breakpoints,
        no_data_color: RgbaColor,
        default_color: RgbaColor,
    },
    LogarithmicGradient {
        breakpoints: Breakpoints,
        no_data_color: RgbaColor,
        default_color: RgbaColor,
    },
    Palette {
        colors: Palette,
        no_data_color: RgbaColor,
    },
    Rgba,
}

impl Colorizer {
    /// A linear gradient linearly interpolates values within breakpoints of a color table
    pub fn linear_gradient(
        breakpoints: Breakpoints,
        no_data_color: RgbaColor,
        default_color: RgbaColor,
    ) -> Result<Self> {
        ensure!(
            breakpoints.len() >= 2,
            error::Colorizer {
                details: "Linear Gradient Colorizer must have a least two breakpoints"
            }
        );

        let colorizer = Self::LinearGradient {
            breakpoints,
            no_data_color,
            default_color,
        };

        ensure!(
            colorizer.min_value() < colorizer.max_value(),
            error::Colorizer {
                details: "A colorizer's min value must be smaller than its max value"
            }
        );

        Ok(colorizer)
    }

    /// A logarithmic gradient logarithmically interpolates values within breakpoints of a color table
    /// and allows only positive values
    pub fn logarithmic_gradient(
        breakpoints: Breakpoints,
        no_data_color: RgbaColor,
        default_color: RgbaColor,
    ) -> Result<Self> {
        ensure!(
            breakpoints.len() >= 2,
            error::Colorizer {
                details: "A log-scale gradient colorizer must have a least two breakpoints"
            }
        );

        let colorizer = Self::LogarithmicGradient {
            breakpoints,
            no_data_color,
            default_color,
        };

        ensure!(
            colorizer.min_value() > 0.,
            error::Colorizer {
                details: "A log-scale colorizer's min value must be positive"
            }
        );
        ensure!(
            colorizer.min_value() < colorizer.max_value(),
            error::Colorizer {
                details: "A colorizer's min value must be smaller than its max value"
            }
        );

        Ok(colorizer)
    }

    /// A palette maps values as classes to a certain color.
    /// Unmapped values results in the NO DATA color
    pub fn palette(colors: Palette, no_data_color: RgbaColor) -> Result<Self> {
        ensure!(
            !colors.is_empty() && colors.len() <= 256,
            error::Colorizer {
                details: "A palette colorizer must have a least one color and at most 256 colors"
            }
        );

        Ok(Self::Palette {
            colors,
            no_data_color,
        })
    }

    /// Rgba colorization means treating the values as red, green, blue and alpha bytes
    pub fn rgba() -> Self {
        Self::Rgba
    }

    /// Returns the minimum value that is covered by this colorizer
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::operations::image::{Colorizer, RgbaColor};
    ///
    /// let colorizer = Colorizer::linear_gradient(
    ///     vec![(0.0.into(), RgbaColor::transparent()), (1.0.into(), RgbaColor::transparent())],
    ///     RgbaColor::transparent(),
    ///     RgbaColor::transparent(),
    /// ).unwrap();
    ///
    /// assert_eq!(colorizer.min_value(), 0.);
    /// ```
    pub fn min_value(&self) -> f64 {
        match self {
            Self::LinearGradient { breakpoints, .. }
            | Self::LogarithmicGradient { breakpoints, .. } => *breakpoints[0].0,
            Self::Palette { .. } | Self::Rgba { .. } => f64::from(std::u8::MIN),
        }
    }

    /// Returns the maxium value that is covered by this colorizer
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::operations::image::{Colorizer, RgbaColor};
    ///
    /// let colorizer = Colorizer::logarithmic_gradient(
    ///     vec![(1.0.into(), RgbaColor::transparent()), (10.0.into(), RgbaColor::transparent())],
    ///     RgbaColor::transparent(),
    ///     RgbaColor::transparent(),
    /// ).unwrap();
    ///
    /// assert_eq!(colorizer.max_value(), 10.);
    /// ```
    pub fn max_value(&self) -> f64 {
        match self {
            Self::LinearGradient { breakpoints, .. }
            | Self::LogarithmicGradient { breakpoints, .. } => {
                *breakpoints[breakpoints.len() - 1].0
            }
            Self::Palette { .. } | Self::Rgba { .. } => f64::from(std::u8::MAX),
        }
    }

    /// Creates a function for mapping raster values to colors
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::operations::image::{Colorizer, RgbaColor};
    ///
    /// let linear_colorizer = Colorizer::linear_gradient(
    ///     vec![(0.0.into(), RgbaColor::black()), (1.0.into(), RgbaColor::white())],
    ///     RgbaColor::transparent(),
    ///     RgbaColor::transparent(),
    /// ).unwrap();
    /// let linear_color_mapper = linear_colorizer.create_color_mapper();
    ///
    /// assert_eq!(linear_color_mapper.call(0.5), RgbaColor::new(128, 128, 128, 255));
    ///
    /// let logarithmic_colorizer = Colorizer::logarithmic_gradient(
    ///     vec![(1.0.into(), RgbaColor::black()), (10.0.into(), RgbaColor::white())],
    ///     RgbaColor::transparent(),
    ///     RgbaColor::transparent(),
    /// ).unwrap();
    /// let logarithmic_color_mapper = logarithmic_colorizer.create_color_mapper();
    ///
    /// assert_eq!(logarithmic_color_mapper.call(5.5), RgbaColor::new(189, 189, 189, 255));
    /// ```
    pub fn create_color_mapper(&self) -> ColorMapper {
        const COLOR_TABLE_SIZE: usize = 254; // use 256 colors with no data and default colors

        let (min_value, max_value) = (self.min_value(), self.max_value());

        match self {
            Self::LinearGradient {
                breakpoints: _,
                no_data_color,
                default_color,
            }
            | Self::LogarithmicGradient {
                breakpoints: _,
                no_data_color,
                default_color,
            } => {
                let color_table = self.color_table(COLOR_TABLE_SIZE, min_value, max_value);

                ColorMapper::ColorTable {
                    color_table,
                    min_value,
                    max_value,
                    no_data_color: *no_data_color,
                    default_color: *default_color,
                }
            }
            Self::Palette {
                colors,
                no_data_color,
            } => ColorMapper::ColorMap {
                color_map: colors,
                no_data_color: *no_data_color,
            },
            Self::Rgba => ColorMapper::Rgba,
        }
    }

    /// Creates a color table of `number_of_colors` colors
    /// This must only be called for colorizers that use breakpoints
    fn color_table(&self, number_of_colors: usize, min: f64, max: f64) -> Vec<RgbaColor> {
        let breakpoints = match self {
            Self::LinearGradient { breakpoints, .. }
            | Self::LogarithmicGradient { breakpoints, .. } => breakpoints,
            _ => unimplemented!("Must never call `color_table` for types without breakpoints"),
        };

        let smallest_breakpoint_value = *breakpoints[0].0;
        let largest_breakpoint_value = *breakpoints[breakpoints.len() - 1].0;

        let first_color = breakpoints[0].1;
        let last_color = breakpoints[breakpoints.len() - 1].1;

        let step = (max - min) / ((number_of_colors - 1) as f64);

        let mut breakpoint_iter = breakpoints.iter();
        let mut breakpoint_prev = breakpoint_iter.next().expect("must have first entry");
        let mut breakpoint_next = breakpoint_iter.next().expect("must have second entry");

        let color_table: Vec<RgbaColor> = std::iter::successors(Some(min), |v| Some(v + step))
            .take(number_of_colors)
            .map(|value| {
                if value < smallest_breakpoint_value {
                    first_color // use these because of potential rounding errors instead of default color
                } else if value > largest_breakpoint_value {
                    last_color // use these because of potential rounding errors instead of default color
                } else {
                    while value > *breakpoint_next.0 {
                        breakpoint_prev = breakpoint_next;
                        breakpoint_next = breakpoint_iter
                            .next()
                            .expect("if-condition must ensure this");
                    }

                    let prev_value = *breakpoint_prev.0;
                    let next_value = *breakpoint_next.0;

                    let prev_color = breakpoint_prev.1;
                    let next_color = breakpoint_next.1;

                    let fraction = match self {
                        Self::LinearGradient { .. } => {
                            (value - prev_value) / (next_value - prev_value)
                        }
                        Self::LogarithmicGradient { .. } => {
                            let nominator = f64::log10(value) - f64::log10(prev_value);
                            let denominator = f64::log10(next_value) - f64::log10(prev_value);
                            nominator / denominator
                        }
                        _ => unreachable!(), // cf. first match in function
                    };

                    prev_color.factor_add(next_color, fraction)
                }
            })
            .collect();

        debug_assert_eq!(color_table.len(), number_of_colors);

        color_table
    }
}

/// A ColorMapper is a function for mapping raster values to colors
pub enum ColorMapper<'c> {
    ColorTable {
        color_table: Vec<RgbaColor>,
        min_value: f64,
        max_value: f64,
        no_data_color: RgbaColor,
        default_color: RgbaColor,
    },
    ColorMap {
        color_map: &'c Palette,
        no_data_color: RgbaColor,
    },
    Rgba,
}

// TODO: use Fn-trait once it is stable
impl<'c> ColorMapper<'c> {
    /// Map a raster value to a color from the colorizer
    pub fn call(&self, value: f64) -> RgbaColor {
        match self {
            ColorMapper::ColorTable {
                color_table,
                min_value,
                max_value,
                no_data_color,
                default_color,
            } => {
                if f64::is_nan(value) {
                    *no_data_color
                } else if value < *min_value || value > *max_value {
                    *default_color
                } else {
                    let color_table_factor = (color_table.len() - 1) as f64;
                    let table_entry = f64::round(
                        color_table_factor * ((value - *min_value) / (*max_value - *min_value)),
                    ) as usize;
                    *color_table.get(table_entry).unwrap_or(default_color)
                }
            }
            ColorMapper::ColorMap {
                color_map,
                no_data_color,
            } => {
                if value.is_nan() {
                    *no_data_color
                } else {
                    let value = unsafe { NotNan::unchecked_new(value) }; // checked by `if` above
                    *color_map.get(&value).unwrap_or(no_data_color)
                }
            }
            ColorMapper::Rgba => value.transmute_to_rgba(),
        }
    }
}

/// A breakpoint is a list of (value, color) tuples.
///
/// It is assumed to be ordered ascending and has at least two entries,
/// although we only check the first and last value for performance reasons.
pub type Breakpoints = Vec<(NotNan<f64>, RgbaColor)>;

/// A map from value to color
///
/// It is assumed that is has at least one and at most 256 entries.
pub type Palette = HashMap<NotNan<f64>, RgbaColor>;

/// RgbaColor defines a 32 bit RGB color with alpha value
#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RgbaColor([u8; 4]);

impl RgbaColor {
    /// Creates a new color from red, green, blue and alpha values
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::operations::image::RgbaColor;
    ///
    /// assert_eq!(RgbaColor::new(0, 0, 0, 255), RgbaColor::black());
    /// assert_eq!(RgbaColor::new(255, 255, 255, 255), RgbaColor::white());
    /// assert_eq!(RgbaColor::new(0, 0, 0, 0), RgbaColor::transparent());
    /// assert_eq!(RgbaColor::new(255, 0, 255, 255), RgbaColor::pink());
    /// ```
    pub fn new(red: u8, green: u8, blue: u8, alpha: u8) -> Self {
        RgbaColor([red, green, blue, alpha])
    }

    pub fn transparent() -> Self {
        RgbaColor::new(0, 0, 0, 0)
    }

    pub fn black() -> Self {
        RgbaColor::new(0, 0, 0, 255)
    }

    pub fn white() -> Self {
        RgbaColor::new(255, 255, 255, 255)
    }

    pub fn pink() -> Self {
        RgbaColor::new(255, 0, 255, 255)
    }

    /// Adds another color with a factor in [0, 1] to this color.
    /// The current color remains in (1 - factor)
    ///
    /// # Example
    ///
    /// ```
    /// use geoengine_datatypes::operations::image::RgbaColor;
    ///
    /// assert_eq!(RgbaColor::black().factor_add(RgbaColor::white(), 0.5), RgbaColor::new(128, 128, 128, 255));
    /// ```
    #[allow(unstable_name_collisions)]
    pub fn factor_add(self, other: Self, factor: f64) -> Self {
        debug_assert!(factor >= 0. && factor <= 1.0);

        let [r, g, b, a] = self.0;
        let [r2, g2, b2, a2] = other.0;

        RgbaColor([
            f64::round((1. - factor) * f64::from(r) + factor * f64::from(r2)).clamp(0., 255.) as u8,
            f64::round((1. - factor) * f64::from(g) + factor * f64::from(g2)).clamp(0., 255.) as u8,
            f64::round((1. - factor) * f64::from(b) + factor * f64::from(b2)).clamp(0., 255.) as u8,
            f64::round((1. - factor) * f64::from(a) + factor * f64::from(a2)).clamp(0., 255.) as u8,
        ])
    }
}

// TODO: use float's clamp function once it is stable
trait Clamp: Sized + PartialOrd {
    /// Restrict a value to a certain interval unless it is NaN.
    /// taken from std-lib nightly
    fn clamp(self, min: Self, max: Self) -> Self {
        assert!(min <= max);
        let mut x = self;
        if x < min {
            x = min;
        }
        if x > max {
            x = max;
        }
        x
    }
}

impl Clamp for f64 {}

impl Into<image::Rgba<u8>> for RgbaColor {
    /// Transform an RgbaColor to its counterpart from the image crate
    fn into(self) -> image::Rgba<u8> {
        let array = self.0;
        image::Rgba([array[0], array[1], array[2], array[3]])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logarithmic_color_table() {
        let colorizer = Colorizer::logarithmic_gradient(
            vec![
                (1.0.into(), RgbaColor::black()),
                (10.0.into(), RgbaColor::white()),
            ],
            RgbaColor::transparent(),
            RgbaColor::transparent(),
        )
        .unwrap();

        let color_table = colorizer.color_table(3, 1., 10.);

        assert_eq!(color_table.len(), 3);

        assert_eq!(color_table[0], RgbaColor::black());
        assert_eq!(color_table[1], RgbaColor::new(189, 189, 189, 255)); // at 5.5
        assert_eq!(color_table[2], RgbaColor::white());
    }

    #[test]
    fn logarithmic_color_table_2() {
        let colorizer = Colorizer::logarithmic_gradient(
            vec![
                (1.0.into(), RgbaColor::black()),
                (51.0.into(), RgbaColor::new(100, 100, 100, 255)),
                (101.0.into(), RgbaColor::white()),
            ],
            RgbaColor::transparent(),
            RgbaColor::transparent(),
        )
        .unwrap();

        let color_table = colorizer.color_table(5, 1., 101.);

        assert_eq!(color_table.len(), 5);

        assert_eq!(color_table[0], RgbaColor::black());

        let v1 = f64::round(0.8286472601695658 * 100.) as u8;
        assert_eq!(color_table[1], RgbaColor::new(v1, v1, v1, 255)); // at 26

        assert_eq!(color_table[2], RgbaColor::new(100, 100, 100, 255));

        let v2 = f64::round(0.5838002256925127 * 255. + (1. - 0.5838002256925127) * 100.) as u8;
        assert_eq!(color_table[3], RgbaColor::new(v2, v2, v2, 255)); // at 76

        assert_eq!(color_table[4], RgbaColor::white());
    }
}
