use crate::error;
use crate::util::Result;
use ordered_float::NotNan;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::collections::HashMap;
use std::ops::{Add, Mul};

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

    pub fn palette(colors: Palette, no_data_color: RgbaColor) -> Result<Self> {
        ensure!(
            colors.len() > 0,
            error::Colorizer {
                details: "A palette colorizer must have a least one color"
            }
        );

        Ok(Self::Palette {
            colors,
            no_data_color,
        })
    }

    pub fn rgba() -> Self {
        Self::Rgba
    }

    pub fn min_value(&self) -> f64 {
        match self {
            Self::LinearGradient { breakpoints, .. }
            | Self::LogarithmicGradient { breakpoints, .. } => *breakpoints[0].0,
            Self::Palette { .. } | Self::Rgba { .. } => f64::from(std::u8::MIN),
        }
    }

    pub fn max_value(&self) -> f64 {
        match self {
            Self::LinearGradient { breakpoints, .. }
            | Self::LogarithmicGradient { breakpoints, .. } => {
                *breakpoints[breakpoints.len() - 1].0
            }
            Self::Palette { .. } | Self::Rgba { .. } => f64::from(std::u8::MAX),
        }
    }

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

                    prev_color * (1. - fraction) + next_color * fraction
                }
            })
            .collect();

        debug_assert_eq!(color_table.len(), number_of_colors);

        color_table
    }
}

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

impl<'c> ColorMapper<'c> {
    /// Map a raster value to a color from the colorizer
    /// TODO: use Fn-trait once it is stable
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
            ColorMapper::Rgba => {
                let bytes: [u8; 4] = (value.to_bits() as u32).to_le_bytes(); // TODO: to util function (?)
                RgbaColor([bytes[0], bytes[1], bytes[2], bytes[3]])
            }
        }
    }
}

pub type Breakpoints = Vec<(NotNan<f64>, RgbaColor)>;
pub type Palette = HashMap<NotNan<f64>, RgbaColor>;

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
pub struct RgbaColor([u8; 4]);

impl RgbaColor {
    pub fn new(red: u8, green: u8, blue: u8, alpha: u8) -> Self {
        RgbaColor([red, green, blue, alpha])
    }

    pub fn transparent() -> Self {
        RgbaColor::new(0, 0, 0, 0)
    }

    pub fn pink() -> Self {
        RgbaColor::new(255, 0, 255, 255)
    }
}

impl Mul<f64> for RgbaColor {
    type Output = RgbaColor;

    fn mul(self, rhs: f64) -> Self::Output {
        let array = self.0;
        RgbaColor([
            (f64::from(array[0]) * rhs) as u8,
            (f64::from(array[1]) * rhs) as u8,
            (f64::from(array[2]) * rhs) as u8,
            (f64::from(array[3]) * rhs) as u8,
        ])
    }
}

impl Add for RgbaColor {
    type Output = RgbaColor;

    fn add(self, rhs: Self) -> Self::Output {
        let array_a = self.0;
        let array_b = rhs.0;
        RgbaColor([
            array_a[0] + array_b[0],
            array_a[1] + array_b[1],
            array_a[2] + array_b[2],
            array_a[3] + array_b[3],
        ])
    }
}

impl Into<image::Rgba<u8>> for RgbaColor {
    fn into(self) -> image::Rgba<u8> {
        let array = self.0;
        image::Rgba([array[0], array[1], array[2], array[3]])
    }
}
