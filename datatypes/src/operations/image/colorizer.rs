use crate::error::{self, Error};
use crate::operations::image::RgbaTransmutable;
use crate::raster::Pixel;
use crate::util::Result;
use crate::util::test::TestDefault;
use ordered_float::{FloatIsNan, NotNan};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::str::FromStr;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum RasterColorizer {
    #[serde(rename_all = "camelCase")]
    SingleBand {
        band: u32,
        band_colorizer: Colorizer,
    },
    #[serde(rename_all = "camelCase")]
    MultiBand {
        /// The band index of the red channel.
        red_band: u32,

        /// The band index of the green channel.
        green_band: u32,

        /// The band index of the blue channel.
        blue_band: u32,

        #[serde(flatten)]
        rgb_params: RgbParams,
    },
}

impl Eq for RasterColorizer {}

impl From<Colorizer> for RasterColorizer {
    fn from(value: Colorizer) -> Self {
        Self::SingleBand {
            band: 0,
            band_colorizer: value,
        }
    }
}

/// The parameters for the RGBA colorizer
#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RgbParams {
    /// The minimum value for the red channel.
    pub red_min: f64,
    /// The maximum value for the red channel.
    pub red_max: f64,
    /// A scaling factor for the red channel between 0 and 1.
    #[serde(default = "num_traits::One::one")]
    pub red_scale: f64,

    /// The minimum value for the red channel.
    pub green_min: f64,
    /// The maximum value for the red channel.
    pub green_max: f64,
    /// A scaling factor for the green channel between 0 and 1.
    #[serde(default = "num_traits::One::one")]
    pub green_scale: f64,

    /// The minimum value for the red channel.
    pub blue_min: f64,
    /// The maximum value for the red channel.
    pub blue_max: f64,
    /// A scaling factor for the blue channel between 0 and 1.
    #[serde(default = "num_traits::One::one")]
    pub blue_scale: f64,

    /// The color for no data values.
    #[serde(default = "RgbaColor::transparent")]
    pub no_data_color: RgbaColor,
}

/// A colorizer specifies a mapping between raster values and an output image
/// There are different variants that perform different kinds of mapping.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum Colorizer {
    #[serde(rename_all = "camelCase")]
    LinearGradient {
        breakpoints: Breakpoints,
        no_data_color: RgbaColor,
        over_color: RgbaColor,
        under_color: RgbaColor,
    },
    #[serde(rename_all = "camelCase")]
    LogarithmicGradient {
        breakpoints: Breakpoints,
        no_data_color: RgbaColor,
        over_color: RgbaColor,
        under_color: RgbaColor,
    },
    #[serde(rename_all = "camelCase")]
    Palette {
        colors: Palette,
        no_data_color: RgbaColor,
        default_color: RgbaColor,
    },
}

impl Colorizer {
    /// A linear gradient linearly interpolates values within breakpoints of a color table
    pub fn linear_gradient(
        breakpoints: Breakpoints,
        no_data_color: RgbaColor,
        over_color: RgbaColor,
        under_color: RgbaColor,
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
            over_color,
            under_color,
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
        over_color: RgbaColor,
        under_color: RgbaColor,
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
            over_color,
            under_color,
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
    pub fn palette(
        colors: HashMap<NotNan<f64>, RgbaColor>,
        no_data_color: RgbaColor,
        default_color: RgbaColor,
    ) -> Result<Self> {
        ensure!(
            !colors.is_empty() && colors.len() <= 256,
            error::Colorizer {
                details: "A palette colorizer must have a least one color and at most 256 colors"
            }
        );

        Ok(Self::Palette {
            colors: Palette(colors),
            no_data_color,
            default_color,
        })
    }

    /// Returns the minimum value that is covered by this colorizer
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::operations::image::{Colorizer, RgbaColor};
    /// use std::convert::TryInto;
    ///
    /// let colorizer = Colorizer::linear_gradient(
    ///     vec![
    ///         (0.0, RgbaColor::transparent()).try_into().unwrap(),
    ///         (1.0, RgbaColor::transparent()).try_into().unwrap(),
    ///     ],
    ///     RgbaColor::transparent(),
    ///     RgbaColor::transparent(),
    ///     RgbaColor::transparent(),
    /// ).unwrap();
    ///
    /// assert_eq!(colorizer.min_value(), 0.);
    /// ```
    pub fn min_value(&self) -> f64 {
        match self {
            Self::LinearGradient { breakpoints, .. }
            | Self::LogarithmicGradient { breakpoints, .. } => *breakpoints[0].value,
            Self::Palette { .. } => f64::from(u8::MIN),
        }
    }

    /// Returns the maxium value that is covered by this colorizer
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::operations::image::{Colorizer, RgbaColor};
    /// use std::convert::TryInto;
    ///
    /// let colorizer = Colorizer::logarithmic_gradient(
    ///     vec![
    ///         (1.0, RgbaColor::transparent()).try_into().unwrap(),
    ///         (10.0, RgbaColor::transparent()).try_into().unwrap(),
    ///     ],
    ///     RgbaColor::transparent(),
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
                *breakpoints[breakpoints.len() - 1].value
            }
            Self::Palette { .. } => f64::from(u8::MAX),
        }
    }

    /// Returns the no data color of this colorizer
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::operations::image::{Colorizer, RgbaColor};
    /// use std::convert::TryInto;
    ///
    /// let colorizer = Colorizer::linear_gradient(
    ///     vec![
    ///         (1.0, RgbaColor::black()).try_into().unwrap(),
    ///         (10.0, RgbaColor::white()).try_into().unwrap(),
    ///         ],
    ///     RgbaColor::transparent(),
    ///     RgbaColor::transparent(),
    ///     RgbaColor::transparent(),
    /// ).unwrap();
    ///
    /// assert_eq!(colorizer.no_data_color(), RgbaColor::transparent());
    /// ```
    pub fn no_data_color(&self) -> RgbaColor {
        match self {
            Colorizer::LinearGradient { no_data_color, .. }
            | Colorizer::LogarithmicGradient { no_data_color, .. }
            | Colorizer::Palette { no_data_color, .. } => *no_data_color,
        }
    }

    pub fn over_color(&self) -> RgbaColor {
        match self {
            Colorizer::LinearGradient { over_color, .. }
            | Colorizer::LogarithmicGradient { over_color, .. } => *over_color,
            Colorizer::Palette { default_color, .. } => *default_color,
        }
    }

    pub fn under_color(&self) -> RgbaColor {
        match self {
            Colorizer::LinearGradient { under_color, .. }
            | Colorizer::LogarithmicGradient { under_color, .. } => *under_color,
            Colorizer::Palette { default_color, .. } => *default_color,
        }
    }

    /// Creates a function for mapping raster values to colors
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::operations::image::{Colorizer, RgbaColor};
    /// use std::convert::TryInto;
    ///
    /// let linear_colorizer = Colorizer::linear_gradient(
    ///     vec![
    ///         (0.0, RgbaColor::black()).try_into().unwrap(),
    ///         (1.0, RgbaColor::white()).try_into().unwrap(),
    ///     ],
    ///     RgbaColor::transparent(),
    ///     RgbaColor::transparent(),
    ///     RgbaColor::transparent(),
    /// ).unwrap();
    /// let linear_color_mapper = linear_colorizer.create_color_mapper();
    ///
    /// assert_eq!(linear_color_mapper.call(0.5), RgbaColor::new(128, 128, 128, 255));
    ///
    /// let logarithmic_colorizer = Colorizer::logarithmic_gradient(
    ///     vec![
    ///         (1.0, RgbaColor::black()).try_into().unwrap(),
    ///         (10.0, RgbaColor::white()).try_into().unwrap(),
    ///     ],
    ///     RgbaColor::transparent(),
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
                over_color,
                under_color,
            }
            | Self::LogarithmicGradient {
                breakpoints: _,
                no_data_color,
                over_color,
                under_color,
            } => {
                let color_table = self.color_table(COLOR_TABLE_SIZE, min_value, max_value);

                ColorMapper::ColorTable {
                    color_table,
                    min_value,
                    max_value,
                    no_data_color: *no_data_color,
                    over_color: *over_color,
                    under_color: *under_color,
                }
            }
            Self::Palette {
                colors,
                no_data_color,
                default_color,
            } => ColorMapper::ColorMap {
                color_map: colors,
                no_data_color: *no_data_color,
                default_color: *default_color,
            },
        }
    }

    /// Creates a color table of `number_of_colors` colors
    /// This must only be called for colorizers that use breakpoints
    fn color_table(&self, number_of_colors: usize, min: f64, max: f64) -> Vec<RgbaColor> {
        let (Self::LinearGradient { breakpoints, .. }
        | Self::LogarithmicGradient { breakpoints, .. }) = self
        else {
            // return empty color table for potential wrong usage

            debug_assert!(
                false,
                "Must never call `color_table` for types without breakpoints"
            );

            return Vec::new();
        };

        let smallest_breakpoint_value = *breakpoints[0].value;
        let largest_breakpoint_value = *breakpoints[breakpoints.len() - 1].value;

        let step = (max - min) / ((number_of_colors - 1) as f64);

        let mut breakpoint_iter = breakpoints.iter();
        let mut breakpoint_prev = breakpoint_iter.next().expect("must have first entry");
        let mut breakpoint_next = breakpoint_iter.next().expect("must have second entry");

        let color_table: Vec<RgbaColor> = std::iter::successors(Some(min), |v| Some(v + step))
            .take(number_of_colors)
            .map(|value| {
                if value < smallest_breakpoint_value {
                    self.under_color()
                } else if value > largest_breakpoint_value {
                    self.over_color()
                } else {
                    while value > *breakpoint_next.value {
                        breakpoint_prev = breakpoint_next;
                        breakpoint_next = breakpoint_iter
                            .next()
                            .expect("if-condition must ensure this");
                    }

                    let prev_value = *breakpoint_prev.value;
                    let next_value = *breakpoint_next.value;

                    let prev_color = breakpoint_prev.color;
                    let next_color = breakpoint_next.color;

                    let fraction = match self {
                        Self::LinearGradient { .. } => {
                            (value - prev_value) / (next_value - prev_value)
                        }
                        Self::LogarithmicGradient { .. } => {
                            let nominator = f64::log10(value) - f64::log10(prev_value);
                            let denominator = f64::log10(next_value) - f64::log10(prev_value);
                            nominator / denominator
                        }
                        Self::Palette { .. } => unreachable!(), // cf. first match in function
                    };

                    prev_color.factor_add(next_color, fraction)
                }
            })
            .collect();

        debug_assert_eq!(color_table.len(), number_of_colors);

        color_table
    }

    /// Rescales the colorizer to the new `min` and `max` values. It distributes the breakpoints
    /// evenly between the new `min` and `max` values and uses the original colors.
    ///
    /// Returns an error if the type of colorizer is not gradient
    #[allow(clippy::missing_panics_doc)]
    pub fn rescale(&self, min: f64, max: f64) -> Result<Self> {
        ensure!(min < max, error::MinMustBeSmallerThanMax { min, max });

        match self {
            Self::LinearGradient {
                breakpoints,
                no_data_color,
                over_color,
                under_color,
            } => {
                let step = (max - min) / (breakpoints.len() - 1) as f64;

                Self::linear_gradient(
                    breakpoints
                        .iter()
                        .enumerate()
                        .map(|(i, b)| {
                            (
                                (min + i as f64 * step)
                                    .try_into()
                                    .expect("no operand is NaN"),
                                b.color,
                            )
                                .into()
                        })
                        .collect(),
                    *no_data_color,
                    *over_color,
                    *under_color,
                )
            }
            Self::LogarithmicGradient {
                breakpoints,
                no_data_color,
                over_color,
                under_color,
            } => {
                let step = (max - min) / (breakpoints.len() - 1) as f64;

                Self::logarithmic_gradient(
                    breakpoints
                        .iter()
                        .enumerate()
                        .map(|(i, b)| {
                            (
                                (min + i as f64 * step)
                                    .try_into()
                                    .expect("no operand is NaN"),
                                b.color,
                            )
                                .into()
                        })
                        .collect(),
                    *no_data_color,
                    *over_color,
                    *under_color,
                )
            }
            Self::Palette {
                colors: _,
                no_data_color: _,
                default_color: _,
            } => Err(Error::ColorizerRescaleNotSupported {
                colorizer: "palette".to_string(),
            }),
        }
    }
}

impl TestDefault for Colorizer {
    #[allow(clippy::unwrap_used)]
    fn test_default() -> Self {
        Colorizer::linear_gradient(
            vec![
                (1.0, RgbaColor::white()).try_into().unwrap(),
                (2.0, RgbaColor::black()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::white(),
            RgbaColor::black(),
        )
        .unwrap()
    }
}

/// A `ColorMapper` is a function for mapping raster values to colors
pub enum ColorMapper<'c> {
    ColorTable {
        color_table: Vec<RgbaColor>,
        min_value: f64,
        max_value: f64,
        no_data_color: RgbaColor,
        over_color: RgbaColor,
        under_color: RgbaColor,
    },
    ColorMap {
        color_map: &'c Palette,
        no_data_color: RgbaColor,
        default_color: RgbaColor,
    },
    Rgba,
}

// TODO: use Fn-trait once it is stable
impl ColorMapper<'_> {
    /// Map a raster value to a color from the colorizer
    pub fn call<T>(&self, value: T) -> RgbaColor
    where
        T: Pixel + RgbaTransmutable,
    {
        match self {
            ColorMapper::ColorTable {
                color_table,
                min_value,
                max_value,
                no_data_color,
                over_color,
                under_color,
            } => {
                let value: f64 = value.as_();
                if f64::is_nan(value) {
                    *no_data_color
                } else if value < *min_value {
                    *under_color
                } else if value > *max_value {
                    *over_color
                } else {
                    let color_table_factor = (color_table.len() - 1) as f64;
                    let table_entry = f64::round(
                        color_table_factor * ((value - *min_value) / (*max_value - *min_value)),
                    ) as usize;

                    *color_table.get(table_entry).unwrap_or(
                        over_color, // should be unreachable, but would exceed index, so `over_color` is most suitable
                    )
                }
            }

            ColorMapper::ColorMap {
                color_map,
                no_data_color,
                default_color,
            } => {
                if let Ok(value) = NotNan::<f64>::new(value.as_()) {
                    *color_map.0.get(&value).unwrap_or(default_color)
                } else {
                    *no_data_color
                }
            }
            ColorMapper::Rgba => value.transmute_to_rgba(),
        }
    }
}

/// A container type for breakpoints that specify a value to color mapping
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct Breakpoint {
    pub value: NotNan<f64>,
    pub color: RgbaColor,
}

impl From<(NotNan<f64>, RgbaColor)> for Breakpoint {
    fn from(tuple: (NotNan<f64>, RgbaColor)) -> Self {
        Self {
            value: tuple.0,
            color: tuple.1,
        }
    }
}

impl TryFrom<(f64, RgbaColor)> for Breakpoint {
    type Error = FloatIsNan;

    fn try_from(tuple: (f64, RgbaColor)) -> Result<Self, Self::Error> {
        Ok(Self {
            value: NotNan::new(tuple.0)?,
            color: tuple.1,
        })
    }
}

/// A breakpoint is a list of (value, color) tuples.
///
/// It is assumed to be ordered ascending and has at least two entries,
/// although we only check the first and last value for performance reasons.
pub type Breakpoints = Vec<Breakpoint>;

/// A map from value to color
///
/// It is assumed that is has at least one and at most 256 entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "SerializablePalette", into = "SerializablePalette")]
pub struct Palette(HashMap<NotNan<f64>, RgbaColor>);

impl Palette {
    pub fn new(map: HashMap<NotNan<f64>, RgbaColor>) -> Self {
        Self(map)
    }

    pub fn into_inner(self) -> HashMap<NotNan<f64>, RgbaColor> {
        self.0
    }

    pub fn inner(&self) -> &HashMap<NotNan<f64>, RgbaColor> {
        &self.0
    }
}

/// A type that is solely for serde's serializability.
/// You cannot serialize floats as JSON map keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializablePalette(HashMap<String, RgbaColor>);

impl From<Palette> for SerializablePalette {
    fn from(palette: Palette) -> Self {
        Self(
            palette
                .0
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        )
    }
}

impl TryFrom<SerializablePalette> for Palette {
    type Error = <NotNan<f64> as FromStr>::Err;

    fn try_from(palette: SerializablePalette) -> Result<Self, Self::Error> {
        let mut inner = HashMap::<NotNan<f64>, RgbaColor>::with_capacity(palette.0.len());
        for (k, v) in palette.0 {
            inner.insert(k.parse()?, v);
        }
        Ok(Self(inner))
    }
}

/// `RgbaColor` defines a 32 bit RGB color with alpha value
#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
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

    pub fn into_inner(self) -> [u8; 4] {
        self.0
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

    pub fn blue() -> Self {
        RgbaColor::new(0, 0, 255, 255)
    }

    pub fn red() -> Self {
        RgbaColor::new(255, 0, 0, 255)
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
    ///
    /// # Panics
    /// On debug, if factor is not in [0, 1]
    ///
    #[allow(unstable_name_collisions)]
    #[must_use]
    pub fn factor_add(self, other: Self, factor: f64) -> Self {
        debug_assert!((0.0..=1.0).contains(&factor));

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

impl From<RgbaColor> for image::Rgba<u8> {
    /// Transform an `RgbaColor` to its counterpart from the image crate
    fn from(color: RgbaColor) -> image::Rgba<u8> {
        // [r, g, b, a]
        image::Rgba(color.0)
    }
}

impl ToSql for RgbaColor {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        w: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let tuple = self.0.map(i16::from);

        let postgres_types::Kind::Domain(inner_type) = ty.kind() else {
            return Err(Box::new(
                crate::error::Error::UnexpectedInvalidDbTypeConversion,
            ));
        };

        <[i16; 4] as ToSql>::to_sql(&tuple, inner_type, w)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        if ty.name() != "RgbaColor" {
            return false;
        }
        let postgres_types::Kind::Domain(inner_type) = ty.kind() else {
            return false;
        };

        <[i16; 4] as ToSql>::accepts(inner_type)
    }

    postgres_types::to_sql_checked!();
}

impl<'a> FromSql<'a> for RgbaColor {
    fn from_sql(
        ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> Result<RgbaColor, Box<dyn std::error::Error + Sync + Send>> {
        let array_ty = match ty.kind() {
            postgres_types::Kind::Domain(inner_type) => inner_type,
            _ => ty,
        };

        let tuple = <[i16; 4] as FromSql>::from_sql(array_ty, raw)?;

        Ok(RgbaColor(tuple.map(|v| v as u8)))
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        type Target = [i16; 4];

        if <Target as FromSql>::accepts(ty) {
            return true;
        }

        if ty.name() != "RgbaColor" {
            return false;
        }
        let postgres_types::Kind::Domain(inner_type) = ty.kind() else {
            return false;
        };

        <Target as FromSql>::accepts(inner_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryInto;

    #[test]
    fn logarithmic_color_table() {
        let colorizer = Colorizer::logarithmic_gradient(
            vec![
                (1.0, RgbaColor::black()).try_into().unwrap(),
                (10.0, RgbaColor::white()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::white(),
            RgbaColor::black(),
        )
        .unwrap();

        let color_table = colorizer.color_table(3, 1., 10.);

        assert_eq!(color_table.len(), 3);

        assert_eq!(color_table[0], RgbaColor::black());
        assert_eq!(color_table[1], RgbaColor::new(189, 189, 189, 255)); // at 5.5
        assert_eq!(color_table[2], RgbaColor::white());
    }

    #[test]
    fn logarithmic_color_table_with_extrema() {
        let colorizer = Colorizer::logarithmic_gradient(
            vec![
                (5.0, RgbaColor::black()).try_into().unwrap(),
                (15.0, RgbaColor::white()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::red(),
            RgbaColor::blue(),
        )
        .unwrap();

        let color_table = colorizer.color_table(5, 4., 16.);

        assert_eq!(color_table.len(), 5);

        assert_eq!(color_table[0], RgbaColor::blue()); // undercolor, value is below threshold
        assert_eq!(color_table[1], RgbaColor::new(78, 78, 78, 255)); // at 7
        assert_eq!(color_table[2], RgbaColor::new(161, 161, 161, 255)); // at 10
        assert_eq!(color_table[3], RgbaColor::new(222, 222, 222, 255)); // at 13
        assert_eq!(color_table[4], RgbaColor::red()); // overcolor, value is above threshold
    }

    #[test]
    fn logarithmic_color_table_2() {
        let colorizer = Colorizer::logarithmic_gradient(
            vec![
                (1.0, RgbaColor::black()).try_into().unwrap(),
                (51.0, RgbaColor::new(100, 100, 100, 255))
                    .try_into()
                    .unwrap(),
                (101.0, RgbaColor::white()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::white(),
            RgbaColor::black(),
        )
        .unwrap();

        let color_table = colorizer.color_table(5, 1., 101.);

        assert_eq!(color_table.len(), 5);

        assert_eq!(color_table[0], RgbaColor::black());

        let v1 = f64::round(0.828_647_260_169_565_8 * 100.) as u8;
        assert_eq!(color_table[1], RgbaColor::new(v1, v1, v1, 255)); // at 26

        assert_eq!(color_table[2], RgbaColor::new(100, 100, 100, 255));

        let v2 = f64::round(0.583_800_225_692_512_7 * 255. + (1. - 0.583_800_225_692_512_7) * 100.)
            as u8;
        assert_eq!(color_table[3], RgbaColor::new(v2, v2, v2, 255)); // at 76

        assert_eq!(color_table[4], RgbaColor::white());
    }

    #[test]
    fn serialized_palette() {
        let colorizer = Colorizer::palette(
            [
                (1.0.try_into().unwrap(), RgbaColor::white()),
                (2.0.try_into().unwrap(), RgbaColor::black()),
            ]
            .iter()
            .copied()
            .collect(),
            RgbaColor::transparent(),
            RgbaColor::transparent(),
        )
        .unwrap();

        let serialized_colorizer = serde_json::to_value(&colorizer).unwrap();

        assert_eq!(
            serialized_colorizer,
            serde_json::json!({
                "type": "palette",
                "colors": {
                    "1": [255, 255, 255, 255],
                    "2": [0, 0, 0, 255]
                },
                "noDataColor": [0, 0, 0, 0],
                "defaultColor": [0, 0, 0, 0]
            })
        );

        assert_eq!(
            serde_json::from_str::<Colorizer>(&serialized_colorizer.to_string()).unwrap(),
            colorizer
        );
    }

    #[test]
    fn serialized_linear_gradient() {
        let colorizer = Colorizer::linear_gradient(
            vec![
                (1.0, RgbaColor::white()).try_into().unwrap(),
                (2.0, RgbaColor::black()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::white(),
            RgbaColor::black(),
        )
        .unwrap();

        let serialized_colorizer = serde_json::to_value(&colorizer).unwrap();
        assert_eq!(
            serialized_colorizer,
            serde_json::json!({
                "type": "linearGradient",
                "breakpoints": [{
                    "value": 1.0,
                    "color": [255, 255, 255, 255]
                }, {
                    "value": 2.0,
                    "color": [0, 0, 0, 255]
                }],
                "noDataColor": [0, 0, 0, 0],
                "overColor": [255, 255, 255, 255],
                "underColor": [0, 0, 0, 255]
            })
        );

        assert_eq!(
            serde_json::from_str::<Colorizer>(&serialized_colorizer.to_string()).unwrap(),
            colorizer
        );
    }

    #[test]
    fn it_rescales() {
        let colorizer = Colorizer::linear_gradient(
            vec![
                (1.0, RgbaColor::white()).try_into().unwrap(),
                (2.0, RgbaColor::black()).try_into().unwrap(),
                (3.0, RgbaColor::white()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::white(),
            RgbaColor::black(),
        )
        .unwrap();

        let rescaled = colorizer.rescale(0.0, 6.0).unwrap();

        match rescaled {
            Colorizer::LinearGradient { breakpoints, .. } => assert_eq!(
                breakpoints,
                vec![
                    (0.0.try_into().unwrap(), RgbaColor::white()).into(),
                    (3.0.try_into().unwrap(), RgbaColor::black()).into(),
                    (6.0.try_into().unwrap(), RgbaColor::white()).into(),
                ]
            ),
            _ => unreachable!(),
        }
    }
}
