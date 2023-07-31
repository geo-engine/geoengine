use ordered_float::NotNan;
use postgres_types::{FromSql, ToSql};

use crate::{
    error::Error,
    projects::{
        ColorParam, DerivedColor, DerivedNumber, LineSymbology, NumberParam, PointSymbology,
        PolygonSymbology, RasterSymbology, Symbology,
    },
};

use super::datatypes::{
    Breakpoint, Colorizer, DefaultColors, LinearGradient, LogarithmicGradient, OverUnderColors,
    Palette, RgbaColor,
};

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "DefaultColors")]
pub struct DefaultColorsDbType {
    pub default_color: Option<RgbaColor>,
    pub over_color: Option<RgbaColor>,
    pub under_color: Option<RgbaColor>,
}

impl From<DefaultColors> for DefaultColorsDbType {
    fn from(value: DefaultColors) -> Self {
        match value {
            DefaultColors::DefaultColor { default_color } => Self {
                default_color: Some(default_color),
                over_color: None,
                under_color: None,
            },
            DefaultColors::OverUnder(over_under) => Self {
                default_color: None,
                over_color: Some(over_under.over_color),
                under_color: Some(over_under.under_color),
            },
        }
    }
}

impl TryFrom<DefaultColorsDbType> for DefaultColors {
    type Error = Error;

    fn try_from(value: DefaultColorsDbType) -> Result<Self, Self::Error> {
        match (value.default_color, value.over_color, value.under_color) {
            (Some(default_color), None, None) => Ok(Self::DefaultColor { default_color }),
            (None, Some(over_color), Some(under_color)) => Ok(Self::OverUnder(OverUnderColors {
                over_color,
                under_color,
            })),
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "Colorizer")]
pub struct ColorizerDbType {
    r#type: ColorizerTypeDbType,
    breakpoints: Option<Vec<Breakpoint>>,
    no_data_color: Option<RgbaColor>,
    color_fields: Option<DefaultColorsDbType>,
    default_color: Option<RgbaColor>,
}

#[derive(Debug, PartialEq, ToSql, FromSql)]
// TODO: use #[postgres(rename_all = "camelCase")]
#[postgres(name = "ColorizerType")]
pub enum ColorizerTypeDbType {
    #[postgres(name = "linearGradient")]
    LinearGradient,
    #[postgres(name = "logarithmicGradient")]
    LogarithmicGradient,
    #[postgres(name = "palette")]
    Palette,
    #[postgres(name = "rgba")]
    Rgba,
}

impl From<Colorizer> for ColorizerDbType {
    fn from(value: Colorizer) -> Self {
        match value {
            Colorizer::LinearGradient(gradient) => ColorizerDbType {
                r#type: ColorizerTypeDbType::LinearGradient,
                breakpoints: Some(gradient.breakpoints),
                no_data_color: Some(gradient.no_data_color),
                color_fields: Some(gradient.color_fields.into()),
                default_color: None,
            },
            Colorizer::LogarithmicGradient(gradient) => ColorizerDbType {
                r#type: ColorizerTypeDbType::LogarithmicGradient,
                breakpoints: Some(gradient.breakpoints),
                no_data_color: Some(gradient.no_data_color),
                color_fields: Some(gradient.color_fields.into()),
                default_color: None,
            },
            Colorizer::Palette {
                colors,
                no_data_color,
                default_color,
            } => ColorizerDbType {
                r#type: ColorizerTypeDbType::Palette,
                breakpoints: Some(
                    colors
                        .0
                        .into_iter()
                        .map(|(k, v)| Breakpoint {
                            value: k.into(),
                            color: v,
                        })
                        .collect(),
                ),
                no_data_color: Some(no_data_color),
                color_fields: None,
                default_color: Some(default_color),
            },
            Colorizer::Rgba => ColorizerDbType {
                r#type: ColorizerTypeDbType::Rgba,
                breakpoints: None,
                no_data_color: None,
                color_fields: None,
                default_color: None,
            },
        }
    }
}

impl TryFrom<ColorizerDbType> for Colorizer {
    type Error = Error;

    fn try_from(value: ColorizerDbType) -> Result<Self, Self::Error> {
        match value {
            ColorizerDbType {
                r#type: ColorizerTypeDbType::LinearGradient,
                breakpoints: Some(breakpoints),
                no_data_color: Some(no_data_color),
                color_fields: Some(color_fields),
                default_color: None,
            } => Ok(Self::LinearGradient(LinearGradient {
                breakpoints,
                no_data_color,
                color_fields: color_fields.try_into()?,
            })),
            ColorizerDbType {
                r#type: ColorizerTypeDbType::LogarithmicGradient,
                breakpoints: Some(breakpoints),
                no_data_color: Some(no_data_color),
                color_fields: Some(color_fields),
                default_color: None,
            } => Ok(Self::LogarithmicGradient(LogarithmicGradient {
                breakpoints,
                no_data_color,
                color_fields: color_fields.try_into()?,
            })),
            ColorizerDbType {
                r#type: ColorizerTypeDbType::Palette,
                breakpoints: Some(breakpoints),
                no_data_color: Some(no_data_color),
                color_fields: None,
                default_color: Some(default_color),
            } => Ok(Self::Palette {
                colors: Palette(
                    breakpoints
                        .into_iter()
                        .map(|b| (NotNan::<f64>::from(b.value), b.color))
                        .collect(),
                ),
                no_data_color,
                default_color,
            }),
            ColorizerDbType {
                r#type: ColorizerTypeDbType::Rgba,
                breakpoints: None,
                no_data_color: None,
                color_fields: None,
                default_color: None,
            } => Ok(Self::Rgba),
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "ColorParam")]
pub struct ColorParamDbType {
    color: Option<RgbaColor>,
    attribute: Option<String>,
    colorizer: Option<Colorizer>,
}

impl From<ColorParam> for ColorParamDbType {
    fn from(value: ColorParam) -> Self {
        match value {
            ColorParam::Static { color } => Self {
                color: Some(color.into()),
                attribute: None,
                colorizer: None,
            },
            ColorParam::Derived(DerivedColor {
                attribute,
                colorizer,
            }) => Self {
                color: None,
                attribute: Some(attribute),
                colorizer: Some(colorizer),
            },
        }
    }
}

impl TryFrom<ColorParamDbType> for ColorParam {
    type Error = Error;

    fn try_from(value: ColorParamDbType) -> Result<Self, Self::Error> {
        match value {
            ColorParamDbType {
                color: Some(color),
                attribute: None,
                colorizer: None,
            } => Ok(Self::Static {
                color: color.into(),
            }),
            ColorParamDbType {
                color: None,
                attribute: Some(attribute),
                colorizer: Some(colorizer),
            } => Ok(Self::Derived(DerivedColor {
                attribute,
                colorizer,
            })),
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "NumberParam")]
pub struct NumberParamDbType {
    value: Option<i64>,
    attribute: Option<String>,
    factor: Option<f64>,
    default_value: Option<f64>,
}

impl From<NumberParam> for NumberParamDbType {
    fn from(value: NumberParam) -> Self {
        match value {
            NumberParam::Static { value } => Self {
                value: Some(value as i64),
                attribute: None,
                factor: None,
                default_value: None,
            },
            NumberParam::Derived(DerivedNumber {
                attribute,
                factor,
                default_value,
            }) => Self {
                value: None,
                attribute: Some(attribute),
                factor: Some(factor),
                default_value: Some(default_value),
            },
        }
    }
}

impl TryFrom<NumberParamDbType> for NumberParam {
    type Error = Error;

    fn try_from(value: NumberParamDbType) -> Result<Self, Self::Error> {
        match value {
            NumberParamDbType {
                value: Some(value),
                attribute: None,
                factor: None,
                default_value: None,
            } => Ok(Self::Static {
                value: value as usize,
            }),
            NumberParamDbType {
                value: None,
                attribute: Some(attribute),
                factor: Some(factor),
                default_value: Some(default_value),
            } => Ok(Self::Derived(DerivedNumber {
                attribute,
                factor,
                default_value,
            })),
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "Symbology")]
pub struct SymbologyDbType {
    raster: Option<RasterSymbology>,
    point: Option<PointSymbology>,
    line: Option<LineSymbology>,
    polygon: Option<PolygonSymbology>,
}

impl From<Symbology> for SymbologyDbType {
    fn from(symbology: Symbology) -> Self {
        match symbology {
            Symbology::Raster(raster) => SymbologyDbType {
                raster: Some(raster),
                point: None,
                line: None,
                polygon: None,
            },
            Symbology::Point(point) => SymbologyDbType {
                raster: None,
                point: Some(point),
                line: None,
                polygon: None,
            },
            Symbology::Line(line) => SymbologyDbType {
                raster: None,
                point: None,
                line: Some(line),
                polygon: None,
            },
            Symbology::Polygon(polygon) => SymbologyDbType {
                raster: None,
                point: None,
                line: None,
                polygon: Some(polygon),
            },
        }
    }
}

impl TryFrom<SymbologyDbType> for Symbology {
    type Error = Error;

    fn try_from(symbology: SymbologyDbType) -> Result<Self, Self::Error> {
        match symbology {
            SymbologyDbType {
                raster: Some(raster),
                point: None,
                line: None,
                polygon: None,
            } => Ok(Self::Raster(raster)),
            SymbologyDbType {
                raster: None,
                point: Some(point),
                line: None,
                polygon: None,
            } => Ok(Self::Point(point)),
            SymbologyDbType {
                raster: None,
                point: None,
                line: Some(line),
                polygon: None,
            } => Ok(Self::Line(line)),
            SymbologyDbType {
                raster: None,
                point: None,
                line: None,
                polygon: Some(polygon),
            } => Ok(Self::Polygon(polygon)),
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

macro_rules! delegate_from_to_sql {
    ( $normal:ident, $dbtype:ident ) => {
        impl ToSql for $normal {
            fn to_sql(
                &self,
                ty: &postgres_types::Type,
                w: &mut bytes::BytesMut,
            ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                <$dbtype as ToSql>::to_sql(&self.clone().into(), ty, w)
            }

            fn accepts(ty: &postgres_types::Type) -> bool {
                <$dbtype as FromSql>::accepts(ty)
            }

            postgres_types::to_sql_checked!();
        }

        impl<'a> FromSql<'a> for $normal {
            fn from_sql(
                ty: &postgres_types::Type,
                raw: &'a [u8],
            ) -> Result<$normal, Box<dyn std::error::Error + Sync + Send>> {
                Ok(<$dbtype as FromSql>::from_sql(ty, raw)?.try_into()?)
            }

            fn accepts(ty: &postgres_types::Type) -> bool {
                <$dbtype as FromSql>::accepts(ty)
            }
        }
    };
}

delegate_from_to_sql!(ColorParam, ColorParamDbType);
delegate_from_to_sql!(Colorizer, ColorizerDbType);
delegate_from_to_sql!(DefaultColors, DefaultColorsDbType);
delegate_from_to_sql!(NumberParam, NumberParamDbType);
delegate_from_to_sql!(Symbology, SymbologyDbType);
