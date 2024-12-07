use super::{
    ClassificationMeasurement, ContinuousMeasurement, Measurement, MultiLineString, MultiPoint,
    MultiPolygon, NoGeometry, TypedGeometry,
};
use crate::{
    delegate_from_to_sql,
    error::Error,
    operations::image::{Breakpoint, Colorizer, RasterColorizer, RgbParams, RgbaColor},
    util::NotNanF64,
};
use postgres_types::{FromSql, ToSql};
use std::collections::HashMap;

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "Measurement")]
pub struct MeasurementDbType {
    continuous: Option<ContinuousMeasurement>,
    classification: Option<ClassificationMeasurementDbType>,
}

#[derive(Debug, ToSql, FromSql)]
struct SmallintTextKeyValue {
    key: i16,
    value: String,
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "ClassificationMeasurement")]
pub struct ClassificationMeasurementDbType {
    measurement: String,
    classes: Vec<SmallintTextKeyValue>,
}

impl From<&Measurement> for MeasurementDbType {
    fn from(measurement: &Measurement) -> Self {
        match measurement {
            Measurement::Unitless => Self {
                continuous: None,
                classification: None,
            },
            Measurement::Continuous(measurement) => Self {
                continuous: Some(measurement.clone()),
                classification: None,
            },
            Measurement::Classification(measurement) => Self {
                continuous: None,
                classification: Some(ClassificationMeasurementDbType {
                    measurement: measurement.measurement.clone(),
                    classes: measurement
                        .classes
                        .iter()
                        .map(|(key, value)| SmallintTextKeyValue {
                            key: i16::from(*key),
                            value: value.clone(),
                        })
                        .collect(),
                }),
            },
        }
    }
}

impl TryFrom<MeasurementDbType> for Measurement {
    type Error = Error;

    fn try_from(measurement: MeasurementDbType) -> Result<Self, Self::Error> {
        match measurement {
            MeasurementDbType {
                continuous: None,
                classification: None,
            } => Ok(Self::Unitless),
            MeasurementDbType {
                continuous: Some(continuous),
                classification: None,
            } => Ok(Self::Continuous(continuous)),
            MeasurementDbType {
                continuous: None,
                classification: Some(classification),
            } => {
                let mut classes = HashMap::with_capacity(classification.classes.len());
                for SmallintTextKeyValue { key, value } in classification.classes {
                    classes.insert(
                        u8::try_from(key).map_err(|_| Error::UnexpectedInvalidDbTypeConversion)?,
                        value,
                    );
                }

                Ok(Self::Classification(ClassificationMeasurement {
                    measurement: classification.measurement,
                    classes,
                }))
            }
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "TypedGeometry")]
pub struct TypedGeometryDbType {
    data: bool,
    multi_point: Option<MultiPoint>,
    multi_line_string: Option<MultiLineString>,
    multi_polygon: Option<MultiPolygon>,
}

impl From<&TypedGeometry> for TypedGeometryDbType {
    fn from(other: &TypedGeometry) -> Self {
        match other {
            TypedGeometry::Data(_) => Self {
                data: true,
                multi_point: None,
                multi_line_string: None,
                multi_polygon: None,
            },
            TypedGeometry::MultiPoint(points) => Self {
                data: false,
                multi_point: Some(points.clone()),
                multi_line_string: None,
                multi_polygon: None,
            },
            TypedGeometry::MultiLineString(lines) => Self {
                data: false,
                multi_point: None,
                multi_line_string: Some(lines.clone()),
                multi_polygon: None,
            },
            TypedGeometry::MultiPolygon(polygons) => Self {
                data: false,
                multi_point: None,
                multi_line_string: None,
                multi_polygon: Some(polygons.clone()),
            },
        }
    }
}

impl TryFrom<TypedGeometryDbType> for TypedGeometry {
    type Error = Error;

    fn try_from(other: TypedGeometryDbType) -> Result<Self, Self::Error> {
        match other {
            TypedGeometryDbType {
                data: true,
                multi_point: None,
                multi_line_string: None,
                multi_polygon: None,
            } => Ok(TypedGeometry::Data(NoGeometry)),
            TypedGeometryDbType {
                data: false,
                multi_point: Some(multi_point),
                multi_line_string: None,
                multi_polygon: None,
            } => Ok(TypedGeometry::MultiPoint(multi_point)),
            TypedGeometryDbType {
                data: false,
                multi_point: None,
                multi_line_string: Some(multi_line_string),
                multi_polygon: None,
            } => Ok(TypedGeometry::MultiLineString(multi_line_string)),
            TypedGeometryDbType {
                data: false,
                multi_point: None,
                multi_line_string: None,
                multi_polygon: Some(multi_polygon),
            } => Ok(TypedGeometry::MultiPolygon(multi_polygon)),
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, FromSql, ToSql)]
#[postgres(name = "Breakpoint")]
pub struct BreakpointDbType {
    pub value: NotNanF64,
    pub color: RgbaColor,
}

impl From<&Breakpoint> for BreakpointDbType {
    fn from(other: &Breakpoint) -> Self {
        Self {
            value: other.value.into(),
            color: other.color,
        }
    }
}

impl TryFrom<BreakpointDbType> for Breakpoint {
    type Error = Error;

    fn try_from(other: BreakpointDbType) -> Result<Self, Self::Error> {
        Ok(Self {
            value: other.value.into(),
            color: other.color,
        })
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "RasterColorizer")]
pub struct RasterColorizerDbType {
    r#type: RasterColorizerDbTypeType,

    band: Option<i64>,
    band_colorizer: Option<ColorizerDbType>,

    red_band: Option<i64>,
    red_min: Option<f64>,
    red_max: Option<f64>,
    red_scale: Option<f64>,
    green_band: Option<i64>,
    green_min: Option<f64>,
    green_max: Option<f64>,
    green_scale: Option<f64>,
    blue_band: Option<i64>,
    blue_min: Option<f64>,
    blue_max: Option<f64>,
    blue_scale: Option<f64>,
    no_data_color: Option<RgbaColor>,
}

#[derive(Debug, PartialEq, ToSql, FromSql)]
// TODO: use #[postgres(rename_all = "camelCase")]
#[postgres(name = "RasterColorizerType")]
pub enum RasterColorizerDbTypeType {
    SingleBand,
    MultiBand,
}

impl From<&RasterColorizer> for RasterColorizerDbType {
    fn from(value: &RasterColorizer) -> Self {
        match value {
            RasterColorizer::SingleBand {
                band,
                band_colorizer,
            } => Self {
                r#type: RasterColorizerDbTypeType::SingleBand,
                band: Some(i64::from(*band)),
                band_colorizer: Some(band_colorizer.into()),
                red_band: None,
                red_min: None,
                red_max: None,
                red_scale: None,
                green_band: None,
                green_min: None,
                green_max: None,
                green_scale: None,
                blue_band: None,
                blue_min: None,
                blue_max: None,
                blue_scale: None,
                no_data_color: None,
            },
            RasterColorizer::MultiBand {
                red_band,
                green_band,
                blue_band,
                rgb_params: rgba_params,
            } => Self {
                r#type: RasterColorizerDbTypeType::MultiBand,
                band: None,
                band_colorizer: None,
                red_band: Some(i64::from(*red_band)),
                red_min: Some(rgba_params.red_min),
                red_max: Some(rgba_params.red_max),
                red_scale: Some(rgba_params.red_scale),
                green_band: Some(i64::from(*green_band)),
                green_min: Some(rgba_params.green_min),
                green_max: Some(rgba_params.green_max),
                green_scale: Some(rgba_params.green_scale),
                blue_band: Some(i64::from(*blue_band)),
                blue_min: Some(rgba_params.blue_min),
                blue_max: Some(rgba_params.blue_max),
                blue_scale: Some(rgba_params.blue_scale),
                no_data_color: Some(rgba_params.no_data_color),
            },
        }
    }
}

impl TryFrom<RasterColorizerDbType> for RasterColorizer {
    type Error = Error;

    fn try_from(value: RasterColorizerDbType) -> Result<Self, Self::Error> {
        match value {
            RasterColorizerDbType {
                r#type: RasterColorizerDbTypeType::SingleBand,
                band: Some(band),
                band_colorizer: Some(colorizer),
                ..
            } => Ok(Self::SingleBand {
                band: u32::try_from(band).map_err(|_| Error::UnexpectedInvalidDbTypeConversion)?,
                band_colorizer: colorizer.try_into()?,
            }),
            RasterColorizerDbType {
                r#type: RasterColorizerDbTypeType::MultiBand,
                red_band: Some(red_band),
                red_min: Some(red_min),
                red_max: Some(red_max),
                red_scale: Some(red_scale),
                green_band: Some(green_band),
                green_min: Some(green_min),
                green_max: Some(green_max),
                green_scale: Some(green_scale),
                blue_band: Some(blue_band),
                blue_min: Some(blue_min),
                blue_max: Some(blue_max),
                blue_scale: Some(blue_scale),
                no_data_color: Some(no_data_color),
                ..
            } => Ok(Self::MultiBand {
                red_band: u32::try_from(red_band)
                    .map_err(|_| Error::UnexpectedInvalidDbTypeConversion)?,
                green_band: u32::try_from(green_band)
                    .map_err(|_| Error::UnexpectedInvalidDbTypeConversion)?,
                blue_band: u32::try_from(blue_band)
                    .map_err(|_| Error::UnexpectedInvalidDbTypeConversion)?,
                rgb_params: RgbParams {
                    red_min,
                    red_max,
                    red_scale,

                    green_min,
                    green_max,
                    green_scale,

                    blue_min,
                    blue_max,
                    blue_scale,
                    no_data_color,
                },
            }),
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

delegate_from_to_sql!(RasterColorizer, RasterColorizerDbType);

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "Colorizer")]
pub struct ColorizerDbType {
    r#type: ColorizerTypeDbType,
    breakpoints: Option<Vec<Breakpoint>>,
    no_data_color: Option<RgbaColor>,
    over_color: Option<RgbaColor>,
    under_color: Option<RgbaColor>,
    default_color: Option<RgbaColor>,
}

#[derive(Debug, PartialEq, ToSql, FromSql)]
// TODO: use #[postgres(rename_all = "camelCase")]
#[postgres(name = "ColorizerType")]
pub enum ColorizerTypeDbType {
    LinearGradient,
    LogarithmicGradient,
    Palette,
}

impl From<&Colorizer> for ColorizerDbType {
    fn from(value: &Colorizer) -> Self {
        match value {
            Colorizer::LinearGradient {
                breakpoints,
                no_data_color,
                over_color,
                under_color,
            } => ColorizerDbType {
                r#type: ColorizerTypeDbType::LinearGradient,
                breakpoints: Some(breakpoints.clone()),
                no_data_color: Some(*no_data_color),
                over_color: Some(*over_color),
                under_color: Some(*under_color),
                default_color: None,
            },
            Colorizer::LogarithmicGradient {
                breakpoints,
                no_data_color,
                over_color,
                under_color,
            } => ColorizerDbType {
                r#type: ColorizerTypeDbType::LogarithmicGradient,
                breakpoints: Some(breakpoints.clone()),
                no_data_color: Some(*no_data_color),
                over_color: Some(*over_color),
                under_color: Some(*under_color),
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
                        .inner()
                        .iter()
                        .map(|(k, v)| Breakpoint {
                            value: *k,
                            color: *v,
                        })
                        .collect(),
                ),
                no_data_color: Some(*no_data_color),
                over_color: None,
                under_color: None,
                default_color: Some(*default_color),
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
                over_color: Some(over_color),
                under_color: Some(under_color),
                default_color: None,
            } => Ok(Self::LinearGradient {
                breakpoints,
                no_data_color,
                over_color,
                under_color,
            }),
            ColorizerDbType {
                r#type: ColorizerTypeDbType::LogarithmicGradient,
                breakpoints: Some(breakpoints),
                no_data_color: Some(no_data_color),
                over_color: Some(over_color),
                under_color: Some(under_color),
                default_color: None,
            } => Ok(Self::LogarithmicGradient {
                breakpoints,
                no_data_color,
                over_color,
                under_color,
            }),
            ColorizerDbType {
                r#type: ColorizerTypeDbType::Palette,
                breakpoints: Some(breakpoints),
                no_data_color: Some(no_data_color),
                over_color: None,
                under_color: None,
                default_color: Some(default_color),
            } => Ok(Self::palette(
                breakpoints
                    .into_iter()
                    .map(|b| (b.value, b.color))
                    .collect(),
                no_data_color,
                default_color,
            )?),
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

delegate_from_to_sql!(Breakpoint, BreakpointDbType);
delegate_from_to_sql!(Colorizer, ColorizerDbType);
delegate_from_to_sql!(Measurement, MeasurementDbType);
delegate_from_to_sql!(TypedGeometry, TypedGeometryDbType);
