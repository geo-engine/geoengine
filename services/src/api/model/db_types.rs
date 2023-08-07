use super::{
    datatypes::{
        BoundingBox2D, Breakpoint, ClassificationMeasurement, Colorizer, ContinuousMeasurement,
        DateTimeParseFormat, DefaultColors, FeatureDataType, LinearGradient, LogarithmicGradient,
        Measurement, OverUnderColors, Palette, RgbaColor, SpatialReferenceOption, TimeInterval,
        TimeStep, VectorDataType,
    },
    operators::{
        CsvHeader, FormatSpecifics, OgrSourceDatasetTimeType, OgrSourceDurationSpec,
        OgrSourceTimeFormat, PlotResultDescriptor, RasterResultDescriptor, TypedResultDescriptor,
        UnixTimeStampType, VectorColumnInfo, VectorResultDescriptor,
    },
};
use crate::{
    error::Error,
    projects::{
        ColorParam, DerivedColor, DerivedNumber, LineSymbology, NumberParam, PointSymbology,
        PolygonSymbology, RasterSymbology, Symbology,
    },
};
use ordered_float::NotNan;
use postgres_types::{FromSql, ToSql};
use std::collections::HashMap;

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "DefaultColors")]
pub struct DefaultColorsDbType {
    pub default_color: Option<RgbaColor>,
    pub over_color: Option<RgbaColor>,
    pub under_color: Option<RgbaColor>,
}

impl From<&DefaultColors> for DefaultColorsDbType {
    fn from(value: &DefaultColors) -> Self {
        match value {
            DefaultColors::DefaultColor { default_color } => Self {
                default_color: Some(*default_color),
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
    LinearGradient,
    LogarithmicGradient,
    Palette,
    Rgba,
}

impl From<&Colorizer> for ColorizerDbType {
    fn from(value: &Colorizer) -> Self {
        match value {
            Colorizer::LinearGradient(gradient) => ColorizerDbType {
                r#type: ColorizerTypeDbType::LinearGradient,
                breakpoints: Some(gradient.breakpoints.clone()),
                no_data_color: Some(gradient.no_data_color),
                color_fields: Some((&gradient.color_fields).into()),
                default_color: None,
            },
            Colorizer::LogarithmicGradient(gradient) => ColorizerDbType {
                r#type: ColorizerTypeDbType::LogarithmicGradient,
                breakpoints: Some(gradient.breakpoints.clone()),
                no_data_color: Some(gradient.no_data_color),
                color_fields: Some((&gradient.color_fields).into()),
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
                        .iter()
                        .map(|(k, v)| Breakpoint {
                            value: (*k).into(),
                            color: *v,
                        })
                        .collect(),
                ),
                no_data_color: Some(*no_data_color),
                color_fields: None,
                default_color: Some(*default_color),
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

impl From<&ColorParam> for ColorParamDbType {
    fn from(value: &ColorParam) -> Self {
        match value {
            ColorParam::Static { color } => Self {
                color: Some((*color).into()),
                attribute: None,
                colorizer: None,
            },
            ColorParam::Derived(DerivedColor {
                attribute,
                colorizer,
            }) => Self {
                color: None,
                attribute: Some(attribute.clone()),
                colorizer: Some(colorizer.clone()),
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

impl From<&NumberParam> for NumberParamDbType {
    fn from(value: &NumberParam) -> Self {
        match value {
            NumberParam::Static { value } => Self {
                value: Some(*value as i64),
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
                attribute: Some(attribute.clone()),
                factor: Some(*factor),
                default_value: Some(*default_value),
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

impl From<&Symbology> for SymbologyDbType {
    fn from(symbology: &Symbology) -> Self {
        match symbology {
            Symbology::Raster(raster) => SymbologyDbType {
                raster: Some(raster.clone()),
                point: None,
                line: None,
                polygon: None,
            },
            Symbology::Point(point) => SymbologyDbType {
                raster: None,
                point: Some(point.clone()),
                line: None,
                polygon: None,
            },
            Symbology::Line(line) => SymbologyDbType {
                raster: None,
                point: None,
                line: Some(line.clone()),
                polygon: None,
            },
            Symbology::Polygon(polygon) => SymbologyDbType {
                raster: None,
                point: None,
                line: None,
                polygon: Some(polygon.clone()),
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

#[derive(Debug, FromSql, ToSql)]
#[postgres(name = "VectorColumnInfo")]
pub struct VectorColumnInfoDbType {
    pub column: String,
    pub data_type: FeatureDataType,
    pub measurement: Measurement,
}

#[derive(Debug, FromSql, ToSql)]
#[postgres(name = "VectorResultDescriptor")]
pub struct VectorResultDescriptorDbType {
    pub data_type: VectorDataType,
    pub spatial_reference: SpatialReferenceOption,
    pub columns: Vec<VectorColumnInfoDbType>,
    pub time: Option<TimeInterval>,
    pub bbox: Option<BoundingBox2D>,
}

impl From<&VectorResultDescriptor> for VectorResultDescriptorDbType {
    fn from(result_descriptor: &VectorResultDescriptor) -> Self {
        Self {
            data_type: result_descriptor.data_type,
            spatial_reference: result_descriptor.spatial_reference,
            columns: result_descriptor
                .columns
                .iter()
                .map(|(column, info)| VectorColumnInfoDbType {
                    column: column.clone(),
                    data_type: info.data_type,
                    measurement: info.measurement.clone(),
                })
                .collect(),
            time: result_descriptor.time,
            bbox: result_descriptor.bbox,
        }
    }
}

impl TryFrom<VectorResultDescriptorDbType> for VectorResultDescriptor {
    type Error = Error;

    fn try_from(result_descriptor: VectorResultDescriptorDbType) -> Result<Self, Self::Error> {
        Ok(Self {
            data_type: result_descriptor.data_type,
            spatial_reference: result_descriptor.spatial_reference,
            columns: result_descriptor
                .columns
                .into_iter()
                .map(|info| {
                    (
                        info.column,
                        VectorColumnInfo {
                            data_type: info.data_type,
                            measurement: info.measurement,
                        },
                    )
                })
                .collect(),
            time: result_descriptor.time,
            bbox: result_descriptor.bbox,
        })
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "ResultDescriptor")]
pub struct TypedResultDescriptorDbType {
    raster: Option<RasterResultDescriptor>,
    vector: Option<VectorResultDescriptor>,
    plot: Option<PlotResultDescriptor>,
}

impl From<&TypedResultDescriptor> for TypedResultDescriptorDbType {
    fn from(result_descriptor: &TypedResultDescriptor) -> Self {
        match result_descriptor {
            TypedResultDescriptor::Raster(raster) => Self {
                raster: Some(raster.clone()),
                vector: None,
                plot: None,
            },
            TypedResultDescriptor::Vector(vector) => Self {
                raster: None,
                vector: Some(vector.clone()),
                plot: None,
            },
            TypedResultDescriptor::Plot(plot) => Self {
                raster: None,
                vector: None,
                plot: Some(*plot),
            },
        }
    }
}

impl TryFrom<TypedResultDescriptorDbType> for TypedResultDescriptor {
    type Error = Error;

    fn try_from(result_descriptor: TypedResultDescriptorDbType) -> Result<Self, Self::Error> {
        match result_descriptor {
            TypedResultDescriptorDbType {
                raster: Some(raster),
                vector: None,
                plot: None,
            } => Ok(Self::Raster(raster)),
            TypedResultDescriptorDbType {
                raster: None,
                vector: Some(vector),
                plot: None,
            } => Ok(Self::Vector(vector)),
            TypedResultDescriptorDbType {
                raster: None,
                vector: None,
                plot: Some(plot),
            } => Ok(Self::Plot(plot)),
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

#[derive(Debug, PartialEq, ToSql, FromSql)]
pub struct TextTextKeyValue {
    key: String,
    value: String,
}

#[derive(Debug, PartialEq, ToSql, FromSql)]
#[postgres(transparent)]
pub struct HashMapTextTextDbType(pub Vec<TextTextKeyValue>);

impl From<&HashMap<String, String>> for HashMapTextTextDbType {
    fn from(map: &HashMap<String, String>) -> Self {
        Self(
            map.iter()
                .map(|(key, value)| TextTextKeyValue {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        )
    }
}

impl<S: std::hash::BuildHasher + std::default::Default> From<HashMapTextTextDbType>
    for HashMap<String, String, S>
{
    fn from(map: HashMapTextTextDbType) -> Self {
        map.0
            .into_iter()
            .map(|TextTextKeyValue { key, value }| (key, value))
            .collect()
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "OgrSourceTimeFormat")]
pub struct OgrSourceTimeFormatDbType {
    custom: Option<OgrSourceTimeFormatCustomDbType>,
    unix_time_stamp: Option<OgrSourceTimeFormatUnixTimeStampDbType>,
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "OgrSourceTimeFormatCustom")]
pub struct OgrSourceTimeFormatCustomDbType {
    custom_format: DateTimeParseFormat,
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "OgrSourceTimeFormatUnixTimeStamp")]
pub struct OgrSourceTimeFormatUnixTimeStampDbType {
    timestamp_type: UnixTimeStampType,
    fmt: DateTimeParseFormat,
}

impl From<&OgrSourceTimeFormat> for OgrSourceTimeFormatDbType {
    fn from(other: &OgrSourceTimeFormat) -> Self {
        match other {
            OgrSourceTimeFormat::Custom { custom_format } => Self {
                custom: Some(OgrSourceTimeFormatCustomDbType {
                    custom_format: custom_format.clone(),
                }),
                unix_time_stamp: None,
            },
            OgrSourceTimeFormat::UnixTimeStamp {
                timestamp_type,
                fmt,
            } => Self {
                custom: None,
                unix_time_stamp: Some(OgrSourceTimeFormatUnixTimeStampDbType {
                    timestamp_type: *timestamp_type,
                    fmt: fmt.clone(),
                }),
            },
            OgrSourceTimeFormat::Auto => Self {
                custom: None,
                unix_time_stamp: None,
            },
        }
    }
}

impl TryFrom<OgrSourceTimeFormatDbType> for OgrSourceTimeFormat {
    type Error = Error;

    fn try_from(other: OgrSourceTimeFormatDbType) -> Result<Self, Self::Error> {
        match other {
            OgrSourceTimeFormatDbType {
                custom: Some(custom),
                unix_time_stamp: None,
            } => Ok(Self::Custom {
                custom_format: custom.custom_format,
            }),
            OgrSourceTimeFormatDbType {
                custom: None,
                unix_time_stamp: Some(unix_time_stamp),
            } => Ok(Self::UnixTimeStamp {
                timestamp_type: unix_time_stamp.timestamp_type,
                fmt: unix_time_stamp.fmt,
            }),
            OgrSourceTimeFormatDbType {
                custom: None,
                unix_time_stamp: None,
            } => Ok(Self::Auto),
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "OgrSourceDurationSpec")]
pub struct OgrSourceDurationSpecDbType {
    infinite: bool,
    zero: bool,
    value: Option<TimeStep>,
}

impl From<&OgrSourceDurationSpec> for OgrSourceDurationSpecDbType {
    fn from(other: &OgrSourceDurationSpec) -> Self {
        match other {
            OgrSourceDurationSpec::Infinite => Self {
                infinite: true,
                zero: false,
                value: None,
            },
            OgrSourceDurationSpec::Zero => Self {
                infinite: false,
                zero: true,
                value: None,
            },
            OgrSourceDurationSpec::Value(value) => Self {
                infinite: false,
                zero: false,
                value: Some(*value),
            },
        }
    }
}

impl TryFrom<OgrSourceDurationSpecDbType> for OgrSourceDurationSpec {
    type Error = Error;

    fn try_from(other: OgrSourceDurationSpecDbType) -> Result<Self, Self::Error> {
        match other {
            OgrSourceDurationSpecDbType {
                infinite: true,
                zero: false,
                value: None,
            } => Ok(Self::Infinite),
            OgrSourceDurationSpecDbType {
                infinite: false,
                zero: true,
                value: None,
            } => Ok(Self::Zero),
            OgrSourceDurationSpecDbType {
                infinite: false,
                zero: false,
                value: Some(value),
            } => Ok(Self::Value(value)),
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "OgrSourceDatasetTimeType")]
pub struct OgrSourceDatasetTimeTypeDbType {
    start: Option<OgrSourceDatasetTimeTypeStartDbType>,
    start_end: Option<OgrSourceDatasetTimeTypeStartEndDbType>,
    start_duration: Option<OgrSourceDatasetTimeTypeStartDurationDbType>,
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "OgrSourceDatasetTimeTypeStart")]
pub struct OgrSourceDatasetTimeTypeStartDbType {
    start_field: String,
    start_format: OgrSourceTimeFormat,
    duration: OgrSourceDurationSpec,
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "OgrSourceDatasetTimeTypeStartEnd")]
pub struct OgrSourceDatasetTimeTypeStartEndDbType {
    start_field: String,
    start_format: OgrSourceTimeFormat,
    end_field: String,
    end_format: OgrSourceTimeFormat,
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "OgrSourceDatasetTimeTypeStartDuration")]
pub struct OgrSourceDatasetTimeTypeStartDurationDbType {
    start_field: String,
    start_format: OgrSourceTimeFormat,
    duration_field: String,
}

impl From<&OgrSourceDatasetTimeType> for OgrSourceDatasetTimeTypeDbType {
    fn from(other: &OgrSourceDatasetTimeType) -> Self {
        match other {
            OgrSourceDatasetTimeType::None => Self {
                start: None,
                start_end: None,
                start_duration: None,
            },
            OgrSourceDatasetTimeType::Start {
                start_field,
                start_format,
                duration,
            } => Self {
                start: Some(OgrSourceDatasetTimeTypeStartDbType {
                    start_field: start_field.clone(),
                    start_format: start_format.clone(),
                    duration: *duration,
                }),
                start_end: None,
                start_duration: None,
            },
            OgrSourceDatasetTimeType::StartEnd {
                start_field,
                start_format,
                end_field,
                end_format,
            } => Self {
                start: None,
                start_end: Some(OgrSourceDatasetTimeTypeStartEndDbType {
                    start_field: start_field.clone(),
                    start_format: start_format.clone(),
                    end_field: end_field.clone(),
                    end_format: end_format.clone(),
                }),
                start_duration: None,
            },
            OgrSourceDatasetTimeType::StartDuration {
                start_field,
                start_format,
                duration_field,
            } => Self {
                start: None,
                start_end: None,
                start_duration: Some(OgrSourceDatasetTimeTypeStartDurationDbType {
                    start_field: start_field.clone(),
                    start_format: start_format.clone(),
                    duration_field: duration_field.clone(),
                }),
            },
        }
    }
}

impl TryFrom<OgrSourceDatasetTimeTypeDbType> for OgrSourceDatasetTimeType {
    type Error = Error;

    fn try_from(other: OgrSourceDatasetTimeTypeDbType) -> Result<Self, Self::Error> {
        match other {
            OgrSourceDatasetTimeTypeDbType {
                start: None,
                start_end: None,
                start_duration: None,
            } => Ok(Self::None),
            OgrSourceDatasetTimeTypeDbType {
                start: Some(start),
                start_end: None,
                start_duration: None,
            } => Ok(Self::Start {
                start_field: start.start_field,
                start_format: start.start_format,
                duration: start.duration,
            }),
            OgrSourceDatasetTimeTypeDbType {
                start: None,
                start_end: Some(start_end),
                start_duration: None,
            } => Ok(Self::StartEnd {
                start_field: start_end.start_field,
                start_format: start_end.start_format,
                end_field: start_end.end_field,
                end_format: start_end.end_format,
            }),
            OgrSourceDatasetTimeTypeDbType {
                start: None,
                start_end: None,
                start_duration: Some(start_duration),
            } => Ok(Self::StartDuration {
                start_field: start_duration.start_field,
                start_format: start_duration.start_format,
                duration_field: start_duration.duration_field,
            }),
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "FormatSpecifics")]
pub struct FormatSpecificsDbType {
    csv: Option<FormatSpecificsCsvDbType>,
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "FormatSpecificsCsv")]
pub struct FormatSpecificsCsvDbType {
    header: CsvHeader,
}

impl From<&FormatSpecifics> for FormatSpecificsDbType {
    fn from(other: &FormatSpecifics) -> Self {
        match other {
            FormatSpecifics::Csv { header } => Self {
                csv: Some(FormatSpecificsCsvDbType { header: *header }),
            },
        }
    }
}

impl TryFrom<FormatSpecificsDbType> for FormatSpecifics {
    type Error = Error;

    fn try_from(other: FormatSpecificsDbType) -> Result<Self, Self::Error> {
        match other {
            FormatSpecificsDbType {
                csv: Some(FormatSpecificsCsvDbType { header }),
            } => Ok(Self::Csv { header }),
            _ => Err(Error::UnexpectedInvalidDbTypeConversion),
        }
    }
}

/// TODO: describe
// macro_rules! create_enum {
//     (
//         $StructName:ident,
//         $postgresName:literal,
//         $EnumName:ty,
//         ( $( $variantName:ident : $Variant:path => $VariantType:ty ),+ )
//     ) => {
//         #[derive(Debug, ToSql, FromSql)]
//         #[postgres(name = $postgresName)]
//         pub struct $StructName {
//             $(pub $variantName: Option< $VariantType >),+
//         }

//         impl From<& $EnumName > for $StructName {
//             fn from(other: & $EnumName) -> Self {
//                 match other {
//                     $(
//                         $Variant(raster) => Self {
//                             $variantName: Some($variantName.clone()),
//                             ..None
//                         }
//                     ),+
//                 }
//             }
//         }

//         impl TryFrom<$StructName> for $EnumName {
//             type Error = Error;

//             fn try_from(
//                 other: $StructName,
//             ) -> Result<Self, Self::Error> {
//                 match other {
//                     $(
//                         $StructName {
//                             $variantName: Some($variantName),
//                             ..
//                         } => Ok(Self::$Variant($variantName))
//                     ),+,
//                     _ => Err(Error::UnexpectedInvalidDbTypeConversion),
//                 }
//             }
//         }
//     };
// }

// use crate::api::model::{
//     datatypes::{MultiLineString, MultiPoint, MultiPolygon, NoGeometry},
//     operators::TypedGeometry,
// };

// create_enum!(
//     TypedGeometryDbType,
//     "TypedGeometry",
//     TypedGeometry,
//     (
//         data : TypedGeometry::Data => NoGeometry,
//         multi_point : TypedGeometry::MultiPoint => MultiPoint,
//         multi_line_string : TypedGeometry::MultiLineString => MultiLineString,
//         multi_polygon : TypedGeometry::MultiPolygon => MultiPolygon
//     )
// );

/// A macro for quickly implementing `FromSql` and `ToSql` for `$RustType` if there is a `From` and `Into`
/// implementation for another type `$DbType` that already implements it.
///
/// # Usage
///
/// ```rust,ignore
/// delegate_from_to_sql!($RustType, $DbType)
/// ```
///
macro_rules! delegate_from_to_sql {
    ( $RustType:ty, $DbType:ty ) => {
        impl ToSql for $RustType {
            fn to_sql(
                &self,
                ty: &postgres_types::Type,
                w: &mut bytes::BytesMut,
            ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                <$DbType as ToSql>::to_sql(&self.into(), ty, w)
            }

            fn accepts(ty: &postgres_types::Type) -> bool {
                <$DbType as FromSql>::accepts(ty)
            }

            postgres_types::to_sql_checked!();
        }

        impl<'a> FromSql<'a> for $RustType {
            fn from_sql(
                ty: &postgres_types::Type,
                raw: &'a [u8],
            ) -> Result<$RustType, Box<dyn std::error::Error + Sync + Send>> {
                Ok(<$DbType as FromSql>::from_sql(ty, raw)?.try_into()?)
            }

            fn accepts(ty: &postgres_types::Type) -> bool {
                <$DbType as FromSql>::accepts(ty)
            }
        }
    };
}

delegate_from_to_sql!(Colorizer, ColorizerDbType);
delegate_from_to_sql!(ColorParam, ColorParamDbType);
delegate_from_to_sql!(DefaultColors, DefaultColorsDbType);
delegate_from_to_sql!(FormatSpecifics, FormatSpecificsDbType);
delegate_from_to_sql!(Measurement, MeasurementDbType);
delegate_from_to_sql!(NumberParam, NumberParamDbType);
delegate_from_to_sql!(OgrSourceDatasetTimeType, OgrSourceDatasetTimeTypeDbType);
delegate_from_to_sql!(OgrSourceDurationSpec, OgrSourceDurationSpecDbType);
delegate_from_to_sql!(OgrSourceTimeFormat, OgrSourceTimeFormatDbType);
delegate_from_to_sql!(Symbology, SymbologyDbType);
delegate_from_to_sql!(TypedResultDescriptor, TypedResultDescriptorDbType);
delegate_from_to_sql!(VectorResultDescriptor, VectorResultDescriptorDbType);
