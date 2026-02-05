use crate::processes::{RasterOperator, VectorOperator};
use anyhow::Context;
use geoengine_macros::type_tag;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::BTreeMap;
use utoipa::ToSchema;

/// A 2D coordinate with `x` and `y` values.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, PartialOrd, Serialize, Default, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Coordinate2D {
    pub x: f64,
    pub y: f64,
}
impl From<Coordinate2D> for geoengine_datatypes::primitives::Coordinate2D {
    fn from(value: Coordinate2D) -> Self {
        geoengine_datatypes::primitives::Coordinate2D {
            x: value.x,
            y: value.y,
        }
    }
}

impl From<geoengine_datatypes::primitives::Coordinate2D> for Coordinate2D {
    fn from(value: geoengine_datatypes::primitives::Coordinate2D) -> Self {
        Coordinate2D {
            x: value.x,
            y: value.y,
        }
    }
}

/// A raster data type.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum RasterDataType {
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
}

impl From<geoengine_datatypes::raster::RasterDataType> for RasterDataType {
    fn from(value: geoengine_datatypes::raster::RasterDataType) -> Self {
        match value {
            geoengine_datatypes::raster::RasterDataType::U8 => Self::U8,
            geoengine_datatypes::raster::RasterDataType::U16 => Self::U16,
            geoengine_datatypes::raster::RasterDataType::U32 => Self::U32,
            geoengine_datatypes::raster::RasterDataType::U64 => Self::U64,
            geoengine_datatypes::raster::RasterDataType::I8 => Self::I8,
            geoengine_datatypes::raster::RasterDataType::I16 => Self::I16,
            geoengine_datatypes::raster::RasterDataType::I32 => Self::I32,
            geoengine_datatypes::raster::RasterDataType::I64 => Self::I64,
            geoengine_datatypes::raster::RasterDataType::F32 => Self::F32,
            geoengine_datatypes::raster::RasterDataType::F64 => Self::F64,
        }
    }
}

impl From<RasterDataType> for geoengine_datatypes::raster::RasterDataType {
    fn from(value: RasterDataType) -> Self {
        match value {
            RasterDataType::U8 => Self::U8,
            RasterDataType::U16 => Self::U16,
            RasterDataType::U32 => Self::U32,
            RasterDataType::U64 => Self::U64,
            RasterDataType::I8 => Self::I8,
            RasterDataType::I16 => Self::I16,
            RasterDataType::I32 => Self::I32,
            RasterDataType::I64 => Self::I64,
            RasterDataType::F32 => Self::F32,
            RasterDataType::F64 => Self::F64,
        }
    }
}

/// Measurement information for a raster band.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum Measurement {
    Unitless(UnitlessMeasurement),
    Continuous(ContinuousMeasurement),
    Classification(ClassificationMeasurement),
}

impl From<geoengine_datatypes::primitives::Measurement> for Measurement {
    fn from(value: geoengine_datatypes::primitives::Measurement) -> Self {
        match value {
            geoengine_datatypes::primitives::Measurement::Unitless => {
                Self::Unitless(UnitlessMeasurement {
                    r#type: Default::default(),
                })
            }
            geoengine_datatypes::primitives::Measurement::Continuous(cm) => {
                Self::Continuous(cm.into())
            }
            geoengine_datatypes::primitives::Measurement::Classification(cm) => {
                Self::Classification(cm.into())
            }
        }
    }
}

impl From<Measurement> for geoengine_datatypes::primitives::Measurement {
    fn from(value: Measurement) -> Self {
        match value {
            Measurement::Unitless(_) => Self::Unitless,
            Measurement::Continuous(cm) => Self::Continuous(cm.into()),
            Measurement::Classification(cm) => Self::Classification(cm.into()),
        }
    }
}

/// A measurement without a unit.
#[type_tag(value = "unitless")]
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema, Default)]
pub struct UnitlessMeasurement {}

/// A continuous measurement, e.g., "temperature".
/// It may have an optional unit, e.g., "°C" for degrees Celsius.
#[type_tag(value = "continuous")]
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
pub struct ContinuousMeasurement {
    pub measurement: String,
    pub unit: Option<String>,
}

impl From<geoengine_datatypes::primitives::ContinuousMeasurement> for ContinuousMeasurement {
    fn from(value: geoengine_datatypes::primitives::ContinuousMeasurement) -> Self {
        ContinuousMeasurement {
            r#type: Default::default(),
            measurement: value.measurement,
            unit: value.unit,
        }
    }
}

impl From<ContinuousMeasurement> for geoengine_datatypes::primitives::ContinuousMeasurement {
    fn from(value: ContinuousMeasurement) -> Self {
        Self {
            measurement: value.measurement,
            unit: value.unit,
        }
    }
}

impl From<geoengine_datatypes::primitives::ClassificationMeasurement>
    for ClassificationMeasurement
{
    fn from(value: geoengine_datatypes::primitives::ClassificationMeasurement) -> Self {
        let mut classes = BTreeMap::new();
        for (k, v) in value.classes {
            classes.insert(k, v);
        }
        ClassificationMeasurement {
            r#type: Default::default(),
            measurement: value.measurement,
            classes,
        }
    }
}

impl From<ClassificationMeasurement>
    for geoengine_datatypes::primitives::ClassificationMeasurement
{
    fn from(value: ClassificationMeasurement) -> Self {
        geoengine_datatypes::primitives::ClassificationMeasurement {
            measurement: value.measurement,
            classes: value.classes,
        }
    }
}

/// A classification measurement.
/// It contains a mapping from class IDs to class names.
#[type_tag(value = "classification")]
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
pub struct ClassificationMeasurement {
    pub measurement: String,
    #[serde(serialize_with = "serialize_classes")]
    #[serde(deserialize_with = "deserialize_classes")]
    pub classes: BTreeMap<u8, String>,
}

fn serialize_classes<S>(classes: &BTreeMap<u8, String>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    use serde::ser::SerializeMap;

    let mut map = serializer.serialize_map(Some(classes.len()))?;
    for (k, v) in classes {
        map.serialize_entry(&k.to_string(), v)?;
    }
    map.end()
}

fn deserialize_classes<'de, D>(deserializer: D) -> Result<BTreeMap<u8, String>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    use serde::de::{MapAccess, Visitor};
    use std::fmt;

    struct ClassesVisitor;

    impl<'de> Visitor<'de> for ClassesVisitor {
        type Value = BTreeMap<u8, String>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a map with numeric string keys")
        }

        fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            let mut map = BTreeMap::new();
            while let Some((key, value)) = access.next_entry::<String, String>()? {
                let k = key.parse::<u8>().map_err(serde::de::Error::custom)?;
                map.insert(k, value);
            }
            Ok(map)
        }
    }

    deserializer.deserialize_map(ClassesVisitor)
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RasterBandDescriptor {
    pub name: String,
    pub measurement: Measurement,
}

impl From<geoengine_operators::engine::RasterBandDescriptor> for RasterBandDescriptor {
    fn from(value: geoengine_operators::engine::RasterBandDescriptor) -> Self {
        Self {
            name: value.name,
            measurement: value.measurement.into(),
        }
    }
}

impl From<RasterBandDescriptor> for geoengine_operators::engine::RasterBandDescriptor {
    fn from(value: RasterBandDescriptor) -> Self {
        Self {
            name: value.name,
            measurement: value.measurement.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum ColumnNames {
    #[schema(title = "Default")]
    Default,
    #[schema(title = "Suffix")]
    Suffix { values: Vec<String> },
    #[schema(title = "Names")]
    Names { values: Vec<String> },
}

impl From<geoengine_operators::processing::ColumnNames> for ColumnNames {
    fn from(value: geoengine_operators::processing::ColumnNames) -> Self {
        match value {
            geoengine_operators::processing::ColumnNames::Default => ColumnNames::Default,
            geoengine_operators::processing::ColumnNames::Suffix(v) => {
                ColumnNames::Suffix { values: v }
            }
            geoengine_operators::processing::ColumnNames::Names(v) => {
                ColumnNames::Names { values: v }
            }
        }
    }
}

impl From<ColumnNames> for geoengine_operators::processing::ColumnNames {
    fn from(value: ColumnNames) -> Self {
        match value {
            ColumnNames::Default => geoengine_operators::processing::ColumnNames::Default,
            ColumnNames::Suffix { values } => {
                geoengine_operators::processing::ColumnNames::Suffix(values)
            }
            ColumnNames::Names { values } => {
                geoengine_operators::processing::ColumnNames::Names(values)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum FeatureAggregationMethod {
    First,
    Mean,
}

impl From<geoengine_operators::processing::FeatureAggregationMethod> for FeatureAggregationMethod {
    fn from(value: geoengine_operators::processing::FeatureAggregationMethod) -> Self {
        match value {
            geoengine_operators::processing::FeatureAggregationMethod::First => {
                FeatureAggregationMethod::First
            }
            geoengine_operators::processing::FeatureAggregationMethod::Mean => {
                FeatureAggregationMethod::Mean
            }
        }
    }
}

impl From<FeatureAggregationMethod> for geoengine_operators::processing::FeatureAggregationMethod {
    fn from(value: FeatureAggregationMethod) -> Self {
        match value {
            FeatureAggregationMethod::First => {
                geoengine_operators::processing::FeatureAggregationMethod::First
            }
            FeatureAggregationMethod::Mean => {
                geoengine_operators::processing::FeatureAggregationMethod::Mean
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum TemporalAggregationMethod {
    None,
    First,
    Mean,
}

impl From<geoengine_operators::processing::TemporalAggregationMethod>
    for TemporalAggregationMethod
{
    fn from(value: geoengine_operators::processing::TemporalAggregationMethod) -> Self {
        match value {
            geoengine_operators::processing::TemporalAggregationMethod::None => {
                TemporalAggregationMethod::None
            }
            geoengine_operators::processing::TemporalAggregationMethod::First => {
                TemporalAggregationMethod::First
            }
            geoengine_operators::processing::TemporalAggregationMethod::Mean => {
                TemporalAggregationMethod::Mean
            }
        }
    }
}

impl From<TemporalAggregationMethod>
    for geoengine_operators::processing::TemporalAggregationMethod
{
    fn from(value: TemporalAggregationMethod) -> Self {
        match value {
            TemporalAggregationMethod::None => {
                geoengine_operators::processing::TemporalAggregationMethod::None
            }
            TemporalAggregationMethod::First => {
                geoengine_operators::processing::TemporalAggregationMethod::First
            }
            TemporalAggregationMethod::Mean => {
                geoengine_operators::processing::TemporalAggregationMethod::Mean
            }
        }
    }
}

/// Spatial bounds derivation options for the [`MockPointSource`].
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum SpatialBoundsDerive {
    Derive(SpatialBoundsDeriveDerive),
    Bounds(SpatialBoundsDeriveBounds),
    None(SpatialBoundsDeriveNone),
}

impl Default for SpatialBoundsDerive {
    fn default() -> Self {
        SpatialBoundsDerive::None(SpatialBoundsDeriveNone::default())
    }
}

#[type_tag(value = "derive")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, Default)]
pub struct SpatialBoundsDeriveDerive {}

#[type_tag(value = "bounds")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
pub struct SpatialBoundsDeriveBounds {
    #[serde(flatten)]
    pub bounding_box: BoundingBox2D,
}

#[type_tag(value = "none")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, Default)]
pub struct SpatialBoundsDeriveNone {}

impl TryFrom<SpatialBoundsDerive> for geoengine_operators::mock::SpatialBoundsDerive {
    type Error = anyhow::Error;
    fn try_from(value: SpatialBoundsDerive) -> Result<Self, Self::Error> {
        Ok(match value {
            SpatialBoundsDerive::Derive(_) => {
                geoengine_operators::mock::SpatialBoundsDerive::Derive
            }
            SpatialBoundsDerive::Bounds(bounds) => {
                geoengine_operators::mock::SpatialBoundsDerive::Bounds(
                    bounds.bounding_box.try_into()?,
                )
            }
            SpatialBoundsDerive::None(_) => geoengine_operators::mock::SpatialBoundsDerive::None,
        })
    }
}

/// A bounding box that includes all border points.
/// Note: may degenerate to a point!
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BoundingBox2D {
    lower_left_coordinate: Coordinate2D,
    upper_right_coordinate: Coordinate2D,
}

impl TryFrom<BoundingBox2D> for geoengine_datatypes::primitives::BoundingBox2D {
    type Error = anyhow::Error;
    fn try_from(value: BoundingBox2D) -> Result<Self, Self::Error> {
        geoengine_datatypes::primitives::BoundingBox2D::new(
            value.lower_left_coordinate.into(),
            value.upper_right_coordinate.into(),
        )
        .context("invalid bounding box")
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[schema(no_recursion)]
#[serde(rename_all = "camelCase")]
pub struct SingleRasterSource {
    pub raster: RasterOperator,
}

impl TryFrom<SingleRasterSource> for geoengine_operators::engine::SingleRasterSource {
    type Error = anyhow::Error;

    fn try_from(value: SingleRasterSource) -> Result<Self, Self::Error> {
        Ok(Self {
            raster: value.raster.try_into()?,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[schema(no_recursion)]
#[serde(rename_all = "camelCase")]
pub struct SingleVectorMultipleRasterSources {
    pub vector: VectorOperator,
    pub rasters: Vec<RasterOperator>,
}

impl TryFrom<SingleVectorMultipleRasterSources>
    for geoengine_operators::engine::SingleVectorMultipleRasterSources
{
    type Error = anyhow::Error;

    fn try_from(value: SingleVectorMultipleRasterSources) -> Result<Self, Self::Error> {
        Ok(Self {
            vector: value.vector.try_into()?,
            rasters: value
                .rasters
                .into_iter()
                .map(std::convert::TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::float_cmp)] // ok for tests

    use geoengine_datatypes::primitives::AxisAlignedRectangle;

    use super::*;

    #[test]
    fn it_converts_coordinates() {
        let dt = geoengine_datatypes::primitives::Coordinate2D { x: 1.5, y: -2.25 };

        let api: Coordinate2D = dt.into();
        assert_eq!(api.x, 1.5);
        assert_eq!(api.y, -2.25);

        let back: geoengine_datatypes::primitives::Coordinate2D = api.into();
        assert_eq!(back.x, 1.5);
        assert_eq!(back.y, -2.25);
    }

    #[test]
    fn it_converts_raster_data_types() {
        use geoengine_datatypes::raster::RasterDataType as Dt;

        let dt = Dt::F32;
        let api: RasterDataType = dt.into();
        assert_eq!(api, RasterDataType::F32);

        let back: geoengine_datatypes::raster::RasterDataType = api.into();
        assert_eq!(back, Dt::F32);
    }

    #[test]
    fn it_converts_raster_band_descriptors() {
        use geoengine_datatypes::primitives::Measurement;
        use geoengine_operators::engine::RasterBandDescriptor as OpsDesc;

        let ops = OpsDesc {
            name: "band 0".into(),
            measurement: Measurement::Unitless,
        };

        let api: RasterBandDescriptor = ops.clone().into();
        assert_eq!(api.name, "band 0");

        let back: geoengine_operators::engine::RasterBandDescriptor = api.into();
        assert_eq!(back, ops);
    }

    #[test]
    fn it_converts_bounding_boxes() {
        let api_bbox = BoundingBox2D {
            lower_left_coordinate: Coordinate2D { x: 1.0, y: 2.0 },
            upper_right_coordinate: Coordinate2D { x: 3.0, y: 4.0 },
        };

        let dt_bbox: geoengine_datatypes::primitives::BoundingBox2D =
            api_bbox.try_into().expect("it should convert");

        assert_eq!(
            dt_bbox.upper_left(),
            geoengine_datatypes::primitives::Coordinate2D { x: 1.0, y: 4.0 }
        );
        assert_eq!(
            dt_bbox.lower_right(),
            geoengine_datatypes::primitives::Coordinate2D { x: 3.0, y: 2.0 }
        );
    }
}
