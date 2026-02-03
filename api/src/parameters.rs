use geoengine_macros::type_tag;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::BTreeMap;
use utoipa::ToSchema;

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

#[type_tag(value = "unitless")]
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema, Default)]
pub struct UnitlessMeasurement {}

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
        let mut classes = std::collections::HashMap::new();
        for (k, v) in value.classes {
            classes.insert(k, v);
        }
        geoengine_datatypes::primitives::ClassificationMeasurement {
            measurement: value.measurement,
            classes,
        }
    }
}

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

#[cfg(test)]
mod tests {
    #![allow(clippy::float_cmp)] // ok for tests

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
}
