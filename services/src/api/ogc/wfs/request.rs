use crate::api::model::datatypes::TimeInterval;
use crate::api::ogc::util::{OgcBoundingBox, parse_ogc_bbox, parse_time_option};
use crate::util::from_str_option;
use geoengine_datatypes::spatial_reference::SpatialReference;
use serde::{Deserialize, Serialize};
use utoipa::openapi::Type;
use utoipa::openapi::schema::{ObjectBuilder, SchemaType};
use utoipa::{IntoParams, PartialSchema, ToSchema};

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub enum WfsRequest {
    GetCapabilities,
    GetFeature,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, ToSchema)]
pub enum WfsService {
    #[serde(rename = "WFS")]
    Wfs,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, ToSchema)]
pub enum WfsVersion {
    #[serde(rename = "2.0.0")]
    V2_0_0,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, IntoParams)]
pub struct GetCapabilities {
    #[serde(alias = "VERSION")]
    pub version: Option<WfsVersion>,
    #[serde(alias = "SERVICE")]
    pub service: WfsService,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct TypeNames {
    pub namespace: Option<String>,
    pub feature_type: String,
}

impl PartialSchema for TypeNames {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        ObjectBuilder::new()
            .schema_type(SchemaType::Type(Type::String))
            .into()
    }
}

impl ToSchema for TypeNames {}

#[derive(PartialEq, Debug, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct GetFeature {
    pub version: Option<WfsVersion>,
    pub service: WfsService,
    #[serde(deserialize_with = "parse_type_names", alias = "typenames")]
    #[param(example = "<Workflow Id>")]
    pub type_names: TypeNames,
    // TODO: fifths parameter can be CRS
    #[serde(deserialize_with = "parse_ogc_bbox")]
    #[param(example = "-90,-180,90,180")]
    pub bbox: OgcBoundingBox,
    #[serde(default)]
    #[serde(deserialize_with = "parse_time_option")]
    #[param(value_type = String, example = "2014-04-01T12:00:00.000Z")]
    pub time: Option<TimeInterval>,
    #[param(example = "EPSG:4326", value_type = Option<String>)]
    #[serde(alias = "srsname")]
    pub srs_name: Option<SpatialReference>,
    pub namespaces: Option<String>, // TODO e.g. xmlns(dog=http://www.example.com/namespaces/dog)
    #[serde(default)]
    #[serde(deserialize_with = "from_str_option")]
    pub count: Option<u64>,
    #[serde(alias = "sortby")]
    pub sort_by: Option<String>, // TODO: Name[+A|+D] (asc/desc)
    #[serde(alias = "resulttype")]
    pub result_type: Option<String>, // TODO: enum: results/hits?
    pub filter: Option<String>, // TODO: parse filters
    #[serde(alias = "propertyname")]
    pub property_name: Option<String>, // TODO comma separated list
                                // TODO: feature_id, ...
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, ToSchema)]
pub enum GetFeatureRequest {
    GetFeature,
}

#[allow(clippy::option_if_let_else)]
pub fn parse_type_names<'de, D>(deserializer: D) -> Result<TypeNames, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    if let Some(pos) = s.find(':') {
        let namespace = Some(s[..pos].to_string());
        let feature_type = s[pos + 1..].to_string();

        Ok(TypeNames {
            namespace,
            feature_type,
        })
    } else {
        Ok(TypeNames {
            namespace: None,
            feature_type: s,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::spatial_reference::SpatialReferenceAuthority;

    #[test]
    fn deserialize_get_feature() {
        let query = "request=GetFeature&service=WFS&version=2.0.0&typeNames=ns:test&bbox=1,2,3,4";
        let parsed: GetFeature = serde_urlencoded::from_str(query).unwrap();

        let request = GetFeature {
            service: WfsService::Wfs,
            version: Some(WfsVersion::V2_0_0),
            time: None,
            srs_name: None,
            namespaces: None,
            count: None,
            sort_by: None,
            result_type: None,
            filter: None,
            bbox: OgcBoundingBox::new(1., 2., 3., 4.),
            type_names: TypeNames {
                namespace: Some("ns".into()),
                feature_type: "test".into(),
            },
            property_name: None,
        };

        assert_eq!(parsed, request);
    }

    #[test]
    fn deserialize_get_feature_full() {
        let params = &[
            ("request", "GetFeature"),
            ("service", "WFS"),
            ("version", "2.0.0"),
            ("typeNames", "ns:test"),
            ("bbox", "1,2,3,4"),
            ("srsName", "EPSG:4326"),
            ("format", "image/png"),
            ("time", "2000-01-01T00:00:00.0Z/2000-01-02T00:00:00.0Z"),
            ("namespaces","xmlns(dog=http://www.example.com/namespaces/dog)"),
            ("count","10"),
            ("sortBy","Name[+A]"),
            ("resultType","results"),
            ("filter","<Filter>
  <And>
    <PropertyIsEqualTo><ValueReference>dog:age</ValueReference><Literal>2</Literal></PropertyIsEqualTo>
    <PropertyIsEqualTo><ValueReference>dog:weight</ValueReference><Literal>5</Literal></PropertyIsEqualTo>
  </And>
</Filter>"),
            ("propertyName","P1,P2"),
        ];
        let query = serde_urlencoded::to_string(params).unwrap();
        let parsed: GetFeature = serde_urlencoded::from_str(&query).unwrap();

        let request =GetFeature {
            service: WfsService::Wfs,
            version: Some(WfsVersion::V2_0_0),
            time: Some(geoengine_datatypes::primitives::TimeInterval::new(946_684_800_000, 946_771_200_000).unwrap().into()),
            srs_name: Some(SpatialReference::new(SpatialReferenceAuthority::Epsg, 4326)),
            namespaces: Some("xmlns(dog=http://www.example.com/namespaces/dog)".into()),
            count: Some(10),
            sort_by: Some("Name[+A]".into()),
            result_type: Some("results".into()),
            filter: Some("<Filter>
  <And>
    <PropertyIsEqualTo><ValueReference>dog:age</ValueReference><Literal>2</Literal></PropertyIsEqualTo>
    <PropertyIsEqualTo><ValueReference>dog:weight</ValueReference><Literal>5</Literal></PropertyIsEqualTo>
  </And>
</Filter>".into()),
            bbox: OgcBoundingBox::new(1., 2., 3., 4.),
            type_names: TypeNames {
                namespace: Some("ns".into()),
                feature_type: "test".into(),
            },
            property_name: Some("P1,P2".into()),
        };

        assert_eq!(parsed, request);
    }

    #[test]
    fn deserialize_url_encoded() {
        let op = r#"{"a":"b"}"#.to_string();

        let params = &[
            ("request", "GetFeature"),
            ("service", "WFS"),
            ("version", "2.0.0"),
            ("typeNames", &format!("json:{op}")),
            ("bbox", "-90,-180,90,180"),
            ("crs", "EPSG:4326"),
        ];
        let url = serde_urlencoded::to_string(params).unwrap();

        let parsed: GetFeature = serde_urlencoded::from_str(&url).unwrap();

        let request = GetFeature {
            service: WfsService::Wfs,
            version: Some(WfsVersion::V2_0_0),
            time: None,
            srs_name: None,
            namespaces: None,
            count: None,
            sort_by: None,
            result_type: None,
            filter: None,
            bbox: OgcBoundingBox::new(-90., -180., 90., 180.),
            type_names: TypeNames {
                namespace: Some("json".into()),
                feature_type: op,
            },
            property_name: None,
        };

        assert_eq!(parsed, request);
    }

    // #[test]
    // fn deserialize_ol_example_request() {
    //     let op = r#"{"a":"b"}"#.to_string();
    //
    //     let params = &[
    //         ("service", "WFS"),
    //         ("version", "2.0.0"),
    //         ("request", "GetFeature"),
    //         ("typeNames", "osm:water_areas"),
    //         ("bbox", "-90,-180,90,180"),
    //         ("crs", "EPSG:4326"),
    //     ];
    //     let url = serde_urlencoded::to_string(params).unwrap();
    //
    //     let parsed: WFSRequest = serde_urlencoded::from_str(&url).unwrap();
    //
    //     let request = WFSRequest::GetFeature(GetFeature {
    //         version: "2.0.0".into(),
    //         time: None,
    //         srs_name: None,
    //         namespaces: None,
    //         count: None,
    //         sort_by: None,
    //         result_type: None,
    //         filter: None,
    //         bbox: BoundingBox2D::new(Coordinate2D::new(-90., -180.), Coordinate2D::new(90., 180.))
    //             .unwrap(),
    //         type_names: TypeNames {
    //             namespace: Some("json".into()),
    //             feature_type: op,
    //         },
    //         property_name: None,
    //     });
    //
    //     assert_eq!(parsed, request);
    // }
}
