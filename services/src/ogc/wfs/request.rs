use crate::ogc::util::{parse_bbox, parse_spatial_resolution_option, parse_time_option};
use crate::util::from_str_option;
use geoengine_datatypes::primitives::{BoundingBox2D, SpatialResolution, TimeInterval};
use geoengine_datatypes::spatial_reference::SpatialReference;
use serde::{Deserialize, Serialize};

// TODO: ignore case for field names

#[derive(PartialEq, Debug, Deserialize, Serialize)]
#[serde(tag = "request")]
// TODO: evaluate overhead of large enum variant and maybe refactor it
#[allow(clippy::large_enum_variant)]
pub enum WfsRequest {
    GetCapabilities(GetCapabilities),
    DescribeFeatureType(DescribeFeatureType),
    GetFeature(GetFeature),
    LockFeature(LockFeature),
    Transaction(Transaction),
    GetPropertyValue(GetPropertyValue),
    GetFeatureWithLock(GetFeatureWithLock),
    CreateStoredQuery(CreateStoredQuery),
    DropStoredQuery(DropStoredQuery),
    ListStoredQueries(ListStoredQueries),
    DescribeStoredQueries(DescribeStoredQueries),
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct GetCapabilities {
    pub version: Option<String>,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct TypeNames {
    pub namespace: Option<String>,
    pub feature_type: String,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct DescribeFeatureType {
    pub version: String,
    #[serde(deserialize_with = "parse_type_names")]
    pub type_names: TypeNames,
    pub exceptions: String,    // TODO
    pub output_format: String, // TODO
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetFeature {
    pub version: String,
    #[serde(deserialize_with = "parse_type_names")]
    pub type_names: TypeNames,
    // TODO: fifths parameter can be CRS
    #[serde(deserialize_with = "parse_bbox")]
    pub bbox: BoundingBox2D,
    #[serde(default)]
    #[serde(deserialize_with = "parse_time_option")]
    pub time: Option<TimeInterval>,
    pub srs_name: Option<SpatialReference>,
    pub namespaces: Option<String>, // TODO e.g. xmlns(dog=http://www.example.com/namespaces/dog)
    #[serde(default)]
    #[serde(deserialize_with = "from_str_option")]
    pub count: Option<u64>,
    pub sort_by: Option<String>,       // TODO: Name[+A|+D] (asc/desc)
    pub result_type: Option<String>,   // TODO: enum: results/hits?
    pub filter: Option<String>,        // TODO: parse filters
    pub property_name: Option<String>, // TODO comma separated list
    // TODO: feature_id, ...
    /// Vendor parameter for specifying a spatial query resolution
    #[serde(default)]
    #[serde(deserialize_with = "parse_spatial_resolution_option")]
    pub query_resolution: Option<SpatialResolution>,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct LockFeature {
    // TODO
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct Transaction {
    // TODO
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct GetPropertyValue {
    // TODO
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct GetFeatureWithLock {
    // TODO
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct CreateStoredQuery {
    // TODO
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct DropStoredQuery {
    // TODO
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct ListStoredQueries {
    // TODO
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct DescribeStoredQueries {
    // TODO
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
    use geoengine_datatypes::primitives::Coordinate2D;
    use geoengine_datatypes::spatial_reference::SpatialReferenceAuthority;

    #[test]
    fn deserialize_get_feature() {
        let query = "request=GetFeature&service=WFS&version=2.0.0&typeNames=ns:test&bbox=1,2,3,4";
        let parsed: WfsRequest = serde_urlencoded::from_str(query).unwrap();

        let request = WfsRequest::GetFeature(GetFeature {
            version: "2.0.0".into(),
            time: None,
            srs_name: None,
            namespaces: None,
            count: None,
            sort_by: None,
            result_type: None,
            filter: None,
            bbox: BoundingBox2D::new(Coordinate2D::new(1., 2.), Coordinate2D::new(3., 4.)).unwrap(),
            type_names: TypeNames {
                namespace: Some("ns".into()),
                feature_type: "test".into(),
            },
            property_name: None,
            query_resolution: None,
        });

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
            ("queryResolution","0.1,0.1"),
        ];
        let query = serde_urlencoded::to_string(params).unwrap();
        let parsed: WfsRequest = serde_urlencoded::from_str(&query).unwrap();

        let request = WfsRequest::GetFeature(GetFeature {
            version: "2.0.0".into(),
            time: Some(TimeInterval::new(946_684_800_000, 946_771_200_000).unwrap()),
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
            bbox: BoundingBox2D::new(Coordinate2D::new(1., 2.), Coordinate2D::new(3., 4.)).unwrap(),
            type_names: TypeNames {
                namespace: Some("ns".into()),
                feature_type: "test".into(),
            },
            property_name: Some("P1,P2".into()),
            query_resolution: Some(SpatialResolution::zero_point_one()),
        });

        assert_eq!(parsed, request);
    }

    #[test]
    fn deserialize_url_encoded() {
        let op = r#"{"a":"b"}"#.to_string();

        let params = &[
            ("request", "GetFeature"),
            ("service", "WFS"),
            ("version", "2.0.0"),
            ("typeNames", &format!("json:{}", op)),
            ("bbox", "-90,-180,90,180"),
            ("crs", "EPSG:4326"),
        ];
        let url = serde_urlencoded::to_string(params).unwrap();

        let parsed: WfsRequest = serde_urlencoded::from_str(&url).unwrap();

        let request = WfsRequest::GetFeature(GetFeature {
            version: "2.0.0".into(),
            time: None,
            srs_name: None,
            namespaces: None,
            count: None,
            sort_by: None,
            result_type: None,
            filter: None,
            bbox: BoundingBox2D::new(Coordinate2D::new(-90., -180.), Coordinate2D::new(90., 180.))
                .unwrap(),
            type_names: TypeNames {
                namespace: Some("json".into()),
                feature_type: op,
            },
            property_name: None,
            query_resolution: None,
        });

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
