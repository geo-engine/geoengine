use snafu::ResultExt;
use warp::reply::Reply;
use warp::{http::Response, Filter};

use crate::error;
use crate::error::Result;
use crate::handlers::Context;
use crate::ogc::wfs::request::{GetCapabilities, GetFeature, TypeNames, WFSRequest};
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::{Workflow, WorkflowId};
use futures::StreamExt;
use geoengine_datatypes::collections::ToGeoJson;
use geoengine_datatypes::primitives::{FeatureData, MultiPoint, TimeInstance, TimeInterval};
use geoengine_datatypes::{
    collections::{FeatureCollection, MultiPointCollection},
    primitives::SpatialResolution,
};
use geoengine_operators::engine::{
    MockExecutionContextCreator, QueryContext, QueryRectangle, TypedVectorQueryProcessor,
    VectorQueryProcessor,
};
use serde_json::json;
use std::str::FromStr;

pub fn wfs_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path!("wfs"))
        .and(warp::query::<WFSRequest>())
        .and(warp::any().map(move || ctx.clone()))
        .and_then(wfs)
}

// TODO: move into handler once async closures are available?
async fn wfs<C: Context>(
    request: WFSRequest,
    ctx: C,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: authentication
    // TODO: more useful error output than "invalid query string"
    match request {
        WFSRequest::GetCapabilities(request) => get_capabilities(&request),
        WFSRequest::GetFeature(request) => get_feature(&request, &ctx).await,
        _ => Ok(Box::new(
            warp::http::StatusCode::NOT_IMPLEMENTED.into_response(),
        )),
    }
}

#[allow(clippy::unnecessary_wraps)] // TODO: remove line once implemented fully
fn get_capabilities(_request: &GetCapabilities) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: implement
    // TODO: inject correct url of the instance and return data for the default layer
    let wfs_url = "http://localhost/wfs".to_string();
    let mock = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<wfs:WFS_Capabilities version="2.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns="http://www.opengis.net/wfs/2.0"
    xmlns:wfs="http://www.opengis.net/wfs/2.0"
    xmlns:ows="http://www.opengis.net/ows/1.1"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xs="http://www.w3.org/2001/XMLSchema" xsi:schemaLocation="http://www.opengis.net/wfs/2.0 http://schemas.opengis.net/wfs/2.0/wfs.xsd"
    xmlns:xml="http://www.w3.org/XML/1998/namespace">
    <ows:ServiceIdentification>
        <ows:Title>Geo Engine</ows:Title>     
        <ows:ServiceType>WFS</ows:ServiceType>
        <ows:ServiceTypeVersion>2.0.0</ows:ServiceTypeVersion>
        <ows:Fees>NONE</ows:Fees>
        <ows:AccessConstraints>NONE</ows:AccessConstraints>
    </ows:ServiceIdentification>
    <ows:ServiceProvider>
        <ows:ProviderName>Geo Engine</ows:ProviderName>    
		<ows:ServiceContact>
            <ows:ContactInfo>
                <ows:Address>
                    <ows:ElectronicMailAddress>info@geoengine.de</ows:ElectronicMailAddress>
                </ows:Address>
            </ows:ContactInfo>
        </ows:ServiceContact>		
    </ows:ServiceProvider>
    <ows:OperationsMetadata>
        <ows:Operation name="GetCapabilities">
            <ows:DCP>
                <ows:HTTP>
                    <ows:Get xlink:href="{wfs_url}"/>
                    <ows:Post xlink:href="{wfs_url}"/>
                </ows:HTTP>
            </ows:DCP>
            <ows:Parameter name="AcceptVersions">
                <ows:AllowedValues>
                    <ows:Value>2.0.0</ows:Value>
                </ows:AllowedValues>
            </ows:Parameter>
            <ows:Parameter name="AcceptFormats">
                <ows:AllowedValues>
                    <ows:Value>text/xml</ows:Value>
                </ows:AllowedValues>
            </ows:Parameter>
        </ows:Operation>  
        <ows:Operation name="GetFeature">
            <ows:DCP>
                <ows:HTTP>
                    <ows:Get xlink:href="{wfs_url}"/>
                    <ows:Post xlink:href="{wfs_url}"/>
                </ows:HTTP>
            </ows:DCP>
            <ows:Parameter name="resultType">
                <ows:AllowedValues>
                    <ows:Value>results</ows:Value>
                    <ows:Value>hits</ows:Value>
                </ows:AllowedValues>
            </ows:Parameter>
            <ows:Parameter name="outputFormat">
                <ows:AllowedValues>
                    <ows:Value>application/json</ows:Value>
                    <ows:Value>json</ows:Value>
                </ows:AllowedValues>
            </ows:Parameter>
            <ows:Constraint name="PagingIsTransactionSafe">
                <ows:NoValues/>
                <ows:DefaultValue>FALSE</ows:DefaultValue>
            </ows:Constraint>
        </ows:Operation>     
        <ows:Constraint name="ImplementsBasicWFS">
            <ows:NoValues/>
            <ows:DefaultValue>TRUE</ows:DefaultValue>
        </ows:Constraint>     
    </ows:OperationsMetadata>
    <FeatureTypeList>
        <FeatureType>
            <Name>Test</Name>
            <Title>Test</Title>
            <DefaultCRS>urn:ogc:def:crs:EPSG::4326</DefaultCRS>
            <ows:WGS84BoundingBox>
                <ows:LowerCorner>-90 -180</ows:LowerCorner>
                <ows:UpperCorner>90 180</ows:UpperCorner>
            </ows:WGS84BoundingBox>
        </FeatureType>       
    </FeatureTypeList>
</wfs:WFS_Capabilities>"#,
        wfs_url = wfs_url
    );

    Ok(Box::new(warp::reply::html(mock)))
}

async fn get_feature<C: Context>(
    request: &GetFeature,
    ctx: &C,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: validate request?
    if request.type_names
        == (TypeNames {
            namespace: None,
            feature_type: "test".to_string(),
        })
    {
        return get_feature_mock(request);
    }

    let workflow: Workflow = match request.type_names.namespace.as_deref() {
        Some("registry") => {
            ctx.workflow_registry_ref()
                .await
                .load(&WorkflowId::from_str(&request.type_names.feature_type)?)
                .await?
        }
        Some("json") => {
            serde_json::from_str(&request.type_names.feature_type).context(error::SerdeJson)?
        }
        Some(_) => {
            return Err(error::Error::InvalidNamespace.into());
        }
        None => {
            return Err(error::Error::InvalidWFSTypeNames.into());
        }
    };

    let operator = workflow.operator.get_vector().context(error::Operator)?;

    // TODO: use global context parameters
    let execution_context_creator = MockExecutionContextCreator::default();
    let execution_context = execution_context_creator.context();
    let initialized = operator
        .initialize(&execution_context)
        .context(error::Operator)?;

    let processor = initialized.query_processor().context(error::Operator)?;

    let query_rect = QueryRectangle {
        bbox: request.bbox,
        time_interval: request.time.unwrap_or_else(|| {
            let time = TimeInstance::from(chrono::offset::Utc::now());
            TimeInterval::new_unchecked(time, time)
        }),
        spatial_resolution: SpatialResolution::zero_point_one(),
    };
    let query_ctx = QueryContext {
        // TODO: use production config and test config sizes here
        chunk_byte_size: 1024,
    };

    // TODO: support geojson output for types other than multipoints
    let json = match processor {
        // TypedVectorQueryProcessor::Data(p) => {
        //     vector_stream_to_geojson(p, query_rect, query_ctx).await
        // }
        TypedVectorQueryProcessor::MultiPoint(p) => {
            point_stream_to_geojson(p, query_rect, query_ctx).await
        }
        // TypedVectorQueryProcessor::MultiLineString(p) => {
        //     vector_stream_to_geojson(p, query_rect, query_ctx).await
        // }
        // TypedVectorQueryProcessor::MultiPolygon(p) => {
        //     vector_stream_to_geojson(p, query_rect, query_ctx).await
        // }
        _ => {
            return Ok(Box::new(
                warp::http::StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            ));
        }
    }?;

    Ok(Box::new(
        Response::builder()
            .header("Content-Type", "application/json")
            .body(json.to_string())
            .context(error::HTTP)?,
    ))
}

// TODO: generify function to work with arbitrary FeatureCollection<T>.
//       Currently the problem is the lifetime on the IntoGeometryOptionIterator trait bound
//       that is required for calling to_geo_json on a feature collection
async fn point_stream_to_geojson(
    processor: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<MultiPoint>>>,
    query_rect: QueryRectangle,
    query_ctx: QueryContext,
) -> Result<serde_json::Value> {
    let features: Vec<serde_json::Value> = Vec::new();

    // TODO: more efficient merging of the partial feature collections
    let stream = processor.vector_query(query_rect, query_ctx);

    let features = stream
        .fold(
            Result::<Vec<serde_json::Value>, error::Error>::Ok(features),
            |output, collection| async move {
                match (output, collection) {
                    (Ok(mut output), Ok(collection)) => {
                        // TODO: avoid parsing the generated json
                        let mut json: serde_json::Value =
                            serde_json::from_str(&collection.to_geo_json())
                                .expect("to_geojson is correct");
                        let more_features = json
                            .get_mut("features")
                            .expect("to_geojson is correct")
                            .as_array_mut()
                            .expect("to geojson is correct");

                        output.append(more_features);
                        Ok(output)
                    }
                    (Err(error), _) => Err(error),
                    (_, Err(error)) => Err(error.into()),
                }
            },
        )
        .await?;

    let mut output = json!({
        "type": "FeatureCollection"
    });

    output
        .as_object_mut()
        .expect("as defined")
        .insert("features".into(), serde_json::Value::Array(features));

    Ok(output)
}

#[allow(clippy::unnecessary_wraps)] // TODO: remove line once implemented fully
fn get_feature_mock(_request: &GetFeature) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    let collection = MultiPointCollection::from_data(
        MultiPoint::many(vec![
            (0.0, 0.1),
            (1.0, 1.1),
            (2.0, 3.1),
            (3.0, 3.1),
            (4.0, 4.1),
        ])
        .unwrap(),
        vec![TimeInterval::new_unchecked(0, 1); 5],
        [(
            "foo".to_string(),
            FeatureData::NullableDecimal(vec![Some(0), None, Some(2), Some(3), Some(4)]),
        )]
        .iter()
        .cloned()
        .collect(),
    )
    .unwrap();

    Ok(Box::new(warp::reply::html(collection.to_geo_json())))
}

#[cfg(test)]
mod tests {
    use geoengine_operators::source::CsvSourceParameters;

    use super::*;
    use crate::{contexts::InMemoryContext, workflows::workflow::Workflow};
    use geoengine_operators::engine::TypedOperator;
    use geoengine_operators::source::{CsvGeometrySpecification, CsvSource, CsvTimeSpecification};
    use serde_json::json;
    use std::io::{Seek, SeekFrom, Write};
    use xml::ParserConfig;

    #[tokio::test]
    async fn mock_test() {
        let ctx = InMemoryContext::default();

        let res = warp::test::request()
            .method("GET")
            .path("/wfs?request=GetFeature&service=WFS&version=2.0.0&typeNames=test&bbox=1,2,3,4")
            .reply(&wfs_handler(ctx))
            .await;
        assert_eq!(res.status(), 200);
        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        assert_eq!(
            body,
            json!({
                "type": "FeatureCollection",
                "features": [{
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [0.0, 0.1]
                        },
                        "properties": {
                            "foo": 0
                        },
                        "when": {
                            "start": "1970-01-01T00:00:00+00:00",
                            "end": "1970-01-01T00:00:00.001+00:00",
                            "type": "Interval"
                        }
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [1.0, 1.1]
                        },
                        "properties": {
                            "foo": null
                        },
                        "when": {
                            "start": "1970-01-01T00:00:00+00:00",
                            "end": "1970-01-01T00:00:00.001+00:00",
                            "type": "Interval"
                        }
                    }, {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [2.0, 3.1]
                        },
                        "properties": {
                            "foo": 2
                        },
                        "when": {
                            "start": "1970-01-01T00:00:00+00:00",
                            "end": "1970-01-01T00:00:00.001+00:00",
                            "type": "Interval"
                        }
                    }, {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [3.0, 3.1]
                        },
                        "properties": {
                            "foo": 3
                        },
                        "when": {
                            "start": "1970-01-01T00:00:00+00:00",
                            "end": "1970-01-01T00:00:00.001+00:00",
                            "type": "Interval"
                        }
                    }, {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [4.0, 4.1]
                        },
                        "properties": {
                            "foo": 4
                        },
                        "when": {
                            "start": "1970-01-01T00:00:00+00:00",
                            "end": "1970-01-01T00:00:00.001+00:00",
                            "type": "Interval"
                        }
                    }
                ]
            })
            .to_string()
        );
    }

    #[tokio::test]
    async fn get_capabilities() {
        let ctx = InMemoryContext::default();

        let res = warp::test::request()
            .method("GET")
            .path("/wfs?request=GetCapabilities&service=WFS")
            .reply(&wfs_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        // TODO: validate against schema
        let reader = ParserConfig::default().create_reader(res.body().as_ref());

        for event in reader {
            assert!(event.is_ok());
        }
    }

    #[tokio::test]
    async fn get_feature_registry() {
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        write!(
            temp_file,
            "
x;y
0;1
2;3
4;5
"
        )
        .unwrap();
        temp_file.seek(SeekFrom::Start(0)).unwrap();

        let ctx = InMemoryContext::default();

        let workflow = Workflow {
            operator: TypedOperator::Vector(Box::new(CsvSource {
                params: CsvSourceParameters {
                    file_path: temp_file.path().into(),
                    field_separator: ';',
                    geometry: CsvGeometrySpecification::XY {
                        x: "x".into(),
                        y: "y".into(),
                    },
                    time: CsvTimeSpecification::None,
                },
            })),
        };

        let id = ctx
            .workflow_registry()
            .write()
            .await
            .register(workflow.clone())
            .await
            .unwrap();

        let res = warp::test::request()
            .method("GET")
            .path(&format!("/wfs?request=GetFeature&service=WFS&version=2.0.0&typeNames=registry:{}&bbox=-90,-180,90,180&crs=EPSG:4326", id.to_string()))
            .reply(&wfs_handler(ctx))
            .await;
        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        assert_eq!(
            body,
            json!({
                "type": "FeatureCollection",
                "features": [{
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [0.0, 1.0]
                    },
                    "properties": {},
                    "when": {
                        "start": "-262144-01-01T00:00:00+00:00",
                        "end": "+262143-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }, {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [2.0, 3.0]
                    },
                    "properties": {},
                    "when": {
                        "start": "-262144-01-01T00:00:00+00:00",
                        "end": "+262143-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }, {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [4.0, 5.0]
                    },
                    "properties": {},
                    "when": {
                        "start": "-262144-01-01T00:00:00+00:00",
                        "end": "+262143-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }]
            })
            .to_string()
        );
        assert_eq!(res.status(), 200);
    }

    #[tokio::test]
    async fn get_feature_json() {
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        write!(
            temp_file,
            "
x;y
0;1
2;3
4;5
"
        )
        .unwrap();
        temp_file.seek(SeekFrom::Start(0)).unwrap();

        let ctx = InMemoryContext::default();

        let workflow = Workflow {
            operator: TypedOperator::Vector(Box::new(CsvSource {
                params: CsvSourceParameters {
                    file_path: temp_file.path().into(),
                    field_separator: ';',
                    geometry: CsvGeometrySpecification::XY {
                        x: "x".into(),
                        y: "y".into(),
                    },
                    time: CsvTimeSpecification::None,
                },
            })),
        };

        let json = serde_json::to_string(&workflow).unwrap();

        let params = &[
            ("request", "GetFeature"),
            ("service", "WFS"),
            ("version", "2.0.0"),
            ("typeNames", &format!("json:{}", json)),
            ("bbox", "-90,-180,90,180"),
            ("crs", "EPSG:4326"),
        ];
        let url = format!("/wfs?{}", &serde_urlencoded::to_string(params).unwrap());
        let res = warp::test::request()
            .method("GET")
            .path(&url)
            .reply(&wfs_handler(ctx))
            .await;
        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        assert_eq!(
            body,
            json!({
                "type": "FeatureCollection",
                "features": [{
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [0.0, 1.0]
                    },
                    "properties": {},
                    "when": {
                        "start": "-262144-01-01T00:00:00+00:00",
                        "end": "+262143-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }, {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [2.0, 3.0]
                    },
                    "properties": {},
                    "when": {
                        "start": "-262144-01-01T00:00:00+00:00",
                        "end": "+262143-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }, {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [4.0, 5.0]
                    },
                    "properties": {},
                    "when": {
                        "start": "-262144-01-01T00:00:00+00:00",
                        "end": "+262143-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }]
            })
            .to_string()
        );
        assert_eq!(res.status(), 200);
    }
}
