use snafu::ResultExt;
use warp::reply::Reply;
use warp::{http::Response, Filter};

use crate::error;
use crate::error::Result;
use crate::handlers::Context;
use crate::ogc::wfs::request::{GetCapabilities, GetFeature, TypeNames, WfsRequest};
use crate::users::session::Session;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::{Workflow, WorkflowId};
use futures::StreamExt;
use geoengine_datatypes::collections::ToGeoJson;
use geoengine_datatypes::{
    collections::{FeatureCollection, MultiPointCollection},
    primitives::SpatialResolution,
};
use geoengine_datatypes::{
    primitives::{FeatureData, Geometry, MultiPoint, TimeInstance, TimeInterval},
    spatial_reference::SpatialReference,
};
use geoengine_operators::engine::VectorOperator;
use geoengine_operators::engine::{
    QueryContext, QueryRectangle, ResultDescriptor, TypedVectorQueryProcessor, VectorQueryProcessor,
};
use geoengine_operators::processing::{Reprojection, ReprojectionParams};
use serde_json::json;
use std::str::FromStr;

pub(crate) fn wfs_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("wfs")
        .and(warp::get())
        .and(warp::query::<WfsRequest>())
        .and(warp::any().map(move || ctx.clone()))
        .and_then(wfs)
}

// TODO: move into handler once async closures are available?
async fn wfs<C: Context>(
    request: WfsRequest,
    ctx: C,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: authentication
    // TODO: more useful error output than "invalid query string"
    match request {
        WfsRequest::GetCapabilities(request) => get_capabilities(&request),
        WfsRequest::GetFeature(request) => get_feature(&request, &ctx).await,
        _ => Ok(Box::new(
            warp::http::StatusCode::NOT_IMPLEMENTED.into_response(),
        )),
    }
}

/// Gets details about the web feature service provider and lists available operations.
///
/// # Example
///
/// ```text
/// GET /wfs?request=GetCapabilities
/// Authorization: Bearer e9da345c-b1df-464b-901c-0335a0419227
/// ```
/// Response:
/// ```xml
/// <?xml version="1.0" encoding="UTF-8"?>
/// <wfs:WFS_Capabilities version="2.0.0"
/// xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
/// xmlns="http://www.opengis.net/wfs/2.0"
/// xmlns:wfs="http://www.opengis.net/wfs/2.0"
/// xmlns:ows="http://www.opengis.net/ows/1.1"
/// xmlns:xlink="http://www.w3.org/1999/xlink"
/// xmlns:xs="http://www.w3.org/2001/XMLSchema" xsi:schemaLocation="http://www.opengis.net/wfs/2.0 http://schemas.opengis.net/wfs/2.0/wfs.xsd"
/// xmlns:xml="http://www.w3.org/XML/1998/namespace">
/// <ows:ServiceIdentification>
///   <ows:Title>Geo Engine</ows:Title>
///   <ows:ServiceType>WFS</ows:ServiceType>
///   <ows:ServiceTypeVersion>2.0.0</ows:ServiceTypeVersion>
///   <ows:Fees>NONE</ows:Fees>
///   <ows:AccessConstraints>NONE</ows:AccessConstraints>
/// </ows:ServiceIdentification>
/// <ows:ServiceProvider>
///   <ows:ProviderName>Geo Engine</ows:ProviderName>
///   <ows:ServiceContact>
///     <ows:ContactInfo>
///       <ows:Address>
///         <ows:ElectronicMailAddress>info@geoengine.de</ows:ElectronicMailAddress>
///       </ows:Address>
///     </ows:ContactInfo>
///   </ows:ServiceContact>
/// </ows:ServiceProvider>
/// <ows:OperationsMetadata>
///   <ows:Operation name="GetCapabilities">
///     <ows:DCP>
///       <ows:HTTP>
///         <ows:Get xlink:href="http://localhost/wfs"/>
///         <ows:Post xlink:href="http://localhost/wfs"/>
///       </ows:HTTP>
///     </ows:DCP>
///     <ows:Parameter name="AcceptVersions">
///       <ows:AllowedValues>
///         <ows:Value>2.0.0</ows:Value>
///       </ows:AllowedValues>
///     </ows:Parameter>
///     <ows:Parameter name="AcceptFormats">
///       <ows:AllowedValues>
///         <ows:Value>text/xml</ows:Value>
///       </ows:AllowedValues>
///     </ows:Parameter>
///   </ows:Operation>
///   <ows:Operation name="GetFeature">
///     <ows:DCP>
///       <ows:HTTP>
///         <ows:Get xlink:href="http://localhost/wfs"/>
///         <ows:Post xlink:href="http://localhost/wfs"/>
///       </ows:HTTP>
///     </ows:DCP>
///     <ows:Parameter name="resultType">
///       <ows:AllowedValues>
///         <ows:Value>results</ows:Value>
///         <ows:Value>hits</ows:Value>
///       </ows:AllowedValues>
///     </ows:Parameter>
///     <ows:Parameter name="outputFormat">
///       <ows:AllowedValues>
///         <ows:Value>application/json</ows:Value>
///         <ows:Value>json</ows:Value>
///       </ows:AllowedValues>
///     </ows:Parameter>
///     <ows:Constraint name="PagingIsTransactionSafe">
///       <ows:NoValues/>
///       <ows:DefaultValue>FALSE</ows:DefaultValue>
///     </ows:Constraint>
///   </ows:Operation>
///   <ows:Constraint name="ImplementsBasicWFS">
///     <ows:NoValues/>
///     <ows:DefaultValue>TRUE</ows:DefaultValue>
///   </ows:Constraint>
/// </ows:OperationsMetadata>
/// <FeatureTypeList>
///   <FeatureType>
///     <Name>Test</Name>
///     <Title>Test</Title>
///     <DefaultCRS>urn:ogc:def:crs:EPSG::4326</DefaultCRS>
///     <ows:WGS84BoundingBox>
///       <ows:LowerCorner>-90 -180</ows:LowerCorner>
///       <ows:UpperCorner>90 180</ows:UpperCorner>
///     </ows:WGS84BoundingBox>
///   </FeatureType>
/// </FeatureTypeList>
/// </wfs:WFS_Capabilities>
/// ```
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

/// Retrieves feature data objects.
///
///  # Example
///
/// ```text
/// GET /wfs?request=GetFeature&version=2.0.0&typeNames=test&bbox=1,2,3,4
/// ```
/// Response:
/// ```text
/// {
///   "type": "FeatureCollection",
///   "features": [
///     {
///       "type": "Feature",
///       "geometry": {
///         "type": "Point",
///         "coordinates": [
///           0.0,
///           0.1
///         ]
///       },
///       "properties": {
///         "foo": 0
///       },
///       "when": {
///         "start": "1970-01-01T00:00:00+00:00",
///         "end": "1970-01-01T00:00:00.001+00:00",
///         "type": "Interval"
///       }
///     },
///     {
///       "type": "Feature",
///       "geometry": {
///         "type": "Point",
///         "coordinates": [
///           1.0,
///           1.1
///         ]
///       },
///       "properties": {
///         "foo": null
///       },
///       "when": {
///         "start": "1970-01-01T00:00:00+00:00",
///         "end": "1970-01-01T00:00:00.001+00:00",
///         "type": "Interval"
///       }
///     },
///     {
///       "type": "Feature",
///       "geometry": {
///         "type": "Point",
///         "coordinates": [
///           2.0,
///           3.1
///         ]
///       },
///       "properties": {
///         "foo": 2
///       },
///       "when": {
///         "start": "1970-01-01T00:00:00+00:00",
///         "end": "1970-01-01T00:00:00.001+00:00",
///         "type": "Interval"
///       }
///     },
///     {
///       "type": "Feature",
///       "geometry": {
///         "type": "Point",
///         "coordinates": [
///           3.0,
///           3.1
///         ]
///       },
///       "properties": {
///         "foo": 3
///       },
///       "when": {
///         "start": "1970-01-01T00:00:00+00:00",
///         "end": "1970-01-01T00:00:00.001+00:00",
///         "type": "Interval"
///       }
///     },
///     {
///       "type": "Feature",
///       "geometry": {
///         "type": "Point",
///         "coordinates": [
///           4.0,
///           4.1
///         ]
///       },
///       "properties": {
///         "foo": 4
///       },
///       "when": {
///         "start": "1970-01-01T00:00:00+00:00",
///         "end": "1970-01-01T00:00:00.001+00:00",
///         "type": "Interval"
///       }
///     }
///   ]
/// }
/// ```
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
            return Err(error::Error::InvalidWfsTypeNames.into());
        }
    };

    let operator = workflow.operator.get_vector().context(error::Operator)?;

    // TODO: use correct session when WFS uses authenticated access
    let execution_context = ctx.execution_context(&Session::mock())?;
    let initialized = operator
        .clone()
        .initialize(&execution_context)
        .context(error::Operator)?;

    // handle request and workflow crs matching
    let workflow_spatial_ref: Option<SpatialReference> =
        initialized.result_descriptor().spatial_reference().into();
    let workflow_spatial_ref = workflow_spatial_ref.ok_or(error::Error::InvalidSpatialReference)?;

    // TODO: use a default spatial reference if it is not set?
    let request_spatial_ref: SpatialReference = request
        .srs_name
        .ok_or(error::Error::InvalidSpatialReference)?;

    // perform reprojection if necessary
    let initialized = if request_spatial_ref == workflow_spatial_ref {
        initialized
    } else {
        let proj = Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: request_spatial_ref,
            },
            raster_sources: vec![],
            vector_sources: vec![operator],
        };

        // TODO: avoid re-initialization of the whole operator graph
        Box::new(proj)
            .initialize(&execution_context)
            .context(error::Operator)?
    };

    let processor = initialized.query_processor().context(error::Operator)?;

    let query_rect = QueryRectangle {
        bbox: request.bbox,
        time_interval: request.time.unwrap_or_else(|| {
            let time = TimeInstance::from(chrono::offset::Utc::now());
            TimeInterval::new_unchecked(time, time)
        }),
        spatial_resolution: SpatialResolution::zero_point_one(),
    };
    let query_ctx = ctx.query_context()?;

    let json = match processor {
        TypedVectorQueryProcessor::Data(p) => {
            vector_stream_to_geojson(p, query_rect, &query_ctx).await
        }
        TypedVectorQueryProcessor::MultiPoint(p) => {
            vector_stream_to_geojson(p, query_rect, &query_ctx).await
        }
        TypedVectorQueryProcessor::MultiLineString(p) => {
            vector_stream_to_geojson(p, query_rect, &query_ctx).await
        }
        TypedVectorQueryProcessor::MultiPolygon(p) => {
            vector_stream_to_geojson(p, query_rect, &query_ctx).await
        }
    }?;

    Ok(Box::new(
        Response::builder()
            .header("Content-Type", "application/json")
            .body(json.to_string())
            .context(error::Http)?,
    ))
}

async fn vector_stream_to_geojson<G>(
    processor: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
    query_rect: QueryRectangle,
    query_ctx: &dyn QueryContext,
) -> Result<serde_json::Value>
where
    G: Geometry + 'static,
    for<'c> FeatureCollection<G>: ToGeoJson<'c>,
{
    let features: Vec<serde_json::Value> = Vec::new();

    // TODO: more efficient merging of the partial feature collections
    let stream = processor.vector_query(query_rect, query_ctx)?;

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
            FeatureData::NullableInt(vec![Some(0), None, Some(2), Some(3), Some(4)]),
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
    use super::*;

    use crate::datasets::storage::{DatasetDefinition, DatasetStore};
    use crate::handlers::{handle_rejection, ErrorResponse};
    use crate::users::user::UserId;
    use crate::util::tests::check_allowed_http_methods;
    use crate::util::user_input::UserInput;
    use crate::{contexts::InMemoryContext, workflows::workflow::Workflow};
    use geoengine_datatypes::dataset::DatasetId;
    use geoengine_datatypes::util::Identifier;
    use geoengine_operators::engine::TypedOperator;
    use geoengine_operators::source::CsvSourceParameters;
    use geoengine_operators::source::{CsvGeometrySpecification, CsvSource, CsvTimeSpecification};
    use serde_json::json;
    use std::io::{Seek, SeekFrom, Write};
    use warp::hyper::body::Bytes;
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

    async fn get_capabilities_test_helper(method: &str) -> Response<Bytes> {
        let ctx = InMemoryContext::default();

        warp::test::request()
            .method(method)
            .path("/wfs?request=GetCapabilities&service=WFS")
            .reply(&wfs_handler(ctx).recover(handle_rejection))
            .await
    }

    #[tokio::test]
    async fn get_capabilities() {
        let res = get_capabilities_test_helper("GET").await;

        assert_eq!(res.status(), 200);

        // TODO: validate against schema
        let reader = ParserConfig::default().create_reader(res.body().as_ref());

        for event in reader {
            assert!(event.is_ok());
        }
    }

    #[tokio::test]
    async fn get_capabilities_invalid_method() {
        check_allowed_http_methods(get_capabilities_test_helper, &["GET"]).await;
    }

    async fn get_feature_registry_test_helper(method: &str) -> Response<Bytes> {
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

        warp::test::request()
            .method(method)
            .path(&format!("/wfs?request=GetFeature&service=WFS&version=2.0.0&typeNames=registry:{}&bbox=-90,-180,90,180&srsName=EPSG:4326", id.to_string()))
            .reply(&wfs_handler(ctx).recover(handle_rejection))
            .await
    }

    #[tokio::test]
    async fn get_feature_registry() {
        let res = get_feature_registry_test_helper("GET").await;

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
    async fn get_feature_registry_invalid_method() {
        check_allowed_http_methods(get_feature_registry_test_helper, &["GET"]).await;
    }

    #[tokio::test]
    async fn get_feature_registry_missing_fields() {
        let ctx = InMemoryContext::default();

        let res = warp::test::request()
            .method("GET")
            .path("/wfs?request=GetFeature&service=WFS&version=2.0.0&bbox=-90,-180,90,180&crs=EPSG:4326")
            .reply(&wfs_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(&res, 400, "InvalidQuery", "Invalid query string.");
    }

    async fn get_feature_json_test_helper(method: &str) -> Response<Bytes> {
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
            ("srsName", "EPSG:4326"),
        ];
        let url = format!("/wfs?{}", &serde_urlencoded::to_string(params).unwrap());
        warp::test::request()
            .method(method)
            .path(&url)
            .reply(&wfs_handler(ctx).recover(handle_rejection))
            .await
    }

    #[tokio::test]
    async fn get_feature_json() {
        let res = get_feature_json_test_helper("GET").await;

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
    async fn get_feature_json_invalid_method() {
        check_allowed_http_methods(get_feature_json_test_helper, &["GET"]).await;
    }

    #[tokio::test]
    async fn get_feature_json_missing_fields() {
        let ctx = InMemoryContext::default();

        let params = &[
            ("request", "GetFeature"),
            ("service", "WFS"),
            ("version", "2.0.0"),
            ("bbox", "-90,-180,90,180"),
            ("crs", "EPSG:4326"),
        ];
        let url = format!("/wfs?{}", &serde_urlencoded::to_string(params).unwrap());
        let res = warp::test::request()
            .method("GET")
            .path(&url)
            .reply(&wfs_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(&res, 400, "InvalidQuery", "Invalid query string.");
    }

    async fn add_dataset_definition_to_datasets(
        ctx: &InMemoryContext,
        dataset_definition: &str,
    ) -> DatasetId {
        let def: DatasetDefinition = serde_json::from_str(dataset_definition).unwrap();

        let mut db = ctx.dataset_db_ref_mut().await;

        db.add_dataset(
            UserId::new(),
            def.properties.validated().unwrap(),
            Box::new(def.meta_data),
        )
        .await
        .unwrap()
    }

    fn dir_up() {
        let mut dir = std::env::current_dir().unwrap();
        dir.pop();

        std::env::set_current_dir(dir).unwrap();
    }

    #[tokio::test]
    async fn raster_vector_join() {
        dir_up();

        let ctx = InMemoryContext::default();

        let ndvi_id = add_dataset_definition_to_datasets(
            &ctx,
            include_str!("../../test-data/dataset_defs/ndvi.json"),
        )
        .await;
        let ne_10m_ports_id = add_dataset_definition_to_datasets(
            &ctx,
            include_str!("../../test-data/dataset_defs/points_with_time.json"),
        )
        .await;

        let workflow = serde_json::json!({
            "type": "Vector",
            "operator": {
                "type": "RasterVectorJoin",
                "params": {
                    "names": [
                        "NDVI"
                    ],
                    "aggregation": "first"
                },
                "vectorSources": [
                    {
                        "type": "OgrSource",
                        "params": {
                            "dataset": ne_10m_ports_id,
                            "attributeProjection": null
                        }
                    }
                ],
                "rasterSources": [
                    {
                        "type": "GdalSource",
                        "params": {
                            "dataset": ndvi_id,
                        }
                    }
                ]
            }
        });

        let json = serde_json::to_string(&workflow).unwrap();

        let params = &[
            ("request", "GetFeature"),
            ("service", "WFS"),
            ("version", "2.0.0"),
            ("typeNames", &format!("json:{}", json)),
            ("bbox", "-90,-180,90,180"),
            ("srsName", "EPSG:4326"),
            ("time", "2014-04-01T12:00:00.000Z/2014-04-01T12:00:00.000Z"),
        ];
        let url = format!("/wfs?{}", &serde_urlencoded::to_string(params).unwrap());

        let response = warp::test::request()
            .method("GET")
            .path(&url)
            .reply(&wfs_handler(ctx).recover(handle_rejection))
            .await;

        let body: serde_json::Value = serde_json::from_slice(&response.body().to_vec()).unwrap();

        assert_eq!(
            body,
            serde_json::json!({
                "type": "FeatureCollection",
                "features": [{
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [12.843_159, 47.825_724]
                    },
                    "properties": {
                        "NDVI": 228
                    },
                    "when": {
                        "start": "2014-04-01T00:00:00+00:00",
                        "end": "2014-07-01T00:00:00+00:00",
                        "type": "Interval"
                    }
                }]
            })
        );
    }
}
