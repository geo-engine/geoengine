use super::{Result, WildliveError, error};
use chrono::{DateTime, Utc};
use geoengine_datatypes::{
    error::BoxedResultExt,
    primitives::{BoundingBox2D, Coordinate2D},
};
use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use std::collections::{BTreeMap, HashMap};
use tracing::debug;
use url::Url;

pub(super) const PAGE_SIZE: usize = 10_000; // TODO: pagination
const MAX_QUERY_LENGTH: usize = 40_000;

#[derive(Debug, serde::Deserialize)]
pub(super) struct Project {
    pub id: String,
    pub name: String,
    pub description: String,
    #[serde(rename = "hasStationsLayouts")]
    pub station_layouts: Vec<String>,
}

#[derive(Debug, PartialEq)]
pub struct ProjectFeature {
    pub id: String,
    pub name: String,
    pub description: String,
    pub geom: BoundingBox2D,
}

#[derive(Debug, PartialEq)]
pub struct StationFeature {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub description: String,
    pub location: String,
    pub geom: Coordinate2D,
}

#[derive(Debug, PartialEq)]
pub struct CaptureFeature {
    pub id: String,
    pub station_setup_id: String,
    pub capture_time_stamp: DateTime<Utc>,
    pub accepted_name_usage_id: String,
    pub vernacular_name: String,
    pub scientific_name: String,
    pub content_url: String,
    pub geom: Coordinate2D,
}

#[derive(Debug, serde::Deserialize, PartialEq)]
struct StationSetup {
    pub id: String,
    pub name: String,
    pub location: String,
    pub description: String,
    #[serde(rename = "decimalLatitude")]
    pub decimal_latitude: f64,
    #[serde(rename = "decimalLongitude")]
    pub decimal_longitude: f64,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ImageObject {
    pub id: String,
    #[serde(rename = "captureTimeStamp")]
    pub capture_time_stamp: String,
    #[serde(rename = "atStation")]
    pub station_setup_id: String,
    #[serde(rename = "hasAnnotations", default)]
    pub annotation_ids: Vec<String>,
    #[serde(rename = "contentUrl")]
    pub content_url: String,
}

#[derive(Debug, serde::Deserialize)]
struct Annotation {
    // pub id: String,
    #[serde(rename = "hasTarget")]
    pub target: AnnotationTarget,
    #[serde(rename = "hasBody")]
    pub body: AnnotationBody,
}

#[derive(Debug, serde::Deserialize)]
struct AnnotationBody {
    #[serde(rename = "acceptedNameUsageID")]
    pub accepted_name_usage_id: String,
    #[serde(rename = "vernacularName")]
    pub vernacular_name: String,
    #[serde(rename = "scientificName")]
    pub scientific_name: String,
}

#[derive(Debug, serde::Deserialize)]
struct AnnotationTarget {
    pub source: String,
}

#[derive(Debug, serde::Deserialize, PartialEq)]
struct QueryResults<T> {
    #[serde(rename = "pageNum")]
    pub page_num: usize,
    #[serde(rename = "pageSize")]
    pub page_size: isize, // can be -1
    pub size: usize,
    #[serde(bound(deserialize = "T: serde::de::Deserialize<'de>"))]
    pub results: Vec<QueryResultContent<T>>,
}

#[derive(Debug, serde::Deserialize, PartialEq)]
struct QueryResultContent<T> {
    pub id: String,
    // pub r#type: String,
    #[serde(bound(deserialize = "T: serde::de::Deserialize<'de>"))]
    pub content: T,
}

impl<T> QueryResultContent<T> {
    fn into_content(self) -> T {
        self.content
    }
}

#[derive(Debug, serde::Deserialize, PartialEq)]
struct StationCoordinate {
    #[serde(rename = "inStationsLayout")]
    pub stations_layout: String,
    #[serde(flatten)]
    pub coordinate: Coordinate,
}

#[derive(Debug, serde::Deserialize, PartialEq)]
struct Coordinate {
    #[serde(rename = "decimalLatitude")]
    pub decimal_latitude: f64,
    #[serde(rename = "decimalLongitude")]
    pub decimal_longitude: f64,
}

impl From<&Coordinate> for Coordinate2D {
    fn from(coordinate: &Coordinate) -> Self {
        Coordinate2D::new(coordinate.decimal_longitude, coordinate.decimal_latitude)
    }
}

pub(super) async fn projects_dataset(
    api_endpoint: &Url,
    api_token: Option<&str>,
) -> Result<Vec<ProjectFeature>> {
    let projects = projects(api_endpoint, api_token).await?;
    let station_coordinates = stations::<StationCoordinate>(
        api_endpoint,
        api_token,
        [
            "/id",
            "/content/inStationsLayout",
            "/content/decimalLatitude",
            "/content/decimalLongitude",
        ]
        .map(ToString::to_string)
        .to_vec(),
        &projects,
    )
    .await?
    .fold(
        HashMap::new(),
        |mut acc: std::collections::HashMap<_, Vec<Coordinate>>, content| {
            acc.entry(content.stations_layout)
                .or_default()
                .push(content.coordinate);
            acc
        },
    );

    let features = crate::util::spawn_blocking(move || {
        let mut features = Vec::<ProjectFeature>::with_capacity(projects.len());

        for project in projects {
            let Some(bbox) = BoundingBox2D::from_coord_iter(
                project
                    .station_layouts
                    .iter()
                    .filter_map(|layout| station_coordinates.get(layout))
                    .flat_map(|coordinates| coordinates.iter().map(Coordinate2D::from)),
            ) else {
                return Err(WildliveError::EmptyProjectBounds {
                    project: project.id,
                });
            };

            features.push(ProjectFeature {
                id: project.id.clone(),
                name: project.name.clone(),
                description: project.description.clone(),
                geom: bbox,
            });
        }

        Ok(features)
    })
    .await
    .boxed_context(error::UnexpectedExecution)??;

    Ok(features)
}

pub(super) async fn projects(api_endpoint: &Url, api_token: Option<&str>) -> Result<Vec<Project>> {
    let mut url = api_endpoint.join("search")?;
    url.query_pairs_mut()
        .append_pair("query", "type:project")
        .append_pair("pageNum", "0")
        .append_pair("pageSize", PAGE_SIZE.to_string().as_str());

    debug!(target: "Query", "{url}");

    let mut request = reqwest::Client::new().get(url);
    if let Some(token) = api_token {
        request = request.bearer_auth(token);
    }

    let response: QueryResults<Project> = request.send().await?.json().await?;

    Ok(response
        .results
        .into_iter()
        .map(|result| result.content)
        .collect())
}

async fn project(api_endpoint: &Url, api_token: Option<&str>, project_id: &str) -> Result<Project> {
    let url = api_endpoint.join(&format!("objects/{project_id}"))?;

    debug!(target: "Query", "{url}");

    let mut request = reqwest::Client::new().get(url);
    if let Some(token) = api_token {
        request = request.bearer_auth(token);
    }

    let response: Project = request.send().await?.json().await?;

    Ok(response)
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub(super) struct SearchRequest {
    pub query: String,
    pub filter: Option<Vec<String>>,
    pub page_num: usize,
    pub page_size: usize,
}

pub(super) async fn project_stations_dataset(
    api_endpoint: &Url,
    api_token: Option<&str>,
    project_id: &str,
) -> Result<Vec<StationFeature>> {
    let mut project = vec![project(api_endpoint, api_token, project_id).await?];
    let stations = stations::<StationSetup>(
        api_endpoint,
        api_token,
        [
            "/id",
            "/content/id",
            "/content/name",
            "/content/location",
            "/content/description",
            "/content/decimalLatitude",
            "/content/decimalLongitude",
        ]
        .map(ToString::to_string)
        .to_vec(),
        &project,
    )
    .await?;

    let project = project.remove(0);
    let features = crate::util::spawn_blocking(move || {
        let mut features = Vec::<StationFeature>::new();

        for station in stations {
            features.push(StationFeature {
                id: station.id.clone(),
                project_id: project.id.clone(),
                name: station.name.clone(),
                description: station.description.clone(),
                location: station.location.clone(),
                geom: Coordinate2D::new(station.decimal_longitude, station.decimal_latitude),
            });
        }

        features
    })
    .await
    .boxed_context(error::UnexpectedExecution)?;

    Ok(features)
}

async fn stations<T>(
    api_endpoint: &Url,
    api_token: Option<&str>,
    fields: Vec<String>,
    projects: &[Project],
) -> Result<impl Iterator<Item = T> + use<T>>
where
    for<'de> T: serde::de::Deserialize<'de>,
    T: std::fmt::Debug + PartialEq + Send + 'static,
{
    let mut station_layouts = projects
        .iter()
        .flat_map(|project| project.station_layouts.iter());
    let stations: Vec<QueryResultContent<T>>;

    let mut query = String::from("(/inStationsLayout:\"");
    if let Some(first) = station_layouts.next() {
        query.push_str(first);
    } else {
        stations = vec![];
        return Ok(stations.into_iter().map(QueryResultContent::into_content));
    }
    for station_layout in station_layouts {
        query.push_str("\" OR /inStationsLayout:\"");
        query.push_str(station_layout);
    }
    query.push_str("\") AND type:StationSetup");

    let url = api_endpoint.join("search")?;

    debug!(target: "Query", "{url}");

    let mut request =
        reqwest::Client::new()
            .post(url)
            .body(serde_json::to_string(&SearchRequest {
                query,
                filter: Some(fields),
                page_num: 0,
                page_size: PAGE_SIZE,
            })?);
    if let Some(token) = api_token {
        request = request.bearer_auth(token);
    }

    let results: QueryResults<T> = request.send().await?.json().await?;
    stations = results.results;

    Ok(stations.into_iter().map(QueryResultContent::into_content))
}

async fn image_objects(
    api_endpoint: &Url,
    api_token: Option<&str>,
    mut stations: impl Iterator<Item = &StationSetup>,
) -> Result<Vec<ImageObject>> {
    let mut query = String::from("type:ImageObject AND (/atStation:\"");
    if let Some(first) = stations.next() {
        query.push_str(&first.id);
    } else {
        return Ok(Vec::new());
    }
    for station in stations {
        query.push_str("\" OR /atStation:\"");
        query.push_str(&station.id);
    }
    query.push_str("\")");

    let url = api_endpoint.join("search")?;

    debug!(target: "Query", "{url}\n{query}");

    let mut request =
        reqwest::Client::new()
            .post(url)
            .body(serde_json::to_string(&SearchRequest {
                query,
                filter: Some(
                    [
                        "/id",
                        "/content/id",
                        "/content/captureTimeStamp",
                        "/content/atStation",
                        "/content/hasAnnotations",
                        "/content/contentUrl",
                    ]
                    .map(ToString::to_string)
                    .to_vec(),
                ),
                page_num: 0,
                page_size: PAGE_SIZE,
            })?);
    if let Some(token) = api_token {
        request = request.bearer_auth(token);
    }

    let results: QueryResults<ImageObject> = request.send().await?.json().await?;
    let images = results.results;

    Ok(images
        .into_iter()
        .map(QueryResultContent::into_content)
        .collect())
}

/// Creates a subquery for the given result type and lookup path.
///
/// Stops when it exceeds the length threshold [`MAX_QUERY_LENGTH`].
fn subquery(result_type: &str, lookup_path: &str, lookup_ids: &mut Vec<&str>) -> String {
    const END_BRACE: &str = "\")";
    const SINGLE_ESCAPE_OVERHEAD: usize = 2; // two quotes for each id

    let Some(first_lookup_id) = lookup_ids.pop() else {
        return String::new();
    };

    let mut query = format!("type:{result_type} AND ({lookup_path}:\"");
    let mut escape_overhead = 0; // escape cost for quotes when turned into JSON
    query.push_str(first_lookup_id);
    while let Some(lookup_id) = lookup_ids.last() {
        let initial_query_length = query.len();

        query.push_str("\" OR ");
        query.push_str(lookup_path);
        query.push_str(":\"");
        query.push_str(lookup_id);

        escape_overhead += SINGLE_ESCAPE_OVERHEAD;

        if query.len() + END_BRACE.len() + escape_overhead > MAX_QUERY_LENGTH {
            // revert last addition
            query.truncate(initial_query_length);
            break;
        }

        lookup_ids.pop(); // consume: we already peeked
    }
    query.push_str(END_BRACE);

    query
}

async fn image_annotations(
    api_endpoint: &Url,
    api_token: Option<&str>,
    image_objects: &[ImageObject],
) -> Result<impl Iterator<Item = Annotation> + use<>> {
    // ) -> Result<HashMap<String, Annotation>> {
    // Note: Not collecting into a string vector but re-using a peekable iterator leads to
    // strange lifetime issues (implementation of `…` is not general enough) in async context
    // when `.await`s where called in-between
    let mut annotation_ids = image_objects
        .iter()
        .flat_map(|image_object| image_object.annotation_ids.iter())
        .map(AsRef::as_ref)
        .collect::<Vec<&str>>();

    let url = api_endpoint.join("search")?;
    let mut annotations: Vec<Vec<QueryResultContent<Annotation>>> = Vec::new();

    while !annotation_ids.is_empty() {
        let query = subquery("Annotation", "/id", &mut annotation_ids);
        if query.is_empty() {
            break;
        }

        debug!(target: "Query", "{url}\n{query}");

        let mut request = reqwest::Client::new()
            .post(url.clone())
            .body(serde_json::to_string(&SearchRequest {
                query,
                filter: Some(
                    [
                        "/id",
                        "/content/id",
                        "/content/hasTarget",
                        "/content/hasBody",
                    ]
                    .map(ToString::to_string)
                    .to_vec(),
                ),
                page_num: 0,
                page_size: PAGE_SIZE,
            })?);
        if let Some(token) = &api_token {
            request = request.bearer_auth(token);
        }

        let reponse = request.send().await?;
        let results: QueryResults<Annotation> = reponse.json().await?;
        annotations.push(results.results);
    }

    Ok(annotations
        .into_iter()
        .flat_map(std::iter::IntoIterator::into_iter)
        .map(QueryResultContent::into_content))
}

pub(super) async fn captures_dataset(
    api_endpoint: &Url,
    api_token: Option<&str>,
    project_id: &str,
) -> Result<Vec<CaptureFeature>> {
    let project = project(api_endpoint, api_token, project_id).await?;
    let stations: BTreeMap<_, _> = stations::<StationSetup>(
        api_endpoint,
        api_token,
        [
            "/id",
            "/content/id",
            "/content/name",
            "/content/location",
            "/content/description",
            "/content/decimalLatitude",
            "/content/decimalLongitude",
        ]
        .map(ToString::to_string)
        .to_vec(),
        &[project],
    )
    .await?
    .map(|station| (station.id.clone(), station))
    .collect();
    let image_objects = image_objects(api_endpoint, api_token, stations.values()).await?;

    let image_annotations = image_annotations(api_endpoint, api_token, &image_objects)
        .await?
        .fold(HashMap::new(), |mut map, annotation| {
            // take first entry, ignore others
            map.entry(annotation.target.source.clone())
                .or_insert(annotation);
            map
        });

    let mut captures = Vec::with_capacity(image_objects.len());
    for image_object in image_objects {
        let station = stations
            .get(&image_object.station_setup_id)
            .context(error::UnableToLookupStation)?;

        // dbg!(&image_object.id, image_annotations.keys());
        let (accepted_name_usage_id, vernacular_name, scientific_name) =
            if let Some(annotation) = image_annotations.get(&image_object.id) {
                (
                    annotation.body.accepted_name_usage_id.clone(),
                    annotation.body.vernacular_name.clone(),
                    annotation.body.scientific_name.clone(),
                )
            } else {
                (String::new(), String::new(), String::new())
            };

        captures.push(CaptureFeature {
            id: image_object.id,
            station_setup_id: image_object.station_setup_id,
            capture_time_stamp: image_object
                .capture_time_stamp
                .parse()
                .boxed_context(error::InvalidCaptureTimeStamp)?,
            accepted_name_usage_id,
            vernacular_name,
            scientific_name,
            content_url: image_object.content_url.clone(),
            geom: Coordinate2D::new(station.decimal_longitude, station.decimal_latitude),
        });
    }

    Ok(captures)
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::test_data;
    use httptest::{Expectation, all_of, matchers, matchers::request, responders};
    use pretty_assertions::assert_eq;
    use std::path::Path;

    #[tokio::test]
    async fn it_downloads_a_projects_dataset() {
        let mock_server = httptest::Server::run();

        mock_server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path("/api/search"),
                request::query(matchers::url_decoded(matchers::contains((
                    "query",
                    "type:project"
                )))),
            ])
            .respond_with(json_responder(test_data!(
                "wildlive/responses/projects.json"
            ))),
        );

        mock_server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::path("/api/search"),
                request::body(matchers::json_decoded(matchers::eq(SearchRequest {
                    query: "(/inStationsLayout:\"wildlive/667cc39364fd45136c7a\" OR /inStationsLayout:\"wildlive/151c43fdd5881eba0bd5\") AND type:StationSetup".to_string(),
                    filter: Some(
                        [
                            "/id",
                            "/content/inStationsLayout",
                            "/content/decimalLatitude",
                            "/content/decimalLongitude",
                        ]
                        .map(ToString::to_string)
                        .to_vec(),
                    ),
                    page_num: 0,
                    page_size: PAGE_SIZE,
                }))
            ),
            ])
            .respond_with(json_responder(test_data!(
                "wildlive/responses/station_coordinates.json"
            ))),
        );

        // let api_endpoint = Url::parse("https://wildlive.senckenberg.de/api/").unwrap();
        let api_endpoint = Url::parse(&mock_server.url_str("/api/")).unwrap();

        // crate::util::tests::initialize_debugging_in_test();

        let dataset = projects_dataset(&api_endpoint, None).await.unwrap();

        assert_eq!(
            dataset,
            vec![ProjectFeature {
                id: "wildlive/ef7833589d61b2d2a905".to_string(),
                name: "CameraTrapping project in Bolivia".to_string(),
                description: "Research project for Jaguar wildlife monitoring".to_string(),
                geom: BoundingBox2D::new(
                    Coordinate2D::new(-62.0, -16.4),
                    Coordinate2D::new(-61.9, -16.3)
                )
                .unwrap(),
            }]
        );
    }

    fn json_responder(path: &Path) -> impl httptest::responders::Responder + use<> {
        let json = std::fs::read_to_string(path).unwrap();
        responders::status_code(200)
            .append_header("Content-Type", "application/json")
            .body(json)
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_downloads_a_project_stations_dataset() {
        let mock_server = httptest::Server::run();

        mock_server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path("/api/objects/wildlive/ef7833589d61b2d2a905"),
            ])
            .respond_with(json_responder(test_data!(
                "wildlive/responses/project.json"
            ))),
        );

        mock_server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::path("/api/search"),
                request::body(matchers::json_decoded(matchers::eq(SearchRequest {
                    query: "(/inStationsLayout:\"wildlive/667cc39364fd45136c7a\" OR /inStationsLayout:\"wildlive/151c43fdd5881eba0bd5\") AND type:StationSetup".to_string(),
                    filter: Some(
                        [
                            "/id",
                            "/content/id",
                            "/content/name",
                            "/content/location",
                            "/content/description",
                            "/content/decimalLatitude",
                            "/content/decimalLongitude",
                        ]
                        .map(ToString::to_string)
                        .to_vec(),
                    ),
                    page_num: 0,
                    page_size: PAGE_SIZE,
                }))
            ),
            ])
            .respond_with(json_responder(test_data!(
                "wildlive/responses/station_setups.json"
            ))),
        );

        // let api_endpoint = Url::parse("https://wildlive.senckenberg.de/api/").unwrap();
        let api_endpoint = Url::parse(&mock_server.url_str("/api/")).unwrap();

        // crate::util::tests::initialize_debugging_in_test();

        let dataset =
            project_stations_dataset(&api_endpoint, None, "wildlive/ef7833589d61b2d2a905")
                .await
                .unwrap();

        assert_eq!(
            dataset,
            vec![
                StationFeature {
                    id: "wildlive/79e043c3053fb39df381".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "VacaMuerta".to_string(),
                    description: "Old station name: Vacamuerta".to_string(),
                    location: "SanSebastian".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.3),
                },
                StationFeature {
                    id: "wildlive/c2bd44066dbda6f0d1ac".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "LaCachuela_arriba".to_string(),
                    description: "Old station name: LaCachuelaArriba".to_string(),
                    location: "SanSebastian".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.4),
                },
                StationFeature {
                    id: "wildlive/16f3b0b65b4a58acb782".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "LaCachuela_CaminoCurichi".to_string(),
                    description: "Old station name: CaminoCurichi".to_string(),
                    location: "SanSebastian".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.4),
                },
                StationFeature {
                    id: "wildlive/0ff0ce1ddfcfb0aff407".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "CaminoCachuela".to_string(),
                    description: "Old station name: CaminoCachuela".to_string(),
                    location: "SanSebastian".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.4),
                },
                StationFeature {
                    id: "wildlive/52baefeffeb2648fdaf7".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "Lagarto".to_string(),
                    description: "Old station name: Lagarto".to_string(),
                    location: "SanSebastian".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.4),
                },
                StationFeature {
                    id: "wildlive/024b9357f1e23877a243".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "LajaDeNoviquia".to_string(),
                    description: "Old station name: LajaDeNoviquia".to_string(),
                    location: "SanSebastian".to_string(),
                    geom: Coordinate2D::new(-61.9, -16.4),
                },
                StationFeature {
                    id: "wildlive/f421dc2239b8fd7a1980".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "Orquidia".to_string(),
                    description: "Old station name: Orquidia".to_string(),
                    location: "SanSebastian".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.4),
                },
                StationFeature {
                    id: "wildlive/797cb6f275e9fc8afa4b".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "Jaguar".to_string(),
                    description: "Old station name: Jaguar".to_string(),
                    location: "SanSebastian".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.4),
                },
                StationFeature {
                    id: "wildlive/a43254afb230ce163256".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "Salitral".to_string(),
                    description: "Old station name: Salitral".to_string(),
                    location: "SanSebastian".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.4),
                },
                StationFeature {
                    id: "wildlive/229392d20de8b45e8114".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "LaCachuela".to_string(),
                    description: "Old station name: LaCachuela".to_string(),
                    location: "SanSebastian".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.4),
                },
                StationFeature {
                    id: "wildlive/3204d6391519562525ec".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "G-06".to_string(),
                    description: "Old station name: nuevo-4".to_string(),
                    location: "SanSebastian".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.3),
                },
                StationFeature {
                    id: "wildlive/468ba2036b2a4ff004c9".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "G-12".to_string(),
                    description: "Old station name: nan".to_string(),
                    location: "MonteFlorI".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.3),
                },
                StationFeature {
                    id: "wildlive/ea64f18b8fa1dec31196".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "G-11".to_string(),
                    description: "Old station name: nuevo-5".to_string(),
                    location: "MonteFlorI".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.3),
                },
                StationFeature {
                    id: "wildlive/259cfcfd85fcb0ce276d".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "G-02".to_string(),
                    description: "Old station name: Quebrada".to_string(),
                    location: "SanSebastian".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.4),
                },
                StationFeature {
                    id: "wildlive/358df8fa949f35e91a64".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "G-05".to_string(),
                    description: "Old station name: nuevo-3".to_string(),
                    location: "Noviquia".to_string(),
                    geom: Coordinate2D::new(-61.9, -16.4),
                },
                StationFeature {
                    id: "wildlive/33516c1ce3b7e26c296d".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "G-04".to_string(),
                    description: "Old station name: nuevo-2".to_string(),
                    location: "Noviquia".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.4),
                },
                StationFeature {
                    id: "wildlive/498ae1629861699f5323".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "G-13".to_string(),
                    description: "Old station name: nan".to_string(),
                    location: "NaN".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.3),
                },
                StationFeature {
                    id: "wildlive/6bf42fa2eb245604bb31".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "G-07".to_string(),
                    description: "Old station name: NuevoManantial".to_string(),
                    location: "SanSebastian".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.3),
                },
                StationFeature {
                    id: "wildlive/2cd0a46deb9e47b0518f".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "G-14".to_string(),
                    description: "Old station name: LaCruz".to_string(),
                    location: "LaCruz".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.3),
                },
                StationFeature {
                    id: "wildlive/8ced32ac3ca4f646a53b".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "G-01".to_string(),
                    description: "Old station name: Popo".to_string(),
                    location: "SanSebastian".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.4),
                },
                StationFeature {
                    id: "wildlive/de7f4396c2689d1fbf6d".to_string(),
                    project_id: "wildlive/ef7833589d61b2d2a905".to_string(),
                    name: "G-03".to_string(),
                    description: "Old station name: nuevo-1".to_string(),
                    location: "Noviquia".to_string(),
                    geom: Coordinate2D::new(-62.0, -16.4),
                },
            ]
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_downloads_a_captures_dataset() {
        let mock_server = httptest::Server::run();

        mock_server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path("/api/objects/wildlive/ef7833589d61b2d2a905"),
            ])
            .respond_with(json_responder(test_data!(
                "wildlive/responses/project.json"
            ))),
        );

        mock_server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::path("/api/search"),
                request::body(matchers::json_decoded(matchers::eq(SearchRequest {
                    query: "(/inStationsLayout:\"wildlive/667cc39364fd45136c7a\" OR /inStationsLayout:\"wildlive/151c43fdd5881eba0bd5\") AND type:StationSetup".to_string(),
                    filter: Some(
                        [
                            "/id",
                            "/content/id",
                            "/content/name",
                            "/content/location",
                            "/content/description",
                            "/content/decimalLatitude",
                            "/content/decimalLongitude",
                        ]
                        .map(ToString::to_string)
                        .to_vec(),
                    ),
                    page_num: 0,
                    page_size: PAGE_SIZE,
                }))
            ),
            ])
            .respond_with(json_responder(test_data!(
                "wildlive/responses/station_setups.json"
            ))),
        );

        mock_server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::path("/api/search"),
                request::body(matchers::json_decoded(matchers::eq(SearchRequest {
                    query: "type:ImageObject AND (/atStation:\"wildlive/024b9357f1e23877a243\" OR /atStation:\"wildlive/0ff0ce1ddfcfb0aff407\" OR /atStation:\"wildlive/16f3b0b65b4a58acb782\" OR /atStation:\"wildlive/229392d20de8b45e8114\" OR /atStation:\"wildlive/259cfcfd85fcb0ce276d\" OR /atStation:\"wildlive/2cd0a46deb9e47b0518f\" OR /atStation:\"wildlive/3204d6391519562525ec\" OR /atStation:\"wildlive/33516c1ce3b7e26c296d\" OR /atStation:\"wildlive/358df8fa949f35e91a64\" OR /atStation:\"wildlive/468ba2036b2a4ff004c9\" OR /atStation:\"wildlive/498ae1629861699f5323\" OR /atStation:\"wildlive/52baefeffeb2648fdaf7\" OR /atStation:\"wildlive/6bf42fa2eb245604bb31\" OR /atStation:\"wildlive/797cb6f275e9fc8afa4b\" OR /atStation:\"wildlive/79e043c3053fb39df381\" OR /atStation:\"wildlive/8ced32ac3ca4f646a53b\" OR /atStation:\"wildlive/a43254afb230ce163256\" OR /atStation:\"wildlive/c2bd44066dbda6f0d1ac\" OR /atStation:\"wildlive/de7f4396c2689d1fbf6d\" OR /atStation:\"wildlive/ea64f18b8fa1dec31196\" OR /atStation:\"wildlive/f421dc2239b8fd7a1980\")".to_string(),
                    filter: Some(
                        [
                            "/id",
                            "/content/id",
                            "/content/captureTimeStamp",
                            "/content/atStation",
                            "/content/hasAnnotations",
                            "/content/contentUrl"
                        ]
                        .map(ToString::to_string)
                        .to_vec(),
                    ),
                    page_num: 0,
                    page_size: PAGE_SIZE,
                }))),
            ])
            .respond_with(json_responder(test_data!(
                "wildlive/responses/image_objects.json"
            ))),
        );

        mock_server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::path("/api/search"),
                request::body(matchers::json_decoded(matchers::eq(SearchRequest {
                    query: "type:Annotation AND (/id:\"wildlive/7ef5664c43cf26299b09\" OR /id:\"wildlive/ebe8d5f722782b0bee73\" OR /id:\"wildlive/a1eaa469eec33a0d3a39\")".to_string(),
                    filter: Some(
                        [
                            "/id",
                            "/content/id",
                            "/content/hasTarget",
                            "/content/hasBody",
                        ]
                        .map(ToString::to_string)
                        .to_vec(),
                    ),
                    page_num: 0,
                    page_size: PAGE_SIZE,
                }))),
            ])
            .respond_with(json_responder(test_data!(
                "wildlive/responses/annotations.json"
            ))),
        );

        // let api_endpoint = Url::parse("https://wildlive.senckenberg.de/api/").unwrap();
        let api_endpoint = Url::parse(&mock_server.url_str("/api/")).unwrap();

        // crate::util::tests::initialize_debugging_in_test();

        let dataset = captures_dataset(&api_endpoint, None, "wildlive/ef7833589d61b2d2a905")
            .await
            .unwrap();

        assert_eq!(dataset.len(), 3);

        assert_eq!(
            dataset[0],
            CaptureFeature {
                id: "wildlive/75243d4b79e5c91bd3b3".into(),
                station_setup_id: "wildlive/ea64f18b8fa1dec31196".into(),
                capture_time_stamp: "2019-02-26T14:48:27Z".parse().unwrap(),
                accepted_name_usage_id: "https://www.gbif.org/species/5219426".into(),
                vernacular_name: "Jaguar".into(),
                scientific_name: "Panthera onca (Linnaeus, 1758)".into(),
                content_url: "https://wildlive.senckenberg.de/api/objects/wildlive/75243d4b79e5c91bd3b3?payload=CamTrapImport_2019-03-11_Grid_G-05_105_A_026.JPG".into(),
                geom: Coordinate2D { x: -62., y: -16.3 },
            },
        );
    }
}
