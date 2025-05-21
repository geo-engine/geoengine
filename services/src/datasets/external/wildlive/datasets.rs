use super::{Result, WildliveError, error};
use geoengine_datatypes::{
    error::BoxedResultExt,
    primitives::{BoundingBox2D, Coordinate2D},
};
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use url::Url;

const PAGE_SIZE: usize = 10_000; // TODO: pagination

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

#[derive(Debug, serde::Deserialize, PartialEq)]
pub(super) struct StationName {
    pub id: String,
    pub name: String,
    pub description: String,
}

impl std::hash::Hash for StationSetup {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Eq for StationSetup {}

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
struct SearchRequest {
    query: String,
    filter: Option<Vec<String>>,
    page_num: usize,
    page_size: usize,
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
    T: std::fmt::Debug + PartialEq,
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
            },]
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
}
