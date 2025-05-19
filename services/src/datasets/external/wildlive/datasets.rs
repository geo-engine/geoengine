use super::{Result, WildliveError};
use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D};
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
    fn to_content(self) -> T {
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
) -> Result<geojson::FeatureCollection> {
    let projects = projects(&api_endpoint, api_token.clone()).await?;
    // let station_coordinates = station_coordinates(&api_endpoint, api_token, &projects).await?;
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
    .fold(HashMap::new(), |mut acc, content| {
        acc.entry(content.stations_layout)
            .or_insert_with(Vec::new)
            .push(content.coordinate);
        acc
    });

    let features = crate::util::spawn_blocking(move || {
        let mut features = Vec::<geojson::Feature>::with_capacity(projects.len());

        for project in projects {
            let Some(bbox) = BoundingBox2D::from_coord_iter(
                project
                    .station_layouts
                    .iter()
                    .filter_map(|layout| station_coordinates.get(layout))
                    .flat_map(|coordinates| coordinates.into_iter().map(Coordinate2D::from)),
            ) else {
                return Err(WildliveError::EmptyProjectBounds {
                    project: project.id,
                });
            };

            features.push(geojson::Feature {
                bbox: None,
                geometry: Some(bbox.into()),
                id: geojson::feature::Id::String(project.id).into(),
                properties: {
                    let mut properties = serde_json::Map::new();
                    properties.insert("name".to_string(), project.name.as_str().into());
                    properties.insert(
                        "description".to_string(),
                        project.description.as_str().into(),
                    );
                    Some(properties)
                },
                foreign_members: None,
            });
        }

        Ok(features)
    })
    .await??;

    Ok(geojson::FeatureCollection {
        bbox: None,
        features,
        foreign_members: None,
    })
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
) -> Result<geojson::FeatureCollection> {
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
        let mut features = Vec::<geojson::Feature>::new();

        for station in stations {
            features.push(geojson::Feature {
                bbox: None,
                geometry: {
                    let point = geojson::Value::Point(vec![
                        station.decimal_longitude,
                        station.decimal_latitude,
                    ]);
                    Some(geojson::Geometry::new(point))
                },
                id: geojson::feature::Id::String(station.id.clone()).into(),
                properties: {
                    let mut properties = serde_json::Map::new();
                    properties.insert("name".to_string(), station.name.as_str().into());
                    properties.insert(
                        "description".to_string(),
                        station.description.as_str().into(),
                    );
                    properties.insert("location".to_string(), station.location.as_str().into());
                    properties.insert("projectId".to_string(), project.id.as_str().into());
                    Some(properties)
                },
                foreign_members: None,
            });
        }

        features
    })
    .await?;

    Ok(geojson::FeatureCollection {
        bbox: None,
        features,
        foreign_members: None,
    })
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
        return Ok(stations.into_iter().map(QueryResultContent::to_content));
    };
    for station_layout in station_layouts {
        query.push_str("\" OR /inStationsLayout:\"");
        query.push_str(station_layout);
    }
    query.push_str("\") AND type:StationSetup");

    let url = api_endpoint.join(&format!("search"))?;

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

    Ok(stations.into_iter().map(QueryResultContent::to_content))
}

pub(super) async fn station_names(
    api_endpoint: &Url,
    api_token: Option<&str>,
    project_id: &str,
) -> Result<Vec<StationName>> {
    let project = project(api_endpoint, api_token, project_id).await?;
    let station_names = stations::<StationName>(
        api_endpoint,
        api_token,
        vec![
            "/id".to_string(),
            "/content/name".to_string(),
            "/content/description".to_string(),
        ],
        &[project],
    )
    .await?;

    Ok(station_names.collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::test_data;
    use geojson::GeoJson;
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
                    query: format!(
                        "(/inStationsLayout:\"wildlive/667cc39364fd45136c7a\" OR /inStationsLayout:\"wildlive/151c43fdd5881eba0bd5\") AND type:StationSetup",
                    ),
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
            GeoJson::from(dataset).to_json_value(),
            serde_json::json!({
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [-62.0,-16.3],
                                    [-61.9,-16.3],
                                    [-61.9,-16.4],
                                    [-62.0,-16.4],
                                    [-62.0,-16.3]
                                ]
                            ]
                        },
                        "properties": {
                            "name": "CameraTrapping project in Bolivia",
                            "description": "Research project for Jaguar wildlife monitoring"
                        },
                        "id": "wildlive/ef7833589d61b2d2a905",
                    }
                ]
            })
        )
    }

    fn json_responder(path: &Path) -> impl httptest::responders::Responder + use<> {
        let json = std::fs::read_to_string(path).unwrap();
        responders::status_code(200)
            .append_header("Content-Type", "application/json")
            .body(json)
    }

    #[tokio::test]
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
                    query: format!(
                        "(/inStationsLayout:\"wildlive/667cc39364fd45136c7a\" OR /inStationsLayout:\"wildlive/151c43fdd5881eba0bd5\") AND type:StationSetup",
                    ),
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
                        .map(|s| s.to_string())
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
            GeoJson::from(dataset).to_string_pretty().unwrap(),
            serde_json::to_string_pretty(&serde_json::json!({
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.3
                            ]
                        },
                        "properties": {
                            "name": "VacaMuerta",
                            "description": "Old station name: Vacamuerta",
                            "location": "SanSebastian",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/79e043c3053fb39df381"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.4
                            ]
                        },
                        "properties": {
                            "name": "LaCachuela_arriba",
                            "description": "Old station name: LaCachuelaArriba",
                            "location": "SanSebastian",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/c2bd44066dbda6f0d1ac"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.4
                            ]
                        },
                        "properties": {
                            "name": "LaCachuela_CaminoCurichi",
                            "description": "Old station name: CaminoCurichi",
                            "location": "SanSebastian",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/16f3b0b65b4a58acb782"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.4
                            ]
                        },
                        "properties": {
                            "name": "CaminoCachuela",
                            "description": "Old station name: CaminoCachuela",
                            "location": "SanSebastian",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/0ff0ce1ddfcfb0aff407"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.4
                            ]
                        },
                        "properties": {
                            "name": "Lagarto",
                            "description": "Old station name: Lagarto",
                            "location": "SanSebastian",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/52baefeffeb2648fdaf7"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -61.9,
                                -16.4
                            ]
                        },
                        "properties": {
                            "name": "LajaDeNoviquia",
                            "description": "Old station name: LajaDeNoviquia",
                            "location": "SanSebastian",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/024b9357f1e23877a243"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.4
                            ]
                        },
                        "properties": {
                            "name": "Orquidia",
                            "description": "Old station name: Orquidia",
                            "location": "SanSebastian",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/f421dc2239b8fd7a1980"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.4
                            ]
                        },
                        "properties": {
                            "name": "Jaguar",
                            "description": "Old station name: Jaguar",
                            "location": "SanSebastian",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/797cb6f275e9fc8afa4b"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.4
                            ]
                        },
                        "properties": {
                            "name": "Salitral",
                            "description": "Old station name: Salitral",
                            "location": "SanSebastian",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/a43254afb230ce163256"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.4
                            ]
                        },
                        "properties": {
                            "name": "LaCachuela",
                            "description": "Old station name: LaCachuela",
                            "location": "SanSebastian",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/229392d20de8b45e8114"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.3
                            ]
                        },
                        "properties": {
                            "name": "G-06",
                            "description": "Old station name: nuevo-4",
                            "location": "SanSebastian",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/3204d6391519562525ec"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.3
                            ]
                        },
                        "properties": {
                            "name": "G-12",
                            "description": "Old station name: nan",
                            "location": "MonteFlorI",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/468ba2036b2a4ff004c9"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.3
                            ]
                        },
                        "properties": {
                            "name": "G-11",
                            "description": "Old station name: nuevo-5",
                            "location": "MonteFlorI",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/ea64f18b8fa1dec31196"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.4
                            ]
                        },
                        "properties": {
                            "name": "G-02",
                            "description": "Old station name: Quebrada",
                            "location": "SanSebastian",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/259cfcfd85fcb0ce276d"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -61.9,
                                -16.4
                            ]
                        },
                        "properties": {
                            "name": "G-05",
                            "description": "Old station name: nuevo-3",
                            "location": "Noviquia",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/358df8fa949f35e91a64"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.4
                            ]
                        },
                        "properties": {
                            "name": "G-04",
                            "description": "Old station name: nuevo-2",
                            "location": "Noviquia",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/33516c1ce3b7e26c296d"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.3
                            ]
                        },
                        "properties": {
                            "name": "G-13",
                            "description": "Old station name: nan",
                            "location": "NaN",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/498ae1629861699f5323"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.3
                            ]
                        },
                        "properties": {
                            "name": "G-07",
                            "description": "Old station name: NuevoManantial",
                            "location": "SanSebastian",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/6bf42fa2eb245604bb31"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.3
                            ]
                        },
                        "properties": {
                            "name": "G-14",
                            "description": "Old station name: LaCruz",
                            "location": "LaCruz",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/2cd0a46deb9e47b0518f"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.4
                            ]
                        },
                        "properties": {
                            "name": "G-01",
                            "description": "Old station name: Popo",
                            "location": "SanSebastian",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/8ced32ac3ca4f646a53b"
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -62.0,
                                -16.4
                            ]
                        },
                        "properties": {
                            "name": "G-03",
                            "description": "Old station name: nuevo-1",
                            "location": "Noviquia",
                            "projectId": "wildlive/ef7833589d61b2d2a905"
                        },
                        "id": "wildlive/de7f4396c2689d1fbf6d"
                    }
                ]
            }))
            .unwrap(),
        )
    }
}
