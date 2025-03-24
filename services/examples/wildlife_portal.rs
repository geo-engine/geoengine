#![allow(clippy::unwrap_used, clippy::print_stderr)] // ok for example
#![allow(dead_code)] // TODO: remove

use futures::{StreamExt, TryFutureExt};
use geoengine_datatypes::primitives::{AxisAlignedRectangle, BoundingBox2D, Coordinate2D};
use std::{collections::HashMap, fs::File};
use url::Url;

// TODO: pagination
const PAGE_SIZE: usize = 10_000;

#[tokio::main]
async fn main() {
    // let url = Url::parse_with_params(
    //     "https://wildlive.senckenberg.de/api/objects/",
    //     [
    //         // ("query", "internal.pointsAt:wildlive/137d6d2fb4e7ae973cb3"),
    //         // ("query", "internal.pointsAt:wildlive/ef19f1efbdad528709a8"),
    //         (
    //             "query",
    //             r#""wildlive/a6ae2faa6309c6999e6b" AND type:"StationSetup""#,
    //         ),
    //         // ("filter")
    //         // ("filterQueries", r#""#),
    //         // ("full", "true"),
    //         ("pageNum", "0"),
    //         ("pageSize", PAGE_SIZE.to_string().as_str()),
    //     ],
    // )
    // .unwrap();
    // let result: serde_json::Value = reqwest::get(url.clone())
    //     .await
    //     .unwrap()
    //     .json()
    //     .await
    //     .unwrap();

    // eprintln!("{url}");
    // eprintln!("{result:#?}");

    let projects = projects().await;
    let station_setups: HashMap<String, Vec<StationSetup>> = futures::stream::iter(&projects)
        .then(async |project| (project.id.clone(), station_setups(project).await))
        .collect()
        .await;
    let projects_with_bounds = projects
        .iter()
        .map(|project| project_bounds(project, &station_setups[&project.id]))
        .collect::<Vec<_>>();

    let image_objects: HashMap<&StationSetup, Vec<ImageObject>> =
        futures::stream::iter(station_setups.values())
            .flat_map(futures::stream::iter)
            .then(async |station| (station, image_objects(station).await))
            .collect()
            .await;

    {
        let mut file = File::create("projects.geojson").unwrap();
        let geojson = projects_geojson(&projects_with_bounds);
        serde_json::to_writer_pretty(&mut file, &geojson).unwrap();
    }

    {
        let mut file = File::create("station_setups.geojson").unwrap();
        let geojson =
            station_setups_geojson(station_setups.iter().flat_map(|(project, setups)| {
                setups.iter().map(|setup| (project.as_str(), setup))
            }));
        serde_json::to_writer_pretty(&mut file, &geojson).unwrap();
    }

    {
        let mut file = File::create("image_objects.geojson").unwrap();
        let geojson = image_objects_geojson(
            image_objects
                .iter()
                .flat_map(|(project, setups)| setups.iter().map(|setup| (*project, setup))),
        );
        serde_json::to_writer_pretty(&mut file, &geojson).unwrap();
    }
}

fn projects_geojson(projects: &[ProjectWithBounds]) -> geojson::FeatureCollection {
    let mut features = Vec::new();
    for project in projects {
        let feature = geojson::Feature {
            bbox: None,
            geometry: {
                let bounds = project.bounds;
                let polygon = geojson::Value::Polygon(vec![vec![
                    vec![bounds.upper_left().x, bounds.upper_left().y],
                    vec![bounds.upper_right().x, bounds.upper_right().y],
                    vec![bounds.lower_right().x, bounds.lower_right().y],
                    vec![bounds.lower_left().x, bounds.lower_left().y],
                    vec![bounds.upper_left().x, bounds.upper_left().y],
                ]]);
                Some(geojson::Geometry::new(polygon))
            },
            id: geojson::feature::Id::String(project.id.clone()).into(),
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
        };
        features.push(feature);
    }

    geojson::FeatureCollection {
        bbox: None,
        features,
        foreign_members: None,
    }
}

fn station_setups_geojson<'p>(
    stations: impl Iterator<Item = (&'p str, &'p StationSetup)>,
) -> geojson::FeatureCollection {
    let mut features = Vec::new();
    for (project_id, station) in stations {
        let feature = geojson::Feature {
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
                properties.insert("projectId".to_string(), project_id.into());
                Some(properties)
            },
            foreign_members: None,
        };
        features.push(feature);
    }

    geojson::FeatureCollection {
        bbox: None,
        features,
        foreign_members: None,
    }
}

fn image_objects_geojson<'s>(
    image_objects: impl Iterator<Item = (&'s StationSetup, &'s ImageObject)>,
) -> geojson::FeatureCollection {
    let mut features = Vec::new();
    for (station, image_object) in image_objects {
        let feature = geojson::Feature {
            bbox: None,
            geometry: {
                let point = geojson::Value::Point(vec![
                    station.decimal_longitude,
                    station.decimal_latitude,
                ]);
                Some(geojson::Geometry::new(point))
            },
            id: geojson::feature::Id::String(image_object.id.clone()).into(),
            properties: {
                let mut properties = serde_json::Map::new();
                properties.insert("stationSetupId".to_string(), station.id.as_str().into());

                properties.insert(
                    "captureTimeStamp".to_string(),
                    image_object.capture_time_stamp.as_str().into(),
                );
                properties.insert(
                    "contentUrl".to_string(),
                    image_object.content_url.as_str().into(),
                );

                if let Some(annotation) = &image_object.first_annotation {
                    properties.insert(
                        "acceptedNameUsageID".to_string(),
                        annotation.accepted_name_usage_id.as_str().into(),
                    );
                    properties.insert(
                        "vernacularName".to_string(),
                        annotation.vernacular_name.as_str().into(),
                    );
                    properties.insert(
                        "scientificName".to_string(),
                        annotation.scientific_name.as_str().into(),
                    );
                }

                Some(properties)
            },
            foreign_members: None,
        };
        features.push(feature);
    }

    geojson::FeatureCollection {
        bbox: None,
        features,
        foreign_members: None,
    }
}

#[derive(Debug, serde::Deserialize)]
struct Project {
    pub id: String,
    pub name: String,
    pub description: String,
    #[serde(rename = "hasStationsLayouts")]
    pub station_layouts: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
struct ProjectWithBounds {
    pub id: String,
    pub name: String,
    pub description: String,
    pub bounds: geoengine_datatypes::primitives::BoundingBox2D,
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

impl std::hash::Hash for StationSetup {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Eq for StationSetup {}

// #[derive(Debug, serde::Deserialize)]
// struct CaptureEvent {
//     pub id: String,
//     #[serde(rename = "startDate")]
//     pub start_date: String,
//     #[serde(rename = "endDate")]
//     pub end_date: String,
//     #[serde(rename = "capturedAt")]
//     pub captured_at: String,
// }

#[derive(Debug, serde::Deserialize)]
struct ImageObject {
    pub id: String,
    #[serde(rename = "captureTimeStamp")]
    pub capture_time_stamp: String,
    #[serde(rename = "atStation")]
    pub station_setup_id: String,
    #[serde(rename = "hasAnnotations")]
    pub annotation_ids: Vec<String>,
    #[serde(rename = "contentUrl")]
    pub content_url: String,
    pub first_annotation: Option<Annotation>,
}

#[derive(Debug, serde::Deserialize)]
struct Annotation {
    // pub id: String,
    #[serde(rename = "acceptedNameUsageID")]
    pub accepted_name_usage_id: String,
    #[serde(rename = "vernacularName")]
    pub vernacular_name: String,
    #[serde(rename = "scientificName")]
    pub scientific_name: String,
}

fn project_bounds(project: &Project, station_setups: &[StationSetup]) -> ProjectWithBounds {
    let mut min_lat = f64::MAX;
    let mut max_lat = f64::MIN;
    let mut min_lon = f64::MAX;
    let mut max_lon = f64::MIN;

    for station_setup in station_setups {
        min_lat = f64::min(min_lat, station_setup.decimal_latitude);
        max_lat = f64::max(max_lat, station_setup.decimal_latitude);
        min_lon = f64::min(min_lon, station_setup.decimal_longitude);
        max_lon = f64::max(max_lon, station_setup.decimal_longitude);
    }

    ProjectWithBounds {
        id: project.id.clone(),
        name: project.name.clone(),
        description: project.description.clone(),
        bounds: BoundingBox2D::new(
            Coordinate2D::new(min_lon, min_lat),
            Coordinate2D::new(max_lon, max_lat),
        )
        .unwrap(),
    }
}

async fn station_setups(project: &Project) -> Vec<StationSetup> {
    let stations_layout_query_part = project
        .station_layouts
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>() // TODO: intersperse
        .join(r#"" OR ""#);
    let url = Url::parse_with_params(
        "https://wildlive.senckenberg.de/api/objects/",
        [
            (
                "query",
                format!(r#""{stations_layout_query_part}" AND type:"StationSetup""#),
            ),
            // TODO: filter only decimalLatitude and decimalLongitude
            ("pageNum", "0".to_string()),
            ("pageSize", PAGE_SIZE.to_string()),
        ],
    )
    .unwrap();

    eprintln!("[QUERY] {url}");

    reqwest::get(url)
        .await
        .unwrap()
        .json()
        .map_ok(|results: serde_json::Value| {
            results
                .get("results")
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .map(|result| {
                    serde_json::from_value::<StationSetup>(result.get("content").unwrap().clone())
                        .unwrap()
                })
                .collect::<Vec<_>>()
        })
        .await
        .unwrap()
}

async fn projects() -> Vec<Project> {
    let url = Url::parse_with_params(
        "https://wildlive.senckenberg.de/api/objects/",
        [
            // ("query", "internal.pointsAt:wildlive/137d6d2fb4e7ae973cb3"),
            ("query", "type:project"),
            ("pageNum", "0"),
            ("pageSize", PAGE_SIZE.to_string().as_str()),
        ],
    )
    .unwrap();

    eprintln!("[QUERY] {url}");

    let projects: Vec<Project> = reqwest::get(url)
        .await
        .unwrap()
        .json()
        .map_ok(|results: serde_json::Value| {
            results
                .get("results")
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .map(|result| {
                    serde_json::from_value::<Project>(result.get("content").unwrap().clone())
                        .unwrap()
                })
                .collect::<Vec<_>>()
        })
        .await
        .unwrap();

    projects
}

async fn image_objects(station: &StationSetup) -> Vec<ImageObject> {
    let url = Url::parse_with_params(
        "https://wildlive.senckenberg.de/api/objects/",
        [
            (
                "query",
                format!(
                    r#""{stations_setup_id}" AND type:"ImageObject""#,
                    stations_setup_id = &station.id
                ),
            ),
            // TODO: filter only decimalLatitude and decimalLongitude
            ("pageNum", "0".to_string()),
            ("pageSize", PAGE_SIZE.to_string()),
        ],
    )
    .unwrap();

    eprintln!("[QUERY] {url}");

    let mut image_objects = reqwest::get(url)
        .await
        .unwrap()
        .json()
        .map_ok(|results: serde_json::Value| {
            results
                .get("results")
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .map(|result| {
                    serde_json::from_value::<ImageObject>(result.get("content").unwrap().clone())
                        .unwrap()
                })
                .collect::<Vec<_>>()
        })
        .await
        .unwrap();

    for image_object in &mut image_objects {
        let Some(first_annotation) = image_object.annotation_ids.first() else {
            continue;
        };
        // TODO: one query for all annotations
        let url = Url::parse(&format!(
            "https://wildlive.senckenberg.de/api/objects/{first_annotation}"
        ))
        .unwrap();

        eprintln!("[QUERY] {url}");

        let annotation = reqwest::get(url)
            .await
            .unwrap()
            .json()
            .map_ok(|results: serde_json::Value| {
                serde_json::from_value::<Annotation>(results.get("hasBody").unwrap().clone())
                    .unwrap()
            })
            .await
            .unwrap();

        image_object.first_annotation = Some(annotation);
    }

    image_objects
}
