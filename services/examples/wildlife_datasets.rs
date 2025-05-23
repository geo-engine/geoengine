#![allow(clippy::unwrap_used, clippy::print_stderr)] // ok for example

use geoengine_datatypes::{
    collections::VectorDataType,
    dataset::LayerId,
    primitives::{CacheTtlSeconds, FeatureDataType, Measurement},
    spatial_reference::SpatialReference,
};
use geoengine_operators::{
    engine::{
        StaticMetaData, TypedOperator, VectorColumnInfo, VectorOperator, VectorResultDescriptor,
    },
    source::{
        OgrSource, OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType,
        OgrSourceDurationSpec, OgrSourceErrorSpec, OgrSourceParameters, OgrSourceTimeFormat,
    },
};
use geoengine_services::{
    datasets::{
        AddDataset, DatasetName,
        listing::Provenance,
        storage::{DatasetDefinition, MetaDataDefinition},
    },
    layers::{
        layer::{LayerCollectionDefinition, LayerDefinition},
        listing::LayerCollectionId,
    },
    test_data,
    workflows::workflow::Workflow,
};
use std::path::PathBuf;

#[allow(clippy::too_many_lines)]
fn main() {
    let project_dataset = project_dataset();
    let stations_dataset = stations_dataset();
    let images_dataset = images_dataset();

    let projects_layer = LayerDefinition {
        id: LayerId("bb6f42a9-a05d-4333-87d2-14f1f573e019".to_string()),
        name: project_dataset.properties.display_name.clone(),
        description: project_dataset.properties.description.clone(),
        properties: Vec::new(),
        workflow: Workflow {
            operator: TypedOperator::Vector(
                OgrSource {
                    params: OgrSourceParameters {
                        data: project_dataset
                            .properties
                            .name
                            .as_ref()
                            .unwrap()
                            .clone()
                            .into(),
                        attribute_projection: None,
                        attribute_filters: None,
                    },
                }
                .boxed(),
            ),
        },
        symbology: None,
        metadata: Default::default(),
    };

    let stations_layer = LayerDefinition {
        id: LayerId("17161d54-8663-42c5-a626-aaa34b39b0a3".to_string()),
        name: stations_dataset.properties.display_name.clone(),
        description: stations_dataset.properties.description.clone(),
        properties: Vec::new(),
        workflow: Workflow {
            operator: TypedOperator::Vector(
                OgrSource {
                    params: OgrSourceParameters {
                        data: stations_dataset
                            .properties
                            .name
                            .as_ref()
                            .unwrap()
                            .clone()
                            .into(),
                        attribute_projection: None,
                        attribute_filters: None,
                    },
                }
                .boxed(),
            ),
        },
        symbology: None,
        metadata: Default::default(),
    };

    let images_layer = LayerDefinition {
        id: LayerId("35dd61dc-568d-4826-b5b9-fbcc06013573".to_string()),
        name: images_dataset.properties.display_name.clone(),
        description: images_dataset.properties.description.clone(),
        properties: Vec::new(),
        workflow: Workflow {
            operator: TypedOperator::Vector(
                OgrSource {
                    params: OgrSourceParameters {
                        data: images_dataset
                            .properties
                            .name
                            .as_ref()
                            .unwrap()
                            .clone()
                            .into(),
                        attribute_projection: None,
                        attribute_filters: None,
                    },
                }
                .boxed(),
            ),
        },
        symbology: None,
        metadata: Default::default(),
    };

    let wildlife_layer_collection = LayerCollectionDefinition {
        id: LayerCollectionId("9d0d454f-2358-4c5f-bea9-a97be74255af".to_string()),
        name: "Wildlife".to_string(),
        description: "Wildlife Test Data".to_string(),
        properties: Vec::new(),
        collections: vec![],
        layers: vec![
            projects_layer.id.clone(),
            stations_layer.id.clone(),
            images_layer.id.clone(),
        ],
    };

    let file = std::fs::File::create(test_data!("dataset_defs/wildlife_projects.json")).unwrap();
    serde_json::to_writer_pretty(file, &project_dataset).unwrap();

    let file = std::fs::File::create(test_data!("dataset_defs/wildlife_stations.json")).unwrap();
    serde_json::to_writer_pretty(file, &stations_dataset).unwrap();

    let file = std::fs::File::create(test_data!("dataset_defs/wildlife_images.json")).unwrap();
    serde_json::to_writer_pretty(file, &images_dataset).unwrap();

    let file = std::fs::File::create(test_data!("layer_defs/wildlife_projects.json")).unwrap();
    serde_json::to_writer_pretty(file, &projects_layer).unwrap();

    let file = std::fs::File::create(test_data!("layer_defs/wildlife_stations.json")).unwrap();
    serde_json::to_writer_pretty(file, &stations_layer).unwrap();

    let file = std::fs::File::create(test_data!("layer_defs/wildlife_images.json")).unwrap();
    serde_json::to_writer_pretty(file, &images_layer).unwrap();

    let file = std::fs::File::create(test_data!("layer_collection_defs/wildlife.json")).unwrap();
    serde_json::to_writer_pretty(file, &wildlife_layer_collection).unwrap();
}

fn project_dataset() -> DatasetDefinition {
    DatasetDefinition {
        properties: AddDataset {
            name: Some(DatasetName::new(None, "wildlife_projects")),
            display_name: "Wildlife Projects".to_string(),
            description: String::new(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: Some(vec![provenance()]),
            tags: None,
        },
        meta_data: MetaDataDefinition::OgrMetaData(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: PathBuf::from("test_data/wildlife/projects.geojson"),
                layer_name: "projects".to_string(),
                data_type: Some(VectorDataType::MultiPolygon),
                time: OgrSourceDatasetTimeType::None,
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: None,
                    x: String::new(),
                    y: None,
                    int: vec![],
                    float: vec![],
                    text: vec![
                        "id".to_string(),
                        "name".to_string(),
                        "description".to_string(),
                    ],
                    bool: vec![],
                    datetime: vec![],
                    rename: None,
                }),
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Abort,
                sql_query: None,
                attribute_query: None,
                cache_ttl: CacheTtlSeconds::default(),
            },
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPolygon,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: [
                    (
                        "id".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "name".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "description".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                ]
                .iter()
                .cloned()
                .collect(),
                time: None,
                bbox: None,
            },
            phantom: Default::default(),
        }),
    }
}

fn stations_dataset() -> DatasetDefinition {
    DatasetDefinition {
        properties: AddDataset {
            name: Some(DatasetName::new(None, "wildlife_stations")),
            display_name: "Wildlife Stations".to_string(),
            description: String::new(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: Some(vec![provenance()]),
            tags: None,
        },
        meta_data: MetaDataDefinition::OgrMetaData(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: PathBuf::from("test_data/wildlife/station_setups.geojson"),
                layer_name: "station_setups".to_string(),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::None,
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: None,
                    x: String::new(),
                    y: None,
                    int: vec![],
                    float: vec![],
                    text: vec![
                        "id".to_string(),
                        "name".to_string(),
                        "description".to_string(),
                        "location".to_string(),
                        "projectId".to_string(),
                    ],
                    bool: vec![],
                    datetime: vec![],
                    rename: None,
                }),
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Abort,
                sql_query: None,
                attribute_query: None,
                cache_ttl: CacheTtlSeconds::default(),
            },
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: [
                    (
                        "id".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "name".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "description".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "location".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "projectId".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                ]
                .iter()
                .cloned()
                .collect(),
                time: None,
                bbox: None,
            },
            phantom: Default::default(),
        }),
    }
}

#[allow(clippy::too_many_lines)]
fn images_dataset() -> DatasetDefinition {
    DatasetDefinition {
        properties: AddDataset {
            name: Some(DatasetName::new(None, "wildlife_images")),
            display_name: "Wildlife Images".to_string(),
            description: String::new(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: Some(vec![provenance()]),
            tags: None,
        },
        meta_data: MetaDataDefinition::OgrMetaData(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: PathBuf::from("test_data/wildlife/image_objects.geojson"),
                layer_name: "image_objects".to_string(),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::Start {
                    start_field: "captureTimeStamp".into(),
                    start_format: OgrSourceTimeFormat::Auto,
                    duration: OgrSourceDurationSpec::Zero,
                },
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: None,
                    x: String::new(),
                    y: None,
                    int: vec![],
                    float: vec![],
                    text: [
                        "id",
                        "stationSetupId",
                        "contentUrl",
                        "acceptedNameUsageID",
                        "vernacularName",
                        "scientificName",
                        "captureTimeStamp", // TODO: support datetime in UI
                    ]
                    .map(String::from)
                    .to_vec(),
                    bool: vec![],
                    datetime: vec![],
                    rename: None,
                }),
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Abort,
                sql_query: None,
                attribute_query: None,
                cache_ttl: CacheTtlSeconds::default(),
            },
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: [
                    (
                        "id".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "stationSetupId".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "captureTimeStamp".to_string(),
                        VectorColumnInfo {
                            // TODO: support datetime in UI
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "contentUrl".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "acceptedNameUsageID".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "vernacularName".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "scientificName".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                ]
                .iter()
                .cloned()
                .collect(),
                time: None,
                bbox: None,
            },
            phantom: Default::default(),
        }),
    }
}

// TODO: check if correct
fn provenance() -> Provenance {
    Provenance {
        citation: "Senckenberg Society for Nature Research".to_string(),
        license: "CC-BY 4.0".to_string(),
        uri: "http://spdx.org/licenses/CC-BY-4.0".to_string(),
    }
}
