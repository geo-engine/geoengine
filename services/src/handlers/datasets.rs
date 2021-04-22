use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    path::Path,
};

use crate::datasets::storage::{AddDataset, DatasetStore, MetaDataSuggestion, SuggestMetaData};
use crate::datasets::upload::UploadRootPath;
use crate::datasets::{
    listing::DatasetProvider,
    storage::{CreateDataset, MetaDataDefinition},
    upload::Upload,
};
use crate::error;
use crate::error::Result;
use crate::handlers::authenticate;
use crate::users::session::Session;
use crate::util::user_input::UserInput;
use crate::{contexts::Context, datasets::storage::AutoCreateDataset};
use crate::{
    datasets::{listing::DatasetListOptions, upload::UploadDb},
    util::IdResponse,
};
use chrono::DateTime;
use gdal::vector::OGRFieldType;
use gdal::{
    vector::{FieldValue, Layer},
    Dataset,
};
use geoengine_datatypes::{
    collections::VectorDataType,
    dataset::{DatasetId, InternalDatasetId},
    primitives::FeatureDataType,
    spatial_reference::SpatialReference,
};
use geoengine_operators::{
    engine::{StaticMetaData, VectorResultDescriptor},
    source::{
        OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType, OgrSourceTimeFormat,
    },
};
use snafu::{OptionExt, ResultExt};
use uuid::Uuid;
use warp::Filter;

pub(crate) fn list_datasets_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path("datasets")
        .and(warp::get())
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::query())
        .and_then(list_datasets)
}

// TODO: move into handler once async closures are available?
async fn list_datasets<C: Context>(
    session: Session,
    ctx: C,
    options: DatasetListOptions,
) -> Result<impl warp::Reply, warp::Rejection> {
    let options = options.validated()?;
    let list = ctx
        .dataset_db_ref()
        .await
        .list(session.user.id, options)
        .await?;
    Ok(warp::reply::json(&list))
}

pub(crate) fn get_dataset_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("dataset" / "internal" / Uuid)
        .map(|id: Uuid| (DatasetId::Internal(InternalDatasetId(id))))
        .and(warp::get())
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and_then(get_dataset)
}

// TODO: move into handler once async closures are available?
async fn get_dataset<C: Context>(
    dataset: DatasetId,
    session: Session,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    let dataset = ctx
        .dataset_db_ref()
        .await
        .load(session.user.id, &dataset)
        .await?;
    Ok(warp::reply::json(&dataset))
}

pub(crate) fn create_dataset_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path("dataset")
        .and(warp::post())
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::body::json())
        .and_then(create_dataset)
}

// TODO: move into handler once async closures are available?
async fn create_dataset<C: Context>(
    session: Session,
    ctx: C,
    create: CreateDataset,
) -> Result<impl warp::Reply, warp::Rejection> {
    let upload = ctx
        .dataset_db_ref()
        .await
        .get_upload(session.user.id, create.upload)
        .await?;

    let mut definition = create.definition;

    adjust_user_path_to_upload_path(&mut definition.meta_data, &upload)?;

    let mut db = ctx.dataset_db_ref_mut().await;
    let meta_data = db.wrap_meta_data(definition.meta_data);
    let id = db
        .add_dataset(
            session.user.id,
            definition.properties.validated()?,
            meta_data,
        )
        .await?;

    Ok(warp::reply::json(&IdResponse::from(id)))
}

fn adjust_user_path_to_upload_path(meta: &mut MetaDataDefinition, upload: &Upload) -> Result<()> {
    match meta {
        crate::datasets::storage::MetaDataDefinition::MockMetaData(_) => {}
        crate::datasets::storage::MetaDataDefinition::OgrMetaData(m) => {
            m.loading_info.file_name = upload.adjust_file_path(&m.loading_info.file_name)?
        }
        crate::datasets::storage::MetaDataDefinition::GdalMetaDataRegular(m) => {
            m.params.file_path = upload.adjust_file_path(&m.params.file_path)?
        }
        crate::datasets::storage::MetaDataDefinition::GdalStatic(m) => {
            m.params.file_path = upload.adjust_file_path(&m.params.file_path)?
        }
    }
    Ok(())
}

pub(crate) fn auto_create_dataset_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("dataset" / "auto")
        .and(warp::post())
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::body::json())
        .and_then(auto_create_dataset)
}

// TODO: move into handler once async closures are available?
async fn auto_create_dataset<C: Context>(
    session: Session,
    ctx: C,
    create: AutoCreateDataset,
) -> Result<impl warp::Reply, warp::Rejection> {
    let upload = ctx
        .dataset_db_ref()
        .await
        .get_upload(session.user.id, create.upload)
        .await?;

    let create = create.validated()?.user_input;

    let main_file_path = upload.id.root_path()?.join(&create.main_file);
    let meta_data = auto_detect_meta_data_definition(&main_file_path)?;

    let properties = AddDataset {
        id: None,
        name: create.dataset_name,
        description: create.dataset_description,
        source_operator: meta_data.source_operator_type().to_owned(),
    };

    let mut db = ctx.dataset_db_ref_mut().await;
    let meta_data = db.wrap_meta_data(meta_data);
    let id = db
        .add_dataset(session.user.id, properties.validated()?, meta_data)
        .await?;

    Ok(warp::reply::json(&IdResponse::from(id)))
}

pub(crate) fn suggest_meta_data_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("dataset" / "suggest")
        .and(warp::get())
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::query())
        .and_then(suggest_meta_data)
}

// TODO: move into handler once async closures are available?
async fn suggest_meta_data<C: Context>(
    session: Session,
    ctx: C,
    suggest: SuggestMetaData,
) -> Result<impl warp::Reply, warp::Rejection> {
    let upload = ctx
        .dataset_db_ref()
        .await
        .get_upload(session.user.id, suggest.upload)
        .await?;

    let main_file = suggest
        .main_file
        .or_else(|| suggest_main_file(&upload))
        .ok_or(error::Error::NoMainFileCandidateFound)?;

    let main_file_path = upload.id.root_path()?.join(&main_file);

    let meta_data = auto_detect_meta_data_definition(&main_file_path)?;

    Ok(warp::reply::json(&MetaDataSuggestion {
        main_file,
        meta_data,
    }))
}

fn suggest_main_file(upload: &Upload) -> Option<String> {
    let known_extensions = ["csv", "shp", "json", "geojson", "gpkg"]; // TODO: rasters

    let mut sorted_files = upload.files.clone();
    sorted_files.sort_by(|a, b| b.byte_size.cmp(&a.byte_size));

    for file in sorted_files {
        if known_extensions.iter().any(|ext| file.name.ends_with(ext)) {
            return Some(file.name);
        }
    }
    None
}

fn auto_detect_meta_data_definition(main_file_path: &Path) -> Result<MetaDataDefinition> {
    let mut dataset = Dataset::open(&main_file_path).context(error::Gdal)?;
    let layer = {
        if let Ok(layer) = dataset.layer(0) {
            layer
        } else {
            // TODO: handle Raster datasets as well
            return Err(crate::error::Error::DatasetHasNoAutoImportableLayer);
        }
    };
    let vector_type = detect_vector_type(&layer)?;
    let spatial_reference: SpatialReference = layer
        .spatial_ref()
        .context(error::Gdal)?
        .try_into()
        .context(error::DataType)?;
    let columns_map = detect_columns(&layer);
    let columns_vecs = column_map_to_column_vecs(&columns_map);
    let time = detect_time_type(&layer, &columns_vecs);

    Ok(MetaDataDefinition::OgrMetaData(StaticMetaData {
        loading_info: OgrSourceDataset {
            file_name: main_file_path.into(),
            layer_name: layer.name(),
            data_type: Some(vector_type),
            time,
            columns: Some(OgrSourceColumnSpec {
                x: "".to_owned(), // TODO: for csv-files: try to find wkt/xy columns
                y: None,
                int: columns_vecs.float,
                float: columns_vecs.int,
                textual: columns_vecs.textual,
            }),
            default_geometry: None,
            force_ogr_time_filter: false,
            on_error: geoengine_operators::source::OgrSourceErrorSpec::Skip,
            provenance: None,
        },
        result_descriptor: VectorResultDescriptor {
            data_type: vector_type,
            spatial_reference: spatial_reference.into(),
            columns: columns_map
                .into_iter()
                .filter_map(|(k, v)| v.try_into().map(|v| (k, v)).ok()) // ignore all columns here that don't have a corresponding type in our collections
                .collect(),
        },
    }))
}

fn detect_time_type(layer: &Layer, columns: &Columns) -> OgrSourceDatasetTimeType {
    let feature = layer.features().next();
    if feature.is_none() {
        return OgrSourceDatasetTimeType::None;
    }
    let feature = feature.expect("checked before");

    let known_start = [
        "start",
        "time",
        "begin",
        "date",
        "time_start",
        "start time",
        "date_start",
        "start date",
        "datetime",
        "date_time",
        "date time",
        "event",
        "timestamp",
    ];
    let known_end = [
        "end",
        "stop",
        "time2",
        "date2",
        "time_end",
        "time_stop",
        "time end",
        "time stop",
        "end time",
        "stop time",
        "date_end",
        "date_stop",
        "date end",
        "date stop",
        "end date",
        "stop date",
    ];
    let known_duration = ["duration", "length", "valid for", "valid_for"];

    let mut start = None;
    let mut end = None;
    for column in columns.textual.iter().chain(&(columns.date)) {
        let is_date = feature
            .field(column)
            .ok()
            .and_then(FieldValue::into_string)
            .map(|s| DateTime::parse_from_rfc3339(&s))
            .is_some();
        if is_date {
            if known_start.contains(&column.as_ref()) && start.is_none() {
                start = Some(column);
            } else if known_end.contains(&column.as_ref()) && end.is_none() {
                end = Some(column);
            }

            if start.is_some() && end.is_some() {
                break;
            }
        }
    }

    let duration = columns
        .int
        .iter()
        .find(|c| known_duration.contains(&c.as_ref()));

    match (start, end, duration) {
        (Some(start), Some(end), _) => OgrSourceDatasetTimeType::StartEnd {
            start_field: start.clone(),
            start_format: OgrSourceTimeFormat::Iso,
            end_field: end.clone(),
            end_format: OgrSourceTimeFormat::Iso,
        },
        (Some(start), None, Some(duration)) => OgrSourceDatasetTimeType::StartDuration {
            start_field: start.clone(),
            start_format: OgrSourceTimeFormat::Iso,
            duration_field: duration.clone(),
        },
        (Some(start), None, None) => OgrSourceDatasetTimeType::Start {
            start_field: start.clone(),
            start_format: OgrSourceTimeFormat::Iso,
            duration: 0,
        },
        _ => OgrSourceDatasetTimeType::None,
    }
}

fn detect_vector_type(layer: &Layer) -> Result<VectorDataType> {
    let ogr_type = layer
        .defn()
        .geom_fields()
        .next()
        .context(error::EmptyDatasetCannotBeImported)?
        .field_type();

    VectorDataType::try_from_ogr_type_code(ogr_type).context(error::DataType)
}

struct Columns {
    int: Vec<String>,
    float: Vec<String>,
    textual: Vec<String>,
    date: Vec<String>,
}

enum ColumnDataType {
    Int,
    Float,
    Text,
    Date,
    Unknown,
}

impl TryFrom<ColumnDataType> for FeatureDataType {
    type Error = error::Error;

    fn try_from(value: ColumnDataType) -> Result<Self, Self::Error> {
        match value {
            ColumnDataType::Int => Ok(FeatureDataType::Int),
            ColumnDataType::Float => Ok(FeatureDataType::Float),
            ColumnDataType::Text => Ok(FeatureDataType::Text),
            _ => Err(error::Error::NoFeatureDataTypeForColumnDataType),
        }
    }
}

fn detect_columns(layer: &Layer) -> HashMap<String, ColumnDataType> {
    let mut columns = HashMap::default();

    for field in layer.defn().fields() {
        let field_type = field.field_type();

        let data_type = match field_type {
            OGRFieldType::OFTInteger | OGRFieldType::OFTInteger64 => ColumnDataType::Int,
            OGRFieldType::OFTReal => ColumnDataType::Float,
            OGRFieldType::OFTString => ColumnDataType::Text,
            OGRFieldType::OFTDate | OGRFieldType::OFTDateTime => ColumnDataType::Date,
            _ => ColumnDataType::Unknown,
        };

        columns.insert(field.name(), data_type);
    }

    columns
}

fn column_map_to_column_vecs(columns: &HashMap<String, ColumnDataType>) -> Columns {
    let mut int = Vec::new();
    let mut float = Vec::new();
    let mut textual = Vec::new();
    let mut date = Vec::new();

    for (k, v) in columns {
        match v {
            ColumnDataType::Int => int.push(k.clone()),
            ColumnDataType::Float => float.push(k.clone()),
            ColumnDataType::Text => textual.push(k.clone()),
            ColumnDataType::Date => date.push(k.clone()),
            ColumnDataType::Unknown => {}
        }
    }

    Columns {
        int,
        float,
        textual,
        date,
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, str::FromStr};

    use super::*;
    use crate::contexts::InMemoryContext;
    use crate::datasets::storage::{AddDataset, DatasetStore};
    use crate::error::Result;
    use crate::users::user::UserId;
    use crate::util::tests::create_session_helper;
    use crate::util::Identifier;
    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use geoengine_operators::engine::{StaticMetaData, VectorResultDescriptor};
    use geoengine_operators::source::{OgrSourceDataset, OgrSourceErrorSpec};
    use serde_json::json;

    #[tokio::test]
    async fn test_list_datasets() -> Result<()> {
        let ctx = InMemoryContext::default();

        let session = create_session_helper(&ctx).await;

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
        };

        let ds = AddDataset {
            id: None,
            name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
        };

        let meta = StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: "".to_string(),
                data_type: None,
                time: Default::default(),
                columns: None,
                default_geometry: None,
                force_ogr_time_filter: false,
                on_error: OgrSourceErrorSpec::Skip,
                provenance: None,
            },
            result_descriptor: descriptor,
        };

        let id = ctx
            .dataset_db_ref_mut()
            .await
            .add_dataset(UserId::new(), ds.validated()?, Box::new(meta))
            .await?;

        let res = warp::test::request()
            .method("GET")
            .path(&format!(
                "/datasets?{}",
                &serde_urlencoded::to_string([
                    ("order", "NameDesc"),
                    ("offset", "0"),
                    ("limit", "2"),
                ])
                .unwrap()
            ))
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&list_datasets_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();

        assert_eq!(
            body,
            json!([{
                "id": {
                    "Internal": id.internal().unwrap()
                },
                "name": "OgrDataset",
                "description": "My Ogr dataset",
                "tags": [],
                "source_operator": "OgrSource",
                "result_descriptor": {
                    "Vector": {
                        "data_type": "Data",
                        "spatial_reference": "",
                        "columns": {}
                    }
                }
            }])
            .to_string()
        );

        Ok(())
    }

    #[tokio::test]
    async fn create_dataset() {
        let ctx = InMemoryContext::default();

        let session = create_session_helper(&ctx).await;

        let s = r#"{
            "upload": "1f7e3e75-4d20-4c91-9497-7f4df7604b62",
            "definition": {
                "properties": {
                    "id": null,
                    "name": "Uploaded Natural Earth 10m Ports",
                    "description": "Ports from Natural Earth",
                    "source_operator": "OgrSource"
                },
                "meta_data": {
                    "OgrMetaData": {
                        "loading_info": {
                            "file_name": "operators/test-data/vector/data/ne_10m_ports/ne_10m_ports.shp",
                            "layer_name": "ne_10m_ports",
                            "data_type": "MultiPoint",
                            "time": "none",
                            "columns": {
                                "x": "",
                                "y": null,
                                "float": ["natlscale"],
                                "int": ["scalerank"],
                                "textual": ["featurecla", "name", "website"]
                            },
                            "default_geometry": null,
                            "force_ogr_time_filter": false,
                            "on_error": "skip",
                            "provenance": null
                        },
                        "result_descriptor": {
                            "data_type": "MultiPoint",
                            "spatial_reference": "EPSG:4326",
                            "columns": {
                                "website": "text",
                                "name": "text",
                                "natlscale": "float",
                                "scalerank": "int",
                                "featurecla": "text"
                            }
                        }
                    }
                }
            }
        }"#;

        let res = warp::test::request()
            .method("POST")
            .path("/dataset")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .body(s)
            .reply(&create_dataset_handler(ctx))
            .await;

        assert_eq!(res.status(), 500, "{:?}", res.body());

        // TODO: add a success test case once it is clear how to upload data from within a test
    }

    #[test]
    fn it_auto_detects() {
        let mut meta_data = auto_detect_meta_data_definition(
            &PathBuf::from_str("../operators/test-data/vector/data/ne_10m_ports/ne_10m_ports.shp")
                .unwrap(),
        )
        .unwrap();

        if let MetaDataDefinition::OgrMetaData(meta_data) = &mut meta_data {
            if let Some(columns) = &mut meta_data.loading_info.columns {
                columns.textual.sort();
            }
        }

        assert_eq!(
            meta_data,
            MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: "../operators/test-data/vector/data/ne_10m_ports/ne_10m_ports.shp"
                        .into(),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    columns: Some(OgrSourceColumnSpec {
                        x: "".to_string(),
                        y: None,
                        int: vec!["natlscale".to_string()],
                        float: vec!["scalerank".to_string()],
                        textual: vec![
                            "featurecla".to_string(),
                            "name".to_string(),
                            "website".to_string(),
                        ],
                    }),
                    default_geometry: None,
                    force_ogr_time_filter: false,
                    on_error: OgrSourceErrorSpec::Skip,
                    provenance: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        ("name".to_string(), FeatureDataType::Text),
                        ("scalerank".to_string(), FeatureDataType::Int),
                        ("website".to_string(), FeatureDataType::Text),
                        ("natlscale".to_string(), FeatureDataType::Float),
                        ("featurecla".to_string(), FeatureDataType::Text),
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                },
            })
        )
    }

    #[test]
    fn it_detects_time() {
        let mut meta_data = auto_detect_meta_data_definition(
            &PathBuf::from_str("../operators/test-data/vector/data/points_with_iso_time.json")
                .unwrap(),
        )
        .unwrap();

        if let MetaDataDefinition::OgrMetaData(meta_data) = &mut meta_data {
            if let Some(columns) = &mut meta_data.loading_info.columns {
                columns.textual.sort();
            }
        }

        assert_eq!(
            meta_data,
            MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: "../operators/test-data/vector/data/points_with_iso_time.json"
                        .into(),
                    layer_name: "points_with_iso_time".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::StartEnd {
                        start_field: "time_start".to_owned(),
                        start_format: OgrSourceTimeFormat::Iso,
                        end_field: "time_end".to_owned(),
                        end_format: OgrSourceTimeFormat::Iso,
                    },
                    columns: Some(OgrSourceColumnSpec {
                        x: "".to_string(),
                        y: None,
                        float: vec![],
                        int: vec![],
                        textual: vec!["time_end".to_owned(), "time_start".to_owned()],
                    }),
                    default_geometry: None,
                    force_ogr_time_filter: false,
                    on_error: OgrSourceErrorSpec::Skip,
                    provenance: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        ("time_end".to_string(), FeatureDataType::Text),
                        ("time_start".to_string(), FeatureDataType::Text),
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                },
            })
        )
    }

    #[tokio::test]
    async fn get_dataset() -> Result<()> {
        let ctx = InMemoryContext::default();

        let session = create_session_helper(&ctx).await;

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
        };

        let ds = AddDataset {
            id: None,
            name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
        };

        let meta = StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: "".to_string(),
                data_type: None,
                time: Default::default(),
                columns: None,
                default_geometry: None,
                force_ogr_time_filter: false,
                on_error: OgrSourceErrorSpec::Skip,
                provenance: None,
            },
            result_descriptor: descriptor,
        };

        let id = ctx
            .dataset_db_ref_mut()
            .await
            .add_dataset(UserId::new(), ds.validated()?, Box::new(meta))
            .await?;

        let res = warp::test::request()
            .method("GET")
            .path(&format!("/dataset/internal/{}", id.internal().unwrap()))
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&get_dataset_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();

        assert_eq!(
            body,
            json!({
                "id": {
                    "Internal": id.internal().unwrap()
                },
                "name": "OgrDataset",
                "description": "My Ogr dataset",
                "result_descriptor": {
                    "Vector": {
                        "data_type": "Data",
                        "spatial_reference": "",
                        "columns": {}
                    }
                },
                "source_operator": "OgrSource"
            })
            .to_string()
        );

        Ok(())
    }
}
