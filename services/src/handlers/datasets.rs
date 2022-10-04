use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    path::Path,
};

use crate::api::model::datatypes::DatasetId;
use crate::datasets::upload::UploadRootPath;
use crate::datasets::{
    listing::DatasetProvider,
    storage::{AddDataset, DatasetStore, MetaDataSuggestion, SuggestMetaData},
};
use crate::datasets::{
    storage::{CreateDataset, MetaDataDefinition},
    upload::Upload,
};
use crate::error;
use crate::error::Result;
use crate::util::user_input::UserInput;
use crate::{contexts::Context, datasets::storage::AutoCreateDataset};
use crate::{
    datasets::{listing::DatasetListOptions, upload::UploadDb},
    util::IdResponse,
};
use actix_web::{web, FromRequest, Responder};
use gdal::{vector::OGRFieldType, DatasetOptions};
use gdal::{
    vector::{Layer, LayerAccess},
    Dataset,
};
use geoengine_datatypes::{
    collections::VectorDataType,
    primitives::{FeatureDataType, Measurement, VectorQueryRectangle},
    spatial_reference::{SpatialReference, SpatialReferenceOption},
};
use geoengine_operators::{
    engine::{StaticMetaData, VectorColumnInfo, VectorResultDescriptor},
    source::{
        OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType, OgrSourceDurationSpec,
        OgrSourceTimeFormat,
    },
    util::gdal::{gdal_open_dataset, gdal_open_dataset_ex},
};
use snafu::ResultExt;

pub(crate) fn init_dataset_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: Context,
    C::Session: FromRequest,
{
    cfg.service(
        web::scope("/dataset")
            .service(web::resource("/suggest").route(web::get().to(suggest_meta_data_handler::<C>)))
            .service(web::resource("/auto").route(web::post().to(auto_create_dataset_handler::<C>)))
            .service(web::resource("/{dataset}").route(web::get().to(get_dataset_handler::<C>)))
            .service(web::resource("").route(web::post().to(create_dataset_handler::<C>))), // must come last to not match other routes
    )
    .service(web::resource("/datasets").route(web::get().to(list_datasets_handler::<C>)));
}

/// Lists available [Datasets](crate::datasets::listing::DatasetListing).
///
/// # Example
///
/// ```text
/// GET /datasets?filter=Germany&offset=0&limit=2&order=NameAsc
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
/// ```
/// Response:
/// ```text
/// [
///   {
///     "id": {
///       "internal": "9c874b9e-cea0-4553-b727-a13cb26ae4bb"
///     },
///     "name": "Germany",
///     "description": "Boundaries of Germany",
///     "tags": [],
///     "sourceOperator": "OgrSource",
///     "resultDescriptor": {
///       "vector": {
///         "dataType": "MultiPolygon",
///         "spatialReference": "EPSG:4326",
///         "columns": {}
///       }
///     }
///   }
/// ]
/// ```
async fn list_datasets_handler<C: Context>(
    session: C::Session,
    ctx: web::Data<C>,
    options: web::Query<DatasetListOptions>,
) -> Result<impl Responder> {
    let options = options.into_inner().validated()?;
    let list = ctx.dataset_db_ref().list(&session, options).await?;
    Ok(web::Json(list))
}

/// Retrieves details about a [Dataset](crate::datasets::listing::DatasetListing) using the internal id.
///
/// # Example
///
/// ```text
/// GET /dataset/internal/9c874b9e-cea0-4553-b727-a13cb26ae4bb
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
/// ```
/// Response:
/// ```text
/// {
///   "id": {
///     "internal": "9c874b9e-cea0-4553-b727-a13cb26ae4bb"
///   },
///   "name": "Germany",
///   "description": "Boundaries of Germany",
///   "resultDescriptor": {
///     "vector": {
///       "dataType": "MultiPolygon",
///       "spatialReference": "EPSG:4326",
///       "columns": {}
///     }
///   },
///   "sourceOperator": "OgrSource"
/// }
/// ```
async fn get_dataset_handler<C: Context>(
    dataset: web::Path<DatasetId>,
    session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let dataset = ctx
        .dataset_db_ref()
        .load(&session, &dataset.into_inner())
        .await?;
    Ok(web::Json(dataset))
}

/// Creates a new [Dataset](CreateDataset) using previously uploaded files.
/// Information about the file contents must be manually supplied.
///
/// # Example
///
/// ```text
/// POST /dataset
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
///
/// {
///   "upload": "420b06de-0a7e-45cb-9c1c-ea901b46ab69",
///   "definition": {
///     "properties": {
///       "name": "Germany Border",
///       "description": "The Outline of Germany",
///       "sourceOperator": "OgrSource"
///     },
///     "metaData": {
///       "OgrMetaData": {
///         "loadingInfo": {
///           "fileName": "germany_polygon.gpkg",
///           "layerName": "test_germany",
///           "dataType": "MultiPolygon",
///           "time": "none",
///           "columns": {
///             "x": "",
///             "y": null,
///             "text": [],
///             "float": [],
///             "int": [],
///             "bool": [],
///             "datetime": [],
///           },
///           "forceOgrTimeFilter": false,
///           "onError": "ignore"
///         },
///         "resultDescriptor": {
///           "dataType": "MultiPolygon",
///           "spatialReference": "EPSG:4326",
///           "columns": {}
///         }
///       }
///     }
///   }
/// }
/// ```
/// Response:
/// ```text
/// {
///   "id": {
///     "internal": "8d3471ab-fcf7-4c1b-bbc1-00477adf07c8"
///   }
/// }
/// ```
async fn create_dataset_handler<C: Context>(
    session: C::Session,
    ctx: web::Data<C>,
    create: web::Json<CreateDataset>,
) -> Result<impl Responder> {
    let upload = ctx
        .dataset_db_ref()
        .get_upload(&session, create.upload)
        .await?;

    let mut definition = create.into_inner().definition;

    adjust_user_path_to_upload_path(&mut definition.meta_data, &upload)?;

    let db = ctx.dataset_db_ref();
    let meta_data = db.wrap_meta_data(definition.meta_data);
    let id = db
        .add_dataset(&session, definition.properties.validated()?, meta_data)
        .await?;

    Ok(web::Json(IdResponse::from(id)))
}

fn adjust_user_path_to_upload_path(meta: &mut MetaDataDefinition, upload: &Upload) -> Result<()> {
    match meta {
        crate::datasets::storage::MetaDataDefinition::MockMetaData(_) => {}
        crate::datasets::storage::MetaDataDefinition::OgrMetaData(m) => {
            m.loading_info.file_name = upload.adjust_file_path(&m.loading_info.file_name)?;
        }
        crate::datasets::storage::MetaDataDefinition::GdalMetaDataRegular(m) => {
            m.params.file_path = upload.adjust_file_path(&m.params.file_path)?;
        }
        crate::datasets::storage::MetaDataDefinition::GdalStatic(m) => {
            m.params.file_path = upload.adjust_file_path(&m.params.file_path)?;
        }
        crate::datasets::storage::MetaDataDefinition::GdalMetadataNetCdfCf(m) => {
            m.params.file_path = upload.adjust_file_path(&m.params.file_path)?;
        }
        crate::datasets::storage::MetaDataDefinition::GdalMetaDataList(m) => {
            for p in &mut m.params {
                if let Some(ref mut params) = p.params {
                    params.file_path = upload.adjust_file_path(&params.file_path)?;
                }
            }
        }
    }
    Ok(())
}

/// Creates a new [Dataset](AutoCreateDataset) using previously uploaded files.
/// The format of the files will be automatically detected when possible.
///
/// # Example
///
/// ```text
/// POST /dataset
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
///
/// {
///   "upload": "420b06de-0a7e-45cb-9c1c-ea901b46ab69",
///   "datasetName": "Germany Border (auto)",
///   "datasetDescription": "The Outline of Germany (auto detected format)",
///   "mainFile": "germany_polygon.gpkg"
/// }
/// ```
/// Response:
/// ```text
/// {
///   "id": {
///     "internal": "664d4b3c-c9d7-4e57-b34d-8c709c1c26e8"
///   }
/// }
/// ```
async fn auto_create_dataset_handler<C: Context>(
    session: C::Session,
    ctx: web::Data<C>,
    create: web::Json<AutoCreateDataset>,
) -> Result<impl Responder> {
    let upload = ctx
        .dataset_db_ref()
        .get_upload(&session, create.upload)
        .await?;

    let create = create.into_inner().validated()?.user_input;

    let main_file_path = upload.id.root_path()?.join(&create.main_file);
    let meta_data = auto_detect_meta_data_definition(&main_file_path)?;

    let properties = AddDataset {
        id: None,
        name: create.dataset_name,
        description: create.dataset_description,
        source_operator: meta_data.source_operator_type().to_owned(),
        symbology: None,
        provenance: None,
    };

    let db = ctx.dataset_db_ref();
    let meta_data = db.wrap_meta_data(meta_data);
    let id = db
        .add_dataset(&session, properties.validated()?, meta_data)
        .await?;

    Ok(web::Json(IdResponse::from(id)))
}

async fn suggest_meta_data_handler<C: Context>(
    session: C::Session,
    ctx: web::Data<C>,
    suggest: web::Query<SuggestMetaData>,
) -> Result<impl Responder> {
    let upload = ctx
        .dataset_db_ref()
        .get_upload(&session, suggest.upload)
        .await?;

    let main_file = suggest
        .into_inner()
        .main_file
        .or_else(|| suggest_main_file(&upload))
        .ok_or(error::Error::NoMainFileCandidateFound)?;

    let main_file_path = upload.id.root_path()?.join(&main_file);

    let meta_data = auto_detect_meta_data_definition(&main_file_path)?;

    Ok(web::Json(MetaDataSuggestion {
        main_file,
        meta_data,
    }))
}

fn suggest_main_file(upload: &Upload) -> Option<String> {
    let known_extensions = ["csv", "shp", "json", "geojson", "gpkg", "sqlite"]; // TODO: rasters

    if upload.files.len() == 1 {
        return Some(upload.files[0].name.clone());
    }

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
    let dataset = gdal_open_dataset(main_file_path).context(error::Operator)?;
    let layer = {
        if let Ok(layer) = dataset.layer(0) {
            layer
        } else {
            // TODO: handle Raster datasets as well
            return Err(crate::error::Error::DatasetHasNoAutoImportableLayer);
        }
    };

    let columns_map = detect_columns(&layer);
    let columns_vecs = column_map_to_column_vecs(&columns_map);

    let mut geometry = detect_vector_geometry(&dataset);
    let mut x = String::new();
    let mut y: Option<String> = None;

    if geometry.data_type == VectorDataType::Data {
        // help Gdal detecting geometry
        if let Some(auto_detect) = gdal_autodetect(main_file_path, &columns_vecs.text) {
            geometry = detect_vector_geometry(&auto_detect.dataset);
            if geometry.data_type != VectorDataType::Data {
                x = auto_detect.x;
                y = auto_detect.y;
            }
        }
    }

    let time = detect_time_type(&columns_vecs);

    Ok(MetaDataDefinition::OgrMetaData(StaticMetaData::<
        _,
        _,
        VectorQueryRectangle,
    > {
        loading_info: OgrSourceDataset {
            file_name: main_file_path.into(),
            layer_name: geometry.layer_name.unwrap_or_else(|| layer.name()),
            data_type: Some(geometry.data_type),
            time,
            default_geometry: None,
            columns: Some(OgrSourceColumnSpec {
                format_specifics: None,
                x,
                y,
                int: columns_vecs.int,
                float: columns_vecs.float,
                text: columns_vecs.text,
                bool: vec![],
                datetime: columns_vecs.date,
                rename: None,
            }),
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: geoengine_operators::source::OgrSourceErrorSpec::Ignore,
            sql_query: None,
            attribute_query: None,
        },
        result_descriptor: VectorResultDescriptor {
            data_type: geometry.data_type,
            spatial_reference: geometry.spatial_reference,
            columns: columns_map
                .into_iter()
                .filter_map(|(k, v)| {
                    v.try_into()
                        .map(|v| {
                            (
                                k,
                                VectorColumnInfo {
                                    data_type: v,
                                    measurement: Measurement::Unitless,
                                },
                            )
                        })
                        .ok()
                }) // ignore all columns here that don't have a corresponding type in our collections
                .collect(),
            time: None,
            bbox: None,
        },
        phantom: Default::default(),
    }))
}

/// create Gdal dataset with autodetect parameters based on available columns
fn gdal_autodetect(path: &Path, columns: &[String]) -> Option<GdalAutoDetect> {
    let columns_lower = columns.iter().map(|s| s.to_lowercase()).collect::<Vec<_>>();

    // TODO: load candidates from config
    let xy = [("x", "y"), ("lon", "lat"), ("longitude", "latitude")];

    for (x, y) in xy {
        let mut found_x = None;
        let mut found_y = None;

        for (column_lower, column) in columns_lower.iter().zip(columns) {
            if x == column_lower {
                found_x = Some(column);
            }

            if y == column_lower {
                found_y = Some(column);
            }

            if let (Some(x), Some(y)) = (found_x, found_y) {
                let mut dataset_options = DatasetOptions::default();

                let open_opts = &[
                    &format!("X_POSSIBLE_NAMES={}", x),
                    &format!("Y_POSSIBLE_NAMES={}", y),
                    "AUTODETECT_TYPE=YES",
                ];

                dataset_options.open_options = Some(open_opts);

                return gdal_open_dataset_ex(path, dataset_options)
                    .ok()
                    .map(|dataset| GdalAutoDetect {
                        dataset,
                        x: x.clone(),
                        y: Some(y.clone()),
                    });
            }
        }
    }

    // TODO: load candidates from config
    let geoms = ["geom", "wkt"];
    for geom in geoms {
        for (column_lower, column) in columns_lower.iter().zip(columns) {
            if geom == column_lower {
                let mut dataset_options = DatasetOptions::default();

                let open_opts = &[
                    &format!("GEOM_POSSIBLE_NAMES={}", column),
                    "AUTODETECT_TYPE=YES",
                ];

                dataset_options.open_options = Some(open_opts);

                return gdal_open_dataset_ex(path, dataset_options)
                    .ok()
                    .map(|dataset| GdalAutoDetect {
                        dataset,
                        x: geom.to_owned(),
                        y: None,
                    });
            }
        }
    }

    None
}

fn detect_time_type(columns: &Columns) -> OgrSourceDatasetTimeType {
    // TODO: load candidate names from config
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
        "time_from",
        "t1",
        "t",
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
        "time_to",
        "t2",
    ];
    let known_duration = ["duration", "length", "valid for", "valid_for"];

    let mut start = None;
    let mut end = None;
    for column in &columns.date {
        if known_start.contains(&column.as_ref()) && start.is_none() {
            start = Some(column);
        } else if known_end.contains(&column.as_ref()) && end.is_none() {
            end = Some(column);
        }

        if start.is_some() && end.is_some() {
            break;
        }
    }

    let duration = columns
        .int
        .iter()
        .find(|c| known_duration.contains(&c.as_ref()));

    match (start, end, duration) {
        (Some(start), Some(end), _) => OgrSourceDatasetTimeType::StartEnd {
            start_field: start.clone(),
            start_format: OgrSourceTimeFormat::Auto,
            end_field: end.clone(),
            end_format: OgrSourceTimeFormat::Auto,
        },
        (Some(start), None, Some(duration)) => OgrSourceDatasetTimeType::StartDuration {
            start_field: start.clone(),
            start_format: OgrSourceTimeFormat::Auto,
            duration_field: duration.clone(),
        },
        (Some(start), None, None) => OgrSourceDatasetTimeType::Start {
            start_field: start.clone(),
            start_format: OgrSourceTimeFormat::Auto,
            duration: OgrSourceDurationSpec::Zero,
        },
        _ => OgrSourceDatasetTimeType::None,
    }
}

fn detect_vector_geometry(dataset: &Dataset) -> DetectedGeometry {
    for layer in dataset.layers() {
        for g in layer.defn().geom_fields() {
            if let Ok(data_type) = VectorDataType::try_from_ogr_type_code(g.field_type()) {
                return DetectedGeometry {
                    layer_name: Some(layer.name()),
                    data_type,
                    spatial_reference: g
                        .spatial_ref()
                        .context(error::Gdal)
                        .and_then(|s| {
                            let s: Result<SpatialReference> = s.try_into().context(error::DataType);
                            s
                        })
                        .map(Into::into)
                        .unwrap_or(SpatialReferenceOption::Unreferenced),
                };
            }
        }
    }

    // fallback type if no geometry was found
    DetectedGeometry {
        layer_name: None,
        data_type: VectorDataType::Data,
        spatial_reference: SpatialReferenceOption::Unreferenced,
    }
}

struct GdalAutoDetect {
    dataset: Dataset,
    x: String,
    y: Option<String>,
}

struct DetectedGeometry {
    layer_name: Option<String>,
    data_type: VectorDataType,
    spatial_reference: SpatialReferenceOption,
}

struct Columns {
    int: Vec<String>,
    float: Vec<String>,
    text: Vec<String>,
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
            ColumnDataType::Date => Ok(FeatureDataType::DateTime),
            ColumnDataType::Unknown => Err(error::Error::NoFeatureDataTypeForColumnDataType),
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
    let mut text = Vec::new();
    let mut date = Vec::new();

    for (k, v) in columns {
        match v {
            ColumnDataType::Int => int.push(k.clone()),
            ColumnDataType::Float => float.push(k.clone()),
            ColumnDataType::Text => text.push(k.clone()),
            ColumnDataType::Date => date.push(k.clone()),
            ColumnDataType::Unknown => {}
        }
    }

    Columns {
        int,
        float,
        text,
        date,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::model::datatypes::DatasetId;
    use crate::contexts::{InMemoryContext, Session, SessionId, SimpleContext, SimpleSession};
    use crate::datasets::storage::{AddDataset, DatasetStore};
    use crate::datasets::upload::UploadId;
    use crate::error::Result;
    use crate::projects::{PointSymbology, Symbology};
    use crate::test_data;
    use crate::util::tests::{
        read_body_json, read_body_string, send_test_request, SetMultipartBody, TestDataUploads,
    };
    use actix_web;
    use actix_web::http::header;
    use actix_web_httpauth::headers::authorization::Bearer;
    use futures::TryStreamExt;
    use geoengine_datatypes::collections::{
        GeometryCollection, MultiPointCollection, VectorDataType,
    };
    use geoengine_datatypes::primitives::{BoundingBox2D, SpatialResolution};
    use geoengine_datatypes::raster::{GridShape2D, TilingSpecification};
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::{
        ExecutionContext, InitializedVectorOperator, QueryProcessor, StaticMetaData,
        VectorOperator, VectorResultDescriptor,
    };
    use geoengine_operators::source::{
        OgrSource, OgrSourceDataset, OgrSourceErrorSpec, OgrSourceParameters,
    };
    use serde_json::{json, Value};
    use std::str::FromStr;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_list_datasets() -> Result<()> {
        let ctx = InMemoryContext::test_default();

        let session_id = ctx.default_session_ref().await.id();

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::MultiPoint,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let id = DatasetId::from_str("370e99ec-9fd8-401d-828d-d67b431a8742")?;
        let ds = AddDataset {
            id: Some(id),
            name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: None,
        };

        let meta = StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: String::new(),
                data_type: None,
                time: Default::default(),
                default_geometry: None,
                columns: None,
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
            },
            result_descriptor: descriptor.clone(),
            phantom: Default::default(),
        };

        let _id = ctx
            .dataset_db_ref()
            .add_dataset(&SimpleSession::default(), ds.validated()?, Box::new(meta))
            .await?;

        let id2 = DatasetId::from_str("370e99ec-9fd8-401d-828d-d67b431a8742")?;

        let ds = AddDataset {
            id: Some(id2),
            name: "OgrDataset2".to_string(),
            description: "My Ogr dataset2".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: Some(Symbology::Point(PointSymbology::default())),
            provenance: None,
        };

        let meta = StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: String::new(),
                data_type: None,
                time: Default::default(),
                default_geometry: None,
                columns: None,
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
            },
            result_descriptor: descriptor,
            phantom: Default::default(),
        };

        let _id2 = ctx
            .dataset_db_ref()
            .add_dataset(&SimpleSession::default(), ds.validated()?, Box::new(meta))
            .await?;

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/datasets?{}",
                &serde_urlencoded::to_string([
                    ("order", "NameDesc"),
                    ("offset", "0"),
                    ("limit", "2"),
                ])
                .unwrap()
            ))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            read_body_json(res).await,
            json!([{
                "id": "370e99ec-9fd8-401d-828d-d67b431a8742",
                "name": "OgrDataset2",
                "description": "My Ogr dataset2",
                "tags": [],
                "sourceOperator": "OgrSource",
                "resultDescriptor": {
                    "type": "vector",
                    "dataType": "MultiPoint",
                    "spatialReference": "",
                    "columns": {},
                    "time": null,
                    "bbox": null
                },
                "symbology": {
                    "type": "point",
                    "radius": {
                        "type": "static",
                        "value": 10
                    },
                    "fillColor": {
                        "type": "static",
                        "color": [255, 255, 255, 255]
                    },
                    "stroke": {
                        "width": {
                            "type": "static",
                            "value": 1
                        },
                        "color": {
                            "type": "static",
                            "color": [0, 0, 0, 255]
                        }
                    },
                    "text": null
                }
            }, {
                "id": "370e99ec-9fd8-401d-828d-d67b431a8742",
                "name": "OgrDataset",
                "description": "My Ogr dataset",
                "tags": [],
                "sourceOperator": "OgrSource",
                "resultDescriptor": {
                    "type": "vector",
                    "dataType": "MultiPoint",
                    "spatialReference": "",
                    "columns": {},
                    "time": null,
                    "bbox": null
                },
                "symbology": null
            }])
        );

        Ok(())
    }

    async fn upload_ne_10m_ports_files<C: SimpleContext>(
        ctx: C,
        session_id: SessionId,
    ) -> Result<UploadId> {
        let files = vec![
            test_data!("vector/data/ne_10m_ports/ne_10m_ports.shp").to_path_buf(),
            test_data!("vector/data/ne_10m_ports/ne_10m_ports.shx").to_path_buf(),
            test_data!("vector/data/ne_10m_ports/ne_10m_ports.prj").to_path_buf(),
            test_data!("vector/data/ne_10m_ports/ne_10m_ports.dbf").to_path_buf(),
            test_data!("vector/data/ne_10m_ports/ne_10m_ports.cpg").to_path_buf(),
        ];

        let req = actix_web::test::TestRequest::post()
            .uri("/upload")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_multipart_files(&files);
        let res = send_test_request(req, ctx).await;
        assert_eq!(res.status(), 200);

        let upload: IdResponse<UploadId> = actix_web::test::read_body_json(res).await;
        let root = upload.id.root_path()?;

        for file in files {
            let file_name = file.file_name().unwrap();
            assert!(root.join(file_name).exists());
        }

        Ok(upload.id)
    }

    async fn construct_dataset_from_upload<C: SimpleContext>(
        ctx: C,
        upload_id: UploadId,
        session_id: SessionId,
    ) -> DatasetId {
        let s = format!("{{\"upload\": \"{}\",", upload_id)
            + r#""definition": {
                "properties": {
                    "id": null,
                    "name": "Uploaded Natural Earth 10m Ports",
                    "description": "Ports from Natural Earth",
                    "sourceOperator": "OgrSource"
                },
                "metaData": {
                    "type": "OgrMetaData",
                    "loadingInfo": {
                        "fileName": "ne_10m_ports.shp",
                        "layerName": "ne_10m_ports",
                        "dataType": "MultiPoint",
                        "time": {
                            "type": "none"
                        },
                        "columns": {
                            "x": "",
                            "y": null,
                            "float": ["natlscale"],
                            "int": ["scalerank"],
                            "text": ["featurecla", "name", "website"],
                            "bool": [],
                            "datetime": []
                        },
                        "forceOgrTimeGilter": false,
                        "onError": "ignore",
                        "provenance": null
                    },
                    "resultDescriptor": {
                        "dataType": "MultiPoint",
                        "spatialReference": "EPSG:4326",
                        "columns": {
                            "website": {
                                "dataType": "text",
                                "measurement": {
                                    "type": "unitless"
                                }
                            },
                            "name": {
                                "dataType": "text",
                                "measurement": {
                                    "type": "unitless"
                                }
                            },
                            "natlscale": {
                                "dataType": "float",
                                "measurement": {
                                    "type": "unitless"
                                }
                            },
                            "scalerank": {
                                "dataType": "int",
                                "measurement": {
                                    "type": "unitless"
                                }
                            },
                            "featurecla": {
                                "dataType": "text",
                                "measurement": {
                                    "type": "unitless"
                                }
                            }
                        }
                    }
                }
            }
        }"#;

        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(s);
        let res = send_test_request(req, ctx).await;
        assert_eq!(res.status(), 200);

        let dataset: IdResponse<DatasetId> = actix_web::test::read_body_json(res).await;
        dataset.id
    }

    async fn make_ogr_source<C: ExecutionContext>(
        exe_ctx: &C,
        dataset_id: DatasetId,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        OgrSource {
            params: OgrSourceParameters {
                data: dataset_id.into(),
                attribute_projection: None,
                attribute_filters: None,
            },
        }
        .boxed()
        .initialize(exe_ctx)
        .await
        .map_err(Into::into)
    }

    #[tokio::test]
    async fn create_dataset() -> Result<()> {
        let mut test_data = TestDataUploads::default(); // remember created folder and remove them on drop

        let exe_ctx_tiling_spec = TilingSpecification {
            origin_coordinate: (0., 0.).into(),
            tile_size_in_pixels: GridShape2D::new([600, 600]),
        };

        // override the pixel size since this test was designed for 600 x 600 pixel tiles
        let ctx = InMemoryContext::new_with_context_spec(
            exe_ctx_tiling_spec,
            TestDefault::test_default(),
        );

        let session = ctx.default_session_ref().await;
        let session_id = session.id();

        let upload_id = upload_ne_10m_ports_files(ctx.clone(), session_id).await?;
        test_data.uploads.push(upload_id);

        let dataset_id = construct_dataset_from_upload(ctx.clone(), upload_id, session_id).await;
        let exe_ctx = ctx.execution_context(session.clone())?;

        let source = make_ogr_source(&exe_ctx, dataset_id).await?;

        let query_processor = source.query_processor()?.multi_point().unwrap();
        let query_ctx = ctx.query_context()?;

        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((1.85, 50.88).into(), (4.82, 52.95).into())?,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.)?,
                },
                &query_ctx,
            )
            .await
            .unwrap();

        let result: Vec<MultiPointCollection> = query.try_collect().await?;

        let coords = result[0].coordinates();
        assert_eq!(coords.len(), 10);
        assert_eq!(
            coords,
            &[
                [2.933_686_69, 51.23].into(),
                [3.204_593_64_f64, 51.336_388_89].into(),
                [4.651_413_428, 51.805_833_33].into(),
                [4.11, 51.95].into(),
                [4.386_160_188, 50.886_111_11].into(),
                [3.767_373_38, 51.114_444_44].into(),
                [4.293_757_362, 51.297_777_78].into(),
                [1.850_176_678, 50.965_833_33].into(),
                [2.170_906_949, 51.021_666_67].into(),
                [4.292_873_969, 51.927_222_22].into(),
            ]
        );

        Ok(())
    }

    #[test]
    fn it_auto_detects() {
        let mut meta_data = auto_detect_meta_data_definition(test_data!(
            "vector/data/ne_10m_ports/ne_10m_ports.shp"
        ))
        .unwrap();

        if let MetaDataDefinition::OgrMetaData(meta_data) = &mut meta_data {
            if let Some(columns) = &mut meta_data.loading_info.columns {
                columns.text.sort();
            }
        }

        assert_eq!(
            meta_data,
            MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/ne_10m_ports/ne_10m_ports.shp").into(),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: String::new(),
                        y: None,
                        int: vec!["scalerank".to_string()],
                        float: vec!["natlscale".to_string()],
                        text: vec![
                            "featurecla".to_string(),
                            "name".to_string(),
                            "website".to_string(),
                        ],
                        bool: vec![],
                        datetime: vec![],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        (
                            "name".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "scalerank".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Int,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "website".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "natlscale".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Float,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "featurecla".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless
                            }
                        ),
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default(),
            })
        );
    }

    #[test]
    fn it_detects_time_json() {
        let mut meta_data =
            auto_detect_meta_data_definition(test_data!("vector/data/points_with_iso_time.json"))
                .unwrap();

        if let MetaDataDefinition::OgrMetaData(meta_data) = &mut meta_data {
            if let Some(columns) = &mut meta_data.loading_info.columns {
                columns.datetime.sort();
            }
        }

        assert_eq!(
            meta_data,
            MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/points_with_iso_time.json").into(),
                    layer_name: "points_with_iso_time".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::StartEnd {
                        start_field: "time_start".to_owned(),
                        start_format: OgrSourceTimeFormat::Auto,
                        end_field: "time_end".to_owned(),
                        end_format: OgrSourceTimeFormat::Auto,
                    },
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: String::new(),
                        y: None,
                        float: vec![],
                        int: vec![],
                        text: vec![],
                        bool: vec![],
                        datetime: vec!["time_end".to_owned(), "time_start".to_owned()],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        (
                            "time_start".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "time_end".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        )
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default()
            })
        );
    }

    #[test]
    fn it_detects_time_gpkg() {
        let mut meta_data =
            auto_detect_meta_data_definition(test_data!("vector/data/points_with_time.gpkg"))
                .unwrap();

        if let MetaDataDefinition::OgrMetaData(meta_data) = &mut meta_data {
            if let Some(columns) = &mut meta_data.loading_info.columns {
                columns.datetime.sort();
            }
        }

        assert_eq!(
            meta_data,
            MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/points_with_time.gpkg").into(),
                    layer_name: "points_with_time".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::StartEnd {
                        start_field: "time_start".to_owned(),
                        start_format: OgrSourceTimeFormat::Auto,
                        end_field: "time_end".to_owned(),
                        end_format: OgrSourceTimeFormat::Auto,
                    },
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: String::new(),
                        y: None,
                        float: vec![],
                        int: vec![],
                        text: vec![],
                        bool: vec![],
                        datetime: vec!["time_end".to_owned(), "time_start".to_owned()],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        (
                            "time_start".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "time_end".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        )
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default(),
            })
        );
    }

    #[test]
    fn it_detects_time_shp() {
        let mut meta_data =
            auto_detect_meta_data_definition(test_data!("vector/data/points_with_date.shp"))
                .unwrap();

        if let MetaDataDefinition::OgrMetaData(meta_data) = &mut meta_data {
            if let Some(columns) = &mut meta_data.loading_info.columns {
                columns.datetime.sort();
            }
        }

        assert_eq!(
            meta_data,
            MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/points_with_date.shp").into(),
                    layer_name: "points_with_date".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::StartEnd {
                        start_field: "time_start".to_owned(),
                        start_format: OgrSourceTimeFormat::Auto,
                        end_field: "time_end".to_owned(),
                        end_format: OgrSourceTimeFormat::Auto,
                    },
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: String::new(),
                        y: None,
                        float: vec![],
                        int: vec![],
                        text: vec![],
                        bool: vec![],
                        datetime: vec!["time_end".to_owned(), "time_start".to_owned()],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        (
                            "time_end".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "time_start".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        )
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default(),
            })
        );
    }

    #[test]
    fn it_detects_time_start_duration() {
        let meta_data = auto_detect_meta_data_definition(test_data!(
            "vector/data/points_with_iso_start_duration.json"
        ))
        .unwrap();

        assert_eq!(
            meta_data,
            MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/points_with_iso_start_duration.json").into(),
                    layer_name: "points_with_iso_start_duration".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::StartDuration {
                        start_field: "time_start".to_owned(),
                        start_format: OgrSourceTimeFormat::Auto,
                        duration_field: "duration".to_owned(),
                    },
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: String::new(),
                        y: None,
                        float: vec![],
                        int: vec!["duration".to_owned()],
                        text: vec![],
                        bool: vec![],
                        datetime: vec!["time_start".to_owned()],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        (
                            "time_start".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "duration".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Int,
                                measurement: Measurement::Unitless
                            }
                        )
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default()
            })
        );
    }

    #[test]
    fn it_detects_csv() {
        let mut meta_data =
            auto_detect_meta_data_definition(test_data!("vector/data/lonlat.csv")).unwrap();

        if let MetaDataDefinition::OgrMetaData(meta_data) = &mut meta_data {
            if let Some(columns) = &mut meta_data.loading_info.columns {
                columns.text.sort();
            }
        }

        assert_eq!(
            meta_data,
            MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/lonlat.csv").into(),
                    layer_name: "lonlat".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: "Longitude".to_string(),
                        y: Some("Latitude".to_string()),
                        float: vec![],
                        int: vec![],
                        text: vec![
                            "Latitude".to_string(),
                            "Longitude".to_string(),
                            "Name".to_string()
                        ],
                        bool: vec![],
                        datetime: vec![],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReferenceOption::Unreferenced,
                    columns: [
                        (
                            "Latitude".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "Longitude".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "Name".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless
                            }
                        )
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default()
            })
        );
    }

    #[tokio::test]
    async fn get_dataset() -> Result<()> {
        let ctx = InMemoryContext::test_default();

        let session_id = ctx.default_session_ref().await.id();

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            id: None,
            name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: None,
        };

        let meta = StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: String::new(),
                data_type: None,
                time: Default::default(),
                default_geometry: None,
                columns: None,
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
            },
            result_descriptor: descriptor,
            phantom: Default::default(),
        };

        let id = ctx
            .dataset_db_ref()
            .add_dataset(
                &*ctx.default_session_ref().await,
                ds.validated()?,
                Box::new(meta),
            )
            .await?;

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/dataset/{}", id))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        let res_status = res.status();
        let res_body = serde_json::from_str::<Value>(&read_body_string(res).await).unwrap();
        assert_eq!(res_status, 200, "{}", res_body);

        assert_eq!(
            res_body,
            json!({
                "id": id,
                "name": "OgrDataset",
                "description": "My Ogr dataset",
                "resultDescriptor": {
                    "type": "vector",
                    "dataType": "Data",
                    "spatialReference": "",
                    "columns": {},
                    "time": null,
                    "bbox": null
                },
                "sourceOperator": "OgrSource",
                "symbology": null,
                "provenance": null,
            })
        );

        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_suggests_metadata() -> Result<()> {
        let mut test_data = TestDataUploads::default(); // remember created folder and remove them on drop

        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let body = vec![(
            "test.json",
            r#"{
                "type": "FeatureCollection",
                "features": [
                  {
                    "type": "Feature",
                    "geometry": {
                      "type": "Point",
                      "coordinates": [
                        1,
                        1
                      ]
                    },
                    "properties": {
                      "name": "foo",
                      "id": 1
                    }
                  },
                  {
                    "type": "Feature",
                    "geometry": {
                      "type": "Point",
                      "coordinates": [
                        2,
                        2
                      ]
                    },
                    "properties": {
                      "name": "bar",
                      "id": 2
                    }
                  }
                ]
              }"#,
        )];

        let req = actix_web::test::TestRequest::post()
            .uri("/upload")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_multipart(body.clone());

        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let upload: IdResponse<UploadId> = actix_web::test::read_body_json(res).await;
        test_data.uploads.push(upload.id);

        let upload_content =
            std::fs::read_to_string(upload.id.root_path().unwrap().join("test.json")).unwrap();

        assert_eq!(&upload_content, body[0].1);

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/dataset/suggest?upload={}", upload.id))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        let res_status = res.status();
        let res_body = read_body_string(res).await;
        assert_eq!(res_status, 200, "{}", res_body);

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&res_body).unwrap(),
            json!({
              "mainFile": "test.json",
              "metaData": {
                "type": "OgrMetaData",
                "loadingInfo": {
                  "fileName": format!("test_upload/{}/test.json", upload.id),
                  "layerName": "test",
                  "dataType": "MultiPoint",
                  "time": {
                    "type": "none"
                  },
                  "defaultGeometry": null,
                  "columns": {
                    "formatSpecifics": null,
                    "x": "",
                    "y": null,
                    "int": [
                      "id"
                    ],
                    "float": [],
                    "text": [
                      "name"
                    ],
                    "bool": [],
                    "datetime": [],
                    "rename": null
                  },
                  "forceOgrTimeFilter": false,
                  "forceOgrSpatialFilter": false,
                  "onError": "ignore",
                  "sqlQuery": null,
                  "attributeQuery": null
                },
                "resultDescriptor": {
                  "dataType": "MultiPoint",
                  "spatialReference": "EPSG:4326",
                  "columns": {
                    "id": {
                      "dataType": "int",
                      "measurement": {
                        "type": "unitless"
                      }
                    },
                    "name": {
                      "dataType": "text",
                      "measurement": {
                        "type": "unitless"
                      }
                    }
                  },
                  "time": null,
                  "bbox": null
                }
              }
            })
        );

        Ok(())
    }
}
