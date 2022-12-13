use std::convert::TryInto;
use std::path::Path;

use crate::api::model::datatypes::DatasetId;
use crate::api::model::services::AddDataset;
use crate::datasets::storage::{DatasetDefinition, DatasetStore, MetaDataDefinition};
use crate::datasets::upload::{UploadId, UploadRootPath};
use crate::error;
use crate::error::Result;
use crate::pro::contexts::ProContext;
use crate::pro::projects::ProProjectDb;
use crate::pro::util::config::Odm;
use crate::util::config::get_config_element;
use crate::util::user_input::UserInput;
use crate::util::IdResponse;
use actix_web::{web, Responder};
use futures_util::StreamExt;
use geoengine_datatypes::primitives::Measurement;
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_datatypes::util::Identifier;
use geoengine_operators::engine::RasterResultDescriptor;
use geoengine_operators::source::GdalMetaDataStatic;
use geoengine_operators::util::gdal::{gdal_open_dataset, gdal_parameters_from_dataset};
use log::info;
use reqwest::header::CONTENT_TYPE;
use reqwest::multipart::{self, Part};
use reqwest::Body;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio_util::codec::{BytesCodec, FramedRead};
use uuid::Uuid;

pub(crate) fn init_drone_mapping_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ProContext,
    C::ProjectDB: ProProjectDb,
{
    cfg.service(web::resource("/droneMapping/task").route(web::post().to(start_task_handler::<C>)))
        .service(
            web::resource("/droneMapping/dataset/{task_id}")
                .route(web::post().to(dataset_from_drone_mapping_handler::<C>)),
        );
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TaskStart {
    pub upload: UploadId,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct OdmTaskStartResponse {
    pub uuid: Option<Uuid>,
    pub error: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct OdmTaskNewUploadResponse {
    pub error: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct OdmErrorResponse {
    pub error: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CreateDatasetResponse {
    upload: UploadId,
    dataset: DatasetId,
}

/// Create a new drone mapping task from a given upload. Returns the task id
///
/// # Example
///
/// ```text
/// POST /droneMapping/task
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
///
/// {
///   "upload": "c21fc231-cb3e-4b5b-be67-35248a4b5a10",
/// }
///
/// Response:
/// ```text
/// {
///   "id": "aae098a4-3272-439b-bd93-40b6c39560cb",
/// },
/// ```
async fn start_task_handler<C: ProContext>(
    _session: C::Session,
    _ctx: web::Data<C>,
    task_start: web::Json<TaskStart>,
) -> Result<impl Responder>
where
    C::ProjectDB: ProProjectDb,
{
    let base_url = get_config_element::<Odm>()?.endpoint;

    // TODO: auth
    let client = reqwest::Client::new();

    // create task
    let response: OdmTaskStartResponse = client
        .post(base_url.join("task/new/init").context(error::Url)?)
        .send()
        .await
        .context(error::Reqwest)?
        .json()
        .await
        .context(error::Reqwest)?;

    if let Some(error) = response.error {
        return Err(error::Error::Odm { reason: error });
    };

    let task_id = response.uuid.ok_or(error::Error::OdmInvalidResponse {
        reason: "No task id in response".to_owned(),
    })?;

    let path = task_start.upload.root_path()?;

    let mut dir = fs::read_dir(path).await.context(error::Io)?;

    // upload files individually
    // TODO: parallel/retry?
    // TODO: upload in background task
    while let Some(entry) = dir.next_entry().await.context(error::Io)? {
        if entry.path().is_dir() {
            continue;
        }

        let file = File::open(&entry.path()).await.context(error::Io)?;

        let file_name = entry.file_name().into_string().unwrap(); // TODO

        let reader = Body::wrap_stream(FramedRead::new(file, BytesCodec::new()));

        let form = multipart::Form::new().part("images", Part::stream(reader).file_name(file_name));

        let response: OdmTaskNewUploadResponse = client
            .post(
                base_url
                    .join(&format!("task/new/upload/{}", task_id))
                    .context(error::Url)?,
            )
            .multipart(form)
            .send()
            .await
            .context(error::Reqwest)?
            .json()
            .await
            .context(error::Reqwest)?;

        if let Some(error) = response.error {
            return Err(error::Error::Odm { reason: error });
        };

        info!("Uploaded {:?}", entry);
    }

    // commit (start) the task
    client
        .post(
            base_url
                .join(&format!("task/new/commit/{}", task_id))
                .context(error::Url)?,
        )
        .send()
        .await
        .context(error::Reqwest)?;

    Ok(web::Json(IdResponse::from(task_id)))
}

/// Create a new dataset from the drone mapping result of the task with the given id.
/// Returns the dataset id and the upload id indicating where the odm result data
/// is stored.
///
/// Returns an error if the task is not finished or doesn't exist.
///
/// # Example
///
/// ```text
/// POST /droneMapping/dataset/{task_id}
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
///
/// Response:
/// ```text
/// {
///   "upload": "3086f494-d5a4-4b51-a14b-3b29f8bf7bb0",
///   "dataset": "94230f0b-4e8a-4cba-9adc-3ace837fe5d4"
///   }
/// }
/// ```
async fn dataset_from_drone_mapping_handler<C: ProContext>(
    task_id: web::Path<Uuid>,
    session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder>
where
    C::ProjectDB: ProProjectDb,
{
    let base_url = get_config_element::<Odm>()?.endpoint;

    // TODO: auth
    let client = reqwest::Client::new();

    // request the zip archive of the drone mapping result
    let response = client
        .get(
            base_url
                .join(&format!("task/{}/download/all.zip", task_id))
                .context(error::Url)?,
        )
        .send()
        .await
        .context(error::Reqwest)?;

    // errors come as json, success as zip
    if response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .ok_or(error::Error::OdmMissingContentTypeHeader)?
        .starts_with("application/json")
    {
        let error: OdmErrorResponse = response.json().await.context(error::Reqwest)?;

        return Err(error::Error::Odm {
            reason: error.error.unwrap_or_default(),
        });
    }

    // create a new geo engine upload
    let upload_id = UploadId::new();
    let root = upload_id.root_path()?;
    let zip_path = root.join("all.zip");
    fs::create_dir_all(&root).await.context(error::Io)?;

    // async download of the zip archive
    let zip_file = tokio::fs::File::create(&zip_path)
        .await
        .context(error::Io)?;
    let mut zip_file_writer = tokio::io::BufWriter::new(zip_file);

    let mut download_stream = response.bytes_stream();

    while let Some(item) = download_stream.next().await {
        let chunk = item.context(error::Reqwest)?;
        zip_file_writer.write_all(&chunk).await.context(error::Io)?;
    }
    zip_file_writer.flush().await.context(error::Io)?;

    // TODO: unzip response stream directly (it is actually not even compressed (STORE algorithm))
    unzip(&zip_path, &root).await?;

    // delete archive
    fs::remove_file(zip_path).await.context(error::Io)?;

    // TODO: create dataset(s)
    let tiff_path = root.join("odm_orthophoto").join("odm_orthophoto.tif");

    let dataset_definition = dataset_definition_from_geotiff(&tiff_path).await?;

    let db = ctx.dataset_db_ref();
    let meta = db.wrap_meta_data(dataset_definition.meta_data);

    let dataset = db
        .add_dataset(&session, dataset_definition.properties.validated()?, meta)
        .await?;

    Ok(web::Json(CreateDatasetResponse {
        upload: upload_id,
        dataset,
    }))
}

/// create `DatasetDefinition` from the infos in geotiff at `tiff_path`
async fn dataset_definition_from_geotiff(
    tiff_path: &Path,
) -> Result<DatasetDefinition, error::Error> {
    let tiff_path = tiff_path.to_owned();
    crate::util::spawn_blocking(move || {
        let dataset = gdal_open_dataset(&tiff_path).context(error::Operator)?;

        let gdal_params = gdal_parameters_from_dataset(&dataset, 1, &tiff_path, None, None)
            .context(error::Operator)?;

        let spatial_reference: SpatialReference =
            dataset.spatial_ref()?.try_into().context(error::DataType)?;

        Ok(DatasetDefinition {
            properties: AddDataset {
                id: Some(DatasetId::new()),
                name: "ODM Result".to_owned(), // TODO: more info
                description: String::new(),    // TODO: more info
                source_operator: "GdalSource".to_owned(),
                symbology: None,
                provenance: None,
            },
            meta_data: MetaDataDefinition::GdalStatic(GdalMetaDataStatic {
                time: None,
                params: gdal_params,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: spatial_reference.into(),
                    measurement: Measurement::Unitless,
                    time: None,       // TODO: determine time
                    bbox: None,       // TODO: determine bbox
                    resolution: None, // TODO: determine resolution
                },
            }),
        })
    })
    .await
    .context(error::TokioJoin)?
}

/// unzip the file at `zip_path` into the `target_path`
async fn unzip(zip_path: &Path, target_path: &Path) -> Result<(), error::Error> {
    let zip_path = zip_path.to_owned();
    let target_path = target_path.to_owned();

    crate::util::spawn_blocking(move || {
        let zip_file_read = std::fs::File::open(&zip_path).context(error::Io)?;
        let mut archive = zip::ZipArchive::new(zip_file_read).unwrap(); // TODO

        for i in 0..archive.len() {
            let mut file = archive.by_index(i).unwrap(); // TODO
            let out_path = match file.enclosed_name() {
                Some(path) => target_path.join(path),
                None => continue,
            };

            if file.name().ends_with('/') {
                std::fs::create_dir_all(&out_path).context(error::Io)?; // TODO
            } else {
                if let Some(p) = out_path.parent() {
                    if !p.exists() {
                        std::fs::create_dir_all(p).context(error::Io)?; // TODO
                    }
                }
                let mut outfile = std::fs::File::create(&out_path).context(error::Io)?;
                std::io::copy(&mut file, &mut outfile).context(error::Io)?;
            }
        }
        Ok(())
    })
    .await
    .context(error::TokioJoin)?
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::primitives::{
        RasterQueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::raster::RasterTile2D;
    use geoengine_datatypes::spatial_reference::SpatialReferenceAuthority;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::source::{
        FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalLoadingInfo,
        GdalLoadingInfoTemporalSlice, GdalSource, GdalSourceParameters,
    };
    use httptest::responders::status_code;
    use httptest::{matchers::request, responders::json_encoded, Expectation, Server};
    use serde_json::json;
    use walkdir::WalkDir;
    use zip::write::FileOptions;

    use super::*;
    use crate::contexts::{Context, Session};
    use crate::error::Result;
    use crate::test_data;
    use crate::util::tests::TestDataUploads;
    use crate::{
        pro::{
            contexts::ProInMemoryContext,
            util::tests::{create_session_helper, send_pro_test_request},
        },
        util::config,
    };
    use actix_web::{http::header, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_operators::engine::{MetaData, MetaDataProvider, RasterOperator};
    use serial_test::serial;
    use std::io::Write;
    use std::io::{Cursor, Read};
    use std::path::PathBuf;

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    #[serial]
    async fn it_works() -> Result<()> {
        let mut test_data = TestDataUploads::default(); // remember created folder and remove them on drop

        let mock_nodeodm = Server::run();
        mock_nodeodm.expect(
            Expectation::matching(request::method_path("POST", "/task/new/init")).respond_with(
                json_encoded(json!({
                    "uuid": "9c64ff33-b6d7-4ff9-b63c-aaba37dfb4b7"
                })),
            ),
        );

        mock_nodeodm.expect(
            Expectation::matching(request::method_path(
                "POST",
                "/task/new/upload/9c64ff33-b6d7-4ff9-b63c-aaba37dfb4b7",
            ))
            .times(2)
            .respond_with(json_encoded(json!({}))),
        );

        mock_nodeodm.expect(
            Expectation::matching(request::method_path(
                "POST",
                "/task/new/commit/9c64ff33-b6d7-4ff9-b63c-aaba37dfb4b7",
            ))
            .respond_with(json_encoded(json!({}))),
        );

        // manipulate config to use the mock nodeodm server
        config::set_config("odm.endpoint", mock_nodeodm.url_str("/")).unwrap();

        let ctx = ProInMemoryContext::test_default();
        let session = create_session_helper(&ctx).await;

        // file upload into geo engine
        // TODO: properly upload the data using the handler once this is possible in a test (after migration to actix)
        let upload_id = UploadId::new();
        let upload_dir = upload_id.root_path()?;
        fs::create_dir_all(&upload_dir).await.context(error::Io)?;

        test_data.uploads.push(upload_id);

        // copy the test data into upload directory
        let mut drone_images_dir = fs::read_dir(test_data!("pro/drone_mapping/drone_images"))
            .await
            .context(error::Io)?;

        while let Some(entry) = drone_images_dir.next_entry().await.context(error::Io)? {
            if entry.path().is_dir() {
                continue;
            }
            fs::copy(entry.path(), upload_dir.join(entry.file_name())).await?;
        }

        // submit the task via geo engine
        let task = TaskStart { upload: upload_id };

        let req = test::TestRequest::post()
            .uri("/droneMapping/task")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(&task);
        let res = send_pro_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let task: IdResponse<Uuid> = test::read_body_json(res).await;

        let task_uuid = task.id;

        // create a dataset from the nodeodm result

        // create zip archive from test data
        let odm_test_data_dir = test_data!("pro/drone_mapping/odm_result").into();
        let odm_all_zip_bytes = zip_dir(odm_test_data_dir).await.unwrap();

        mock_nodeodm.expect(
            Expectation::matching(request::method_path(
                "GET",
                format!("/task/{}/download/all.zip", task_uuid),
            ))
            .respond_with(
                status_code(200)
                    .append_header(CONTENT_TYPE, "application/zip")
                    .body(odm_all_zip_bytes),
            ),
        );

        // download odm result through geo engine and create dataset
        let req = test::TestRequest::post()
            .uri(&format!("/droneMapping/dataset/{}", task_uuid))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(task);
        let res = send_pro_test_request(req, ctx.clone()).await;

        let dataset_response: CreateDatasetResponse = test::read_body_json(res).await;
        test_data.uploads.push(dataset_response.upload);

        // test if the meta data is correct
        let dataset_id = dataset_response.dataset;
        let meta: Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> =
            ctx.execution_context(session.clone())
                .unwrap()
                .meta_data(&dataset_id.into())
                .await
                .unwrap();

        let result_descriptor = meta.result_descriptor().await.unwrap();
        assert_eq!(
            result_descriptor,
            RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 32630)
                    .into(),
                measurement: Measurement::Unitless,
                time: None,
                bbox: None,
                resolution: None,
            }
        );

        let query = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked(
                (0.0, 0.0).into(),
                (200.0, -200.0).into(),
            ),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::new_unchecked(1., 1.),
        };

        let mut loading_info = meta.loading_info(query).await.unwrap();

        let part = loading_info.info.next().unwrap().unwrap();
        assert!(loading_info.info.next().is_none());

        let file_path = &part.params.as_ref().unwrap().file_path;

        assert_eq!(
            file_path,
            &dataset_response
                .upload
                .root_path()
                .unwrap()
                .join("odm_orthophoto")
                .join("odm_orthophoto.tif")
        );

        assert_eq!(
            part,
            GdalLoadingInfoTemporalSlice {
                time: TimeInterval::default(),
                params: Some(GdalDatasetParameters {
                    file_path: file_path.clone(),
                    rasterband_channel: 1,
                    geo_transform: GdalDatasetGeoTransform {
                        origin_coordinate: (0.0, 0.0).into(),
                        x_pixel_size: 1.0,
                        y_pixel_size: -1.0,
                    },
                    width: 200,
                    height: 200,
                    file_not_found_handling: FileNotFoundHandling::Error,
                    no_data_value: None,
                    properties_mapping: None,
                    gdal_open_options: None,
                    gdal_config_options: None,
                    allow_alphaband_as_mask: true,
                }),
            }
        );

        // test if the data can be loaded
        let op = GdalSource {
            params: GdalSourceParameters {
                data: dataset_id.into(),
            },
        }
        .boxed();

        let exe_ctx = ctx.execution_context(session.clone()).unwrap();
        let initialized = op.initialize(&exe_ctx).await.unwrap();

        let processor = initialized.query_processor().unwrap().get_u8().unwrap();

        let query_ctx = ctx.query_context(session).unwrap();
        let result = processor.raster_query(query, &query_ctx).await.unwrap();

        let result = result
            .map(Result::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;

        assert_eq!(result.len(), 1);

        Ok(())
    }

    /// create a zip file from the content of `source_dir` and its subfolders and output it as a byte vector
    async fn zip_dir(source_dir: PathBuf) -> Result<Vec<u8>> {
        crate::util::spawn_blocking(move || {
            let mut output = vec![];
            {
                let mut zip = zip::ZipWriter::new(Cursor::new(&mut output));
                let options =
                    FileOptions::default().compression_method(zip::CompressionMethod::Stored);

                let mut buffer = Vec::new();
                for entry in WalkDir::new(&source_dir).min_depth(1) {
                    let entry = entry.unwrap();
                    let path = entry.path();
                    let name = path.strip_prefix(&source_dir).unwrap().to_str().unwrap();

                    if path.is_file() {
                        zip.start_file(name, options).unwrap();
                        let mut f = std::fs::File::open(path)?;
                        f.read_to_end(&mut buffer)?;
                        zip.write_all(&buffer)?;
                        buffer.clear();
                    } else if !name.is_empty() {
                        zip.add_directory(name, options).unwrap();
                    }
                }
                zip.finish().unwrap();
            }
            Ok(output)
        })
        .await
        .context(error::TokioJoin)?
    }
}
