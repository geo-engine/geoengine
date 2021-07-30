use std::convert::TryInto;
use std::path::Path;

use crate::datasets::storage::{AddDataset, DatasetDefinition, DatasetStore, MetaDataDefinition};
use crate::datasets::upload::{UploadId, UploadRootPath};
use crate::error;
use crate::handlers::authenticate;
use crate::pro::contexts::ProContext;
use crate::pro::projects::ProProjectDb;
use crate::util::config::{get_config_element, Odm};
use crate::util::user_input::UserInput;
use crate::util::IdResponse;

use futures_util::StreamExt;
use geoengine_datatypes::dataset::InternalDatasetId;
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
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio_util::codec::{BytesCodec, FramedRead};
use uuid::Uuid;
use warp::hyper::Body;
use warp::Filter;

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
pub(crate) fn start_task_handler<C: ProContext>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone
where
    C::ProjectDB: ProProjectDb,
{
    warp::path!("droneMapping" / "task")
        .and(warp::post())
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::body::json())
        .and_then(start_task)
}

// TODO: move into handler once async closures are available?
async fn start_task<C: ProContext>(
    _session: C::Session,
    _ctx: C,
    task_start: TaskStart,
) -> Result<impl warp::Reply, warp::Rejection>
where
    C::ProjectDB: ProProjectDb,
{
    let base_url = get_config_element::<Odm>()?.endpoint;

    // TODO: auth
    let client = reqwest::Client::new();

    // create task
    let response: OdmTaskStartResponse = client
        .post(format!("{}task/new/init", base_url))
        .send()
        .await
        .context(error::Reqwest)?
        .json()
        .await
        .context(error::Reqwest)?;

    if let Some(error) = response.error {
        return Err(error::Error::Odm { reason: error }.into());
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
            .post(format!("{}task/new/upload/{}", base_url, task_id))
            .multipart(form)
            .send()
            .await
            .context(error::Reqwest)?
            .json()
            .await
            .context(error::Reqwest)?;

        if let Some(error) = response.error {
            return Err(error::Error::Odm { reason: error }.into());
        };

        info!("Uploaded {:?}", entry);
    }

    // commit (start) the task
    client
        .post(format!("{}task/new/commit/{}", base_url, task_id))
        .send()
        .await
        .context(error::Reqwest)?;

    Ok(warp::reply::json(&IdResponse::from(task_id)))
}

/// Create a new dataset from the drone mapping result of the task with the given id.
/// Returns the dataset id.
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
///   "id": {
///     "type": "internal",
///     "datasetId": "94230f0b-4e8a-4cba-9adc-3ace837fe5d4"
///   }
/// }
/// ```
pub(crate) fn dataset_from_drone_mapping_handler<C: ProContext>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone
where
    C::ProjectDB: ProProjectDb,
{
    warp::path!("droneMapping" / "dataset" / Uuid)
        .and(warp::post())
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and_then(dataset_from_drone_mapping)
}

// TODO: move into handler once async closures are available?
async fn dataset_from_drone_mapping<C: ProContext>(
    task_id: Uuid,
    session: C::Session,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection>
where
    C::ProjectDB: ProProjectDb,
{
    let base_url = get_config_element::<Odm>()?.endpoint;

    // TODO: auth
    let client = reqwest::Client::new();

    // request the zip archive of the drone mapping result
    let response = client
        .get(format!(
            "{}task/{}/download/{}",
            base_url, task_id, "all.zip"
        ))
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
            reason: error.error.unwrap_or_else(|| "".to_owned()),
        }
        .into());
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

    let mut db = ctx.dataset_db_ref_mut().await;
    let meta = db.wrap_meta_data(dataset_definition.meta_data);

    let dataset_id = db
        .add_dataset(&session, dataset_definition.properties.validated()?, meta)
        .await?;

    Ok(warp::reply::json(&IdResponse::from(dataset_id)))
}

/// create `DatasetDefinition` from the infos in geotiff at `tiff_path`
async fn dataset_definition_from_geotiff(
    tiff_path: &Path,
) -> Result<DatasetDefinition, error::Error> {
    let tiff_path = tiff_path.to_owned();
    tokio::task::spawn_blocking(move || {
        let dataset = gdal_open_dataset(&tiff_path).context(error::Operator)?;

        let gdal_params = gdal_parameters_from_dataset(&dataset, 1, &tiff_path, None, None)
            .context(error::Operator)?;

        let spatial_reference: SpatialReference =
            dataset.spatial_ref()?.try_into().context(error::DataType)?;

        Ok(DatasetDefinition {
            properties: AddDataset {
                id: Some(InternalDatasetId::new().into()),
                name: "ODM Result".to_owned(), // TODO: more info
                description: "".to_owned(),    // TODO: more info
                source_operator: "GdalSource".to_owned(),
                symbology: None,
            },
            meta_data: MetaDataDefinition::GdalStatic(GdalMetaDataStatic {
                time: None,
                params: gdal_params,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: spatial_reference.into(),
                    measurement: Measurement::Unitless,
                    no_data_value: None, // TODO
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

    tokio::task::spawn_blocking(move || {
        let zip_file_read = std::fs::File::open(&zip_path).context(error::Io)?;
        let mut archive = zip::ZipArchive::new(zip_file_read).unwrap(); // TODO

        for i in 0..archive.len() {
            let mut file = archive.by_index(i).unwrap(); // TODO
            let out_path = match file.enclosed_name() {
                Some(path) => target_path.join(path),
                None => continue,
            };

            if (&*file.name()).ends_with('/') {
                std::fs::create_dir_all(&out_path).context(error::Io)?; // TODO
            } else {
                if let Some(p) = out_path.parent() {
                    if !p.exists() {
                        std::fs::create_dir_all(&p).context(error::Io)?; // TODO
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
