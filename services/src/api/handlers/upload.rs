use crate::api::model::responses::IdResponse;
use crate::contexts::{ApplicationContext, SessionContext};
use crate::datasets::upload::{FileId, FileUpload, Upload, UploadDb, UploadId, UploadRootPath};
use crate::error::Result;
use crate::error::{self, Error};
use crate::util::path_with_base_path;
use actix_multipart::Multipart;
use actix_web::{web, FromRequest, Responder};
use futures::StreamExt;
use gdal::vector::LayerAccess;
use geoengine_datatypes::util::Identifier;
use geoengine_operators::util::gdal::gdal_open_dataset;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::path::Path;
use tokio::{fs, io::AsyncWriteExt};
use utoipa::{ToResponse, ToSchema};

pub(crate) fn init_upload_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
    C::Session: FromRequest,
{
    cfg.service(web::resource("/upload").route(web::post().to(upload_handler::<C>)))
        .service(
            web::resource("/uploads/{upload_id}/files")
                .route(web::get().to(list_upload_files_handler)),
        )
        .service(
            web::resource("/uploads/{upload_id}/files/{file_name}/layers")
                .route(web::get().to(list_upload_file_layers_handler)),
        );
}

struct FileUploadRequest;

impl<'a> ToSchema<'a> for FileUploadRequest {
    fn schema() -> (&'a str, utoipa::openapi::RefOr<utoipa::openapi::Schema>) {
        use utoipa::openapi::*;
        (
            "FileUploadRequest",
            ObjectBuilder::new()
                .property(
                    "files[]",
                    ArrayBuilder::new().items(
                        ObjectBuilder::new()
                            .schema_type(SchemaType::String)
                            .format(Some(SchemaFormat::KnownFormat(KnownFormat::Binary))),
                    ),
                )
                .required("files[]")
                .into(),
        )
    }
}

/// Uploads files.
#[utoipa::path(
    tag = "Uploads",
    post,
    path = "/upload",
    request_body(content = inline(FileUploadRequest), content_type = "multipart/form-data"),
    responses(
        (status = 200, response = crate::api::model::responses::IdResponse::<UploadId>)
    ),
    security(
        ("session_token" = [])
    )
)]
async fn upload_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    mut body: Multipart,
) -> Result<web::Json<IdResponse<UploadId>>> {
    let upload_id = UploadId::new();

    let root = upload_id.root_path()?;

    fs::create_dir_all(&root).await.context(error::Io)?;

    let mut files: Vec<FileUpload> = vec![];
    while let Some(item) = body.next().await {
        let mut field = item?;
        let file_name = field
            .content_disposition()
            .get_filename()
            .ok_or(error::Error::UploadFieldMissingFileName)?
            .to_owned();

        let file_id = FileId::new();
        let mut file = fs::File::create(root.join(&file_name))
            .await
            .context(error::Io)?;

        let mut byte_size = 0_u64;
        while let Some(chunk) = field.next().await {
            let bytes = chunk?;
            file.write_all(&bytes).await.context(error::Io)?;
            byte_size += bytes.len() as u64;
        }
        file.flush().await.context(error::Io)?;

        files.push(FileUpload {
            id: file_id,
            name: file_name,
            byte_size,
        });
    }

    app_ctx
        .session_context(session)
        .db()
        .create_upload(Upload {
            id: upload_id,
            files,
        })
        .await?;

    Ok(web::Json(IdResponse::from(upload_id)))
}

#[derive(Deserialize, Serialize, ToSchema, ToResponse)]
pub struct UploadFilesResponse {
    files: Vec<String>,
}

/// List the files of on upload.
#[utoipa::path(
    tag = "Uploads",
    get,
    path = "/uploads/{upload_id}/files",
    responses(
        (status = 200, body = UploadFilesResponse,
            example = json!({"files": ["file1", "file2"]}))
    ),
    params(
        ("upload_id" = UploadId, description = "Upload id"),
    ),
    security(
        ("session_token" = [])
    )
)]
async fn list_upload_files_handler(upload_id: web::Path<UploadId>) -> Result<impl Responder> {
    let root = upload_id.root_path()?;

    let mut entries = fs::read_dir(root).await?;

    let mut files = Vec::new();

    while let Some(entry) = entries.next_entry().await? {
        if entry.path().is_file() {
            files.push(entry.file_name().to_string_lossy().to_string());
        }
    }

    Ok(web::Json(UploadFilesResponse { files }))
}

#[derive(Deserialize, Serialize, ToSchema, ToResponse)]
pub struct UploadFileLayersResponse {
    layers: Vec<String>,
}

/// List the layers of on uploaded file.
#[utoipa::path(
    tag = "Uploads",
    get,
    path = "/uploads/{upload_id}/files/{file_name}/layers",
    responses(
        (status = 200, body = UploadFilesResponse,
             example = json!({"layers": ["layer1", "layer2"]}))
    ),
    params(
        ("upload_id" = UploadId, description = "Upload id"),
        ("file_name" = String, description = "File name")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn list_upload_file_layers_handler(
    path: web::Path<(UploadId, String)>,
) -> Result<impl Responder> {
    let (upload_id, file_name) = path.into_inner();
    let file_path = path_with_base_path(&upload_id.root_path()?, Path::new(&file_name))?;

    let layers = crate::util::spawn_blocking(move || {
        let dataset = gdal_open_dataset(&file_path)?;

        // TODO: hide system/internal layer like "layer_styles"
        Result::<_, Error>::Ok(dataset.layers().map(|l| l.name()).collect::<Vec<_>>())
    })
    .await??;

    Ok(web::Json(UploadFileLayersResponse { layers }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{Session, SimpleApplicationContext};
    use crate::util::tests::with_temp_context;
    use crate::util::tests::{send_test_request, SetMultipartBody, TestDataUploads};
    use actix_web::{http::header, test};
    use actix_web_httpauth::headers::authorization::Bearer;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn upload() {
        let mut test_data = TestDataUploads::default(); // remember created folder and remove them on drop

        with_temp_context(|app_ctx, _| async move {
            let ctx = app_ctx.default_session_context().await.unwrap();
            let session_id = ctx.session().id();

            let body = vec![("bar.txt", "bar"), ("foo.txt", "foo")];

            let req = test::TestRequest::post()
                .uri("/upload")
                .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
                .set_multipart(body);

            let res = send_test_request(req, app_ctx.clone()).await;

            assert_eq!(res.status(), 200);

            let upload: IdResponse<UploadId> = test::read_body_json(res).await;
            test_data.uploads.push(upload.id);

            let root = upload.id.root_path().unwrap();
            assert!(root.join("foo.txt").exists() && root.join("bar.txt").exists());

            let req = test::TestRequest::get()
                .uri(&format!("/uploads/{}/files", upload.id))
                .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

            let res = send_test_request(req, app_ctx).await;

            assert_eq!(res.status(), 200);

            let mut files: UploadFilesResponse = test::read_body_json(res).await;
            files.files.sort();

            assert_eq!(
                files.files,
                vec!["bar.txt".to_string(), "foo.txt".to_string()]
            );
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_lists_layers() {
        let mut test_data = TestDataUploads::default(); // remember created folder and remove them on drop

        with_temp_context(|app_ctx, _| async move {
            let ctx = app_ctx.default_session_context().await.unwrap();
            let session_id = ctx.session().id();

            let files =
                vec![geoengine_datatypes::test_data!("vector/data/two_layers.gpkg").to_path_buf()];

            let req = actix_web::test::TestRequest::post()
                .uri("/upload")
                .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
                .set_multipart_files(&files);

            let res = send_test_request(req, app_ctx.clone()).await;

            assert_eq!(res.status(), 200);

            let upload: IdResponse<UploadId> = test::read_body_json(res).await;
            test_data.uploads.push(upload.id);

            let req = test::TestRequest::get()
                .uri(&format!(
                    "/uploads/{}/files/two_layers.gpkg/layers",
                    upload.id
                ))
                .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

            let res = send_test_request(req, app_ctx).await;

            assert_eq!(res.status(), 200);

            let layers: UploadFileLayersResponse = test::read_body_json(res).await;

            assert_eq!(
                layers.layers,
                vec![
                    "points_with_time".to_string(),
                    "points_with_time_and_more".to_string(),
                    "layer_styles".to_string() // TOOO: remove once internal/system layers are hidden
                ]
            );
        })
        .await;
    }
}
