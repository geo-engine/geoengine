use tokio::{fs, io::AsyncWriteExt};

use actix_multipart::Multipart;
use actix_web::{web, FromRequest, Responder};
use futures::StreamExt;
use geoengine_datatypes::util::Identifier;

use crate::contexts::ApplicationContext;
use crate::datasets::upload::{FileId, FileUpload, Upload, UploadDb, UploadId, UploadRootPath};
use crate::error;
use crate::error::Result;
use crate::handlers::SessionContext;
use crate::util::IdResponse;
use snafu::ResultExt;
use utoipa::ToSchema;

pub(crate) fn init_upload_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
    C::Session: FromRequest,
{
    cfg.service(web::resource("/upload").route(web::post().to(upload_handler::<C>)));
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
        (status = 200, response = crate::api::model::responses::IdResponse)
    ),
    security(
        ("session_token" = [])
    )
)]
async fn upload_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    mut body: Multipart,
) -> Result<impl Responder> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{InMemoryContext, Session, SimpleApplicationContext};
    use crate::util::tests::{send_test_request, SetMultipartBody, TestDataUploads};
    use actix_web::{http::header, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::util::test::TestDefault;

    #[tokio::test]
    async fn upload() {
        let mut test_data = TestDataUploads::default(); // remember created folder and remove them on drop

        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;
        let session_id = ctx.session().id();

        let body = vec![("bar.txt", "bar"), ("foo.txt", "foo")];

        let req = test::TestRequest::post()
            .uri("/upload")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_multipart(body);

        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);

        let upload: IdResponse<UploadId> = test::read_body_json(res).await;
        test_data.uploads.push(upload.id);

        let root = upload.id.root_path().unwrap();
        assert!(root.join("foo.txt").exists() && root.join("bar.txt").exists());
    }
}
