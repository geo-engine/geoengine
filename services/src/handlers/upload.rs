use tokio::{fs, io::AsyncWriteExt};

use actix_multipart::Multipart;
use actix_web::{web, Responder};
use futures::StreamExt;
use geoengine_datatypes::util::Identifier;

use crate::datasets::upload::{FileId, FileUpload, Upload, UploadDb, UploadId, UploadRootPath};
use crate::error;
use crate::error::Result;
use crate::handlers::Context;
use crate::util::IdResponse;
use snafu::ResultExt;

/// Uploads files.
///
/// # Example
///
/// ```text
/// POST /upload
/// Authorization: Bearer 4f0d02f9-68e8-46fb-9362-80f862b7db54
/// Content-Type: multipart/form-data; boundary=---------------------------10196671711503402186283068890
///
/// ---------------------------10196671711503402186283068890
/// Content-Disposition: form-data; name="files[]"; filename="germany_polygon.gpkg"
/// <Insert SQLite file>
/// ---------------------------10196671711503402186283068890
/// ```
/// Response:
/// ```text
/// {
///   "id": "420b06de-0a7e-45cb-9c1c-ea901b46ab69"
/// }
/// ```
pub(crate) async fn upload_handler<C: Context>(
    session: C::Session,
    ctx: web::Data<C>,
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
            .ok_or(error::Error::UploadFieldMissingFileName)?
            .get_filename()
            .ok_or(error::Error::UploadFieldMissingFileName)?
            .to_owned();

        let file_id = FileId::new();
        let mut file = fs::File::create(root.join(&file_name))
            .await
            .context(error::Io)?;

        let mut byte_size = 0;
        while let Some(chunk) = field.next().await {
            let bytes = chunk?;
            file.write_all(&bytes).await.context(error::Io)?;
            byte_size += bytes.len();
        }

        files.push(FileUpload {
            id: file_id,
            name: file_name,
            byte_size,
        });
    }

    ctx.dataset_db_ref_mut()
        .await
        .create_upload(
            &session,
            Upload {
                id: upload_id,
                files,
            },
        )
        .await?;

    Ok(web::Json(IdResponse::from(upload_id)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{InMemoryContext, Session, SimpleContext};
    use crate::handlers::handle_rejection;

    #[tokio::test]
    async fn upload() {
        let ctx = InMemoryContext::default();

        let session_id = ctx.default_session_ref().await.id();

        let body = r#"-----------------------------10196671711503402186283068890
Content-Disposition: form-data; name="files[]"; filename="bar.txt"
Content-Type: text/plain

bar
-----------------------------10196671711503402186283068890
Content-Disposition: form-data; name="files[]"; filename="foo.txt"
Content-Type: text/plain

foo
-----------------------------10196671711503402186283068890--
"#
        .to_string();

        let res = warp::test::request()
            .method("POST")
            .path("/upload")
            .header("Content-Length", body.len())
            .header(
                "Authorization",
                format!("Bearer {}", session_id.to_string()),
            )
            .header("Content-Type", "multipart/form-data; boundary=---------------------------10196671711503402186283068890")
            .body(
                body,
            )
            .reply(&upload_handler(ctx).recover(handle_rejection))
            .await;

        assert_eq!(res.status(), 200);

        let body = std::str::from_utf8(&res.body()).unwrap();
        let _upload = serde_json::from_str::<IdResponse<UploadId>>(&body).unwrap();

        // TODO: fix: body doesn't arrive at handler in test
        // let root = upload.id.root_path().unwrap();
        // assert!(root.join("foo.txt").exists() && root.join("bar.txt").exists());

        // TODO: delete upload directory or configure test settings to use temp dir
    }
}
