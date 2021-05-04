use tokio::{fs, io::AsyncWriteExt};

use futures::{Stream, TryStreamExt};
use geoengine_datatypes::util::Identifier;
use warp::Filter;

use crate::datasets::upload::{FileId, FileUpload, Upload, UploadDb, UploadId, UploadRootPath};
use crate::error;
use crate::handlers::{authenticate, Context};
use crate::users::session::Session;
use crate::util::IdResponse;
use bytes::Buf;
use mime::Mime;
use mpart_async::server::MultipartStream;
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
pub(crate) fn upload_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path("upload")
        .and(warp::post())
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::header::<Mime>("content-type"))
        .and(warp::body::stream())
        .and_then(upload)
}

// TODO: move into handler once async closures are available?
async fn upload<C: Context>(
    session: Session,
    ctx: C,
    mime: Mime,
    body: impl Stream<Item = Result<impl Buf, warp::Error>> + Unpin,
) -> Result<impl warp::Reply, warp::Rejection> {
    let boundary = mime
        .get_param("boundary")
        .map(|v| v.to_string())
        .ok_or(error::Error::MultiPartBoundaryMissing)?;

    let mut stream = MultipartStream::new(
        boundary,
        body.map_ok(|mut buf| buf.copy_to_bytes(buf.remaining())),
    );

    let upload_id = UploadId::new();

    let root = upload_id.root_path()?;

    fs::create_dir_all(&root).await.context(error::Io)?;

    let mut files: Vec<FileUpload> = vec![];
    while let Ok(Some(mut field)) = stream.try_next().await {
        let file_name = field
            .filename()
            .map_err(|_| error::Error::UploadFieldMissingFileName)?
            .to_owned();

        let file_id = FileId::new();
        let mut file = fs::File::create(root.join(&file_name))
            .await
            .context(error::Io)?;

        let mut byte_size = 0;
        while let Ok(Some(bytes)) = field.try_next().await {
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
            session.user.id,
            Upload {
                id: upload_id,
                files,
            },
        )
        .await?;

    Ok(warp::reply::json(&IdResponse::from(upload_id)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::InMemoryContext;
    use crate::handlers::handle_rejection;
    use crate::util::tests::create_session_helper;

    #[tokio::test]
    async fn upload() {
        let ctx = InMemoryContext::default();

        let session = create_session_helper(&ctx).await;

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
                format!("Bearer {}", session.id.to_string()),
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
