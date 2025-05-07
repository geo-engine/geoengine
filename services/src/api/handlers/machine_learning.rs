use actix_web::{FromRequest, HttpResponse, ResponseError, web};

use crate::{
    api::model::responses::{ErrorResponse, ml_models::MlModelNameResponse},
    contexts::{ApplicationContext, SessionContext},
    machine_learning::{
        MlModel, MlModelDb, MlModelListOptions, error::MachineLearningError, name::MlModelName,
    },
};

pub(crate) fn init_ml_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
    C::Session: FromRequest,
{
    cfg.service(
        web::scope("/ml").service(
            web::scope("/models")
                .service(
                    web::resource("")
                        .route(web::post().to(add_ml_model::<C>))
                        .route(web::get().to(list_ml_models::<C>)),
                )
                .service(web::resource("/{model_name}").route(web::get().to(get_ml_model::<C>))),
        ),
    );
}

impl ResponseError for MachineLearningError {
    fn status_code(&self) -> actix_http::StatusCode {
        match self {
            MachineLearningError::Postgres { .. } | MachineLearningError::Bb8 { .. } => {
                actix_http::StatusCode::INTERNAL_SERVER_ERROR
            }
            _ => actix_http::StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> HttpResponse<actix_http::body::BoxBody> {
        HttpResponse::build(self.status_code()).json(ErrorResponse::from(self))
    }
}

/// Create a new ml model.
#[utoipa::path(
    tag = "ML",
    post,
    path = "/ml/models",
    request_body = MlModel,
    responses(
        (status = 200, body = MlModelNameResponse)
    ),
    security(
        ("session_token" = [])
    )
)]

pub(crate) async fn add_ml_model<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    model: web::Json<MlModel>,
) -> Result<web::Json<MlModelNameResponse>, MachineLearningError> {
    let model = model.into_inner();
    let id_and_name = app_ctx
        .session_context(session)
        .db()
        .add_model(model)
        .await?;
    Ok(web::Json(id_and_name.name.into()))
}

/// List ml models.
#[utoipa::path(
    tag = "ML",
    get,
    path = "/ml/models",
    responses(
        (status = 200, body = [MlModel])
    ),
    security(
        ("session_token" = [])
    )
)]

pub(crate) async fn list_ml_models<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    options: web::Query<MlModelListOptions>,
) -> Result<web::Json<Vec<MlModel>>, MachineLearningError> {
    let options = options.into_inner();
    let models = app_ctx
        .session_context(session)
        .db()
        .list_models(&options)
        .await?;
    Ok(web::Json(models))
}

/// Get ml model by name.
#[utoipa::path(
    tag = "ML",
    get,
    path = "/ml/models/{model_name}",
    responses(
        (status = 200, body = MlModel)
    ),
    params(
        ("model_name" = MlModelName, description = "Ml Model Name")
    ),
    security(
        ("session_token" = [])
    )
)]

pub(crate) async fn get_ml_model<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    model_name: web::Path<MlModelName>,
) -> Result<web::Json<MlModel>, MachineLearningError> {
    let model_name = model_name.into_inner();

    let models = app_ctx
        .session_context(session)
        .db()
        .load_model(&model_name)
        .await?;
    Ok(web::Json(models))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::model::{datatypes::RasterDataType, datatypes::MlTensorShape3D, responses::IdResponse},
        contexts::PostgresContext,
        contexts::Session,
        datasets::upload::UploadId,
        ge_context,
        machine_learning::MlModelMetadata,
        users::UserAuth,
        util::tests::{SetMultipartBody, TestDataUploads, send_test_request},
    };
    use actix_http::header;
    use actix_web::test;
    use actix_web_httpauth::headers::authorization::Bearer;
    use tokio_postgres::NoTls;

    #[ge_context::test]
    async fn it_stores_ml_models_for_application(app_ctx: PostgresContext<NoTls>) {
        let mut test_data = TestDataUploads::default(); // remember created folder and remove them on drop

        let session = app_ctx.create_anonymous_session().await.unwrap();
        let session_id = session.id();

        let body = vec![(
            "model.onnx",
            include_bytes!("../../../../test_data/ml/onnx/test_classification.onnx"),
        )];

        let req = test::TestRequest::post()
            .uri("/upload")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_multipart(body);

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let upload: IdResponse<UploadId> = test::read_body_json(res).await;
        test_data.uploads.push(upload.id);

        let model = MlModel {
            name: MlModelName::new(Some(session.user.id.to_string()), "test_classification"),
            display_name: "Test Classification".to_string(),
            description: "Test Classification Model".to_string(),
            upload: upload.id,
            metadata: MlModelMetadata {
                file_name: "model.onnx".to_string(),
                input_type: RasterDataType::F32,
                output_type: RasterDataType::I64,
                input_shape: MlTensorShape3D::new_y_x_attr(1, 1, 2),
                output_shape: MlTensorShape3D::new_y_x_attr(1, 1, 1),
            },
        };

        let req = test::TestRequest::post()
            .uri("/ml/models")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(&model);

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let req = test::TestRequest::get()
            .uri("/ml/models?offset=0&limit=10")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let models: Vec<MlModel> = test::read_body_json(res).await;

        assert_eq!(models.len(), 1);

        assert_eq!(model, models[0]);

        let req = test::TestRequest::get()
            .uri(&format!("/ml/models/{}", model.name))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);

        let res_model: MlModel = test::read_body_json(res).await;

        assert_eq!(model, res_model);
    }
}
