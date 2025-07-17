use actix_web::{FromRequest, HttpResponse, ResponseError, web};
use geoengine_datatypes::machine_learning::MlModelName;
use geoengine_operators::{
    engine::ExecutionContext,
    machine_learning::onnx_util::{
        check_model_shape, check_onnx_model_matches_metadata, load_onnx_model_from_loading_info,
    },
};

use crate::{
    api::model::{
        datatypes::MlModelName as ApiMlModelName,
        responses::{ErrorResponse, ml_models::MlModelNameResponse},
        services::MlModel,
    },
    contexts::{ApplicationContext, SessionContext},
    machine_learning::{MlModelDb, MlModelListOptions, error::MachineLearningError},
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
    let session_context = app_ctx.session_context(session);
    let exe_context = session_context
        .execution_context()
        .expect("Execution Context must exist");

    // convert the payload from json to apy and then to backend type
    let model: crate::machine_learning::MlModel = model.into_inner().into();

    // This call also checks that the file is available!
    let ml_model_metadata = model.loading_info()?;
    // Check that the in/out shapes are ok
    check_model_shape(
        &ml_model_metadata.metadata,
        exe_context.tiling_specification().tile_size_in_pixels,
    )?;
    // initialize model
    let session = load_onnx_model_from_loading_info(&ml_model_metadata)?;
    // Check that the model is initializable and that the types are vaild
    check_onnx_model_matches_metadata(&session, &ml_model_metadata.metadata)?;

    let id_and_name = session_context.db().add_model(model).await?;
    let api_name: crate::api::model::datatypes::MlModelName = id_and_name.name.into();
    Ok(web::Json(api_name.into()))
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
    let models_api = models.into_iter().map(Into::into).collect::<Vec<_>>();
    Ok(web::Json(models_api))
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
        ("model_name" = ApiMlModelName, description = "Ml Model Name")
    ),
    security(
        ("session_token" = [])
    )
)]

pub(crate) async fn get_ml_model<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    model_name: web::Path<ApiMlModelName>,
) -> Result<web::Json<MlModel>, MachineLearningError> {
    let model_name: ApiMlModelName = model_name.into_inner();
    let model_name: MlModelName = model_name.into();

    let models = app_ctx
        .session_context(session)
        .db()
        .load_model(&model_name)
        .await?;
    let models_api = models.into();
    Ok(web::Json(models_api))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::model::responses::IdResponse,
        contexts::{PostgresContext, Session},
        datasets::upload::UploadId,
        ge_context,
        users::UserAuth,
        util::tests::{SetMultipartBody, TestDataUploads, send_test_request},
    };
    use actix_http::header;
    use actix_web::test;
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::{machine_learning::MlTensorShape3D, raster::RasterDataType};
    use geoengine_operators::machine_learning::{
        MlModelInputNoDataHandling, MlModelMetadata, MlModelOutputNoDataHandling,
    };
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

        let model = crate::machine_learning::MlModel {
            name: MlModelName::try_new(Some(session.user.id.to_string()), "test_classification")
                .unwrap(),
            display_name: "Test Classification".to_string(),
            description: "Test Classification Model".to_string(),
            upload: upload.id,
            file_name: "model.onnx".to_string(),
            metadata: MlModelMetadata {
                input_type: RasterDataType::F32,
                output_type: RasterDataType::I64,
                input_shape: MlTensorShape3D::new_y_x_bands(1, 1, 2),
                output_shape: MlTensorShape3D::new_y_x_bands(1, 1, 1),
                input_no_data_handling: MlModelInputNoDataHandling::SkipIfNoData,
                output_no_data_handling: MlModelOutputNoDataHandling::NanIsNoData,
            },
        };

        let api_model: MlModel = model.clone().into();
        let req = test::TestRequest::post()
            .uri("/ml/models")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(&api_model);

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let req = test::TestRequest::get()
            .uri("/ml/models?offset=0&limit=10")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let models: Vec<MlModel> = test::read_body_json(res).await;

        assert_eq!(models.len(), 1);

        assert_eq!(model, models[0].clone().into());

        let req = test::TestRequest::get()
            .uri(&format!("/ml/models/{}", api_model.name))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);

        let res_model: MlModel = test::read_body_json(res).await;

        assert_eq!(model, res_model.into());
    }
}
