use actix_web::{web, FromRequest, HttpResponse, ResponseError};

use crate::{
    api::model::responses::ErrorResponse,
    contexts::{ApplicationContext, SessionContext},
    machine_learning::{
        error::MachineLearningError, name::MlModelName, MlModel, MlModelDb, MlModelListOptions,
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
        (status = 200)
    ),
    security(
        ("session_token" = [])
    )
)]

pub(crate) async fn add_ml_model<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    model: web::Json<MlModel>,
) -> Result<HttpResponse, MachineLearningError> {
    let model = model.into_inner();
    app_ctx
        .session_context(session)
        .db()
        .add_model(model)
        .await?;
    Ok(HttpResponse::Ok().finish())
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
