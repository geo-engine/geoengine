use crate::api::model::responses::IdResponse;
use crate::config;
use crate::contexts::ApplicationContext;
use crate::contexts::SessionContext;
use crate::error;
use crate::error::Result;
use crate::permissions::{RoleDescription, RoleId};
use crate::projects::ProjectId;
use crate::projects::STRectangle;
use crate::quota::ComputationQuota;
use crate::quota::DataUsage;
use crate::quota::DataUsageSummary;
use crate::quota::OperatorQuota;
use crate::users::UserAuth;
use crate::users::UserDb;
use crate::users::UserId;
use crate::users::UserRegistration;
use crate::users::UserSession;
use crate::users::{AuthCodeRequestURL, AuthCodeResponse, RoleDb, UserCredentials};
use crate::util::extractors::ValidatedJson;
use actix_web::FromRequest;
use actix_web::{web, HttpResponse, Responder};
use geoengine_datatypes::error::BoxedResultExt;
use serde::Deserialize;
use serde::Serialize;
use snafu::ensure;
use snafu::ResultExt;
use utoipa::IntoParams;
use utoipa::ToSchema;
use uuid::Uuid;

pub(crate) fn init_user_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext<Session = UserSession>,
    C::Session: FromRequest,
{
    cfg.service(web::resource("/user").route(web::post().to(register_user_handler::<C>)))
        .service(web::resource("/anonymous").route(web::post().to(anonymous_handler::<C>)))
        .service(web::resource("/login").route(web::post().to(login_handler::<C>)))
        .service(web::resource("/logout").route(web::post().to(logout_handler::<C>)))
        .service(web::resource("/session").route(web::get().to(session_handler::<C>)))
        .service(
            web::resource("/session/project/{project}")
                .route(web::post().to(session_project_handler::<C>)),
        )
        .service(web::resource("/session/view").route(web::post().to(session_view_handler::<C>)))
        .service(web::resource("/quota").route(web::get().to(quota_handler::<C>)))
        .service(
            web::resource("/quota/computations")
                .route(web::get().to(computations_quota_handler::<C>)),
        )
        .service(
            web::resource("/quota/computations/{computation}")
                .route(web::get().to(computation_quota_handler::<C>)),
        )
        .service(web::resource("/quota/dataUsage").route(web::get().to(data_usage_handler::<C>)))
        .service(
            web::resource("/quota/dataUsage/summary")
                .route(web::get().to(data_usage_summary_handler::<C>)),
        )
        .service(
            web::resource("/quotas/{user}")
                .route(web::get().to(get_user_quota_handler::<C>))
                .route(web::post().to(update_user_quota_handler::<C>)),
        )
        .service(web::resource("/oidcInit").route(web::post().to(oidc_init::<C>)))
        .service(web::resource("/oidcLogin").route(web::post().to(oidc_login::<C>)))
        .service(web::resource("/roles").route(web::put().to(add_role_handler::<C>)))
        .service(
            web::resource("/roles/byName/{name}")
                .route(web::get().to(get_role_by_name_handler::<C>)),
        )
        .service(web::resource("/roles/{role}").route(web::delete().to(remove_role_handler::<C>)))
        .service(
            web::resource("/users/{user}/roles/{role}")
                .route(web::post().to(assign_role_handler::<C>))
                .route(web::delete().to(revoke_role_handler::<C>)),
        )
        .service(
            web::resource("/user/roles/descriptions")
                .route(web::get().to(get_role_descriptions::<C>)),
        );
}

/// Registers a user.
#[utoipa::path(
    tag = "Session",
    post,
    path = "/user",
    request_body = UserRegistration,
    responses(
        (status = 200, description = "The id of the created user", body = UserId,
            example = json!({
                "id": "5b4466d2-8bab-4ed8-a182-722af3c80958"
            })
        )
    ))]
pub(crate) async fn register_user_handler<C: ApplicationContext + UserAuth>(
    user: ValidatedJson<UserRegistration>,
    app_ctx: web::Data<C>,
) -> Result<impl Responder> {
    ensure!(
        config::get_config_element::<crate::config::User>()?.registration,
        error::UserRegistrationDisabled
    );

    let user = user.into_inner();
    let id = app_ctx.register_user(user).await?;
    Ok(web::Json(IdResponse::from(id)))
}

/// Creates a session by providing user credentials. The session's id serves as a Bearer token for requests.
#[utoipa::path(
tag = "Session",
post,
path = "/login",
request_body = UserCredentials,
responses(
    (status = 200, description = "The created session", body = UserSession,
        example = json!({
            "id": "208fa24e-7a92-4f57-a3fe-d1177d9f18ad",
            "user": {
                "id": "5b4466d2-8bab-4ed8-a182-722af3c80958",
                "email": "foo@example.com",
                "realName": "Foo Bar"
            },
            "created": "2021-04-26T13:47:10.579724800Z",
            "validUntil": "2021-04-26T14:47:10.579775400Z",
            "project": null,
            "view": null,
            "roles": [
                "fa5be363-bc0d-4bfa-85c7-ebb5cd9a8783",
                "4e8081b6-8aa6-4275-af0c-2fa2da557d28"
            ]
        })
    )
))]
pub(crate) async fn login_handler<C: ApplicationContext + UserAuth>(
    user: ValidatedJson<UserCredentials>,
    app_ctx: web::Data<C>,
) -> Result<impl Responder> {
    let session = app_ctx
        .login(user.into_inner())
        .await
        .map_err(Box::new)
        .context(error::Unauthorized)?;
    Ok(web::Json(session))
}

/// Ends a session.
#[utoipa::path(
    tag = "Session",
    post,
    path = "/logout",
    responses(
        (status = 200, description = "The Session was deleted.")
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn logout_handler<C: ApplicationContext<Session = UserSession>>(
    session: UserSession,
    app_ctx: web::Data<C>,
) -> Result<impl Responder> {
    app_ctx.session_context(session).db().logout().await?;
    Ok(HttpResponse::Ok())
}

/// Retrieves details about the current session.
#[utoipa::path(
    tag = "Session",
    get,
    path = "/session",
    responses(
        (status = 200, description = "The current session", body = UserSession,
            example = json!({
                "id": "208fa24e-7a92-4f57-a3fe-d1177d9f18ad",
                "user": {
                    "id": "5b4466d2-8bab-4ed8-a182-722af3c80958",
                    "email": "foo@example.com",
                    "realName": "Foo Bar"
                },
                "created": "2021-04-26T13:47:10.579724800Z",
                "validUntil": "2021-04-26T14:47:10.579775400Z",
                "project": null,
                "view": null,
                "roles": [
                    "fa5be363-bc0d-4bfa-85c7-ebb5cd9a8783",
                    "4e8081b6-8aa6-4275-af0c-2fa2da557d28"
                ]
            })
        )
    ),
    security(
        ("session_token" = [])
    )
)]
#[allow(clippy::unused_async)] // the function signature of request handlers requires it
pub(crate) async fn session_handler<C: ApplicationContext>(session: C::Session) -> impl Responder {
    web::Json(session)
}

/// Creates session for anonymous user. The session's id serves as a Bearer token for requests.
#[utoipa::path(
    tag = "Session",
    post,
    path = "/anonymous",
    responses(
        (status = 200, description = "The created session", body = UserSession,
            example = json!({
                "id": "208fa24e-7a92-4f57-a3fe-d1177d9f18ad",
                "user": {
                    "id": "5b4466d2-8bab-4ed8-a182-722af3c80958",
                    "email": null,
                    "realName": null
                },
                "created": "2021-04-26T13:47:10.579724800Z",
                "validUntil": "2021-04-26T14:47:10.579775400Z",
                "project": null,
                "view": null,
                "roles": [
                    "8a27e61f-cc4d-4d0b-ae8c-4f1c91d07f5a",
                    "fd8e87bf-515c-4f36-8da6-1a53702ff102"
                ]
            })
        )
    )
)]
pub(crate) async fn anonymous_handler<C: ApplicationContext + UserAuth>(
    app_ctx: web::Data<C>,
) -> Result<impl Responder> {
    if !config::get_config_element::<crate::config::Session>()?.anonymous_access {
        return Err(error::Error::Unauthorized {
            source: Box::new(error::Error::AnonymousAccessDisabled),
        });
    }

    let session = app_ctx.create_anonymous_session().await?;
    Ok(web::Json(session))
}

/// Sets the active project of the session.
#[utoipa::path(
    tag = "Session",
    post,
    path = "/session/project/{project}",
    responses(
        (status = 200, description = "The project of the session was updated."),
    ),
    params(
        ("project" = ProjectId, description = "Project id")
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn session_project_handler<C: ApplicationContext<Session = UserSession>>(
    project: web::Path<ProjectId>,
    session: UserSession,
    app_ctx: web::Data<C>,
) -> Result<impl Responder> {
    app_ctx
        .session_context(session)
        .db()
        .set_session_project(project.into_inner())
        .await?;

    Ok(HttpResponse::Ok())
}

// TODO: /view instead of /session/view
#[utoipa::path(
    tag = "Session",
    post,
    path = "/session/view",
    request_body = STRectangle,
    responses(
        (status = 200, description = "The view of the session was updated."),
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn session_view_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    view: web::Json<STRectangle>,
) -> Result<impl Responder> {
    app_ctx
        .session_context(session)
        .db()
        .set_session_view(view.into_inner())
        .await?;

    Ok(HttpResponse::Ok())
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct Quota {
    pub available: i64,
    pub used: u64,
}

/// Retrieves the available and used quota of the current user.
#[utoipa::path(
    tag = "User",
    get,
    path = "/quota",
    responses(
        (status = 200, description = "The available and used quota of the user", body = Quota,
            example = json!({
                "available": 4321,
                "used": 1234,
            })
        )
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn quota_handler<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    session: C::Session,
) -> Result<web::Json<Quota>> {
    let db = app_ctx.session_context(session).db();
    let available = db.quota_available().await?;
    let used = db.quota_used().await?;

    Ok(web::Json(Quota { available, used }))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ComputationQuotaParams {
    pub offset: usize,
    pub limit: usize,
}

/// Retrieves the quota used by computations
#[utoipa::path(
    tag = "User",
    get,
    path = "/quota/computations",
    responses(
        (status = 200, description = "The quota used by computations", body = Vec<ComputationQuota>)
    ),
    security(
        ("session_token" = [])
    ),
    params(
        ComputationQuotaParams
    )
)]
pub(crate) async fn computations_quota_handler<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    params: web::Query<ComputationQuotaParams>,
    session: C::Session,
) -> Result<web::Json<Vec<ComputationQuota>>> {
    let params = params.into_inner();

    let db = app_ctx.session_context(session).db();
    let computations_quota = db
        .quota_used_by_computations(params.offset, params.limit)
        .await?;

    Ok(web::Json(computations_quota))
}

/// Retrieves the quota used by computation with the given computation id
#[utoipa::path(
    tag = "User",
    get,
    path = "/quota/computations/{computation}",
    responses(
        (status = 200, description = "The quota used by computation", body = Vec<OperatorQuota>)
    ),
    security(
        ("session_token" = [])
    ),
    params(
        ("computation" = Uuid, description = "Computation id")
    )
)]
pub(crate) async fn computation_quota_handler<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    computation: web::Path<Uuid>,
    session: C::Session,
) -> Result<web::Json<Vec<OperatorQuota>>> {
    let computation = computation.into_inner();

    let db = app_ctx.session_context(session).db();
    let computation_quota = db.quota_used_by_computation(computation).await?;

    Ok(web::Json(computation_quota))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct UsageParams {
    pub offset: u64,
    pub limit: u64,
}

/// Retrieves the data usage
#[utoipa::path(
    tag = "User",
    get,
    path = "/quota/dataUsage",
    responses(
        (status = 200, description = "The quota used on data", body = Vec<DataUsage>)
    ),
    params(
        UsageParams
    ),
    security(
        ("session_token" = [])
    ),
)]
pub(crate) async fn data_usage_handler<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    params: web::Query<UsageParams>,
    session: C::Session,
) -> Result<web::Json<Vec<DataUsage>>> {
    let params = params.into_inner();

    let db = app_ctx.session_context(session).db();
    let data_usage = db.quota_used_on_data(params.offset, params.limit).await?;

    Ok(web::Json(data_usage))
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum UsageSummaryGranularity {
    Minutes,
    Hours,
    Days,
    Months,
    Years,
}

#[derive(Debug, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct UsageSummaryParams {
    pub granularity: UsageSummaryGranularity,
    pub offset: u64,
    pub limit: u64,
    pub dataset: Option<String>,
}

/// Retrieves the data usage summary
#[utoipa::path(
    tag = "User",
    get,
    path = "/quota/dataUsage/summary",
    responses(
        (status = 200, description = "The quota used on data", body = Vec<DataUsageSummary>)
    ),
    params(
        UsageSummaryParams
    ),
    security(
        ("session_token" = [])
    ),
)]
pub(crate) async fn data_usage_summary_handler<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    params: web::Query<UsageSummaryParams>,
    session: C::Session,
) -> Result<web::Json<Vec<DataUsageSummary>>> {
    let params = params.into_inner();

    let db = app_ctx.session_context(session).db();
    let data_usage = db
        .quota_used_on_data_summary(
            params.dataset,
            params.granularity,
            params.offset,
            params.limit,
        )
        .await?;

    Ok(web::Json(data_usage))
}

/// Retrieves the available and used quota of a specific user.
#[utoipa::path(
    tag = "User",
    get,
    path = "/quotas/{user}",
    responses(
        (status = 200, description = "The available and used quota of the user", body = Quota,
            example = json!({
                "available": 4321,
                "used": 1234,
            })
        )
    ),
    params(
        ("user" = UserId, description = "User id")
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn get_user_quota_handler<C: ApplicationContext<Session = UserSession>>(
    app_ctx: web::Data<C>,
    session: UserSession,
    user: web::Path<UserId>,
) -> Result<web::Json<Quota>> {
    let user = user.into_inner();

    if session.user.id != user && !session.is_admin() {
        return Err(error::Error::Unauthorized {
            source: Box::new(error::Error::OperationRequiresAdminPrivilige),
        });
    }

    let ctx = app_ctx.session_context(session);
    let db = ctx.db();
    let available = db.quota_available_by_user(&user).await?;
    let used = db.quota_used_by_user(&user).await?;
    Ok(web::Json(Quota { available, used }))
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct UpdateQuota {
    pub available: i64,
}

/// Update the available quota of a specific user.
#[utoipa::path(
    tag = "User",
    post,
    path = "/quotas/{user}",
    request_body = UpdateQuota,
    responses(
        (status = 200, description = "Quota was updated")
    ),
    params(
        ("user" = UserId, description = "User id")
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn update_user_quota_handler<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    session: C::Session,
    user: web::Path<UserId>,
    update: web::Json<UpdateQuota>,
) -> Result<HttpResponse> {
    let user = user.into_inner();

    let update = update.into_inner();

    app_ctx
        .session_context(session)
        .db()
        .update_quota_available_by_user(&user, update.available)
        .await?;

    Ok(actix_web::HttpResponse::Ok().finish())
}

#[derive(Debug, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct RedirectUri {
    pub redirect_uri: String,
}

/// Initializes the Open Id Connect login procedure by requesting a parametrized url to the configured Id Provider.
///
/// # Errors
///
/// This call fails if Open ID Connect is disabled, misconfigured or the Id Provider is unreachable.
///
#[utoipa::path(
    tag = "Session",
    post,
    path = "/oidcInit",
    responses(
        (status = 200, body = AuthCodeRequestURL,
        example = json!({
            "url": "http://someissuer.com/authorize?client_id=someclient&redirect_uri=someuri&response_type=code&scope=somescope&state=somestate&nonce=somenonce&codechallenge=somechallenge&code_challenge_method=S256"
        })
    )),
    params(
        RedirectUri
    )
)]
pub(crate) async fn oidc_init<C: ApplicationContext>(
    params: web::Query<RedirectUri>,
    app_ctx: web::Data<C>,
) -> Result<web::Json<AuthCodeRequestURL>> {
    ensure!(
        config::get_config_element::<crate::config::Oidc>()?.enabled,
        crate::users::OidcDisabled
    );
    let auth_code_request_url = app_ctx
        .oidc_manager()
        .get_client_with_redirect_uri(params.into_inner().redirect_uri)
        .await?
        .generate_request()
        .await?;

    Ok::<web::Json<AuthCodeRequestURL>, crate::error::Error>(web::Json(auth_code_request_url))
}

/// Creates a session for a user via a login with Open Id Connect.
/// This call must be preceded by a call to oidcInit and match the parameters of that call.
///
/// # Errors
///
/// This call fails if the [`AuthCodeResponse`] is invalid,
/// if a previous oidcLogin call with the same state was already successfully or unsuccessfully resolved,
/// if the Open Id Connect configuration is invalid,
/// or if the Id Provider is unreachable.
///
#[utoipa::path(
    tag = "Session",
    post,
    path = "/oidcLogin",
    request_body = AuthCodeResponse,
    responses(
        (status = 200, body = UserSession,
        example = json!({
           "id": "208fa24e-7a92-4f57-a3fe-d1177d9f18ad",
           "user": {
             "id": "5b4466d2-8bab-4ed8-a182-722af3c80958",
             "email": "foo@bar.de",
             "realName": "Foo Bar"
           },
           "created": "2021-04-26T13:47:10.579724800Z",
           "validUntil": "2021-04-26T14:47:10.579775400Z",
           "project": null,
           "view": null
        })
    )),
    params(
        RedirectUri
    )
)]
pub(crate) async fn oidc_login<C: ApplicationContext + UserAuth>(
    response: web::Json<AuthCodeResponse>,
    params: web::Query<RedirectUri>,
    app_ctx: web::Data<C>,
) -> Result<web::Json<UserSession>> {
    ensure!(
        config::get_config_element::<crate::config::Oidc>()?.enabled,
        crate::users::OidcDisabled
    );
    let authentication_response = app_ctx
        .oidc_manager()
        .get_client_with_redirect_uri(params.into_inner().redirect_uri)
        .await?
        .resolve_request(response.into_inner())
        .await?;

    let session = app_ctx
        .login_external(
            authentication_response.user_claims,
            authentication_response.oidc_tokens,
        )
        .await?;

    Ok(web::Json(session))
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct AddRole {
    pub name: String,
}

/// Add a new role. Requires admin privilige.
#[utoipa::path(
    tag = "User",
    put,
    path = "/roles",
    request_body = AddRole,
    responses(
        (status = 200, description = "Role was added", body = RoleId,
        example = json!({
            "id": "5b4466d2-8bab-4ed8-a182-722af3c80958"
        })
    )),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn add_role_handler<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    session: C::Session,
    add_role: web::Json<AddRole>,
) -> Result<web::Json<IdResponse<RoleId>>> {
    let add_role = add_role.into_inner();

    let id = app_ctx
        .session_context(session)
        .db()
        .add_role(&add_role.name)
        .await
        .boxed_context(crate::error::RoleDb)?;

    Ok(web::Json(IdResponse::from(id)))
}

/// Get role by name
#[utoipa::path(
    tag = "User",
    get,
    path = "/roles/byName/{name}",
    responses(
        (status = 200, response = IdResponse<RoleId>)
    ),
    params(
        ("name" = String, description = "Role Name")
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn get_role_by_name_handler<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    session: C::Session,
    role_name: web::Path<String>,
) -> Result<web::Json<IdResponse<RoleId>>> {
    let role_name = role_name.into_inner();

    let role_id = app_ctx
        .session_context(session)
        .db()
        .load_role_by_name(&role_name)
        .await
        .boxed_context(crate::error::RoleDb)?;

    Ok(web::Json(IdResponse::from(role_id)))
}

/// Remove a role. Requires admin privilige.
#[utoipa::path(
    tag = "User",
    delete,
    path = "/roles/{role}",
    responses(
        (status = 200, description = "Role was removed")
    ),
    params(
        ("role" = RoleId, description = "Role id")
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn remove_role_handler<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    session: C::Session,
    role: web::Path<RoleId>,
) -> Result<HttpResponse> {
    let role = role.into_inner();

    app_ctx
        .session_context(session)
        .db()
        .remove_role(&role)
        .await
        .boxed_context(crate::error::RoleDb)?;

    Ok(actix_web::HttpResponse::Ok().finish())
}

/// Assign a role to a user. Requires admin privilige.
#[utoipa::path(
    tag = "User",
    post,
    path = "/users/{user}/roles/{role}",
    responses(
        (status = 200, description = "Role was assigned")
    ),
    params(
        ("user" = UserId, description = "User id"),
        ("role" = RoleId, description = "Role id")
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn assign_role_handler<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    session: C::Session,
    user: web::Path<(UserId, RoleId)>,
) -> Result<HttpResponse> {
    let (user, role) = user.into_inner();

    app_ctx
        .session_context(session)
        .db()
        .assign_role(&role, &user)
        .await
        .boxed_context(crate::error::RoleDb)?;

    Ok(actix_web::HttpResponse::Ok().finish())
}

/// Revoke a role from a user. Requires admin privilige.
#[utoipa::path(
    tag = "User",
    delete,
    path = "/users/{user}/roles/{role}",
    responses(
        (status = 200, description = "Role was revoked")
    ),
    params(
        ("user" = UserId, description = "User id"),
        ("role" = RoleId, description = "Role id")
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn revoke_role_handler<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    session: C::Session,
    user: web::Path<(UserId, RoleId)>,
) -> Result<HttpResponse> {
    let (user, role) = user.into_inner();

    app_ctx
        .session_context(session)
        .db()
        .revoke_role(&role, &user)
        .await
        .boxed_context(crate::error::RoleDb)?;

    Ok(actix_web::HttpResponse::Ok().finish())
}

/// Query roles for the current user.
#[utoipa::path(
    tag = "User",
    get,
    path = "/user/roles/descriptions",
    responses(
        (status = 200,
        description = "The description for roles of the current user",
        body = Vec<RoleDescription>,
        example = json!([{
                        "role": {
                            "id": "5b4466d2-8bab-4ed8-a182-722af3c80958",
                            "name": "foo@example.com"
                        },
                        "individual": true
                    },
                    {
                        "role": {
                            "id": "fa5be363-bc0d-4bfa-85c7-ebb5cd9a8783",
                            "name": "Example role"
                        },
                        "individual": false
                    }
                ])
        )
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn get_role_descriptions<C: ApplicationContext<Session = UserSession>>(
    app_ctx: web::Data<C>,
    session: C::Session,
) -> Result<web::Json<Vec<RoleDescription>>> {
    let user = session.user.id;

    let res = app_ctx
        .session_context(session)
        .db()
        .get_role_descriptions(&user)
        .await
        .boxed_context(crate::error::RoleDb)?;

    Ok(web::Json(res))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::model::datatypes::RasterColorizer;
    use crate::api::model::responses::ErrorResponse;
    use crate::config::Oidc;
    use crate::contexts::PostgresContext;
    use crate::contexts::{Session, SessionContext};
    use crate::ge_context;
    use crate::permissions::Role;
    use crate::users::{AuthCodeRequestURL, OidcManager, UserAuth, UserId};
    use crate::util::tests::mock_oidc::{
        mock_refresh_server, mock_token_response, mock_valid_provider_discovery,
        MockRefreshServerConfig, MockTokenConfig, SINGLE_STATE,
    };
    use crate::util::tests::{
        admin_login, create_project_helper2, create_session_helper, register_ndvi_workflow_helper,
    };
    use crate::util::tests::{check_allowed_http_methods, read_body_string, send_test_request};
    use actix_http::header::CONTENT_TYPE;
    use actix_web::dev::ServiceResponse;
    use actix_web::{http::header, http::Method, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use core::time::Duration;
    use geoengine_datatypes::operations::image::{Colorizer, RgbaColor};
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use geoengine_operators::engine::WorkflowOperatorPath;
    use geoengine_operators::meta::quota::ComputationUnit;
    use httptest::matchers::request;
    use httptest::responders::status_code;
    use httptest::{Expectation, Server};
    use serde_json::json;
    use tokio_postgres::NoTls;
    use uuid::Uuid;

    const DUMMY_REDIRECT_URI: &str = "http://dummy.redirect-uri.com";

    async fn register_test_helper(
        app_ctx: PostgresContext<NoTls>,
        method: Method,
        email: &str,
    ) -> ServiceResponse {
        let user = UserRegistration {
            email: email.to_string(),
            password: "secret123".to_string(),
            real_name: " Foo Bar".to_string(),
        };

        // register user
        let req = test::TestRequest::default()
            .method(method)
            .uri("/user")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_json(&user);
        send_test_request(req, app_ctx).await
    }

    #[ge_context::test]
    async fn register(app_ctx: PostgresContext<NoTls>) {
        let res = register_test_helper(app_ctx, Method::POST, "foo@example.com").await;

        assert_eq!(res.status(), 200);

        let _user: IdResponse<UserId> = test::read_body_json(res).await;
    }

    #[ge_context::test]
    async fn register_fail(app_ctx: PostgresContext<NoTls>) {
        let res = register_test_helper(app_ctx, Method::POST, "notanemail").await;

        ErrorResponse::assert(res, 400, "ValidationError", "email: invalid email\n").await;
    }

    #[ge_context::test]
    async fn register_duplicate_email(app_ctx: PostgresContext<NoTls>) {
        register_test_helper(app_ctx.clone(), Method::POST, "foo@example.com").await;

        // register user
        let res = register_test_helper(app_ctx, Method::POST, "foo@example.com").await;

        ErrorResponse::assert(
            res,
            409,
            "Duplicate",
            "Tried to create duplicate: E-mail already exists",
        )
        .await;
    }

    #[ge_context::test]
    async fn register_invalid_method(app_ctx: PostgresContext<NoTls>) {
        check_allowed_http_methods(
            |method| register_test_helper(app_ctx.clone(), method, "foo@example.com"),
            &[Method::POST],
        )
        .await;
    }

    #[ge_context::test]
    async fn register_invalid_body(app_ctx: PostgresContext<NoTls>) {
        // register user
        let req = test::TestRequest::post()
            .uri("/user")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_payload("no json");
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            415,
            "UnsupportedMediaType",
            "Unsupported content type header.",
        )
        .await;
    }

    #[ge_context::test]
    async fn register_missing_fields(app_ctx: PostgresContext<NoTls>) {
        let user = json!({
            "password": "secret123",
            "real_name": " Foo Bar",
        });

        // register user
        let req = test::TestRequest::post()
            .uri("/user")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_json(&user);
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "Error in user input: missing field `email` at line 1 column 47",
        )
        .await;
    }

    #[ge_context::test]
    async fn register_invalid_type(app_ctx: PostgresContext<NoTls>) {
        // register user
        let req = test::TestRequest::post()
            .uri("/user")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::CONTENT_TYPE, "text/html"));
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            415,
            "UnsupportedMediaType",
            "Unsupported content type header.",
        )
        .await;
    }

    async fn login_test_helper(
        app_ctx: PostgresContext<NoTls>,
        method: Method,
        password: &str,
    ) -> ServiceResponse {
        let password = password.to_string();

        let user = UserRegistration {
            email: "foo@example.com".to_string(),
            password: "secret123".to_string(),
            real_name: " Foo Bar".to_string(),
        };

        // TODO: remove user at the end of method if possible and then unwrap this
        let _ = app_ctx.register_user(user).await; // .unwrap();

        let credentials = UserCredentials {
            email: "foo@example.com".to_string(),
            password,
        };

        let req = test::TestRequest::default()
            .method(method)
            .uri("/login")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_json(&credentials);

        send_test_request(req, app_ctx).await
    }

    #[ge_context::test]
    async fn login(app_ctx: PostgresContext<NoTls>) {
        let res = login_test_helper(app_ctx, Method::POST, "secret123").await;

        assert_eq!(res.status(), 200);

        let _id: UserSession = test::read_body_json(res).await;
    }

    #[ge_context::test]
    async fn login_fail(app_ctx: PostgresContext<NoTls>) {
        let res = login_test_helper(app_ctx, Method::POST, "wrongpassword").await;

        ErrorResponse::assert(
            res,
            401,
            "Unauthorized",
            "Authorization error: User does not exist or password is wrong.",
        )
        .await;
    }

    #[ge_context::test]
    async fn login_invalid_method(app_ctx: PostgresContext<NoTls>) {
        check_allowed_http_methods(
            |method| login_test_helper(app_ctx.clone(), method, "secret123"),
            &[Method::POST],
        )
        .await;
    }

    #[ge_context::test]
    async fn login_invalid_body(app_ctx: PostgresContext<NoTls>) {
        let req = test::TestRequest::post()
            .uri("/login")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::CONTENT_TYPE, mime::APPLICATION_JSON))
            .set_payload("no json");
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "Error in user input: expected ident at line 1 column 2",
        )
        .await;
    }

    #[ge_context::test]
    async fn login_missing_fields(app_ctx: PostgresContext<NoTls>) {
        let user = UserRegistration {
            email: "foo@example.com".to_string(),
            password: "secret123".to_string(),
            real_name: " Foo Bar".to_string(),
        };

        // TODO: remove user at the end of method if possible and then unwrap this
        let _ = app_ctx.register_user(user).await; // .unwrap();

        let credentials = json!({
            "email": "foo@example.com",
        });

        let req = test::TestRequest::post()
            .uri("/login")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_json(&credentials);
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "Error in user input: missing field `password` at line 1 column 27",
        )
        .await;
    }

    async fn logout_test_helper(
        app_ctx: PostgresContext<NoTls>,
        method: Method,
    ) -> ServiceResponse {
        let user = UserRegistration {
            email: "foo@example.com".to_string(),
            password: "secret123".to_string(),
            real_name: " Foo Bar".to_string(),
        };

        // TODO: remove user at the end of method if possible and then unwrap this
        let _ = app_ctx.register_user(user).await; // .unwrap();

        let credentials = UserCredentials {
            email: "foo@example.com".to_string(),
            password: "secret123".to_string(),
        };

        let session = app_ctx.login(credentials).await.unwrap();

        let req = test::TestRequest::default()
            .method(method)
            .uri("/logout")
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_payload("no json");
        send_test_request(req, app_ctx).await
    }

    #[ge_context::test]
    async fn logout(app_ctx: PostgresContext<NoTls>) {
        let res = logout_test_helper(app_ctx, Method::POST).await;

        assert_eq!(res.status(), 200);
        assert_eq!(read_body_string(res).await, "");
    }

    #[ge_context::test]
    async fn logout_missing_header(app_ctx: PostgresContext<NoTls>) {
        let req = test::TestRequest::post().uri("/logout");
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "Unauthorized",
            "Authorization error: Header with authorization token not provided.",
        )
        .await;
    }

    #[ge_context::test]
    async fn logout_wrong_token(app_ctx: PostgresContext<NoTls>) {
        let req = test::TestRequest::post().uri("/logout").append_header((
            header::AUTHORIZATION,
            Bearer::new("6ecff667-258e-4108-9dc9-93cb8c64793c"),
        ));
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "Unauthorized",
            "Authorization error: The session id is invalid.",
        )
        .await;
    }

    #[ge_context::test]
    async fn logout_wrong_scheme(app_ctx: PostgresContext<NoTls>) {
        let req = test::TestRequest::post()
            .uri("/logout")
            .append_header((header::AUTHORIZATION, "7e855f3c-b0cd-46d1-b5b3-19e6e3f9ea5"));
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "Unauthorized",
            "Authorization error: Authentication scheme must be Bearer.",
        )
        .await;
    }

    #[ge_context::test]
    async fn logout_invalid_token(app_ctx: PostgresContext<NoTls>) {
        let req = test::TestRequest::post()
            .uri("/logout")
            .append_header((header::AUTHORIZATION, format!("Bearer {}", "no uuid")));
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "Unauthorized",
            "Authorization error: Identifier does not have the right format.",
        )
        .await;
    }

    #[ge_context::test]
    async fn logout_invalid_method(app_ctx: PostgresContext<NoTls>) {
        check_allowed_http_methods(
            |method| logout_test_helper(app_ctx.clone(), method),
            &[Method::POST],
        )
        .await;
    }

    #[ge_context::test]
    async fn session(app_ctx: PostgresContext<NoTls>) {
        let session = create_session_helper(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let req = test::TestRequest::get()
            .uri("/session")
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx.clone()).await;

        let session: UserSession = test::read_body_json(res).await;
        let db = ctx.db();

        db.logout().await.unwrap();

        let req = test::TestRequest::get()
            .uri("/session")
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "Unauthorized",
            "Authorization error: The session id is invalid.",
        )
        .await;
    }

    #[ge_context::test]
    async fn session_view_project(app_ctx: PostgresContext<NoTls>) {
        let (session, project) = create_project_helper2(&app_ctx).await;

        let req = test::TestRequest::post()
            .uri(&format!("/session/project/{project}"))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            app_ctx
                .user_session_by_id(session.id,)
                .await
                .unwrap()
                .project,
            Some(project)
        );

        let rect =
            STRectangle::new_unchecked(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1);
        let req = test::TestRequest::post()
            .append_header((header::CONTENT_LENGTH, 0))
            .uri("/session/view")
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(&rect);
        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            app_ctx
                .user_session_by_id(session.id(),)
                .await
                .unwrap()
                .view,
            Some(rect)
        );
    }

    #[ge_context::test(test_execution = "serial")]
    async fn it_disables_anonymous_access(app_ctx: PostgresContext<NoTls>) {
        let req = test::TestRequest::post().uri("/anonymous");
        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        config::set_config("session.anonymous_access", false).unwrap();

        let req = test::TestRequest::post().uri("/anonymous");
        let res = send_test_request(req, app_ctx.clone()).await;

        // required to not corrupt other tests
        config::set_config("session.anonymous_access", true).unwrap();

        ErrorResponse::assert(
            res,
            401,
            "Unauthorized",
            "Authorization error: Anonymous access is disabled, please log in",
        )
        .await;
    }

    #[ge_context::test(test_execution = "serial")]
    async fn it_disables_user_registration(app_ctx: PostgresContext<NoTls>) {
        let user_reg = UserRegistration {
            email: "foo@example.com".to_owned(),
            password: "secret123".to_owned(),
            real_name: "Foo Bar".to_owned(),
        };

        let req = test::TestRequest::post()
            .append_header((header::CONTENT_LENGTH, 0))
            .uri("/user")
            .set_json(&user_reg);
        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        config::set_config("user.registration", false).unwrap();

        let user_reg = UserRegistration {
            email: "foo@example.com".to_owned(),
            password: "secret123".to_owned(),
            real_name: "Foo Bar".to_owned(),
        };

        let req = test::TestRequest::post()
            .append_header((header::CONTENT_LENGTH, 0))
            .uri("/user")
            .set_json(&user_reg);
        let res = send_test_request(req, app_ctx.clone()).await;

        config::set_config("user.registration", true).unwrap();

        ErrorResponse::assert(
            res,
            400,
            "UserRegistrationDisabled",
            "User registration is disabled",
        )
        .await;
    }

    const MOCK_CLIENT_ID: &str = "";

    fn single_state_nonce_request_db(issuer: String) -> OidcManager {
        let oidc_config = Oidc {
            enabled: true,
            issuer,
            client_id: MOCK_CLIENT_ID.to_string(),
            client_secret: None,
            scopes: vec!["profile".to_string(), "email".to_string()],
            token_encryption_password: None,
        };

        OidcManager::from_oidc_with_static_tokens(oidc_config)
    }

    async fn oidc_init_test_helper(method: Method, ctx: PostgresContext<NoTls>) -> ServiceResponse {
        let req = test::TestRequest::default()
            .method(method)
            .uri(&format!("/oidcInit?redirectUri={DUMMY_REDIRECT_URI}"))
            .append_header((header::CONTENT_LENGTH, 0));
        send_test_request(req, ctx).await
    }

    async fn oidc_login_test_helper(
        method: Method,
        ctx: PostgresContext<NoTls>,
        auth_code_response: AuthCodeResponse,
    ) -> ServiceResponse {
        let req = test::TestRequest::default()
            .method(method)
            .uri(&format!("/oidcLogin?redirectUri={DUMMY_REDIRECT_URI}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .set_json(&auth_code_response);
        send_test_request(req, ctx).await
    }

    fn oidc_attr_for_test() -> (Server, impl Fn() -> OidcManager) {
        let server = mock_valid_provider_discovery(1);
        let server_url = format!("http://{}", server.addr());

        (server, move || {
            single_state_nonce_request_db(server_url.clone())
        })
    }

    #[ge_context::test(oidc_db = "oidc_attr_for_test")]
    async fn oidc_init(app_ctx: PostgresContext<NoTls>) {
        let res = oidc_init_test_helper(Method::POST, app_ctx).await;

        assert_eq!(res.status(), 200);
        let _auth_code_url: AuthCodeRequestURL = test::read_body_json(res).await;
    }

    fn oidc_bad_server() -> (Server, impl Fn() -> OidcManager) {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());

        let error_message = serde_json::to_string(&json!({
            "error_description": "Dummy bad request",
            "error": "catch_all_error"
        }))
        .expect("Serde Json unsuccessful");

        server.expect(
            Expectation::matching(request::method_path(
                "GET",
                "/.well-known/openid-configuration",
            ))
            .respond_with(
                status_code(404)
                    .insert_header("content-type", "application/json")
                    .body(error_message),
            ),
        );

        (server, move || {
            single_state_nonce_request_db(server_url.clone())
        })
    }

    #[ge_context::test(oidc_db = "oidc_bad_server")]
    async fn oidc_illegal_provider(app_ctx: PostgresContext<NoTls>) {
        let res = oidc_init_test_helper(Method::POST, app_ctx).await;

        ErrorResponse::assert_eq_message_starts_with(
            res,
            400,
            "Oidc",
            "OidcError: ProviderDiscoveryError: Server returned invalid response: HTTP status code 404 Not Found",
        ).await;
    }

    #[ge_context::test]
    async fn oidc_init_invalid_method(app_ctx: PostgresContext<NoTls>) {
        check_allowed_http_methods(
            |method| oidc_init_test_helper(method, app_ctx.clone()),
            &[Method::POST],
        )
        .await;
    }

    fn oidc_login_oidc_db() -> (Server, impl Fn() -> OidcManager) {
        let server = mock_valid_provider_discovery(2);
        let server_url = format!("http://{}", server.addr());

        let mock_token_config = MockTokenConfig::create_from_issuer_and_client(
            server_url.clone(),
            MOCK_CLIENT_ID.to_string(),
        );
        let token_response = mock_token_response(mock_token_config);
        server.expect(
            Expectation::matching(request::method_path("POST", "/token")).respond_with(
                status_code(200)
                    .insert_header("content-type", "application/json")
                    .body(serde_json::to_string(&token_response).unwrap()),
            ),
        );

        (server, move || {
            single_state_nonce_request_db(server_url.clone())
        })
    }

    #[ge_context::test(oidc_db = "oidc_login_oidc_db")]
    async fn oidc_login(app_ctx: PostgresContext<NoTls>) {
        let auth_code_response = AuthCodeResponse {
            session_state: String::new(),
            code: String::new(),
            state: SINGLE_STATE.to_string(),
        };

        let request = app_ctx
            .oidc_manager()
            .get_client_with_redirect_uri(DUMMY_REDIRECT_URI.to_string())
            .await
            .unwrap()
            .generate_request()
            .await;

        assert!(request.is_ok());

        let res = oidc_login_test_helper(Method::POST, app_ctx, auth_code_response).await;

        assert_eq!(res.status(), 200);

        let _id: UserSession = test::read_body_json(res).await;
    }

    fn oidc_login_illegal_request_oidc_db() -> (Server, impl Fn() -> OidcManager) {
        let server = mock_valid_provider_discovery(1);
        let server_url = format!("http://{}", server.addr());

        (server, move || {
            single_state_nonce_request_db(server_url.clone())
        })
    }

    #[ge_context::test(oidc_db = "oidc_login_illegal_request_oidc_db")]
    async fn oidc_login_illegal_request(app_ctx: PostgresContext<NoTls>) {
        let auth_code_response = AuthCodeResponse {
            session_state: String::new(),
            code: String::new(),
            state: SINGLE_STATE.to_string(),
        };

        let res = oidc_login_test_helper(Method::POST, app_ctx, auth_code_response).await;

        ErrorResponse::assert(res, 400, "Oidc", "OidcError: Login failed: Request unknown").await;
    }

    fn oidc_login_fail_oidc_db() -> (Server, impl Fn() -> OidcManager) {
        let server = mock_valid_provider_discovery(2);
        let server_url = format!("http://{}", server.addr());

        let error_message = serde_json::to_string(&json!({
            "error_description": "Dummy bad request",
            "error": "catch_all_error"
        }))
        .expect("Serde Json unsuccessful");

        server.expect(
            Expectation::matching(request::method_path("POST", "/token")).respond_with(
                status_code(404)
                    .insert_header("content-type", "application/json")
                    .body(error_message),
            ),
        );

        (server, move || {
            single_state_nonce_request_db(server_url.clone())
        })
    }

    #[ge_context::test(oidc_db = "oidc_login_fail_oidc_db")]
    async fn oidc_login_fail(app_ctx: PostgresContext<NoTls>) {
        let auth_code_response = AuthCodeResponse {
            session_state: String::new(),
            code: String::new(),
            state: SINGLE_STATE.to_string(),
        };

        let request = app_ctx
            .oidc_manager()
            .get_client_with_redirect_uri(DUMMY_REDIRECT_URI.to_string())
            .await
            .unwrap()
            .generate_request()
            .await;

        assert!(request.is_ok());

        let res = oidc_login_test_helper(Method::POST, app_ctx, auth_code_response).await;

        ErrorResponse::assert(
            res,
            400,
            "Oidc",
            "OidcError: Verification failed: Request for code to token exchange failed",
        )
        .await;
    }

    #[ge_context::test]
    async fn oidc_login_invalid_method(app_ctx: PostgresContext<NoTls>) {
        let auth_code_response = AuthCodeResponse {
            session_state: String::new(),
            code: String::new(),
            state: String::new(),
        };

        check_allowed_http_methods(
            |method| oidc_login_test_helper(method, app_ctx.clone(), auth_code_response.clone()),
            &[Method::POST],
        )
        .await;
    }

    #[ge_context::test]
    async fn oidc_login_invalid_body(app_ctx: PostgresContext<NoTls>) {
        let req = test::TestRequest::post()
            .uri("/oidcLogin")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_payload("no json");
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            415,
            "UnsupportedMediaType",
            "Unsupported content type header.",
        )
        .await;
    }

    #[ge_context::test]
    async fn oidc_login_missing_fields(app_ctx: PostgresContext<NoTls>) {
        let auth_code_response = json!({
            "sessionState": "",
            "code": "",
        });

        // register user
        let req = test::TestRequest::post()
            .uri("/oidcLogin")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_json(&auth_code_response);
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "Error in user input: missing field `state` at line 1 column 29",
        )
        .await;
    }

    #[ge_context::test]
    async fn oidc_login_invalid_type(app_ctx: PostgresContext<NoTls>) {
        // register user
        let req = test::TestRequest::post()
            .uri("/oidcLogin")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::CONTENT_TYPE, "text/html"));
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            415,
            "UnsupportedMediaType",
            "Unsupported content type header.",
        )
        .await;
    }

    pub fn oidc_refresh() -> (Server, impl Fn() -> OidcManager) {
        let mock_refresh_server_config = MockRefreshServerConfig {
            expected_discoveries: 3,
            token_duration: Duration::from_secs(1),
            creates_first_token: true,
            first_access_token: "FIRST_ACCESS_TOKEN".to_string(),
            first_refresh_token: "FIRST_REFRESH_TOKEN".to_string(),
            second_access_token: "SECOND_ACCESS_TOKEN".to_string(),
            second_refresh_token: "SECOND_REFRESH_TOKEN".to_string(),
            client_side_password: None,
        };

        let (server, oidc_manager) = mock_refresh_server(mock_refresh_server_config);

        (server, move || {
            OidcManager::from_oidc_with_static_tokens(oidc_manager.clone())
        })
    }

    #[ge_context::test(oidc_db = "oidc_refresh")]
    async fn oidc_login_refresh_session(app_ctx: PostgresContext<NoTls>) {
        let auth_code_response = AuthCodeResponse {
            session_state: String::new(),
            code: String::new(),
            state: SINGLE_STATE.to_string(),
        };

        let request = app_ctx
            .oidc_manager()
            .get_client_with_redirect_uri(DUMMY_REDIRECT_URI.to_string())
            .await
            .unwrap()
            .generate_request()
            .await;

        assert!(request.is_ok());

        let res = oidc_login_test_helper(Method::POST, app_ctx.clone(), auth_code_response).await;

        assert_eq!(res.status(), 200);

        let original_session: UserSession = test::read_body_json(res).await;

        //Access token duration oidc_login_refresh is 1 sec, i.e., session times out after 1 sec.
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        let req = actix_web::test::TestRequest::get()
            .uri("/session")
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(original_session.id.to_string()),
            ));
        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let updated_session: UserSession = test::read_body_json(res).await;

        assert!(original_session.valid_until < updated_session.valid_until);
    }

    fn oidc_short_duration() -> (Server, impl Fn() -> OidcManager) {
        let server = mock_valid_provider_discovery(2);
        let server_url = format!("http://{}", server.addr());

        let mut mock_token_config = MockTokenConfig::create_from_issuer_and_client(
            server_url.clone(),
            MOCK_CLIENT_ID.to_string(),
        );
        mock_token_config.duration = Some(Duration::from_secs(1));
        let token_response = mock_token_response(mock_token_config);
        server.expect(
            Expectation::matching(request::method_path("POST", "/token")).respond_with(
                status_code(200)
                    .insert_header("content-type", "application/json")
                    .body(serde_json::to_string(&token_response).unwrap()),
            ),
        );

        (server, move || {
            single_state_nonce_request_db(server_url.clone())
        })
    }

    #[ge_context::test(oidc_db = "oidc_short_duration")]
    async fn oidc_login_no_refresh(app_ctx: PostgresContext<NoTls>) {
        let auth_code_response = AuthCodeResponse {
            session_state: String::new(),
            code: String::new(),
            state: SINGLE_STATE.to_string(),
        };

        let request = app_ctx
            .oidc_manager()
            .get_client_with_redirect_uri(DUMMY_REDIRECT_URI.to_string())
            .await
            .unwrap()
            .generate_request()
            .await;

        assert!(request.is_ok());

        let res = oidc_login_test_helper(Method::POST, app_ctx.clone(), auth_code_response).await;

        assert_eq!(res.status(), 200);

        let original_session: UserSession = test::read_body_json(res).await;

        //Access token duration oidc_login_refresh is 1 sec, i.e., session times out after 1 sec.
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        let req = actix_web::test::TestRequest::get()
            .uri("/session")
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(original_session.id.to_string()),
            ));
        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 401);
    }

    #[ge_context::test]
    async fn it_gets_quota(app_ctx: PostgresContext<NoTls>) {
        let user = UserRegistration {
            email: "foo@example.com".to_string(),
            password: "secret123".to_string(),
            real_name: "Foo Bar".to_string(),
        };

        app_ctx.register_user(user).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@example.com".to_string(),
            password: "secret123".to_string(),
        };

        let session = app_ctx.login(credentials).await.unwrap();

        let admin_session = admin_login(&app_ctx).await;
        let admin_db = app_ctx.session_context(admin_session.clone()).db();

        admin_db
            .increment_quota_used(&session.user.id, 111)
            .await
            .unwrap();

        // current user quota
        let req = test::TestRequest::get()
            .uri("/quota")
            .append_header((header::AUTHORIZATION, format!("Bearer {}", session.id)));
        let res = send_test_request(req, app_ctx.clone()).await;
        let quota: Quota = test::read_body_json(res).await;
        assert_eq!(quota.used, 111);

        // specific user quota (self)
        let req = test::TestRequest::get()
            .uri(&format!("/quotas/{}", session.user.id))
            .append_header((header::AUTHORIZATION, format!("Bearer {}", session.id)));
        let res = send_test_request(req, app_ctx.clone()).await;
        let quota: Quota = test::read_body_json(res).await;
        assert_eq!(quota.used, 111);

        // specific user quota (other)
        let req = test::TestRequest::get()
            .uri(&format!("/quotas/{}", uuid::Uuid::new_v4()))
            .append_header((header::AUTHORIZATION, format!("Bearer {}", session.id)));
        let res = send_test_request(req, app_ctx.clone()).await;
        assert_eq!(res.status(), 401);

        // specific user quota as admin
        let req = test::TestRequest::get()
            .uri(&format!("/quotas/{}", session.user.id))
            .append_header((
                header::AUTHORIZATION,
                format!("Bearer {}", admin_session.id()),
            ));
        let res = send_test_request(req, app_ctx).await;
        let quota: Quota = test::read_body_json(res).await;
        assert_eq!(quota.used, 111);
    }

    #[ge_context::test]
    async fn it_updates_quota(app_ctx: PostgresContext<NoTls>) {
        let user = UserRegistration {
            email: "foo@example.com".to_string(),
            password: "secret123".to_string(),
            real_name: "Foo Bar".to_string(),
        };

        let user_id = app_ctx.register_user(user).await.unwrap();

        let admin_session = admin_login(&app_ctx).await;

        let req = test::TestRequest::get()
            .uri(&format!("/quotas/{user_id}"))
            .append_header((
                header::AUTHORIZATION,
                format!("Bearer {}", admin_session.id),
            ));
        let res = send_test_request(req, app_ctx.clone()).await;
        let quota: Quota = test::read_body_json(res).await;
        assert_eq!(
            quota.available,
            crate::config::get_config_element::<crate::config::Quota>()
                .unwrap()
                .initial_credits
        );

        let update = UpdateQuota { available: 123 };

        let req = test::TestRequest::post()
            .uri(&format!("/quotas/{user_id}"))
            .append_header((
                header::AUTHORIZATION,
                format!("Bearer {}", admin_session.id),
            ))
            .set_json(update);
        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let req = test::TestRequest::get()
            .uri(&format!("/quotas/{user_id}"))
            .append_header((
                header::AUTHORIZATION,
                format!("Bearer {}", admin_session.id),
            ));
        let res = send_test_request(req, app_ctx.clone()).await;
        let quota: Quota = test::read_body_json(res).await;
        assert_eq!(quota.available, 123);
    }

    #[ge_context::test]
    async fn it_checks_quota_before_querying(app_ctx: PostgresContext<NoTls>) {
        let admin_db = app_ctx.session_context(UserSession::admin_session()).db();

        let session = app_ctx.create_anonymous_session().await.unwrap();

        let (_, id) = register_ndvi_workflow_helper(&app_ctx).await;

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::white()).try_into().unwrap(),
                (255.0, RgbaColor::black()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::white(),
            RgbaColor::black(),
        )
        .unwrap();

        let raster_colorizer = RasterColorizer::SingleBand {
            band: 0,
            band_colorizer: colorizer.into(),
        };

        let params = &[
            ("request", "GetMap"),
            ("service", "WMS"),
            ("version", "1.3.0"),
            ("layers", &id.to_string()),
            (
                "bbox",
                "1.95556640625,0.90087890625,1.9775390625,0.9228515625",
            ),
            ("width", "256"),
            ("height", "256"),
            ("crs", "EPSG:4326"),
            (
                "styles",
                &format!(
                    "custom:{}",
                    serde_json::to_string(&raster_colorizer).unwrap()
                ),
            ),
            ("format", "image/png"),
            ("time", "2014-04-01T12:00:00.0Z"),
            ("exceptions", "JSON"),
        ];

        admin_db
            .update_quota_available_by_user(&session.user.id, 0)
            .await
            .unwrap();

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/wms/{}?{}",
                id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id.to_string())));
        let res = send_test_request(req, app_ctx.clone()).await;

        ErrorResponse::assert(
            res,
            200,
            "CreatingProcessorFailed",
            "CreatingProcessorFailed: QuotaExhausted",
        )
        .await;

        admin_db
            .update_quota_available_by_user(&session.user.id, 9999)
            .await
            .unwrap();

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/wms/{}?{}",
                id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);
        assert_eq!(
            res.headers().get(&CONTENT_TYPE),
            Some(&header::HeaderValue::from_static("image/png"))
        );
    }

    #[ge_context::test]
    async fn it_adds_and_removes_role(app_ctx: PostgresContext<NoTls>) {
        let admin_session = admin_login(&app_ctx).await;

        let add = AddRole {
            name: "foo".to_string(),
        };

        let req = actix_web::test::TestRequest::put()
            .uri("/roles")
            .set_json(add)
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session.id.to_string()),
            ));
        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);
        let role_id: IdResponse<RoleId> = test::read_body_json(res).await;

        let req = actix_web::test::TestRequest::delete()
            .uri(&format!("/roles/{}", role_id.id))
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session.id.to_string()),
            ));
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);
    }

    #[ge_context::test]
    async fn it_assigns_and_revokes_role(app_ctx: PostgresContext<NoTls>) {
        let admin_session = admin_login(&app_ctx).await;

        let user_id = app_ctx
            .register_user(UserRegistration {
                email: "foo@example.com".to_string(),
                password: "secret123".to_string(),
                real_name: "Foo Bar".to_string(),
            })
            .await
            .unwrap();

        let add = AddRole {
            name: "foo".to_string(),
        };

        let req = actix_web::test::TestRequest::put()
            .uri("/roles")
            .set_json(add)
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session.id.to_string()),
            ));
        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);
        let role_id: IdResponse<RoleId> = test::read_body_json(res).await;

        // assign role
        let req = actix_web::test::TestRequest::post()
            .uri(&format!("/users/{}/roles/{}", user_id, role_id.id))
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session.id.to_string()),
            ));
        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        // revoke role
        let req = actix_web::test::TestRequest::delete()
            .uri(&format!("/users/{}/roles/{}", user_id, role_id.id))
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session.id.to_string()),
            ));
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);
    }

    #[ge_context::test]
    async fn it_gets_role_descriptions(app_ctx: PostgresContext<NoTls>) {
        let admin_session = admin_login(&app_ctx).await;
        let admin_db = app_ctx.session_context(admin_session.clone()).db();
        let role_id = admin_db.add_role("foo").await.unwrap();

        let user_id = app_ctx
            .register_user(UserRegistration {
                email: "foo@example.com".to_string(),
                password: "secret123".to_string(),
                real_name: "Foo Bar".to_string(),
            })
            .await
            .unwrap();
        admin_db.assign_role(&role_id, &user_id).await.unwrap();

        let expected_user_role_description = RoleDescription {
            role: Role {
                id: RoleId::from(user_id),
                name: "foo@example.com".to_string(),
            },
            individual: true,
        };
        let expected_registered_role_description = RoleDescription {
            role: Role {
                id: Role::registered_user_role_id(),
                name: "user".to_string(),
            },
            individual: false,
        };
        let expected_foo_role_description = RoleDescription {
            role: Role {
                id: role_id,
                name: "foo".to_string(),
            },
            individual: false,
        };

        let user_session = app_ctx
            .login(UserCredentials {
                email: "foo@example.com".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

        let req = actix_web::test::TestRequest::get()
            .uri("/user/roles/descriptions")
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(user_session.id.to_string()),
            ));
        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);
        let role_descriptions: Vec<RoleDescription> = test::read_body_json(res).await;
        assert_eq!(
            vec![
                expected_foo_role_description,
                expected_user_role_description,
                expected_registered_role_description,
            ],
            role_descriptions
        );
    }

    #[ge_context::test]
    async fn it_logs_quota(app_ctx: PostgresContext<NoTls>) {
        let user = UserRegistration {
            email: "foo@example.com".to_string(),
            password: "secret123".to_string(),
            real_name: "Foo Bar".to_string(),
        };

        app_ctx.register_user(user).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@example.com".to_string(),
            password: "secret123".to_string(),
        };

        let session = app_ctx.login(credentials).await.unwrap();

        let admin_session = admin_login(&app_ctx).await;
        let admin_db = app_ctx.session_context(admin_session.clone()).db();

        let workflow_id = Uuid::new_v4();

        admin_db
            .log_quota_used(vec![ComputationUnit {
                user: session.user.id.0,
                workflow: workflow_id,
                computation: Uuid::new_v4(),
                operator_name: "Foo",
                operator_path: WorkflowOperatorPath::initialize_root(),
                data: None,
            }])
            .await
            .unwrap();

        let req = test::TestRequest::get()
            .uri("/quota/computations?offset=0&limit=10")
            .append_header((header::AUTHORIZATION, format!("Bearer {}", session.id)));
        let res = send_test_request(req, app_ctx.clone()).await;

        let quota: Vec<ComputationQuota> = test::read_body_json(res).await;
        assert_eq!(quota.len(), 1);

        let req = test::TestRequest::get()
            .uri(&format!("/quota/computations/{}", quota[0].computation_id))
            .append_header((header::AUTHORIZATION, format!("Bearer {}", session.id)));
        let res = send_test_request(req, app_ctx.clone()).await;

        // let json: serde_json::Value = test::read_body_json(res).await;
        // dbg!(json);

        let computation: Vec<OperatorQuota> = test::read_body_json(res).await;

        assert_eq!(
            computation,
            vec![OperatorQuota {
                operator_name: "Foo".to_string(),
                operator_path: "[]".to_string(),
                count: 1
            }]
        );
    }

    #[ge_context::test]
    async fn it_logs_data_usage(app_ctx: PostgresContext<NoTls>) {
        let user = UserRegistration {
            email: "foo@example.com".to_string(),
            password: "secret123".to_string(),
            real_name: "Foo Bar".to_string(),
        };

        app_ctx.register_user(user).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@example.com".to_string(),
            password: "secret123".to_string(),
        };

        let session = app_ctx.login(credentials).await.unwrap();

        let admin_session = admin_login(&app_ctx).await;
        let admin_db = app_ctx.session_context(admin_session.clone()).db();

        let workflow_id = Uuid::new_v4();
        let computation_id = Uuid::new_v4();
        let computation_id2 = Uuid::new_v4();

        admin_db
            .log_quota_used(vec![ComputationUnit {
                user: session.user.id.0,
                workflow: workflow_id,
                computation: computation_id,
                operator_name: "GdalSource",
                operator_path: WorkflowOperatorPath::initialize_root(),
                data: Some("foo".to_string()),
            }])
            .await
            .unwrap();

        admin_db
            .log_quota_used(vec![ComputationUnit {
                user: session.user.id.0,
                workflow: workflow_id,
                computation: computation_id,
                operator_name: "GdalSource",
                operator_path: WorkflowOperatorPath::initialize_root(),
                data: Some("foo".to_string()),
            }])
            .await
            .unwrap();

        admin_db
            .log_quota_used(vec![ComputationUnit {
                user: session.user.id.0,
                workflow: workflow_id,
                computation: computation_id,
                operator_name: "OgrSource",
                operator_path: WorkflowOperatorPath::initialize_root(),
                data: Some("bar".to_string()),
            }])
            .await
            .unwrap();

        admin_db
            .log_quota_used(vec![ComputationUnit {
                user: session.user.id.0,
                workflow: workflow_id,
                computation: computation_id2,
                operator_name: "GdalSource",
                operator_path: WorkflowOperatorPath::initialize_root(),
                data: Some("foo".to_string()),
            }])
            .await
            .unwrap();

        let req = test::TestRequest::get()
            .uri("/quota/dataUsage?offset=0&limit=10")
            .append_header((
                header::AUTHORIZATION,
                format!("Bearer {}", admin_session.id),
            ));
        let res = send_test_request(req, app_ctx.clone()).await;

        let usage: Vec<DataUsage> = test::read_body_json(res).await;

        assert_eq!(usage.len(), 3);
        assert_eq!(usage[0].data, "foo");
        assert_eq!(usage[0].count, 1);
        assert_eq!(usage[1].data, "bar");
        assert_eq!(usage[1].count, 1);
        assert_eq!(usage[2].data, "foo");
        assert_eq!(usage[2].count, 2);

        let req = test::TestRequest::get()
            .uri("/quota/dataUsage/summary?granularity=years&offset=0&limit=10")
            .append_header((
                header::AUTHORIZATION,
                format!("Bearer {}", admin_session.id),
            ));
        let res = send_test_request(req, app_ctx.clone()).await;

        let usage: Vec<DataUsageSummary> = test::read_body_json(res).await;

        assert_eq!(usage.len(), 2);
        assert_eq!(usage[0].data, "bar");
        assert_eq!(usage[0].count, 1);
        assert_eq!(usage[1].data, "foo");
        assert_eq!(usage[1].count, 3);
    }
}
