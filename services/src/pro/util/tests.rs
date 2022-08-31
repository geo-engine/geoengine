use geoengine_datatypes::{
    primitives::DateTime, spatial_reference::SpatialReferenceOption, util::Identifier,
};

use crate::{
    contexts::SessionId,
    handlers, pro,
    pro::{
        contexts::ProContext,
        datasets::Role,
        projects::ProProjectDb,
        users::{UserCredentials, UserDb, UserId, UserInfo, UserRegistration, UserSession},
    },
    projects::{CreateProject, ProjectDb, ProjectId, STRectangle},
    util::server::{configure_extractors, render_404, render_405},
    util::user_input::UserInput,
};
use actix_web::dev::ServiceResponse;
use actix_web::{http, middleware, test, web, App};

#[allow(clippy::missing_panics_doc)]
pub async fn create_session_helper<C: ProContext>(ctx: &C) -> UserSession {
    ctx.user_db_ref()
        .register(
            UserRegistration {
                email: "foo@example.com".to_string(),
                password: "secret123".to_string(),
                real_name: "Foo Bar".to_string(),
            }
            .validated()
            .unwrap(),
        )
        .await
        .unwrap();

    ctx.user_db_ref()
        .login(UserCredentials {
            email: "foo@example.com".to_string(),
            password: "secret123".to_string(),
        })
        .await
        .unwrap()
}

pub fn create_random_user_session_helper() -> UserSession {
    let user_id = UserId::new();

    UserSession {
        id: SessionId::new(),
        user: UserInfo {
            id: user_id,
            email: Some(user_id.to_string()),
            real_name: Some(user_id.to_string()),
        },
        created: DateTime::MIN,
        valid_until: DateTime::MAX,
        project: None,
        view: None,
        roles: vec![user_id.into(), Role::user_role_id()],
    }
}

#[allow(clippy::missing_panics_doc)]
pub async fn create_project_helper<C: ProContext>(ctx: &C) -> (UserSession, ProjectId) {
    let session = create_session_helper(ctx).await;

    let project = ctx
        .project_db_ref()
        .create(
            &session,
            CreateProject {
                name: "Test".to_string(),
                description: "Foo".to_string(),
                bounds: STRectangle::new(
                    SpatialReferenceOption::Unreferenced,
                    0.,
                    0.,
                    1.,
                    1.,
                    0,
                    1,
                )
                .unwrap(),
                time_step: None,
            }
            .validated()
            .unwrap(),
        )
        .await
        .unwrap();

    (session, project)
}

pub async fn send_pro_test_request<C>(req: test::TestRequest, ctx: C) -> ServiceResponse
where
    C: ProContext,
    C::ProjectDB: ProProjectDb,
{
    #[allow(unused_mut)]
    let mut app = App::new()
        .app_data(web::Data::new(ctx))
        .wrap(
            middleware::ErrorHandlers::default()
                .handler(http::StatusCode::NOT_FOUND, render_404)
                .handler(http::StatusCode::METHOD_NOT_ALLOWED, render_405),
        )
        .wrap(middleware::NormalizePath::trim())
        .configure(configure_extractors)
        .configure(handlers::datasets::init_dataset_routes::<C>)
        .configure(handlers::plots::init_plot_routes::<C>)
        .configure(pro::handlers::projects::init_project_routes::<C>)
        .configure(pro::handlers::users::init_user_routes::<C>)
        .configure(handlers::spatial_references::init_spatial_reference_routes::<C>)
        .configure(handlers::upload::init_upload_routes::<C>)
        .configure(handlers::wcs::init_wcs_routes::<C>)
        .configure(handlers::wfs::init_wfs_routes::<C>)
        .configure(handlers::wms::init_wms_routes::<C>)
        .configure(handlers::workflows::init_workflow_routes::<C>);
    #[cfg(feature = "odm")]
    {
        app = app.configure(pro::handlers::drone_mapping::init_drone_mapping_routes::<C>);
    }
    let app = test::init_service(app).await;
    test::call_service(&app, req.to_request())
        .await
        .map_into_boxed_body()
}
