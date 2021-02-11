use crate::contexts::Context;
use crate::handlers::ErrorResponse;
use crate::projects::project::{
    CreateProject, Layer, LayerInfo, LayerUpdate, ProjectId, RasterInfo, STRectangle, UpdateProject,
};
use crate::projects::projectdb::ProjectDB;
use crate::users::session::Session;
use crate::users::user::{UserCredentials, UserRegistration};
use crate::users::userdb::UserDB;
use crate::util::user_input::UserInput;
use crate::util::Identifier;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::{Workflow, WorkflowId};
use geoengine_datatypes::operations::image::Colorizer;
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use geoengine_operators::engine::{RasterOperator, TypedOperator};
use geoengine_operators::source::{GdalSource, GdalSourceParameters};
use warp::http::Response;
use warp::hyper::body::Bytes;

pub async fn create_session_helper<C: Context>(ctx: &C) -> Session {
    ctx.user_db()
        .write()
        .await
        .register(
            UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: "Foo Bar".to_string(),
            }
            .validated()
            .unwrap(),
        )
        .await
        .unwrap();

    ctx.user_db()
        .write()
        .await
        .login(UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        })
        .await
        .unwrap()
}

pub async fn create_project_helper<C: Context>(ctx: &C) -> (Session, ProjectId) {
    let session = create_session_helper(ctx).await;

    let project = ctx
        .project_db()
        .write()
        .await
        .create(
            session.user.id,
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

pub fn update_project_helper(project: ProjectId) -> UpdateProject {
    UpdateProject {
        id: project,
        name: Some("TestUpdate".to_string()),
        description: None,
        layers: Some(vec![LayerUpdate::UpdateOrInsert(Layer {
            workflow: WorkflowId::new(),
            name: "L1".to_string(),
            info: LayerInfo::Raster(RasterInfo {
                colorizer: Colorizer::Rgba,
            }),
            visibility: Default::default(),
        })]),
        plots: None,
        bounds: None,
        time_step: None,
    }
}

pub async fn register_workflow_helper<C: Context>(ctx: &C) -> (Workflow, WorkflowId) {
    let workflow = Workflow {
        operator: TypedOperator::Raster(
            GdalSource {
                params: GdalSourceParameters {
                    dataset_id: "modis_ndvi".to_owned(),
                    channel: None,
                },
            }
            .boxed(),
        ),
    };

    let id = ctx
        .workflow_registry()
        .write()
        .await
        .register(workflow.clone())
        .await
        .unwrap();

    (workflow, id)
}

pub async fn check_allowed_http_methods2<'a, T, TRes, P, PParam>(
    test_helper: T,
    allowed_methods: &'a [&str],
    projector: P,
) where
    T: Fn(&'a str) -> TRes,
    TRes: futures::Future<Output = PParam>,
    P: Fn(PParam) -> Response<Bytes>,
{
    const HTTP_METHODS: [&str; 9] = [
        "GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH",
    ];

    for method in &HTTP_METHODS {
        if !allowed_methods.contains(method) {
            let res = test_helper(method).await;
            let res = projector(res);

            ErrorResponse::assert(&res, 405, "MethodNotAllowed", "HTTP method not allowed.");
        }
    }
}

pub fn check_allowed_http_methods<'a, T, TRes>(
    test_helper: T,
    allowed_methods: &'a [&str],
) -> impl futures::Future + 'a
where
    T: Fn(&'a str) -> TRes + 'a,
    TRes: futures::Future<Output = Response<Bytes>> + 'a,
{
    check_allowed_http_methods2(test_helper, allowed_methods, |res| res)
}
