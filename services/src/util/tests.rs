use crate::datasets::storage::AddDataset;
use crate::datasets::storage::DatasetStore;
use crate::handlers::ErrorResponse;
use crate::projects::project::{
    CreateProject, Layer, LayerUpdate, ProjectId, RasterSymbology, STRectangle, Symbology,
    UpdateProject,
};
use crate::projects::projectdb::ProjectDb;
use crate::users::session::Session;
use crate::users::user::UserId;
use crate::users::user::{UserCredentials, UserRegistration};
use crate::users::userdb::UserDb;
use crate::util::user_input::UserInput;
use crate::util::Identifier;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::{Workflow, WorkflowId};
use crate::{
    contexts::{Context, InMemoryContext},
    datasets::storage::{DatasetDefinition, MetaDataDefinition},
};
use actix_web::dev::ServiceResponse;
use actix_web::http::Method;
use actix_web::{test, App};
use geoengine_datatypes::dataset::DatasetId;
use geoengine_datatypes::operations::image::Colorizer;
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use geoengine_operators::engine::{RasterOperator, TypedOperator};
use geoengine_operators::source::{GdalSource, GdalSourceParameters};
use geoengine_operators::util::gdal::create_ndvi_meta_data;
use crate::server::init_routes;

#[allow(clippy::missing_panics_doc)]
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

#[allow(clippy::missing_panics_doc)]
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
            visibility: Default::default(),
            symbology: Symbology::Raster(RasterSymbology {
                opacity: 1.0,
                colorizer: Colorizer::Rgba,
            }),
        })]),
        plots: None,
        bounds: None,
        time_step: None,
    }
}

#[allow(clippy::missing_panics_doc)]
pub async fn register_ndvi_workflow_helper(ctx: &InMemoryContext) -> (Workflow, WorkflowId) {
    let dataset = add_ndvi_to_datasets(ctx).await;

    let workflow = Workflow {
        operator: TypedOperator::Raster(
            GdalSource {
                params: GdalSourceParameters { dataset },
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

pub async fn add_ndvi_to_datasets(ctx: &InMemoryContext) -> DatasetId {
    let ndvi = DatasetDefinition {
        properties: AddDataset {
            id: None,
            name: "NDVI".to_string(),
            description: "NDVI data from MODIS".to_string(),
            source_operator: "GdalSource".to_string(),
        },
        meta_data: MetaDataDefinition::GdalMetaDataRegular(create_ndvi_meta_data()),
    };

    ctx.dataset_db_ref_mut()
        .await
        .add_dataset(
            UserId::new(),
            ndvi.properties
                .validated()
                .expect("valid dataset description"),
            Box::new(ndvi.meta_data),
        )
        .await
        .expect("dataset db access")
}

pub async fn check_allowed_http_methods2<T, TRes, P, PParam>(
    test_helper: T,
    allowed_methods: &[Method],
    projector: P,
) where
    T: Fn(Method) -> TRes,
    TRes: futures::Future<Output = PParam>,
    P: Fn(PParam) -> ServiceResponse,
{
    const HTTP_METHODS: [Method; 9] = [
        Method::GET,
        Method::HEAD,
        Method::POST,
        Method::PUT,
        Method::DELETE,
        Method::CONNECT,
        Method::OPTIONS,
        Method::TRACE,
        Method::PATCH,
    ];

    for method in HTTP_METHODS {
        if !allowed_methods.contains(&method) {
            let res = test_helper(method).await;
            let res = projector(res);

            ErrorResponse::assert(res, 405, "MethodNotAllowed", "HTTP method not allowed.");
        }
    }
}

pub fn check_allowed_http_methods<T, TRes>(
    test_helper: T,
    allowed_methods: &[Method],
) -> impl futures::Future + '_
where
    T: Fn(Method) -> TRes,
    TRes: futures::Future<Output = ServiceResponse>,
{
    check_allowed_http_methods2(test_helper, allowed_methods, |res| res)
}
