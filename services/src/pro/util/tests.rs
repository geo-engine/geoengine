use chrono::{MAX_DATETIME, MIN_DATETIME};
use geoengine_datatypes::{spatial_reference::SpatialReferenceOption, util::Identifier};

use crate::{
    contexts::SessionId,
    pro::{
        contexts::ProContext,
        users::{UserCredentials, UserDb, UserId, UserInfo, UserRegistration, UserSession},
    },
    projects::{CreateProject, ProjectDb, ProjectId, STRectangle},
    util::user_input::UserInput,
};

#[allow(clippy::missing_panics_doc)]
pub async fn create_session_helper<C: ProContext>(ctx: &C) -> UserSession {
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
pub fn create_random_user_session_helper() -> UserSession {
    let user_id = UserId::new();

    UserSession {
        id: SessionId::new(),
        user: UserInfo {
            id: user_id,
            email: Some(user_id.to_string()),
            real_name: Some(user_id.to_string()),
        },
        created: MIN_DATETIME,
        valid_until: MAX_DATETIME,
        project: None,
        view: None,
    }
}

#[allow(clippy::missing_panics_doc)]
pub async fn create_project_helper<C: ProContext>(ctx: &C) -> (UserSession, ProjectId) {
    let session = create_session_helper(ctx).await;

    let project = ctx
        .project_db()
        .write()
        .await
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
