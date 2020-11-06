use crate::error::Result;
use crate::handlers::{authenticate, Context};
use crate::projects::project::{ProjectId, STRectangle};
use crate::users::user::{UserCredentials, UserRegistration};
use crate::users::userdb::UserDB;
use crate::util::identifiers::Identifier;
use crate::util::user_input::UserInput;
use uuid::Uuid;
use warp::reply::Reply;
use warp::Filter;

pub fn register_user_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path("user"))
        .and(warp::body::json())
        .and(warp::any().map(move || ctx.clone()))
        .and_then(register_user)
}

// TODO: move into handler once async closures are available?
async fn register_user<C: Context>(
    user: UserRegistration,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    let user = user.validated()?;
    let id = ctx.user_db_ref_mut().await.register(user).await?;
    Ok(warp::reply::json(&id))
}

pub fn login_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path("login"))
        .and(warp::body::json())
        .and(warp::any().map(move || ctx.clone()))
        .and_then(login)
}

// TODO: move into handler once async closures are available?
async fn login<C: Context>(
    user: UserCredentials,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    match ctx.user_db_ref_mut().await.login(user).await {
        Ok(id) => Ok(warp::reply::json(&id).into_response()),
        Err(_) => Ok(warp::http::StatusCode::UNAUTHORIZED.into_response()),
    }
}

pub fn logout_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path("logout"))
        .and(authenticate(ctx))
        .and_then(logout)
}

// TODO: move into handler once async closures are available?
async fn logout<C: Context>(ctx: C) -> Result<impl warp::Reply, warp::Rejection> {
    match ctx.user_db_ref_mut().await.logout(ctx.session()?.id).await {
        Ok(_) => Ok(warp::reply().into_response()),
        Err(_) => Ok(warp::http::StatusCode::UNAUTHORIZED.into_response()),
    }
}

pub fn session_project_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("session" / "project" / Uuid).map(ProjectId::from_uuid))
        .and(authenticate(ctx))
        .and_then(session_project)
}

// TODO: move into handler once async closures are available?
async fn session_project<C: Context>(
    project: ProjectId,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    ctx.user_db_ref_mut()
        .await
        .set_session_project(ctx.session()?, project)
        .await?;

    Ok(warp::reply())
}

pub fn session_view_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("session" / "view"))
        .and(authenticate(ctx))
        .and(warp::body::json())
        .and_then(session_view)
}

// TODO: move into handler once async closures are available?
async fn session_view<C: Context>(
    ctx: C,
    view: STRectangle,
) -> Result<impl warp::Reply, warp::Rejection> {
    ctx.user_db_ref_mut()
        .await
        .set_session_view(ctx.session()?, view)
        .await?;

    Ok(warp::reply())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projects::project::{CreateProject, STRectangle};
    use crate::projects::projectdb::ProjectDB;
    use crate::users::session::Session;
    use crate::users::user::UserId;
    use crate::users::userdb::UserDB;
    use crate::util::user_input::Validated;
    use crate::{contexts::InMemoryContext, handlers::handle_rejection};
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;

    #[tokio::test]
    async fn register() {
        let ctx = InMemoryContext::default();

        let user = UserRegistration {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
            real_name: " Foo Bar".to_string(),
        };

        // register user
        let res = warp::test::request()
            .method("POST")
            .path("/user")
            .header("Content-Length", "0")
            .json(&user)
            .reply(&register_user_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        assert!(serde_json::from_str::<UserId>(&body).is_ok());
    }

    #[tokio::test]
    async fn register_fail() {
        let ctx = InMemoryContext::default();

        let user = UserRegistration {
            email: "notanemail".to_string(),
            password: "secret123".to_string(),
            real_name: " Foo Bar".to_string(),
        };

        // register user
        let res = warp::test::request()
            .method("POST")
            .path("/user")
            .header("Content-Length", "0")
            .json(&user)
            .reply(&register_user_handler(ctx).recover(handle_rejection))
            .await;

        assert_eq!(res.status(), 400);
    }

    #[tokio::test]
    async fn login() {
        let ctx = InMemoryContext::default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            },
        };

        ctx.user_db().write().await.register(user).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        };

        let res = warp::test::request()
            .method("POST")
            .path("/login")
            .header("Content-Length", "0")
            .json(&credentials)
            .reply(&login_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        let _id: Session = serde_json::from_str(&body).unwrap();
    }

    #[tokio::test]
    async fn login_fail() {
        let ctx = InMemoryContext::default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            },
        };

        ctx.user_db().write().await.register(user).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "wrong".to_string(),
        };

        let res = warp::test::request()
            .method("POST")
            .path("/login")
            .header("Content-Length", "0")
            .json(&credentials)
            .reply(&login_handler(ctx))
            .await;

        assert_eq!(res.status(), 401);
    }

    #[tokio::test]
    async fn logout() {
        let ctx = InMemoryContext::default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            },
        };

        ctx.user_db().write().await.register(user).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        };

        let session = ctx
            .user_db()
            .write()
            .await
            .login(credentials)
            .await
            .unwrap();

        let res = warp::test::request()
            .method("POST")
            .path("/logout")
            .header("Authorization", session.id.to_string())
            .reply(&logout_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);
        assert_eq!(res.body(), "");
    }

    #[tokio::test]
    async fn logout_missing_header() {
        let ctx = InMemoryContext::default();

        let res = warp::test::request()
            .method("POST")
            .path("/logout")
            .reply(&logout_handler(ctx))
            .await;

        assert_eq!(res.status(), 400);
        assert_eq!(res.body(), "Missing request header \"authorization\"");
    }

    #[tokio::test]
    async fn logout_wrong_token() {
        let ctx = InMemoryContext::default();

        let res = warp::test::request()
            .method("POST")
            .path("/logout")
            .header("Authorization", "7e855f3c-b0cd-46d1-b5b3-19e6e3f9ea5")
            .reply(&logout_handler(ctx))
            .await;

        assert_eq!(res.status(), 404); // TODO: 401?
        assert_eq!(res.body(), "");
    }

    #[tokio::test]
    async fn logout_invalid_token() {
        let ctx = InMemoryContext::default();

        let res = warp::test::request()
            .method("POST")
            .path("/logout")
            .header("Authorization", "no uuid")
            .reply(&logout_handler(ctx))
            .await;

        assert_eq!(res.status(), 404); // TODO: 400?
        assert_eq!(res.body(), "");
    }

    #[tokio::test]
    async fn session_view_project() {
        let ctx = InMemoryContext::default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            },
        };

        ctx.user_db().write().await.register(user).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        };

        let session = ctx
            .user_db()
            .write()
            .await
            .login(credentials)
            .await
            .unwrap();

        let project = ctx
            .project_db()
            .write()
            .await
            .create(
                session.user,
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
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let res = warp::test::request()
            .method("POST")
            .path(&format!("/session/project/{}", project.to_string()))
            .header("Authorization", session.id.to_string())
            .reply(&session_project_handler(ctx.clone()))
            .await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            ctx.user_db()
                .read()
                .await
                .session(session.id)
                .await
                .unwrap()
                .project,
            Some(project)
        );

        let rect =
            STRectangle::new_unchecked(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1);
        let res = warp::test::request()
            .method("POST")
            .header("Content-Length", "0")
            .path("/session/view")
            .header("Authorization", session.id.to_string())
            .json(&rect)
            .reply(&session_view_handler(ctx.clone()))
            .await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            ctx.user_db()
                .read()
                .await
                .session(session.id)
                .await
                .unwrap()
                .view,
            Some(rect)
        );
    }
}
