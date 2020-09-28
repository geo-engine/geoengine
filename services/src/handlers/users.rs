use crate::error::Result;
use crate::handlers::{authenticate, Context};
use crate::users::user::{UserCredentials, UserRegistration};
use crate::users::userdb::UserDB;
use crate::util::user_input::UserInput;
use warp::reply::Reply;
use warp::Filter;

pub fn register_user_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("user" / "register"))
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
    let id = ctx.user_db().write().await.register(user)?;
    Ok(warp::reply::json(&id))
}

pub fn login_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("user" / "login"))
        .and(warp::body::json())
        .and(warp::any().map(move || ctx.clone()))
        .and_then(login)
}

// TODO: move into handler once async closures are available?
async fn login<C: Context>(
    user: UserCredentials,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    match ctx.user_db().write().await.login(user) {
        Ok(id) => Ok(warp::reply::json(&id).into_response()),
        Err(_) => Ok(warp::http::StatusCode::UNAUTHORIZED.into_response()),
    }
}

pub fn logout_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("user" / "logout"))
        .and(authenticate(ctx))
        .and_then(logout)
}

// TODO: move into handler once async closures are available?
async fn logout<C: Context>(ctx: C) -> Result<impl warp::Reply, warp::Rejection> {
    match ctx.user_db().write().await.logout(ctx.session()?.token) {
        Ok(_) => Ok(warp::reply().into_response()),
        Err(_) => Ok(warp::http::StatusCode::UNAUTHORIZED.into_response()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::{handle_rejection, DefaultContext};
    use crate::users::session::Session;
    use crate::users::user::UserId;
    use crate::users::userdb::UserDB;
    use crate::util::user_input::Validated;

    #[tokio::test]
    async fn register() {
        let ctx = DefaultContext::default();

        let user = UserRegistration {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
            real_name: " Foo Bar".to_string(),
        };

        // register user
        let res = warp::test::request()
            .method("POST")
            .path("/user/register")
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
        let ctx = DefaultContext::default();

        let user = UserRegistration {
            email: "notanemail".to_string(),
            password: "secret123".to_string(),
            real_name: " Foo Bar".to_string(),
        };

        // register user
        let res = warp::test::request()
            .method("POST")
            .path("/user/register")
            .header("Content-Length", "0")
            .json(&user)
            .reply(&register_user_handler(ctx).recover(handle_rejection))
            .await;

        assert_eq!(res.status(), 400);
    }

    #[tokio::test]
    async fn login() {
        let ctx = DefaultContext::default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            },
        };

        ctx.user_db().write().await.register(user).unwrap();

        let credentials = UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        };

        let res = warp::test::request()
            .method("POST")
            .path("/user/login")
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
        let ctx = DefaultContext::default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            },
        };

        ctx.user_db().write().await.register(user).unwrap();

        let credentials = UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "wrong".to_string(),
        };

        let res = warp::test::request()
            .method("POST")
            .path("/user/login")
            .header("Content-Length", "0")
            .json(&credentials)
            .reply(&login_handler(ctx))
            .await;

        assert_eq!(res.status(), 401);
    }

    #[tokio::test]
    async fn logout() {
        let ctx = DefaultContext::default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            },
        };

        ctx.user_db().write().await.register(user).unwrap();

        let credentials = UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        };

        let session = ctx.user_db().write().await.login(credentials).unwrap();

        let res = warp::test::request()
            .method("POST")
            .path("/user/logout")
            .header("Authorization", session.token.to_string())
            .reply(&logout_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);
        assert_eq!(res.body(), "");
    }

    #[tokio::test]
    async fn logout_missing_header() {
        let ctx = DefaultContext::default();

        let res = warp::test::request()
            .method("POST")
            .path("/user/logout")
            .reply(&logout_handler(ctx))
            .await;

        assert_eq!(res.status(), 400);
        assert_eq!(res.body(), "Missing request header \"authorization\"");
    }

    #[tokio::test]
    async fn logout_wrong_token() {
        let ctx = DefaultContext::default();

        let res = warp::test::request()
            .method("POST")
            .path("/user/logout")
            .header("Authorization", "7e855f3c-b0cd-46d1-b5b3-19e6e3f9ea5")
            .reply(&logout_handler(ctx))
            .await;

        assert_eq!(res.status(), 404); // TODO: 401?
        assert_eq!(res.body(), "");
    }

    #[tokio::test]
    async fn logout_invalid_token() {
        let ctx = DefaultContext::default();

        let res = warp::test::request()
            .method("POST")
            .path("/user/logout")
            .header("Authorization", "no uuid")
            .reply(&logout_handler(ctx))
            .await;

        assert_eq!(res.status(), 404); // TODO: 400?
        assert_eq!(res.body(), "");
    }
}
