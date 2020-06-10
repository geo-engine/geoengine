use warp::Filter;
use std::sync::Arc;
use warp::reply::Reply;
use crate::error::Result;
use crate::users::userdb::UserDB;
use crate::users::session::Session;
use crate::users::user::{UserRegistration, UserCredentials, UserInput};
use crate::handlers::{DB, authenticate};

pub fn register_user_handler<T: UserDB>(user_db: DB<T>) -> impl Filter<Extract=impl warp::Reply, Error=warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("user" / "register"))
        .and(warp::body::json())
        .and(warp::any().map(move || Arc::clone(&user_db)))
        .and_then(register_user)
}

// TODO: move into handler once async closures are available?
async fn register_user<T: UserDB>(user: UserRegistration, user_db: DB<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let user = user.validated().map_err(warp::reject::custom)?;
    let id = user_db.write().await.register(user).map_err(warp::reject::custom)?;
    Ok(warp::reply::json(&id))
}

pub fn login_handler<T: UserDB>(user_db: DB<T>) -> impl Filter<Extract=impl warp::Reply, Error=warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("user" / "login"))
        .and(warp::body::json())
        .and(warp::any().map(move || Arc::clone(&user_db.clone())))
        .and_then(login)
}

// TODO: move into handler once async closures are available?
async fn login<T: UserDB>(user: UserCredentials, user_db: DB<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let mut db = user_db.write().await;
    match db.login(user) {
        Ok(id) => Ok(warp::reply::json(&id).into_response()),
        Err(_) => Ok(warp::http::StatusCode::UNAUTHORIZED.into_response())
    }
}

pub fn logout_handler<T: UserDB>(user_db: DB<T>) -> impl Filter<Extract=impl warp::Reply, Error=warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("user" / "logout"))
        .and(authenticate(user_db.clone()))
        .and(warp::any().map(move || Arc::clone(&user_db.clone())))
        .and_then(logout)
}

// TODO: move into handler once async closures are available?
async fn logout<T: UserDB>(session: Session, user_db: DB<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let mut db = user_db.write().await;
    match db.logout(session.token) {
        Ok(_) => Ok(warp::reply().into_response()),
        Err(_) => Ok(warp::http::StatusCode::UNAUTHORIZED.into_response())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::users::userdb::UserDB;
    use crate::users::hashmap_userdb::HashMapUserDB;
    use crate::users::user::{UserIdentification, Validated};
    use crate::handlers::handle_rejection;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn register() {
        let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));

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
            .reply(&register_user_handler(user_db.clone()))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        assert!(serde_json::from_str::<UserIdentification>(&body).is_ok());
    }

    #[tokio::test]
    async fn register_fail() {
        let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));

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
            .reply(&register_user_handler(user_db.clone()).
                recover(handle_rejection))
            .await;

        assert_eq!(res.status(), 400);
    }

    #[tokio::test]
    async fn login() {
        let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            }
        };

        user_db.write().await.register(user).unwrap();

        let credentials = UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        };

        let res = warp::test::request()
            .method("POST")
            .path("/user/login")
            .header("Content-Length", "0")
            .json(&credentials)
            .reply(&login_handler(user_db.clone()))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        let _id: Session = serde_json::from_str(&body).unwrap();
    }

    #[tokio::test]
    async fn login_fail() {
        let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            }
        };

        user_db.write().await.register(user).unwrap();

        let credentials = UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "wrong".to_string(),
        };

        let res = warp::test::request()
            .method("POST")
            .path("/user/login")
            .header("Content-Length", "0")
            .json(&credentials)
            .reply(&login_handler(user_db.clone()))
            .await;

        assert_eq!(res.status(), 401);
    }

    #[tokio::test]
    async fn logout() {
        let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            }
        };

        user_db.write().await.register(user).unwrap();

        let credentials = UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        };

        let session = user_db.write().await.login(credentials).unwrap();

        let res = warp::test::request()
            .method("POST")
            .path("/user/logout")
            .header("Authorization", session.token.to_string())
            .reply(&logout_handler(user_db.clone()))
            .await;

        assert_eq!(res.status(), 200);
        assert_eq!(res.body(), "");
    }

    #[tokio::test]
    async fn logout_missing_header() {
        let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));

        let res = warp::test::request()
            .method("POST")
            .path("/user/logout")
            .reply(&logout_handler(user_db.clone()))
            .await;

        assert_eq!(res.status(), 400);
        assert_eq!(res.body(), "Missing request header \"authorization\"");
    }

    #[tokio::test]
    async fn logout_wrong_token() {
        let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));

        let res = warp::test::request()
            .method("POST")
            .path("/user/logout")
            .header("Authorization", "7e855f3c-b0cd-46d1-b5b3-19e6e3f9ea5")
            .reply(&logout_handler(user_db.clone()))
            .await;

        assert_eq!(res.status(), 404); // TODO: 401?
        assert_eq!(res.body(), "");
    }

    #[tokio::test]
    async fn logout_invalid_token() {
        let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));

        let res = warp::test::request()
            .method("POST")
            .path("/user/logout")
            .header("Authorization", "no uuid")
            .reply(&logout_handler(user_db.clone()))
            .await;

        assert_eq!(res.status(), 404); // TODO: 400?
        assert_eq!(res.body(), "");
    }
}
