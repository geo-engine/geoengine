use std::sync::Arc;
use tokio::sync::RwLock;
use geoengine_services::users::user::{UserRegistration, UserIdentification, UserCredentials, Session, HashMapUserDB};
use geoengine_services::handlers::users::{register_user_handler, login_handler, logout_handler};

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_login_logout() {
        let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));

        let user = UserRegistration {
            email: "foo@bar.de".to_string(),
            password: "secret".to_string(),
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

        // failed login
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
        assert_eq!(res.body(), "");

        // successful login
        let credentials = UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret".to_string(),
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
        let id: Session = serde_json::from_str(&body).unwrap();

        // successful logout
        let res = warp::test::request()
            .method("POST")
            .path("/user/logout")
            .header("Authorization", id.token.to_string())
            .reply(&logout_handler(user_db.clone()))
            .await;

        assert_eq!(res.status(), 200);
        assert_eq!(res.body(), "");

        // failed logout (missing header)
        let res = warp::test::request()
            .method("POST")
            .path("/user/logout")
            .reply(&logout_handler(user_db.clone()))
            .await;

        assert_eq!(res.status(), 400);
        assert_eq!(res.body(), "Missing request header \"authorization\"");

        // failed logout (wrong session token)
        let res = warp::test::request()
            .method("POST")
            .path("/user/logout")
            .header("Authorization", "7e855f3c-b0cd-46d1-b5b3-19e6e3f9ea5")
            .reply(&logout_handler(user_db.clone()))
            .await;

        assert_eq!(res.status(), 404); // TODO: 401?
        assert_eq!(res.body(), "");

        // failed logout (invalid session token)
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
