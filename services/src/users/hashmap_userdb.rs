use std::collections::HashMap;

use pwhash::bcrypt;
use snafu::ensure;

use crate::error;
use crate::error::Result;
use crate::users::session::{Session, SessionToken};
use crate::users::user::{User, UserCredentials, UserId, UserRegistration};
use crate::users::userdb::UserDB;
use crate::util::user_input::Validated;
use async_trait::async_trait;

#[derive(Default)]
pub struct HashMapUserDB {
    users: HashMap<String, User>,
    sessions: HashMap<SessionToken, Session>,
}

#[async_trait]
impl UserDB for HashMapUserDB {
    /// Register a user
    async fn register(&mut self, user_registration: Validated<UserRegistration>) -> Result<UserId> {
        let user_registration = user_registration.user_input;
        ensure!(
            !self.users.contains_key(&user_registration.email),
            error::RegistrationFailed {
                reason: "E-mail already exists "
            }
        );

        let user = User::from(user_registration.clone());
        let id = user.id;
        self.users.insert(user_registration.email, user);
        Ok(id)
    }

    /// Log user in
    async fn login(&mut self, user_credentials: UserCredentials) -> Result<Session> {
        match self.users.get(&user_credentials.email) {
            Some(user) if bcrypt::verify(user_credentials.password, &user.password_hash) => {
                let session = Session::new(user);
                self.sessions.insert(session.token.clone(), session.clone());
                Ok(session)
            }
            _ => Err(error::Error::LoginFailed),
        }
    }

    /// Log user out
    async fn logout(&mut self, token: SessionToken) -> Result<()> {
        match self.sessions.remove(&token) {
            Some(_) => Ok(()),
            None => Err(error::Error::LogoutFailed),
        }
    }

    /// Get session for token
    async fn session(&self, token: SessionToken) -> Result<Session> {
        match self.sessions.get(&token) {
            Some(session) => Ok(session.clone()),
            None => Err(error::Error::SessionDoesNotExist),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::user_input::UserInput;

    #[tokio::test]
    async fn register() {
        let mut user_db = HashMapUserDB::default();

        let user_registration = UserRegistration {
            email: "foo@bar.de".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        }
        .validated()
        .unwrap();

        assert!(user_db.register(user_registration).await.is_ok());
    }

    #[tokio::test]
    async fn login() {
        let mut user_db = HashMapUserDB::default();

        let user_registration = UserRegistration {
            email: "foo@bar.de".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        }
        .validated()
        .unwrap();

        assert!(user_db.register(user_registration).await.is_ok());

        let user_credentials = UserCredentials {
            email: "foo@bar.de".into(),
            password: "secret123".into(),
        };

        assert!(user_db.login(user_credentials).await.is_ok());
    }

    #[tokio::test]
    async fn logout() {
        let mut user_db = HashMapUserDB::default();

        let user_registration = UserRegistration {
            email: "foo@bar.de".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        }
        .validated()
        .unwrap();

        assert!(user_db.register(user_registration).await.is_ok());

        let user_credentials = UserCredentials {
            email: "foo@bar.de".into(),
            password: "secret123".into(),
        };

        let session = user_db.login(user_credentials).await.unwrap();

        assert!(user_db.logout(session.token).await.is_ok());
    }

    #[tokio::test]
    async fn session() {
        let mut user_db = HashMapUserDB::default();

        let user_registration = UserRegistration {
            email: "foo@bar.de".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        }
        .validated()
        .unwrap();

        assert!(user_db.register(user_registration).await.is_ok());

        let user_credentials = UserCredentials {
            email: "foo@bar.de".into(),
            password: "secret123".into(),
        };

        let session = user_db.login(user_credentials).await.unwrap();

        assert!(user_db.session(session.token).await.is_ok());
    }
}
