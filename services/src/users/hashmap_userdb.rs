use std::collections::HashMap;

use pwhash::bcrypt;
use snafu::ensure;

use crate::error;
use crate::error::Result;
use crate::users::session::{Session, SessionId};
use crate::users::user::{User, UserCredentials, UserId, UserRegistration};
use crate::users::userdb::UserDB;
use crate::util::user_input::Validated;
use async_trait::async_trait;

#[derive(Default)]
pub struct HashMapUserDB {
    users: HashMap<String, User>,
    sessions: HashMap<SessionId, Session>,
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
                self.sessions.insert(session.id, session.clone());
                Ok(session)
            }
            _ => Err(error::Error::LoginFailed),
        }
    }

    /// Log user out
    async fn logout(&mut self, session: SessionId) -> Result<()> {
        match self.sessions.remove(&session) {
            Some(_) => Ok(()),
            None => Err(error::Error::LogoutFailed),
        }
    }

    /// Get session
    async fn session(&self, session: SessionId) -> Result<Session> {
        match self.sessions.get(&session) {
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

        assert!(user_db.logout(session.id).await.is_ok());
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

        assert!(user_db.session(session.id).await.is_ok());
    }
}
