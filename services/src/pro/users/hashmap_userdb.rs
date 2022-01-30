use std::collections::HashMap;

use async_trait::async_trait;
use geoengine_datatypes::primitives::TimeInstance;
use pwhash::bcrypt;
use snafu::ensure;

use crate::contexts::SessionId;
use crate::error::{self, Result};
use crate::pro::datasets::Role;
use crate::pro::users::{
    User, UserCredentials, UserDb, UserId, UserInfo, UserRegistration, UserSession,
};
use crate::projects::{ProjectId, STRectangle};
use crate::util::user_input::Validated;
use geoengine_datatypes::util::Identifier;

#[derive(Default)]
pub struct HashMapUserDb {
    users: HashMap<String, User>,
    sessions: HashMap<SessionId, UserSession>,
}

#[async_trait]
impl UserDb for HashMapUserDb {
    /// Register a user
    async fn register(&mut self, user_registration: Validated<UserRegistration>) -> Result<UserId> {
        let user_registration = user_registration.user_input;
        ensure!(
            !self.users.contains_key(&user_registration.email),
            error::Duplicate {
                reason: "E-mail already exists"
            }
        );

        let user = User::from(user_registration.clone());
        let id = user.id;
        self.users.insert(user_registration.email, user);
        Ok(id)
    }

    async fn anonymous(&mut self) -> Result<UserSession> {
        let id = UserId::new();
        let user = User {
            id,
            email: id.to_string(),
            password_hash: "".to_string(),
            real_name: "".to_string(),
            active: true,
        };

        self.users.insert(id.to_string(), user);

        let session = UserSession {
            id: SessionId::new(),
            user: UserInfo {
                id,
                email: None,
                real_name: None,
            },
            created: TimeInstance::now(),
            valid_until: TimeInstance::now() + 60 * 60 * 1000, // + 60 minutes
            project: None,
            view: None,
            roles: vec![id.into(), Role::anonymous_role_id()],
        };

        self.sessions.insert(session.id, session.clone());
        Ok(session)
    }

    /// Log user in
    async fn login(&mut self, user_credentials: UserCredentials) -> Result<UserSession> {
        match self.users.get(&user_credentials.email) {
            Some(user) if bcrypt::verify(user_credentials.password, &user.password_hash) => {
                let session = UserSession {
                    id: SessionId::new(),
                    user: UserInfo {
                        id: user.id,
                        email: Some(user.email.clone()),
                        real_name: Some(user.real_name.clone()),
                    },
                    created: TimeInstance::now(),
                    // TODO: make session length configurable
                    valid_until: TimeInstance::now() + 60 * 60 * 1000, // + 60 minutes
                    project: None,
                    view: None,
                    roles: vec![user.id.into(), Role::user_role_id()],
                };

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

    async fn session(&self, session: SessionId) -> Result<UserSession> {
        match self.sessions.get(&session) {
            Some(session) => Ok(session.clone()),
            None => Err(error::Error::InvalidSession),
        }
    }

    async fn set_session_project(
        &mut self,
        session: &UserSession,
        project: ProjectId,
    ) -> Result<()> {
        // TODO: check project exists
        match self.sessions.get_mut(&session.id) {
            Some(session) => {
                session.project = Some(project);
                Ok(())
            }
            None => Err(error::Error::InvalidSession),
        }
    }

    async fn set_session_view(&mut self, session: &UserSession, view: STRectangle) -> Result<()> {
        match self.sessions.get_mut(&session.id) {
            Some(session) => {
                session.view = Some(view);
                Ok(())
            }
            None => Err(error::Error::InvalidSession),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::user_input::UserInput;

    #[tokio::test]
    async fn register() {
        let mut user_db = HashMapUserDb::default();

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
        let mut user_db = HashMapUserDb::default();

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
        let mut user_db = HashMapUserDb::default();

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
        let mut user_db = HashMapUserDb::default();

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
