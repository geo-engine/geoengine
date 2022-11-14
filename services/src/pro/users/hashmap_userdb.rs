use std::collections::HashMap;

use async_trait::async_trait;
use geoengine_datatypes::primitives::{DateTime, Duration};
use openidconnect::SubjectIdentifier;
use pwhash::bcrypt;
use snafu::ensure;

use crate::contexts::{Db, SessionId};
use crate::error::{self, Result};
use crate::pro::datasets::Role;
use crate::pro::users::oidc::{ExternalUser, ExternalUserClaims};
use crate::pro::users::{
    User, UserCredentials, UserDb, UserId, UserInfo, UserRegistration, UserSession,
};
use crate::projects::{ProjectId, STRectangle};
use crate::util::user_input::Validated;
use geoengine_datatypes::util::Identifier;

#[derive(Default)]
pub struct HashMapUserDb {
    users: Db<HashMap<String, User>>,
    external_users: Db<HashMap<SubjectIdentifier, ExternalUser>>, //TODO: Key only works if a single identity provider is used
    sessions: Db<HashMap<SessionId, UserSession>>,
    quota_used: Db<HashMap<UserId, u64>>,
}

#[async_trait]
impl UserDb for HashMapUserDb {
    /// Register a user
    async fn register(&self, user_registration: Validated<UserRegistration>) -> Result<UserId> {
        let user_registration = user_registration.user_input;
        let mut users = self.users.write().await;
        ensure!(
            !users.contains_key(&user_registration.email),
            error::Duplicate {
                reason: "E-mail already exists"
            }
        );

        let user = User::from(user_registration.clone());
        let id = user.id;
        users.insert(user_registration.email, user);
        Ok(id)
    }

    async fn anonymous(&self) -> Result<UserSession> {
        let id = UserId::new();
        let user = User {
            id,
            email: id.to_string(),
            password_hash: String::new(),
            real_name: String::new(),
            active: true,
        };

        self.users.write().await.insert(id.to_string(), user);

        let session = UserSession {
            id: SessionId::new(),
            user: UserInfo {
                id,
                email: None,
                real_name: None,
            },
            created: DateTime::now(),
            valid_until: DateTime::now() + Duration::minutes(60),
            project: None,
            view: None,
            roles: vec![id.into(), Role::anonymous_role_id()],
        };

        self.sessions
            .write()
            .await
            .insert(session.id, session.clone());
        Ok(session)
    }

    /// Log user in
    async fn login(&self, user_credentials: UserCredentials) -> Result<UserSession> {
        match self.users.read().await.get(&user_credentials.email) {
            Some(user) if bcrypt::verify(user_credentials.password, &user.password_hash) => {
                let session = UserSession {
                    id: SessionId::new(),
                    user: UserInfo {
                        id: user.id,
                        email: Some(user.email.clone()),
                        real_name: Some(user.real_name.clone()),
                    },
                    created: DateTime::now(),
                    // TODO: make session length configurable
                    valid_until: DateTime::now() + Duration::minutes(60),
                    project: None,
                    view: None,
                    roles: vec![user.id.into(), Role::user_role_id()],
                };

                self.sessions
                    .write()
                    .await
                    .insert(session.id, session.clone());
                Ok(session)
            }
            _ => Err(error::Error::LoginFailed),
        }
    }

    async fn login_external(
        &self,
        user: ExternalUserClaims,
        duration: Duration,
    ) -> Result<UserSession> {
        let mut db = self.external_users.write().await;

        let external_id = user.external_id.clone();

        let internal_id = match db.get(&external_id) {
            Some(user) => user.id,
            None => {
                let id = UserId::new();
                let result = ExternalUser {
                    id,
                    claims: user.clone(),
                    active: true,
                };
                db.insert(external_id, result);
                id
            }
        };

        let session_created = DateTime::now(); //TODO: Differs from normal login - maybe change duration handling.

        let session = UserSession {
            id: SessionId::new(),
            user: UserInfo {
                id: internal_id,
                email: Some(user.email.clone()),
                real_name: Some(user.real_name.clone()),
            },
            created: session_created,
            valid_until: session_created + duration,
            project: None,
            view: None,
            roles: vec![internal_id.into(), Role::user_role_id()],
        };

        self.sessions
            .write()
            .await
            .insert(session.id, session.clone());
        Ok(session)
    }

    /// Log user out
    async fn logout(&self, session: SessionId) -> Result<()> {
        match self.sessions.write().await.remove(&session) {
            Some(_) => Ok(()),
            None => Err(error::Error::LogoutFailed),
        }
    }

    async fn session(&self, session: SessionId) -> Result<UserSession> {
        match self.sessions.read().await.get(&session) {
            Some(session) => Ok(session.clone()), //TODO: Session validity is not checked.
            None => Err(error::Error::InvalidSession),
        }
    }

    async fn set_session_project(&self, session: &UserSession, project: ProjectId) -> Result<()> {
        // TODO: check project exists
        match self.sessions.write().await.get_mut(&session.id) {
            Some(session) => {
                session.project = Some(project);
                Ok(())
            }
            None => Err(error::Error::InvalidSession),
        }
    }

    async fn set_session_view(&self, session: &UserSession, view: STRectangle) -> Result<()> {
        match self.sessions.write().await.get_mut(&session.id) {
            Some(session) => {
                session.view = Some(view);
                Ok(())
            }
            None => Err(error::Error::InvalidSession),
        }
    }

    async fn increment_quota_used(&self, user: &UserId, quota_used: u64) -> Result<()> {
        *self.quota_used.write().await.entry(*user).or_default() += quota_used;
        Ok(())
    }

    async fn quota_used(&self, session: &UserSession) -> Result<u64> {
        Ok(self
            .quota_used
            .read()
            .await
            .get(&session.user.id)
            .copied()
            .unwrap_or_default())
    }

    async fn quota_used_by_user(&self, user: &UserId) -> Result<u64> {
        Ok(self
            .quota_used
            .read()
            .await
            .get(user)
            .copied()
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::user_input::UserInput;

    #[tokio::test]
    async fn register() {
        let user_db = HashMapUserDb::default();

        let user_registration = UserRegistration {
            email: "foo@example.com".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        }
        .validated()
        .unwrap();

        assert!(user_db.register(user_registration).await.is_ok());
    }

    #[tokio::test]
    async fn login() {
        let user_db = HashMapUserDb::default();

        let user_registration = UserRegistration {
            email: "foo@example.com".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        }
        .validated()
        .unwrap();

        assert!(user_db.register(user_registration).await.is_ok());

        let user_credentials = UserCredentials {
            email: "foo@example.com".into(),
            password: "secret123".into(),
        };

        assert!(user_db.login(user_credentials).await.is_ok());
    }

    #[tokio::test]
    async fn logout() {
        let user_db = HashMapUserDb::default();

        let user_registration = UserRegistration {
            email: "foo@example.com".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        }
        .validated()
        .unwrap();

        assert!(user_db.register(user_registration).await.is_ok());

        let user_credentials = UserCredentials {
            email: "foo@example.com".into(),
            password: "secret123".into(),
        };

        let session = user_db.login(user_credentials).await.unwrap();

        assert!(user_db.logout(session.id).await.is_ok());
    }

    #[tokio::test]
    async fn session() {
        let user_db = HashMapUserDb::default();

        let user_registration = UserRegistration {
            email: "foo@example.com".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        }
        .validated()
        .unwrap();

        assert!(user_db.register(user_registration).await.is_ok());

        let user_credentials = UserCredentials {
            email: "foo@example.com".into(),
            password: "secret123".into(),
        };

        let session = user_db.login(user_credentials).await.unwrap();

        assert!(user_db.session(session.id).await.is_ok());
    }

    #[tokio::test]
    async fn login_external() {
        let db = HashMapUserDb::default();

        let external_user_claims = ExternalUserClaims {
            external_id: SubjectIdentifier::new("Foo bar Id".into()),
            email: "foo@bar.de".into(),
            real_name: "Foo Bar".into(),
        };
        let duration = Duration::minutes(30);
        let login_result = db
            .login_external(external_user_claims.clone(), duration)
            .await;
        assert!(login_result.is_ok());

        let session_1 = login_result.unwrap();
        let previous_user_id = session_1.user.id; //TODO: Not a deterministic test.

        assert!(session_1.user.email.is_some());
        assert_eq!(session_1.user.email.unwrap(), "foo@bar.de");
        assert!(session_1.user.real_name.is_some());
        assert_eq!(session_1.user.real_name.unwrap(), "Foo Bar");

        let expected_duration = session_1.created + duration;
        assert_eq!(session_1.valid_until, expected_duration);

        assert!(db.session(session_1.id).await.is_ok());

        assert!(db.logout(session_1.id).await.is_ok());

        assert!(db.session(session_1.id).await.is_err());

        let duration = Duration::minutes(10);
        let login_result = db
            .login_external(external_user_claims.clone(), duration)
            .await;
        assert!(login_result.is_ok());

        let session_2 = login_result.unwrap();

        assert!(session_2.user.email.is_some()); //TODO: Technically, user details could change for each login. For simplicity, this is not covered yet.
        assert_eq!(session_2.user.email.unwrap(), "foo@bar.de");
        assert!(session_2.user.real_name.is_some());
        assert_eq!(session_2.user.real_name.unwrap(), "Foo Bar");
        assert_eq!(session_2.user.id, previous_user_id);

        let expected_duration = session_2.created + duration;
        assert_eq!(session_2.valid_until, expected_duration);

        assert!(db.session(session_2.id).await.is_ok());

        assert!(db.logout(session_2.id).await.is_ok());

        assert!(db.session(session_2.id).await.is_err());
    }
}
