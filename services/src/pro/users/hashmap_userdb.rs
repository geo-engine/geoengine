use std::collections::HashMap;

use async_trait::async_trait;
use geoengine_datatypes::primitives::{DateTime, Duration};
use openidconnect::SubjectIdentifier;
use pwhash::bcrypt;
use snafu::ensure;

use crate::contexts::SessionId;
use crate::error::{self, Result};
use crate::pro::contexts::{ProInMemoryContext, ProInMemoryDb};
use crate::pro::permissions::Role;
use crate::pro::users::oidc::{ExternalUser, ExternalUserClaims};
use crate::pro::users::{
    User, UserCredentials, UserDb, UserId, UserInfo, UserRegistration, UserSession,
};
use crate::projects::{ProjectId, STRectangle};
use crate::util::user_input::Validated;
use geoengine_datatypes::util::Identifier;

use super::userdb::Auth;

pub struct HashMapUserDbBackend {
    users: HashMap<String, User>,
    external_users: HashMap<SubjectIdentifier, ExternalUser>, //TODO: Key only works if a single identity provider is used
    sessions: HashMap<SessionId, UserSession>,
    quota_used: HashMap<UserId, u64>,
    quota_available: HashMap<UserId, i64>,
    // TODO: roles
}

impl Default for HashMapUserDbBackend {
    fn default() -> Self {
        let mut users = HashMap::new();

        let user_config =
            crate::util::config::get_config_element::<crate::pro::util::config::User>()
                .expect("User config should exist because there are defaults in the Settings.toml");

        users.insert(
            user_config.admin_email.clone(),
            User {
                id: UserId(*Role::admin_role_id().uuid()),
                email: user_config.admin_email,
                password_hash: bcrypt::hash(user_config.admin_password)
                    .expect("Admin password hash should be valid"),
                real_name: "Admin".to_string(),
                active: true,
                roles: vec![Role::admin_role_id()],
            },
        );

        Self {
            users,
            external_users: HashMap::new(),
            sessions: HashMap::new(),
            quota_used: HashMap::new(),
            quota_available: HashMap::new(),
        }
    }
}

#[async_trait]
impl Auth for ProInMemoryContext {
    /// Register a user
    async fn register(&self, user_registration: Validated<UserRegistration>) -> Result<UserId> {
        let user_registration = user_registration.user_input;

        let mut backend = self.db.user_db.write().await;

        let users = &mut backend.users;
        ensure!(
            !users.contains_key(&user_registration.email),
            error::Duplicate {
                reason: "E-mail already exists"
            }
        );

        let user = User::from(user_registration.clone());
        let id = user.id;
        users.insert(user_registration.email, user);

        backend.quota_available.insert(
            id,
            crate::util::config::get_config_element::<crate::pro::util::config::User>()?
                .default_available_quota,
        );

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
            roles: vec![Role::anonymous_role_id()],
        };

        let mut backend = self.db.user_db.write().await;

        backend.users.insert(id.to_string(), user);

        backend.quota_available.insert(
            id,
            crate::util::config::get_config_element::<crate::pro::util::config::User>()?
                .default_available_quota,
        );

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

        backend.sessions.insert(session.id, session.clone());
        Ok(session)
    }

    /// Log user in
    async fn login(&self, user_credentials: UserCredentials) -> Result<UserSession> {
        let mut backend = self.db.user_db.write().await;

        match backend.users.get(&user_credentials.email) {
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
                    roles: user.roles.clone(),
                };

                backend.sessions.insert(session.id, session.clone());
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
        let mut backend = self.db.user_db.write().await;

        let db = &mut backend.external_users;

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

                backend.quota_available.insert(
                    id,
                    crate::util::config::get_config_element::<crate::pro::util::config::User>()?
                        .default_available_quota,
                );
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
            roles: vec![internal_id.into(), Role::registered_user_role_id()],
        };

        backend.sessions.insert(session.id, session.clone());
        Ok(session)
    }

    async fn session(&self, session: SessionId) -> Result<UserSession> {
        match self.db.user_db.read().await.sessions.get(&session) {
            Some(session) => Ok(session.clone()), //TODO: Session validity is not checked.
            None => Err(error::Error::InvalidSession),
        }
    }
}

#[async_trait]
impl UserDb for ProInMemoryDb {
    /// Log user out
    async fn logout(&self) -> Result<()> {
        match self
            .backend
            .user_db
            .write()
            .await
            .sessions
            .remove(&self.session.id)
        {
            Some(_) => Ok(()),
            None => Err(error::Error::LogoutFailed),
        }
    }

    async fn set_session_project(&self, project: ProjectId) -> Result<()> {
        // TODO: check project exists
        match self
            .backend
            .user_db
            .write()
            .await
            .sessions
            .get_mut(&self.session.id)
        {
            Some(session) => {
                session.project = Some(project);
                Ok(())
            }
            None => Err(error::Error::InvalidSession),
        }
    }

    async fn set_session_view(&self, view: STRectangle) -> Result<()> {
        match self
            .backend
            .user_db
            .write()
            .await
            .sessions
            .get_mut(&self.session.id)
        {
            Some(session) => {
                session.view = Some(view);
                Ok(())
            }
            None => Err(error::Error::InvalidSession),
        }
    }

    async fn increment_quota_used(&self, user: &UserId, quota_used: u64) -> Result<()> {
        ensure!(self.session.is_admin(), error::PermissionDenied);

        *self
            .backend
            .user_db
            .write()
            .await
            .quota_used
            .entry(*user)
            .or_default() += quota_used;

        *self
            .backend
            .user_db
            .write()
            .await
            .quota_available
            .entry(*user)
            .or_default() -= quota_used as i64;
        Ok(())
    }

    async fn quota_used(&self) -> Result<u64> {
        Ok(self
            .backend
            .user_db
            .read()
            .await
            .quota_used
            .get(&self.session.user.id)
            .copied()
            .unwrap_or_default())
    }

    async fn quota_used_by_user(&self, user: &UserId) -> Result<u64> {
        ensure!(self.session.is_admin(), error::PermissionDenied);

        Ok(self
            .backend
            .user_db
            .read()
            .await
            .quota_used
            .get(user)
            .copied()
            .unwrap_or_default())
    }

    async fn quota_available(&self) -> Result<i64> {
        Ok(self
            .backend
            .user_db
            .read()
            .await
            .quota_available
            .get(&self.session.user.id)
            .copied()
            .unwrap_or_default())
    }

    async fn quota_available_by_user(&self, user: &UserId) -> Result<i64> {
        ensure!(
            self.session.user.id == *user || self.session.is_admin(),
            error::PermissionDenied
        );

        Ok(self
            .backend
            .user_db
            .read()
            .await
            .quota_available
            .get(user)
            .copied()
            .unwrap_or_default())
    }

    async fn update_quota_available_by_user(
        &self,
        user: &UserId,
        new_available_quota: i64,
    ) -> Result<()> {
        ensure!(self.session.is_admin(), error::PermissionDenied);

        *self
            .backend
            .user_db
            .write()
            .await
            .quota_available
            .entry(*user)
            .or_insert(new_available_quota) = new_available_quota;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::util::test::TestDefault;

    use super::*;
    use crate::{contexts::Context, util::user_input::UserInput};

    #[tokio::test]
    async fn register() {
        let ctx = ProInMemoryContext::test_default();

        let user_registration = UserRegistration {
            email: "foo@example.com".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        }
        .validated()
        .unwrap();

        assert!(ctx.register(user_registration).await.is_ok());
    }

    #[tokio::test]
    async fn login() {
        let ctx = ProInMemoryContext::test_default();

        let user_registration = UserRegistration {
            email: "foo@example.com".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        }
        .validated()
        .unwrap();

        assert!(ctx.register(user_registration).await.is_ok());

        let user_credentials = UserCredentials {
            email: "foo@example.com".into(),
            password: "secret123".into(),
        };

        assert!(ctx.login(user_credentials).await.is_ok());
    }

    #[tokio::test]
    async fn logout() {
        let ctx = ProInMemoryContext::test_default();

        let user_registration = UserRegistration {
            email: "foo@example.com".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        }
        .validated()
        .unwrap();

        assert!(ctx.register(user_registration).await.is_ok());

        let user_credentials = UserCredentials {
            email: "foo@example.com".into(),
            password: "secret123".into(),
        };

        let session = ctx.login(user_credentials).await.unwrap();

        let db = ctx.db(session.clone());

        assert!(db.logout().await.is_ok());
    }

    #[tokio::test]
    async fn session() {
        let ctx = ProInMemoryContext::test_default();

        let user_registration = UserRegistration {
            email: "foo@example.com".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        }
        .validated()
        .unwrap();

        assert!(ctx.register(user_registration).await.is_ok());

        let user_credentials = UserCredentials {
            email: "foo@example.com".into(),
            password: "secret123".into(),
        };

        let session = ctx.login(user_credentials).await.unwrap();

        assert!(ctx.session(session.id).await.is_ok());
    }

    #[tokio::test]
    async fn login_external() {
        let ctx = ProInMemoryContext::test_default();

        let external_user_claims = ExternalUserClaims {
            external_id: SubjectIdentifier::new("Foo bar Id".into()),
            email: "foo@bar.de".into(),
            real_name: "Foo Bar".into(),
        };
        let duration = Duration::minutes(30);
        let login_result = ctx
            .login_external(external_user_claims.clone(), duration)
            .await;
        assert!(login_result.is_ok());

        let session_1 = login_result.unwrap();
        let db = ctx.db(session_1.clone());

        let previous_user_id = session_1.user.id; //TODO: Not a deterministic test.

        assert!(session_1.user.email.is_some());
        assert_eq!(session_1.user.email.unwrap(), "foo@bar.de");
        assert!(session_1.user.real_name.is_some());
        assert_eq!(session_1.user.real_name.unwrap(), "Foo Bar");

        let expected_duration = session_1.created + duration;
        assert_eq!(session_1.valid_until, expected_duration);

        assert!(ctx.session(session_1.id).await.is_ok());

        assert!(db.logout().await.is_ok());

        assert!(ctx.session(session_1.id).await.is_err());

        let duration = Duration::minutes(10);
        let login_result = ctx
            .login_external(external_user_claims.clone(), duration)
            .await;
        assert!(login_result.is_ok());

        let session_2 = login_result.unwrap();

        let db2 = ctx.db(session_2.clone());

        assert!(session_2.user.email.is_some()); //TODO: Technically, user details could change for each login. For simplicity, this is not covered yet.
        assert_eq!(session_2.user.email.unwrap(), "foo@bar.de");
        assert!(session_2.user.real_name.is_some());
        assert_eq!(session_2.user.real_name.unwrap(), "Foo Bar");
        assert_eq!(session_2.user.id, previous_user_id);

        let expected_duration = session_2.created + duration;
        assert_eq!(session_2.valid_until, expected_duration);

        assert!(ctx.session(session_2.id).await.is_ok());

        assert!(db2.logout().await.is_ok());

        assert!(ctx.session(session_2.id).await.is_err());
    }
}
