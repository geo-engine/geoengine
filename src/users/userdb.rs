use std::collections::HashMap;

use pwhash::bcrypt;
use snafu::ensure;

use crate::error;
use crate::error::Result;
use crate::users::session::{Session, SessionToken};
use crate::users::user::{User, UserCredentials, UserIdentification, UserRegistration, Validated};

pub trait UserDB: Send + Sync {
    fn register(&mut self, user: Validated<UserRegistration>) -> Result<UserIdentification>;
    fn login(&mut self, user: UserCredentials) -> Result<Session>;
    fn logout(&mut self, session: SessionToken) -> Result<()>;
    fn session(&self, token: SessionToken) -> Result<Session>;
}

#[derive(Default)]
pub struct HashMapUserDB {
    users: HashMap<String, User>,
    sessions: HashMap<SessionToken, Session>,
}

impl UserDB for HashMapUserDB {
    /// Register a user
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_services::users::user::{UserRegistration, UserInput};
    /// use geoengine_services::users::userdb::{UserDB, HashMapUserDB};
    ///
    /// let mut user_db = HashMapUserDB::default();
    ///
    /// let user_registration = UserRegistration {
    ///     email: "foo@bar.de".into(),
    ///     password: "secret123".into(),
    ///     real_name: "Foo Bar".into()
    /// }.validated().unwrap();
    ///
    /// assert!(user_db.register(user_registration).is_ok());
    /// ```
    fn register(&mut self, user_registration: Validated<UserRegistration>) -> Result<UserIdentification> {
        let user_registration = user_registration.user_input;
        ensure!(
            !self.users.contains_key(&user_registration.email),
            error::RegistrationFailed { reason: "E-mail already exists "}
        );

        let user = User::from(user_registration.clone());
        let id = user.id.clone();
        self.users.insert(user_registration.email, user);
        Ok(id)
    }

    /// Log user in
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_services::users::user::{UserRegistration, UserInput, UserCredentials};
    /// use geoengine_services::users::userdb::{UserDB, HashMapUserDB};
    ///
    /// let mut user_db = HashMapUserDB::default();
    ///
    /// let user_registration = UserRegistration {
    ///     email: "foo@bar.de".into(),
    ///     password: "secret123".into(),
    ///     real_name: "Foo Bar".into()
    /// }.validated().unwrap();
    ///
    /// assert!(user_db.register(user_registration).is_ok());
    ///
    /// let user_credentials = UserCredentials {
    ///     email: "foo@bar.de".into(),
    ///     password: "secret123".into()
    /// };
    ///
    /// assert!(user_db.login(user_credentials).is_ok());
    /// ```
    fn login(&mut self, user_credentials: UserCredentials) -> Result<Session> {
        match self.users.get(&user_credentials.email) {
            Some(user) if bcrypt::verify(user_credentials.password, &user.password_hash) => {
                let session = Session::new(user);
                self.sessions.insert(session.token.clone(), session.clone());
                Ok(session)
            },
            _ => Err(error::Error::LoginFailed)
        }
    }

    /// Log user out
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_services::users::user::{UserRegistration, UserInput, UserCredentials};
    /// use geoengine_services::users::userdb::{UserDB, HashMapUserDB};
    ///
    /// let mut user_db = HashMapUserDB::default();
    ///
    /// let user_registration = UserRegistration {
    ///     email: "foo@bar.de".into(),
    ///     password: "secret123".into(),
    ///     real_name: "Foo Bar".into()
    /// }.validated().unwrap();
    ///
    /// assert!(user_db.register(user_registration).is_ok());
    ///
    /// let user_credentials = UserCredentials {
    ///     email: "foo@bar.de".into(),
    ///     password: "secret123".into()
    /// };
    ///
    /// let session = user_db.login(user_credentials).unwrap();
    ///
    /// assert!(user_db.logout(session.token).is_ok());
    /// ```
    fn logout(&mut self, token: SessionToken) -> Result<()> {
        match self.sessions.remove(&token) {
            Some(_) => Ok(()),
            None => Err(error::Error::LogoutFailed)
        }
    }

    /// Get session for token
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_services::users::user::{UserRegistration, UserInput, UserCredentials};
    /// use geoengine_services::users::userdb::{UserDB, HashMapUserDB};
    ///
    /// let mut user_db = HashMapUserDB::default();
    ///
    /// let user_registration = UserRegistration {
    ///     email: "foo@bar.de".into(),
    ///     password: "secret123".into(),
    ///     real_name: "Foo Bar".into()
    /// }.validated().unwrap();
    ///
    /// assert!(user_db.register(user_registration).is_ok());
    ///
    /// let user_credentials = UserCredentials {
    ///     email: "foo@bar.de".into(),
    ///     password: "secret123".into()
    /// };
    ///
    /// let session = user_db.login(user_credentials).unwrap();
    ///
    /// assert!(user_db.session(session.token).is_ok());
    /// ```
    fn session(&self, token: SessionToken) -> Result<Session> {
        match self.sessions.get(&token) {
            Some(session) => Ok(session.clone()),
            None => Err(error::Error::SessionDoesNotExist)
        }
    }
}
