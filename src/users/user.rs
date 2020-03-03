use std::collections::HashMap;
use snafu::ensure;
use pwhash::bcrypt;
use uuid::Uuid;
use serde::{Serialize, Deserialize};

use crate::error::{Result, Error};
use crate::error;
use core::fmt;
use std::str::FromStr;

pub trait UserInput {
    fn validate(&self) -> Result<()>;

    fn validated(self) -> Result<Validated<Self>> where Self : Sized {
        self.validate().map(|_| Validated { user_input: self })
    }
}

pub struct Validated<T: UserInput> {
    user_input: T
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct UserRegistration {
    pub email: String,
    pub password: String,
    pub real_name: String,
}

impl UserInput for UserRegistration {
    fn validate(&self) -> Result<(), Error> {
        ensure!(
            self.email.contains("@"),
            error::RegistrationFailed{reason: "Invalid e-mail address"}
        );

        ensure!(
            self.password.len() >= 8,
            error::RegistrationFailed{reason: "Password must have at least 8 characters"}
        );

        ensure!(
            self.real_name.len() > 0,
            error::RegistrationFailed{reason: "Real name must not be empty"}
        );

        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct UserCredentials {
    pub email: String,
    pub password: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct UserIdentification {
    id: Uuid,
}

impl UserIdentification {
    fn new() -> Self {
        Self { id: Uuid::new_v4() }
    }
}

#[derive(Clone)]
struct User {
    id: UserIdentification,
    email: String,
    password_hash: String,
    real_name: String,
    active: bool,
}

impl User {
    fn from_user_registration(user_registration: &UserRegistration) -> Self {
        Self {
            id: UserIdentification::new(),
            email: user_registration.email.clone(),
            password_hash: bcrypt::hash(&user_registration.password).unwrap(),
            real_name: user_registration.real_name.clone(),
            active: true,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct SessionToken {
    token: Uuid,
}

impl FromStr for SessionToken {
    type Err = error::Error;

    fn from_str(token: &str) -> Result<Self> {
        Uuid::parse_str(token).map(|id| Self { token: id }).map_err(|_| error::Error::InvalidSessionToken)
    }
}

impl Default for SessionToken {
    fn default() -> Self {
        Self { token: Uuid::new_v4() }
    }
}

impl fmt::Display for SessionToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.token)
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct Session {
    user: UserIdentification,
    pub token: SessionToken,
}

impl Session {
    fn new(user: &User) -> Session {
        Self { user: user.id.clone(), token: SessionToken::default() }
    }
}

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
    fn register(&mut self, user_registration: Validated<UserRegistration>) -> Result<UserIdentification> {
        let user_registration = user_registration.user_input;
        ensure!(
            !self.users.contains_key(&user_registration.email),
            error::RegistrationFailed { reason: "E-mail already exists "}
        );

        let user = User::from_user_registration(&user_registration);
        let id = user.id.clone();
        self.users.insert(user_registration.email, user);
        Ok(id)
    }

    fn login(&mut self, user_credentials: UserCredentials) -> Result<Session> {
        match self.users.get(&user_credentials.email) {
            Some(user) => {
                if bcrypt::verify(user_credentials.password, &user.password_hash) {
                    let session = Session::new(user);
                    self.sessions.insert(session.token.clone(), session.clone());
                    Ok(session)
                } else {
                    Err(error::Error::LoginFailed)
                }
            }
            None => Err(error::Error::LoginFailed)
        }
    }

    fn logout(&mut self, token: SessionToken) -> Result<()> {
        match self.sessions.remove(&token) {
            Some(_) => Ok(()),
            None => Err(error::Error::LogoutFailed)
        }
    }

    fn session(&self, token: SessionToken) -> Result<Session> {
        match self.sessions.get(&token) {
            Some(session) => Ok(session.clone()),
            None => Err(error::Error::SessionDoesNotExist)
        }
    }
}
