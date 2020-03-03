use pwhash::bcrypt;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::error;

pub trait UserInput {
    fn validate(&self) -> Result<()>;

    fn validated(self) -> Result<Validated<Self>> where Self : Sized {
        self.validate().map(|_| Validated { user_input: self })
    }
}

pub struct Validated<T: UserInput> {
    pub user_input: T
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct UserRegistration {
    pub email: String,
    pub password: String,
    pub real_name: String,
}

impl UserInput for UserRegistration {
    fn validate(&self) -> Result<(), Error> {
        // TODO: more sophisticated input validation
        ensure!(
            self.email.contains('@'),
            error::RegistrationFailed{reason: "Invalid e-mail address"}
        );

        ensure!(
            self.password.len() >= 8,
            error::RegistrationFailed{reason: "Password must have at least 8 characters"}
        );

        ensure!(
            !self.real_name.is_empty(),
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
pub struct User {
    pub id: UserIdentification,
    pub email: String,
    pub password_hash: String,
    pub real_name: String,
    pub active: bool,
}

impl User {
    pub fn from_user_registration(user_registration: &UserRegistration) -> Self {
        Self {
            id: UserIdentification::new(),
            email: user_registration.email.clone(),
            password_hash: bcrypt::hash(&user_registration.password).unwrap(),
            real_name: user_registration.real_name.clone(),
            active: true,
        }
    }
}
