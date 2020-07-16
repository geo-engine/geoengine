use pwhash::bcrypt;
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::error;
use crate::error::{Error, Result};
use crate::util::identifiers::Identifier;
use crate::util::user_input::UserInput;

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
            error::RegistrationFailed {
                reason: "Invalid e-mail address"
            }
        );

        ensure!(
            self.password.len() >= 8,
            error::RegistrationFailed {
                reason: "Password must have at least 8 characters"
            }
        );

        ensure!(
            !self.real_name.is_empty(),
            error::RegistrationFailed {
                reason: "Real name must not be empty"
            }
        );

        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct UserCredentials {
    pub email: String,
    pub password: String,
}

identifier!(UserId);

#[derive(Clone)]
pub struct User {
    pub id: UserId,
    pub email: String,
    pub password_hash: String,
    pub real_name: String,
    pub active: bool,
}

impl From<UserRegistration> for User {
    fn from(user_registration: UserRegistration) -> Self {
        Self {
            id: UserId::new(),
            email: user_registration.email,
            password_hash: bcrypt::hash(&user_registration.password).unwrap(),
            real_name: user_registration.real_name,
            active: true,
        }
    }
}
