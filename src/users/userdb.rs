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
