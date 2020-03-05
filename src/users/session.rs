use core::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error;
use crate::error::Result;
use crate::users::user::{User, UserIdentification};

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
    pub fn new(user: &User) -> Session {
        Self { user: user.id.clone(), token: SessionToken::default() }
    }
}
