use serde::{Deserialize, Serialize};

use crate::users::user::{User, UserId};
use crate::util::identifiers::Identifier;

identifier!(SessionId);

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct Session {
    pub id: SessionId,
    pub user: UserId,
    // TODO: session creation time/validity
}

impl Session {
    pub fn new(user: &User) -> Self {
        Self {
            id: SessionId::new(),
            user: user.id,
        }
    }

    pub fn from_user_id(user_id: UserId) -> Self {
        Self {
            id: SessionId::new(),
            user: user_id,
        }
    }

    pub fn from_fields(user: UserId, session: SessionId) -> Self {
        Self { id: session, user }
    }
}
