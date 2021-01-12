use serde::{Deserialize, Serialize};

use crate::identifier;
use crate::projects::project::{ProjectId, STRectangle};
use crate::users::user::UserId;
use chrono::{DateTime, Utc};

identifier!(SessionId);

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct UserInfo {
    pub id: UserId,
    pub email: Option<String>,
    pub real_name: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Session {
    pub id: SessionId,
    pub user: UserInfo,
    pub created: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub project: Option<ProjectId>,
    pub view: Option<STRectangle>,
}
