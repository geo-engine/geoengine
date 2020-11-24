use serde::{Deserialize, Serialize};

use crate::identifier;
use crate::projects::project::{ProjectId, STRectangle};
use crate::users::user::UserId;
use chrono::{DateTime, Utc};

identifier!(SessionId);

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Session {
    pub id: SessionId,
    pub user: UserId,
    pub created: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub project: Option<ProjectId>,
    pub view: Option<STRectangle>,
}
