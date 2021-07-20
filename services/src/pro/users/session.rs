use serde::{Deserialize, Serialize};

use crate::contexts::{MockableSession, Session, SessionId};
use crate::error;
use crate::pro::users::UserId;
use crate::projects::{ProjectId, STRectangle};
use crate::util::Identifier;
use actix_http::Payload;
use actix_web::{FromRequest, HttpRequest};
use chrono::{DateTime, Utc};
use futures::future::{err, ok, Ready};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserInfo {
    pub id: UserId,
    pub email: Option<String>,
    pub real_name: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserSession {
    pub id: SessionId,
    pub user: UserInfo,
    pub created: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub project: Option<ProjectId>,
    pub view: Option<STRectangle>,
}

impl MockableSession for UserSession {
    fn mock() -> Self {
        Self {
            id: SessionId::new(),
            user: UserInfo {
                id: UserId::new(),
                email: None,
                real_name: None,
            },
            created: chrono::Utc::now(),
            valid_until: chrono::Utc::now(),
            project: None,
            view: None,
        }
    }
}

impl Session for UserSession {
    fn id(&self) -> SessionId {
        self.id
    }

    fn created(&self) -> &DateTime<Utc> {
        &self.created
    }

    fn valid_until(&self) -> &DateTime<Utc> {
        &self.valid_until
    }

    fn project(&self) -> Option<ProjectId> {
        self.project
    }

    fn view(&self) -> Option<&STRectangle> {
        self.view.as_ref()
    }
}

impl FromRequest for UserSession {
    type Config = ();
    type Error = error::Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        let session = req.extensions_mut().remove::<Self>();

        match session {
            Some(session) => ok(session),
            None => err(error::Error::MissingAuthorizationHeader),
        }
    }
}
