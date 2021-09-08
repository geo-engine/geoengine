use chrono::{DateTime, Utc, MAX_DATETIME, MIN_DATETIME};
use geoengine_datatypes::identifier;
use geoengine_datatypes::util::Identifier;
use serde::{Deserialize, Serialize};

use crate::contexts::{Context, InMemoryContext};
use crate::error;
use crate::handlers::get_token;
use crate::projects::ProjectId;
use crate::projects::STRectangle;
use actix_http::Payload;
use actix_web::{web, FromRequest, HttpRequest};
use futures::future::{err, LocalBoxFuture};
use futures_util::FutureExt;

identifier!(SessionId);

pub trait Session: Send + Sync + Serialize {
    fn id(&self) -> SessionId;
    fn created(&self) -> &DateTime<Utc>;
    fn valid_until(&self) -> &DateTime<Utc>;
    fn project(&self) -> Option<ProjectId>;
    fn view(&self) -> Option<&STRectangle>;
}

pub trait MockableSession: Session {
    fn mock() -> Self;
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct SimpleSession {
    id: SessionId,
    pub project: Option<ProjectId>,
    pub view: Option<STRectangle>,
}

impl Default for SimpleSession {
    fn default() -> Self {
        Self {
            id: SessionId::new(),
            project: None,
            view: None,
        }
    }
}

impl Session for SimpleSession {
    fn id(&self) -> SessionId {
        self.id
    }

    fn created(&self) -> &DateTime<Utc> {
        &MIN_DATETIME
    }

    fn valid_until(&self) -> &DateTime<Utc> {
        &MAX_DATETIME
    }

    fn project(&self) -> Option<ProjectId> {
        self.project
    }

    fn view(&self) -> Option<&STRectangle> {
        self.view.as_ref()
    }
}

impl MockableSession for SimpleSession {
    fn mock() -> Self {
        Self::default()
    }
}

impl FromRequest for SimpleSession {
    type Config = ();
    type Error = error::Error;
    type Future = LocalBoxFuture<'static, Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        let token = match get_token(req) {
            Ok(token) => token,
            Err(error) => return Box::pin(err(error)),
        };
        let ctx = req.app_data::<web::Data<InMemoryContext>>().expect(
            "InMemoryContext will be registered because SimpleSession is only used in demo mode",
        ).get_ref().clone();
        async move { ctx.session_by_id(token).await.map_err(Into::into) }.boxed_local()
    }
}
