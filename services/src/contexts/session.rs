use geoengine_datatypes::identifier;
use geoengine_datatypes::primitives::TimeInstance;
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
    fn created(&self) -> TimeInstance;
    fn valid_until(&self) -> TimeInstance;
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

    fn created(&self) -> TimeInstance {
        TimeInstance::MIN
    }

    fn valid_until(&self) -> TimeInstance {
        TimeInstance::MAX
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
    type Error = error::Error;
    type Future = LocalBoxFuture<'static, Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        let token = match get_token(req) {
            Ok(token) => token,
            Err(error) => return Box::pin(err(error)),
        };
        let ctx = req
            .app_data::<web::Data<InMemoryContext>>()
            .expect("InMemoryContext must be available")
            .get_ref()
            .clone();
        async move { ctx.session_by_id(token).await.map_err(Into::into) }.boxed_local()
    }
}
