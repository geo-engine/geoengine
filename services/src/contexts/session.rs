use crate::contexts::{Context, InMemoryContext};
use crate::error::{self, Error};
use crate::handlers::get_token;
use crate::projects::ProjectId;
use crate::projects::STRectangle;
use crate::util::config;
use actix_http::Payload;
use actix_web::{web, FromRequest, HttpRequest};
use futures::future::{err, LocalBoxFuture};
use futures_util::FutureExt;
use geoengine_datatypes::identifier;
use geoengine_datatypes::primitives::DateTime;
use geoengine_datatypes::util::Identifier;
use serde::{Deserialize, Serialize};

identifier!(SessionId);

pub trait Session: Send + Sync + Serialize {
    fn id(&self) -> SessionId;
    fn created(&self) -> &DateTime;
    fn valid_until(&self) -> &DateTime;
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
        let id = config::get_config_element::<crate::util::config::Session>()
            .ok()
            .and_then(|session| session.fixed_session_token)
            .unwrap_or_else(SessionId::new);

        Self {
            id,
            project: None,
            view: None,
        }
    }
}

impl Session for SimpleSession {
    fn id(&self) -> SessionId {
        self.id
    }

    fn created(&self) -> &DateTime {
        &DateTime::MIN
    }

    fn valid_until(&self) -> &DateTime {
        &DateTime::MAX
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

/// Session for sys admin duties
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AdminSession {
    id: SessionId,
}

impl Default for AdminSession {
    fn default() -> Self {
        // get id from config or use random id
        let id = config::get_config_element::<crate::util::config::Session>()
            .ok()
            .and_then(|session| session.admin_session_token)
            .unwrap_or_else(SessionId::new);

        Self { id }
    }
}

impl Session for AdminSession {
    fn id(&self) -> SessionId {
        self.id
    }

    fn created(&self) -> &DateTime {
        &DateTime::MIN
    }

    fn valid_until(&self) -> &DateTime {
        &DateTime::MAX
    }

    fn project(&self) -> Option<ProjectId> {
        None
    }

    fn view(&self) -> Option<&STRectangle> {
        None
    }
}

impl MockableSession for AdminSession {
    fn mock() -> Self {
        Self::default()
    }
}

impl FromRequest for AdminSession {
    type Error = error::Error;
    type Future = LocalBoxFuture<'static, Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        let request_token = get_token(req);
        async {
            let request_token = request_token?;

            let admin_session = Self::default();

            if request_token != admin_session.id() {
                return Err(Error::Authorization {
                    source: Box::new(Error::InvalidAdminToken),
                });
            }

            Ok(admin_session)
        }
        .boxed()
    }
}

impl From<AdminSession> for SimpleSession {
    fn from(admin_session: AdminSession) -> Self {
        Self {
            id: admin_session.id(),
            project: admin_session.project(),
            view: admin_session.view().cloned(),
        }
    }
}
