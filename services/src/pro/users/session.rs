use serde::{Deserialize, Serialize};

use crate::contexts::{Context, MockableSession, Session, SessionId};
use crate::error;
use crate::handlers::get_token;
use crate::pro::contexts::{PostgresContext, ProInMemoryContext};
use crate::pro::users::UserId;
use crate::projects::{ProjectId, STRectangle};
use crate::util::Identifier;
use actix_http::Payload;
use actix_web::{web, FromRequest, HttpRequest};
use bb8_postgres::tokio_postgres::NoTls;
use chrono::{DateTime, Utc};
use futures::future::err;
use futures_util::future::LocalBoxFuture;
use futures_util::FutureExt;

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
    type Error = error::Error;
    type Future = LocalBoxFuture<'static, Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        let token = match get_token(req) {
            Ok(token) => token,
            Err(error) => return Box::pin(err(error)),
        };

        #[cfg(feature = "postgres")]
        {
            if let Some(pg_ctx) = req.app_data::<web::Data<PostgresContext<NoTls>>>() {
                let pg_ctx = pg_ctx.get_ref().clone();
                return async move { pg_ctx.session_by_id(token).await.map_err(Into::into) }
                    .boxed_local();
            }
        }
        let mem_ctx = req
            .app_data::<web::Data<ProInMemoryContext>>()
            .expect("ProInMemoryContext will be registered because Postgres was not activated");
        let mem_ctx = mem_ctx.get_ref().clone();
        async move { mem_ctx.session_by_id(token).await.map_err(Into::into) }.boxed_local()
    }
}
