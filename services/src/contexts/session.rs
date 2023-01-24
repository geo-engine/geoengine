use crate::contexts::{Context, InMemoryContext};
use crate::error::{self, Error};
use crate::handlers::get_token;
use crate::identifier;
use crate::projects::ProjectId;
use crate::projects::STRectangle;
use crate::util::config;
use actix_http::Payload;
use actix_web::{web, FromRequest, HttpRequest};
use futures::future::{err, LocalBoxFuture};
use futures_util::FutureExt;
use geoengine_datatypes::primitives::DateTime;
use geoengine_datatypes::util::Identifier;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, ToSchema)]
pub struct SimpleSession {
    id: SessionId,
    pub project: Option<ProjectId>,
    pub view: Option<STRectangle>,
    #[serde(skip)]
    is_admin: bool, // TODO: remove this flag; we should only distinguish between admin and non-admin in Pro version
}

impl SimpleSession {
    pub fn is_admin(&self) -> bool {
        self.is_admin
    }
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
            is_admin: false,
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
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
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
            is_admin: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use geoengine_datatypes::{primitives::TimeInstance, spatial_reference::SpatialReference};
    use std::str::FromStr;

    #[test]
    fn test_session_serialization() {
        let session = SimpleSession {
            id: SessionId::from_str("d1322969-5ada-4a2c-bacf-a3045383ba41").unwrap(),
            project: Some(ProjectId::from_str("c26e05b2-6709-4d96-ad00-9361ee68a25c").unwrap()),
            view: Some(
                STRectangle::new(
                    SpatialReference::epsg_4326(),
                    0.,
                    1.,
                    2.,
                    3.,
                    TimeInstance::from(DateTime::from_str("2020-01-01T00:00:00Z").unwrap()),
                    TimeInstance::from(DateTime::from_str("2021-01-01T00:00:00Z").unwrap()),
                )
                .unwrap(),
            ),
            is_admin: false,
        };

        assert_eq!(
            serde_json::to_value(&session).unwrap(),
            serde_json::json!({
                "id": "d1322969-5ada-4a2c-bacf-a3045383ba41", // redundant, but id is not stable
                "project": "c26e05b2-6709-4d96-ad00-9361ee68a25c",
                "view": {
                    "spatialReference": "EPSG:4326",
                    "boundingBox": {
                        "lowerLeftCoordinate": {"x": 0.0, "y": 1.0 },
                        "upperRightCoordinate": {"x": 2.0, "y": 3.0}
                    },
                    "timeInterval": {"start": 1_577_836_800_000_i64, "end": 1_609_459_200_000_i64}
                }
            })
        );
    }
}
