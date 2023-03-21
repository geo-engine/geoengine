use crate::contexts::{ApplicationContext, MockableSession, Session, SessionId};
use crate::error;
use crate::handlers::get_token;
use crate::pro::contexts::{PostgresContext, ProInMemoryContext};
use crate::pro::permissions::{Role, RoleId};
use crate::pro::users::UserId;
use crate::projects::{ProjectId, STRectangle};
use crate::util::Identifier;
use actix_http::Payload;
use actix_web::{web, FromRequest, HttpRequest};
use bb8_postgres::tokio_postgres::NoTls;
use futures::future::err;
use futures_util::future::LocalBoxFuture;
use futures_util::FutureExt;
use geoengine_datatypes::primitives::DateTime;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UserInfo {
    pub id: UserId,
    pub email: Option<String>,
    pub real_name: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UserSession {
    pub id: SessionId,
    pub user: UserInfo,
    pub created: DateTime,
    pub valid_until: DateTime,
    pub project: Option<ProjectId>,
    pub view: Option<STRectangle>,
    pub roles: Vec<RoleId>, // a user has a default role (= its user id) and other additonal roles
}

impl UserSession {
    pub fn admin_session() -> UserSession {
        let role = Role::admin_role_id();
        let user_id = UserId(role.0);
        Self {
            id: SessionId::new(),
            user: UserInfo {
                id: user_id,
                email: None,
                real_name: None,
            },
            created: DateTime::now(),
            valid_until: DateTime::now(),
            project: None,
            view: None,
            roles: vec![role],
        }
    }

    pub fn is_admin(&self) -> bool {
        self.roles.contains(&Role::admin_role_id())
    }
}

impl MockableSession for UserSession {
    fn mock() -> Self {
        let user_id = UserId::new();
        Self {
            id: SessionId::new(),
            user: UserInfo {
                id: user_id,
                email: None,
                real_name: None,
            },
            created: DateTime::now(),
            valid_until: DateTime::now(),
            project: None,
            view: None,
            roles: vec![user_id.into(), Role::registered_user_role_id()],
        }
    }
}

impl Session for UserSession {
    fn id(&self) -> SessionId {
        self.id
    }

    fn created(&self) -> &DateTime {
        &self.created
    }

    fn valid_until(&self) -> &DateTime {
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

#[cfg(test)]
mod tests {
    use super::*;

    use geoengine_datatypes::{primitives::TimeInstance, spatial_reference::SpatialReference};
    use std::str::FromStr;

    #[test]
    fn test_session_serialization() {
        let session = UserSession {
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
            user: UserInfo {
                id: UserId::from_str("9273bb02-95a6-49fe-b1c6-a32ff171d4a3").unwrap(),
                email: Some("foo@example.com".to_string()),
                real_name: Some("Max Muster".to_string()),
            },
            created: DateTime::from_str("2020-01-01T00:00:00Z").unwrap(),
            valid_until: DateTime::from_str("2021-01-01T00:00:00Z").unwrap(),
            roles: vec![RoleId::from_str("da3825dd-6240-460d-a324-02bd06704aaa").unwrap()],
        };

        assert_eq!(
            serde_json::to_value(&session).unwrap(),
            serde_json::json!({
                "id": "d1322969-5ada-4a2c-bacf-a3045383ba41",
                "user": {
                    "id": "9273bb02-95a6-49fe-b1c6-a32ff171d4a3",
                    "email": "foo@example.com",
                    "realName": "Max Muster"
                },
                "created": "2020-01-01T00:00:00.000Z",
                "validUntil": "2021-01-01T00:00:00.000Z",
                "project": "c26e05b2-6709-4d96-ad00-9361ee68a25c",
                "view": {
                    "spatialReference": "EPSG:4326",
                    "boundingBox": {
                        "lowerLeftCoordinate": {
                            "x": 0.0,
                            "y": 1.0
                        },
                        "upperRightCoordinate": {
                            "x": 2.0,
                            "y": 3.0
                        }
                    },
                    "timeInterval": {
                        "start": 1_577_836_800_000_i64,
                        "end": 1_609_459_200_000_i64
                    }
                },
                "roles": ["da3825dd-6240-460d-a324-02bd06704aaa"]
            })
        );
    }
}
