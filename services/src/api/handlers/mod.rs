use crate::contexts::SessionId;
use crate::error::{Error, Result};
use actix_web::http::header;
use actix_web::HttpRequest;
use actix_web_httpauth::headers::authorization::{Bearer, Scheme};
use std::str::FromStr;

pub mod datasets;
pub mod ebv;
pub mod layers;
pub mod machine_learning;
pub mod permissions;
pub mod plots;
pub mod projects;
pub mod spatial_references;
pub mod tasks;
pub mod upload;
pub mod users;
pub mod wcs;
pub mod wfs;
pub mod wms;
pub mod workflows;

pub fn get_token(req: &HttpRequest) -> Result<SessionId> {
    let header = req
        .headers()
        .get(header::AUTHORIZATION)
        .ok_or(Error::Unauthorized {
            source: Box::new(Error::MissingAuthorizationHeader),
        })?;
    let scheme = Bearer::parse(header).map_err(|_| Error::Unauthorized {
        source: Box::new(Error::InvalidAuthorizationScheme),
    })?;
    SessionId::from_str(scheme.token()).map_err(|_err| Error::Unauthorized {
        source: Box::new(Error::InvalidUuid),
    })
}
