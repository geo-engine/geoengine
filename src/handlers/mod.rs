use warp::{Rejection, Reply};
use crate::error::Error;

pub mod users;
pub mod workflows;

pub async fn handle_rejection(error: Rejection) -> Result<impl Reply, Rejection> {
    if let Some(err) = error.find::<Error>() {
        let json = warp::reply::json(&err.to_string());
        Ok(warp::reply::with_status(json, warp::http::StatusCode::BAD_REQUEST))
    } else {
        Err(warp::reject())
    }
}
