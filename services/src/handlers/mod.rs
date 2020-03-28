use crate::error::Error;
use warp::{Rejection, Reply};

pub mod users;
pub mod workflows;

/// A handler for custom rejections
///
/// # Errors
///
/// Fails if the rejection is not custom
///
pub async fn handle_rejection(error: Rejection) -> Result<impl Reply, Rejection> {
    if let Some(err) = error.find::<Error>() {
        let json = warp::reply::json(&err.to_string());
        Ok(warp::reply::with_status(
            json,
            warp::http::StatusCode::BAD_REQUEST,
        ))
    } else {
        Err(warp::reject())
    }
}
