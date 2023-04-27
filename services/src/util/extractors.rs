use std::fmt;

use actix_http::Payload;
use actix_web::{web, FromRequest, HttpRequest};
use futures::future::LocalBoxFuture;
use futures::FutureExt;
use serde::de::DeserializeOwned;
use validator::Validate;

use crate::handlers::ErrorResponse;

/// A Json extractor that validates the content after deserialization
#[derive(Debug)]
pub struct ValidatedJson<T>(pub T);

impl<T> ValidatedJson<T> {
    /// Deconstruct to an inner value
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> AsRef<T> for ValidatedJson<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T> std::ops::Deref for ValidatedJson<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> FromRequest for ValidatedJson<T>
where
    T: DeserializeOwned + Validate + 'static,
{
    type Error = actix_web::Error;
    type Future = LocalBoxFuture<'static, Result<Self, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, payload: &mut Payload) -> Self::Future {
        web::Json::<T>::from_request(req, payload)
            .map(|res: Result<web::Json<T>, _>| match res {
                Ok(data) => data
                    .validate()
                    .map(|_| ValidatedJson(data.into_inner()))
                    .map_err(|validation_errors| {
                        ErrorResponse {
                            error: "ValidationError".to_string(),
                            message: validation_errors_to_string(&validation_errors),
                        }
                        .into()
                    }),
                Err(e) => Err(e),
            })
            .boxed_local()
    }
}

/// A Query extractor that validates the content after deserialization
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct ValidatedQuery<T>(pub T);

impl<T> AsRef<T> for ValidatedQuery<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T> std::ops::Deref for ValidatedQuery<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> std::ops::DerefMut for ValidatedQuery<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T: fmt::Debug> fmt::Debug for ValidatedQuery<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: fmt::Display> fmt::Display for ValidatedQuery<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T> ValidatedQuery<T>
where
    T: Validate,
{
    /// Deconstruct to an inner value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> FromRequest for ValidatedQuery<T>
where
    T: DeserializeOwned + Validate + 'static,
{
    type Error = actix_web::Error;
    type Future = LocalBoxFuture<'static, Result<Self, Self::Error>>;

    #[inline]
    fn from_request(
        req: &actix_web::HttpRequest,
        payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        web::Query::<T>::from_request(req, payload)
            .map(|res: Result<web::Query<T>, _>| match res {
                Ok(data) => data
                    .validate()
                    .map(|_| ValidatedQuery(data.into_inner()))
                    .map_err(|validation_errors| {
                        ErrorResponse {
                            error: "ValidationError".to_string(),
                            message: validation_errors_to_string(&validation_errors),
                        }
                        .into()
                    }),
                Err(e) => Err(e),
            })
            .boxed_local()
    }
}

// TODO: Validated Extractors for Path and Form

fn validation_errors_to_string(validation_errors: &validator::ValidationErrors) -> String {
    // TODO: display struct, and list level errors
    validation_errors
        .field_errors()
        .iter()
        .map(|(field, errors)| {
            let error_string = errors
                .iter()
                .map(|e| e.code.to_string())
                .collect::<Vec<String>>()
                .join(", ");
            format!("{field}: invalid {error_string}\n")
        })
        .collect::<Vec<String>>()
        .join("\n")
}
