use crate::error;
use crate::error::Result;
use actix_http::Payload;
use actix_web::{FromRequest, HttpRequest};
use futures::future::{err, ok, Ready};
use serde::de;

pub trait UserInput: Clone {
    /// Validates user input and returns itself
    ///
    /// # Errors
    ///
    /// Fails if the user input is invalid
    ///
    fn validate(&self) -> Result<()>;

    /// Validates user input and returns itself
    ///
    /// # Errors
    ///
    /// Fails if the user input is invalid
    ///
    fn validated(self) -> Result<Validated<Self>>
    where
        Self: Sized,
    {
        self.validate().map(|_| Validated { user_input: self })
    }
}

#[derive(Debug, Clone)]
pub struct Validated<T: UserInput + Clone> {
    pub user_input: T,
}

pub struct QueryEx<T>(pub T);

impl<T> QueryEx<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> FromRequest for QueryEx<T>
where
    T: de::DeserializeOwned,
{
    type Config = ();
    type Error = error::Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        let query_string = req.query_string().replace("REQUEST", "request");
        serde_urlencoded::from_str::<T>(&query_string).map_or_else(
            move |e| {
                log::debug!(
                    "Failed during Query extractor deserialization. \
                     Request path: {:?}",
                    req.path()
                );

                err(error::Error::UnableToParseQueryString { source: e })
            },
            |val| ok(QueryEx(val)),
        )
    }
}
