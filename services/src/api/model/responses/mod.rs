pub mod datasets;
pub mod ml_models;

use actix_http::StatusCode;
use actix_web::{HttpResponse, dev::ServiceResponse};
use convert_case::{Converter, Pattern};
use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::{openapi::schema::SchemaType, ToResponse, ToSchema};

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct IdResponse<T> {
    pub id: T,
}

impl<T> From<T> for IdResponse<T> {
    fn from(id: T) -> Self {
        Self { id }
    }
}

// ToResponse needs to be manually implemented, because
// otherwiese type of T can't be set to string/uuid.
impl<'a, T> ToResponse<'a> for IdResponse<T> {
    fn response() -> (
        &'a str,
        utoipa::openapi::RefOr<utoipa::openapi::response::Response>,
    ) {
        use utoipa::openapi::*;
        (
            "IdResponse",
            ResponseBuilder::new()
                .description("Id of generated resource")
                .content(
                    "application/json",
                    ContentBuilder::new()
                        .schema(Some(
                            ObjectBuilder::new()
                                .title(Some("IdResponse"))
                                .property(
                                    "id",
                                    ObjectBuilder::new()
                                        .schema_type(SchemaType::Type(Type::String))
                                        .format(Some(SchemaFormat::KnownFormat(KnownFormat::Uuid))),
                                )
                                .required("id"),
                        ))
                        .example(Some(serde_json::json!({
                            "id": "36574dc3-560a-4b09-9d22-d5945f2b8093"
                        })))
                        .into(),
                )
                .into(),
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
}

impl ErrorResponse {
    /// Assert that a `Response` has a certain `status` and `error` message.
    ///
    /// # Panics
    /// Panics if `status` or `error` do not match.
    ///
    pub async fn assert(res: ServiceResponse, status: u16, error: &str, message: &str) {
        assert_eq!(res.status(), status);

        let body: Self = actix_web::test::read_body_json(res).await;
        assert_eq!(
            body,
            Self {
                error: error.to_string(),
                message: message.to_string(),
            }
        );
    }

    /// Assert that a `Response` has a certain `status` and `error` message starts with a given prefix.
    ///
    /// # Panics
    /// Panics if `status` or `error` do not match.
    ///
    pub async fn assert_eq_message_starts_with(
        res: ServiceResponse,
        status: u16,
        error: &str,
        message: &str,
    ) {
        assert_eq!(res.status(), status);

        let body: Self = actix_web::test::read_body_json(res).await;
        assert_eq!(&body.error, error);
        assert!(&body.message.starts_with(message));
    }

    /// Extracts the inner variant from a service error and
    /// generates a response based on that.
    /// This leads to more specific error codes for `Operator`
    /// and `DataType` errors.
    pub fn from_service_error(error: &crate::error::Error) -> Self {
        match error {
            crate::error::Error::DataType { source } => Self::from(source),
            crate::error::Error::Operator { source } => match source {
                geoengine_operators::error::Error::DataType { source } => Self::from(source),
                _ => Self::from(source),
            },
            _ => Self::from(error),
        }
    }
}

impl<'a, T> From<&'a T> for ErrorResponse
where
    T: snafu::Error,
    &'a T: Into<&'static str>, // from strum::IntoStaticStr
{
    /// Generates a response based on a general error.
    ///
    /// If you have a [service error](crate::error::Error), please use the more specific
    /// function [`ErrorResponse::from_service_error`] to have better error codes.
    fn from(value: &'a T) -> Self {
        let variant_name: &str = value.into();
        let mut message = value.to_string();

        if message == variant_name {
            // error variant was not tagged with custom display
            // => derive more sensible default than snafu
            let conv = Converter::new()
                .set_pattern(Pattern::Sentence)
                .set_delim(' ');
            message = conv.convert(variant_name);
        }
        ErrorResponse {
            error: variant_name.to_string(),
            message,
        }
    }
}

impl actix_web::ResponseError for ErrorResponse {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(self)
    }

    fn status_code(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }
}

impl fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {}", self.error, self.message)
    }
}

#[derive(ToResponse)]
#[response(description = "Media type of application/json is expected", example = json!({
    "error": "UnsupportedMediaType",
    "message": "Unsupported content type header."
}))]
#[allow(dead_code)]
pub struct UnsupportedMediaTypeForJsonResponse(ErrorResponse);

#[derive(ToResponse)]
#[response(description = "Payload too large", examples(
    ("Unknown payload size" = (value = json!({
        "error": "Overflow",
        "message": "JSON payload has exceeded limit (2097152 bytes)."
    }))),
    ("Known payload size" = (value = json!({
        "error": "Overflow",
        "message": "JSON payload (XXX bytes) is larger than allowed (limit: 2097152 bytes)."
    })))
))]
#[allow(dead_code)]
pub struct PayloadTooLargeResponse(ErrorResponse);

#[derive(ToResponse)]
#[response(description = "Authorization failed", examples(
    ("Authorization Header is missing" = (value = json!({
        "error": "Unauthorized",
        "message": "Authorization error: Header with authorization token not provided."
    }))),
    ("Authorization Scheme other than Bearer is used" = (value = json!({
        "error": "Unauthorized",
        "message": "Authorization error: Authentication scheme must be Bearer."
    }))),
    ("Provided auth token has an invalid format" = (value = json!({
        "error": "Unauthorized",
        "message": "Authorization error: Identifier does not have the right format."
    }))),
    ("Session id is invalid" = (value = json!({
        "error": "Unauthorized",
        "message": "Authorization error: The session id is invalid."
    }))),
    ("Auth token does not correspond to an admin" = (value = json!({
        "error": "Unauthorized",
        "message": "Authorization error: Invalid admin token"
    })))
))]
#[allow(dead_code)]
pub struct UnauthorizedAdminResponse(ErrorResponse);

#[derive(ToResponse)]
#[response(description = "Authorization failed", examples(
    ("Authorization Header is missing" = (value = json!({
        "error": "Unauthorized",
        "message": "Authorization error: Header with authorization token not provided."
    }))),
    ("Authorization Scheme other than Bearer is used" = (value = json!({
        "error": "Unauthorized",
        "message": "Authorization error: Authentication scheme must be Bearer."
    }))),
    ("Provided auth token has an invalid format" = (value = json!({
        "error": "Unauthorized",
        "message": "Authorization error: Identifier does not have the right format."
    }))),
    ("Session id is invalid" = (value = json!({
        "error": "Unauthorized",
        "message": "Authorization error: The session id is invalid."
    })))
))]
#[allow(dead_code)]
pub struct UnauthorizedUserResponse(ErrorResponse);

#[derive(ToResponse)]
#[response(description = "Bad request", examples(
    ("Missing field in query string" = (value = json!({
        "error": "UnableToParseQueryString",
        "message": "Unable to parse query string: missing field `offset`"
    }))),
    ("Number in query string contains letters" = (value = json!({
        "error": "UnableToParseQueryString",
        "message": "Unable to parse query string: invalid digit found in string"
    })))
))]
#[allow(dead_code)]
pub struct BadRequestQueryResponse(ErrorResponse);

// #[derive(ToResponse)]
// #[response(description = "ZIP Archive", content_type = "application/zip", example = json!("zip bytes"))]
pub struct ZipResponse(pub Vec<u8>);

// #[derive(ToResponse)]
// #[response(description = "PNG Image", content_type = "image/png", example = json!("image bytes"))]
pub struct PngResponse(pub Vec<u8>);

// OpenAPI 3.1 allows empty schemas for known content types.
// However, …
//  …utoipa generates an array of i32 which is bad and
//  …the openapi generator generates garbage.
// TODO: change, when utoipa and openapi-generator are fixed.
//       cf. https://github.com/juhaku/utoipa/issues/1146
mod bytes_fix {
    use super::*;
    use std::marker::PhantomData;
    use utoipa::{
        openapi::{ContentBuilder, RefOr, Response, ResponseBuilder},
        PartialSchema,
    };

    #[derive(ToSchema)]
    #[schema(value_type = String, format = Binary)]
    struct BinaryFile(PhantomData<Vec<u8>>);

    impl<'r> ToResponse<'r> for PngResponse {
        fn response() -> (&'r str, RefOr<Response>) {
            let response = ResponseBuilder::new()
                .description("PNG Image")
                .content(
                    "image/png",
                    ContentBuilder::new()
                        .schema(Some(BinaryFile::schema()))
                        .build(),
                )
                .into();

            ("PngResponse", response)
        }
    }

    impl<'r> ToResponse<'r> for ZipResponse {
        fn response() -> (&'r str, RefOr<Response>) {
            let response = ResponseBuilder::new()
                .description("ZIP Archive")
                .content(
                    "application/zip",
                    ContentBuilder::new()
                        .schema(Some(BinaryFile::schema()))
                        .build(),
                )
                .into();

            ("ZipResponse", response)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_extracts_inner_variant_and_derives_message() {
        let inner_error = geoengine_datatypes::error::Error::InvalidUuid;
        assert_eq!(
            inner_error.to_string(),
            "InvalidUuid",
            "Precondition failed. InvalidUuid error should not have display annotation."
        );
        let wrapped_error = crate::error::Error::Operator {
            source: geoengine_operators::error::Error::DataType {
                source: inner_error,
            },
        };

        let json_default = ErrorResponse::from(&wrapped_error);
        assert_eq!(
            json_default,
            ErrorResponse {
                error: "Operator".to_owned(),
                message: "InvalidUuid".to_owned()
            }
        );

        let json_pretty = ErrorResponse::from_service_error(&wrapped_error);
        assert_eq!(
            json_pretty,
            ErrorResponse {
                error: "InvalidUuid".to_owned(),
                message: "Invalid uuid".to_owned()
            }
        );
    }
}
