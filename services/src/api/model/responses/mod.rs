pub mod datasets;

use actix_http::StatusCode;
use actix_web::{dev::ServiceResponse, HttpResponse};
use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::{ToResponse, ToSchema};

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
                        .schema(
                            ObjectBuilder::new()
                                .property(
                                    "id",
                                    ObjectBuilder::new()
                                        .schema_type(SchemaType::String)
                                        .format(Some(SchemaFormat::KnownFormat(KnownFormat::Uuid))),
                                )
                                .required("id"),
                        )
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
}

impl<'a, T> From<&'a T> for ErrorResponse
where
    T: snafu::Error,
    &'static str: From<&'a T>,
{
    fn from(value: &'a T) -> Self {
        ErrorResponse {
            error: Into::<&str>::into(value).to_string(),
            message: value.to_string(),
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
pub struct BadRequestQueryResponse(ErrorResponse);

#[derive(ToResponse)]
#[response(description = "ZIP Archive", content_type = "application/zip", example = json!("zip bytes"))]
pub struct ZipResponse(Vec<u8>);

#[derive(ToResponse)]
#[response(description = "PNG Image", content_type = "image/png", example = json!("image bytes"))]
pub struct PngResponse(Vec<u8>);
