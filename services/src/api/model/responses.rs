use crate::handlers::ErrorResponse;
use utoipa::ToResponse;

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
#[response(description = "Id of generated resource", example = json!({
    "id": "36574dc3-560a-4b09-9d22-d5945f2b8093"
}))]
pub struct IdResponse {
    pub id: String,
}

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
