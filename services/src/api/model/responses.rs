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

/*("Operation can only be executed by an admin" = (value = json!({
    "error": "OperationRequiresAdminPrivilige",
    "message": "OperationRequiresAdminPrivilige"
})))*/

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
#[response(description = "Bad request", examples(
    ("Body is invalid json" = (value = json!({
        "error": "BodyDeserializeError",
        "message": "expected `,` or `}` at line 13 column 7"
    }))),
    ("Failed to read body" = (value = json!({
        "error": "Payload",
        "message": "Error that occur during reading payload: Can not decode content-encoding."
    }))),
    ("Referenced an unknown upload" = (value = json!({
        "error": "UnknownUploadId",
        "message": "UnknownUploadId"
    }))),
    ("Dataset name is empty" = (value = json!({
        "error": "InvalidDatasetName",
        "message": "InvalidDatasetName"
    }))),
    ("Upload filename is invalid" = (value = json!({
        "error": "InvalidUploadFileName",
        "message": "InvalidUploadFileName"
    }))),
    ("File does not exist" = (value = json!({
        "error": "Operator",
        "message": "Operator: GdalError: GDAL method 'GDALOpenEx' returned a NULL pointer. Error msg: 'upload/0bdd1062-7796-4d44-a655-e548144281a6/asdf: No such file or directory'"
    }))),
    ("Dataset has no auto-importable layer" = (value = json!({
        "error": "DatasetHasNoAutoImportableLayer",
        "message": "DatasetHasNoAutoImportableLayer"
    }))),
))]
pub struct BadRequestAutoCreateDatasetResponse(ErrorResponse);

#[derive(ToResponse)]
#[response(description = "Bad request", examples(
    ("Missing field in query string" = (value = json!({
        "error": "UnableToParseQueryString",
        "message": "Unable to parse query string: missing field `offset`"
    }))),
    ("Number in query string contains letters" = (value = json!({
        "error": "UnableToParseQueryString",
        "message": "Unable to parse query string: invalid digit found in string"
    }))),
    ("Referenced an unknown upload" = (value = json!({
        "error": "UnknownUploadId",
        "message": "UnknownUploadId"
    }))),
    ("No suitable mainfile found" = (value = json!({
        "error": "NoMainFileCandidateFound",
        "message": "NoMainFileCandidateFound"
    }))),
    ("File does not exist" = (value = json!({
        "error": "Operator",
        "message": "Operator: GdalError: GDAL method 'GDALOpenEx' returned a NULL pointer. Error msg: 'upload/0bdd1062-7796-4d44-a655-e548144281a6/asdf: No such file or directory'"
    }))),
    ("Dataset has no auto-importable layer" = (value = json!({
        "error": "DatasetHasNoAutoImportableLayer",
        "message": "DatasetHasNoAutoImportableLayer"
    }))),
))]
pub struct BadRequestSuggestMetadataResponse(ErrorResponse);

#[derive(ToResponse)]
#[response(description = "Bad request", examples(
    ("Referenced an unknown dataset" = (value = json!({
        "error": "UnknownDatasetId",
        "message": "UnknownDatasetId"
    }))),
    ("Given dataset can only be deleted by owner" = (value = json!({
        "error": "OperationRequiresOwnerPermission",
        "message": "OperationRequiresOwnerPermission"
    })))
))]
pub struct BadRequestDeleteDatasetResponse(ErrorResponse);
