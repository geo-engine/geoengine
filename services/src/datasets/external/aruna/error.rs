use aruna_rust_api::api::storage::models::v2::Status;
use snafu::prelude::*;
use tonic::metadata::errors::InvalidMetadataValue;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum ArunaProviderError {
    InvalidAPIToken { source: InvalidMetadataValue },
    InvalidDataId,
    InvalidUri { uri_string: String },
    InvalidMetaObject,
    InvalidObjectStatus { status: Status },
    MissingProject,
    MissingCollection,
    MissingDataset,
    MissingObject,
    MissingDataObject,
    MissingMetaObject,
    MissingArunaMetaData,
    MissingRelation,
    MissingURL,
    Reqwest { source: reqwest::Error },
    UnexpectedObjectHierarchy,
    TonicStatus { source: tonic::Status },
    TonicTransport { source: tonic::transport::Error },
}

impl From<tonic::Status> for ArunaProviderError {
    fn from(source: tonic::Status) -> Self {
        Self::TonicStatus { source }
    }
}

impl From<tonic::transport::Error> for ArunaProviderError {
    fn from(source: tonic::transport::Error) -> Self {
        Self::TonicTransport { source }
    }
}

impl From<reqwest::Error> for ArunaProviderError {
    fn from(source: reqwest::Error) -> Self {
        Self::Reqwest { source }
    }
}
