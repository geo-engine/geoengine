use snafu::Snafu;

use crate::error::Error;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))]
pub enum CacheError {
    CacheMustBeLargerOrEqualThanLandingZone,
    NotEnoughSpaceInLandingZone,
    NotEnoughSpaceInCache,
    QueryNotFoundInLandingZone,
    InvalidRasterDataTypeForInsertion,
}

impl From<CacheError> for Error {
    fn from(e: CacheError) -> Self {
        Error::QueryingProcessorFailed {
            source: Box::new(e),
        }
    }
}
