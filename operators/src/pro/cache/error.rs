use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))]
pub enum CacheError {
    LandingZoneRatioMustBeLargerThanZero,
    LandingZoneRatioMustBeSmallerThanOne,
    NotEnoughSpaceInLandingZone,
    NotEnoughSpaceInCache,
    QueryNotFoundInLandingZone,
    OperatorCacheEntryNotFound,
    InvalidRasterDataTypeForInsertion,
    TileExpiredBeforeInsertion,
    NegativeSizeOfLandingZone,
    NegativeSizeOfCache,
    QueryIdAlreadyInLandingZone,
    CacheEntryIdAlreadyInCache,
}
