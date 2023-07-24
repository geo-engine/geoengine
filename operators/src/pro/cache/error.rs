use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))]
pub enum CacheError {
    LandingZoneRatioMustBeLargerThanZero,
    LandingZoneRatioMustBeSmallerThenHalfCacheSize,
    ElementAndQueryDoNotIntersect,
    NotEnoughSpaceInLandingZone,
    NotEnoughSpaceInCache,
    QueryNotFoundInLandingZone,
    OperatorCacheEntryNotFound,
    InvalidTypeForInsertion,
    #[snafu(display("The Element inserted into the cache is already expired"))]
    TileExpiredBeforeInsertion,
    NegativeSizeOfLandingZone,
    NegativeSizeOfCache,
    QueryIdAlreadyInLandingZone,
    CacheEntryIdAlreadyInCache,
    CouldNotFilterResults,
}
