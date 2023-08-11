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
    #[snafu(display("Compressed element could not be decompressed"))]
    CouldNotDecompressElement {
        source: lz4_flex::block::DecompressError,
    },
    BlockingElementConversion,
    #[snafu(display("Could not run decompression task"))]
    CouldNotRunDecompressionTask {
        source: tokio::task::JoinError,
    },
}
