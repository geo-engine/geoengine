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
    #[snafu(display("Could not run compression task"))]
    CouldNotRunCompressionTask {
        source: tokio::task::JoinError,
    },
    #[snafu(display("Could not run decompression task"))]
    CouldNotRunDecompressionTask {
        source: tokio::task::JoinError,
    },
    #[snafu(display("Could not convert Arrow element to bytes"))]
    CouldNotWriteElementToBytes {
        source: arrow::error::ArrowError,
    },
    #[snafu(display("Could not convert bytes to Arrow element"))]
    CouldNotReadElementFromBytes {
        source: arrow::error::ArrowError,
    },
    #[snafu(display("The type of the cached element does not match the requested type"))]
    InvalidTypeForRetrieval,
    #[snafu(display("The wrapped operator did not produce a tile for the requested query"))]
    SourceProducedNoTile,
    #[snafu(display("Could not resolve the time query needed to look up the cache: {message}"))]
    TimeQueryFailed {
        message: String,
    },
}
