use gdal::errors::GdalError;
use geoengine_datatypes::{
    dataset::{DataId, DataProviderId},
    error::ErrorSource,
};
use snafu::Snafu;
use std::path::PathBuf;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum NetCdfCf4DProviderError {
    NotYetImplemented,
    DataTypeNotYetImplemented {
        data_type: String,
    },
    MissingTimeCoverageStart {
        source: GdalError,
    },
    MissingTimeCoverageEnd {
        source: GdalError,
    },
    MissingTimeCoverageResolution {
        source: GdalError,
    },
    MissingTitle {
        source: GdalError,
    },
    MissingSummary {
        source: GdalError,
    },
    MissingCrs {
        source: GdalError,
    },
    MissingSubdatasets,
    MissingEntities {
        source: GdalError,
    },
    MissingTimeDimension {
        source: GdalError,
    },
    MissingGroupName,
    MissingFileName,
    NoTitleForGroup {
        metadata_key: String,
    },
    CannotParseNumberOfEntities {
        source: std::num::ParseIntError,
    },
    CannotSplitNumberOfEntities,
    CannotConvertTimeCoverageToInt {
        source: std::num::ParseIntError,
    },
    CannotParseTimeCoverageDate {
        source: Box<dyn ErrorSource>,
    },
    CannotComputeMinMax {
        source: GdalError,
    },
    TimeCoverageYearOverflows {
        year: i32,
    },
    CannotParseTimeCoverageResolution {
        source: chrono::format::ParseError,
    },
    TimeCoverageResolutionMustConsistsOnlyOfIntParts {
        source: std::num::ParseIntError,
    },
    TimeCoverageResolutionPartsMustNotBeEmpty,
    TimeCoverageResolutionMustStartWithP,
    CannotDefineTimeCoverageEnd {
        source: geoengine_datatypes::error::Error,
    },
    GeneratingResultDescriptorFromDataset {
        source: geoengine_operators::error::Error,
    },
    GeneratingParametersFromDataset {
        source: geoengine_operators::error::Error,
    },
    InvalidTimeCoverageInstant {
        source: geoengine_datatypes::error::Error,
    },
    InvalidTimeCoverageInterval {
        source: geoengine_datatypes::error::Error,
    },
    CannotCalculateStepsInTimeCoverageInterval {
        source: geoengine_datatypes::error::Error,
    },
    InvalidExternalDataId {
        data_id: DataId,
    },
    InvalidDataIdLength {
        length: usize,
    },
    InvalidDatasetIdFile {
        source: geoengine_operators::error::Error,
    },
    CannotParseCrs {
        source: geoengine_datatypes::error::Error,
    },
    DatasetIdEntityNotANumber {
        source: std::num::ParseIntError,
    },
    CannotComputeSubdatasetsFromMetadata,
    GdalMd {
        source: GdalError,
    },
    UnknownGdalDatatype {
        type_number: u32,
    },
    MustBe4DDataset {
        number_of_dimensions: usize,
    },
    CannotGetGeoTransform {
        source: GdalError,
    },
    InvalidGeoTransformLength {
        length: usize,
    },
    InvalidGeoTransformNumbers {
        source: std::num::ParseFloatError,
    },
    CannotParseDatasetId {
        source: serde_json::Error,
    },
    InvalidTimeRangeForDataset {
        source: geoengine_datatypes::error::Error,
    },
    PathToDataIsEmpty,
    MissingDataType,
    CannotOpenColorizerFile {
        source: std::io::Error,
    },
    CannotReadColorizerFile {
        source: std::io::Error,
    },
    CannotParseColorizer {
        source: serde_json::Error,
    },
    CannotCreateFallbackColorizer {
        source: geoengine_datatypes::error::Error,
    },
    DatasetIsNotInProviderPath {
        source: Box<dyn ErrorSource>,
    },
    FileIsNotInProviderPath {
        file: String,
    },
    CannotRetrieveUnit {
        source: GdalError,
    },
    CannotReadDimensions {
        source: GdalError,
    },
    InvalidDirectory {
        source: Box<dyn ErrorSource>,
    },
    CannotCreateOverviews {
        source: Box<dyn ErrorSource>,
    },

    #[snafu(display("For a refresh, all overview files for `{dataset}` must be there, but {missing} is missing.", dataset = dataset.display(), missing = missing.display()))]
    OverviewMissingForRefresh {
        dataset: PathBuf,
        missing: PathBuf,
    },

    #[snafu(display("Cannot find overviews for `{dataset}`", dataset = dataset.display()))]
    MissingOverviews {
        dataset: PathBuf,
    },

    #[snafu(display("New metadata for `{dataset}` do not match group and entity structure. Please recreate the overviews.", dataset = dataset.display()))]
    RefreshedMetadataDoNotMatch {
        dataset: PathBuf,
    },

    CannotWriteMetadataFile {
        source: Box<dyn ErrorSource>,
    },
    CannotReadInputFileDir {
        path: PathBuf,
    },
    CannotOpenNetCdfDataset {
        source: Box<dyn ErrorSource>,
    },
    CannotOpenNetCdfSubdataset {
        source: Box<dyn ErrorSource>,
    },

    CannotGenerateLoadingInfo {
        source: Box<dyn ErrorSource>,
    },

    #[snafu(display("Cannot store loading info in database: {source}"))]
    CannotStoreLoadingInfo {
        source: Box<dyn ErrorSource>,
    },

    InvalidCollectionId {
        id: String,
    },

    #[snafu(display("Cannot parse NetCDF file metadata: {source}"))]
    CannotParseNetCdfFile {
        source: Box<dyn ErrorSource>,
    },
    #[snafu(display("Cannot lookup dataset with id {id}"))]
    CannotLookupDataset {
        id: String,
    },
    #[snafu(display("Cannot find NetCdfCf provider with id {id}"))]
    NoNetCdfCfProviderForId {
        id: DataProviderId,
    },
    NoNetCdfCfProviderAvailable,
    #[snafu(display("NetCdfCf provider with id {id} cannot list files"))]
    CdfCfProviderCannotListFiles {
        id: DataProviderId,
    },
    #[snafu(display("Internal server error"))]
    Internal {
        source: Box<dyn ErrorSource>,
    },
    CannotCreateInProgressFlag {
        source: Box<dyn ErrorSource>,
    }, //
    CannotRemoveInProgressFlag {
        source: Box<dyn ErrorSource>,
    },
    NoOverviewsGeneratedForSource {
        path: String,
    },
    CannotRemoveOverviewsWhileCreationIsInProgress,

    #[snafu(display("NetCdfCf provider cannot remove overviews: {source}"))]
    CannotRemoveOverviews {
        source: Box<dyn ErrorSource>,
    },
    #[snafu(display("NetCdfCf provider cannot create overviews for `{dataset}`: {source}", dataset = dataset.display()))]
    CannotCreateOverview {
        dataset: PathBuf,
        source: Box<dyn ErrorSource>,
    },
    UnsupportedMetaDataDefinition,

    #[snafu(display("Cannot serialize layer definition: {source}"))]
    CannotSerializeLayer {
        source: serde_json::Error,
    },

    #[snafu(display("Cannot serialize layer collection definition: {source}"))]
    CannotSerializeLayerCollection {
        source: serde_json::Error,
    },

    #[snafu(display("Cannot get database connection: {source}"))]
    DatabaseConnection {
        source: Box<dyn ErrorSource>,
    },

    #[snafu(display("Cannot start database transaction: {source}"))]
    DatabaseTransaction {
        source: Box<dyn ErrorSource>,
    },

    #[snafu(display("Cannot commit overview to database: {source}"))]
    DatabaseTransactionCommit {
        source: Box<dyn ErrorSource>,
    },

    #[snafu(display("Unexpected execution error: Please contact the system administrator"))]
    UnexpectedExecution {
        source: Box<dyn ErrorSource>,
    },
}

mod send_sync_ensurance {

    use super::*;

    trait SendSyncEnsurance: Send + Sync {}

    impl SendSyncEnsurance for NetCdfCf4DProviderError {}

    impl<'a> SendSyncEnsurance for tokio_postgres::Transaction<'a> {}

    impl<Tls> SendSyncEnsurance
        for bb8_postgres::bb8::PooledConnection<
            'static,
            bb8_postgres::PostgresConnectionManager<Tls>,
        >
    where
        Tls: tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
        <Tls as tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket>>::Stream: Send + Sync,
        <Tls as tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket>>::TlsConnect: Send,
        <<Tls as tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket>>::TlsConnect as tokio_postgres::tls::TlsConnect<tokio_postgres::Socket>>::Future: Send,
    {
    }

    impl<Tls> SendSyncEnsurance
        for ouroboros::macro_help::AliasableBox< bb8_postgres::bb8::PooledConnection<
            'static,
            bb8_postgres::PostgresConnectionManager<Tls>,
        >>
    where
        Tls: tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
        <Tls as tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket>>::Stream: Send + Sync,
        <Tls as tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket>>::TlsConnect: Send,
        <<Tls as tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket>>::TlsConnect as tokio_postgres::tls::TlsConnect<tokio_postgres::Socket>>::Future: Send,
    {
    }
}
