use std::path::PathBuf;

use gdal::errors::GdalError;
use snafu::Snafu;

use crate::api::model::datatypes::DataProviderId;
use geoengine_datatypes::error::ErrorSource;

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
        provider: DataProviderId,
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
    CannotRemoveOverviews {
        source: Box<dyn ErrorSource>,
    },
    #[snafu(display("NetCdfCf provider cannot create overviews"))]
    CannotCreateOverview {
        dataset: PathBuf,
        source: Box<dyn ErrorSource>,
    },
    UnsupportedMetaDataDefinition,
}
