use crate::source::gdal_worker_process::GdalProcessPoolError;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum MdGdalSourceError {
    #[snafu(display("Unsupported MD data request: {message}"))]
    UnsupportedBandRequest { message: String },

    #[snafu(display("Error in the MD GdalSource reading process: {source}"))]
    #[snafu(context(false))]
    GdalProcessPoolError { source: GdalProcessPoolError },

    #[snafu(display("MD GdalSource probe error: {message}"))]
    ProbeError { message: String },

    #[snafu(display(
        "Stored MD time axis is inconsistent: the time descriptor does not describe the stored time steps"
    ))]
    InconsistentTimeAxis,

    #[snafu(display(
        "Stored MD time axis is not ordered: time steps do not run in non-decreasing start order"
    ))]
    UnorderedTimeAxis,

    #[snafu(display("MD dataset variable name `{name}` is used more than once"))]
    DuplicateVariableName { name: String },

    #[snafu(display(
        "MD file refers to variable index {index}, but the dataset has {count} variables"
    ))]
    UnknownVariable { index: u32, count: usize },

    #[snafu(display(
        "MD dataset mixes variables with and without a time axis; all variables must share one time axis"
    ))]
    MixedZAxisKinds,

    #[snafu(display("MD grid cannot be presented as wrapped: {message}"))]
    UnpresentableGrid { message: String },
}
