use crate::source::gdal_worker_process::GdalProcessPoolError;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum MdGdalSourceError {
    #[snafu(display(
        "MD data only supports requesting either the first band (z=time) or a band index (z=band): {message}"
    ))]
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
}
