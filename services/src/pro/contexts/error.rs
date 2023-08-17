use snafu::Snafu;

use crate::contexts::ApplicationContextError;

/// High-level error, reported to the user.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum ProApplicationContextError {
    ApplicationContext { source: ApplicationContextError },
    CreateContext { source: InternalError },
    // SessionById { source: InternalError },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum InternalError {
    ApplicationContextInternal {
        source: crate::contexts::error::InternalError,
    },
}
