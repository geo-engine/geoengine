use snafu::Snafu;

/// High-level error, reported to the user.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum ApplicationContextError {
    CreateContext { source: InternalError },
    SessionById { source: InternalError },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum SimpleApplicationContextError {
    DefaultSession { source: InternalError },
    UpdateDefaultSessionProject { source: InternalError },
    UpdateDefaultSessionView { source: InternalError },
    DefaultSessionContext { source: InternalError },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum SessionContextError {
    Volumes,
}

/// Low-level internal error, not (directly) reported to the user.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum InternalError {
    CouldNotGetPostgresConfig,
    CouldNotGetSchemaStatus {
        source: tokio_postgres::Error,
    },
    CouldNotGetClearDatabaseOnStartConfig {
        source: tokio_postgres::Error,
    },
    #[snafu(display(
        "Database cannot be cleared on startup because it was initially started without that setting."
    ))]
    ClearDatabaseOnStartupNotAllowed,
    CouldNotRecreateDatabaseSchema {
        source: tokio_postgres::Error,
    },
    CouldNotInitializeSchema {
        source: tokio_postgres::Error,
    },
    CouldNotCreateDefaultSession {
        source: tokio_postgres::Error,
    },
    CouldNotLoadDefaultSession {
        source: tokio_postgres::Error,
    },
    InvalidSession,
    Postgres {
        source: tokio_postgres::Error,
    },
    Bb8 {
        source: bb8_postgres::bb8::RunError<tokio_postgres::Error>,
    },
    // TODO: add source error from user db once it is refactored
    CouldNotGetSessionById,
}
