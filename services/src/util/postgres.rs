use bb8_postgres::{PostgresConnectionManager, bb8::PooledConnection};
use serde::{Deserialize, Serialize};
use tokio_postgres::Config;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DatabaseConnectionConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub schema: String,
    pub user: String,
    pub password: String,
}

impl DatabaseConnectionConfig {
    pub fn pg_config(&self) -> Config {
        let mut config = Config::new();
        config
            .user(&self.user)
            .password(&self.password)
            .host(&self.host)
            .dbname(&self.database)
            .port(self.port)
            .options(format!("-c search_path={}", self.schema));
        config
    }

    pub fn ogr_pg_config(&self) -> String {
        format!(
            "PG:host={} port={} dbname={} user={} password={}",
            self.host, self.port, self.database, self.user, self.password
        )
    }
}

impl From<crate::config::Postgres> for DatabaseConnectionConfig {
    fn from(config: crate::config::Postgres) -> Self {
        Self {
            host: config.host,
            port: config.port,
            database: config.database,
            schema: config.schema,
            user: config.user,
            password: config.password,
        }
    }
}

/// test `FromSql`/`ToSql` for a type and its postgres type
///
/// # Panics
///
/// Panics if the type cannot be converted to/from postgres
///
pub async fn assert_sql_type<T>(
    conn: &PooledConnection<'_, PostgresConnectionManager<tokio_postgres::NoTls>>,
    sql_type: &str,
    checks: impl IntoIterator<Item = T>,
) where
    T: PartialEq + postgres_types::FromSqlOwned + postgres_types::ToSql + Sync,
{
    const UNQUOTED: [&str; 3] = ["double precision", "int", "point[]"];

    // don't quote built-in types
    let quote = if UNQUOTED.contains(&sql_type) || sql_type.contains('[') {
        ""
    } else {
        "\""
    };

    for value in checks {
        let stmt = conn
            .prepare(&format!("SELECT $1::{quote}{sql_type}{quote}"))
            .await
            .expect("it should panic since this is an assertion");
        let result: T = conn
            .query_one(&stmt, &[&value])
            .await
            .expect("it should panic since this is an assertion")
            .get(0);

        assert_eq!(value, result);
    }
}

/// Checks whether the given error presents a unique constraint violation on the given column
pub fn postgres_error_is_unique_violation(
    err: &tokio_postgres::Error,
    table_name: &str,
    column_name: &str,
) -> bool {
    if err.code() != Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) {
        return false;
    }

    let Some(db_error) = err.as_db_error() else {
        return false;
    };

    let Some(constraint) = db_error.constraint() else {
        return false;
    };

    let expected_constraint_name = format!("{table_name}_{column_name}_key");

    constraint == expected_constraint_name
}

pub trait PostgresErrorExt<T, E> {
    /// Maps a postgres unique violation that matches a specific table and column to a custom error.
    /// Otherwise it returns the original error.
    fn map_unique_violation(
        self,
        table_name: &str,
        column_name: &str,
        map_unique_violation: impl FnOnce() -> E,
    ) -> Result<T, E>;
}

impl<T, E> PostgresErrorExt<T, E> for Result<T, tokio_postgres::Error>
where
    E: From<tokio_postgres::Error>,
{
    fn map_unique_violation(
        self,
        table_name: &str,
        column_name: &str,
        map_unique_violation: impl FnOnce() -> E,
    ) -> Result<T, E> {
        let postgres_error = match self {
            Ok(v) => return Ok(v),
            Err(e) => e,
        };

        if postgres_error_is_unique_violation(&postgres_error, table_name, column_name) {
            Err(map_unique_violation())
        } else {
            Err(postgres_error.into())
        }
    }
}
