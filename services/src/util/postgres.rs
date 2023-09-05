use bb8_postgres::{bb8::PooledConnection, PostgresConnectionManager};
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
            .dbname(&self.database);
        config
    }

    pub fn ogr_pg_config(&self) -> String {
        format!(
            "PG:host={} port={} dbname={} user={} password={}",
            self.host, self.port, self.database, self.user, self.password
        )
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
            .unwrap();
        let result: T = conn.query_one(&stmt, &[&value]).await.unwrap().get(0);

        assert_eq!(value, result);
    }
}
