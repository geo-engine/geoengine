use serde::{Deserialize, Serialize};
use tokio_postgres::Config;

#[derive(Clone, Debug, Serialize, Deserialize)]
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
