use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Socket,
};

use self::{database_migration::Migration, migration_0000_initial::Migration0000Initial};

mod database_migration;
mod migration_0000_initial;

pub use database_migration::{migrate_database, MigrationResult};

/// All migrations that are available. The migrations are applied in the order they are defined here, starting from the current version of the database.
///
/// NEW MIGRATIONS HAVE TO BE REGISTERED HERE!
///
fn migrations<Tls>() -> Vec<Box<dyn Migration<Tls>>>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    vec![Box::new(Migration0000Initial)]
}
