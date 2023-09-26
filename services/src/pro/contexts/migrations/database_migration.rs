use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::contexts::{DatabaseVersion, Migration};
use crate::error::Result;

/// A pro migration extends a regular migration by first applying the regular migration and then itself.
pub struct ProMigrationImpl<M>
where
    M: Migration,
{
    migration: M,
}

impl<M> From<M> for ProMigrationImpl<M>
where
    M: Migration,
{
    fn from(migration: M) -> Self {
        Self { migration }
    }
}

/// A pro migration only contains the migration itself. The `prev_version` and `version` are taken from the corresponding (free) migration.
#[async_trait]
pub trait ProMigration: Send + Sync {
    async fn pro_migrate(&self, conn: &Transaction<'_>) -> Result<()>;
}

/// A generic implementaion of the `Migration` trait that first applies the regular and then the pro migration.
#[async_trait]
impl<M> Migration for ProMigrationImpl<M>
where
    M: Migration,
    Self: ProMigration,
{
    fn prev_version(&self) -> Option<DatabaseVersion> {
        self.migration.prev_version()
    }

    fn version(&self) -> DatabaseVersion {
        self.migration.version()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        self.migration.migrate(tx).await?;
        self.pro_migrate(tx).await
    }
}
