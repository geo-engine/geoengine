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

/// A pro migration which does nothing except from applying the regular migration.
pub struct NoProMigrationImpl<M>
where
    M: Migration,
{
    migration: M,
}

impl<M> From<M> for NoProMigrationImpl<M>
where
    M: Migration,
{
    fn from(migration: M) -> Self {
        Self { migration }
    }
}

#[async_trait]
impl<M> ProMigration for NoProMigrationImpl<M>
where
    M: Migration,
{
    async fn pro_migrate(&self, _conn: &Transaction<'_>) -> Result<()> {
        // Nothing to do
        Ok(())
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

/// A generic implementaion of the `Migration` trait that only applies the regular migration.
#[async_trait]
impl<M> Migration for NoProMigrationImpl<M>
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
        self.migration.migrate(tx).await
    }
}

#[cfg(test)]
mod tests {
    use crate::contexts::{initialize_database, CurrentSchemaMigration, Migration0000Initial};
    use crate::pro::permissions::RoleId;
    use crate::pro::users::UserDb;
    use crate::projects::{ProjectDb, ProjectListOptions};
    use crate::util::postgres::DatabaseConnectionConfig;
    use crate::workflows::registry::WorkflowRegistry;
    use crate::workflows::workflow::WorkflowId;
    use crate::{
        contexts::{migrate_database, SessionId},
        pro::{
            contexts::{migrations::pro_migrations, ProPostgresDb},
            users::{UserId, UserInfo, UserSession},
        },
        util::config::get_config_element,
    };
    use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
    use geoengine_datatypes::primitives::DateTime;
    use geoengine_datatypes::test_data;
    use std::str::FromStr;
    use tokio_postgres::NoTls;

    use super::*;

    #[tokio::test]
    async fn it_performs_all_pro_migrations() -> Result<()> {
        let postgres_config = get_config_element::<crate::util::config::Postgres>()?;
        let db_config = DatabaseConnectionConfig::from(postgres_config);
        let pg_mgr = PostgresConnectionManager::new(db_config.pg_config(), NoTls);

        let pool = Pool::builder().build(pg_mgr).await?;

        let mut conn = pool.get().await?;

        migrate_database(&mut conn, &pro_migrations()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn it_uses_the_current_schema_if_the_database_is_empty() -> Result<()> {
        let postgres_config = get_config_element::<crate::util::config::Postgres>()?;
        let db_config = DatabaseConnectionConfig::from(postgres_config);
        let pg_mgr = PostgresConnectionManager::new(db_config.pg_config(), NoTls);

        let pool = Pool::builder().build(pg_mgr).await?;

        let mut conn = pool.get().await?;

        initialize_database(
            &mut conn,
            Box::new(ProMigrationImpl::from(CurrentSchemaMigration)),
            &pro_migrations(),
        )
        .await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_migrates_data() -> Result<()> {
        // This test creates the initial schema and fills it with test data.
        // Then, it migrates the database to the newest version.
        // Finally, it tries to load the test data again via the Db implementations.

        let postgres_config = get_config_element::<crate::util::config::Postgres>()?;
        let db_config = DatabaseConnectionConfig::from(postgres_config);
        let pg_mgr = PostgresConnectionManager::new(db_config.pg_config(), NoTls);

        let pool = Pool::builder().max_size(1).build(pg_mgr).await?;

        let mut conn = pool.get().await?;

        // initial schema
        migrate_database(
            &mut conn,
            &[Box::new(ProMigrationImpl::from(Migration0000Initial))],
        )
        .await?;

        // insert test data on initial schema
        let test_data_sql = std::fs::read_to_string(test_data!("migrations/test_data.sql"))?;
        conn.batch_execute(&test_data_sql).await?;

        let test_data_sql = std::fs::read_to_string(test_data!("pro/migrations/test_data.sql"))?;
        conn.batch_execute(&test_data_sql).await?;

        // migrate to latest schema
        migrate_database(&mut conn, &pro_migrations()).await?;

        // drop the connection because the pool is limited to one connection, s.t. we can reuse the temporary schema
        drop(conn);

        // create `ProPostgresDb` on migrated database and test methods
        let db = ProPostgresDb::new(
            pool.clone(),
            UserSession {
                id: SessionId::from_str("e11c7674-7ca5-4e07-840c-260835d3fc8d").unwrap(),
                user: UserInfo {
                    id: UserId::from_str("b589a590-9c0c-4b55-9aa2-d178a5f42a78").unwrap(),
                    email: Some("foobar@example.org".to_string()),
                    real_name: Some("Foo Bar".to_string()),
                },
                created: DateTime::new_utc(2023, 1, 1, 0, 0, 0),
                valid_until: DateTime::new_utc(9999, 1, 1, 0, 0, 0),
                project: None,
                view: None,
                roles: vec![RoleId::from_str("b589a590-9c0c-4b55-9aa2-d178a5f42a78").unwrap()],
            },
        );

        let projects = db
            .list_projects(ProjectListOptions {
                order: crate::projects::OrderBy::NameAsc,
                offset: 0,
                limit: 10,
            })
            .await
            .unwrap();

        assert!(!projects.is_empty());

        db.load_workflow(&WorkflowId::from_str("38ddfc17-016e-4910-8adf-b1af36a8590c").unwrap())
            .await
            .unwrap();

        assert_eq!(db.quota_available().await.unwrap(), 0);

        // TODO: test more methods and more Dbs

        Ok(())
    }
}
