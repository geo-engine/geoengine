use super::{
    all_migrations,
    database_migration::{DatabaseVersion, Migration},
};
use crate::{
    error::Result,
    layers::{
        add_from_directory::UNSORTED_COLLECTION_ID, storage::INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
    },
};
use async_trait::async_trait;
use pwhash::bcrypt;
use tokio_postgres::Transaction;
use uuid::Uuid;

const ADMIN_ROLE_ID: Uuid = Uuid::from_u128(0xd532_8854_6190_4af9_ad69_4e74_b096_1ac9);
const REGISTERED_USER_ROLE_ID: Uuid = Uuid::from_u128(0x4e80_81b6_8aa6_4275_af0c_2fa2_da55_7d28);
const ANONYMOUS_USER_ROLE_ID: Uuid = Uuid::from_u128(0xfd8e_87bf_515c_4f36_8da6_1a53_702f_f102);

const ADMIN_QUOTA: i64 = 9_223_372_036_854_775_807; // max postgres `bigint` value

/// Migration to create the current schema instead of starting by version `0000`.
pub struct CurrentSchemaMigration;

impl CurrentSchemaMigration {
    pub async fn create_current_schema(
        &self,
        tx: &Transaction<'_>,
        config: &crate::config::Postgres,
    ) -> Result<()> {
        let schema_name = &config.schema;

        if schema_name != "pg_temp" {
            tx.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema_name};",))
                .await?;
        }

        tx.batch_execute(include_str!("current_schema.sql")).await?;

        Ok(())
    }

    /// Populates the current schema with the initial data.
    ///
    /// # Panics
    ///
    /// Panics if the password hash for the admin user cannot be created.
    ///
    #[allow(clippy::too_many_lines)]
    pub async fn populate_current_schema(
        &self,
        tx: &Transaction<'_>,
        config: &crate::config::Postgres,
        user_config: crate::config::User,
    ) -> Result<()> {
        tx.execute(
            "
            INSERT INTO geoengine (
                clear_database_on_start,
                database_version
            ) VALUES (
                $1,
                $2
            );
            ",
            &[&config.clear_database_on_start, &self.version()],
        )
        .await?;

        tx.execute(
            r#"
            INSERT INTO layer_collections (
                id,
                name,
                description,
                properties
            ) VALUES (
                $1,
                'Layers',
                'All available Geo Engine layers',
                ARRAY[]::"PropertyType"[]
            );
            "#,
            &[&INTERNAL_LAYER_DB_ROOT_COLLECTION_ID],
        )
        .await?;

        tx.execute(
            r#"
            INSERT INTO layer_collections (
                id,
                name,
                description,
                properties
            ) VALUES (
                $1,
                'Unsorted',
                'Unsorted Layers',
                ARRAY[]::"PropertyType"[]
            );
            "#,
            &[&UNSORTED_COLLECTION_ID],
        )
        .await?;

        tx.execute(
            "
            INSERT INTO collection_children (
                parent,
                child
            ) VALUES (
                $1,
                $2
            );
            ",
            &[
                &INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
                &UNSORTED_COLLECTION_ID,
            ],
        )
        .await?;

        tx.execute(
            "
                INSERT INTO roles
                    (id, name)
                VALUES
                    ($1, 'admin'),
                    ($2, 'user'),
                    ($3, 'anonymous')
                ;
                ",
            &[
                &ADMIN_ROLE_ID,
                &REGISTERED_USER_ROLE_ID,
                &ANONYMOUS_USER_ROLE_ID,
            ],
        )
        .await?;

        tx.execute(
            "
                INSERT INTO users (
                    id, 
                    email,
                    password_hash,
                    real_name,
                    quota_available,
                    active
                ) VALUES (
                    $1, 
                    $2,
                    $3,
                    'admin',
                    $4,
                    true
                );
                ",
            &[
                &ADMIN_ROLE_ID,
                &user_config.admin_email,
                &bcrypt::hash(user_config.admin_password)
                    .expect("Admin password hash should be valid"),
                &ADMIN_QUOTA,
            ],
        )
        .await?;

        tx.execute(
            "
                INSERT INTO user_roles (
                    user_id,
                    role_id
                ) VALUES (
                    $1,
                    $1
                );
                ",
            &[&ADMIN_ROLE_ID],
        )
        .await?;

        tx.execute(
            "
                    INSERT INTO permissions
                        (role_id, layer_collection_id, permission)  
                    VALUES 
                        ($1, $4, 'Owner'),
                        ($2, $4, 'Read'),
                        ($3, $4, 'Read'),
                        ($1, $5, 'Owner'),
                        ($2, $5, 'Read'),
                        ($3, $5, 'Read')
                    ;
                    ",
            &[
                &ADMIN_ROLE_ID,
                &REGISTERED_USER_ROLE_ID,
                &ANONYMOUS_USER_ROLE_ID,
                &INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
                &UNSORTED_COLLECTION_ID,
            ],
        )
        .await?;

        Ok(())
    }
}

#[async_trait]
impl Migration for CurrentSchemaMigration {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        None
    }

    fn version(&self) -> DatabaseVersion {
        all_migrations()
            .last()
            .expect("to have at least one migration")
            .version()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        let config = crate::config::get_config_element::<crate::config::Postgres>()?;
        let user_config = crate::config::get_config_element::<crate::config::User>()?;

        self.create_current_schema(tx, &config).await?;

        self.populate_current_schema(tx, &config, user_config).await
    }
}
