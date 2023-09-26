use async_trait::async_trait;
use pwhash::bcrypt;
use tokio_postgres::Transaction;

use super::database_migration::{ProMigration, ProMigrationImpl};
use crate::{
    contexts::Migration0000Initial,
    error::Result,
    layers::{
        add_from_directory::UNSORTED_COLLECTION_ID, storage::INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
    },
    pro::permissions::Role,
    util::config::get_config_element,
};

#[async_trait]
impl ProMigration for ProMigrationImpl<Migration0000Initial> {
    async fn pro_migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        let user_config = get_config_element::<crate::pro::util::config::User>()?;

        tx.batch_execute(include_str!("migration_0000_initial.sql"))
            .await?;

        let stmt = tx
            .prepare(
                r#"
            INSERT INTO roles (id, name) VALUES
                ($1, 'admin'),
                ($2, 'user'),
                ($3, 'anonymous');"#,
            )
            .await?;

        tx.execute(
            &stmt,
            &[
                &Role::admin_role_id(),
                &Role::registered_user_role_id(),
                &Role::anonymous_role_id(),
            ],
        )
        .await?;

        let stmt = tx
            .prepare(
                r#"
            INSERT INTO users (
                id, 
                email,
                password_hash,
                real_name,
                quota_available,
                active)
            VALUES (
                $1, 
                $2,
                $3,
                'admin',
                $4,
                true
            );"#,
            )
            .await?;

        let quota_available =
            crate::util::config::get_config_element::<crate::pro::util::config::Quota>()?
                .default_available_quota;

        tx.execute(
            &stmt,
            &[
                &Role::admin_role_id(),
                &user_config.admin_email,
                &bcrypt::hash(user_config.admin_password)
                    .expect("Admin password hash should be valid"),
                &quota_available,
            ],
        )
        .await?;

        let stmt = tx
            .prepare(
                r#"
            INSERT INTO user_roles 
                (user_id, role_id)
            VALUES 
                ($1, $1);"#,
            )
            .await?;

        tx.execute(&stmt, &[&Role::admin_role_id()]).await?;

        let stmt = tx
            .prepare(
                r#"
            INSERT INTO permissions
             (role_id, layer_collection_id, permission)  
            VALUES 
                ($1, $4, 'Owner'),
                ($2, $4, 'Read'),
                ($3, $4, 'Read'),
                ($1, $5, 'Owner'),
                ($2, $5, 'Read'),
                ($3, $5, 'Read');"#,
            )
            .await?;

        tx.execute(
            &stmt,
            &[
                &Role::admin_role_id(),
                &Role::registered_user_role_id(),
                &Role::anonymous_role_id(),
                &INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
                &UNSORTED_COLLECTION_ID,
            ],
        )
        .await?;

        Ok(())
    }
}
