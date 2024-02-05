use async_trait::async_trait;
use pwhash::bcrypt;
use tokio_postgres::Transaction;
use uuid::Uuid;

use super::database_migration::{ProMigration, ProMigrationImpl};
use crate::{contexts::Migration0000Initial, error::Result, util::config::get_config_element};

const INTERNAL_LAYER_DB_ROOT_COLLECTION_ID: Uuid =
    Uuid::from_u128(0x0510_2bb3_a855_4a37_8a8a_3002_6a91_fef1);
const UNSORTED_COLLECTION_ID: Uuid = Uuid::from_u128(0xffb2_dd9e_f5ad_427c_b7f1_c9a0_c7a0_ae3f);

const ADMIN_ROLE_ID: Uuid = Uuid::from_u128(0xd532_8854_6190_4af9_ad69_4e74_b096_1ac9);
const REGISTERED_USER_ROLE_ID: Uuid = Uuid::from_u128(0x4e80_81b6_8aa6_4275_af0c_2fa2_da55_7d28);
const ANONYMOUS_USER_ROLE_ID: Uuid = Uuid::from_u128(0xfd8e_87bf_515c_4f36_8da6_1a53_702f_f102);

const ADMIN_QUOTA: i64 = 9_223_372_036_854_775_807; // max postgres `bigint` value

#[async_trait]
impl ProMigration for ProMigrationImpl<Migration0000Initial> {
    async fn pro_migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        let user_config = get_config_element::<crate::pro::util::config::User>()?;

        tx.batch_execute(include_str!("migration_0000_initial.sql"))
            .await?;

        let stmt = tx
            .prepare(
                "
            INSERT INTO roles (id, name) VALUES
                ($1, 'admin'),
                ($2, 'user'),
                ($3, 'anonymous');",
            )
            .await?;

        tx.execute(
            &stmt,
            &[
                &ADMIN_ROLE_ID,
                &REGISTERED_USER_ROLE_ID,
                &ANONYMOUS_USER_ROLE_ID,
            ],
        )
        .await?;

        let stmt = tx
            .prepare(
                "
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
            );",
            )
            .await?;

        tx.execute(
            &stmt,
            &[
                &ADMIN_ROLE_ID,
                &user_config.admin_email,
                &bcrypt::hash(user_config.admin_password)
                    .expect("Admin password hash should be valid"),
                &ADMIN_QUOTA,
            ],
        )
        .await?;

        let stmt = tx
            .prepare(
                "
            INSERT INTO user_roles 
                (user_id, role_id)
            VALUES 
                ($1, $1);",
            )
            .await?;

        tx.execute(&stmt, &[&ADMIN_ROLE_ID]).await?;

        let stmt = tx
            .prepare(
                "
            INSERT INTO permissions
             (role_id, layer_collection_id, permission)  
            VALUES 
                ($1, $4, 'Owner'),
                ($2, $4, 'Read'),
                ($3, $4, 'Read'),
                ($1, $5, 'Owner'),
                ($2, $5, 'Read'),
                ($3, $5, 'Read');",
            )
            .await?;

        tx.execute(
            &stmt,
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
