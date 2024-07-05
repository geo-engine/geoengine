use async_trait::async_trait;
use tokio_postgres::Transaction;

use super::database_migration::{ProMigration, ProMigrationImpl};
use crate::{contexts::Migration0009OidcTokens, error::Result};

#[async_trait]
impl ProMigration for ProMigrationImpl<Migration0009OidcTokens> {
    async fn pro_migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(
            "
                CREATE TABLE oidc_session_tokens (
                    session_id uuid PRIMARY KEY REFERENCES sessions (id) ON DELETE CASCADE NOT NULL,
                    access_token bytea NOT NULL,
                    access_token_encryption_nonce bytea,
                    access_token_valid_until timestamp with time zone NOT NULL,
                    refresh_token bytea,
                    refresh_token_encryption_nonce bytea
                );
            ",
        )
        .await?;

        Ok(())
    }
}
