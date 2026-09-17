//! Keep plaintext credentials in the runtime/API model and encrypt only at the
//! PostgreSQL boundary, including nested provider definitions and updates.

use super::{StacProviderAuthentication, StacProviderS3Config};
use crate::config::{DataProvider, get_config_element};
use crate::util::encryption::{
    AesGcmStringPasswordEncryption, MaybeEncryptedBytes, OptionalStringEncryption, U96,
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use bytes::BytesMut;
use postgres_types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use std::{fmt, sync::LazyLock};

type StorageError = Box<dyn std::error::Error + Send + Sync>;

// Key derivation is expensive. Settings are fixed for the process lifetime, so
// derive once instead of blocking on PBKDF2 for every database read and write.
static PASSWORD_ENCRYPTION: LazyLock<crate::error::Result<OptionalStringEncryption>> =
    LazyLock::new(|| {
        Ok(OptionalStringEncryption::new(
            get_config_element::<DataProvider>()?
                .password_encryption_key
                .map(|key| AesGcmStringPasswordEncryption::new(&key)),
        ))
    });

fn password_encryption() -> Result<&'static OptionalStringEncryption, StorageError> {
    PASSWORD_ENCRYPTION
        .as_ref()
        .map_err(|error| error.to_string().into())
}

#[derive(ToSql, FromSql)]
#[postgres(name = "StacProviderAuthentication")]
struct StoredStacProviderAuthentication {
    endpoint: String,
    client_id: String,
    username: String,
    password: Vec<u8>,
    password_encryption_nonce: Option<U96>,
}

impl fmt::Debug for StoredStacProviderAuthentication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredStacProviderAuthentication")
            .field("endpoint", &self.endpoint)
            .field("client_id", &self.client_id)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .field(
                "password_encryption_nonce",
                &self.password_encryption_nonce.is_some(),
            )
            .finish()
    }
}

#[derive(ToSql, FromSql)]
#[postgres(name = "StacProviderS3Config")]
struct StoredStacProviderS3Config {
    endpoint: String,
    access_key: Option<String>,
    secret_key: Option<String>,
    access_key_encryption_nonce: Option<U96>,
    secret_key_encryption_nonce: Option<U96>,
}

impl fmt::Debug for StoredStacProviderS3Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredStacProviderS3Config")
            .field("endpoint", &self.endpoint)
            .field(
                "access_key",
                &self.access_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "secret_key",
                &self.secret_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "access_key_encryption_nonce",
                &self.access_key_encryption_nonce.is_some(),
            )
            .field(
                "secret_key_encryption_nonce",
                &self.secret_key_encryption_nonce.is_some(),
            )
            .finish()
    }
}

fn encrypt_s3_credential(
    value: Option<&String>,
    encryption: &OptionalStringEncryption,
) -> Result<(Option<String>, Option<U96>), StorageError> {
    let Some(value) = value else {
        return Ok((None, None));
    };

    let encrypted = encryption.to_bytes(value.clone())?;
    match encrypted.nonce {
        Some(nonce) => Ok((Some(BASE64.encode(encrypted.value)), Some(nonce))),
        None => Ok((Some(value.clone()), None)),
    }
}

fn decrypt_s3_credential(
    value: Option<String>,
    nonce: Option<U96>,
    encryption: &OptionalStringEncryption,
) -> Result<Option<String>, StorageError> {
    let Some(value) = value else {
        if nonce.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "encrypted S3 credential is missing",
            )
            .into());
        }
        return Ok(None);
    };

    let Some(nonce) = nonce else {
        return Ok(Some(value));
    };

    Ok(Some(encryption.to_string(MaybeEncryptedBytes {
        value: BASE64.decode(value)?,
        nonce: Some(nonce),
    })?))
}

impl StoredStacProviderS3Config {
    fn encrypt(
        value: &StacProviderS3Config,
        encryption: &OptionalStringEncryption,
    ) -> Result<Self, StorageError> {
        let (access_key, access_key_encryption_nonce) =
            encrypt_s3_credential(value.access_key.as_ref(), encryption)?;
        let (secret_key, secret_key_encryption_nonce) =
            encrypt_s3_credential(value.secret_key.as_ref(), encryption)?;

        Ok(Self {
            endpoint: value.endpoint.clone(),
            access_key,
            secret_key,
            access_key_encryption_nonce,
            secret_key_encryption_nonce,
        })
    }

    fn decrypt(
        self,
        encryption: &OptionalStringEncryption,
    ) -> Result<StacProviderS3Config, StorageError> {
        Ok(StacProviderS3Config {
            endpoint: self.endpoint,
            access_key: decrypt_s3_credential(
                self.access_key,
                self.access_key_encryption_nonce,
                encryption,
            )?,
            secret_key: decrypt_s3_credential(
                self.secret_key,
                self.secret_key_encryption_nonce,
                encryption,
            )?,
        })
    }
}

impl StoredStacProviderAuthentication {
    fn encrypt(
        value: &StacProviderAuthentication,
        encryption: &OptionalStringEncryption,
    ) -> Result<Self, StorageError> {
        let encrypted = encryption.to_bytes(value.password.clone())?;
        Ok(Self {
            endpoint: value.endpoint.clone(),
            client_id: value.client_id.clone(),
            username: value.username.clone(),
            password: encrypted.value,
            password_encryption_nonce: encrypted.nonce,
        })
    }

    fn decrypt(
        self,
        encryption: &OptionalStringEncryption,
    ) -> Result<StacProviderAuthentication, StorageError> {
        // Rows written before encryption was enabled remain readable and are
        // encrypted on their next update. Encrypted rows always require the key.
        let password = if self.password_encryption_nonce.is_none() {
            String::from_utf8(self.password)?
        } else {
            encryption.to_string(MaybeEncryptedBytes {
                value: self.password,
                nonce: self.password_encryption_nonce,
            })?
        };
        Ok(StacProviderAuthentication {
            endpoint: self.endpoint,
            client_id: self.client_id,
            username: self.username,
            password,
        })
    }
}

impl ToSql for StacProviderAuthentication {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, StorageError> {
        StoredStacProviderAuthentication::encrypt(self, password_encryption()?)?.to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool {
        <StoredStacProviderAuthentication as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl FromSql<'_> for StacProviderAuthentication {
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, StorageError> {
        StoredStacProviderAuthentication::from_sql(ty, raw)?.decrypt(password_encryption()?)
    }

    fn accepts(ty: &Type) -> bool {
        <StoredStacProviderAuthentication as FromSql>::accepts(ty)
    }
}

impl ToSql for StacProviderS3Config {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, StorageError> {
        StoredStacProviderS3Config::encrypt(self, password_encryption()?)?.to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool {
        <StoredStacProviderS3Config as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl FromSql<'_> for StacProviderS3Config {
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, StorageError> {
        StoredStacProviderS3Config::from_sql(ty, raw)?.decrypt(password_encryption()?)
    }

    fn accepts(ty: &Type) -> bool {
        <StoredStacProviderS3Config as FromSql>::accepts(ty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{ApplicationContext, PostgresContext, SessionContext};
    use crate::datasets::external::stac::StacDataProviderDefinition;
    use crate::layers::storage::LayerProviderDb;
    use crate::users::UserSession;
    use tokio_postgres::NoTls;

    fn authentication() -> StacProviderAuthentication {
        StacProviderAuthentication {
            endpoint: "https://identity.example/token".into(),
            client_id: "client".into(),
            username: "user".into(),
            password: "database-password-must-be-encrypted".into(),
        }
    }

    fn s3_config() -> StacProviderS3Config {
        StacProviderS3Config {
            endpoint: "https://s3.example".into(),
            access_key: Some("access-key-must-be-encrypted".into()),
            secret_key: Some("secret-key-must-be-encrypted".into()),
        }
    }

    #[test]
    fn optional_s3_storage_encryption() {
        let configured = password_encryption().unwrap();
        let unconfigured = OptionalStringEncryption::new(None);
        let value = s3_config();

        let encrypted = StoredStacProviderS3Config::encrypt(&value, configured).unwrap();
        assert_ne!(encrypted.access_key.as_deref(), value.access_key.as_deref());
        assert_ne!(encrypted.secret_key.as_deref(), value.secret_key.as_deref());
        assert!(encrypted.access_key_encryption_nonce.is_some());
        assert!(encrypted.secret_key_encryption_nonce.is_some());
        assert_eq!(encrypted.decrypt(configured).unwrap(), value);

        let plaintext = StoredStacProviderS3Config::encrypt(&value, &unconfigured).unwrap();
        assert_eq!(plaintext.access_key.as_deref(), value.access_key.as_deref());
        assert_eq!(plaintext.secret_key.as_deref(), value.secret_key.as_deref());
        assert!(plaintext.access_key_encryption_nonce.is_none());
        assert!(plaintext.secret_key_encryption_nonce.is_none());
        assert_eq!(plaintext.decrypt(configured).unwrap(), value);

        assert!(
            StoredStacProviderS3Config::encrypt(&value, configured)
                .unwrap()
                .decrypt(&unconfigured)
                .is_err()
        );
    }

    #[test]
    fn optional_password_storage_encryption() {
        let configured = password_encryption().unwrap();
        let unconfigured = OptionalStringEncryption::new(None);
        let value = authentication();

        let first = StoredStacProviderAuthentication::encrypt(&value, configured).unwrap();
        let second = StoredStacProviderAuthentication::encrypt(&value, configured).unwrap();
        assert_ne!(first.password, value.password.as_bytes());
        assert_ne!(first.password, second.password);
        assert_ne!(
            first.password_encryption_nonce,
            second.password_encryption_nonce
        );
        assert_eq!(first.decrypt(configured).unwrap(), value);
        assert!(second.decrypt(&unconfigured).is_err());

        let plaintext = StoredStacProviderAuthentication::encrypt(&value, &unconfigured).unwrap();
        assert_eq!(plaintext.password, value.password.as_bytes());
        assert!(plaintext.password_encryption_nonce.is_none());
        assert_eq!(plaintext.decrypt(&unconfigured).unwrap(), value);

        let plaintext = StoredStacProviderAuthentication::encrypt(&value, &unconfigured).unwrap();
        assert_eq!(plaintext.decrypt(configured).unwrap(), value);
    }

    #[test]
    fn password_storage_rejects_wrong_key_and_corruption() {
        let configured = password_encryption().unwrap();
        let wrong_key = OptionalStringEncryption::new(Some(AesGcmStringPasswordEncryption::new(
            &"wrong-key".into(),
        )));
        let value = authentication();
        let encrypted = StoredStacProviderAuthentication::encrypt(&value, configured).unwrap();
        assert!(encrypted.decrypt(&wrong_key).is_err());

        let mut encrypted = StoredStacProviderAuthentication::encrypt(&value, configured).unwrap();
        encrypted.password[0] ^= 1;
        assert!(encrypted.decrypt(configured).is_err());
        assert!(U96::from_sql(&Type::BYTEA, &[0; 11]).is_err());
    }

    #[crate::ge_context::test]
    async fn password_storage_encrypts_provider_inserts_and_updates(
        app_ctx: PostgresContext<NoTls>,
    ) {
        let db = app_ctx.session_context(UserSession::admin_session()).db();
        let api: crate::api::model::services::StacDataProviderDefinition = serde_json::from_str(
            include_str!("../../../../../test_data/stac_responses/expected-mapping-code-de.json"),
        )
        .unwrap();
        let mut provider: StacDataProviderDefinition = api.into();
        provider.authentication = Some(authentication());
        provider.s3_config = Some(s3_config());
        let id = db
            .add_layer_provider(provider.clone().into())
            .await
            .unwrap();
        let conn = db.conn_pool.get().await.unwrap();
        let sql = "SELECT (((definition).stac_data_provider_definition).s3_config).access_key,
                          (((definition).stac_data_provider_definition).s3_config).secret_key,
                          (((definition).stac_data_provider_definition).s3_config).access_key_encryption_nonce,
                          (((definition).stac_data_provider_definition).s3_config).secret_key_encryption_nonce,
                          (((definition).stac_data_provider_definition).authentication).password,
                          (((definition).stac_data_provider_definition).authentication).password_encryption_nonce
                   FROM layer_providers WHERE id = $1";
        let row = conn.query_one(sql, &[&id]).await.unwrap();
        let access_key: String = row.get(0);
        let secret_key: String = row.get(1);
        let access_key_nonce: Vec<u8> = row.get(2);
        let secret_key_nonce: Vec<u8> = row.get(3);
        let ciphertext: Vec<u8> = row.get(4);
        let nonce: Vec<u8> = row.get(5);
        assert_ne!(access_key, s3_config().access_key.unwrap());
        assert_ne!(secret_key, s3_config().secret_key.unwrap());
        assert_eq!(access_key_nonce.len(), 12);
        assert_eq!(secret_key_nonce.len(), 12);
        assert_ne!(ciphertext, authentication().password.as_bytes());
        assert_eq!(nonce.len(), 12);
        assert_eq!(
            db.get_layer_provider_definition(id).await.unwrap(),
            provider.clone().into()
        );

        // An API round trip redacts the password. Updating another field must
        // preserve the decrypted secret and write new ciphertext, not "*****".
        let api = crate::api::model::services::StacDataProviderDefinition::from(provider.clone());
        let api: crate::api::model::services::StacDataProviderDefinition =
            serde_json::from_value(serde_json::to_value(api).unwrap()).unwrap();
        let mut update = StacDataProviderDefinition::from(api);
        update.description = "updated description".into();
        db.update_layer_provider_definition(id, update.into())
            .await
            .unwrap();
        let row = conn.query_one(sql, &[&id]).await.unwrap();
        let new_access_key: String = row.get(0);
        let new_secret_key: String = row.get(1);
        let new_ciphertext: Vec<u8> = row.get(4);
        assert_ne!(new_access_key, access_key);
        assert_ne!(new_secret_key, secret_key);
        assert_ne!(new_ciphertext, ciphertext);
        provider.description = "updated description".into();
        assert_eq!(
            db.get_layer_provider_definition(id).await.unwrap(),
            provider.clone().into()
        );

        provider.authentication.as_mut().unwrap().password = "replacement-password".into();
        db.update_layer_provider_definition(id, provider.clone().into())
            .await
            .unwrap();
        assert_eq!(
            db.get_layer_provider_definition(id).await.unwrap(),
            provider.into()
        );

        // A corrupt nonce must propagate a database decoding error, not panic.
        conn.execute(
            "UPDATE layer_providers SET definition.stac_data_provider_definition.authentication.password_encryption_nonce = $2 WHERE id = $1",
            &[&id, &vec![0_u8; 11]],
        ).await.unwrap();
        assert!(db.get_layer_provider_definition(id).await.is_err());
    }
}
