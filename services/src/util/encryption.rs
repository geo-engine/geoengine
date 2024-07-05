use crate::error::Result;
use aes_gcm::aead::consts::U12;
use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::{Aead, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, Key, KeyInit, Nonce};
use bytes::BytesMut;
use pbkdf2::pbkdf2_hmac_array;
use postgres_types::{accepts, FromSql, IsNull, ToSql, Type};
use sha2::Sha256;
use snafu::Snafu;
use std::error::Error;
use std::fmt::Debug;
use std::string::FromUtf8Error;

const SALT: &[u8] = "CUSTOM_SALT".as_bytes();
const ROUNDS: u32 = 1_000_000;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))]
pub enum EncryptionError {
    #[snafu(display("AesGcm Error {:?}", error))]
    AesGcm {
        error: aes_gcm::Error,
    },
    StringConversion {
        source: FromUtf8Error,
    },
    #[snafu(display("Option Mismatch: Is Some {}, Is None: {}", some, none))]
    FailedOptionalDecryption {
        some: String,
        none: String,
    },
}

impl From<aes_gcm::Error> for EncryptionError {
    fn from(source: aes_gcm::Error) -> Self {
        EncryptionError::AesGcm { error: source }
    }
}

impl From<FromUtf8Error> for EncryptionError {
    fn from(source: FromUtf8Error) -> Self {
        EncryptionError::StringConversion { source }
    }
}

#[derive(Debug, PartialEq)]
pub struct U96(GenericArray<u8, U12>);

impl From<Nonce<U12>> for U96 {
    fn from(value: Nonce<U12>) -> Self {
        U96(value)
    }
}

impl ToSql for U96 {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let tuple: [u8; 12] = self.0.as_slice().try_into()?;
        <[u8; 12] as ToSql>::to_sql(&tuple, ty, out)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        <[u8; 12] as ToSql>::accepts(ty)
    }

    postgres_types::to_sql_checked!();
}

impl<'a> FromSql<'a> for U96 {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(U96(*GenericArray::from_slice(raw)))
    }

    accepts!(BYTEA);
}

pub struct EncryptedString {
    pub ciphertext: Vec<u8>,
    pub nonce: U96,
}

#[derive(Clone)]
pub struct AesGcmStringPasswordEncryption {
    encryption_key: Vec<u8>,
}

impl AesGcmStringPasswordEncryption {
    pub fn new(password: &String) -> AesGcmStringPasswordEncryption {
        let encryption_key =
            pbkdf2_hmac_array::<Sha256, 32>(password.as_bytes(), SALT, ROUNDS).to_vec();
        AesGcmStringPasswordEncryption { encryption_key }
    }

    pub fn encrypt(&self, value: &String) -> Result<EncryptedString, EncryptionError> {
        let key = Key::<Aes256Gcm>::from_slice(&self.encryption_key);
        let cipher = Aes256Gcm::new(key);

        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = cipher.encrypt(&nonce, value.as_bytes())?;

        let result = EncryptedString {
            ciphertext,
            nonce: nonce.into(),
        };

        Ok(result)
    }

    pub fn decrypt(&self, value: &EncryptedString) -> Result<String, EncryptionError> {
        let key = Key::<Aes256Gcm>::from_slice(&self.encryption_key);
        let cipher = Aes256Gcm::new(key);
        let plaintext = cipher.decrypt(&value.nonce.0, value.ciphertext.as_ref())?;

        Ok(String::from_utf8(plaintext)?)
    }
}

pub struct MaybeEncryptedBytes {
    pub value: Vec<u8>,
    pub nonce: Option<U96>,
}

#[derive(Clone)]
pub struct OptionalStringEncryption {
    string_encryption: Option<AesGcmStringPasswordEncryption>,
}

impl OptionalStringEncryption {
    pub fn new(
        string_encryption: Option<AesGcmStringPasswordEncryption>,
    ) -> OptionalStringEncryption {
        OptionalStringEncryption { string_encryption }
    }

    pub fn to_bytes(&self, value: String) -> Result<MaybeEncryptedBytes, EncryptionError> {
        let (value, nonce) = if let Some(encryption) = &self.string_encryption {
            let encrypted = encryption.encrypt(&value)?;
            (encrypted.ciphertext, Some(encrypted.nonce))
        } else {
            (value.into_bytes(), None)
        };

        let result = MaybeEncryptedBytes { value, nonce };

        Ok(result)
    }

    #[allow(clippy::unwrap_used, clippy::missing_panics_doc)]
    pub fn to_string(&self, field: MaybeEncryptedBytes) -> Result<String, EncryptionError> {
        if self.string_encryption.is_none() && field.nonce.is_some() {
            Err(EncryptionError::FailedOptionalDecryption {
                some: "Nonce".to_string(),
                none: "Encryption Key".to_string(),
            })
        } else if self.string_encryption.is_some() && field.nonce.is_none() {
            Err(EncryptionError::FailedOptionalDecryption {
                some: "Encryption Key".to_string(),
                none: "Nonce".to_string(),
            })
        } else if let Some(encryption) = &self.string_encryption {
            Ok(encryption.decrypt(&EncryptedString {
                ciphertext: field.value,
                nonce: field.nonce.unwrap(),
            })?)
        } else {
            let utf_conversion = String::from_utf8(field.value)?;
            Ok(utf_conversion)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::util::encryption::{AesGcmStringPasswordEncryption, OptionalStringEncryption};

    #[test]
    fn it_generates_key() {
        let encryption = AesGcmStringPasswordEncryption::new(&"password123".to_string());
        assert_eq!(encryption.encryption_key.len(), 32);
    }

    #[test]
    fn it_encrypts_decrypts() {
        let encryption = AesGcmStringPasswordEncryption::new(&"complex_password".to_string());

        let plaintext = "Decrypt_me".to_string();
        let encrypted = encryption.encrypt(&plaintext).unwrap();
        assert_ne!(plaintext.as_bytes(), encrypted.ciphertext);

        let decrypted = encryption.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn it_handles_optional_encryption() {
        let plaintext = "Decrypt_me".to_string();

        let encryption = OptionalStringEncryption::new(Some(AesGcmStringPasswordEncryption::new(
            &"complex_password".to_string(),
        )));
        let encrypted = encryption.to_bytes(plaintext.clone()).unwrap();
        assert!(encrypted.nonce.is_some());
        assert_ne!(plaintext.as_bytes(), encrypted.value);
        let decrypted = encryption.to_string(encrypted).unwrap();
        assert_eq!(decrypted, plaintext);

        let no_encryption = OptionalStringEncryption::new(None);
        let encrypted = no_encryption.to_bytes(plaintext.clone()).unwrap();
        assert!(encrypted.nonce.is_none());
        assert_eq!(plaintext.as_bytes(), encrypted.value);
        let decrypted = no_encryption.to_string(encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }
}
