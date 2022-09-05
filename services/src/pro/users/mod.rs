mod hashmap_userdb;
mod oidc;
#[cfg(feature = "postgres")]
mod postgres_userdb;
mod session;
mod user;
mod userdb;

pub use hashmap_userdb::HashMapUserDb;
#[cfg(feature = "postgres")]
pub use postgres_userdb::PostgresUserDb;
pub use session::{UserInfo, UserSession};
pub use user::{User, UserCredentials, UserId, UserRegistration};
pub use userdb::UserDb;
pub(crate) use oidc::OidcError;
pub(super) use oidc::{AuthCodeResponse, OidcDisabled, OidcRequestDb};
#[cfg(test)]
pub(super) use oidc::{
    AuthCodeRequestURL, ExternalUserClaims, DefaultJsonWebKeySet, DefaultProviderMetadata
};
