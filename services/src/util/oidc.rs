use bytes::BytesMut;
use postgres_types::{FromSql, ToSql, to_sql_checked};
use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(transparent)]
pub struct RefreshToken(oauth2::RefreshToken);

impl RefreshToken {
    pub fn new(token: String) -> Self {
        Self(oauth2::RefreshToken::new(token))
    }

    /// Returns the actual token string.
    pub fn as_str(&self) -> &str {
        self.0.secret()
    }

    pub fn into_inner(self) -> oauth2::RefreshToken {
        self.0
    }
}

impl Deref for RefreshToken {
    type Target = oauth2::RefreshToken;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<oauth2::RefreshToken> for RefreshToken {
    fn from(token: oauth2::RefreshToken) -> Self {
        Self(token)
    }
}

impl From<String> for RefreshToken {
    fn from(token: String) -> Self {
        Self(oauth2::RefreshToken::new(token))
    }
}

impl From<&str> for RefreshToken {
    fn from(token: &str) -> Self {
        Self(oauth2::RefreshToken::new(token.to_string()))
    }
}

impl AsRef<str> for RefreshToken {
    fn as_ref(&self) -> &str {
        self.0.secret()
    }
}

impl PartialEq for RefreshToken {
    fn eq(&self, other: &RefreshToken) -> bool {
        self.as_str() == other.as_str()
    }
}

impl FromSql<'_> for RefreshToken {
    fn from_sql(
        ty: &postgres_types::Type,
        raw: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let token = <String as FromSql>::from_sql(ty, raw)?;
        Ok(Self::new(token))
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        <String as FromSql>::accepts(ty)
    }
}

impl ToSql for RefreshToken {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        out: &mut BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        <String as ToSql>::to_sql(self.0.secret(), ty, out)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        <String as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}
