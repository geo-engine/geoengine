use crate::error;
use crate::error::Result;
use crate::users::session::{Session, SessionId};
use crate::users::user::User;
use crate::users::user::{UserCredentials, UserId, UserRegistration};
use crate::users::userdb::UserDB;
use crate::util::identifiers::Identifier;
use crate::util::user_input::Validated;
use async_trait::async_trait;
use bb8_postgres::PostgresConnectionManager;
use bb8_postgres::{
    bb8::Pool, tokio_postgres::tls::MakeTlsConnect, tokio_postgres::tls::TlsConnect,
    tokio_postgres::Socket,
};
use pwhash::bcrypt;

pub struct PostgresUserDB<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    conn_pool: Pool<PostgresConnectionManager<Tls>>, // TODO: support Tls connection as well
}

impl<Tls> PostgresUserDB<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new(conn_pool: Pool<PostgresConnectionManager<Tls>>) -> Self {
        Self { conn_pool }
    }
}

#[async_trait]
impl<Tls> UserDB for PostgresUserDB<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    // TODO: clean up expired sessions?

    async fn register(&mut self, user: Validated<UserRegistration>) -> Result<UserId> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "INSERT INTO users (id, email, password_hash, real_name, active) VALUES ($1, $2, $3, $4, $5);",
            )
            .await?;

        let user = User::from(user.user_input);
        conn.execute(
            &stmt,
            &[
                &user.id.uuid(),
                &user.email,
                &user.password_hash,
                &user.real_name,
                &user.active,
            ],
        )
        .await?;

        Ok(user.id)
    }

    async fn login(&mut self, user_credentials: UserCredentials) -> Result<Session> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("SELECT id, password_hash FROM users WHERE email = $1;")
            .await?;

        let row = conn
            .query_one(&stmt, &[&user_credentials.email])
            .await
            .map_err(|_| error::Error::LoginFailed)?;

        let user_id = UserId::from_uuid(row.get(0));
        let password_hash = row.get(1);

        if bcrypt::verify(user_credentials.password, password_hash) {
            let session = Session::from_user_id(user_id);

            let stmt = conn
                .prepare("INSERT INTO sessions (id, user_id) VALUES ($1, $2);")
                .await?;

            conn.execute(&stmt, &[&session.id.uuid(), &user_id.uuid()])
                .await?;
            Ok(session)
        } else {
            Err(error::Error::LoginFailed)
        }
    }

    async fn logout(&mut self, session: SessionId) -> Result<()> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("DELETE FROM sessions WHERE id = $1;") // TODO: only invalidate session?
            .await?;

        conn.execute(&stmt, &[&session.uuid()])
            .await
            .map_err(|_| error::Error::LogoutFailed)?;
        Ok(())
    }

    async fn session(&self, session: SessionId) -> Result<Session> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("SELECT user_id FROM sessions WHERE id = $1;") // TODO: check session is still valid
            .await?;

        let row = conn
            .query_one(&stmt, &[&session.uuid()])
            .await
            .map_err(|_| error::Error::SessionDoesNotExist)?;

        let user_id = UserId::from_uuid(row.get(0));

        Ok(Session::from_fields(user_id, session))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::Context;
    use crate::{contexts::PostgresContext, util::user_input::UserInput};
    use bb8_postgres::tokio_postgres;
    use std::str::FromStr;

    #[tokio::test]
    #[ignore]
    async fn test() {
        // TODO: load from test config
        // TODO: add postgres to ci
        // TODO: clear database
        let config = tokio_postgres::config::Config::from_str(
            "postgresql://geoengine:geoengine@localhost:5432",
        )
        .unwrap();

        let ctx = PostgresContext::new(config, tokio_postgres::NoTls)
            .await
            .unwrap();

        let user_db = ctx.user_db();
        let mut db = user_db.write().await;

        let user_registration = UserRegistration {
            email: "foo@bar.de".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        }
        .validated()
        .unwrap();

        assert!(db.register(user_registration).await.is_ok());

        let credentials = UserCredentials {
            email: "foo@bar.de".into(),
            password: "secret123".into(),
        };

        let result = db.login(credentials).await;
        assert!(result.is_ok());

        let session = result.unwrap();

        assert!(db.session(session.id).await.is_ok());

        assert!(db.logout(session.id).await.is_ok());

        assert!(db.session(session.id).await.is_err());
    }
}
