use crate::error;
use crate::error::Result;
use crate::users::session::{Session, SessionId};
use crate::users::user::User;
use crate::users::user::{UserCredentials, UserId, UserRegistration};
use crate::users::userdb::UserDB;
use crate::util::identifiers::Identifier;
use crate::util::user_input::Validated;
use async_trait::async_trait;
use bb8_postgres::bb8::Pool;
use bb8_postgres::tokio_postgres::error::SqlState;
use bb8_postgres::tokio_postgres::NoTls;
use bb8_postgres::PostgresConnectionManager;
use pwhash::bcrypt;
use uuid::Uuid;

pub struct PostgresUserDB {
    conn_pool: Pool<PostgresConnectionManager<NoTls>>, // TODO: support Tls connection as well
}

impl PostgresUserDB {
    pub async fn new(conn_pool: Pool<PostgresConnectionManager<NoTls>>) -> Result<Self> {
        let a = Self { conn_pool };
        a.update_schema().await?;
        Ok(a)
    }

    async fn schema_version(&self) -> Result<i32> {
        let conn = self.conn_pool.get().await?;

        let stmt = match conn.prepare("SELECT version from version").await {
            Ok(stmt) => stmt,
            Err(e) => {
                if let Some(code) = e.code() {
                    if *code == SqlState::UNDEFINED_TABLE {
                        // TODO: log
                        println!("UserDB: Uninitialized schema");
                        return Ok(0);
                    }
                }
                return Err(error::Error::TokioPostgres { source: e });
            }
        };

        let row = conn.query_one(&stmt, &[]).await?;

        Ok(row.get::<usize, i32>(0))
    }

    async fn update_schema(&self) -> Result<()> {
        let mut version = self.schema_version().await?;

        let conn = self.conn_pool.get().await?;
        loop {
            match version {
                0 => {
                    conn.batch_execute(
                        "\
                        CREATE TABLE version (\
                            version INT\
                        );\
                        INSERT INTO version VALUES (1);\

                        CREATE TABLE users (
                            id UUID PRIMARY KEY,
                            email character varying (256) UNIQUE,
                            password_hash character varying (256),
                            real_name character varying (256)
                        );

                        CREATE TABLE sessions (
                            id UUID PRIMARY KEY,
                            user_id UUID REFERENCES users(id)
                        );
                        ",
                    )
                    .await?;
                    // TODO log
                    println!("Updated user database to schema version {}", version + 1);
                }
                1 => {
                    conn.batch_execute(
                        "\
                        ALTER TABLE users ADD COLUMN active boolean;

                        UPDATE version SET version = 2;\
                        ",
                    )
                    .await?;
                    // TODO log
                    println!("Updated user database to schema version {}", version + 1);
                }
                _ => return Ok(()),
            }
            version += 1;
        }
    }
}

#[async_trait]
impl UserDB for PostgresUserDB {
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

        let user_id = UserId::from_uuid(row.get::<usize, Uuid>(0));
        let password_hash = row.get::<usize, &str>(1);

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

        let user_id = UserId::from_uuid(row.get::<usize, Uuid>(0));

        Ok(Session::from_fields(user_id, session))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::user_input::UserInput;
    use bb8_postgres::bb8::Pool;
    use bb8_postgres::{tokio_postgres, PostgresConnectionManager};
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
        let pg_mgr = PostgresConnectionManager::new(config, tokio_postgres::NoTls);

        let pool = Pool::builder().build(pg_mgr).await.unwrap();

        let mut db = PostgresUserDB::new(pool.clone()).await.unwrap();

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
