use crate::error;
use crate::error::Result;
use crate::users::session::{Session, SessionToken};
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
use snafu::ResultExt;

struct PostgresUserDB {
    conn_pool: Pool<PostgresConnectionManager<NoTls>>, // TODO: support Tls connection as well
}

impl PostgresUserDB {
    async fn new(conn_pool: Pool<PostgresConnectionManager<NoTls>>) -> Result<Self> {
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
    async fn register(&mut self, user: Validated<UserRegistration>) -> Result<UserId> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "INSERT INTO users (id, email, password_hash, real_name, active) VALUES ($1, $2, $3, $4, $5);",
            )
            .await
            .context(error::TokioPostgres)?;

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

    async fn login(&mut self, user: UserCredentials) -> Result<Session> {
        todo!()
    }

    async fn logout(&mut self, session: SessionToken) -> Result<()> {
        todo!()
    }

    async fn session(&self, token: SessionToken) -> Result<Session> {
        todo!()
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
    }
}
