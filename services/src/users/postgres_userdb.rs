use crate::contexts::PostgresContext;
use crate::error;
use crate::error::Result;
use crate::projects::project::{ProjectId, ProjectPermission, STRectangle};
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
use uuid::Uuid;

pub struct PostgresUserDB<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    conn_pool: Pool<PostgresConnectionManager<Tls>>,
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
                &user.id,
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
            .map_err(|_error| error::Error::LoginFailed)?;

        let user_id = UserId(row.get(0));
        let password_hash = row.get(1);

        if bcrypt::verify(user_credentials.password, password_hash) {
            let session_id = SessionId::new();
            let stmt = conn
                .prepare(
                    "
                INSERT INTO sessions (id, user_id, created, valid_until)
                VALUES ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + make_interval(secs:=$3)) 
                RETURNING created, valid_until;",
                )
                .await?;

            // TODO: load from config
            let session_duration = chrono::Duration::days(30);
            let row = conn
                .query_one(
                    &stmt,
                    &[
                        &session_id,
                        &user_id,
                        &(session_duration.num_seconds() as f64),
                    ],
                )
                .await?;
            Ok(Session {
                id: session_id,
                user: user_id,
                created: row.get(0),
                valid_until: row.get(1),
                project: None,
                view: None,
            })
        } else {
            Err(error::Error::LoginFailed)
        }
    }

    async fn logout(&mut self, session: SessionId) -> Result<()> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("DELETE FROM sessions WHERE id = $1;") // TODO: only invalidate session?
            .await?;

        conn.execute(&stmt, &[&session])
            .await
            .map_err(|_error| error::Error::LogoutFailed)?;
        Ok(())
    }

    async fn session(&self, session: SessionId) -> Result<Session> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "
            SELECT 
                user_id, 
                created, 
                valid_until, 
                project_id,
                view
            FROM sessions 
            WHERE id = $1 AND CURRENT_TIMESTAMP < valid_until;",
            )
            .await?;

        let row = conn
            .query_one(&stmt, &[&session])
            .await
            .map_err(|_error| error::Error::SessionDoesNotExist)?;

        Ok(Session {
            id: session,
            user: UserId(row.get(0)),
            created: row.get(1),
            valid_until: row.get(2),
            project: row.get::<usize, Option<Uuid>>(3).map(ProjectId),
            view: row.get(4),
        })
    }

    async fn set_session_project(&mut self, session: &Session, project: ProjectId) -> Result<()> {
        let conn = self.conn_pool.get().await?;
        PostgresContext::check_user_project_permission(
            &conn,
            session.user,
            project,
            &[
                ProjectPermission::Read,
                ProjectPermission::Write,
                ProjectPermission::Owner,
            ],
        )
        .await?;

        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("UPDATE sessions SET project_id = $1 WHERE id = $2;")
            .await?;

        conn.execute(&stmt, &[&project, &session.id]).await?;

        Ok(())
    }

    async fn set_session_view(&mut self, session: &Session, view: STRectangle) -> Result<()> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("UPDATE sessions SET view = $1 WHERE id = $2;")
            .await?;

        conn.execute(&stmt, &[&view, &session.id]).await?;

        Ok(())
    }
}
