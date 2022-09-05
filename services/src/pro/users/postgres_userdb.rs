use crate::contexts::SessionId;
use crate::error::Result;
use crate::pro::datasets::Role;
use crate::pro::projects::ProjectPermission;
use crate::pro::users::oidc::ExternalUserClaims;
use crate::pro::users::{
    User, UserCredentials, UserDb, UserId, UserInfo, UserRegistration, UserSession,
};
use crate::projects::{ProjectId, STRectangle};
use crate::util::user_input::Validated;
use crate::util::Identifier;
use crate::{error, pro::contexts::PostgresContext};
use async_trait::async_trait;
use bb8_postgres::PostgresConnectionManager;
use bb8_postgres::{
    bb8::Pool, tokio_postgres::tls::MakeTlsConnect, tokio_postgres::tls::TlsConnect,
    tokio_postgres::Socket,
};
use geoengine_datatypes::primitives::Duration;
use pwhash::bcrypt;
use uuid::Uuid;

pub struct PostgresUserDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    conn_pool: Pool<PostgresConnectionManager<Tls>>,
}

impl<Tls> PostgresUserDb<Tls>
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
impl<Tls> UserDb for PostgresUserDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    // TODO: clean up expired sessions?

    async fn register(&self, user: Validated<UserRegistration>) -> Result<UserId> {
        let mut conn = self.conn_pool.get().await?;

        let tx = conn.build_transaction().start().await?;

        let user = User::from(user.user_input);

        let stmt = tx
            .prepare("INSERT INTO roles (id, name) VALUES ($1, $2);")
            .await?;
        tx.execute(&stmt, &[&user.id, &user.email]).await?;

        let stmt = tx
            .prepare(
                "INSERT INTO users (id, email, password_hash, real_name, active) VALUES ($1, $2, $3, $4, $5);",
            )
            .await?;

        tx.execute(
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

        let stmt = tx
            .prepare("INSERT INTO user_roles (user_id, role_id) VALUES ($1, $2);")
            .await?;
        tx.execute(&stmt, &[&user.id, &user.id]).await?;

        let stmt = tx
            .prepare("INSERT INTO user_roles (user_id, role_id) VALUES ($1, $2);")
            .await?;
        tx.execute(&stmt, &[&user.id, &Role::user_role_id()])
            .await?;

        tx.commit().await?;

        Ok(user.id)
    }

    async fn anonymous(&self) -> Result<UserSession> {
        let mut conn = self.conn_pool.get().await?;

        let tx = conn.build_transaction().start().await?;

        let user_id = UserId::new();

        let stmt = tx
            .prepare("INSERT INTO roles (id, name) VALUES ($1, $2);")
            .await?;
        tx.execute(&stmt, &[&user_id, &"anonymous_user"]).await?;

        let stmt = tx
            .prepare("INSERT INTO users (id, active) VALUES ($1, TRUE);")
            .await?;

        tx.execute(&stmt, &[&user_id]).await?;

        let stmt = tx
            .prepare("INSERT INTO user_roles (user_id, role_id) VALUES ($1, $2);")
            .await?;
        tx.execute(&stmt, &[&user_id, &user_id]).await?;

        let stmt = tx
            .prepare("INSERT INTO user_roles (user_id, role_id) VALUES ($1, $2);")
            .await?;
        tx.execute(&stmt, &[&user_id, &Role::anonymous_role_id()])
            .await?;

        let session_id = SessionId::new();
        let stmt = tx
            .prepare(
                "
                INSERT INTO sessions (id, user_id, created, valid_until)
                VALUES ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + make_interval(secs:=$3)) 
                RETURNING created, valid_until;",
            )
            .await?;

        // TODO: load from config
        let session_duration = chrono::Duration::days(30);
        let row = tx
            .query_one(
                &stmt,
                &[
                    &session_id,
                    &user_id,
                    &(session_duration.num_seconds() as f64),
                ],
            )
            .await?;

        tx.commit().await?;

        Ok(UserSession {
            id: session_id,
            user: UserInfo {
                id: user_id,
                email: None,
                real_name: None,
            },
            created: row.get(0),
            valid_until: row.get(1),
            project: None,
            view: None,
            roles: vec![user_id.into(), Role::anonymous_role_id()],
        })
    }

    async fn login(&self, user_credentials: UserCredentials) -> Result<UserSession> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("SELECT id, password_hash, email, real_name FROM users WHERE email = $1;")
            .await?;

        let row = conn
            .query_one(&stmt, &[&user_credentials.email])
            .await
            .map_err(|_error| error::Error::LoginFailed)?;

        let user_id = UserId(row.get(0));
        let password_hash = row.get(1);
        let email = row.get(2);
        let real_name = row.get(3);

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

            let stmt = conn
                .prepare("SELECT role_id FROM user_roles WHERE user_id = $1;")
                .await?;

            let rows = conn
                .query(&stmt, &[&user_id])
                .await
                .map_err(|_error| error::Error::LoginFailed)?;

            let roles = rows.into_iter().map(|row| row.get(0)).collect();

            Ok(UserSession {
                id: session_id,
                user: UserInfo {
                    id: user_id,
                    email,
                    real_name,
                },
                created: row.get(0),
                valid_until: row.get(1),
                project: None,
                view: None,
                roles,
            })
        } else {
            Err(error::Error::LoginFailed)
        }
    }

    async fn login_external(
        &self,
        user: ExternalUserClaims,
        duration: Duration,
    ) -> Result<UserSession> {
        let mut conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("SELECT id, external_id, email, real_name FROM external_users WHERE external_id = $1;")
            .await?;

        let row = conn
            .query_opt(&stmt, &[&user.external_id.to_string()])
            .await
            .map_err(|_error| error::Error::LoginFailed)?;

        let user_id = match row {
            Some(row) => UserId(row.get(0)),
            None => {
                let tx = conn.build_transaction().start().await?;

                let user_id = UserId::new();

                let stmt = tx
                    .prepare("INSERT INTO roles (id, name) VALUES ($1, $2);")
                    .await?;
                tx.execute(&stmt, &[&user_id, &user.email]).await?;

                //TODO: Inconsistent to hashmap implementation, where an external user is not part of the user database.
                //TODO: A user might be able to login without external login using this (internal) id. Would be a problem with anonymous users as well.
                let stmt = tx
                    .prepare("INSERT INTO users (id, active) VALUES ($1, TRUE);")
                    .await?;
                tx.execute(&stmt, &[&user_id]).await?;

                let stmt = tx
                    .prepare(
                        "INSERT INTO external_users (id, external_id, email, real_name, active) VALUES ($1, $2, $3, $4, $5);",
                    )
                    .await?;

                tx.execute(
                    &stmt,
                    &[
                        &user_id,
                        &user.external_id.to_string(),
                        &user.email,
                        &user.real_name,
                        &true,
                    ],
                )
                .await?;

                let stmt = tx
                    .prepare("INSERT INTO user_roles (user_id, role_id) VALUES ($1, $2);")
                    .await?;
                tx.execute(&stmt, &[&user_id, &user_id]).await?;

                let stmt = tx
                    .prepare("INSERT INTO user_roles (user_id, role_id) VALUES ($1, $2);")
                    .await?;
                tx.execute(&stmt, &[&user_id, &Role::user_role_id()])
                    .await?;

                tx.commit().await?;

                user_id
            }
        };

        let session_id = SessionId::new();
        let stmt = conn
            .prepare(
                "
            INSERT INTO sessions (id, user_id, created, valid_until)
            VALUES ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + make_interval(secs:=$3))
            RETURNING created, valid_until;",
            )
            .await?; //TODO: Check documentation if inconsistent to hashmap implementation - would happen if CURRENT_TIMESTAMP is called twice in postgres for a single query. Worked in tests.

        let row = conn
            .query_one(
                &stmt,
                &[&session_id, &user_id, &(duration.num_seconds() as f64)],
            )
            .await?;

        let stmt = conn
            .prepare("SELECT role_id FROM user_roles WHERE user_id = $1;")
            .await?;

        let rows = conn
            .query(&stmt, &[&user_id])
            .await
            .map_err(|_error| error::Error::LoginFailed)?;

        let roles = rows.into_iter().map(|row| row.get(0)).collect();

        Ok(UserSession {
            id: session_id,
            user: UserInfo {
                id: user_id,
                email: Some(user.email.clone()),
                real_name: Some(user.real_name.clone()),
            },
            created: row.get(0),
            valid_until: row.get(1),
            project: None,
            view: None,
            roles,
        })
    }

    async fn logout(&self, session: SessionId) -> Result<()> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("DELETE FROM sessions WHERE id = $1;") // TODO: only invalidate session?
            .await?;

        conn.execute(&stmt, &[&session])
            .await
            .map_err(|_error| error::Error::LogoutFailed)?;
        Ok(())
    }

    async fn session(&self, session: SessionId) -> Result<UserSession> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "
            SELECT 
                u.id,   
                u.email,
                u.real_name,             
                s.created, 
                s.valid_until, 
                s.project_id,
                s.view           
            FROM sessions s JOIN users u ON (s.user_id = u.id)
            WHERE s.id = $1 AND CURRENT_TIMESTAMP < s.valid_until;",
            )
            .await?;

        let row = conn
            .query_one(&stmt, &[&session])
            .await
            .map_err(|_error| error::Error::InvalidSession)?;

        Ok(UserSession {
            id: session,
            user: UserInfo {
                id: row.get(0),
                email: row.get(1),
                real_name: row.get(2),
            },
            created: row.get(3),
            valid_until: row.get(4),
            project: row.get::<usize, Option<Uuid>>(5).map(ProjectId),
            view: row.get(6),
            roles: vec![], // TODO
        })
    }

    async fn set_session_project(&self, session: &UserSession, project: ProjectId) -> Result<()> {
        let conn = self.conn_pool.get().await?;
        PostgresContext::check_user_project_permission(
            &conn,
            session.user.id,
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

    async fn set_session_view(&self, session: &UserSession, view: STRectangle) -> Result<()> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("UPDATE sessions SET view = $1 WHERE id = $2;")
            .await?;

        conn.execute(&stmt, &[&view, &session.id]).await?;

        Ok(())
    }
}
