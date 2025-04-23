use crate::api::handlers::users::UsageSummaryGranularity;
use crate::contexts::PostgresDb;
use crate::contexts::{ApplicationContext, SessionId};
use crate::error::{Error, Result};
use crate::permissions::TxPermissionDb;
use crate::permissions::{Role, RoleDescription, RoleId};
use crate::projects::{ProjectId, STRectangle};
use crate::quota::{ComputationQuota, DataUsage, DataUsageSummary, OperatorQuota};
use crate::users::oidc::{FlatMaybeEncryptedOidcTokens, OidcTokens, UserClaims};
use crate::users::userdb::{
    CannotRevokeRoleThatIsNotAssignedRoleDbError, RoleIdDoesNotExistRoleDbError,
};
use crate::users::{
    SessionTokenStore, StoredOidcTokens, User, UserCredentials, UserDb, UserId, UserInfo,
    UserRegistration, UserSession,
};
use crate::util::Identifier;
use crate::util::postgres::PostgresErrorExt;
use crate::{contexts::PostgresContext, error};
use async_trait::async_trait;
use geoengine_operators::meta::quota::ComputationUnit;

use crate::util::encryption::MaybeEncryptedBytes;
use bb8_postgres::{
    tokio_postgres::Socket, tokio_postgres::tls::MakeTlsConnect, tokio_postgres::tls::TlsConnect,
};
use oauth2::AccessToken;
use pwhash::bcrypt;
use snafu::{ResultExt, ensure};
use tokio_postgres::Transaction;
use uuid::Uuid;

use super::userdb::{
    Bb8RoleDbError, PermissionDbRoleDbError, PostgresRoleDbError, RoleDb, RoleDbError, UserAuth,
};

#[async_trait]
impl<Tls> UserAuth for PostgresContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    // TODO: clean up expired sessions?

    async fn register_user(&self, user: UserRegistration) -> Result<UserId> {
        let mut conn = self.pool.get().await?;

        let tx = conn.build_transaction().start().await?;

        let user = User::from(user);

        let stmt = tx
            .prepare("INSERT INTO roles (id, name) VALUES ($1, $2);")
            .await?;
        tx.execute(&stmt, &[&user.id, &user.email])
            .await
            .map_unique_violation("roles", "name", || error::Error::Duplicate {
                reason: "E-mail already exists".to_string(),
            })?;

        let stmt = tx
            .prepare(
                "INSERT INTO users (id, email, password_hash, real_name, quota_available, active) VALUES ($1, $2, $3, $4, $5, $6);",
            )
            .await?;

        let quota_available =
            crate::config::get_config_element::<crate::config::Quota>()?.initial_credits;

        tx.execute(
            &stmt,
            &[
                &user.id,
                &user.email,
                &user.password_hash,
                &user.real_name,
                &quota_available,
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
        tx.execute(&stmt, &[&user.id, &Role::registered_user_role_id()])
            .await?;

        tx.commit().await?;

        Ok(user.id)
    }

    async fn create_anonymous_session(&self) -> Result<UserSession> {
        let mut conn = self.pool.get().await?;

        let tx = conn.build_transaction().start().await?;

        let user_id = UserId::new();

        let stmt = tx
            .prepare("INSERT INTO roles (id, name) VALUES ($1, $2);")
            .await?;
        tx.execute(&stmt, &[&user_id, &format!("anonymous_user_{user_id}")])
            .await?;

        let quota_available =
            crate::config::get_config_element::<crate::config::Quota>()?.initial_credits;

        let stmt = tx
            .prepare("INSERT INTO users (id, quota_available, active) VALUES ($1, $2, TRUE);")
            .await?;

        tx.execute(&stmt, &[&user_id, &quota_available]).await?;

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

        // TODO: load from config
        let session_duration = chrono::Duration::days(30);
        let row = tx
            .query_one(
                "
                INSERT INTO 
                    sessions (id, user_id, created, valid_until) 
                VALUES 
                    ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + make_interval(secs:=$3))
                RETURNING 
                    created, valid_until;
                ",
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
        let mut conn = self.pool.get().await?;

        let tx = conn.build_transaction().start().await?;

        let stmt = tx
            .prepare("SELECT id, password_hash, email, real_name FROM users WHERE email = $1;")
            .await?;

        let row = tx
            .query_one(&stmt, &[&user_credentials.email])
            .await
            .map_err(|_error| error::Error::LoginFailed)?;

        let user_id = UserId(row.get(0));
        let password_hash = row.get(1);
        let email = row.get(2);
        let real_name = row.get(3);

        if bcrypt::verify(user_credentials.password, password_hash) {
            let session_id = SessionId::new();

            // TODO: load from config
            let session_duration = chrono::Duration::days(30);

            let row = tx
                .query_one(
                    "
                    INSERT INTO 
                        sessions (id, user_id, created, valid_until) 
                    VALUES 
                        ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + make_interval(secs:=$3))
                    RETURNING 
                        created, valid_until;
                    ",
                    &[
                        &session_id,
                        &user_id,
                        &(session_duration.num_seconds() as f64),
                    ],
                )
                .await?;

            let stmt = tx
                .prepare("SELECT role_id FROM user_roles WHERE user_id = $1;")
                .await?;

            let rows = tx
                .query(&stmt, &[&user_id])
                .await
                .map_err(|_error| error::Error::LoginFailed)?;

            tx.commit().await?;

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

    #[allow(clippy::too_many_lines)]
    async fn login_external(
        &self,
        user: UserClaims,
        oidc_tokens: OidcTokens,
    ) -> Result<UserSession> {
        let mut conn = self.pool.get().await?;
        let tx = conn.build_transaction().start().await?;

        let stmt = tx
            .prepare("SELECT id, external_id, email, real_name FROM external_users WHERE external_id = $1;")
            .await?;

        let row = tx
            .query_opt(&stmt, &[&user.external_id.to_string()])
            .await
            .map_err(|_error| error::Error::LoginFailed)?;

        let user_id = match row {
            Some(row) => UserId(row.get(0)),
            None => {
                let user_id = UserId::new();

                let stmt = tx
                    .prepare("INSERT INTO roles (id, name) VALUES ($1, $2);")
                    .await?;
                tx.execute(&stmt, &[&user_id, &user.email]).await?;

                let quota_available =
                    crate::config::get_config_element::<crate::config::Quota>()?.initial_credits;

                //TODO: A user might be able to login without external login using this (internal) id. Would be a problem with anonymous users as well.
                let stmt = tx
                    .prepare(
                        "INSERT INTO users (id, quota_available, active) VALUES ($1, $2, TRUE);",
                    )
                    .await?;
                tx.execute(&stmt, &[&user_id, &quota_available]).await?;

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
                tx.execute(&stmt, &[&user_id, &Role::registered_user_role_id()])
                    .await?;

                user_id
            }
        };

        let session_id = SessionId::new();

        let row = tx
            .query_one(
                "
                INSERT INTO 
                    sessions (id, user_id, created, valid_until) 
                VALUES 
                    ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + make_interval(secs:=$3))
                RETURNING 
                    created, valid_until;
                ",
                &[
                    &session_id,
                    &user_id,
                    &(oidc_tokens.expires_in.num_seconds() as f64),
                ],
            )
            .await?;

        self.store_oidc_session_tokens(session_id, oidc_tokens, &tx)
            .await?;

        let stmt = tx
            .prepare("SELECT role_id FROM user_roles WHERE user_id = $1;")
            .await?;

        let rows = tx
            .query(&stmt, &[&user_id])
            .await
            .map_err(|_error| error::Error::LoginFailed)?;

        let roles = rows.into_iter().map(|row| row.get(0)).collect();

        tx.commit().await?;

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

    async fn user_session_by_id(&self, session: SessionId) -> Result<UserSession> {
        let mut conn = self.pool.get().await?;

        let tx = conn.build_transaction().start().await?;

        let stmt = tx
            .prepare(
                "
            SELECT 
                u.id,   
                COALESCE(u.email, eu.email) AS email,
                COALESCE(u.real_name, eu.real_name) AS real_name,
                s.created, 
                s.valid_until, 
                s.project_id,
                s.view,
                CASE WHEN CURRENT_TIMESTAMP < s.valid_until THEN TRUE ELSE FALSE END AS valid_session
            FROM
                sessions s
                    JOIN users u ON (s.user_id = u.id)
                    LEFT JOIN external_users eu ON (u.id = eu.id)
                    LEFT JOIN oidc_session_tokens t ON (s.id = t.session_id)
            WHERE s.id = $1 AND (CURRENT_TIMESTAMP < s.valid_until OR t.refresh_token IS NOT NULL);",
            )
            .await?;

        let row = tx
            .query_one(&stmt, &[&session])
            .await
            .map_err(|_error| error::Error::InvalidSession)?;

        let valid_session: bool = row.get(7);

        let valid_until = if valid_session {
            row.get(4)
        } else {
            log::debug!("Session expired, trying to extend");
            let refresh_result = self.refresh_oidc_session_tokens(session, &tx).await;

            if let Err(refresh_error) = refresh_result {
                log::debug!("Session extension failed {refresh_error}");
                return Err(Error::InvalidSession);
            }
            log::debug!("Session extended");
            refresh_result
                .expect("Refresh result should exist")
                .db_valid_until
        };

        let mut session = UserSession {
            id: session,
            user: UserInfo {
                id: row.get(0),
                email: row.get(1),
                real_name: row.get(2),
            },
            created: row.get(3),
            valid_until,
            project: row.get::<usize, Option<Uuid>>(5).map(ProjectId),
            view: row.get(6),
            roles: vec![],
        };

        let stmt = tx
            .prepare(
                "
            SELECT role_id FROM user_roles WHERE user_id = $1;
            ",
            )
            .await?;

        let rows = tx.query(&stmt, &[&session.user.id]).await?;

        tx.commit().await?;

        session.roles = rows.into_iter().map(|row| row.get(0)).collect();

        Ok(session)
    }
}

#[async_trait]
impl<Tls> SessionTokenStore for PostgresContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn store_oidc_session_tokens(
        &self,
        session: SessionId,
        oidc_tokens: OidcTokens,
        tx: &Transaction<'_>,
    ) -> Result<StoredOidcTokens> {
        let flat_tokens: FlatMaybeEncryptedOidcTokens = self
            .oidc_manager()
            .maybe_encrypt_tokens(&oidc_tokens)?
            .into();

        let stmt = tx
            .prepare(
                "
                INSERT INTO oidc_session_tokens
                    (session_id, access_token, access_token_encryption_nonce, access_token_valid_until, refresh_token, refresh_token_encryption_nonce)
                VALUES
                    ($1, $2, $3, CURRENT_TIMESTAMP + make_interval(secs:=$4), $5, $6)
                RETURNING
                    access_token_valid_until;
                ;"
            )
            .await?;

        let db_valid_until = tx
            .query_one(
                &stmt,
                &[
                    &session,
                    &flat_tokens.access_token_value,
                    &flat_tokens.access_token_nonce,
                    &(oidc_tokens.expires_in.num_seconds() as f64),
                    &flat_tokens.refresh_token_value,
                    &flat_tokens.refresh_token_nonce,
                ],
            )
            .await?
            .get(0);

        Ok(StoredOidcTokens {
            oidc_tokens,
            db_valid_until,
        })
    }

    async fn refresh_oidc_session_tokens(
        &self,
        session: SessionId,
        tx: &Transaction<'_>,
    ) -> Result<StoredOidcTokens> {
        let stmt = tx
            .prepare(
                "
            SELECT
                refresh_token,
                refresh_token_encryption_nonce
            FROM
                oidc_session_tokens
            WHERE session_id = $1 AND refresh_token IS NOT NULL;",
            )
            .await?;

        let rows = tx.query_opt(&stmt, &[&session]).await?;

        if let Some(refresh_string) = rows {
            let string_field_and_nonce = MaybeEncryptedBytes {
                value: refresh_string.get(0),
                nonce: refresh_string.get(1),
            };

            let refresh_token = self
                .oidc_manager()
                .maybe_decrypt_refresh_token(string_field_and_nonce)?;

            let oidc_manager = self.oidc_manager();

            let oidc_tokens = oidc_manager
                .get_client()
                .await?
                .refresh_access_token(refresh_token)
                .await?;

            let flat_tokens: FlatMaybeEncryptedOidcTokens = self
                .oidc_manager()
                .maybe_encrypt_tokens(&oidc_tokens)?
                .into();

            let update_session_tokens = tx.prepare("
                UPDATE
                    oidc_session_tokens
                SET
                    access_token = $2, access_token_encryption_nonce = $3, access_token_valid_until = CURRENT_TIMESTAMP + make_interval(secs:=$4), refresh_token = $5, refresh_token_encryption_nonce = $6
                WHERE
                    session_id = $1;",

            ).await?;

            tx.execute(
                &update_session_tokens,
                &[
                    &session,
                    &flat_tokens.access_token_value,
                    &flat_tokens.access_token_nonce,
                    &(oidc_tokens.expires_in.num_seconds() as f64),
                    &flat_tokens.refresh_token_value,
                    &flat_tokens.refresh_token_nonce,
                ],
            )
            .await?;

            let expiration = tx
                .query_one(
                    "
                    UPDATE
                        sessions
                    SET
                        valid_until = CURRENT_TIMESTAMP + make_interval(secs:=$2)
                    WHERE
                        id = $1
                    RETURNING
                        valid_until;
                    ",
                    &[&session, &(oidc_tokens.expires_in.num_seconds() as f64)],
                )
                .await?
                .get(0);

            return Ok(StoredOidcTokens {
                oidc_tokens,
                db_valid_until: expiration,
            });
        }

        Err(Error::InvalidSession)
    }

    async fn get_access_token(&self, session: SessionId) -> Result<AccessToken> {
        let mut conn = self.pool.get().await?;

        let tx = conn.build_transaction().start().await?;

        let stmt = tx
            .prepare(
                "
            SELECT
                access_token,
                access_token_encryption_nonce
            FROM
                oidc_session_tokens
            WHERE session_id = $1 AND (CURRENT_TIMESTAMP < access_token_valid_until);",
            )
            .await?;

        let rows = tx.query_opt(&stmt, &[&session]).await?;

        let access_token = if let Some(token_row) = rows {
            let string_field_and_nonce = MaybeEncryptedBytes {
                value: token_row.get(0),
                nonce: token_row.get(1),
            };
            self.oidc_manager()
                .maybe_decrypt_access_token(string_field_and_nonce)?
        } else {
            self.refresh_oidc_session_tokens(session, &tx)
                .await?
                .oidc_tokens
                .access
        };

        tx.commit().await?;

        Ok(access_token)
    }
}

#[async_trait]
impl<Tls> UserDb for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    // TODO: clean up expired sessions?

    async fn logout(&self) -> Result<()> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("DELETE FROM sessions WHERE id = $1;") // TODO: only invalidate session?
            .await?;

        conn.execute(&stmt, &[&self.session.id])
            .await
            .map_err(|_error| error::Error::LogoutFailed)?;
        Ok(())
    }

    async fn set_session_project(&self, project: ProjectId) -> Result<()> {
        // TODO: check permission

        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("UPDATE sessions SET project_id = $1 WHERE id = $2;")
            .await?;

        conn.execute(&stmt, &[&project, &self.session.id]).await?;

        Ok(())
    }

    async fn set_session_view(&self, view: STRectangle) -> Result<()> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("UPDATE sessions SET view = $1 WHERE id = $2;")
            .await?;

        conn.execute(&stmt, &[&view, &self.session.id]).await?;

        Ok(())
    }

    async fn increment_quota_used(&self, user: &UserId, quota_used: u64) -> Result<()> {
        ensure!(self.session.is_admin(), error::PermissionDenied);

        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "
            UPDATE users SET 
                quota_available = quota_available - $1, 
                quota_used = quota_used + $1
            WHERE id = $2;",
            )
            .await?;

        conn.execute(&stmt, &[&(quota_used as i64), &user]).await?;

        Ok(())
    }

    async fn bulk_increment_quota_used<I: IntoIterator<Item = (UserId, u64)> + Send>(
        &self,
        quota_used_updates: I,
    ) -> Result<()> {
        ensure!(self.session.is_admin(), error::PermissionDenied);

        let conn = self.conn_pool.get().await?;

        // collect the user ids and quotas into separate vectors to pass them as parameters to the query
        let (users, quotas): (Vec<UserId>, Vec<i64>) = quota_used_updates
            .into_iter()
            .map(|(user, quota)| (user, quota as i64))
            .unzip();

        let query = "
            UPDATE users
            SET quota_available = quota_available - quota_changes.quota, 
                quota_used = quota_used + quota_changes.quota
            FROM 
                (SELECT * FROM UNNEST($1::uuid[], $2::bigint[]) AS t(id, quota)) AS quota_changes
            WHERE users.id = quota_changes.id;
        ";

        conn.execute(query, &[&users, &quotas]).await?;

        Ok(())
    }

    async fn quota_used(&self) -> Result<u64> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("SELECT quota_used FROM users WHERE id = $1;")
            .await?;

        let row = conn
            .query_one(&stmt, &[&self.session.user.id])
            .await
            .map_err(|_error| error::Error::InvalidSession)?;

        Ok(row.get::<usize, i64>(0) as u64)
    }

    async fn quota_used_by_user(&self, user: &UserId) -> Result<u64> {
        ensure!(
            self.session.user.id == *user || self.session.is_admin(),
            error::PermissionDenied
        );

        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("SELECT quota_used FROM users WHERE id = $1;")
            .await?;

        let row = conn
            .query_one(&stmt, &[&user])
            .await
            .map_err(|_error| error::Error::InvalidSession)?;

        Ok(row.get::<usize, i64>(0) as u64)
    }

    async fn quota_available(&self) -> Result<i64> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("SELECT quota_available FROM users WHERE id = $1;")
            .await?;

        let row = conn
            .query_one(&stmt, &[&self.session.user.id])
            .await
            .map_err(|_error| error::Error::InvalidSession)?;

        Ok(row.get::<usize, i64>(0))
    }

    async fn quota_available_by_user(&self, user: &UserId) -> Result<i64> {
        ensure!(
            self.session.user.id == *user || self.session.is_admin(),
            error::PermissionDenied
        );

        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare("SELECT quota_available FROM users WHERE id = $1;")
            .await?;

        let row = conn
            .query_one(&stmt, &[&user])
            .await
            .map_err(|_error| error::Error::InvalidSession)?;

        Ok(row.get::<usize, i64>(0))
    }

    async fn log_quota_used<I: IntoIterator<Item = ComputationUnit> + Send>(
        &self,
        log: I,
    ) -> Result<()> {
        ensure!(self.session.is_admin(), error::PermissionDenied);

        let conn = self.conn_pool.get().await?;

        // collect the log into separate vectors to pass them as parameters to the query
        let mut users = Vec::new();
        let mut workflows = Vec::new();
        let mut computations = Vec::new();
        let mut operators_names = Vec::new();
        let mut operator_paths = Vec::new();
        let mut datas = Vec::new();

        for unit in log {
            users.push(unit.user);
            workflows.push(unit.workflow);
            computations.push(unit.computation);
            operators_names.push(unit.operator_name);
            operator_paths.push(unit.operator_path.to_string());
            datas.push(unit.data);
        }

        let query = "
            INSERT INTO quota_log (user_id, workflow_id, computation_id, operator_name, operator_path, data)
                (SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::uuid[], $4::text[], $5::text[], $6::text[]))
        ";

        conn.execute(
            query,
            &[
                &users,
                &workflows,
                &computations,
                &operators_names,
                &operator_paths,
                &datas,
            ],
        )
        .await?;

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn quota_used_by_computations(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ComputationQuota>> {
        let limit = limit.min(10); // TODO: use list limit from config

        let conn = self.conn_pool.get().await?;

        let rows = conn
            .query(
                "
            SELECT
                computation_id,
                workflow_id,
                MIN(timestamp) AS timestamp,
                COUNT(*) AS count
            FROM
                quota_log
            WHERE
                user_id = $1
            GROUP BY
                computation_id,
                workflow_id
            ORDER BY 
                MIN(TIMESTAMP) DESC
            OFFSET $2
            LIMIT $3;",
                &[&self.session.user.id, &(offset as i64), &(limit as i64)],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|row| ComputationQuota {
                computation_id: row.get(0),
                workflow_id: row.get(1),
                timestamp: row.get(2),
                count: row.get::<_, i64>(3) as u64,
            })
            .collect())
    }

    async fn quota_used_by_computation(&self, computation_id: Uuid) -> Result<Vec<OperatorQuota>> {
        let conn = self.conn_pool.get().await?;

        let rows = conn
            .query(
                "
            SELECT
                operator_name,
                operator_path,
                COUNT(*) AS count
            FROM
                quota_log
            WHERE
                user_id = $1 AND
                computation_id = $2
            GROUP BY
                operator_name, operator_path
            ORDER BY 
            operator_name DESC;",
                &[&self.session.user.id, &computation_id],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|row| OperatorQuota {
                operator_name: row.get(0),
                operator_path: row.get(1),
                count: row.get::<_, i64>(2) as u64,
            })
            .collect())
    }

    async fn quota_used_on_data(&self, offset: u64, limit: u64) -> Result<Vec<DataUsage>> {
        ensure!(self.session.is_admin(), error::PermissionDenied);

        let conn = self.conn_pool.get().await?;

        let rows = conn
            .query(
                "
            SELECT
                user_id,
                computation_id,
                data,
                MIN(timestamp) AS timestamp,
                COUNT(*) AS count
            FROM
                quota_log
            WHERE 
                data IS NOT NULL
            GROUP BY
               user_id,
               computation_id,
               workflow_id,
               data
            ORDER BY
                MIN(timestamp) DESC, user_id ASC, computation_id ASC, workflow_id ASC, data ASC
            OFFSET
                $1
            LIMIT
                $2;",
                &[&(offset as i64), &(limit as i64)],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|row| DataUsage {
                user_id: row.get(0),
                computation_id: row.get(1),
                data: row.get(2),
                timestamp: row.get(3),
                count: row.get::<_, i64>(4) as u64,
            })
            .collect())
    }

    async fn quota_used_on_data_summary(
        &self,
        dataset: Option<String>,
        granularity: UsageSummaryGranularity,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<DataUsageSummary>> {
        ensure!(self.session.is_admin(), error::PermissionDenied);

        // TODO: check if user is Owner of dataset

        let conn = self.conn_pool.get().await?;

        let trunc = match granularity {
            UsageSummaryGranularity::Minutes => "minute",
            UsageSummaryGranularity::Hours => "hour",
            UsageSummaryGranularity::Days => "day",
            UsageSummaryGranularity::Months => "month",
            UsageSummaryGranularity::Years => "year",
        };

        let rows = if let Some(dataset) = dataset {
            conn.query(
                &format!(
                    "
            SELECT
                date_trunc('{trunc}', timestamp) AS trunc,
                data
                COUNT(*) AS count
            FROM
                quota_log
            WHERE 
                data = $3
            GROUP BY
                trunc, data
            ORDER BY
                trunc DESC, data ASC
            OFFSET
                $1
            LIMIT 
                $2;",
                ),
                &[&(offset as i64), &(limit as i64), &dataset],
            )
            .await?
        } else {
            conn.query(
                &format!(
                    "
            SELECT
                date_trunc('{trunc}', timestamp) AS trunc,
                data,
                COUNT(*) AS count
            FROM
                quota_log
            WHERE 
                data IS NOT NULL
            GROUP BY
                trunc, data
            ORDER BY
                trunc DESC, data ASC
            OFFSET
                $1
            LIMIT 
                $2;",
                ),
                &[&(offset as i64), &(limit as i64)],
            )
            .await?
        };

        Ok(rows
            .iter()
            .map(|row| DataUsageSummary {
                timestamp: row.get(0),
                data: row.get(1),
                count: row.get::<_, i64>(2) as u64,
            })
            .collect())
    }

    async fn update_quota_available_by_user(
        &self,
        user: &UserId,
        new_available_quota: i64,
    ) -> Result<()> {
        ensure!(self.session.is_admin(), error::PermissionDenied);

        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "
            UPDATE users SET 
                quota_available = $1
            WHERE id = $2;",
            )
            .await?;

        conn.execute(&stmt, &[&(new_available_quota), &user])
            .await?;

        Ok(())
    }
}

#[async_trait]
impl<Tls> RoleDb for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn add_role(&self, role_name: &str) -> Result<RoleId, RoleDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8RoleDbError)?;

        let tx = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresRoleDbError)?;

        self.ensure_admin_in_tx(&tx)
            .await
            .context(PermissionDbRoleDbError)?;

        let id = RoleId::new();

        let res = tx
            .execute(
                "INSERT INTO roles (id, name) VALUES ($1, $2);",
                &[&id, &role_name],
            )
            .await;

        if let Err(err) = res {
            if err.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) {
                return Err(RoleDbError::RoleAlreadyExists {
                    role_name: role_name.to_string(),
                });
            }
        }

        tx.commit().await.context(PostgresRoleDbError)?;

        Ok(id)
    }

    async fn load_role_by_name(&self, role_name: &str) -> Result<RoleId, RoleDbError> {
        let conn = self.conn_pool.get().await.context(Bb8RoleDbError)?;

        let row = conn
            .query_opt("SELECT id FROM roles WHERE name = $1;", &[&role_name])
            .await
            .context(PostgresRoleDbError)?
            .ok_or(RoleDbError::RoleNameDoesNotExist {
                role_name: role_name.to_string(),
            })?;

        Ok(RoleId(row.get(0)))
    }

    async fn remove_role(&self, role_id: &RoleId) -> Result<(), RoleDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8RoleDbError)?;

        let tx = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresRoleDbError)?;

        self.ensure_admin_in_tx(&tx)
            .await
            .context(PermissionDbRoleDbError)?;

        let deleted = tx
            .execute("DELETE FROM roles WHERE id = $1;", &[&role_id])
            .await
            .context(PostgresRoleDbError)?;

        tx.commit().await.context(PostgresRoleDbError)?;

        ensure!(
            deleted > 0,
            RoleIdDoesNotExistRoleDbError { role_id: *role_id }
        );

        Ok(())
    }

    async fn assign_role(&self, role_id: &RoleId, user_id: &UserId) -> Result<(), RoleDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8RoleDbError)?;

        let tx = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresRoleDbError)?;

        self.ensure_admin_in_tx(&tx)
            .await
            .context(PermissionDbRoleDbError)?;

        tx.execute(
            "INSERT INTO user_roles (user_id, role_id) VALUES ($1, $2) ON CONFLICT DO NOTHING;",
            &[&user_id, &role_id],
        )
        .await
        .context(PostgresRoleDbError)?;

        tx.commit().await.context(PostgresRoleDbError)?;

        Ok(())
    }

    async fn revoke_role(&self, role_id: &RoleId, user_id: &UserId) -> Result<(), RoleDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8RoleDbError)?;

        let tx = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresRoleDbError)?;

        self.ensure_admin_in_tx(&tx)
            .await
            .context(PermissionDbRoleDbError)?;

        let deleted = tx
            .execute(
                "DELETE FROM user_roles WHERE user_id= $1 AND role_id = $2;",
                &[&user_id, &role_id],
            )
            .await
            .context(PostgresRoleDbError)?;

        tx.commit().await.context(PostgresRoleDbError)?;

        ensure!(
            deleted > 0,
            CannotRevokeRoleThatIsNotAssignedRoleDbError { role_id: *role_id }
        );

        Ok(())
    }

    async fn get_role_descriptions(
        &self,
        user_id: &UserId,
    ) -> Result<Vec<RoleDescription>, RoleDbError> {
        let conn = self.conn_pool.get().await.context(Bb8RoleDbError)?;

        let stmt = conn
            .prepare(
                "SELECT roles.id, roles.name \
                FROM roles JOIN user_roles ON (roles.id=user_roles.role_id) \
                WHERE user_roles.user_id=$1 \
                ORDER BY roles.name;",
            )
            .await
            .context(PostgresRoleDbError)?;

        let results = conn
            .query(&stmt, &[&user_id])
            .await
            .context(PostgresRoleDbError)?;

        let mut result_vec = Vec::new();

        for result in results {
            let id = result.get(0);
            let name = result.get(1);
            let individual = UserId(id) == *user_id;
            result_vec.push(RoleDescription {
                role: Role {
                    id: RoleId(id),
                    name,
                },
                individual,
            });
        }

        Ok(result_vec)
    }
}
