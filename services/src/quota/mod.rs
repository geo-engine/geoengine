use crate::config::QuotaTrackingMode;
use crate::users::{UserDb, UserId, UserSession};
use geoengine_datatypes::{primitives::DateTime, util::test::TestDefault};
use geoengine_operators::ge_tracing_removed_trace;
use geoengine_operators::meta::quota::{ComputationUnit, QuotaMessage, QuotaTracking};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::{collections::HashMap, time::Duration};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))]
pub enum QuotaError {
    QuotaExhausted,
}

#[derive(Debug, Clone)]
pub struct QuotaTrackingFactory {
    quota_sender: UnboundedSender<QuotaMessage>,
}

impl QuotaTrackingFactory {
    pub fn new(quota_sender: UnboundedSender<QuotaMessage>) -> Self {
        Self { quota_sender }
    }

    pub fn create_quota_tracking(
        &self,
        session: &UserSession,
        workflow: Uuid,
        computation: Uuid,
    ) -> QuotaTracking {
        QuotaTracking::new(
            self.quota_sender.clone(),
            session.user.id.0,
            workflow,
            computation,
        )
    }
}

impl TestDefault for QuotaTrackingFactory {
    fn test_default() -> Self {
        let (quota_sender, _quota_receiver) = unbounded_channel();
        Self::new(quota_sender)
    }
}

pub struct QuotaManager<U: UserDb + 'static> {
    mode: QuotaTrackingMode,
    user_db: U,
    quota_receiver: UnboundedReceiver<QuotaMessage>,
    quota_sender: UnboundedSender<QuotaMessage>,
    quota_log_buffer: Vec<ComputationUnit>,
    increment_quota_buffer: HashMap<UserId, u64>,
    buffer_size: usize,
    buffer_used: usize,
    buffer_timeout_seconds: u64,
}

impl<U: UserDb + 'static> QuotaManager<U> {
    pub fn new(
        mode: QuotaTrackingMode,
        user_db: U,
        quota_receiver: UnboundedReceiver<QuotaMessage>,
        quota_sender: UnboundedSender<QuotaMessage>,
        buffer_size: usize,
        buffer_timeout_seconds: u64,
    ) -> Self {
        Self {
            mode,
            user_db,
            quota_receiver,
            quota_sender,
            quota_log_buffer: Vec::new(),
            increment_quota_buffer: HashMap::new(),
            buffer_size,
            buffer_used: 0,
            buffer_timeout_seconds,
        }
    }

    pub fn run(mut self) {
        if self.mode == QuotaTrackingMode::Disabled {
            // if the quota tracking is disabled, we still consume the messages in order to empty the channel, but ignore the messages
            crate::util::spawn(async move {
                while let Some(_message) = self.quota_receiver.recv().await {}
            });

            return;
        }

        crate::util::spawn(async move {
            while let Some(message) = self.quota_receiver.recv().await {
                let flush_buffer = match message {
                    QuotaMessage::ComputationUnit(computation) => {
                        // TODO: issue a tracing event instead?
                        ge_tracing_removed_trace!(
                            "Quota received. User: {}, Workflow: {}, Computation: {}",
                            computation.user,
                            computation.workflow,
                            computation.computation
                        );

                        let user = UserId(computation.user);

                        *self.increment_quota_buffer.entry(user).or_default() += 1;

                        self.quota_log_buffer.push(computation);

                        self.buffer_used += 1;

                        self.buffer_used >= self.buffer_size
                    }
                    QuotaMessage::Flush => {
                        ge_tracing_removed_trace!("Flush `increment_quota_buffer'");
                        true
                    }
                };

                if flush_buffer && self.buffer_used > 0 {
                    // TODO: what to do if this fails (quota can't be recorded)? Try again later?

                    if let Err(error) = self
                        .user_db
                        .bulk_increment_quota_used(self.increment_quota_buffer.drain())
                        .await
                    {
                        log::error!("Could not increment quota for users {error:?}");
                    }

                    if let Err(error) = self
                        .user_db
                        .log_quota_used(self.quota_log_buffer.drain(..))
                        .await
                    {
                        log::error!("Could not log quota used {error:?}");
                    }

                    self.buffer_used = 0;
                }
            }
        });

        // Periodically flush the increment_quota_buffer
        crate::util::spawn(async move {
            let timeout_duration = Duration::from_secs(self.buffer_timeout_seconds);

            loop {
                tokio::time::sleep(timeout_duration).await;
                let _ = self.quota_sender.send(QuotaMessage::Flush);
            }
        });
    }
}

pub fn initialize_quota_tracking<U: UserDb + 'static>(
    mode: QuotaTrackingMode,
    user_db: U,
    increment_quota_buffer_size: usize,
    increment_quota_buffer_timeout_seconds: u64,
) -> QuotaTrackingFactory {
    let (quota_sender, quota_receiver) = unbounded_channel::<QuotaMessage>();

    QuotaManager::new(
        mode,
        user_db,
        quota_receiver,
        quota_sender.clone(),
        increment_quota_buffer_size,
        increment_quota_buffer_timeout_seconds,
    )
    .run();

    QuotaTrackingFactory::new(quota_sender)
}

#[derive(Debug, Serialize, Deserialize, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OperatorQuota {
    pub operator_name: String,
    pub operator_path: String,
    pub count: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ComputationQuota {
    pub timestamp: DateTime,
    pub computation_id: Uuid,
    pub workflow_id: Uuid,
    pub count: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DataUsage {
    pub timestamp: DateTime,
    pub user_id: Uuid,
    pub computation_id: Uuid,
    pub data: String,
    pub count: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DataUsageSummary {
    pub timestamp: DateTime,
    pub data: String,
    pub count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        contexts::PostgresContext,
        contexts::{ApplicationContext, SessionContext},
        ge_context,
        users::{UserAuth, UserCredentials, UserRegistration},
        util::tests::{MockQuotaTracking, admin_login},
    };
    use tokio_postgres::NoTls;

    #[ge_context::test]
    async fn it_tracks_quota(app_ctx: PostgresContext<NoTls>) {
        let _user = app_ctx
            .register_user(UserRegistration {
                email: "foo@example.com".to_string(),
                password: "secret1234".to_string(),
                real_name: "Foo Bar".to_string(),
            })
            .await
            .unwrap();

        let session = app_ctx
            .login(UserCredentials {
                email: "foo@example.com".to_string(),
                password: "secret1234".to_string(),
            })
            .await
            .unwrap();

        let admin_session = admin_login(&app_ctx).await;

        let quota = initialize_quota_tracking(
            QuotaTrackingMode::Check,
            app_ctx.session_context(admin_session.clone()).db(),
            0,
            60,
        );

        let tracking = quota.create_quota_tracking(&session, Uuid::new_v4(), Uuid::new_v4());

        tracking.mock_work_unit_done();
        tracking.mock_work_unit_done();

        let db = app_ctx.session_context(session).db();

        // wait for quota to be recorded
        let mut success = false;
        for _ in 0..10 {
            let used = db.quota_used().await.unwrap();
            let available = db.quota_available().await.unwrap();

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            if used == 2 && available == 9997 {
                success = true;
                break;
            }
        }

        assert!(success);
    }

    #[ge_context::test]
    async fn it_tracks_quota_buffered(app_ctx: PostgresContext<NoTls>) {
        let _user = app_ctx
            .register_user(UserRegistration {
                email: "foo@example.com".to_string(),
                password: "secret1234".to_string(),
                real_name: "Foo Bar".to_string(),
            })
            .await
            .unwrap();

        let session = app_ctx
            .login(UserCredentials {
                email: "foo@example.com".to_string(),
                password: "secret1234".to_string(),
            })
            .await
            .unwrap();

        let admin_session = admin_login(&app_ctx).await;

        let quota = initialize_quota_tracking(
            QuotaTrackingMode::Check,
            app_ctx.session_context(admin_session.clone()).db(),
            5,
            60,
        );

        let tracking = quota.create_quota_tracking(&session, Uuid::new_v4(), Uuid::new_v4());

        tracking.mock_work_unit_done();
        tracking.mock_work_unit_done();
        tracking.mock_work_unit_done();
        tracking.mock_work_unit_done();
        tracking.mock_work_unit_done();

        let db = app_ctx.session_context(session).db();

        // wait for quota to be recorded
        let mut success = false;
        for _ in 0..10 {
            let used = db.quota_used().await.unwrap();
            let available = db.quota_available().await.unwrap();

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            if used == 5 && available == 9994 {
                success = true;
                break;
            }
        }

        assert!(success);
    }

    #[ge_context::test]
    async fn it_tracks_quota_buffered_timeout(app_ctx: PostgresContext<NoTls>) {
        let _user = app_ctx
            .register_user(UserRegistration {
                email: "foo@example.com".to_string(),
                password: "secret1234".to_string(),
                real_name: "Foo Bar".to_string(),
            })
            .await
            .unwrap();

        let session = app_ctx
            .login(UserCredentials {
                email: "foo@example.com".to_string(),
                password: "secret1234".to_string(),
            })
            .await
            .unwrap();

        let admin_session = admin_login(&app_ctx).await;

        let quota = initialize_quota_tracking(
            QuotaTrackingMode::Check,
            app_ctx.session_context(admin_session.clone()).db(),
            5,
            2,
        );

        let tracking = quota.create_quota_tracking(&session, Uuid::new_v4(), Uuid::new_v4());

        tracking.mock_work_unit_done();
        tracking.mock_work_unit_done();

        let db = app_ctx.session_context(session).db();

        tokio::time::sleep(std::time::Duration::from_secs(4)).await;

        let used = db.quota_used().await.unwrap();
        let available = db.quota_available().await.unwrap();

        assert_eq!(used, 2);
        assert_eq!(available, 9997);
    }
}
