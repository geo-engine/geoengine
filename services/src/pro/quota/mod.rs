use super::{
    users::{UserDb, UserSession},
    util::config::QuotaTrackingMode,
};
use crate::pro::users::UserId;
use geoengine_datatypes::util::test::TestDefault;
use geoengine_operators::meta::quota::{
    ComputationContext, ComputationUnit, QuotaMessage, QuotaTracking,
};
use snafu::Snafu;
use std::{collections::HashMap, time::Duration};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

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
        context: ComputationContext,
    ) -> QuotaTracking {
        QuotaTracking::new(
            self.quota_sender.clone(),
            ComputationUnit {
                issuer: session.user.id.0,
                context,
            },
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
    increment_quota_buffer: HashMap<UserId, u64>,
    increment_quota_buffer_size: usize,
    increment_quota_buffer_used: usize,
    increment_quota_buffer_timeout_seconds: u64,
}

impl<U: UserDb + 'static> QuotaManager<U> {
    pub fn new(
        mode: QuotaTrackingMode,
        user_db: U,
        quota_receiver: UnboundedReceiver<QuotaMessage>,
        quota_sender: UnboundedSender<QuotaMessage>,
        increment_quota_buffer_size: usize,
        increment_quota_buffer_timeout_seconds: u64,
    ) -> Self {
        Self {
            mode,
            user_db,
            quota_receiver,
            quota_sender,
            increment_quota_buffer: HashMap::new(),
            increment_quota_buffer_size,
            increment_quota_buffer_timeout_seconds,
            increment_quota_buffer_used: 0,
        }
    }

    pub fn run(mut self) {
        if self.mode == QuotaTrackingMode::Disabled {
            // if the quota tracking is disabled, we still consume the messages in order to empty the channel, but ignore the messages
            crate::util::spawn(async move {
                while let Some(_message) = self.quota_receiver.recv().await {
                    continue;
                }
            });

            return;
        }

        crate::util::spawn(async move {
            while let Some(message) = self.quota_receiver.recv().await {
                let flush_buffer = match message {
                    QuotaMessage::ComputationUnit(computation) => {
                        // TODO: issue a tracing event instead?
                        // TODO: also log workflow, or connect the computation context to a workflow beforehand.
                        //       However, currently it is possible to reuse a context for multiple queries.
                        //       Also: the operators crate knows nothing about workflows as of yet.
                        log::trace!(
                            "Quota received. User: {}, Context: {}",
                            computation.issuer,
                            computation.context
                        );

                        let user = UserId(computation.issuer);

                        *self.increment_quota_buffer.entry(user).or_default() += 1;
                        self.increment_quota_buffer_used += 1;

                        self.increment_quota_buffer_used >= self.increment_quota_buffer_size
                    }
                    QuotaMessage::Flush => {
                        log::trace!("Flush `increment_quota_buffer'");
                        true
                    }
                };

                if flush_buffer {
                    // TODO: what to do if this fails (quota can't be recorded)? Try again later?

                    if let Err(error) = self
                        .user_db
                        .bulk_increment_quota_used(self.increment_quota_buffer.drain())
                        .await
                    {
                        log::error!("Could not increment quota for users {error:?}");
                    }

                    self.increment_quota_buffer_used = 0;
                }
            }
        });

        // Periodically flush the increment_quota_buffer
        crate::util::spawn(async move {
            let timeout_duration = Duration::from_secs(self.increment_quota_buffer_timeout_seconds);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        contexts::{ApplicationContext, SessionContext},
        pro::{
            contexts::ProPostgresContext,
            ge_context,
            users::{UserAuth, UserCredentials, UserRegistration},
            util::tests::admin_login,
        },
    };
    use geoengine_datatypes::util::Identifier;
    use tokio_postgres::NoTls;

    #[ge_context::test]
    async fn it_tracks_quota(app_ctx: ProPostgresContext<NoTls>) {
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

        let tracking = quota.create_quota_tracking(&session, ComputationContext::new());

        tracking.work_unit_done();
        tracking.work_unit_done();

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
    async fn it_tracks_quota_buffered(app_ctx: ProPostgresContext<NoTls>) {
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

        let tracking = quota.create_quota_tracking(&session, ComputationContext::new());

        tracking.work_unit_done();
        tracking.work_unit_done();
        tracking.work_unit_done();
        tracking.work_unit_done();
        tracking.work_unit_done();

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
    async fn it_tracks_quota_buffered_timeout(app_ctx: ProPostgresContext<NoTls>) {
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

        let tracking = quota.create_quota_tracking(&session, ComputationContext::new());

        tracking.work_unit_done();
        tracking.work_unit_done();

        let db = app_ctx.session_context(session).db();

        tokio::time::sleep(std::time::Duration::from_secs(4)).await;

        let used = db.quota_used().await.unwrap();
        let available = db.quota_available().await.unwrap();

        assert_eq!(used, 2);
        assert_eq!(available, 9997);
    }
}
