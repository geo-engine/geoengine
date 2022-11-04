use geoengine_operators::pro::meta::quota::{ComputationContext, ComputationUnit, QuotaTracking};
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use geoengine_datatypes::util::test::TestDefault;

use crate::pro::users::UserId;

use super::users::{UserDb, UserSession};

#[derive(Debug, Clone)]
pub struct QuotaTrackingFactory {
    quota_sender: UnboundedSender<ComputationUnit>,
}

impl QuotaTrackingFactory {
    pub fn new(quota_sender: UnboundedSender<ComputationUnit>) -> Self {
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
    user_db: Arc<U>,
    quota_receiver: UnboundedReceiver<ComputationUnit>,
}

impl<U: UserDb + 'static> QuotaManager<U> {
    pub fn new(user_db: Arc<U>, quota_receiver: UnboundedReceiver<ComputationUnit>) -> Self {
        Self {
            user_db,
            quota_receiver,
        }
    }

    pub fn run(mut self) {
        crate::util::spawn(async move {
            while let Some(computation) = self.quota_receiver.recv().await {
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
                // TODO: what to do if this fails (quota can't be recorded)? Try again later?
                let r = self.user_db.increment_quota_used(&user, 1).await;

                if r.is_err() {
                    log::error!("Could not increment quota for user {}", user);
                }
            }
        });
    }
}

pub fn initialize_quota_tracking<U: UserDb + 'static>(user_db: Arc<U>) -> QuotaTrackingFactory {
    let (quota_sender, quota_receiver) = unbounded_channel::<ComputationUnit>();

    QuotaManager::new(user_db, quota_receiver).run();

    QuotaTrackingFactory::new(quota_sender)
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::util::Identifier;

    use crate::{
        pro::{
            contexts::{ProContext, ProInMemoryContext},
            users::{UserCredentials, UserRegistration},
        },
        util::user_input::UserInput,
    };

    use super::*;

    #[tokio::test]
    async fn it_tracks_quota() {
        let ctx = ProInMemoryContext::test_default();

        let _user = ctx
            .user_db_ref()
            .register(
                UserRegistration {
                    email: "foo@example.com".to_string(),
                    password: "secret1234".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let session = ctx
            .user_db_ref()
            .login(UserCredentials {
                email: "foo@example.com".to_string(),
                password: "secret1234".to_string(),
            })
            .await
            .unwrap();

        let quota = initialize_quota_tracking(ctx.user_db());

        let tracking = quota.create_quota_tracking(&session, ComputationContext::new());

        tracking.work_unit_done();
        tracking.work_unit_done();

        // wait for quota to be recorded
        let mut success = false;
        for _ in 0..10 {
            let used = ctx
                .user_db_ref()
                .quota_used_by_session(&session)
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            if used == 2 {
                success = true;
                break;
            }
        }

        assert!(success);
    }
}
