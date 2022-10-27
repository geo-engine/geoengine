use futures::{
    channel::mpsc::{channel, Receiver, Sender},
    StreamExt,
};
use std::sync::Arc;

use geoengine_datatypes::util::test::TestDefault;
use geoengine_operators::pro::{ComputationId, QuotaTracking};

use crate::pro::users::UserId;

use super::users::{UserDb, UserSession};

// TODO: find a good default, choose w.r.t. resources or make configurable
const QUOTA_CHANNEL_SIZE: usize = 1000;

#[derive(Debug, Clone)]
pub struct QuotaTrackingFactory {
    // user_db: Arc<dyn UserDb>,
    // quota_sender: SyncSender<ComputationId>,
    quota_sender: Sender<ComputationId>,
}

impl QuotaTrackingFactory {
    // pub fn new(quota_sender: SyncSender<ComputationId>) -> Self {
    pub fn new(quota_sender: Sender<ComputationId>) -> Self {
        Self { quota_sender }
    }

    pub fn create_quota_tracking(&self, session: &UserSession) -> QuotaTracking {
        QuotaTracking::new(self.quota_sender.clone(), session.user.id.0)
    }
}

impl TestDefault for QuotaTrackingFactory {
    fn test_default() -> Self {
        // let (quota_sender, _quota_receiver) = sync_channel(QUOTA_CHANNEL_SIZE);
        let (quota_sender, _quota_receiver) = channel(QUOTA_CHANNEL_SIZE);
        Self::new(quota_sender)
    }
}

pub struct QuotaManager<U: UserDb + 'static> {
    user_db: Arc<U>,
    quota_receiver: Receiver<ComputationId>,
}

impl<U: UserDb + 'static> QuotaManager<U> {
    pub fn new(user_db: Arc<U>, quota_receiver: Receiver<ComputationId>) -> Self {
        Self {
            user_db,
            quota_receiver,
        }
    }

    pub fn run(mut self) {
        crate::util::spawn(async move {
            while let Some(computation) = self.quota_receiver.next().await {
                log::info!("Quota received: {}", computation);

                let user = UserId(computation);
                // TODO: what to do if this fails (quota can't be recorded)? Try again later?
                // TODO: do we HAVE to wait for this to finish?
                let r = self.user_db.increment_quota_used(&user, 1).await;

                if r.is_err() {
                    log::error!("Could not increment quota for user {}", user);
                }
            }
        });
    }
}

pub fn initialize_quota_tracking<U: UserDb + 'static>(user_db: Arc<U>) -> QuotaTrackingFactory {
    let (quota_sender, quota_receiver) = channel::<ComputationId>(QUOTA_CHANNEL_SIZE);

    QuotaManager::new(user_db, quota_receiver).run();

    QuotaTrackingFactory::new(quota_sender)
}

#[cfg(test)]
mod tests {
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

        let tracking = quota.create_quota_tracking(&session);

        tracking.work_unit_done().await;
        tracking.work_unit_done().await;

        // wait for quota to be recorded
        let mut success = false;
        for _ in 0..10 {
            let used = ctx.user_db_ref().quota_used(&session).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            if used == 2 {
                success = true;
                break;
            }
        }

        assert!(success);
    }
}
