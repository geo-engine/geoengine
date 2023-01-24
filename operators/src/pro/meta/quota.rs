use crate::util::Result;
use async_trait::async_trait;
use geoengine_datatypes::identifier;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

identifier!(ComputationContext);

/// An Id for a computation used for quota tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComputationUnit {
    pub issuer: Uuid,                // TODO: use UserId?
    pub context: ComputationContext, // TODO: introduce the concept of workflows to the operators crate and use/add it here
}

/// This type holds a [`Sender`] to a channel that is used to track the computation units.
/// It is passed to the [`StreamStatisticsAdapter`] via the [`QueryContext`].
#[derive(Clone)]
pub struct QuotaTracking {
    quota_sender: UnboundedSender<ComputationUnit>,
    computation: ComputationUnit,
}

impl QuotaTracking {
    pub fn new(
        quota_sender: UnboundedSender<ComputationUnit>,
        computation: ComputationUnit,
    ) -> Self {
        Self {
            quota_sender,
            computation,
        }
    }

    pub fn work_unit_done(&self) {
        let _ = self.quota_sender.send(self.computation); // ignore the Result because the quota receiver should never close the receiving end of the channel
    }
}

#[async_trait]
pub trait QuotaCheck {
    async fn check_quota(&self) -> Result<bool>;
}

pub type QuotaChecker = Box<dyn QuotaCheck + Send + Sync>;
