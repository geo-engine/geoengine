use crate::{engine::WorkflowOperatorPath, util::Result};
use async_trait::async_trait;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

/// An Id for a computation used for quota tracking
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputationUnit {
    pub user: Uuid,
    pub workflow: Uuid,
    pub computation: Uuid,
    pub operator: WorkflowOperatorPath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuotaMessage {
    ComputationUnit(ComputationUnit),
    Flush,
}

impl From<ComputationUnit> for QuotaMessage {
    fn from(value: ComputationUnit) -> Self {
        Self::ComputationUnit(value)
    }
}

/// This type holds a [`Sender`] to a channel that is used to track the computation units.
/// It is passed to the [`StreamStatisticsAdapter`] via the [`QueryContext`].
#[derive(Clone)]
pub struct QuotaTracking {
    quota_sender: UnboundedSender<QuotaMessage>,
    user: Uuid,
    workflow: Uuid,
    computation: Uuid,
}

impl QuotaTracking {
    pub fn new(
        quota_sender: UnboundedSender<QuotaMessage>,
        user: Uuid,
        workflow: Uuid,
        computation: Uuid,
    ) -> Self {
        Self {
            quota_sender,
            user,
            workflow,
            computation,
        }
    }

    pub fn work_unit_done(&self, operator: WorkflowOperatorPath) {
        let _ = self
            .quota_sender
            .send(QuotaMessage::ComputationUnit(ComputationUnit {
                user: self.user,
                workflow: self.workflow,
                computation: self.computation,
                operator,
            })); // ignore the Result because the quota receiver should never close the receiving end of the channel
    }
}

#[async_trait]
pub trait QuotaCheck {
    /// checks if the quota is available and if not, returns an error
    async fn ensure_quota_available(&self) -> Result<()>;
}

pub type QuotaChecker = Box<dyn QuotaCheck + Send + Sync>;
