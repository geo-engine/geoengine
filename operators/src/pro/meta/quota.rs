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
    pub batch: Option<u64>,
}

impl ComputationUnit {
    pub fn new(issuer: Uuid, context: ComputationContext) -> Self {
        Self {
            issuer,
            context,
            batch: None,
        }
    }

    pub fn new_batch(issuer: Uuid, context: ComputationContext, batch: u64) -> Self {
        Self {
            issuer,
            context,
            batch: Some(batch),
        }
    }
}

/// This type holds a [`Sender`] to a channel that is used to track the computation units.
/// It is passed to the [`StreamStatisticsAdapter`] via the [`QueryContext`].
#[derive(Clone)]
pub struct QuotaTracking {
    quota_sender: UnboundedSender<ComputationUnit>,
    computation: ComputationUnit,
    batch_size: u64,
    batch_count: u64,
}

impl QuotaTracking {
    pub fn new(
        quota_sender: UnboundedSender<ComputationUnit>,
        computation: ComputationUnit,
    ) -> Self {
        Self {
            quota_sender,
            computation,
            batch_size: 100,
            batch_count: 0,
        }
    }

    pub fn work_unit_done(&mut self) {
        self.batch_count += 1;
        if self.batch_count >= self.batch_size {
            let batch_cu = ComputationUnit::new_batch(
                self.computation.issuer,
                self.computation.context,
                self.batch_count,
            );
            self.batch_count = 0;
            let _ = self.quota_sender.send(batch_cu); // ignore the Result because the quota receiver should never close the receiving end of the channel
        }
    }

    fn computation_done(&mut self) {
        if self.batch_count > 0 {
            let batch_cu = ComputationUnit::new_batch(
                self.computation.issuer,
                self.computation.context,
                self.batch_count,
            );
            self.batch_count = 0;
            let _ = self.quota_sender.send(batch_cu); // ignore the Result because the quota receiver should never close the receiving end of the channel
        }
    }
}

impl Drop for QuotaTracking {
    fn drop(&mut self) {
        self.computation_done();
    }
}

#[async_trait]
pub trait QuotaCheck {
    /// checks if the quota is available and if not, returns an error
    async fn ensure_quota_available(&self) -> Result<()>;
}

pub type QuotaChecker = Box<dyn QuotaCheck + Send + Sync>;
