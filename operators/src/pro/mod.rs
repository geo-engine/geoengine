// This is an inclusion point of Geo Engine Pro

use futures::{channel::mpsc::Sender, Future, SinkExt};
use uuid::Uuid;

pub mod adapters;
pub mod meta;

/// An Id for a computation used for quota tracking
pub type ComputationId = Uuid;

#[derive(Clone)]
pub struct QuotaTracking {
    quota_sender: Sender<ComputationId>,
    computation: ComputationId,
}

impl QuotaTracking {
    pub fn new(quota_sender: Sender<ComputationId>, computation: ComputationId) -> Self {
        Self {
            quota_sender,
            computation,
        }
    }

    pub fn work_unit_done(&self) -> impl Future<Output = ()> {
        let mut sender = self.quota_sender.clone();
        let computation = self.computation;
        async move {
            sender
                .send(computation)
                .await
                .expect("the quota receiver should never close the receiving end of the channel");
        }
    }
}
