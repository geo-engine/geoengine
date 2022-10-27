use futures::{channel::mpsc::Sender, Future, SinkExt};
use geoengine_datatypes::identifier;
use uuid::Uuid;

identifier!(ComputationContext);

/// An Id for a computation used for quota tracking
#[derive(Debug, Clone, Copy)]
pub struct ComputationUnit {
    pub issuer: Uuid,                // TODO: use UserId?
    pub context: ComputationContext, // TODO: introduce the concept of workflows to the operators crate and use/add it here
}

#[derive(Clone)]
pub struct QuotaTracking {
    quota_sender: Sender<ComputationUnit>,
    computation: ComputationUnit,
}

impl QuotaTracking {
    pub fn new(quota_sender: Sender<ComputationUnit>, computation: ComputationUnit) -> Self {
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
