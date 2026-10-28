use crate::error::Result;
use crate::identifier;
use geoengine_operators::engine::TypedOperator as OperatorsTypedOperator;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

identifier!(ProcessingGraphId);

impl ProcessingGraphId {
    #[allow(clippy::missing_panics_doc)]
    pub fn from_hash(workflow: &ProcessingGraph) -> Self {
        Self(Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            serde_json::to_string(workflow)
                .expect("It is always possible to create a workflow id from a workflow.")
                .as_bytes(),
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProcessingGraph {
    #[serde(flatten)]
    pub operator: OperatorsTypedOperator,
}

impl PartialEq for ProcessingGraph {
    fn eq(&self, other: &Self) -> bool {
        match (serde_json::to_string(self), serde_json::to_string(other)) {
            (Ok(a), Ok(b)) => a == b,
            _ => false,
        }
    }
}
