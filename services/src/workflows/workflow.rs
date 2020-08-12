use serde::{Deserialize, Serialize};
use uuid::Uuid;

use geoengine_operators::engine::TypedOperator;

identifier!(WorkflowId);

impl WorkflowId {
    pub fn from_hash(workflow: &Workflow) -> Self {
        Self {
            id: Uuid::new_v5(
                &Uuid::NAMESPACE_OID,
                serde_json::to_string(workflow).unwrap().as_bytes(),
            ),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Workflow {
    pub operator: TypedOperator,
}
