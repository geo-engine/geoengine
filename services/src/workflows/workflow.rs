use serde::{Deserialize, Serialize};

use geoengine_operators::Operator;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub struct Workflow {
    pub operator: Operator
}

// TODO: derive if/when operator is Clone
impl Clone for Workflow {
    fn clone(&self) -> Self {
        let serialized = serde_json::to_string(&self.operator).unwrap();
        Self { operator: serde_json::from_str(&serialized).unwrap() }
    }
}
