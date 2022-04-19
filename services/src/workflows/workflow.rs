use serde::{Deserialize, Serialize};
use uuid::Uuid;

use geoengine_datatypes::identifier;
use geoengine_operators::engine::TypedOperator;

use crate::error::Result;
use crate::storage::Storable;
use crate::util::user_input::UserInput;

identifier!(WorkflowId);

impl WorkflowId {
    pub fn from_hash(workflow: &Workflow) -> Self {
        Self(Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            serde_json::to_string(workflow)
                .expect("It is always possible to create a workflow id from a workflow.")
                .as_bytes(),
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Workflow {
    #[serde(flatten)]
    pub operator: TypedOperator,
}

impl Storable for Workflow {
    type Id = WorkflowId;
    type Item = Workflow;
    type ItemListing = WorkflowListing;
    type ListOptions = ();
}

pub struct WorkflowListing {
    pub id: WorkflowId,
}

impl UserInput for Workflow {
    fn validate(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::primitives::Coordinate2D;
    use geoengine_operators::engine::VectorOperator;
    use geoengine_operators::mock::{MockPointSource, MockPointSourceParams};

    #[test]
    fn serde() {
        let workflow = Workflow {
            operator: TypedOperator::Vector(
                MockPointSource {
                    params: MockPointSourceParams {
                        points: vec![Coordinate2D::new(1., 2.); 3],
                    },
                }
                .boxed(),
            ),
        };

        let serialized_workflow = serde_json::to_string(&workflow).unwrap();

        assert_eq!(
            serialized_workflow,
            serde_json::json!({
                "type": "Vector",
                "operator": {
                    "type": "MockPointSource",
                    "params": {
                        "points": [{
                            "x": 1.0,
                            "y": 2.0
                        }, {
                            "x": 1.0,
                            "y": 2.0
                        }, {
                            "x": 1.0,
                            "y": 2.0
                        }]
                    }
                }
            })
            .to_string()
        );

        // TODO: check deserialization
    }
}
