use crate::api::{handlers::workflows::WorkflowApiError, model::processing_graphs::TypedOperator};
use crate::error::Result;
use crate::identifier;
use geoengine_operators::engine::TypedOperator as OperatorsTypedOperator;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

identifier!(WorkflowId);

impl WorkflowId {
    #[allow(clippy::missing_panics_doc)]
    pub fn from_hash(workflow: &Workflow) -> Self {
        Self(Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            serde_json::to_string(workflow)
                .expect("It is always possible to create a workflow id from a workflow.")
                .as_bytes(),
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum Workflow {
    Typed {
        #[serde(flatten)]
        operator: TypedOperator,
    },
    // TODO: remove this variant when all workflows are migrated to typed ones
    Legacy {
        #[serde(flatten)]
        #[schema(value_type = crate::api::model::operators::LegacyTypedOperator)]
        operator: OperatorsTypedOperator,
    },
}

impl PartialEq for Workflow {
    fn eq(&self, other: &Self) -> bool {
        match (serde_json::to_string(self), serde_json::to_string(other)) {
            (Ok(a), Ok(b)) => a == b,
            _ => false,
        }
    }
}

impl Workflow {
    pub fn operator(&self) -> Result<OperatorsTypedOperator> {
        match self {
            Workflow::Typed { operator } => {
                operator
                    .clone()
                    .try_into()
                    .map_err(|source: anyhow::Error| crate::error::Error::WorkflowApi {
                        source: WorkflowApiError::EngineTypeConversion { source },
                    })
            }
            Workflow::Legacy { operator } => Ok(operator.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::model::{
        datatypes::Coordinate2D,
        processing_graphs::{
            MockPointSource, MockPointSourceParameters, SpatialBoundsDerive, VectorOperator,
        },
    };

    #[test]
    fn serde() {
        let workflow = Workflow::Typed {
            operator: TypedOperator::Vector(VectorOperator::MockPointSource(MockPointSource {
                r#type: Default::default(),
                params: MockPointSourceParameters {
                    points: vec![Coordinate2D { x: 1., y: 2. }; 3],
                    spatial_bounds: SpatialBoundsDerive::None(Default::default()),
                },
            })),
        };

        let serialized_workflow = serde_json::to_value(&workflow).unwrap();

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
                        }],
                        "spatialBounds": {
                            "type": "none"
                        }
                    }
                }
            })
        );

        let deserialized_workflow: Workflow = serde_json::from_value(serialized_workflow).unwrap();

        assert_eq!(workflow, deserialized_workflow);
    }
}
