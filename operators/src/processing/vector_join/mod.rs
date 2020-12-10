use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::engine::{
    ExecutionContext, InitializedOperator, InitializedOperatorImpl, InitializedVectorOperator,
    Operator, TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor,
    VectorResultDescriptor,
};
use crate::error::{self, Error};
use crate::util::Result;

use geoengine_datatypes::collections::VectorDataType;

mod equi_data_join;

/// The vector join operator requires two inputs and the join type.
pub type VectorJoin = Operator<VectorJoinParams>;

/// A set of parameters for the `VectorJoin`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VectorJoinParams {
    #[serde(flatten)]
    join_type: VectorJoinType,
}

/// Define the type of join
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum VectorJoinType {
    /// A left equi join between a `GeoFeatureCollection` and a `DataCollection`
    EquiLeft { left: String, right: String },
}

#[typetag::serde]
impl VectorOperator for VectorJoin {
    fn initialize(
        self: Box<Self>,
        context: &ExecutionContext,
    ) -> Result<Box<InitializedVectorOperator>> {
        ensure!(
            self.vector_sources.len() == 2,
            error::InvalidNumberOfVectorInputs {
                expected: 2..3,
                found: self.vector_sources.len()
            }
        );
        ensure!(
            self.raster_sources.is_empty(),
            error::InvalidNumberOfRasterInputs {
                expected: 0..1,
                found: self.raster_sources.len()
            }
        );

        // Initialize sources
        let vector_sources = self
            .vector_sources
            .into_iter()
            .map(|o| o.initialize(context))
            .collect::<Result<Vec<Box<InitializedVectorOperator>>>>()?;

        match self.params.join_type {
            VectorJoinType::EquiLeft { .. } => {
                ensure!(
                    vector_sources[0].result_descriptor().data_type != VectorDataType::Data,
                    error::InvalidType {
                        expected: "a geo data collection".to_string(),
                        found: vector_sources[0].result_descriptor().data_type.to_string(),
                    }
                );
                ensure!(
                    vector_sources[1].result_descriptor().data_type == VectorDataType::Data,
                    error::InvalidType {
                        expected: VectorDataType::Data.to_string(),
                        found: vector_sources[1].result_descriptor().data_type.to_string(),
                    }
                );
            }
        }

        Ok(InitializedVectorJoin::new(
            self.params,
            vector_sources[0].result_descriptor(),
            vec![],
            vector_sources,
            (),
        )
        .boxed())
    }
}

pub type InitializedVectorJoin =
    InitializedOperatorImpl<VectorJoinParams, VectorResultDescriptor, ()>;

impl InitializedOperator<VectorResultDescriptor, TypedVectorQueryProcessor>
    for InitializedVectorJoin
{
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params() {
        let params = VectorJoinParams {
            join_type: VectorJoinType::EquiLeft {
                left: "foo".to_string(),
                right: "bar".to_string(),
            },
        };

        let json = serde_json::json!({
            "type": "EquiLeft",
            "left": "foo",
            "right": "bar"
        })
        .to_string();

        assert_eq!(json, serde_json::to_string(&params).unwrap());

        let params_deserialized: VectorJoinParams = serde_json::from_str(&json).unwrap();

        assert_eq!(params, params_deserialized);
    }
}
