use serde::{Deserialize, Serialize};
use snafu::ensure;

use geoengine_datatypes::collections::VectorDataType;

use crate::engine::{
    ExecutionContext, InitializedOperator, InitializedOperatorImpl, InitializedVectorOperator,
    Operator, TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor,
    VectorResultDescriptor,
};
use crate::error;
use crate::util::Result;

use self::equi_data_join::EquiGeoToDataJoinProcessor;

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
    /// An inner equi-join between a `GeoFeatureCollection` and a `DataCollection`
    EquiGeoToData {
        left_column: String,
        right_column: String,
        /// which suffix to use if columns have conflicting names?
        /// the default is "right"
        right_column_suffix: Option<String>,
    },
}

#[typetag::serde]
impl VectorOperator for VectorJoin {
    fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
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
            VectorJoinType::EquiGeoToData { .. } => {
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
        match &self.params.join_type {
            VectorJoinType::EquiGeoToData {
                left_column,
                right_column,
                right_column_suffix,
            } => {
                let right_processor = self.vector_sources[1]
                    .query_processor()?
                    .data()
                    .expect("checked in constructor");

                let left = self.vector_sources[0].query_processor()?;

                let right_column_suffix: &str =
                    right_column_suffix.as_ref().map_or("right", String::as_str);

                Ok(match left {
                    TypedVectorQueryProcessor::Data(_) => unreachable!("check in constructor"),
                    TypedVectorQueryProcessor::MultiPoint(left_processor) => {
                        TypedVectorQueryProcessor::MultiPoint(
                            EquiGeoToDataJoinProcessor::new(
                                left_processor,
                                right_processor,
                                left_column.clone(),
                                right_column.clone(),
                                right_column_suffix.to_string(),
                            )
                            .boxed(),
                        )
                    }
                    TypedVectorQueryProcessor::MultiLineString(left_processor) => {
                        TypedVectorQueryProcessor::MultiLineString(
                            EquiGeoToDataJoinProcessor::new(
                                left_processor,
                                right_processor,
                                left_column.clone(),
                                right_column.clone(),
                                right_column_suffix.to_string(),
                            )
                            .boxed(),
                        )
                    }
                    TypedVectorQueryProcessor::MultiPolygon(left_processor) => {
                        TypedVectorQueryProcessor::MultiPolygon(
                            EquiGeoToDataJoinProcessor::new(
                                left_processor,
                                right_processor,
                                left_column.clone(),
                                right_column.clone(),
                                right_column_suffix.to_string(),
                            )
                            .boxed(),
                        )
                    }
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params() {
        let params = VectorJoinParams {
            join_type: VectorJoinType::EquiGeoToData {
                left_column: "foo".to_string(),
                right_column: "bar".to_string(),
                right_column_suffix: Some("baz".to_string()),
            },
        };

        let json = serde_json::json!({
            "type": "EquiGeoToData",
            "left_column": "foo",
            "right_column": "bar",
            "right_column_suffix": "baz",
        })
        .to_string();

        assert_eq!(json, serde_json::to_string(&params).unwrap());

        let params_deserialized: VectorJoinParams = serde_json::from_str(&json).unwrap();

        assert_eq!(params, params_deserialized);
    }
}
