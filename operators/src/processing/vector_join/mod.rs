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
use crate::processing::vector_join::util::translation_table;
use std::collections::HashMap;

mod equi_data_join;
mod util;

/// The vector join operator requires two inputs and the join type.
pub type VectorJoin = Operator<VectorJoinParams>;

/// A set of parameters for the `VectorJoin`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
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

        // TODO: find out if column prefixes are the same for more than one join type and generify
        let column_translation_table = match &self.params.join_type {
            VectorJoinType::EquiGeoToData {
                right_column_suffix,
                ..
            } => {
                let right_column_suffix: &str =
                    right_column_suffix.as_ref().map_or("right", String::as_str);
                translation_table(
                    vector_sources[0].result_descriptor().columns.keys(),
                    vector_sources[1].result_descriptor().columns.keys(),
                    right_column_suffix,
                )
            }
        };

        let result_descriptor = vector_sources[0]
            .result_descriptor()
            .map_columns(|left_columns| {
                let mut columns = left_columns.clone();
                for (right_column_name, right_column_type) in
                    &vector_sources[1].result_descriptor().columns
                {
                    columns.insert(
                        column_translation_table[right_column_name].clone(),
                        *right_column_type,
                    );
                }
                columns
            });

        Ok(InitializedVectorJoin::new(
            result_descriptor,
            vec![],
            vector_sources,
            InitializedVectorJoinParams {
                join_type: self.params.join_type.clone(),
                column_translation_table,
            },
        )
        .boxed())
    }
}

/// A set of parameters for the `VectorJoin`
#[derive(Debug, Clone, PartialEq)]
pub struct InitializedVectorJoinParams {
    join_type: VectorJoinType,
    column_translation_table: HashMap<String, String>,
}

pub type InitializedVectorJoin =
    InitializedOperatorImpl<VectorResultDescriptor, InitializedVectorJoinParams>;

impl InitializedOperator<VectorResultDescriptor, TypedVectorQueryProcessor>
    for InitializedVectorJoin
{
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        match &self.state.join_type {
            VectorJoinType::EquiGeoToData {
                left_column,
                right_column,
                right_column_suffix: _right_column_suffix,
            } => {
                let right_processor = self.vector_sources[1]
                    .query_processor()?
                    .data()
                    .expect("checked in constructor");

                let left = self.vector_sources[0].query_processor()?;

                Ok(match left {
                    TypedVectorQueryProcessor::Data(_) => unreachable!("check in constructor"),
                    TypedVectorQueryProcessor::MultiPoint(left_processor) => {
                        TypedVectorQueryProcessor::MultiPoint(
                            EquiGeoToDataJoinProcessor::new(
                                left_processor,
                                right_processor,
                                left_column.clone(),
                                right_column.clone(),
                                self.state.column_translation_table.clone(),
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
                                self.state.column_translation_table.clone(),
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
                                self.state.column_translation_table.clone(),
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
    use crate::engine::MockExecutionContext;
    use crate::mock::MockFeatureCollectionSource;
    use geoengine_datatypes::collections::{DataCollection, MultiPointCollection};
    use geoengine_datatypes::primitives::{FeatureData, NoGeometry, TimeInterval};

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

    #[test]
    fn initialization() {
        let operator = VectorJoin {
            params: VectorJoinParams {
                join_type: VectorJoinType::EquiGeoToData {
                    left_column: "foo".to_string(),
                    right_column: "bar".to_string(),
                    right_column_suffix: Some("baz".to_string()),
                },
            },
            raster_sources: vec![],
            vector_sources: vec![
                MockFeatureCollectionSource::single(
                    MultiPointCollection::from_slices(
                        &[(0.0, 0.1)],
                        &[TimeInterval::default()],
                        &[("join_column", FeatureData::Int(vec![5]))],
                    )
                    .unwrap(),
                )
                .boxed(),
                MockFeatureCollectionSource::single(
                    DataCollection::from_slices(
                        &[] as &[NoGeometry],
                        &[TimeInterval::default()],
                        &[("join_column", FeatureData::Int(vec![5]))],
                    )
                    .unwrap(),
                )
                .boxed(),
            ],
        };

        operator
            .boxed()
            .initialize(&MockExecutionContext::default())
            .unwrap();
    }
}
