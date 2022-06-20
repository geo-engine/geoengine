use geoengine_datatypes::dataset::DatasetId;
use serde::{Deserialize, Serialize};
use snafu::ensure;

use geoengine_datatypes::collections::VectorDataType;

use crate::engine::{
    ExecutionContext, InitializedVectorOperator, Operator, OperatorDatasets,
    TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
};
use crate::error;
use crate::util::Result;

use self::equi_data_join::EquiGeoToDataJoinProcessor;
use crate::processing::vector_join::util::translation_table;
use async_trait::async_trait;
use std::collections::HashMap;

mod equi_data_join;
mod util;

/// The vector join operator requires two inputs and the join type.
pub type VectorJoin = Operator<VectorJoinParams, VectorJoinSources>;

/// A set of parameters for the `VectorJoin`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VectorJoinParams {
    #[serde(flatten)]
    join_type: VectorJoinType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorJoinSources {
    left: Box<dyn VectorOperator>,
    right: Box<dyn VectorOperator>,
}

impl OperatorDatasets for VectorJoinSources {
    fn datasets_collect(&self, datasets: &mut Vec<DatasetId>) {
        self.left.datasets_collect(datasets);
        self.right.datasets_collect(datasets);
    }
}

/// Define the type of join
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
#[async_trait]
impl VectorOperator for VectorJoin {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        let left = self.sources.left.initialize(context).await?;
        let right = self.sources.right.initialize(context).await?;

        match self.params.join_type {
            VectorJoinType::EquiGeoToData { .. } => {
                ensure!(
                    left.result_descriptor().data_type != VectorDataType::Data,
                    error::InvalidType {
                        expected: "a geo data collection".to_string(),
                        found: left.result_descriptor().data_type.to_string(),
                    }
                );
                ensure!(
                    right.result_descriptor().data_type == VectorDataType::Data,
                    error::InvalidType {
                        expected: VectorDataType::Data.to_string(),
                        found: right.result_descriptor().data_type.to_string(),
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
                    left.result_descriptor().columns.keys(),
                    right.result_descriptor().columns.keys(),
                    right_column_suffix,
                )
            }
        };

        let result_descriptor = left.result_descriptor().map_columns(|left_columns| {
            let mut columns = left_columns.clone();
            for (right_column_name, right_column_type) in &right.result_descriptor().columns {
                columns.insert(
                    column_translation_table[right_column_name].clone(),
                    right_column_type.clone(),
                );
            }
            columns
        });

        let initialized_operator = InitializedVectorJoin {
            result_descriptor,
            left,
            right,
            state: InitializedVectorJoinParams {
                join_type: self.params.join_type.clone(),
                column_translation_table,
            },
        };

        Ok(initialized_operator.boxed())
    }
}

/// A set of parameters for the `VectorJoin`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitializedVectorJoinParams {
    join_type: VectorJoinType,
    column_translation_table: HashMap<String, String>,
}

pub struct InitializedVectorJoin {
    result_descriptor: VectorResultDescriptor,
    left: Box<dyn InitializedVectorOperator>,
    right: Box<dyn InitializedVectorOperator>,
    state: InitializedVectorJoinParams,
}

impl InitializedVectorOperator for InitializedVectorJoin {
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        match &self.state.join_type {
            VectorJoinType::EquiGeoToData {
                left_column,
                right_column,
                right_column_suffix: _right_column_suffix,
            } => {
                let right_processor = self
                    .right
                    .query_processor()?
                    .data()
                    .expect("checked in constructor");

                let left = self.left.query_processor()?;

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

    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::MockExecutionContext;
    use crate::mock::MockFeatureCollectionSource;
    use geoengine_datatypes::collections::{DataCollection, MultiPointCollection};
    use geoengine_datatypes::primitives::{FeatureData, NoGeometry, TimeInterval};
    use geoengine_datatypes::util::test::TestDefault;

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

    #[tokio::test]
    async fn initialization() {
        let operator = VectorJoin {
            params: VectorJoinParams {
                join_type: VectorJoinType::EquiGeoToData {
                    left_column: "foo".to_string(),
                    right_column: "bar".to_string(),
                    right_column_suffix: Some("baz".to_string()),
                },
            },
            sources: VectorJoinSources {
                left: MockFeatureCollectionSource::single(
                    MultiPointCollection::from_slices(
                        &[(0.0, 0.1)],
                        &[TimeInterval::default()],
                        &[("join_column", FeatureData::Int(vec![5]))],
                    )
                    .unwrap(),
                )
                .boxed(),
                right: MockFeatureCollectionSource::single(
                    DataCollection::from_slices(
                        &[] as &[NoGeometry],
                        &[TimeInterval::default()],
                        &[("join_column", FeatureData::Int(vec![5]))],
                    )
                    .unwrap(),
                )
                .boxed(),
            },
        };

        operator
            .boxed()
            .initialize(&MockExecutionContext::test_default())
            .await
            .unwrap();
    }
}
