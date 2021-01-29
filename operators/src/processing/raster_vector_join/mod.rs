mod aggregator;
mod points;
mod util;

use crate::engine::{
    ExecutionContext, InitializedOperator, InitializedOperatorImpl, InitializedVectorOperator,
    Operator, TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor,
    VectorResultDescriptor,
};
use crate::error;
use crate::util::Result;

use crate::processing::raster_vector_join::points::RasterPointJoinProcessor;
use geoengine_datatypes::collections::VectorDataType;
use serde::{Deserialize, Serialize};
use snafu::ensure;

/// An operator that attaches raster values to vector data
pub type RasterVectorJoin = Operator<RasterVectorJoinParams>;

const MAX_NUMBER_OF_RASTER_INPUTS: usize = 8;

/// The parameter spec for `RasterVectorJoin`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RasterVectorJoinParams {
    /// Each name reflects the output column of the join result.
    /// For each raster input, one name must be defined.
    pub names: Vec<String>,

    /// Specifies which method is used for aggregating values
    pub aggregation: AggregationMethod,
}

/// The aggregation method for extracted values
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Copy)]
#[serde(rename_all = "snake_case")]
pub enum AggregationMethod {
    First,
    Mean,
}

#[typetag::serde]
impl VectorOperator for RasterVectorJoin {
    fn initialize(
        mut self: Box<Self>,
        context: &ExecutionContext<'_>,
    ) -> Result<Box<InitializedVectorOperator>> {
        ensure!(
            self.vector_sources.len() == 1,
            error::InvalidNumberOfVectorInputs {
                expected: 1..2,
                found: self.vector_sources.len()
            }
        );

        ensure!(
            !self.raster_sources.is_empty()
                || self.raster_sources.len() > MAX_NUMBER_OF_RASTER_INPUTS,
            error::InvalidNumberOfRasterInputs {
                expected: 1..MAX_NUMBER_OF_RASTER_INPUTS,
                found: self.raster_sources.len()
            }
        );
        ensure!(
            self.raster_sources.len() == self.params.names.len(),
            error::InvalidOperatorSpec {
                reason: "`raster_sources` must be of equal length as `names`"
            }
        );

        let vector_source = self.vector_sources.remove(0).initialize(context)?;

        ensure!(
            vector_source.result_descriptor().data_type != VectorDataType::Data,
            error::InvalidType {
                expected: format!(
                    "{}, {} or {}",
                    VectorDataType::MultiPoint,
                    VectorDataType::MultiLineString,
                    VectorDataType::MultiPolygon
                ),
                found: VectorDataType::Data.to_string()
            },
        );

        // TODO: check for column clashes earlier with the result descriptor
        // TODO: update result descriptor with new column(s)

        Ok(InitializedRasterVectorJoin {
            params: self.params,
            raster_sources: self
                .raster_sources
                .into_iter()
                .map(|source| source.initialize(context))
                .collect::<Result<Vec<_>>>()?,
            result_descriptor: vector_source.result_descriptor(),
            vector_sources: vec![vector_source],
            state: (),
        }
        .boxed())
    }
}

pub type InitializedRasterVectorJoin =
    InitializedOperatorImpl<RasterVectorJoinParams, VectorResultDescriptor, ()>;

impl InitializedOperator<VectorResultDescriptor, TypedVectorQueryProcessor>
    for InitializedRasterVectorJoin
{
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        let typed_raster_processors = self
            .raster_sources
            .iter()
            .map(|r| r.query_processor())
            .collect::<Result<Vec<_>>>()?;

        Ok(match self.vector_sources[0].query_processor()? {
            TypedVectorQueryProcessor::Data(_) => unreachable!(),
            TypedVectorQueryProcessor::MultiPoint(points) => TypedVectorQueryProcessor::MultiPoint(
                RasterPointJoinProcessor::new(
                    points,
                    typed_raster_processors,
                    self.params.names.clone(),
                    self.params.aggregation,
                )
                .boxed(),
            ),
            TypedVectorQueryProcessor::MultiLineString(_)
            | TypedVectorQueryProcessor::MultiPolygon(_) => todo!("implement"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::{
        MockExecutionContextCreator, QueryContext, QueryProcessor, QueryRectangle, RasterOperator,
    };
    use crate::mock::MockFeatureCollectionSource;
    use crate::source::{GdalSource, GdalSourceParameters};
    use chrono::NaiveDate;
    use float_cmp::approx_eq;
    use futures::StreamExt;
    use geoengine_datatypes::collections::{FeatureCollectionInfos, MultiPointCollection};
    use geoengine_datatypes::primitives::{
        BoundingBox2D, FeatureDataRef, MultiPoint, SpatialResolution, TimeInterval,
    };
    use serde_json::json;

    #[test]
    fn serialization() {
        let raster_vector_join = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: ["foo", "bar"].iter().cloned().map(str::to_string).collect(),
                aggregation: AggregationMethod::Mean,
            },
            raster_sources: vec![],
            vector_sources: vec![],
        };

        let serialized = json!({
            "type": "RasterVectorJoin",
            "params": {
                "names": ["foo", "bar"],
                "aggregation": "mean",
            },
            "raster_sources": [],
            "vector_sources": [],
        })
        .to_string();

        let deserialized: RasterVectorJoin = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.params, raster_vector_join.params);
    }

    fn ndvi_source() -> Box<dyn RasterOperator> {
        let gdal_source = GdalSource {
            params: GdalSourceParameters {
                dataset_id: "modis_ndvi".to_string(),
                channel: None,
            },
        };

        gdal_source.boxed()
    }

    fn raster_dir() -> std::path::PathBuf {
        let mut current_path = std::env::current_dir().unwrap();

        if !current_path.ends_with("operators") {
            current_path = current_path.join("operators");
        }

        current_path = current_path.join("test-data/raster");

        current_path
    }

    #[tokio::test]
    async fn ndvi_time_point() {
        let point_source = MockFeatureCollectionSource::single(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![
                    (-13.95, 20.05),
                    (-14.05, 20.05),
                    (-13.95, 19.95),
                    (-14.05, 19.95),
                ])
                .unwrap(),
                vec![
                    TimeInterval::new(
                        NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
                        NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
                    )
                    .unwrap();
                    4
                ],
                Default::default(),
            )
            .unwrap(),
        )
        .boxed();

        let operator = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: vec!["ndvi".to_string()],
                aggregation: AggregationMethod::First,
            },
            raster_sources: vec![ndvi_source()],
            vector_sources: vec![point_source],
        };

        let execution_context_creator = MockExecutionContextCreator::default();
        let mut execution_context = execution_context_creator.context();

        execution_context.raster_data_root = raster_dir();

        let operator = operator.boxed().initialize(&execution_context).unwrap();

        let query_processor = operator.query_processor().unwrap().multi_point().unwrap();

        let result = query_processor
            .query(
                QueryRectangle {
                    bbox: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                },
                QueryContext { chunk_byte_size: 0 },
            )
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let data = if let FeatureDataRef::Decimal(data) = result[0].data("ndvi").unwrap() {
            data
        } else {
            unreachable!();
        };

        // these values are taken from loading the tiff in QGIS
        assert_eq!(data.as_ref(), &[54, 55, 51, 55]);
    }

    #[tokio::test]
    #[allow(clippy::float_cmp)]
    async fn ndvi_time_range() {
        let point_source = MockFeatureCollectionSource::single(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![
                    (-13.95, 20.05),
                    (-14.05, 20.05),
                    (-13.95, 19.95),
                    (-14.05, 19.95),
                ])
                .unwrap(),
                vec![
                    TimeInterval::new(
                        NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
                        NaiveDate::from_ymd(2014, 3, 1).and_hms(0, 0, 0),
                    )
                    .unwrap();
                    4
                ],
                Default::default(),
            )
            .unwrap(),
        )
        .boxed();

        let operator = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: vec!["ndvi".to_string()],
                aggregation: AggregationMethod::Mean,
            },
            raster_sources: vec![ndvi_source()],
            vector_sources: vec![point_source],
        };

        let execution_context_creator = MockExecutionContextCreator::default();
        let mut execution_context = execution_context_creator.context();

        execution_context.raster_data_root = raster_dir();

        let operator = operator.boxed().initialize(&execution_context).unwrap();

        let query_processor = operator.query_processor().unwrap().multi_point().unwrap();

        let result = query_processor
            .query(
                QueryRectangle {
                    bbox: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                },
                QueryContext { chunk_byte_size: 0 },
            )
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let data = if let FeatureDataRef::Number(data) = result[0].data("ndvi").unwrap() {
            data
        } else {
            unreachable!();
        };

        // these values are taken from loading the tiff in QGIS
        approx_eq!(f64, data.as_ref()[0], (54. + 52.) / 2.);
        approx_eq!(f64, data.as_ref()[1], (55. + 55.) / 2.);
        approx_eq!(f64, data.as_ref()[2], (51. + 50.) / 2.);
        approx_eq!(f64, data.as_ref()[3], (55. + 53.) / 2.);
    }
}
