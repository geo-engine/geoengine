mod aggregator;
mod points;
mod points_aggregated;
mod util;

use crate::engine::{
    ExecutionContext, InitializedOperator, InitializedRasterOperator, InitializedVectorOperator,
    Operator, SingleVectorMultipleRasterSources, TypedVectorQueryProcessor, VectorOperator,
    VectorQueryProcessor, VectorResultDescriptor,
};
use crate::error;
use crate::util::Result;

use crate::processing::raster_vector_join::points::RasterPointJoinProcessor;
use crate::processing::raster_vector_join::points_aggregated::RasterPointAggregateJoinProcessor;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::primitives::FeatureDataType;
use geoengine_datatypes::raster::RasterDataType;
use serde::{Deserialize, Serialize};
use snafu::ensure;

/// An operator that attaches raster values to vector data
pub type RasterVectorJoin = Operator<RasterVectorJoinParams, SingleVectorMultipleRasterSources>;

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
#[serde(rename_all = "camelCase")]
pub enum AggregationMethod {
    None,
    First,
    Mean,
}

#[typetag::serde]
impl VectorOperator for RasterVectorJoin {
    fn initialize(
        mut self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<InitializedVectorOperator>> {
        ensure!(
            (1..=MAX_NUMBER_OF_RASTER_INPUTS).contains(&self.sources.rasters.len()),
            error::InvalidNumberOfRasterInputs {
                expected: 1..MAX_NUMBER_OF_RASTER_INPUTS,
                found: self.sources.rasters.len()
            }
        );
        ensure!(
            self.sources.rasters.len() == self.params.names.len(),
            error::InvalidOperatorSpec {
                reason: "`rasters` must be of equal length as `names`"
            }
        );

        let vector_source = self.sources.vector.initialize(context)?;

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

        let raster_sources = self
            .sources
            .rasters
            .drain(..)
            .map(|source| source.initialize(context))
            .collect::<Result<Vec<_>>>()?;

        let params = self.params;

        let result_descriptor = vector_source.result_descriptor().map_columns(|columns| {
            let mut columns = columns.clone();
            for (i, new_column_name) in params.names.iter().enumerate() {
                let feature_data_type = match params.aggregation {
                    AggregationMethod::First | AggregationMethod::None => {
                        match raster_sources[i].result_descriptor().data_type {
                            RasterDataType::U8
                            | RasterDataType::U16
                            | RasterDataType::U32
                            | RasterDataType::U64
                            | RasterDataType::I8
                            | RasterDataType::I16
                            | RasterDataType::I32
                            | RasterDataType::I64 => FeatureDataType::Int,
                            RasterDataType::F32 | RasterDataType::F64 => FeatureDataType::Float,
                        }
                    }
                    AggregationMethod::Mean => FeatureDataType::Float,
                };
                columns.insert(new_column_name.clone(), feature_data_type);
            }
            columns
        });

        Ok(InitializedRasterVectorJoin {
            result_descriptor,
            vector_source,
            raster_sources,
            state: params,
        }
        .boxed())
    }
}

pub struct InitializedRasterVectorJoin {
    result_descriptor: VectorResultDescriptor,
    vector_source: Box<InitializedVectorOperator>,
    raster_sources: Vec<Box<InitializedRasterOperator>>,
    state: RasterVectorJoinParams,
}

impl InitializedOperator<VectorResultDescriptor, TypedVectorQueryProcessor>
    for InitializedRasterVectorJoin
{
    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        let typed_raster_processors = self
            .raster_sources
            .iter()
            .map(|r| r.query_processor())
            .collect::<Result<Vec<_>>>()?;

        Ok(match self.vector_source.query_processor()? {
            TypedVectorQueryProcessor::Data(_) => unreachable!(),
            TypedVectorQueryProcessor::MultiPoint(points) => {
                TypedVectorQueryProcessor::MultiPoint(match self.state.aggregation {
                    AggregationMethod::None => RasterPointJoinProcessor::new(
                        points,
                        typed_raster_processors,
                        self.state.names.clone(),
                    )
                    .boxed(),
                    AggregationMethod::First | AggregationMethod::Mean => {
                        RasterPointAggregateJoinProcessor::new(
                            points,
                            typed_raster_processors,
                            self.state.names.clone(),
                            self.state.aggregation,
                        )
                        .boxed()
                    }
                })
            }
            TypedVectorQueryProcessor::MultiLineString(_)
            | TypedVectorQueryProcessor::MultiPolygon(_) => todo!("implement"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::{
        MockExecutionContext, MockQueryContext, QueryProcessor, QueryRectangle, RasterOperator,
    };
    use crate::mock::MockFeatureCollectionSource;
    use crate::source::{GdalSource, GdalSourceParameters};
    use crate::util::gdal::add_ndvi_dataset;
    use chrono::NaiveDate;
    use futures::StreamExt;
    use geoengine_datatypes::collections::{FeatureCollectionInfos, MultiPointCollection};
    use geoengine_datatypes::dataset::DatasetId;
    use geoengine_datatypes::primitives::{
        BoundingBox2D, DataRef, FeatureDataRef, MultiPoint, SpatialResolution, TimeInterval,
    };
    use serde_json::json;

    #[test]
    fn serialization() {
        let raster_vector_join = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: ["foo", "bar"].iter().copied().map(str::to_string).collect(),
                aggregation: AggregationMethod::Mean,
            },
            sources: SingleVectorMultipleRasterSources {
                vector: MockFeatureCollectionSource::<MultiPoint>::multiple(vec![]).boxed(),
                rasters: vec![],
            },
        };

        let serialized = json!({
            "type": "RasterVectorJoin",
            "params": {
                "names": ["foo", "bar"],
                "aggregation": "mean",
            },
            "sources": {
                "vector": {
                    "type": "MockFeatureCollectionSourceMultiPoint",
                    "params": {
                        "collections": []
                    }
                },
                "rasters": [],
            },
        })
        .to_string();

        let deserialized: RasterVectorJoin = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.params, raster_vector_join.params);
    }

    fn ndvi_source(id: DatasetId) -> Box<dyn RasterOperator> {
        let gdal_source = GdalSource {
            params: GdalSourceParameters { dataset: id },
        };

        gdal_source.boxed()
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

        let mut exe_ctc = MockExecutionContext::default();
        let ndvi_id = add_ndvi_dataset(&mut exe_ctc);

        let operator = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: vec!["ndvi".to_string()],
                aggregation: AggregationMethod::First,
            },
            sources: SingleVectorMultipleRasterSources {
                vector: point_source,
                rasters: vec![ndvi_source(ndvi_id.clone())],
            },
        };

        let operator = operator.boxed().initialize(&exe_ctc).unwrap();

        let query_processor = operator.query_processor().unwrap().multi_point().unwrap();

        let result = query_processor
            .query(
                QueryRectangle {
                    bbox: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                },
                &MockQueryContext::new(0),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let data = if let FeatureDataRef::Int(data) = result[0].data("ndvi").unwrap() {
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

        let mut exe_ctc = MockExecutionContext::default();
        let ndvi_id = add_ndvi_dataset(&mut exe_ctc);

        let operator = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: vec!["ndvi".to_string()],
                aggregation: AggregationMethod::Mean,
            },
            sources: SingleVectorMultipleRasterSources {
                vector: point_source,
                rasters: vec![ndvi_source(ndvi_id.clone())],
            },
        };

        let operator = operator.boxed().initialize(&exe_ctc).unwrap();

        let query_processor = operator.query_processor().unwrap().multi_point().unwrap();

        let result = query_processor
            .query(
                QueryRectangle {
                    bbox: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                },
                &MockQueryContext::new(0),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let data = if let FeatureDataRef::Float(data) = result[0].data("ndvi").unwrap() {
            data
        } else {
            unreachable!();
        };

        // these values are taken from loading the tiff in QGIS
        assert_eq!(
            data.as_ref(),
            &[
                (54. + 52.) / 2.,
                (55. + 55.) / 2.,
                (51. + 50.) / 2.,
                (55. + 53.) / 2.,
            ]
        );
    }

    #[tokio::test]
    #[allow(clippy::float_cmp)]
    async fn ndvi_with_default_time() {
        let point_source = MockFeatureCollectionSource::single(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![
                    (-13.95, 20.05),
                    (-14.05, 20.05),
                    (-13.95, 19.95),
                    (-14.05, 19.95),
                ])
                .unwrap(),
                vec![TimeInterval::default(); 4],
                Default::default(),
            )
            .unwrap(),
        )
        .boxed();

        let mut exe_ctc = MockExecutionContext::default();
        let ndvi_id = add_ndvi_dataset(&mut exe_ctc);

        let operator = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: vec!["ndvi".to_string()],
                aggregation: AggregationMethod::Mean,
            },
            sources: SingleVectorMultipleRasterSources {
                vector: point_source,
                rasters: vec![ndvi_source(ndvi_id.clone())],
            },
        };

        let operator = operator.boxed().initialize(&exe_ctc).unwrap();

        let query_processor = operator.query_processor().unwrap().multi_point().unwrap();

        let result = query_processor
            .query(
                QueryRectangle {
                    bbox: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                },
                &MockQueryContext::new(0),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let data = if let FeatureDataRef::Float(data) = result[0].data("ndvi").unwrap() {
            data
        } else {
            unreachable!();
        };

        assert_eq!(data.as_ref(), &[0., 0., 0., 0.]);

        assert_eq!(data.nulls(), vec![true, true, true, true]);
    }
}
