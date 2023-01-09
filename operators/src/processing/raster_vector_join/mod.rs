mod aggregated;
mod aggregator;
mod non_aggregated;
mod util;

use crate::engine::{
    CreateSpan, ExecutionContext, InitializedRasterOperator, InitializedVectorOperator, Operator,
    OperatorName, SingleVectorMultipleRasterSources, TypedVectorQueryProcessor, VectorColumnInfo,
    VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
};
use crate::error::{self, Error};
use crate::processing::raster_vector_join::non_aggregated::RasterVectorJoinProcessor;
use crate::util::Result;

use crate::processing::raster_vector_join::aggregated::RasterVectorAggregateJoinProcessor;
use async_trait::async_trait;
use futures::future::join_all;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::primitives::FeatureDataType;
use geoengine_datatypes::raster::{Pixel, RasterDataType};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use tracing::{span, Level};

use self::aggregator::{
    Aggregator, FirstValueFloatAggregator, FirstValueIntAggregator, MeanValueAggregator,
    TypedAggregator,
};

/// An operator that attaches raster values to vector data
pub type RasterVectorJoin = Operator<RasterVectorJoinParams, SingleVectorMultipleRasterSources>;

impl OperatorName for RasterVectorJoin {
    const TYPE_NAME: &'static str = "RasterVectorJoin";
}

const MAX_NUMBER_OF_RASTER_INPUTS: usize = 8;

/// The parameter spec for `RasterVectorJoin`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RasterVectorJoinParams {
    /// Each name reflects the output column of the join result.
    /// For each raster input, one name must be defined.
    pub names: Vec<String>,

    /// Specifies which method is used for aggregating values for a feature
    pub feature_aggregation: FeatureAggregationMethod,

    /// Specifies which method is used for aggregating values over time
    pub temporal_aggregation: TemporalAggregationMethod,
}

/// How to aggregate the values for the geometries inside a feature e.g.
/// the mean of all the raster values corresponding to the individual
/// points inside a `MultiPoint` feature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Copy)]
#[serde(rename_all = "camelCase")]
pub enum FeatureAggregationMethod {
    First,
    Mean,
}

/// How to aggregate the values over time
/// If there are multiple rasters valid during the validity of a feature
/// the featuer is either split into multiple (None-aggregation) or the
/// values are aggreagated
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Copy)]
#[serde(rename_all = "camelCase")]
pub enum TemporalAggregationMethod {
    None,
    First,
    Mean,
}

#[typetag::serde]
#[async_trait]
impl VectorOperator for RasterVectorJoin {
    async fn _initialize(
        mut self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
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

        let vector_source = self.sources.vector.initialize(context).await?;
        let vector_rd = vector_source.result_descriptor();

        ensure!(
            vector_rd.data_type != VectorDataType::Data,
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

        let raster_sources = join_all(
            self.sources
                .rasters
                .into_iter()
                .map(|s| s.initialize(context)),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

        let spatial_reference = vector_rd.spatial_reference;

        for other_spatial_reference in raster_sources
            .iter()
            .map(|source| source.result_descriptor().spatial_reference)
        {
            ensure!(
                spatial_reference == other_spatial_reference,
                crate::error::InvalidSpatialReference {
                    expected: spatial_reference,
                    found: other_spatial_reference,
                }
            );
        }

        let params = self.params;

        let result_descriptor = vector_rd.map_columns(|columns| {
            let mut columns = columns.clone();
            for (i, new_column_name) in params.names.iter().enumerate() {
                let feature_data_type = match params.temporal_aggregation {
                    TemporalAggregationMethod::First | TemporalAggregationMethod::None => {
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
                    TemporalAggregationMethod::Mean => FeatureDataType::Float,
                };
                columns.insert(
                    new_column_name.clone(),
                    VectorColumnInfo {
                        data_type: feature_data_type,
                        measurement: raster_sources[i].result_descriptor().measurement.clone(),
                    },
                );
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

    span_fn!(RasterVectorJoin);
}

pub struct InitializedRasterVectorJoin {
    result_descriptor: VectorResultDescriptor,
    vector_source: Box<dyn InitializedVectorOperator>,
    raster_sources: Vec<Box<dyn InitializedRasterOperator>>,
    state: RasterVectorJoinParams,
}

impl InitializedVectorOperator for InitializedRasterVectorJoin {
    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        let typed_raster_processors = self
            .raster_sources
            .iter()
            .map(InitializedRasterOperator::query_processor)
            .collect::<Result<Vec<_>>>()?;

        Ok(match self.vector_source.query_processor()? {
            TypedVectorQueryProcessor::Data(_) => unreachable!(),
            TypedVectorQueryProcessor::MultiPoint(points) => {
                TypedVectorQueryProcessor::MultiPoint(match self.state.temporal_aggregation {
                    TemporalAggregationMethod::None => RasterVectorJoinProcessor::new(
                        points,
                        typed_raster_processors,
                        self.state.names.clone(),
                        self.state.feature_aggregation,
                    )
                    .boxed(),
                    TemporalAggregationMethod::First | TemporalAggregationMethod::Mean => {
                        RasterVectorAggregateJoinProcessor::new(
                            points,
                            typed_raster_processors,
                            self.state.names.clone(),
                            self.state.feature_aggregation,
                            self.state.temporal_aggregation,
                        )
                        .boxed()
                    }
                })
            }
            TypedVectorQueryProcessor::MultiPolygon(polygons) => {
                TypedVectorQueryProcessor::MultiPolygon(match self.state.temporal_aggregation {
                    TemporalAggregationMethod::None => RasterVectorJoinProcessor::new(
                        polygons,
                        typed_raster_processors,
                        self.state.names.clone(),
                        self.state.feature_aggregation,
                    )
                    .boxed(),
                    TemporalAggregationMethod::First | TemporalAggregationMethod::Mean => {
                        RasterVectorAggregateJoinProcessor::new(
                            polygons,
                            typed_raster_processors,
                            self.state.names.clone(),
                            self.state.feature_aggregation,
                            self.state.temporal_aggregation,
                        )
                        .boxed()
                    }
                })
            }
            TypedVectorQueryProcessor::MultiLineString(_) => return Err(Error::NotYetImplemented),
        })
    }
}

pub fn create_feature_aggregator<P: Pixel>(
    number_of_features: usize,
    aggregation: FeatureAggregationMethod,
) -> TypedAggregator {
    match aggregation {
        FeatureAggregationMethod::First => match P::TYPE {
            RasterDataType::U8
            | RasterDataType::U16
            | RasterDataType::U32
            | RasterDataType::U64
            | RasterDataType::I8
            | RasterDataType::I16
            | RasterDataType::I32
            | RasterDataType::I64 => FirstValueIntAggregator::new(number_of_features).into_typed(),
            RasterDataType::F32 | RasterDataType::F64 => {
                FirstValueFloatAggregator::new(number_of_features).into_typed()
            }
        },
        FeatureAggregationMethod::Mean => MeanValueAggregator::new(number_of_features).into_typed(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    use crate::engine::{
        ChunkByteSize, MockExecutionContext, MockQueryContext, QueryProcessor, RasterOperator,
    };
    use crate::mock::MockFeatureCollectionSource;
    use crate::source::{GdalSource, GdalSourceParameters};
    use crate::util::gdal::add_ndvi_dataset;
    use futures::StreamExt;
    use geoengine_datatypes::collections::{FeatureCollectionInfos, MultiPointCollection};
    use geoengine_datatypes::dataset::DataId;
    use geoengine_datatypes::primitives::{
        BoundingBox2D, DataRef, DateTime, FeatureDataRef, MultiPoint, SpatialResolution,
        TimeInterval, VectorQueryRectangle,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::{gdal::hide_gdal_errors, test::TestDefault};
    use serde_json::json;

    #[test]
    fn serialization() {
        let raster_vector_join = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: ["foo", "bar"].iter().copied().map(str::to_string).collect(),
                feature_aggregation: FeatureAggregationMethod::First,
                temporal_aggregation: TemporalAggregationMethod::Mean,
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
                "featureAggregation": "first",
                "temporalAggregation": "mean",
            },
            "sources": {
                "vector": {
                    "type": "MockFeatureCollectionSourceMultiPoint",
                    "params": {
                        "collections": [],
                        "spatialReference": "EPSG:4326",
                        "measurements": {},
                    }
                },
                "rasters": [],
            },
        })
        .to_string();

        let deserialized: RasterVectorJoin = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.params, raster_vector_join.params);
    }

    fn ndvi_source(id: DataId) -> Box<dyn RasterOperator> {
        let gdal_source = GdalSource {
            params: GdalSourceParameters { data: id },
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
                        DateTime::new_utc(2014, 1, 1, 0, 0, 0),
                        DateTime::new_utc(2014, 1, 1, 0, 0, 0),
                    )
                    .unwrap();
                    4
                ],
                Default::default(),
            )
            .unwrap(),
        )
        .boxed();

        let mut exe_ctc = MockExecutionContext::test_default();
        let ndvi_id = add_ndvi_dataset(&mut exe_ctc);

        let operator = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: vec!["ndvi".to_string()],
                feature_aggregation: FeatureAggregationMethod::First,
                temporal_aggregation: TemporalAggregationMethod::First,
            },
            sources: SingleVectorMultipleRasterSources {
                vector: point_source,
                rasters: vec![ndvi_source(ndvi_id.clone())],
            },
        };

        let operator = operator.boxed().initialize(&exe_ctc).await.unwrap();

        let query_processor = operator.query_processor().unwrap().multi_point().unwrap();

        let result = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let FeatureDataRef::Int(data) = result[0].data("ndvi").unwrap() else { unreachable!(); };

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
                        DateTime::new_utc(2014, 1, 1, 0, 0, 0),
                        DateTime::new_utc(2014, 3, 1, 0, 0, 0),
                    )
                    .unwrap();
                    4
                ],
                Default::default(),
            )
            .unwrap(),
        )
        .boxed();

        let mut exe_ctc = MockExecutionContext::test_default();
        let ndvi_id = add_ndvi_dataset(&mut exe_ctc);

        let operator = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: vec!["ndvi".to_string()],
                feature_aggregation: FeatureAggregationMethod::First,
                temporal_aggregation: TemporalAggregationMethod::Mean,
            },
            sources: SingleVectorMultipleRasterSources {
                vector: point_source,
                rasters: vec![ndvi_source(ndvi_id.clone())],
            },
        };

        let operator = operator.boxed().initialize(&exe_ctc).await.unwrap();

        let query_processor = operator.query_processor().unwrap().multi_point().unwrap();

        let result = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let FeatureDataRef::Float(data) = result[0].data("ndvi").unwrap() else { unreachable!(); };

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
        hide_gdal_errors();

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

        let mut exe_ctc = MockExecutionContext::test_default();
        let ndvi_id = add_ndvi_dataset(&mut exe_ctc);

        let operator = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: vec!["ndvi".to_string()],
                feature_aggregation: FeatureAggregationMethod::First,
                temporal_aggregation: TemporalAggregationMethod::Mean,
            },
            sources: SingleVectorMultipleRasterSources {
                vector: point_source,
                rasters: vec![ndvi_source(ndvi_id.clone())],
            },
        };

        let operator = operator.boxed().initialize(&exe_ctc).await.unwrap();

        let query_processor = operator.query_processor().unwrap().multi_point().unwrap();

        let result = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let FeatureDataRef::Float(data) = result[0].data("ndvi").unwrap() else { unreachable!(); };

        assert_eq!(data.as_ref(), &[0., 0., 0., 0.]);

        assert_eq!(data.nulls(), vec![true, true, true, true]);
    }

    #[tokio::test]
    async fn it_checks_sref() {
        let point_source = MockFeatureCollectionSource::with_collections_and_sref(
            vec![MultiPointCollection::from_data(
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
            .unwrap()],
            SpatialReference::from_str("EPSG:3857").unwrap(),
        )
        .boxed();

        let mut exe_ctc = MockExecutionContext::test_default();
        let ndvi_id = add_ndvi_dataset(&mut exe_ctc);

        let operator = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: vec!["ndvi".to_string()],
                feature_aggregation: FeatureAggregationMethod::First,
                temporal_aggregation: TemporalAggregationMethod::Mean,
            },
            sources: SingleVectorMultipleRasterSources {
                vector: point_source,
                rasters: vec![ndvi_source(ndvi_id.clone())],
            },
        }
        .boxed()
        .initialize(&exe_ctc)
        .await;

        assert!(matches!(
            operator,
            Err(Error::InvalidSpatialReference {
                expected: _,
                found: _,
            })
        ));
    }
}
