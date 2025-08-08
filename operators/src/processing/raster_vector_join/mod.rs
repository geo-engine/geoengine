mod aggregated;
mod aggregator;
mod non_aggregated;
mod util;

use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedRasterOperator, InitializedVectorOperator,
    Operator, OperatorName, SingleVectorMultipleRasterSources, TypedRasterQueryProcessor,
    TypedVectorQueryProcessor, VectorColumnInfo, VectorOperator, VectorQueryProcessor,
    VectorResultDescriptor, WorkflowOperatorPath,
};
use crate::error::{self, ColumnNameConflict, Error};
use crate::processing::raster_vector_join::non_aggregated::RasterVectorJoinProcessor;
use crate::util::Result;

use crate::processing::raster_vector_join::aggregated::RasterVectorAggregateJoinProcessor;
use async_trait::async_trait;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::primitives::FeatureDataType;
use geoengine_datatypes::raster::{Pixel, RasterDataType, RenameBands};
use serde::{Deserialize, Serialize};
use snafu::ensure;

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
    /// The names of the new columns are derived from the names of the raster bands.
    /// This parameter specifies how to perform the derivation.
    pub names: ColumnNames,

    /// Specifies which method is used for aggregating values for a feature
    pub feature_aggregation: FeatureAggregationMethod,

    /// Whether NO DATA values should be ignored in aggregating the joined feature data
    /// `false` by default
    #[serde(default)]
    pub feature_aggregation_ignore_no_data: bool,

    /// Specifies which method is used for aggregating values over time
    pub temporal_aggregation: TemporalAggregationMethod,

    /// Whether NO DATA values should be ignored in aggregating the joined temporal data
    /// `false` by default
    #[serde(default)]
    pub temporal_aggregation_ignore_no_data: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type", content = "values")]
pub enum ColumnNames {
    Default, // use the input band name and append " (n)" to the band name for the `n`-th conflict,
    Suffix(Vec<String>), // A suffix for every input, to be appended to the original band names
    Names(Vec<String>), // A column name for each band, to be used instead of the original band names
}

impl From<ColumnNames> for RenameBands {
    fn from(column_names: ColumnNames) -> Self {
        match column_names {
            ColumnNames::Default => RenameBands::Default,
            ColumnNames::Suffix(suffixes) => RenameBands::Suffix(suffixes),
            ColumnNames::Names(names) => RenameBands::Rename(names),
        }
    }
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

#[allow(clippy::too_many_lines)]
#[typetag::serde]
#[async_trait]
impl VectorOperator for RasterVectorJoin {
    async fn _initialize(
        mut self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        ensure!(
            (1..=MAX_NUMBER_OF_RASTER_INPUTS).contains(&self.sources.rasters.len()),
            error::InvalidNumberOfRasterInputs {
                expected: 1..MAX_NUMBER_OF_RASTER_INPUTS,
                found: self.sources.rasters.len()
            }
        );

        let name = CanonicOperatorName::from(&self);

        let vector_source = self
            .sources
            .vector
            .initialize(path.clone_and_append(0), context)
            .await?;

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

        let raster_sources = futures::future::try_join_all(
            self.sources
                .rasters
                .into_iter()
                .enumerate()
                .map(|(i, op)| op.initialize(path.clone_and_append(i as u8 + 1), context)),
        )
        .await?;

        let source_descriptors = raster_sources
            .iter()
            .map(InitializedRasterOperator::result_descriptor)
            .collect::<Vec<_>>();

        let spatial_reference = vector_rd.spatial_reference;

        for other_spatial_reference in source_descriptors
            .iter()
            .map(|source_descriptor| source_descriptor.spatial_reference)
        {
            ensure!(
                spatial_reference == other_spatial_reference,
                crate::error::InvalidSpatialReference {
                    expected: spatial_reference,
                    found: other_spatial_reference,
                }
            );
        }

        let raster_sources_bands = source_descriptors
            .iter()
            .map(|rd| rd.bands.count() as usize)
            .collect::<Vec<_>>();

        let rename_bands: RenameBands = self.params.names.clone().into();

        let new_column_names = rename_bands.apply(
            source_descriptors
                .iter()
                .map(|d| d.bands.iter().map(|b| b.name.clone()).collect())
                .collect(),
        )?;

        for name in vector_rd.columns.keys() {
            ensure!(
                !new_column_names.contains(name),
                ColumnNameConflict { name }
            );
        }

        let params = self.params;

        let result_descriptor = vector_rd.map_columns(|columns| {
            let mut columns = columns.clone();
            let mut new_column_name_idx = 0;

            for source_descriptor in &source_descriptors {
                let feature_data_type = match params.temporal_aggregation {
                    TemporalAggregationMethod::First | TemporalAggregationMethod::None => {
                        match source_descriptor.data_type {
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

                for band in source_descriptor.bands.iter() {
                    let column_name = new_column_names[new_column_name_idx].clone();
                    new_column_name_idx += 1;

                    columns.insert(
                        column_name,
                        VectorColumnInfo {
                            data_type: feature_data_type,
                            measurement: band.measurement.clone(),
                        },
                    );
                }
            }
            columns
        });

        Ok(InitializedRasterVectorJoin {
            name,
            path,
            result_descriptor,
            vector_source,
            raster_sources,
            raster_sources_bands,
            state: params,
            new_column_names,
        }
        .boxed())
    }

    span_fn!(RasterVectorJoin);
}

pub struct RasterInput {
    pub processor: TypedRasterQueryProcessor,
    pub column_names: Vec<String>,
}

pub struct InitializedRasterVectorJoin {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    result_descriptor: VectorResultDescriptor,
    vector_source: Box<dyn InitializedVectorOperator>,
    raster_sources: Vec<Box<dyn InitializedRasterOperator>>,
    raster_sources_bands: Vec<usize>,
    state: RasterVectorJoinParams,
    new_column_names: Vec<String>,
}

impl InitializedVectorOperator for InitializedRasterVectorJoin {
    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        let mut raster_inputs = vec![];

        let mut names = self.new_column_names.clone();
        for (raster_source, num_bands) in self
            .raster_sources
            .iter()
            .zip(self.raster_sources_bands.iter())
        {
            let processor = raster_source.query_processor()?;
            let column_names = names.drain(0..*num_bands).collect::<Vec<_>>();

            raster_inputs.push(RasterInput {
                processor,
                column_names,
            });
        }

        Ok(match self.vector_source.query_processor()? {
            TypedVectorQueryProcessor::Data(_) => unreachable!(),
            TypedVectorQueryProcessor::MultiPoint(points) => {
                TypedVectorQueryProcessor::MultiPoint(match self.state.temporal_aggregation {
                    TemporalAggregationMethod::None => RasterVectorJoinProcessor::new(
                        points,
                        self.result_descriptor.clone(),
                        raster_inputs,
                        self.state.feature_aggregation,
                        self.state.feature_aggregation_ignore_no_data,
                    )
                    .boxed(),
                    TemporalAggregationMethod::First | TemporalAggregationMethod::Mean => {
                        RasterVectorAggregateJoinProcessor::new(
                            points,
                            self.result_descriptor.clone(),
                            raster_inputs,
                            self.state.feature_aggregation,
                            self.state.feature_aggregation_ignore_no_data,
                            self.state.temporal_aggregation,
                            self.state.temporal_aggregation_ignore_no_data,
                        )
                        .boxed()
                    }
                })
            }
            TypedVectorQueryProcessor::MultiPolygon(polygons) => {
                TypedVectorQueryProcessor::MultiPolygon(match self.state.temporal_aggregation {
                    TemporalAggregationMethod::None => RasterVectorJoinProcessor::new(
                        polygons,
                        self.result_descriptor.clone(),
                        raster_inputs,
                        self.state.feature_aggregation,
                        self.state.feature_aggregation_ignore_no_data,
                    )
                    .boxed(),
                    TemporalAggregationMethod::First | TemporalAggregationMethod::Mean => {
                        RasterVectorAggregateJoinProcessor::new(
                            polygons,
                            self.result_descriptor.clone(),
                            raster_inputs,
                            self.state.feature_aggregation,
                            self.state.feature_aggregation_ignore_no_data,
                            self.state.temporal_aggregation,
                            self.state.temporal_aggregation_ignore_no_data,
                        )
                        .boxed()
                    }
                })
            }
            TypedVectorQueryProcessor::MultiLineString(_) => return Err(Error::NotYetImplemented),
        })
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        RasterVectorJoin::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }
}

pub fn create_feature_aggregator<P: Pixel>(
    number_of_features: usize,
    aggregation: FeatureAggregationMethod,
    ignore_no_data: bool,
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
            | RasterDataType::I64 => {
                FirstValueIntAggregator::new(number_of_features, ignore_no_data).into_typed()
            }
            RasterDataType::F32 | RasterDataType::F64 => {
                FirstValueFloatAggregator::new(number_of_features, ignore_no_data).into_typed()
            }
        },
        FeatureAggregationMethod::Mean => {
            MeanValueAggregator::new(number_of_features, ignore_no_data).into_typed()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    use crate::engine::{
        ChunkByteSize, MockExecutionContext, QueryProcessor, RasterBandDescriptor,
        RasterBandDescriptors, RasterOperator, RasterResultDescriptor, SpatialGridDescriptor,
    };
    use crate::mock::{MockFeatureCollectionSource, MockRasterSource, MockRasterSourceParams};
    use crate::source::{GdalSource, GdalSourceParameters};
    use crate::util::gdal::add_ndvi_dataset;
    use futures::StreamExt;
    use geoengine_datatypes::collections::{FeatureCollectionInfos, MultiPointCollection};
    use geoengine_datatypes::dataset::NamedData;
    use geoengine_datatypes::primitives::{
        BoundingBox2D, ColumnSelection, DataRef, DateTime, FeatureDataRef, MultiPoint,
        TimeInterval, VectorQueryRectangle,
    };
    use geoengine_datatypes::primitives::{CacheHint, Measurement};
    use geoengine_datatypes::raster::{GeoTransform, GridBoundingBox2D, RasterTile2D};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::{gdal::hide_gdal_errors, test::TestDefault};
    use serde_json::json;

    #[test]
    fn serialization() {
        let raster_vector_join = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: ColumnNames::Names(vec!["foo".to_string(), "bar".to_string()]),
                feature_aggregation: FeatureAggregationMethod::First,
                feature_aggregation_ignore_no_data: false,
                temporal_aggregation: TemporalAggregationMethod::Mean,
                temporal_aggregation_ignore_no_data: false,
            },
            sources: SingleVectorMultipleRasterSources {
                vector: MockFeatureCollectionSource::<MultiPoint>::multiple(vec![]).boxed(),
                rasters: vec![],
            },
        };

        let serialized = json!({
            "type": "RasterVectorJoin",
            "params": {
                "names": {
                    "type": "names",
                    "values": ["foo", "bar"],
                },
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

    fn ndvi_source(name: NamedData) -> Box<dyn RasterOperator> {
        let gdal_source = GdalSource {
            params: GdalSourceParameters::new(name),
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
                CacheHint::default(),
            )
            .unwrap(),
        )
        .boxed();

        let mut exe_ctc = MockExecutionContext::test_default();
        let ndvi_id = add_ndvi_dataset(&mut exe_ctc);

        let operator = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: ColumnNames::Names(vec!["ndvi".to_owned()]),
                feature_aggregation: FeatureAggregationMethod::First,
                feature_aggregation_ignore_no_data: false,
                temporal_aggregation: TemporalAggregationMethod::First,
                temporal_aggregation_ignore_no_data: false,
            },
            sources: SingleVectorMultipleRasterSources {
                vector: point_source,
                rasters: vec![ndvi_source(ndvi_id.clone())],
            },
        };

        let operator = operator
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctc)
            .await
            .unwrap();

        let query_processor = operator.query_processor().unwrap().multi_point().unwrap();

        let result = query_processor
            .query(
                VectorQueryRectangle::new(
                    BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    TimeInterval::default(),
                    ColumnSelection::all(),
                ),
                &exe_ctc.mock_query_context(ChunkByteSize::MIN),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let FeatureDataRef::Int(data) = result[0].data("ndvi").unwrap() else {
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
                        DateTime::new_utc(2014, 1, 1, 0, 0, 0),
                        DateTime::new_utc(2014, 3, 1, 0, 0, 0),
                    )
                    .unwrap();
                    4
                ],
                Default::default(),
                CacheHint::default(),
            )
            .unwrap(),
        )
        .boxed();

        let mut exe_ctc = MockExecutionContext::test_default();
        let ndvi_id = add_ndvi_dataset(&mut exe_ctc);

        let operator = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: ColumnNames::Names(vec!["ndvi".to_owned()]),
                feature_aggregation: FeatureAggregationMethod::First,
                feature_aggregation_ignore_no_data: false,
                temporal_aggregation: TemporalAggregationMethod::Mean,
                temporal_aggregation_ignore_no_data: false,
            },
            sources: SingleVectorMultipleRasterSources {
                vector: point_source,
                rasters: vec![ndvi_source(ndvi_id.clone())],
            },
        };

        let operator = operator
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctc)
            .await
            .unwrap();

        let query_processor = operator.query_processor().unwrap().multi_point().unwrap();

        let result = query_processor
            .query(
                VectorQueryRectangle::new(
                    BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    TimeInterval::default(),
                    ColumnSelection::all(),
                ),
                &exe_ctc.mock_query_context(ChunkByteSize::MIN),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let FeatureDataRef::Float(data) = result[0].data("ndvi").unwrap() else {
            unreachable!();
        };

        // these values are taken from loading the tiff in QGIS
        assert_eq!(
            data.as_ref(),
            &[
                f64::midpoint(54., 52.),
                f64::midpoint(55., 55.),
                f64::midpoint(51., 50.),
                f64::midpoint(55., 53.),
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
                CacheHint::default(),
            )
            .unwrap(),
        )
        .boxed();

        let mut exe_ctc = MockExecutionContext::test_default();
        let ndvi_id = add_ndvi_dataset(&mut exe_ctc);

        let operator = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: ColumnNames::Names(vec!["ndvi".to_owned()]),
                feature_aggregation: FeatureAggregationMethod::First,
                feature_aggregation_ignore_no_data: false,
                temporal_aggregation: TemporalAggregationMethod::Mean,
                temporal_aggregation_ignore_no_data: false,
            },
            sources: SingleVectorMultipleRasterSources {
                vector: point_source,
                rasters: vec![ndvi_source(ndvi_id.clone())],
            },
        };

        let operator = operator
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctc)
            .await
            .unwrap();

        let query_processor = operator.query_processor().unwrap().multi_point().unwrap();

        let result = query_processor
            .query(
                VectorQueryRectangle::new(
                    BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    TimeInterval::default(),
                    ColumnSelection::all(),
                ),
                &exe_ctc.mock_query_context(ChunkByteSize::MIN),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let FeatureDataRef::Float(data) = result[0].data("ndvi").unwrap() else {
            unreachable!();
        };

        assert_eq!(data.as_ref(), &[0., 0., 0., 0.]);

        assert_eq!(data.nulls(), vec![true, true, true, true]);
    }

    #[tokio::test]
    async fn it_checks_sref() {
        let point_source = MockFeatureCollectionSource::with_collections_and_sref(
            vec![
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
                    CacheHint::default(),
                )
                .unwrap(),
            ],
            SpatialReference::from_str("EPSG:3857").unwrap(),
        )
        .boxed();

        let mut exe_ctc = MockExecutionContext::test_default();
        let ndvi_id = add_ndvi_dataset(&mut exe_ctc);

        let operator = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: ColumnNames::Default,
                feature_aggregation: FeatureAggregationMethod::First,
                feature_aggregation_ignore_no_data: false,
                temporal_aggregation: TemporalAggregationMethod::Mean,
                temporal_aggregation_ignore_no_data: false,
            },
            sources: SingleVectorMultipleRasterSources {
                vector: point_source,
                rasters: vec![ndvi_source(ndvi_id.clone())],
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctc)
        .await;

        assert!(matches!(
            operator,
            Err(Error::InvalidSpatialReference {
                expected: _,
                found: _,
            })
        ));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_includes_bands_in_result_descriptor() {
        let point_source = MockFeatureCollectionSource::with_collections_and_sref(
            vec![
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
                    CacheHint::default(),
                )
                .unwrap(),
            ],
            SpatialReference::from_str("EPSG:4326").unwrap(),
        )
        .boxed();

        let raster_source = MockRasterSource {
            params: MockRasterSourceParams {
                data: Vec::<RasterTile2D<u8>>::new(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        GeoTransform::test_default(),
                        GridBoundingBox2D::new_min_max(0, 0, 2, 2).unwrap(),
                    ),
                    bands: RasterBandDescriptors::new(vec![
                        RasterBandDescriptor::new_unitless("band_0".into()),
                        RasterBandDescriptor::new_unitless("band_1".into()),
                    ])
                    .unwrap(),
                },
            },
        }
        .boxed();

        let raster_source2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: Vec::<RasterTile2D<u8>>::new(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        GeoTransform::test_default(),
                        GridBoundingBox2D::new_min_max(0, 0, 2, 2).unwrap(),
                    ),
                    bands: RasterBandDescriptors::new(vec![
                        RasterBandDescriptor::new_unitless("band_0".into()),
                        RasterBandDescriptor::new_unitless("band_1".into()),
                        RasterBandDescriptor::new_unitless("band_2".into()),
                    ])
                    .unwrap(),
                },
            },
        }
        .boxed();

        let exe_ctc = MockExecutionContext::test_default();

        let join = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: ColumnNames::Default,
                feature_aggregation: FeatureAggregationMethod::First,
                feature_aggregation_ignore_no_data: false,
                temporal_aggregation: TemporalAggregationMethod::None,
                temporal_aggregation_ignore_no_data: false,
            },
            sources: SingleVectorMultipleRasterSources {
                vector: point_source,
                rasters: vec![raster_source, raster_source2],
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctc)
        .await
        .unwrap();

        assert_eq!(
            join.result_descriptor(),
            &VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: [
                    (
                        "band_0".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Int,
                            measurement: Measurement::Unitless
                        }
                    ),
                    (
                        "band_1".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Int,
                            measurement: Measurement::Unitless
                        }
                    ),
                    (
                        "band_0 (1)".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Int,
                            measurement: Measurement::Unitless
                        }
                    ),
                    (
                        "band_1 (1)".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Int,
                            measurement: Measurement::Unitless
                        }
                    ),
                    (
                        "band_2".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Int,
                            measurement: Measurement::Unitless
                        }
                    )
                ]
                .into_iter()
                .collect(),
                time: None,
                bbox: None
            }
        );
    }
}
