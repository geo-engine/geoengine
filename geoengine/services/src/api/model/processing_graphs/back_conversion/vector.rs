use crate::api::model::{
    datatypes::VectorDataType,
    processing_graphs::{
        AttributeFilter, ColumnRangeFilter, ColumnRangeFilterParameters, LineSimplification,
        LineSimplificationAlgorithm, LineSimplificationParameters, MockPointSource,
        MockPointSourceParameters, OgrSource, OgrSourceParameters, PointInPolygonFilter,
        PointInPolygonFilterParameters, PointInPolygonFilterSource, RasterVectorJoin,
        RasterVectorJoinParameters, Reprojection, ReprojectionParameters, TimeProjection,
        TimeProjectionParameters, VectorExpression, VectorExpressionParameters, VectorJoin,
        VectorJoinParameters, VectorJoinSources, VectorOperator, VisualPointClustering,
        VisualPointClusteringParameters,
        back_conversion::downcast_runtime_operator,
        parameters::OutputColumn as ApiOutputColumn,
        processing::{
            AttributeAggregateDef, AttributeAggregateType, VectorJoinType,
            VectorJoinTypeEquiGeoToData,
        },
    },
};
use geoengine_datatypes::collections::GeoVectorDataType;
use geoengine_operators::{
    engine::{
        OperatorName, RasterOperator as OperatorsRasterOperator,
        VectorOperator as OperatorsVectorOperator,
    },
    mock::MockPointSource as OperatorsMockPointSource,
    processing::{
        AttributeAggregateType as OperatorsAttributeAggregateType,
        ColumnRangeFilter as OperatorsColumnRangeFilter,
        LineSimplification as OperatorsLineSimplification,
        LineSimplificationAlgorithm as OperatorsLineSimplificationAlgorithm,
        OutputColumn as OperatorsOutputColumn,
        PointInPolygonFilter as OperatorsPointInPolygonFilter,
        RasterVectorJoin as OperatorsRasterVectorJoin, Reprojection as OperatorsReprojection,
        TimeProjection as OperatorsTimeProjection, VectorExpression as OperatorsVectorExpression,
        VectorJoin as OperatorsVectorJoin, VectorJoinType as OperatorsVectorJoinType,
        VisualPointClustering as OperatorsVisualPointClustering,
    },
    source::OgrSource as OperatorsOgrSource,
    util::input::RasterOrVectorOperator,
};

macro_rules! convert {
    ($type_name:expr, $operator:expr, $($runtime:ty => $variant:ident),+ $(,)?) => {{
        match $type_name {
            $(
                <$runtime>::TYPE_NAME => {
                    let op = downcast_runtime_vector_operator::<$runtime>($operator, $type_name)?;
                    return Ok(VectorOperator::$variant($variant::try_from(op)?));
                }
            )+
            _ => anyhow::bail!(
                "cannot convert runtime vector operator {type_name} to ProcessingGraph",
                type_name = $type_name
            ),
        }
    }};
}

/// Converts a `&dyn VectorOperator` vector operator from `operators` into an API vector operator.
pub fn vector_operator_from_runtime(
    operator: &dyn OperatorsVectorOperator,
) -> anyhow::Result<VectorOperator> {
    let type_name = operator.typetag_name();
    convert!(type_name, operator,
        OperatorsColumnRangeFilter => ColumnRangeFilter,
        OperatorsLineSimplification => LineSimplification,
        OperatorsMockPointSource => MockPointSource,
        OperatorsOgrSource => OgrSource,
        OperatorsPointInPolygonFilter => PointInPolygonFilter,
        OperatorsRasterVectorJoin => RasterVectorJoin,
        OperatorsReprojection => Reprojection,
        OperatorsTimeProjection => TimeProjection,
        OperatorsVectorExpression => VectorExpression,
        OperatorsVectorJoin => VectorJoin,
        OperatorsVisualPointClustering => VisualPointClustering,
    );
}

fn downcast_runtime_vector_operator<'a, T: 'static>(
    operator: &'a dyn geoengine_datatypes::util::AsAny,
    type_name: &'a str,
) -> anyhow::Result<&'a T> {
    downcast_runtime_operator::<T>("vector", operator, type_name)
}

fn raster_operator_from_runtime(
    operator: &dyn OperatorsRasterOperator,
) -> anyhow::Result<crate::api::model::processing_graphs::RasterOperator> {
    crate::api::model::processing_graphs::back_conversion::raster::raster_operator_from_runtime(
        operator,
    )
}

impl TryFrom<&OperatorsColumnRangeFilter> for ColumnRangeFilter {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsColumnRangeFilter) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: ColumnRangeFilterParameters {
                column: value.params.column.clone(),
                ranges: value
                    .params
                    .ranges
                    .iter()
                    .cloned()
                    .map(Into::into)
                    .collect(),
                keep_nulls: value.params.keep_nulls,
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleVectorSource {
                vector: vector_operator_from_runtime(value.sources.vector.as_ref())?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsLineSimplification> for LineSimplification {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsLineSimplification) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: LineSimplificationParameters {
                algorithm: match value.params.algorithm {
                    OperatorsLineSimplificationAlgorithm::DouglasPeucker => {
                        LineSimplificationAlgorithm::DouglasPeucker
                    }
                    OperatorsLineSimplificationAlgorithm::Visvalingam => {
                        LineSimplificationAlgorithm::Visvalingam
                    }
                },
                epsilon: value.params.epsilon,
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleVectorSource {
                vector: vector_operator_from_runtime(value.sources.vector.as_ref())?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsMockPointSource> for MockPointSource {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsMockPointSource) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: MockPointSourceParameters {
                points: value
                    .params
                    .points
                    .iter()
                    .copied()
                    .map(Into::into)
                    .collect(),
                spatial_bounds: value.params.spatial_bounds.clone().into(),
            },
        })
    }
}

impl TryFrom<&OperatorsOgrSource> for OgrSource {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsOgrSource) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: OgrSourceParameters {
                data: value.params.data.clone().into(),
                attribute_projection: value.params.attribute_projection.clone(),
                attribute_filters: value.params.attribute_filters.clone().map(|filters| {
                    filters
                        .into_iter()
                        .map(|filter| AttributeFilter {
                            attribute: filter.attribute.clone(),
                            ranges: filter.ranges.into_iter().map(Into::into).collect(),
                            keep_nulls: filter.keep_nulls,
                        })
                        .collect()
                }),
            },
        })
    }
}

impl TryFrom<&OperatorsPointInPolygonFilter> for PointInPolygonFilter {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsPointInPolygonFilter) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: PointInPolygonFilterParameters {},
            sources: PointInPolygonFilterSource {
                points: Box::new(vector_operator_from_runtime(value.sources.points.as_ref())?),
                polygons: Box::new(vector_operator_from_runtime(
                    value.sources.polygons.as_ref(),
                )?),
            },
        })
    }
}

impl TryFrom<&OperatorsRasterVectorJoin> for RasterVectorJoin {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsRasterVectorJoin) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: RasterVectorJoinParameters {
                names: value.params.names.clone().into(),
                feature_aggregation: value.params.feature_aggregation.into(),
                feature_aggregation_ignore_no_data: value.params.feature_aggregation_ignore_no_data,
                temporal_aggregation: value.params.temporal_aggregation.into(),
                temporal_aggregation_ignore_no_data: value.params.temporal_aggregation_ignore_no_data,
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleVectorMultipleRasterSources {
                vector: vector_operator_from_runtime(value.sources.vector.as_ref())?,
                rasters: value
                    .sources
                    .rasters
                    .iter()
                    .map(|raster| raster_operator_from_runtime(raster.as_ref()))
                    .collect::<Result<Vec<_>, _>>()?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsReprojection> for Reprojection {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsReprojection) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: ReprojectionParameters {
                target_spatial_reference: value.params.target_spatial_reference.into(),
                derive_out_spec: match value.params.derive_out_spec {
                    geoengine_operators::processing::DeriveOutRasterSpecsSource::DataBounds => {
                        crate::api::model::processing_graphs::DeriveOutRasterSpecsSource::DataBounds
                    }
                    geoengine_operators::processing::DeriveOutRasterSpecsSource::ProjectionBounds => {
                        crate::api::model::processing_graphs::DeriveOutRasterSpecsSource::ProjectionBounds
                    }
                },
            },
            sources: match &value.sources.source {
                RasterOrVectorOperator::Raster(raster) => {
                    crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorSource {
                        source: crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorOperator::Raster(
                            raster_operator_from_runtime(raster.as_ref())?,
                        ),
                    }
                }
                RasterOrVectorOperator::Vector(vector) => {
                    crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorSource {
                        source: crate::api::model::processing_graphs::source_parameters::SingleRasterOrVectorOperator::Vector(
                            vector_operator_from_runtime(vector.as_ref())?,
                        ),
                    }
                }
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsTimeProjection> for TimeProjection {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsTimeProjection) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: TimeProjectionParameters {
                step: value.params.step.into(),
                step_reference: value.params.step_reference.map(Into::into),
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleVectorSource {
                vector: vector_operator_from_runtime(value.sources.vector.as_ref())?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsVectorExpression> for VectorExpression {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsVectorExpression) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: VectorExpressionParameters {
                input_columns: value.params.input_columns.clone(),
                expression: value.params.expression.clone(),
                output_column: match &value.params.output_column {
                    OperatorsOutputColumn::Geometry(geo_vector_data_type) => {
                        ApiOutputColumn::Geometry(match geo_vector_data_type {
                            GeoVectorDataType::MultiPoint => VectorDataType::MultiPoint,
                            GeoVectorDataType::MultiLineString => VectorDataType::MultiLineString,
                            GeoVectorDataType::MultiPolygon => VectorDataType::MultiPolygon,
                        })
                    }
                    OperatorsOutputColumn::Column(column_name) => {
                        ApiOutputColumn::Column(column_name.clone())
                    }
                },
                geometry_column_name: value.params.geometry_column_name.clone(),
                output_measurement: value.params.output_measurement.clone().into(),
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleVectorSource {
                vector: vector_operator_from_runtime(value.sources.vector.as_ref())?,
            }
            .into(),
        })
    }
}

impl TryFrom<&OperatorsVectorJoin> for VectorJoin {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsVectorJoin) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: VectorJoinParameters {
                join_type: match &value.params.join_type {
                    OperatorsVectorJoinType::EquiGeoToData {
                        left_column,
                        right_column,
                        right_column_suffix,
                    } => VectorJoinType::EquiGeoToData(VectorJoinTypeEquiGeoToData {
                        r#type: Default::default(),
                        left_column: left_column.clone(),
                        right_column: right_column.clone(),
                        right_column_suffix: right_column_suffix.clone(),
                    }),
                },
            },
            sources: Box::new(VectorJoinSources {
                left: Box::new(vector_operator_from_runtime(value.sources.left.as_ref())?),
                right: Box::new(vector_operator_from_runtime(value.sources.right.as_ref())?),
            }),
        })
    }
}

impl TryFrom<&OperatorsVisualPointClustering> for VisualPointClustering {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorsVisualPointClustering) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: Default::default(),
            params: VisualPointClusteringParameters {
                min_radius_px: value.params.min_radius_px,
                delta_px: value.params.delta_px,
                resolution: value.params.resolution,
                radius_column: value.params.radius_column.clone(),
                count_column: value.params.count_column.clone(),
                column_aggregates: value
                    .params
                    .column_aggregates
                    .iter()
                    .map(|(key, aggregate)| {
                        (
                            key.clone(),
                            AttributeAggregateDef {
                                column_name: aggregate.column_name.clone(),
                                aggregate_type: match aggregate.aggregate_type {
                                    OperatorsAttributeAggregateType::MeanNumber => {
                                        AttributeAggregateType::MeanNumber
                                    }
                                    OperatorsAttributeAggregateType::StringSample => {
                                        AttributeAggregateType::StringSample
                                    }
                                    OperatorsAttributeAggregateType::Null => {
                                        AttributeAggregateType::Null
                                    }
                                },
                                measurement: aggregate.measurement.clone().map(Into::into),
                            },
                        )
                    })
                    .collect(),
            },
            sources: crate::api::model::processing_graphs::source_parameters::SingleVectorSource {
                vector: vector_operator_from_runtime(value.sources.vector.as_ref())?,
            }
            .into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::{dataset::NamedData, primitives::Measurement};
    use geoengine_operators::{
        engine::{
            RasterOperator as OperatorsRasterOperator, SingleRasterOrVectorSource,
            SingleVectorMultipleRasterSources, SingleVectorSource,
            VectorOperator as OperatorsVectorOperator,
        },
        mock::{MockPointSource, MockPointSourceParams, SpatialBoundsDerive},
        processing::{
            AttributeAggregateType, ColumnRangeFilter as OperatorsColumnRangeFilter,
            ColumnRangeFilterParams, LineSimplification as OperatorsLineSimplification,
            LineSimplificationAlgorithm, LineSimplificationParams,
            PointInPolygonFilter as OperatorsPointInPolygonFilter, PointInPolygonFilterParams,
            PointInPolygonFilterSource, RasterVectorJoin as OperatorsRasterVectorJoin,
            RasterVectorJoinParams, Reprojection as OperatorsReprojection, ReprojectionParams,
            TimeProjection as OperatorsTimeProjection, TimeProjectionParams,
            VectorExpression as OperatorsVectorExpression, VectorExpressionParams,
            VectorJoin as OperatorsVectorJoin, VectorJoinParams, VectorJoinSources, VectorJoinType,
            VisualPointClustering as OperatorsVisualPointClustering, VisualPointClusteringParams,
        },
        source::{
            GdalSource as OperatorsGdalSource,
            GdalSourceParameters as OperatorsGdalSourceParameters,
        },
    };

    fn raster_source() -> Box<dyn OperatorsRasterOperator> {
        Box::new(OperatorsGdalSource {
            params: OperatorsGdalSourceParameters::new(NamedData::with_system_name("test-raster")),
        })
    }

    fn vector_source() -> Box<dyn geoengine_operators::engine::VectorOperator> {
        Box::new(MockPointSource {
            params: MockPointSourceParams {
                points: vec![(1., 2.).into()],
                spatial_bounds: SpatialBoundsDerive::None,
            },
        })
    }

    #[test]
    #[allow(clippy::too_many_lines, clippy::type_complexity)]
    fn it_converts_runtime_vector_operators_to_processing_graph() {
        let cases: Vec<(Box<dyn OperatorsVectorOperator>, fn(VectorOperator) -> bool)> = vec![
            (
                Box::new(OperatorsColumnRangeFilter {
                    params: ColumnRangeFilterParams {
                        column: "value".to_string(),
                        ranges: vec![geoengine_operators::util::input::StringOrNumberRange::Float(
                            0.0..=1.0,
                        )],
                        keep_nulls: true,
                    },
                    sources: SingleVectorSource { vector: vector_source() },
                }),
                |graph| matches!(graph, VectorOperator::ColumnRangeFilter(_)),
            ),
            (
                Box::new(OperatorsLineSimplification {
                    params: LineSimplificationParams {
                        algorithm: LineSimplificationAlgorithm::DouglasPeucker,
                        epsilon: 1.0,
                    },
                    sources: SingleVectorSource { vector: vector_source() },
                }),
                |graph| matches!(graph, VectorOperator::LineSimplification(_)),
            ),
            (
                Box::new(MockPointSource {
                    params: MockPointSourceParams {
                        points: vec![(3., 4.).into()],
                        spatial_bounds: SpatialBoundsDerive::None,
                    },
                }),
                |graph| matches!(graph, VectorOperator::MockPointSource(_)),
            ),
            (
                Box::new(geoengine_operators::source::OgrSource {
                    params: geoengine_operators::source::OgrSourceParameters {
                        data: NamedData::with_system_name("test-vector"),
                        attribute_projection: Some(vec!["value".to_string()]),
                        attribute_filters: None,
                    },
                }),
                |graph| matches!(graph, VectorOperator::OgrSource(_)),
            ),
            (
                Box::new(OperatorsPointInPolygonFilter {
                    params: PointInPolygonFilterParams {},
                    sources: PointInPolygonFilterSource {
                        points: vector_source(),
                        polygons: vector_source(),
                    },
                }),
                |graph| matches!(graph, VectorOperator::PointInPolygonFilter(_)),
            ),
            (
                Box::new(OperatorsRasterVectorJoin {
                    params: RasterVectorJoinParams {
                        names: geoengine_operators::processing::ColumnNames::Default,
                        feature_aggregation: geoengine_operators::processing::FeatureAggregationMethod::First,
                        feature_aggregation_ignore_no_data: false,
                        temporal_aggregation: geoengine_operators::processing::TemporalAggregationMethod::None,
                        temporal_aggregation_ignore_no_data: false,
                    },
                    sources: SingleVectorMultipleRasterSources {
                        vector: vector_source(),
                        rasters: vec![raster_source()],
                    },
                }),
                |graph| matches!(graph, VectorOperator::RasterVectorJoin(_)),
            ),
            (
                Box::new(OperatorsReprojection {
                    params: ReprojectionParams {
                        target_spatial_reference: geoengine_datatypes::spatial_reference::SpatialReference::epsg_4326(),
                        derive_out_spec: geoengine_operators::processing::DeriveOutRasterSpecsSource::ProjectionBounds,
                    },
                    sources: SingleRasterOrVectorSource {
                        source: RasterOrVectorOperator::Vector(vector_source()),
                    },
                }),
                |graph| matches!(graph, VectorOperator::Reprojection(_)),
            ),
            (
                Box::new(OperatorsTimeProjection {
                    params: TimeProjectionParams {
                        step: geoengine_datatypes::primitives::TimeStep::new(
                            geoengine_datatypes::primitives::TimeGranularity::Months,
                            1,
                        )
                        .expect("valid monthly time step"),
                        step_reference: None,
                    },
                    sources: SingleVectorSource { vector: vector_source() },
                }),
                |graph| matches!(graph, VectorOperator::TimeProjection(_)),
            ),
            (
                Box::new(OperatorsVectorExpression {
                    params: VectorExpressionParams {
                        input_columns: vec!["value".to_string()],
                        expression: "value".to_string(),
                        output_column: geoengine_operators::processing::OutputColumn::Column(
                            "result".to_string(),
                        ),
                        geometry_column_name: "geom".to_string(),
                        output_measurement: Measurement::Unitless,
                    },
                    sources: SingleVectorSource { vector: vector_source() },
                }),
                |graph| matches!(graph, VectorOperator::VectorExpression(_)),
            ),
            (
                Box::new(OperatorsVectorJoin {
                    params: VectorJoinParams {
                        join_type: VectorJoinType::EquiGeoToData {
                            left_column: "id".to_string(),
                            right_column: "id".to_string(),
                            right_column_suffix: Some("_other".to_string()),
                        },
                    },
                    sources: VectorJoinSources {
                        left: vector_source(),
                        right: vector_source(),
                    },
                }),
                |graph| matches!(graph, VectorOperator::VectorJoin(_)),
            ),
            (
                Box::new(OperatorsVisualPointClustering {
                    params: VisualPointClusteringParams {
                        min_radius_px: 8.0,
                        delta_px: 1.0,
                        resolution: 0.5,
                        radius_column: "__radius".to_string(),
                        count_column: "__count".to_string(),
                        column_aggregates: std::collections::HashMap::from([(
                            "mean_value".to_string(),
                            geoengine_operators::processing::AttributeAggregateDef {
                                column_name: "value".to_string(),
                                aggregate_type: AttributeAggregateType::MeanNumber,
                                measurement: Some(Measurement::Unitless),
                            },
                        )]),
                    },
                    sources: SingleVectorSource { vector: vector_source() },
                }),
                |graph| matches!(graph, VectorOperator::VisualPointClustering(_)),
            ),
        ];

        for (operator, assert_variant) in cases {
            let converted = vector_operator_from_runtime(operator.as_ref())
                .expect("vector conversion should work");
            assert!(
                assert_variant(converted),
                "runtime vector conversion failed for {}",
                operator.typetag_name()
            );
        }
    }
}
