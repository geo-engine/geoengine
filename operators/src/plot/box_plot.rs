use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use geoengine_datatypes::collections::FeatureCollectionInfos;
use geoengine_datatypes::plots::{BoxPlotAttribute, Plot, PlotData};

use crate::engine::VectorQueryRectangle;
use crate::engine::{
    ExecutionContext, InitializedPlotOperator, InitializedVectorOperator, Operator, PlotOperator,
    PlotQueryProcessor, PlotResultDescriptor, QueryContext, TypedPlotQueryProcessor,
    TypedVectorQueryProcessor,
};
use crate::engine::{QueryProcessor, SingleVectorSource};
use crate::error::Error;
use crate::util::statistics::PSquareQuantileEstimator;
use crate::util::Result;

pub const BOXPLOT_OPERATOR_NAME: &str = "BoxPlot";
const EXACT_CALC_BOUND: usize = 10_000;

/// A box plot about vector data attribute values
pub type BoxPlot = Operator<BoxPlotParams, SingleVectorSource>;

/// The parameter spec for `BoxPlot`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BoxPlotParams {
    /// Name of the (numeric) attributes to compute the box plots on.
    #[serde(default)]
    pub column_names: Vec<String>,
    /// Whether to create an interactive output (`false` by default)
    #[serde(default)]
    pub interactive: bool,
}

#[typetag::serde]
#[async_trait]
impl PlotOperator for BoxPlot {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedPlotOperator>> {
        if self.params.column_names.is_empty() {
            return Err(Error::InvalidOperatorSpec {
                reason: "BoxPlot requires at least one numeric column ('column_names' parameter)."
                    .to_string(),
            });
        }

        let source = self.sources.vector.initialize(context).await?;
        for cn in &self.params.column_names {
            match source.result_descriptor().columns.get(cn.as_str()) {
                Some(column) if !column.is_numeric() => {
                    return Err(Error::InvalidOperatorSpec {
                        reason: format!("Column '{}' is not numeric.", cn),
                    });
                }
                Some(_) => {
                    // OK
                }
                None => {
                    return Err(Error::ColumnDoesNotExist {
                        column: cn.to_string(),
                    });
                }
            }
        }
        Ok(InitializedBoxPlot::new(PlotResultDescriptor {}, self.params, source).boxed())
    }
}

/// The initialization of `Histogram`
pub struct InitializedBoxPlot<Op> {
    result_descriptor: PlotResultDescriptor,
    column_names: Vec<String>,
    interactive: bool,
    source: Op,
}

impl<Op> InitializedBoxPlot<Op> {
    pub fn new(result_descriptor: PlotResultDescriptor, params: BoxPlotParams, source: Op) -> Self {
        Self {
            result_descriptor,
            column_names: params.column_names,
            interactive: params.interactive,
            source,
        }
    }
}
impl InitializedPlotOperator for InitializedBoxPlot<Box<dyn InitializedVectorOperator>> {
    fn result_descriptor(&self) -> &PlotResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        let processor = BoxPlotQueryProcessor {
            input: self.source.query_processor()?,
            column_names: self.column_names.clone(),
            interactive: self.interactive,
        };

        Ok(TypedPlotQueryProcessor::JsonVega(processor.boxed()))
    }
}

/// A query processor that calculates the boxplots about its vector inputs.
pub struct BoxPlotQueryProcessor {
    input: TypedVectorQueryProcessor,
    column_names: Vec<String>,
    interactive: bool,
}

#[async_trait]
impl PlotQueryProcessor for BoxPlotQueryProcessor {
    type OutputFormat = PlotData;

    fn plot_type(&self) -> &'static str {
        BOXPLOT_OPERATOR_NAME
    }

    async fn plot_query<'p>(
        &'p self,
        query: VectorQueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<Self::OutputFormat> {
        let mut accums: Vec<BoxPlotAccum> = self
            .column_names
            .iter()
            .map(|name| BoxPlotAccum::new(name.clone()))
            .collect();

        call_on_generic_vector_processor!(&self.input, processor => {
            let mut query = processor.query(query, ctx).await?;
            while let Some(collection) = query.next().await {
                let collection = collection?;

                for accum in &mut accums {
                    let feature_data = collection.data(&accum.name).expect("checked in param");
                    let iter = feature_data.float_options_iter().map(|o| match o {
                        Some(v) => v,
                        None => f64::NAN,
                    });
                    accum.update(iter)?;
                }
            }
        });

        let mut chart = geoengine_datatypes::plots::BoxPlot::new();
        for accum in &mut accums {
            if let Some(attrib) = accum.finish()? {
                chart.add_attribute(attrib)
            }
        }
        Ok(chart.to_vega_embeddable(self.interactive)?)
    }
}

struct BoxPlotAccum {
    name: String,
    values: Vec<f64>,
    estimator: Option<PSquareQuantileEstimator<f64>>,
}

impl BoxPlotAccum {
    fn new(name: String) -> BoxPlotAccum {
        BoxPlotAccum {
            name,
            values: Vec::new(),
            estimator: None,
        }
    }

    fn update(&mut self, mut values: impl Iterator<Item = f64>) -> crate::util::Result<()> {
        match self.estimator.as_mut() {
            None => {
                for v in &mut values {
                    if !v.is_nan() {
                        self.values.push(v);
                    }
                }
            }
            Some(est) => {
                for v in &mut values {
                    est.update(v);
                }
            }
        }

        // Turn into estimator
        if self.estimator.is_none() && self.values.len() >= EXACT_CALC_BOUND {
            let values = self.values.as_slice();
            self.estimator
                .replace(PSquareQuantileEstimator::new(0.5, values)?);
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<Option<geoengine_datatypes::plots::BoxPlotAttribute>> {
        match self.estimator.as_ref() {
            Some(est) => Ok(Some(BoxPlotAttribute::new(
                self.name.clone(),
                est.min(),
                est.max(),
                est.quantile_estimate(),
                est.marker2(),
                est.marker4(),
                false,
            )?)),
            None if self.values.is_empty() => Ok(None),
            None => {
                self.values
                    .sort_unstable_by(|a, b| a.partial_cmp(b).expect("Impossible"));
                let min = self.values[0];
                let max = self.values[self.values.len() - 1];
                let median = self.values[self.values.len() / 2];
                let q1 = self.values[(self.values.len() as f64 * 0.25) as usize];
                let q3 = self.values[(self.values.len() as f64 * 0.75) as usize];

                Ok(Some(BoxPlotAttribute::new(
                    self.name.clone(),
                    min,
                    max,
                    median,
                    q1,
                    q3,
                    true,
                )?))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use geoengine_datatypes::primitives::{
        BoundingBox2D, FeatureData, NoGeometry, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::{collections::DataCollection, primitives::MultiPoint};

    use crate::engine::{MockExecutionContext, MockQueryContext, VectorOperator};
    use crate::mock::MockFeatureCollectionSource;

    use super::*;

    #[test]
    fn serialization() {
        let histogram = BoxPlot {
            params: BoxPlotParams {
                column_names: vec!["foobar".to_string()],
                interactive: false,
            },
            sources: MockFeatureCollectionSource::<MultiPoint>::multiple(vec![])
                .boxed()
                .into(),
        };

        let serialized = json!({
            "type": "BoxPlot",
            "params": {
                "columnNames": ["foobar"],
                "interactive": false,
            },
            "sources": {
                "vector": {
                    "type": "MockFeatureCollectionSourceMultiPoint",
                    "params": {
                        "collections": []
                    }
                }
            }
        })
        .to_string();

        let deserialized: BoxPlot = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.params, histogram.params);
    }

    #[test]
    fn serialization_alt() {
        let histogram = BoxPlot {
            params: BoxPlotParams {
                column_names: vec![],
                interactive: false,
            },
            sources: MockFeatureCollectionSource::<MultiPoint>::multiple(vec![])
                .boxed()
                .into(),
        };

        let serialized = json!({
            "type": "BoxPlot",
            "params": {
            },
            "sources": {
                "vector": {
                    "type": "MockFeatureCollectionSourceMultiPoint",
                    "params": {
                        "collections": []
                    }
                }
            }
        })
        .to_string();

        let deserialized: BoxPlot = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.params, histogram.params);
    }

    #[tokio::test]
    async fn vector_data() {
        let vector_source = MockFeatureCollectionSource::multiple(vec![
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 8],
                &[
                    ("foo", FeatureData::Int(vec![1, 1, 2, 2, 3, 3, 4, 4])),
                    ("bar", FeatureData::Int(vec![1, 1, 2, 2, 3, 3, 4, 4])),
                ],
            )
            .unwrap(),
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 4],
                &[
                    ("foo", FeatureData::Int(vec![5, 6, 7, 8])),
                    ("bar", FeatureData::Int(vec![5, 6, 7, 8])),
                ],
            )
            .unwrap(),
        ])
        .boxed();

        let box_plot = BoxPlot {
            params: BoxPlotParams {
                column_names: vec!["foo".to_string(), "bar".to_string()],
                interactive: true,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = box_plot
            .boxed()
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(0),
            )
            .await
            .unwrap();

        let mut expected = geoengine_datatypes::plots::BoxPlot::new();
        expected.add_attribute(
            BoxPlotAttribute::new("foo".to_string(), 1.0, 8.0, 4.0, 2.0, 6.0, true).unwrap(),
        );
        expected.add_attribute(
            BoxPlotAttribute::new("bar".to_string(), 1.0, 8.0, 4.0, 2.0, 6.0, true).unwrap(),
        );

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn vector_data_with_nulls() {
        let vector_source = MockFeatureCollectionSource::single(
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 7],
                &[(
                    "foo",
                    FeatureData::NullableFloat(vec![
                        Some(1.),
                        Some(2.),
                        None,
                        Some(4.),
                        None,
                        Some(6.),
                        Some(7.),
                    ]),
                )],
            )
            .unwrap(),
        )
        .boxed();

        let box_plot = BoxPlot {
            params: BoxPlotParams {
                column_names: vec!["foo".to_string()],
                interactive: false,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = box_plot
            .boxed()
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(0),
            )
            .await
            .unwrap();

        let mut expected = geoengine_datatypes::plots::BoxPlot::new();
        expected.add_attribute(
            BoxPlotAttribute::new("foo".to_string(), 1.0, 7.0, 4.0, 2.0, 6.0, true).unwrap(),
        );

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn vector_data_text_column() {
        let vector_source = MockFeatureCollectionSource::single(
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 1],
                &[("foo", FeatureData::Text(vec!["test".to_string()]))],
            )
            .unwrap(),
        )
        .boxed();

        let box_plot = BoxPlot {
            params: BoxPlotParams {
                column_names: vec!["foo".to_string()],
                interactive: false,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::default();

        let init = box_plot.boxed().initialize(&execution_context).await;

        assert!(init.is_err());
    }

    #[tokio::test]
    async fn vector_data_missing_column() {
        let vector_source = MockFeatureCollectionSource::single(
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 1],
                &[("foo", FeatureData::Text(vec!["test".to_string()]))],
            )
            .unwrap(),
        )
        .boxed();

        let box_plot = BoxPlot {
            params: BoxPlotParams {
                column_names: vec![],
                interactive: false,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::default();

        let init = box_plot.boxed().initialize(&execution_context).await;

        assert!(init.is_err());
    }

    #[tokio::test]
    async fn vector_data_single_feature() {
        let vector_source =
            MockFeatureCollectionSource::multiple(vec![DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 1],
                &[("foo", FeatureData::Int(vec![1]))],
            )
            .unwrap()])
            .boxed();

        let box_plot = BoxPlot {
            params: BoxPlotParams {
                column_names: vec!["foo".to_string()],
                interactive: true,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = box_plot
            .boxed()
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(0),
            )
            .await
            .unwrap();

        let mut expected = geoengine_datatypes::plots::BoxPlot::new();
        expected.add_attribute(
            BoxPlotAttribute::new("foo".to_string(), 1.0, 1.0, 1.0, 1.0, 1.0, true).unwrap(),
        );

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn vector_data_empty() {
        let vector_source =
            MockFeatureCollectionSource::multiple(vec![DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[] as &[TimeInterval],
                &[("foo", FeatureData::Int(vec![]))],
            )
            .unwrap()])
            .boxed();

        let box_plot = BoxPlot {
            params: BoxPlotParams {
                column_names: vec!["foo".to_string()],
                interactive: true,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = box_plot
            .boxed()
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(0),
            )
            .await
            .unwrap();

        let expected = geoengine_datatypes::plots::BoxPlot::new();
        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn vector_data_estimator_switch() {
        let mut data = Vec::<i64>::with_capacity(2 * super::EXACT_CALC_BOUND);

        for i in 1..=data.capacity() as i64 {
            data.push(i)
        }

        let vector_source =
            MockFeatureCollectionSource::multiple(vec![DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 2 * super::EXACT_CALC_BOUND],
                &[("foo", FeatureData::Int(data))],
            )
            .unwrap()])
            .boxed();

        let box_plot = BoxPlot {
            params: BoxPlotParams {
                column_names: vec!["foo".to_string()],
                interactive: true,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = box_plot
            .boxed()
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(0),
            )
            .await
            .unwrap();

        let mut expected = geoengine_datatypes::plots::BoxPlot::new();
        expected.add_attribute(
            BoxPlotAttribute::new(
                "foo".to_string(),
                1.0,
                2.0 * super::EXACT_CALC_BOUND as f64,
                super::EXACT_CALC_BOUND as f64,
                0.5 * super::EXACT_CALC_BOUND as f64,
                1.5 * super::EXACT_CALC_BOUND as f64,
                false,
            )
            .unwrap(),
        );

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    // #[tokio::test]
    // async fn no_data_raster() {
    //     let no_data_value = Some(0);
    //     let histogram = Histogram {
    //         params: HistogramParams {
    //             column_name: None,
    //             bounds: HistogramBounds::Data(Data::default()),
    //             buckets: None,
    //             interactive: false,
    //         },
    //         sources: MockRasterSource {
    //             params: MockRasterSourceParams {
    //                 data: vec![RasterTile2D::new_with_tile_info(
    //                     TimeInterval::default(),
    //                     TileInformation {
    //                         global_geo_transform: TestDefault::test_default(),
    //                         global_tile_position: [0, 0].into(),
    //                         tile_size_in_pixels: [3, 2].into(),
    //                     },
    //                     Grid2D::new([3, 2].into(), vec![0, 0, 0, 0, 0, 0], no_data_value)
    //                         .unwrap()
    //                         .into(),
    //                 )],
    //                 result_descriptor: RasterResultDescriptor {
    //                     data_type: RasterDataType::U8,
    //                     spatial_reference: SpatialReference::epsg_4326().into(),
    //                     measurement: Measurement::Unitless,
    //                     no_data_value: no_data_value.map(AsPrimitive::as_),
    //                 },
    //             },
    //         }
    //         .boxed()
    //         .into(),
    //     };
    //
    //     let execution_context = MockExecutionContext::default();
    //
    //     let query_processor = histogram
    //         .boxed()
    //         .initialize(&execution_context)
    //         .await
    //         .unwrap()
    //         .query_processor()
    //         .unwrap()
    //         .json_vega()
    //         .unwrap();
    //
    //     let result = query_processor
    //         .plot_query(
    //             VectorQueryRectangle {
    //                 spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
    //                     .unwrap(),
    //                 time_interval: TimeInterval::default(),
    //                 spatial_resolution: SpatialResolution::one(),
    //             },
    //             &MockQueryContext::new(0),
    //         )
    //         .await
    //         .unwrap();
    //
    //     assert_eq!(
    //         result,
    //         geoengine_datatypes::plots::Histogram::builder(1, 0., 0., Measurement::Unitless)
    //             .build()
    //             .unwrap()
    //             .to_vega_embeddable(false)
    //             .unwrap()
    //     );
    // }
    //
    // #[tokio::test]
    // async fn single_value_raster_stream() {
    //     let execution_context = MockExecutionContext::default();
    //
    //     let no_data_value = None;
    //     let histogram = Histogram {
    //         params: HistogramParams {
    //             column_name: None,
    //             bounds: HistogramBounds::Data(Data::default()),
    //             buckets: None,
    //             interactive: false,
    //         },
    //         sources: MockRasterSource {
    //             params: MockRasterSourceParams {
    //                 data: vec![RasterTile2D::new_with_tile_info(
    //                     TimeInterval::default(),
    //                     TileInformation {
    //                         global_geo_transform: TestDefault::test_default(),
    //                         global_tile_position: [0, 0].into(),
    //                         tile_size_in_pixels: [3, 2].into(),
    //                     },
    //                     Grid2D::new([3, 2].into(), vec![4; 6], no_data_value)
    //                         .unwrap()
    //                         .into(),
    //                 )],
    //                 result_descriptor: RasterResultDescriptor {
    //                     data_type: RasterDataType::U8,
    //                     spatial_reference: SpatialReference::epsg_4326().into(),
    //                     measurement: Measurement::Unitless,
    //                     no_data_value: no_data_value.map(AsPrimitive::as_),
    //                 },
    //             },
    //         }
    //         .boxed()
    //         .into(),
    //     };
    //
    //     let query_processor = histogram
    //         .boxed()
    //         .initialize(&execution_context)
    //         .await
    //         .unwrap()
    //         .query_processor()
    //         .unwrap()
    //         .json_vega()
    //         .unwrap();
    //
    //     let result = query_processor
    //         .plot_query(
    //             VectorQueryRectangle {
    //                 spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
    //                     .unwrap(),
    //                 time_interval: TimeInterval::new_instant(
    //                     NaiveDate::from_ymd(2013, 12, 1).and_hms(12, 0, 0),
    //                 )
    //                 .unwrap(),
    //                 spatial_resolution: SpatialResolution::one(),
    //             },
    //             &MockQueryContext::default(),
    //         )
    //         .await
    //         .unwrap();
    //
    //     assert_eq!(
    //         result,
    //         geoengine_datatypes::plots::Histogram::builder(1, 4., 4., Measurement::Unitless)
    //             .counts(vec![6])
    //             .build()
    //             .unwrap()
    //             .to_vega_embeddable(false)
    //             .unwrap()
    //     );
    // }
}
