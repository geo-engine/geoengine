use crate::engine::{
    ExecutionContext, InitializedOperator, InitializedOperatorImpl, InitializedPlotOperator,
    Operator, PlotOperator, PlotQueryProcessor, PlotResultDescriptor, QueryContext, QueryProcessor,
    QueryRectangle, TypedPlotQueryProcessor, TypedRasterQueryProcessor,
};
use crate::error;
use crate::error::Error;
use crate::string_token;
use crate::util::Result;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use geoengine_datatypes::plots::Plot;
use geoengine_datatypes::primitives::{FeatureDataType, Measurement};
use geoengine_datatypes::raster::RasterTile2D;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt};

/// A histogram plot about either a raster or a vector input.
///
/// For vector inputs, it calculates the histogram on one of its attributes.
///
pub type Histogram = Operator<HistogramParams>;

/// The parameter spec for `Histogram`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistogramParams {
    /// Name of the (numeric) attribute to compute the histogram on. Ignored for operation on rasters.
    column_name: Option<String>,
    /// The bounds (min/max) of the histogram.
    bounds: HistogramBounds,
    /// If the bounds are empty, it is derived from the square-root choice rule.
    buckets: Option<usize>,
}

string_token!(Data, "data");

/// Let the bounds either be computed or given.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HistogramBounds {
    Data(Data),
    Values { min: f64, max: f64 },
    // TODO: use bounds in measurement if they are available
}

#[typetag::serde]
impl PlotOperator for Histogram {
    fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<InitializedPlotOperator>> {
        ensure!(
            self.vector_sources.len() + self.raster_sources.len() == 1,
            error::InvalidNumberOfVectorInputs {
                expected: 1..2,
                found: self.vector_sources.len() + self.raster_sources.len()
            }
        );

        let vector_sources = self
            .vector_sources
            .into_iter()
            .map(|o| o.initialize(context))
            .collect::<Result<Vec<_>>>()?;
        if !vector_sources.is_empty() {
            let column_name =
                self.params
                    .column_name
                    .as_ref()
                    .context(error::InvalidOperatorSpec {
                        reason: "Histogram on vector input is missing `column_name` field"
                            .to_string(),
                    })?;

            let vector_result_descriptor = vector_sources[0].result_descriptor();

            match vector_result_descriptor.columns.get(column_name) {
                None => {
                    return Err(Error::ColumnDoesNotExist {
                        column: column_name.to_string(),
                    });
                }
                Some(FeatureDataType::Categorical) | Some(FeatureDataType::Text) => {
                    // TODO: incorporate categorical data
                    return Err(Error::InvalidOperatorSpec {
                        reason: format!("column `{}` must be numerical", column_name),
                    });
                }
                Some(FeatureDataType::Decimal) | Some(FeatureDataType::Number) => {
                    // okay
                }
            }
        }

        Ok(InitializedHistogram {
            params: self.params,
            result_descriptor: PlotResultDescriptor {},
            raster_sources: self
                .raster_sources
                .into_iter()
                .map(|o| o.initialize(context))
                .collect::<Result<Vec<_>>>()?,
            vector_sources,
            state: (),
        }
        .boxed())
    }
}

/// The initialization of `Histogram`
pub type InitializedHistogram = InitializedOperatorImpl<HistogramParams, PlotResultDescriptor, ()>;

impl InitializedOperator<PlotResultDescriptor, TypedPlotQueryProcessor> for InitializedHistogram {
    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        let (min, max) = if let HistogramBounds::Values { min, max } = self.params.bounds {
            (Some(min), Some(max))
        } else {
            (None, None)
        };
        let number_of_buckets = self.params.buckets;

        if self.vector_sources.is_empty() {
            let raster_source = &self.raster_sources[0];

            Ok(TypedPlotQueryProcessor::Json(
                HistogramQueryProcessor {
                    input: raster_source.query_processor()?,
                    measurement: raster_source.result_descriptor().measurement.clone(),
                    number_of_buckets,
                    min,
                    max,
                }
                .boxed(),
            ))
        } else {
            todo!()
        }
    }
}

/// A query processor that calculates the Histogram about its inputs.
pub struct HistogramQueryProcessor {
    input: TypedRasterQueryProcessor,
    measurement: Measurement,
    number_of_buckets: Option<usize>,
    min: Option<f64>,
    max: Option<f64>,
}

impl PlotQueryProcessor for HistogramQueryProcessor {
    type PlotType = serde_json::Value;

    fn plot_query<'a>(
        &'a self,
        query: QueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> BoxFuture<'a, Result<Self::PlotType>> {
        let histogram_metadata = self.preprocess();

        let query = call_on_generic_raster_processor!(&self.input, processor => {
            processor.query(query, ctx)
                     .map(move |r| r.map(RasterTile2D::convert::<f64>))
                     .boxed()
        });

        let histogram = geoengine_datatypes::plots::Histogram::builder(
            histogram_metadata.number_of_buckets,
            histogram_metadata.min,
            histogram_metadata.max,
            self.measurement.clone(),
        )
        .build()
        .map_err(Error::from);

        query
            .fold(histogram, |builder, tile| async move {
                let mut builder = builder?;
                let tile = tile?;

                builder.add_raster_data(&tile.grid_array.data, tile.grid_array.no_data_value);

                Ok(builder)
            })
            .map(|histogram| {
                histogram
                    .and_then(|histogram| histogram.to_vega_embeddable(false).map_err(Into::into))
                    .and_then(|histogram| serde_json::to_value(histogram).map_err(Into::into))
            })
            .boxed()
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
struct HistogramMetadata {
    pub number_of_buckets: usize,
    pub min: f64,
    pub max: f64,
}

impl HistogramQueryProcessor {
    fn preprocess(&self) -> HistogramMetadata {
        if let (Some(number_of_buckets), Some(min), Some(max)) =
            (self.number_of_buckets, self.min, self.max)
        {
            return HistogramMetadata {
                number_of_buckets,
                min,
                max,
            };
        }

        todo!("preprocess")
    }
}

// enum SliceProducer {
//     Raster(TypedRasterQueryProcessor),
//     Vector(TypedVectorQueryProcessor, String),
// }
//
// impl SliceProducer {
//     fn query<'a>(
//         &'a self,
//         query: QueryRectangle,
//         ctx: &'a dyn QueryContext,
//     ) -> BoxStream<'a, Result<Box<dyn AsRef<[f64]>>>> {
//         match self {
//             SliceProducer::Raster(generic_processor) => {
//                 call_on_generic_raster_processor!(generic_processor, processor => {
//                     Box::pin(processor.query(query, ctx)
//                              .map(move |r| r.map(|tile| Self::box_vec(tile.convert::<f64>().grid_array.data)))
//                              .boxed())
//                 })
//             }
//             SliceProducer::Vector(generic_processor, column_name) => match generic_processor {
//                 TypedVectorQueryProcessor::Data(processor) => {
//                     processor.query(query, ctx).map(|collection_result| {
//                         collection_result.and_then(|collection| {
//                             collection.data(column_name).map(|data| Box::new(data))
//                         })
//                     })
//                 }
//                 TypedVectorQueryProcessor::MultiPoint(_) => {
//                     todo!()
//                 }
//                 TypedVectorQueryProcessor::MultiLineString(_) => {
//                     todo!()
//                 }
//                 TypedVectorQueryProcessor::MultiPolygon(_) => {
//                     todo!()
//                 }
//             },
//         }
//     }
//
//     fn box_vec(vec: Vec<f64>) -> Box<dyn AsRef<[f64]>> {
//         Box::new(vec)
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::{
        MockExecutionContext, MockQueryContext, RasterOperator, RasterResultDescriptor,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use geoengine_datatypes::primitives::{BoundingBox2D, SpatialResolution, TimeInterval};
    use geoengine_datatypes::raster::{Grid2D, RasterDataType, TileInformation};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use serde_json::json;

    #[test]
    fn serialization() {
        let histogram = Histogram {
            params: HistogramParams {
                column_name: Some("foobar".to_string()),
                bounds: HistogramBounds::Values {
                    min: 5.0,
                    max: 10.0,
                },
                buckets: Some(15),
            },
            raster_sources: vec![],
            vector_sources: vec![],
        };

        let serialized = json!({
            "type": "Histogram",
            "params": {
                "column_name": "foobar",
                "bounds": {
                    "min": 5.0,
                    "max": 10.0,
                },
                "buckets": 15,
            },
            "raster_sources": [],
            "vector_sources": [],
        })
        .to_string();

        let deserialized: Histogram = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.params, histogram.params);
    }

    #[test]
    fn serialization_alt() {
        let histogram = Histogram {
            params: HistogramParams {
                column_name: None,
                bounds: HistogramBounds::Data(Default::default()),
                buckets: None,
            },
            raster_sources: vec![],
            vector_sources: vec![],
        };

        let serialized = json!({
            "type": "Histogram",
            "params": {
                "bounds": "data",
            },
            "raster_sources": [],
            "vector_sources": [],
        })
        .to_string();

        let deserialized: Histogram = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.params, histogram.params);
    }

    #[tokio::test]
    async fn simple_raster() {
        let raster_source = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![RasterTile2D::new_with_tile_info(
                    TimeInterval::default(),
                    TileInformation {
                        global_geo_transform: Default::default(),
                        global_tile_position: [0, 0].into(),
                        tile_size_in_pixels: [3, 2].into(),
                    },
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], None).unwrap(),
                )],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                },
            },
        }
        .boxed();

        let histogram = Histogram {
            params: HistogramParams {
                column_name: None,
                bounds: HistogramBounds::Values { min: 0.0, max: 8.0 },
                buckets: Some(3),
            },
            raster_sources: vec![raster_source],
            vector_sources: vec![],
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = histogram
            .boxed()
            .initialize(&execution_context)
            .unwrap()
            .query_processor()
            .unwrap()
            .json()
            .unwrap();

        let result = query_processor
            .plot_query(
                QueryRectangle {
                    bbox: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(0),
            )
            .await
            .unwrap();

        assert_eq!(
            result.to_string(),
            json!({
                "vega_string": "{\"$schema\":\"https://vega.github.io/schema/vega-lite/v4.17.0.json\",\"data\":{\"values\":[{\"v\":1,\"dim\":[3],\"data\":[0.0,2.6666666666666665,2.0]},{\"v\":1,\"dim\":[3],\"data\":[2.666666666666667,5.333333333333334,3.0]},{\"v\":1,\"dim\":[3],\"data\":[5.333333333333334,8.0,1.0]}]},\"encoding\":{\"x\":{\"bin\":\"binned\",\"field\":\"data.0\",\"title\":\"\",\"type\":\"quantitative\"},\"x2\":{\"field\":\"data.1\"},\"y\":{\"field\":\"data.2\",\"title\":\"Frequency\",\"type\":\"quantitative\"}},\"mark\":\"bar\",\"padding\":5.0}",
                "metadata": {
                    "selection_name": null
                }
            })
            .to_string()
        );
    }
}
