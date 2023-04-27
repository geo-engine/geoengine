use crate::engine::{
    ExecutionContext, InitializedPlotOperator, InitializedRasterOperator, InitializedSources,
    Operator, OperatorName, PlotOperator, PlotQueryProcessor, PlotResultDescriptor, QueryContext,
    QueryProcessor, RasterQueryProcessor, SingleRasterSource, TypedPlotQueryProcessor,
    WorkflowOperatorPath,
};
use crate::util::math::average_floor;
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::plots::{AreaLineChart, Plot, PlotData};
use geoengine_datatypes::primitives::{
    Coordinate2D, Measurement, PlotQueryRectangle, RasterQueryRectangle, TimeInstance, TimeInterval,
};
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const MEAN_RASTER_PIXEL_VALUES_OVER_TIME_NAME: &str = "Mean Raster Pixel Values over Time";

/// A plot that shows the mean values of rasters over time as an area plot.
pub type MeanRasterPixelValuesOverTime =
    Operator<MeanRasterPixelValuesOverTimeParams, SingleRasterSource>;

impl OperatorName for MeanRasterPixelValuesOverTime {
    const TYPE_NAME: &'static str = "MeanRasterPixelValuesOverTime";
}

/// The parameter spec for `MeanRasterPixelValuesOverTime`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MeanRasterPixelValuesOverTimeParams {
    /// Where should the x-axis (time) tick be positioned?
    /// At either time start, time end or in the center.
    pub time_position: MeanRasterPixelValuesOverTimePosition,

    /// Whether to fill the area under the curve.
    #[serde(default = "default_true")]
    pub area: bool,
}

const fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MeanRasterPixelValuesOverTimePosition {
    Start,
    Center,
    End,
}

#[typetag::serde]
#[async_trait]
impl PlotOperator for MeanRasterPixelValuesOverTime {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedPlotOperator>> {
        let initalized_sources = self.sources.initialize_sources(path, context).await?;
        let raster = initalized_sources.raster;

        let in_desc = raster.result_descriptor().clone();

        let initialized_operator = InitializedMeanRasterPixelValuesOverTime {
            result_descriptor: in_desc.into(),
            raster,
            state: self.params,
        };

        Ok(initialized_operator.boxed())
    }

    span_fn!(MeanRasterPixelValuesOverTime);
}

/// The initialization of `MeanRasterPixelValuesOverTime`
pub struct InitializedMeanRasterPixelValuesOverTime {
    result_descriptor: PlotResultDescriptor,
    raster: Box<dyn InitializedRasterOperator>,
    state: MeanRasterPixelValuesOverTimeParams,
}

impl InitializedPlotOperator for InitializedMeanRasterPixelValuesOverTime {
    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        let input_processor = self.raster.query_processor()?;
        let time_position = self.state.time_position;
        let measurement = self.raster.result_descriptor().measurement.clone();
        let draw_area = self.state.area;

        let processor = call_on_generic_raster_processor!(input_processor, raster => {
            MeanRasterPixelValuesOverTimeQueryProcessor { raster, time_position, measurement, draw_area }.boxed()
        });

        Ok(TypedPlotQueryProcessor::JsonVega(processor))
    }

    fn result_descriptor(&self) -> &PlotResultDescriptor {
        &self.result_descriptor
    }
}

/// A query processor that calculates the `TemporalRasterMeanPlot` about its input.
pub struct MeanRasterPixelValuesOverTimeQueryProcessor<P: Pixel> {
    raster: Box<dyn RasterQueryProcessor<RasterType = P>>,
    time_position: MeanRasterPixelValuesOverTimePosition,
    measurement: Measurement,
    draw_area: bool,
}

#[async_trait]
impl<P: Pixel> PlotQueryProcessor for MeanRasterPixelValuesOverTimeQueryProcessor<P> {
    type OutputFormat = PlotData;

    fn plot_type(&self) -> &'static str {
        MEAN_RASTER_PIXEL_VALUES_OVER_TIME_NAME
    }

    async fn plot_query<'a>(
        &'a self,
        query: PlotQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<Self::OutputFormat> {
        let raster_query_rect = RasterQueryRectangle::with_vector_query_and_grid_origin(
            query,
            Coordinate2D::default(), // FIXME: this is the default tiling specification origin. The actual origin is not known here. It should be derived from the input result descriptor!
        );

        let means = Self::calculate_means(
            self.raster.query(raster_query_rect, ctx).await?,
            self.time_position,
        )
        .await?;

        let plot = Self::generate_plot(means, self.measurement.clone(), self.draw_area)?;

        let plot_data = plot.to_vega_embeddable(false)?;

        Ok(plot_data)
    }
}

impl<P: Pixel> MeanRasterPixelValuesOverTimeQueryProcessor<P> {
    async fn calculate_means(
        mut tile_stream: BoxStream<'_, Result<RasterTile2D<P>>>,
        position: MeanRasterPixelValuesOverTimePosition,
    ) -> Result<BTreeMap<TimeInstance, MeanCalculator>> {
        let mut means: BTreeMap<TimeInstance, MeanCalculator> = BTreeMap::new();

        while let Some(tile) = tile_stream.next().await {
            let tile = tile?;

            match tile.grid_array {
                geoengine_datatypes::raster::GridOrEmpty::Grid(g) => {
                    let time = Self::time_interval_projection(tile.time, position);
                    let mean = means.entry(time).or_default();
                    mean.add(g.masked_element_deref_iterator());
                }
                geoengine_datatypes::raster::GridOrEmpty::Empty(_) => (),
            }
        }

        Ok(means)
    }

    #[inline]
    fn time_interval_projection(
        time_interval: TimeInterval,
        time_position: MeanRasterPixelValuesOverTimePosition,
    ) -> TimeInstance {
        match time_position {
            MeanRasterPixelValuesOverTimePosition::Start => time_interval.start(),
            MeanRasterPixelValuesOverTimePosition::Center => TimeInstance::from_millis_unchecked(
                average_floor(time_interval.start().inner(), time_interval.end().inner()),
            ),
            MeanRasterPixelValuesOverTimePosition::End => time_interval.end(),
        }
    }

    fn generate_plot(
        means: BTreeMap<TimeInstance, MeanCalculator>,
        measurement: Measurement,
        draw_area: bool,
    ) -> Result<AreaLineChart> {
        let mut timestamps = Vec::with_capacity(means.len());
        let mut values = Vec::with_capacity(means.len());

        for (timestamp, mean_calculator) in means {
            timestamps.push(timestamp);
            values.push(mean_calculator.mean());
        }

        AreaLineChart::new(timestamps, values, measurement, draw_area).map_err(Into::into)
    }
}

struct MeanCalculator {
    mean: f64,
    n: usize,
}

impl Default for MeanCalculator {
    fn default() -> Self {
        Self { mean: 0., n: 0 }
    }
}

impl MeanCalculator {
    #[inline]
    fn add<P: Pixel, I: Iterator<Item = Option<P>>>(&mut self, values: I) {
        values.flatten().for_each(|value| {
            self.add_single_value(value);
        });
    }

    #[inline]
    fn add_single_value<P: Pixel>(&mut self, value: P) {
        let value: f64 = value.as_();

        if value.is_nan() {
            return;
        }

        self.n += 1;
        let delta = value - self.mean;
        self.mean += delta / (self.n as f64);
    }

    #[inline]
    fn mean(&self) -> f64 {
        self.mean
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        engine::{
            ChunkByteSize, MockExecutionContext, MockQueryContext, RasterOperator,
            RasterResultDescriptor,
        },
        source::GdalSource,
    };
    use crate::{
        mock::{MockRasterSource, MockRasterSourceParams},
        source::GdalSourceParameters,
    };
    use geoengine_datatypes::{dataset::DatasetId, plots::PlotMetaData, primitives::DateTime};
    use geoengine_datatypes::{
        primitives::{BoundingBox2D, Measurement, SpatialResolution, TimeInterval},
        util::Identifier,
    };
    use geoengine_datatypes::{raster::TilingSpecification, spatial_reference::SpatialReference};
    use geoengine_datatypes::{
        raster::{Grid2D, RasterDataType, TileInformation},
        util::test::TestDefault,
    };
    use serde_json::{json, Value};

    #[test]
    fn serialization() {
        let temporal_raster_mean_plot = MeanRasterPixelValuesOverTime {
            params: MeanRasterPixelValuesOverTimeParams {
                time_position: MeanRasterPixelValuesOverTimePosition::Start,
                area: true,
            },
            sources: SingleRasterSource {
                raster: GdalSource {
                    params: GdalSourceParameters {
                        data: DatasetId::new().into(),
                    },
                }
                .boxed(),
            },
        };

        let serialized = json!({
            "type": "MeanRasterPixelValuesOverTime",
            "params": {
                "timePosition": "start",
                "area": true,
            },
            "sources": {
                "raster": {
                    "type": "GdalSource",
                    "params": {
                        "data": {
                            "type": "internal",
                            "datasetId": "a626c880-1c41-489b-9e19-9596d129859c"
                        }
                    }
                }
            },
            "vectorSources": [],
        })
        .to_string();

        serde_json::from_str::<Box<dyn PlotOperator>>(&serialized).unwrap();

        let deserialized: MeanRasterPixelValuesOverTime =
            serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.params, temporal_raster_mean_plot.params);
    }

    #[tokio::test]
    async fn single_raster() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };
        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let temporal_raster_mean_plot = MeanRasterPixelValuesOverTime {
            params: MeanRasterPixelValuesOverTimeParams {
                time_position: MeanRasterPixelValuesOverTimePosition::Center,
                area: true,
            },
            sources: SingleRasterSource {
                raster: generate_mock_raster_source(
                    vec![TimeInterval::new(
                        TimeInstance::from(DateTime::new_utc(1990, 1, 1, 0, 0, 0)),
                        TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0)),
                    )
                    .unwrap()],
                    vec![vec![1, 2, 3, 4, 5, 6]],
                ),
            },
        };

        let temporal_raster_mean_plot = temporal_raster_mean_plot
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let processor = temporal_raster_mean_plot
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = processor
            .plot_query(
                PlotQueryRectangle::with_bounds_and_resolution(
                    BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    TimeInterval::default(),
                    SpatialResolution::one(),
                ),
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert!(matches!(result.metadata, PlotMetaData::None));

        let vega_json: Value = serde_json::from_str(&result.vega_string).unwrap();

        assert_eq!(
            vega_json,
            json!({
                "$schema": "https://vega.github.io/schema/vega-lite/v4.17.0.json",
                "data": {
                    "values": [{
                        "x": "1995-01-01T00:00:00+00:00",
                        "y": 3.5
                    }]
                },
                "description": "Area Plot",
                "encoding": {
                    "x": {
                        "field": "x",
                        "title": "Time",
                        "type": "temporal"
                    },
                    "y": {
                        "field": "y",
                        "title": "",
                        "type": "quantitative"
                    }
                },
                "mark": {
                    "type": "area",
                    "line": true,
                    "point": true
                }
            })
        );
    }

    fn generate_mock_raster_source(
        time_intervals: Vec<TimeInterval>,
        values_vec: Vec<Vec<u8>>,
    ) -> Box<dyn RasterOperator> {
        assert_eq!(time_intervals.len(), values_vec.len());
        assert!(values_vec.iter().all(|v| v.len() == 6));

        let mut tiles = Vec::with_capacity(time_intervals.len());
        for (time_interval, values) in time_intervals.into_iter().zip(values_vec) {
            tiles.push(RasterTile2D::new_with_tile_info(
                time_interval,
                TileInformation {
                    global_geo_transform: TestDefault::test_default(),
                    global_tile_position: [0, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                },
                Grid2D::new([3, 2].into(), values).unwrap().into(),
            ));
        }

        MockRasterSource {
            params: MockRasterSourceParams {
                data: tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
            },
        }
        .boxed()
    }

    #[tokio::test]
    async fn raster_series() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };
        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let temporal_raster_mean_plot = MeanRasterPixelValuesOverTime {
            params: MeanRasterPixelValuesOverTimeParams {
                time_position: MeanRasterPixelValuesOverTimePosition::Start,
                area: true,
            },

            sources: SingleRasterSource {
                raster: generate_mock_raster_source(
                    vec![
                        TimeInterval::new(
                            TimeInstance::from(DateTime::new_utc(1990, 1, 1, 0, 0, 0)),
                            TimeInstance::from(DateTime::new_utc(1995, 1, 1, 0, 0, 0)),
                        )
                        .unwrap(),
                        TimeInterval::new(
                            TimeInstance::from(DateTime::new_utc(1995, 1, 1, 0, 0, 0)),
                            TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0)),
                        )
                        .unwrap(),
                        TimeInterval::new(
                            TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0)),
                            TimeInstance::from(DateTime::new_utc(2005, 1, 1, 0, 0, 0)),
                        )
                        .unwrap(),
                    ],
                    vec![
                        vec![1, 2, 3, 4, 5, 6],
                        vec![9, 9, 8, 8, 8, 9],
                        vec![3, 4, 5, 6, 7, 8],
                    ],
                ),
            },
        };

        let temporal_raster_mean_plot = temporal_raster_mean_plot
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let processor = temporal_raster_mean_plot
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = processor
            .plot_query(
                PlotQueryRectangle::with_bounds_and_resolution(
                    BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    TimeInterval::default(),
                    SpatialResolution::one(),
                ),
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            AreaLineChart::new(
                vec![
                    TimeInstance::from(DateTime::new_utc(1990, 1, 1, 0, 0, 0)),
                    TimeInstance::from(DateTime::new_utc(1995, 1, 1, 0, 0, 0)),
                    TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0))
                ],
                vec![3.5, 8.5, 5.5],
                Measurement::Unitless,
                true,
            )
            .unwrap()
            .to_vega_embeddable(false)
            .unwrap()
        );
    }
}
