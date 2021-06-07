use crate::engine::{
    ExecutionContext, InitializedOperator, InitializedPlotOperator, InitializedRasterOperator,
    Operator, PlotOperator, PlotQueryProcessor, PlotResultDescriptor, QueryContext, QueryProcessor,
    QueryRectangle, RasterQueryProcessor, SingleRasterSource, TypedPlotQueryProcessor,
};
use crate::util::math::average_floor;
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::plots::{AreaLineChart, Plot, PlotData};
use geoengine_datatypes::primitives::{Measurement, TimeInstance, TimeInterval};
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const MEAN_RASTER_PIXEL_VALUES_OVER_TIME_NAME: &str = "Mean Raster Pixel Values over Time";

/// A plot that shows the mean values of rasters over time as an area plot.
pub type MeanRasterPixelValuesOverTime =
    Operator<MeanRasterPixelValuesOverTimeParams, SingleRasterSource>;

/// The parameter spec for `MeanRasterPixelValuesOverTime`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MeanRasterPixelValuesOverTimeParams {
    /// Where should the x-axis (time) tick be positioned?
    /// At either time start, time end or in the center.
    time_position: MeanRasterPixelValuesOverTimePosition,

    /// Whether to fill the area under the curve.
    #[serde(default = "default_true")]
    area: bool,
}

const fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MeanRasterPixelValuesOverTimePosition {
    Start,
    Center,
    End,
}

#[typetag::serde]
impl PlotOperator for MeanRasterPixelValuesOverTime {
    fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<InitializedPlotOperator>> {
        let initialized_operator = InitializedMeanRasterPixelValuesOverTime {
            result_descriptor: PlotResultDescriptor {},
            raster: self.sources.raster.initialize(context)?,
            state: self.params,
        };

        Ok(initialized_operator.boxed())
    }
}

/// The initialization of `MeanRasterPixelValuesOverTime`
pub struct InitializedMeanRasterPixelValuesOverTime {
    result_descriptor: PlotResultDescriptor,
    raster: Box<InitializedRasterOperator>,
    state: MeanRasterPixelValuesOverTimeParams,
}

impl InitializedOperator<PlotResultDescriptor, TypedPlotQueryProcessor>
    for InitializedMeanRasterPixelValuesOverTime
{
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
        query: QueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<Self::OutputFormat> {
        let means =
            Self::calculate_means(self.raster.query(query, ctx).await?, self.time_position).await?;

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

            if tile.grid_array.is_empty() {
                continue;
            }

            let tile = tile.into_materialized_tile(); // this should be free since we checked for empty tiles

            let time = Self::time_interval_projection(tile.time, position);

            let mean = means.entry(time).or_default();
            mean.add(&tile.grid_array.data, tile.grid_array.no_data_value);
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
    fn add<P: Pixel>(&mut self, values: &[P], no_data: Option<P>) {
        if let Some(no_data) = no_data {
            self.add_with_no_data(values, no_data);
        } else {
            self.add_without_no_data(values);
        }
    }

    #[inline]
    fn add_without_no_data<P: Pixel>(&mut self, values: &[P]) {
        for &value in values {
            self.add_single_value(value);
        }
    }

    #[inline]
    fn add_with_no_data<P: Pixel>(&mut self, values: &[P], no_data: P) {
        for &value in values {
            if value == no_data {
                continue;
            }

            self.add_single_value(value);
        }
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
        engine::{MockExecutionContext, MockQueryContext, RasterOperator, RasterResultDescriptor},
        source::GdalSource,
    };
    use crate::{
        mock::{MockRasterSource, MockRasterSourceParams},
        source::GdalSourceParameters,
    };
    use chrono::NaiveDate;
    use geoengine_datatypes::raster::{Grid2D, RasterDataType, TileInformation};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::{
        dataset::InternalDatasetId,
        plots::{PlotData, PlotMetaData},
    };
    use geoengine_datatypes::{
        primitives::{BoundingBox2D, Measurement, SpatialResolution, TimeInterval},
        util::Identifier,
    };
    use num_traits::AsPrimitive;
    use serde_json::json;

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
                        dataset: InternalDatasetId::new().into(),
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
                        "dataset": {
                            "internal": "a626c880-1c41-489b-9e19-9596d129859c"
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
        let temporal_raster_mean_plot = MeanRasterPixelValuesOverTime {
            params: MeanRasterPixelValuesOverTimeParams {
                time_position: MeanRasterPixelValuesOverTimePosition::Center,
                area: true,
            },
            sources: SingleRasterSource {
                raster: generate_mock_raster_source(
                    vec![TimeInterval::new(
                        TimeInstance::from(NaiveDate::from_ymd(1990, 1, 1).and_hms(0, 0, 0)),
                        TimeInstance::from(NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0)),
                    )
                    .unwrap()],
                    vec![vec![1, 2, 3, 4, 5, 6]],
                ),
            },
        };

        let execution_context = MockExecutionContext::default();

        let temporal_raster_mean_plot = temporal_raster_mean_plot
            .boxed()
            .initialize(&execution_context)
            .unwrap();

        let processor = temporal_raster_mean_plot
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = processor
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
            result,
            PlotData {
                vega_string: r#"{"$schema":"https://vega.github.io/schema/vega-lite/v4.17.0.json","data":{"values":[{"x":"1995-01-01T00:00:00+00:00","y":3.5}]},"description":"Area Plot","encoding":{"x":{"field":"x","title":"Time","type":"temporal"},"y":{"field":"y","title":"","type":"quantitative"}},"mark":{"type":"area","line":true,"point":true}}"#.to_owned(),
                metadata: PlotMetaData::None,
            }
        );
    }

    fn generate_mock_raster_source(
        time_intervals: Vec<TimeInterval>,
        values_vec: Vec<Vec<u8>>,
    ) -> Box<dyn RasterOperator> {
        assert_eq!(time_intervals.len(), values_vec.len());
        assert!(values_vec.iter().all(|v| v.len() == 6));

        let no_data_value = None;

        let mut tiles = Vec::with_capacity(time_intervals.len());
        for (time_interval, values) in time_intervals.into_iter().zip(values_vec) {
            tiles.push(RasterTile2D::new_with_tile_info(
                time_interval,
                TileInformation {
                    global_geo_transform: Default::default(),
                    global_tile_position: [0, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                },
                Grid2D::new([3, 2].into(), values, no_data_value)
                    .unwrap()
                    .into(),
            ));
        }

        MockRasterSource {
            params: MockRasterSourceParams {
                data: tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed()
    }

    #[tokio::test]
    async fn raster_series() {
        let temporal_raster_mean_plot = MeanRasterPixelValuesOverTime {
            params: MeanRasterPixelValuesOverTimeParams {
                time_position: MeanRasterPixelValuesOverTimePosition::Start,
                area: true,
            },

            sources: SingleRasterSource {
                raster: generate_mock_raster_source(
                    vec![
                        TimeInterval::new(
                            TimeInstance::from(NaiveDate::from_ymd(1990, 1, 1).and_hms(0, 0, 0)),
                            TimeInstance::from(NaiveDate::from_ymd(1995, 1, 1).and_hms(0, 0, 0)),
                        )
                        .unwrap(),
                        TimeInterval::new(
                            TimeInstance::from(NaiveDate::from_ymd(1995, 1, 1).and_hms(0, 0, 0)),
                            TimeInstance::from(NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0)),
                        )
                        .unwrap(),
                        TimeInterval::new(
                            TimeInstance::from(NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0)),
                            TimeInstance::from(NaiveDate::from_ymd(2005, 1, 1).and_hms(0, 0, 0)),
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

        let execution_context = MockExecutionContext::default();

        let temporal_raster_mean_plot = temporal_raster_mean_plot
            .boxed()
            .initialize(&execution_context)
            .unwrap();

        let processor = temporal_raster_mean_plot
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = processor
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
            result,
            AreaLineChart::new(
                vec![
                    TimeInstance::from(NaiveDate::from_ymd(1990, 1, 1).and_hms(0, 0, 0)),
                    TimeInstance::from(NaiveDate::from_ymd(1995, 1, 1).and_hms(0, 0, 0)),
                    TimeInstance::from(NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0))
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
