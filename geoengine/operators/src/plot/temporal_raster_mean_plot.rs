use crate::engine::{
    BoxRasterQueryProcessor, CanonicOperatorName, ExecutionContext, InitializedPlotOperator,
    InitializedRasterOperator, InitializedSources, Operator, OperatorName, PlotOperator,
    PlotQueryProcessor, PlotResultDescriptor, QueryContext, QueryProcessor, SingleRasterSource,
    TypedPlotQueryProcessor, WorkflowOperatorPath,
};
use crate::error::Error;
use crate::optimization::OptimizationError;
use crate::plot::util::{
    SelectedBand, masked_pixels_in_query, optimize_raster_plot_source, select_bands,
};
use crate::util::Result;
use crate::util::math::average_floor;
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use geoengine_datatypes::plots::{AreaLineChart, DataPoint, MultiLineChart, Plot, PlotData};
use geoengine_datatypes::primitives::{
    BandSelection, Measurement, PlotQueryRectangle, RasterQueryRectangle, SpatialResolution,
    TimeInstance, TimeInterval,
};
use geoengine_datatypes::raster::{GridBoundingBox2D, Pixel, RasterTile2D};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

pub const MEAN_RASTER_PIXEL_VALUES_OVER_TIME_NAME: &str = "Mean Raster Pixel Values over Time";

/// A plot that shows the mean values of rasters over time.
///
/// It draws an area plot for a single band and a line per band for multiple bands.
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
    /// Only applies to a single band, multiple bands are drawn as lines.
    #[serde(default = "default_true")]
    pub area: bool,

    /// The names of the bands to compute the means for.
    /// All bands are used if this is empty.
    #[serde(default)]
    pub band_names: Vec<String>,
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
        let name = CanonicOperatorName::from(&self);

        let initalized_sources = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?;
        let raster = initalized_sources.raster;
        // means aggregate pixels: halo pixels would count multiple times
        raster
            .result_descriptor()
            .ensure_no_tile_overlap(MeanRasterPixelValuesOverTime::TYPE_NAME)?;

        let in_desc = raster.result_descriptor().clone();

        let bands = select_bands(&in_desc.bands, &self.params.band_names)?;

        let initialized_operator = InitializedMeanRasterPixelValuesOverTime {
            name,
            result_descriptor: in_desc.into(),
            raster,
            bands,
            state: self.params,
        };

        Ok(initialized_operator.boxed())
    }

    span_fn!(MeanRasterPixelValuesOverTime);
}

/// The initialization of `MeanRasterPixelValuesOverTime`
pub struct InitializedMeanRasterPixelValuesOverTime {
    name: CanonicOperatorName,
    result_descriptor: PlotResultDescriptor,
    raster: Box<dyn InitializedRasterOperator>,
    bands: Vec<SelectedBand>,
    state: MeanRasterPixelValuesOverTimeParams,
}

impl InitializedPlotOperator for InitializedMeanRasterPixelValuesOverTime {
    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        let input_processor = self.raster.query_processor()?;
        let time_position = self.state.time_position;
        let measurement = self.measurement();
        let bands = self.bands.clone();
        let draw_area = self.state.area;

        let processor = call_on_generic_raster_processor!(input_processor, raster => {
            MeanRasterPixelValuesOverTimeQueryProcessor { raster, bands, time_position, measurement, draw_area }.boxed()
        });

        Ok(TypedPlotQueryProcessor::JsonVega(processor))
    }

    fn result_descriptor(&self) -> &PlotResultDescriptor {
        &self.result_descriptor
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn optimize(
        &self,
        target_resolution: SpatialResolution,
    ) -> Result<Box<dyn PlotOperator>, OptimizationError> {
        Ok(MeanRasterPixelValuesOverTime {
            params: self.state.clone(),
            sources: SingleRasterSource {
                raster: optimize_raster_plot_source(self.raster.as_ref(), target_resolution)?,
            },
        }
        .boxed())
    }
}

impl InitializedMeanRasterPixelValuesOverTime {
    /// The measurement of the y-axis: the measurement of the selected bands if they share one, otherwise unitless.
    fn measurement(&self) -> Measurement {
        let bands = &self.raster.result_descriptor().bands;
        let mut measurements = self
            .bands
            .iter()
            .map(|band| &bands[band.index as usize].measurement);

        match measurements.next() {
            Some(first) if measurements.all(|measurement| measurement == first) => first.clone(),
            _ => Measurement::Unitless,
        }
    }
}

/// A query processor that calculates the `TemporalRasterMeanPlot` about its input.
pub struct MeanRasterPixelValuesOverTimeQueryProcessor<P: Pixel> {
    raster: BoxRasterQueryProcessor<P>,
    bands: Vec<SelectedBand>,
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
        let rd = self.raster.result_descriptor();

        let raster_query_rect = RasterQueryRectangle::from_bounds_and_geo_transform(
            &query,
            BandSelection::new(self.bands.iter().map(|band| band.index).collect())?,
            rd.tiling_grid_definition().geo_transform,
        );

        let query_bounds = raster_query_rect.spatial_bounds();

        let means = Self::calculate_means(
            self.raster.query(raster_query_rect, ctx).await?,
            &self.bands,
            query_bounds,
            self.time_position,
        )
        .await?;

        let plot_data = if let [means] = means.as_slice() {
            Self::generate_area_line_plot(means, self.measurement.clone(), self.draw_area)?
                .to_vega_embeddable(false)?
        } else {
            Self::generate_multi_line_plot(&self.bands, &means, self.measurement.clone())
                .to_vega_embeddable(false)?
        };

        Ok(plot_data)
    }
}

impl<P: Pixel> MeanRasterPixelValuesOverTimeQueryProcessor<P> {
    /// Calculates the means per time step for each of the `bands`, in the order of `bands`.
    async fn calculate_means(
        mut tile_stream: BoxStream<'_, Result<RasterTile2D<P>>>,
        bands: &[SelectedBand],
        query_bounds: GridBoundingBox2D,
        position: MeanRasterPixelValuesOverTimePosition,
    ) -> Result<Vec<BTreeMap<TimeInstance, MeanCalculator>>> {
        // tiles carry the index of their band in the source raster
        let means_index_of_band: HashMap<u32, usize> = bands
            .iter()
            .enumerate()
            .map(|(i, band)| (band.index, i))
            .collect();

        let mut means: Vec<BTreeMap<TimeInstance, MeanCalculator>> =
            bands.iter().map(|_| BTreeMap::new()).collect();

        while let Some(tile) = tile_stream.next().await {
            let tile = tile?;

            let band_means = means_index_of_band
                .get(&tile.band)
                .map(|&i| &mut means[i])
                .ok_or(Error::InvalidOperatorSpec {
                    reason: format!(
                        "MeanRasterPixelValuesOverTime received a tile of unexpected band {}.",
                        tile.band
                    ),
                })?;

            // empty tiles also fill the time gaps of the source, so they must not add time steps
            match tile.grid_array {
                geoengine_datatypes::raster::GridOrEmpty::Grid(_) => {
                    let time = Self::time_interval_projection(tile.time, position);
                    let mean = band_means.entry(time).or_default();
                    mean.add(masked_pixels_in_query(&tile, &query_bounds));
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

    fn generate_area_line_plot(
        means: &BTreeMap<TimeInstance, MeanCalculator>,
        measurement: Measurement,
        draw_area: bool,
    ) -> Result<AreaLineChart> {
        let mut timestamps = Vec::with_capacity(means.len());
        let mut values = Vec::with_capacity(means.len());

        for (timestamp, mean_calculator) in means {
            timestamps.push(*timestamp);
            values.push(mean_calculator.mean());
        }

        AreaLineChart::new(timestamps, values, measurement, draw_area).map_err(Into::into)
    }

    /// Generates one line per band, named after the band.
    fn generate_multi_line_plot(
        bands: &[SelectedBand],
        means: &[BTreeMap<TimeInstance, MeanCalculator>],
        measurement: Measurement,
    ) -> MultiLineChart {
        let data = bands
            .iter()
            .zip(means)
            .flat_map(|(band, band_means)| {
                band_means
                    .iter()
                    .map(|(timestamp, mean_calculator)| DataPoint {
                        series: band.name.clone(),
                        time: *timestamp,
                        value: mean_calculator.mean(),
                    })
            })
            .collect();

        MultiLineChart::new(data, measurement)
    }
}

#[derive(Default)]
struct MeanCalculator {
    mean: f64,
    n: usize,
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

    /// Returns `None` if there are no valid values.
    #[inline]
    fn mean(&self) -> Option<f64> {
        (self.n > 0).then_some(self.mean)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::raster::TileIdx;
    use geoengine_datatypes::raster::TileOverlap;
    use geoengine_datatypes::raster::TileSize;

    use crate::{
        engine::{
            ChunkByteSize, MockExecutionContext, RasterBandDescriptor, RasterBandDescriptors,
            RasterOperator, RasterResultDescriptor, SpatialGridDescriptor, TimeDescriptor,
        },
        source::GdalSource,
    };
    use crate::{
        mock::{MockRasterSource, MockRasterSourceParams},
        source::GdalSourceParameters,
    };
    use geoengine_datatypes::primitives::{
        BoundingBox2D, CacheHint, Coordinate2D, Measurement, PlotSeriesSelection, TimeInterval,
    };
    use geoengine_datatypes::raster::GeoTransform;
    use geoengine_datatypes::{
        dataset::NamedData,
        plots::PlotMetaData,
        primitives::DateTime,
        raster::{BoundedGrid, GridShape2D},
    };
    use geoengine_datatypes::{raster::TilingSpecification, spatial_reference::SpatialReference};
    use geoengine_datatypes::{
        raster::{
            EmptyGrid2D, Grid2D, GridOrEmpty2D, MaskedGrid2D, RasterDataType, TileInformation,
        },
        util::test::TestDefault,
    };
    use serde_json::{Value, json};

    #[test]
    fn serialization() {
        let temporal_raster_mean_plot = MeanRasterPixelValuesOverTime {
            params: MeanRasterPixelValuesOverTimeParams {
                time_position: MeanRasterPixelValuesOverTimePosition::Start,
                area: true,
                band_names: vec![],
            },
            sources: SingleRasterSource {
                raster: GdalSource {
                    params: GdalSourceParameters::new(NamedData::with_system_name("test")),
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
                        "data": "test"
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
        let tile_size = TileSize::new_unchecked(3, 2);
        let tiling_specification = TilingSpecification::new(tile_size);
        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let temporal_raster_mean_plot = MeanRasterPixelValuesOverTime {
            params: MeanRasterPixelValuesOverTimeParams {
                time_position: MeanRasterPixelValuesOverTimePosition::Center,
                area: true,
                band_names: vec![],
            },
            sources: SingleRasterSource {
                raster: generate_mock_raster_source(
                    vec![
                        TimeInterval::new(
                            TimeInstance::from(DateTime::new_utc(1990, 1, 1, 0, 0, 0)),
                            TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0)),
                        )
                        .unwrap(),
                    ],
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
                PlotQueryRectangle::new(
                    BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    TimeInterval::default(),
                    PlotSeriesSelection::all(),
                ),
                &execution_context.mock_query_context(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert!(matches!(result.metadata, PlotMetaData::None));

        let vega_json: Value = result.vega_spec.clone();

        assert_eq!(
            vega_json,
            json!({
                "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
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

    #[tokio::test]
    async fn it_uses_only_pixels_in_query_rectangle() {
        let tile_size_in_pixels = GridShape2D::new_2d(3, 2);
        let tiling_specification = TilingSpecification::new(TileSize::new_unchecked(
            tile_size_in_pixels.y(),
            tile_size_in_pixels.x(),
        ));
        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let temporal_raster_mean_plot = MeanRasterPixelValuesOverTime {
            params: MeanRasterPixelValuesOverTimeParams {
                time_position: MeanRasterPixelValuesOverTimePosition::Start,
                area: false,
                band_names: vec![],
            },
            sources: SingleRasterSource {
                raster: generate_mock_raster_source(
                    vec![
                        TimeInterval::new(
                            TimeInstance::from(DateTime::new_utc(1990, 1, 1, 0, 0, 0)),
                            TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0)),
                        )
                        .unwrap(),
                    ],
                    vec![vec![1, 2, 3, 4, 5, 6]],
                ),
            },
        };

        let processor = temporal_raster_mean_plot
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        // the tile covers x in [0, 2) and y in (-3, 0], the query only the pixels with values 1 and 3
        let result = processor
            .plot_query(
                PlotQueryRectangle::new(
                    BoundingBox2D::new((0.2, -1.8).into(), (0.8, -0.2).into()).unwrap(),
                    TimeInterval::default(),
                    PlotSeriesSelection::all(),
                ),
                &execution_context.mock_query_context(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        let vega_json: Value = result.vega_spec.clone();

        assert_eq!(
            vega_json["data"]["values"],
            json!([{
                "x": "1990-01-01T00:00:00+00:00",
                "y": 2.0
            }])
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
                    overlap: TileOverlap::zero(),
                    global_geo_transform: TestDefault::test_default(),
                    tile_position: TileIdx::new_y_x(0, 0),
                    tile_size: TileSize::new_unchecked(3, 2),
                },
                0,
                Grid2D::new([3, 2].into(), values).unwrap().into(),
                CacheHint::no_cache(),
            ));
        }

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            // TODO: find out if this can handle regular time as well
            time: TimeDescriptor::new_irregular(Some(
                TimeInterval::new(
                    tiles.first().unwrap().time.start(),
                    tiles.last().unwrap().time.end(),
                )
                .unwrap(),
            )),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 1., -1.),
                GridShape2D::new_2d(3, 2).bounding_box(),
                TileSize::new_unchecked(256, 256),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        MockRasterSource {
            params: MockRasterSourceParams {
                data: tiles,
                result_descriptor,
            },
        }
        .boxed()
    }

    #[tokio::test]
    async fn raster_series() {
        let tile_size = TileSize::new_unchecked(3, 2);
        let tiling_specification = TilingSpecification::new(tile_size);
        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let temporal_raster_mean_plot = MeanRasterPixelValuesOverTime {
            params: MeanRasterPixelValuesOverTimeParams {
                time_position: MeanRasterPixelValuesOverTimePosition::Start,
                area: true,
                band_names: vec![],
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
                PlotQueryRectangle::new(
                    BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    TimeInterval::default(),
                    PlotSeriesSelection::all(),
                ),
                &execution_context.mock_query_context(ChunkByteSize::MIN),
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
                vec![Some(3.5), Some(8.5), Some(5.5)],
                Measurement::Unitless,
                true,
            )
            .unwrap()
            .to_vega_embeddable(false)
            .unwrap()
        );
    }

    /// A source with the bands `a` and `b` and a single time step, where band `a` has the values 1..=6 and band `b` the values 7..=12.
    fn generate_mock_multiband_raster_source(
        time_interval: TimeInterval,
        measurements: [Measurement; 2],
    ) -> Box<dyn RasterOperator> {
        let tiles = [vec![1, 2, 3, 4, 5, 6], vec![7, 8, 9, 10, 11, 12]]
            .into_iter()
            .enumerate()
            .map(|(band, values)| {
                RasterTile2D::new_with_tile_info(
                    time_interval,
                    TileInformation {
                        global_geo_transform: TestDefault::test_default(),
                        tile_position: [0, 0].into(),
                        tile_size: TileSize::new_unchecked(3, 2),
                    },
                    band as u32,
                    Grid2D::new([3, 2].into(), values)
                        .expect("grid should match the tile size")
                        .into(),
                    CacheHint::no_cache(),
                )
            })
            .collect();

        let [measurement_a, measurement_b] = measurements;

        MockRasterSource {
            params: MockRasterSourceParams {
                data: tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: TimeDescriptor::new_irregular(Some(time_interval)),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 1., -1.),
                        GridShape2D::new_2d(3, 2).bounding_box(),
                        TileSize::new_unchecked(3, 2),
                    ),
                    bands: RasterBandDescriptors::new(vec![
                        RasterBandDescriptor::new("a".to_string(), measurement_a),
                        RasterBandDescriptor::new("b".to_string(), measurement_b),
                    ])
                    .expect("band names should be unique"),
                },
            },
        }
        .boxed()
    }

    async fn query_multiband_plot(
        band_names: Vec<String>,
        measurements: [Measurement; 2],
    ) -> Result<PlotData> {
        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new(TileSize::new_unchecked(3, 2)),
        );

        let time_interval = TimeInterval::new(
            TimeInstance::from(DateTime::new_utc(1990, 1, 1, 0, 0, 0)),
            TimeInstance::from(DateTime::new_utc(2000, 1, 1, 0, 0, 0)),
        )
        .expect("time interval should be valid");

        let processor = MeanRasterPixelValuesOverTime {
            params: MeanRasterPixelValuesOverTimeParams {
                time_position: MeanRasterPixelValuesOverTimePosition::Start,
                area: true,
                band_names,
            },
            sources: SingleRasterSource {
                raster: generate_mock_multiband_raster_source(time_interval, measurements),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await?
        .query_processor()?
        .json_vega()
        .expect("the operator should produce a vega plot");

        processor
            .plot_query(
                PlotQueryRectangle::new(
                    BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .expect("bounding box should be valid"),
                    TimeInterval::default(),
                    PlotSeriesSelection::all(),
                ),
                &execution_context.mock_query_context(ChunkByteSize::MIN),
            )
            .await
    }

    #[tokio::test]
    async fn it_plots_one_line_per_band() {
        let measurement =
            Measurement::continuous("temperature".to_string(), Some("°C".to_string()));

        let result = query_multiband_plot(vec![], [measurement.clone(), measurement.clone()])
            .await
            .expect("plotting all bands should work");

        let start = TimeInstance::from(DateTime::new_utc(1990, 1, 1, 0, 0, 0));
        assert_eq!(
            result,
            MultiLineChart::new(
                vec![
                    DataPoint {
                        series: "a".to_string(),
                        time: start,
                        value: Some(3.5),
                    },
                    DataPoint {
                        series: "b".to_string(),
                        time: start,
                        value: Some(9.5),
                    },
                ],
                measurement,
            )
            .to_vega_embeddable(false)
            .expect("the chart should be serializable")
        );
    }

    #[tokio::test]
    async fn it_uses_unitless_measurement_for_bands_with_different_measurements() {
        let result = query_multiband_plot(
            vec![],
            [
                Measurement::continuous("temperature".to_string(), Some("°C".to_string())),
                Measurement::continuous("precipitation".to_string(), Some("mm".to_string())),
            ],
        )
        .await
        .expect("plotting all bands should work");

        assert_eq!(result.vega_spec["encoding"]["y"]["title"], json!(""));
    }

    #[tokio::test]
    async fn it_plots_the_selected_band_as_area() {
        let result = query_multiband_plot(
            vec!["b".to_string()],
            [Measurement::Unitless, Measurement::Unitless],
        )
        .await
        .expect("plotting band `b` should work");

        assert_eq!(
            result,
            AreaLineChart::new(
                vec![TimeInstance::from(DateTime::new_utc(1990, 1, 1, 0, 0, 0))],
                vec![Some(9.5)],
                Measurement::Unitless,
                true,
            )
            .expect("the chart should be valid")
            .to_vega_embeddable(false)
            .expect("the chart should be serializable")
        );
    }

    #[tokio::test]
    async fn it_fails_on_unknown_band() {
        let result = query_multiband_plot(
            vec!["c".to_string()],
            [Measurement::Unitless, Measurement::Unitless],
        )
        .await;

        assert!(matches!(
            result,
            Err(Error::InvalidOperatorSpec { reason }) if reason == "Band 'c' does not exist."
        ));
    }

    #[tokio::test]
    async fn it_has_no_mean_for_time_steps_without_valid_pixels_and_skips_empty_tiles() {
        let time_steps: Vec<TimeInterval> = (0..3)
            .map(|i| {
                TimeInterval::new(i * 1000, (i + 1) * 1000).expect("time interval should be valid")
            })
            .collect();
        let shape: GridShape2D = [3, 2].into();

        let valid: GridOrEmpty2D<u8> = Grid2D::new(shape, vec![1, 2, 3, 4, 5, 6])
            .expect("grid should match the shape")
            .into();
        let empty: GridOrEmpty2D<u8> = EmptyGrid2D::new(shape).into();
        let masked: GridOrEmpty2D<u8> = MaskedGrid2D::new(
            Grid2D::new(shape, vec![1; 6]).expect("grid should match the shape"),
            Grid2D::new(shape, vec![false; 6]).expect("mask should match the shape"),
        )
        .expect("mask should match the grid")
        .into();

        let tiles = time_steps
            .iter()
            .zip([valid, empty, masked])
            .map(|(time, grid)| {
                Ok(RasterTile2D::new_with_tile_info(
                    *time,
                    TileInformation {
                        global_geo_transform: TestDefault::test_default(),
                        tile_position: [0, 0].into(),
                        tile_size: TileSize::new_unchecked(shape.y(), shape.x()),
                    },
                    0,
                    grid,
                    CacheHint::no_cache(),
                ))
            })
            .collect::<Vec<_>>();

        let means = MeanRasterPixelValuesOverTimeQueryProcessor::<u8>::calculate_means(
            futures::stream::iter(tiles).boxed(),
            &[SelectedBand {
                index: 0,
                name: "band".to_string(),
            }],
            shape.bounding_box(),
            MeanRasterPixelValuesOverTimePosition::Start,
        )
        .await
        .expect("calculating the means should work");

        let means = means[0]
            .iter()
            .map(|(time, mean)| (*time, mean.mean()))
            .collect::<Vec<_>>();

        assert_eq!(
            means,
            vec![
                (time_steps[0].start(), Some(3.5)),
                (time_steps[2].start(), None),
            ]
        );
    }
}
