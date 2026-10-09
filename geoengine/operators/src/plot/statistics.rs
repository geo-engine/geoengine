use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedPlotOperator, InitializedRasterOperator,
    InitializedVectorOperator, Operator, OperatorName, PlotOperator, PlotQueryProcessor,
    PlotResultDescriptor, QueryContext, QueryProcessor, SingleRasterOrVectorSource,
    TypedPlotQueryProcessor, TypedRasterQueryProcessor, TypedVectorQueryProcessor,
    WorkflowOperatorPath,
};
use crate::error;
use crate::error::Error;
use crate::optimization::OptimizationError;
use crate::plot::util::{SelectedBand, masked_pixels_in_query, pixel_count_in_query, select_bands};
use crate::util::Result;
use crate::util::input::RasterOrVectorOperator;
use crate::util::number_statistics::NumberStatistics;
use crate::util::statistics::{SafePSquareQuantileEstimator, StatisticsError};
use async_trait::async_trait;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use geoengine_datatypes::collections::FeatureCollectionInfos;
use geoengine_datatypes::plots::{Plot, PlotData, Table, TableColumn};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BandSelection, BoundingBox2D, ColumnSelection, PlotQueryRectangle,
    RasterQueryRectangle, SpatialResolution,
};
use geoengine_datatypes::raster::ConvertDataTypeParallel;
use geoengine_datatypes::raster::GridOrEmpty;
use itertools::Itertools;
use num_traits::AsPrimitive;
use ordered_float::NotNan;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::collections::HashMap;

pub const STATISTICS_OPERATOR_NAME: &str = "Statistics";

/// A plot that outputs basic statistics about its inputs
///
/// Does currently not use a weighted computations, so it assumes equally weighted
/// time steps in the sources.
pub type Statistics = Operator<StatisticsParams, SingleRasterOrVectorSource>;

impl OperatorName for Statistics {
    const TYPE_NAME: &'static str = "Statistics";
}

/// The parameter spec for `Statistics`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatisticsParams {
    /// Names of the (numeric) attributes or raster bands to compute the statistics on.
    #[serde(default)]
    pub column_names: Vec<String>,
    #[serde(default)]
    pub percentiles: Vec<NotNan<f64>>,
}

#[typetag::serde]
#[async_trait]
#[allow(clippy::too_many_lines)]
impl PlotOperator for Statistics {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedPlotOperator>> {
        let name = CanonicOperatorName::from(&self);

        ensure!(
            self.params.percentiles.len() <= 8,
            error::InvalidOperatorSpec {
                reason: "Only up to 8 percentiles can be computed at the same time.".to_string(),
            }
        );

        ensure!(
            self.params
                .percentiles
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                == self.params.percentiles.len(),
            error::InvalidOperatorSpec {
                reason: "The percentiles must be unique.".to_string(),
            }
        );

        let percentiles = self
            .params
            .percentiles
            .iter()
            .map(|p| p.into_inner())
            .collect();

        match self.sources.source {
            RasterOrVectorOperator::Raster(raster_source) => {
                let initialized_raster = raster_source
                    .initialize(path.clone_and_append(0), context)
                    .await?;

                let in_descriptor = initialized_raster.result_descriptor();

                let bands = select_bands(&in_descriptor.bands, &self.params.column_names)?;

                let bbox = in_descriptor.spatial_bounds();

                let initialized_operator = InitializedStatistics::new(
                    name,
                    PlotResultDescriptor {
                        spatial_reference: in_descriptor.spatial_reference,
                        time: in_descriptor.time.bounds,
                        bbox: BoundingBox2D::new(bbox.lower_left(), bbox.upper_right()).ok(),
                    },
                    bands,
                    percentiles,
                    initialized_raster,
                );

                Ok(initialized_operator.boxed())
            }
            RasterOrVectorOperator::Vector(vector_source) => {
                let initialized_vector = vector_source
                    .initialize(path.clone_and_append(0), context)
                    .await?;

                let in_descriptor = initialized_vector.result_descriptor();

                let column_names = if self.params.column_names.is_empty() {
                    in_descriptor
                        .columns
                        .clone()
                        .into_iter()
                        .filter(|(_, info)| info.data_type.is_numeric())
                        .map(|(name, _)| name)
                        .sorted() // the columns are a `HashMap`, so sort them for a stable row order
                        .collect()
                } else {
                    for cn in &self.params.column_names {
                        match in_descriptor.column_data_type(cn.as_str()) {
                            Some(column) if !column.is_numeric() => {
                                return Err(Error::InvalidOperatorSpec {
                                    reason: format!("Column '{cn}' is not numeric."),
                                });
                            }
                            Some(_) => {
                                // OK
                            }
                            None => {
                                return Err(Error::ColumnDoesNotExist { column: cn.clone() });
                            }
                        }
                    }
                    self.params.column_names.clone()
                };

                let initialized_operator = InitializedStatistics::new(
                    name,
                    PlotResultDescriptor {
                        spatial_reference: in_descriptor.spatial_reference,
                        time: in_descriptor.time,
                        bbox: in_descriptor.bbox,
                    },
                    column_names,
                    percentiles,
                    initialized_vector,
                );

                Ok(initialized_operator.boxed())
            }
        }
    }

    span_fn!(Statistics);
}

/// The initialization of `Statistics`
///
/// `Columns` are the names of the vector columns or the selected raster bands.
pub struct InitializedStatistics<Op, Columns> {
    name: CanonicOperatorName,
    result_descriptor: PlotResultDescriptor,
    columns: Vec<Columns>,
    percentiles: Vec<f64>,
    source: Op,
}

impl<Op, Columns> InitializedStatistics<Op, Columns> {
    pub fn new(
        name: CanonicOperatorName,
        result_descriptor: PlotResultDescriptor,
        columns: Vec<Columns>,
        percentiles: Vec<f64>,
        source: Op,
    ) -> Self {
        Self {
            name,
            result_descriptor,
            columns,
            percentiles,
            source,
        }
    }

    fn optimized_params(&self, column_names: Vec<String>) -> StatisticsParams {
        StatisticsParams {
            column_names,
            percentiles: self
                .percentiles
                .iter()
                .copied()
                .map(NotNan::<f64>::new)
                .collect::<Result<Vec<_>, _>>()
                .expect(
                    "percentiles should be not nan because they are NotNan<f64> during initialization",
                ),
        }
    }
}

impl InitializedPlotOperator for InitializedStatistics<Box<dyn InitializedVectorOperator>, String> {
    fn result_descriptor(&self) -> &PlotResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        Ok(TypedPlotQueryProcessor::JsonVega(
            StatisticsVectorQueryProcessor {
                vector: self.source.query_processor()?,
                column_names: self.columns.clone(),
                percentiles: self.percentiles.clone(),
            }
            .boxed(),
        ))
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn optimize(
        &self,
        target_resolution: SpatialResolution,
    ) -> Result<Box<dyn PlotOperator>, OptimizationError> {
        Ok(Statistics {
            params: self.optimized_params(self.columns.clone()),
            sources: SingleRasterOrVectorSource {
                source: RasterOrVectorOperator::Vector(self.source.optimize(target_resolution)?),
            },
        }
        .boxed())
    }
}

impl InitializedPlotOperator
    for InitializedStatistics<Box<dyn InitializedRasterOperator>, SelectedBand>
{
    fn result_descriptor(&self) -> &PlotResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        Ok(TypedPlotQueryProcessor::JsonVega(
            StatisticsRasterQueryProcessor {
                raster: self.source.query_processor()?,
                bands: self.columns.clone(),
                percentiles: self.percentiles.clone(),
            }
            .boxed(),
        ))
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn optimize(
        &self,
        target_resolution: SpatialResolution,
    ) -> Result<Box<dyn PlotOperator>, OptimizationError> {
        Ok(Statistics {
            params: self
                .optimized_params(self.columns.iter().map(|band| band.name.clone()).collect()),
            sources: SingleRasterOrVectorSource {
                source: RasterOrVectorOperator::Raster(self.source.optimize(target_resolution)?),
            },
        }
        .boxed())
    }
}

/// A query processor that calculates the statistics about its vector input.
pub struct StatisticsVectorQueryProcessor {
    vector: TypedVectorQueryProcessor,
    column_names: Vec<String>,
    percentiles: Vec<f64>,
}

#[async_trait]
impl PlotQueryProcessor for StatisticsVectorQueryProcessor {
    type OutputFormat = PlotData;

    fn plot_type(&self) -> &'static str {
        STATISTICS_OPERATOR_NAME
    }

    async fn plot_query<'a>(
        &'a self,
        query: PlotQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<Self::OutputFormat> {
        let mut statistics: Vec<(String, StatisticsAggregator<f64>)> = self
            .column_names
            .iter()
            .map(|column| {
                (
                    column.clone(),
                    StatisticsAggregator::with_percentiles(&self.percentiles),
                )
            })
            .collect();

        let query = query.select_attributes(ColumnSelection::all());

        call_on_generic_vector_processor!(&self.vector, processor => {
            let mut query = processor.query(query, ctx).await?;

            while let Some(collection) = query.next().await {
                let collection = collection?;

                for (column, stats) in &mut statistics {
                    match collection.data(column) {
                        Ok(data) => for value in data.float_options_iter(){
                                match value {
                                    Some(v) => stats.add(v)?,
                                    None => stats.add_no_data()
                                }

                            },
                        Err(_) => stats.add_no_data_batch(collection.len())
                    }
                }
            }
        });

        statistics_table(
            statistics
                .iter()
                .map(|(column, number_statistics)| {
                    StatisticsOutput::new(column.clone(), number_statistics)
                })
                .collect(),
            &self.percentiles,
        )
    }
}

/// A query processor that calculates the statistics about the bands of its raster input.
pub struct StatisticsRasterQueryProcessor {
    raster: TypedRasterQueryProcessor,
    bands: Vec<SelectedBand>,
    percentiles: Vec<f64>,
}

#[async_trait]
impl PlotQueryProcessor for StatisticsRasterQueryProcessor {
    type OutputFormat = PlotData;

    fn plot_type(&self) -> &'static str {
        STATISTICS_OPERATOR_NAME
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
            rd.tiling_grid_definition(ctx.tiling_specification())
                .tiling_geo_transform(),
        );
        let query_bounds = raster_query_rect.spatial_bounds();

        // tiles carry the index of their band in the source raster
        let statistics_index_of_band: HashMap<u32, usize> = self
            .bands
            .iter()
            .enumerate()
            .map(|(i, band)| (band.index, i))
            .collect();

        let tiles = call_on_generic_raster_processor!(&self.raster, processor => {
            processor.query(raster_query_rect, ctx).await?
                .and_then(move |tile| crate::util::spawn_blocking_with_thread_pool(ctx.thread_pool().clone(), move || tile.convert_data_type_parallel()).map_err(Into::into))
                .boxed()
        });

        let statistics = tiles
            .try_fold(
                vec![StatisticsAggregator::with_percentiles(&self.percentiles); self.bands.len()],
                |mut statistics: Vec<StatisticsAggregator<f64>>, raster_tile| {
                    let result = statistics_index_of_band
                        .get(&raster_tile.band)
                        .ok_or(Error::InvalidOperatorSpec {
                            reason: format!(
                                "Statistics received a tile of unexpected band {}.",
                                raster_tile.band
                            ),
                        })
                        .and_then(|&i| {
                            match &raster_tile.grid_array {
                                GridOrEmpty::Grid(_) => process_raster(
                                    &mut statistics[i],
                                    masked_pixels_in_query(&raster_tile, &query_bounds),
                                )?,
                                GridOrEmpty::Empty(_) => statistics[i].add_no_data_batch(
                                    pixel_count_in_query(&raster_tile, &query_bounds),
                                ),
                            }
                            Ok(statistics)
                        });

                    futures::future::ready(result)
                },
            )
            .await?;

        statistics_table(
            self.bands
                .iter()
                .zip(&statistics)
                .map(|(band, stat)| StatisticsOutput::new(band.name.clone(), stat))
                .collect(),
            &self.percentiles,
        )
    }
}

fn process_raster<I>(
    statistics: &mut StatisticsAggregator<f64>,
    data: I,
) -> Result<(), StatisticsError>
where
    I: Iterator<Item = Option<f64>>,
{
    for value_option in data {
        if let Some(value) = value_option {
            statistics.add(value)?;
        } else {
            statistics.add_no_data();
        }
    }

    Ok(())
}

#[derive(Debug, Default, Clone)]
struct StatisticsAggregator<T: AsPrimitive<f64>> {
    number_statistics: NumberStatistics,
    percentile_estimators: Vec<PercentileEstimator<T>>,
}

impl<T: AsPrimitive<f64>> StatisticsAggregator<T> {
    fn with_percentiles(percentiles: &[f64]) -> Self {
        Self {
            number_statistics: NumberStatistics::default(),
            percentile_estimators: percentiles
                .iter()
                .map(|p| PercentileEstimator::new(*p))
                .collect(),
        }
    }

    fn add(&mut self, value: T) -> Result<(), StatisticsError> {
        self.number_statistics.add(value);
        for estimator in &mut self.percentile_estimators {
            estimator.update(value)?;
        }

        Ok(())
    }

    fn add_no_data(&mut self) {
        self.number_statistics.add_no_data();
    }

    fn add_no_data_batch(&mut self, batch_size: usize) {
        self.number_statistics.add_no_data_batch(batch_size);
    }
}

#[derive(Debug, Clone)]
enum PercentileEstimator<T: AsPrimitive<f64>> {
    Unitialized(f64),
    Initialized(SafePSquareQuantileEstimator<T>),
}

impl<T: AsPrimitive<f64>> PercentileEstimator<T> {
    pub fn new(quantile: f64) -> Self {
        Self::Unitialized(quantile)
    }

    pub fn percentile_estimate(&self) -> Option<f64> {
        match self {
            Self::Unitialized(_) => None,
            Self::Initialized(estimator) => Some(estimator.quantile_estimate()),
        }
    }

    pub fn percentile_arg(&self) -> f64 {
        match self {
            Self::Unitialized(quantile) => *quantile,
            Self::Initialized(estimator) => estimator.quantile_arg(),
        }
    }

    pub fn update(&mut self, sample: T) -> Result<(), StatisticsError> {
        match self {
            Self::Unitialized(quantile) => {
                // initial sample must be finite, if the current sample is not, stay uninitialized
                if f64::is_finite(sample.as_()) {
                    *self =
                        Self::Initialized(SafePSquareQuantileEstimator::new(*quantile, sample)?);
                }
            }
            Self::Initialized(estimator) => estimator.update(sample),
        }

        Ok(())
    }
}

/// Creates a table with one row of `statistics` per band or column.
///
/// The rows contain the raw statistics in the `data.values` of the Vega spec,
/// the columns display them and one computed column per percentile.
fn statistics_table(statistics: Vec<StatisticsOutput>, percentiles: &[f64]) -> Result<PlotData> {
    let rows = statistics
        .into_iter()
        .map(|row| match serde_json::to_value(row)? {
            serde_json::Value::Object(row) => Ok(row),
            _ => unreachable!("statistics output should serialize to a JSON object"),
        })
        .collect::<Result<Vec<_>>>()?;

    let mut columns = vec![
        TableColumn::new("valueCount").with_format(",d"),
        TableColumn::new("validCount").with_format(",d"),
        TableColumn::new("min").with_format(".2f"),
        TableColumn::new("max").with_format(".2f"),
        TableColumn::new("mean").with_format(".2f"),
        TableColumn::new("stddev").with_format(".2f"),
    ];
    columns.extend(percentiles.iter().enumerate().map(|(i, percentile)| {
        // round to avoid titles like `p33.300000000000004`
        let title = format!("p{}", (percentile * 100_000.).round() / 1_000.);
        TableColumn::computed(format!("p{i}"), format!("datum.percentiles[{i}].value"))
            .with_title(title)
            .with_format(".2f")
    }));

    Ok(Table::new("name", rows, columns)?.to_vega_embeddable(false)?)
}

/// The statistics summary output type for each raster input/vector input column
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StatisticsOutput {
    /// The name of the band or column
    pub name: String,
    pub value_count: usize,
    pub valid_count: usize,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub stddev: f64,
    pub percentiles: Vec<PercentileOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PercentileOutput {
    percentile: f64,
    value: f64,
}

impl StatisticsOutput {
    fn new(name: String, statistics: &StatisticsAggregator<f64>) -> Self {
        let number_statistics = statistics.number_statistics;
        Self {
            name,
            value_count: number_statistics.count() + number_statistics.nan_count(),
            valid_count: number_statistics.count(),
            min: number_statistics.min(),
            max: number_statistics.max(),
            mean: number_statistics.mean(),
            stddev: number_statistics.std_dev(),
            percentiles: statistics
                .percentile_estimators
                .iter()
                .map(|estimator| PercentileOutput {
                    percentile: estimator.percentile_arg(),
                    value: estimator.percentile_estimate().unwrap_or(f64::NAN),
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::collections::DataCollection;
    use geoengine_datatypes::primitives::{CacheHint, Coordinate2D, PlotSeriesSelection};
    use geoengine_datatypes::util::test::TestDefault;
    use serde_json::json;

    use super::*;
    use crate::engine::{
        ChunkByteSize, MockExecutionContext, RasterOperator, RasterResultDescriptor,
        SpatialGridDescriptor, TimeDescriptor,
    };
    use crate::engine::{RasterBandDescriptor, RasterBandDescriptors, VectorOperator};
    use crate::mock::{MockFeatureCollectionSource, MockRasterSource, MockRasterSourceParams};
    use geoengine_datatypes::primitives::{
        BoundingBox2D, FeatureData, Measurement, NoGeometry, TimeInterval,
    };
    use geoengine_datatypes::raster::{
        BoundedGrid, GeoTransform, Grid2D, GridBoundingBox2D, GridShape2D, RasterDataType,
        RasterTile2D, TileInformation, TilingSpecification,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;

    #[test]
    fn serialization() {
        let statistics = Statistics {
            params: StatisticsParams {
                column_names: vec!["band".to_string()],
                percentiles: vec![],
            },
            sources: MockRasterSource {
                params: MockRasterSourceParams::<u8> {
                    data: vec![],
                    result_descriptor: multi_band_result_descriptor(1),
                },
            }
            .boxed()
            .into(),
        };

        let serialized = serde_json::to_value(&statistics).unwrap();

        assert_eq!(
            serialized["sources"]["source"]["type"],
            "MockRasterSourceu8"
        );

        let deserialized: Statistics = serde_json::from_value(serialized).unwrap();

        assert_eq!(deserialized.params, statistics.params);
        assert!(deserialized.sources.source.is_raster());
    }

    #[test]
    fn it_rejects_multiple_raster_sources() {
        let serialized = json!({
            "type": "Statistics",
            "params": {},
            "sources": {
                "source": [],
            },
        });

        assert!(serde_json::from_value::<Statistics>(serialized).is_err());
    }

    /// A result descriptor for a raster with `num_bands` bands named `band_0`, `band_1`, …
    fn multi_band_result_descriptor(num_bands: u32) -> RasterResultDescriptor {
        RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_irregular(Some(TimeInterval::default())),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
                GridShape2D::new_2d(3, 2).bounding_box(),
            ),
            bands: RasterBandDescriptors::new(
                (0..num_bands)
                    .map(|i| RasterBandDescriptor::new(format!("band_{i}"), Measurement::Unitless))
                    .collect(),
            )
            .unwrap(),
        }
    }

    /// A raster source with one 3x2 tile at tile position [0, 0] per band, filled with `band_values`.
    fn multi_band_raster_source(band_values: Vec<Vec<u8>>) -> Box<dyn RasterOperator> {
        let tile_size_in_pixels = GridShape2D::new_2d(3, 2);

        MockRasterSource {
            params: MockRasterSourceParams {
                result_descriptor: multi_band_result_descriptor(band_values.len() as u32),
                data: band_values
                    .into_iter()
                    .enumerate()
                    .map(|(band, values)| {
                        RasterTile2D::new_with_tile_info(
                            TimeInterval::default(),
                            TileInformation {
                                global_geo_transform: TestDefault::test_default(),
                                global_tile_position: [0, 0].into(),
                                tile_size_in_pixels,
                            },
                            band as u32,
                            Grid2D::new(tile_size_in_pixels, values).unwrap().into(),
                            CacheHint::no_cache(),
                        )
                    })
                    .collect(),
            },
        }
        .boxed()
    }

    async fn raster_statistics(
        source: Box<dyn RasterOperator>,
        params: StatisticsParams,
        bbox: BoundingBox2D,
    ) -> Result<serde_json::Value> {
        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new(GridShape2D::new_2d(3, 2)),
        );

        let statistics = Statistics {
            params,
            sources: source.into(),
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await?;

        let processor = statistics.query_processor()?.json_vega().unwrap();

        processor
            .plot_query(
                PlotQueryRectangle::new(bbox, TimeInterval::default(), PlotSeriesSelection::all()),
                &execution_context.mock_query_context(ChunkByteSize::MIN),
            )
            .await
            .map(|plot| table_rows(&plot))
    }

    /// The rows of the statistics table, i.e., the `data.values` of the Vega spec
    fn table_rows(plot: &PlotData) -> serde_json::Value {
        let spec: serde_json::Value = serde_json::from_str(&plot.vega_string).unwrap();
        spec["data"]["values"].clone()
    }

    #[tokio::test]
    async fn it_creates_a_column_per_percentile() {
        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new(GridShape2D::new_2d(3, 2)),
        );
        let plot = Statistics {
            params: StatisticsParams {
                column_names: vec![],
                percentiles: vec![NotNan::new(0.25).unwrap(), NotNan::new(1. / 3.).unwrap()],
            },
            sources: multi_band_raster_source(vec![vec![1, 2, 3, 4, 5, 6]]).into(),
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .json_vega()
        .unwrap()
        .plot_query(
            PlotQueryRectangle::new(
                world_bbox(),
                TimeInterval::default(),
                PlotSeriesSelection::all(),
            ),
            &execution_context.mock_query_context(ChunkByteSize::MIN),
        )
        .await
        .unwrap();

        let spec: serde_json::Value = serde_json::from_str(&plot.vega_string).unwrap();

        assert_eq!(
            spec["encoding"]["x"]["sort"],
            json!([
                "valueCount",
                "validCount",
                "min",
                "max",
                "mean",
                "stddev",
                "p25",
                "p33.333"
            ])
        );
        assert_eq!(
            spec["transform"][0],
            json!({"calculate": "datum.percentiles[0].value", "as": "p0"})
        );
        assert_eq!(
            spec["transform"][1],
            json!({"calculate": "datum.percentiles[1].value", "as": "p1"})
        );
    }

    #[tokio::test]
    async fn it_rejects_duplicate_percentiles() {
        let result = raster_statistics(
            multi_band_raster_source(vec![vec![1, 2, 3, 4, 5, 6]]),
            StatisticsParams {
                column_names: vec![],
                percentiles: vec![NotNan::new(0.5).unwrap(), NotNan::new(0.5).unwrap()],
            },
            world_bbox(),
        )
        .await;

        assert!(
            matches!(result, Err(error::Error::InvalidOperatorSpec{reason}) if reason == *"The percentiles must be unique.")
        );
    }

    fn world_bbox() -> BoundingBox2D {
        BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap()
    }

    #[tokio::test]
    async fn single_raster_implicit_name() {
        let result = raster_statistics(
            multi_band_raster_source(vec![vec![1, 2, 3, 4, 5, 6]]),
            StatisticsParams {
                column_names: vec![],
                percentiles: vec![],
            },
            world_bbox(),
        )
        .await
        .unwrap();

        assert_eq!(
            result,
            json!([
                {
                    "name": "band_0",
                    "valueCount": 65_341, // 361*181: the query bounds include the pixels at the right and lower edge
                    "validCount": 6,
                    "min": 1.0,
                    "max": 6.0,
                    "mean": 3.5,
                    "stddev": 1.707_825_127_659_933,
                    "percentiles": [],
                }
            ])
        );
    }

    #[tokio::test]
    async fn it_computes_statistics_for_all_bands() {
        let result = raster_statistics(
            multi_band_raster_source(vec![vec![1, 2, 3, 4, 5, 6], vec![7, 8, 9, 10, 11, 12]]),
            StatisticsParams {
                column_names: vec![],
                percentiles: vec![],
            },
            world_bbox(),
        )
        .await
        .unwrap();

        assert_eq!(
            result,
            json!([
                {
                    "name": "band_0",
                    "valueCount": 65_341, // 361*181: the query bounds include the pixels at the right and lower edge
                    "validCount": 6,
                    "min": 1.0,
                    "max": 6.0,
                    "mean": 3.5,
                    "stddev": 1.707_825_127_659_933,
                    "percentiles": [],
                },
                {
                    "name": "band_1",
                    "valueCount": 65_341,
                    "validCount": 6,
                    "min": 7.0,
                    "max": 12.0,
                    "mean": 9.5,
                    "stddev": 1.707_825_127_659_933,
                    "percentiles": [],
                },
            ])
        );
    }

    #[tokio::test]
    async fn it_computes_statistics_for_selected_bands() {
        let result = raster_statistics(
            multi_band_raster_source(vec![
                vec![1, 2, 3, 4, 5, 6],
                vec![7, 8, 9, 10, 11, 12],
                vec![13, 14, 15, 16, 17, 18],
            ]),
            StatisticsParams {
                column_names: vec!["band_2".to_string(), "band_0".to_string()],
                percentiles: vec![],
            },
            world_bbox(),
        )
        .await
        .unwrap();

        assert_eq!(
            result,
            json!([
                {
                    "name": "band_0",
                    "valueCount": 65_341,
                    "validCount": 6,
                    "min": 1.0,
                    "max": 6.0,
                    "mean": 3.5,
                    "stddev": 1.707_825_127_659_933,
                    "percentiles": [],
                },
                {
                    "name": "band_2",
                    "valueCount": 65_341,
                    "validCount": 6,
                    "min": 13.0,
                    "max": 18.0,
                    "mean": 15.5,
                    "stddev": 1.707_825_127_659_933,
                    "percentiles": [],
                },
            ])
        );
    }

    #[tokio::test]
    async fn it_fails_on_unknown_band_name() {
        let result = raster_statistics(
            multi_band_raster_source(vec![vec![1, 2, 3, 4, 5, 6]]),
            StatisticsParams {
                column_names: vec!["foo".to_string()],
                percentiles: vec![],
            },
            world_bbox(),
        )
        .await;

        assert!(
            matches!(result, Err(error::Error::InvalidOperatorSpec{reason}) if reason == *"Band 'foo' does not exist.")
        );
    }

    #[tokio::test]
    async fn it_counts_only_pixels_in_query_rectangle() {
        // the tile covers x in [0, 2) and y in (-3, 0], the query only the pixels with values 1 and 3
        let result = raster_statistics(
            multi_band_raster_source(vec![vec![1, 2, 3, 4, 5, 6]]),
            StatisticsParams {
                column_names: vec![],
                percentiles: vec![],
            },
            BoundingBox2D::new((0.2, -1.8).into(), (0.8, -0.2).into()).unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(
            result,
            json!([
                {
                    "name": "band_0",
                    "valueCount": 2,
                    "validCount": 2,
                    "min": 1.0,
                    "max": 3.0,
                    "mean": 2.0,
                    "stddev": 1.0,
                    "percentiles": [],
                }
            ])
        );
    }

    #[tokio::test]
    async fn vector_no_column() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            tile_size_in_pixels,
        };

        let vector_source = MockFeatureCollectionSource::multiple(vec![
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 7],
                &[
                    (
                        "foo",
                        FeatureData::NullableFloat(vec![
                            Some(1.0),
                            None,
                            Some(3.0),
                            None,
                            Some(f64::NAN),
                            Some(6.0),
                            Some(f64::NAN),
                        ]),
                    ),
                    (
                        "bar",
                        FeatureData::NullableFloat(vec![
                            Some(1.0),
                            Some(2.0),
                            None,
                            None,
                            Some(5.0),
                            Some(f64::NAN),
                            Some(f64::NAN),
                        ]),
                    ),
                ],
            )
            .unwrap(),
        ])
        .boxed();

        let statistics = Statistics {
            params: StatisticsParams {
                column_names: vec![],
                percentiles: vec![],
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let statistics = statistics
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let processor = statistics.query_processor().unwrap().json_vega().unwrap();

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
        let result = table_rows(&result);

        assert_eq!(
            result,
            json!([
                {
                    "name": "bar",
                    "valueCount": 7,
                    "validCount": 3,
                    "min": 1.0,
                    "max": 5.0,
                    "mean": 2.666_666_666_666_667,
                    "stddev": 1.699_673_171_197_595,
                    "percentiles": [],
                },
                {
                    "name": "foo",
                    "valueCount": 7,
                    "validCount": 3,
                    "min": 1.0,
                    "max": 6.0,
                    "mean": 3.333_333_333_333_333,
                    "stddev": 2.054_804_667_656_325_6,
                    "percentiles": [],
                },
            ])
        );
    }

    #[tokio::test]
    async fn vector_single_column() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            tile_size_in_pixels,
        };

        let vector_source = MockFeatureCollectionSource::multiple(vec![
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 7],
                &[
                    (
                        "foo",
                        FeatureData::NullableFloat(vec![
                            Some(1.0),
                            None,
                            Some(3.0),
                            None,
                            Some(f64::NAN),
                            Some(6.0),
                            Some(f64::NAN),
                        ]),
                    ),
                    (
                        "bar",
                        FeatureData::NullableFloat(vec![
                            Some(1.0),
                            Some(2.0),
                            None,
                            None,
                            Some(5.0),
                            Some(f64::NAN),
                            Some(f64::NAN),
                        ]),
                    ),
                ],
            )
            .unwrap(),
        ])
        .boxed();

        let statistics = Statistics {
            params: StatisticsParams {
                column_names: vec!["foo".to_string()],
                percentiles: vec![],
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let statistics = statistics
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let processor = statistics.query_processor().unwrap().json_vega().unwrap();

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
        let result = table_rows(&result);

        assert_eq!(
            result.to_string(),
            json!([
                {
                    "name": "foo",
                    "valueCount": 7,
                    "validCount": 3,
                    "min": 1.0,
                    "max": 6.0,
                    "mean": 3.333_333_333_333_333,
                    "stddev": 2.054_804_667_656_325_6,
                    "percentiles": [],
                },
            ])
            .to_string()
        );
    }

    #[tokio::test]
    async fn vector_two_columns() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            tile_size_in_pixels,
        };

        let vector_source = MockFeatureCollectionSource::multiple(vec![
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 7],
                &[
                    (
                        "foo",
                        FeatureData::NullableFloat(vec![
                            Some(1.0),
                            None,
                            Some(3.0),
                            None,
                            Some(f64::NAN),
                            Some(6.0),
                            Some(f64::NAN),
                        ]),
                    ),
                    (
                        "bar",
                        FeatureData::NullableFloat(vec![
                            Some(1.0),
                            Some(2.0),
                            None,
                            None,
                            Some(5.0),
                            Some(f64::NAN),
                            Some(f64::NAN),
                        ]),
                    ),
                ],
            )
            .unwrap(),
        ])
        .boxed();

        let statistics = Statistics {
            params: StatisticsParams {
                column_names: vec!["foo".to_string(), "bar".to_string()],
                percentiles: vec![],
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let statistics = statistics
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let processor = statistics.query_processor().unwrap().json_vega().unwrap();

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
        let result = table_rows(&result);

        assert_eq!(
            result,
            json!([
                {
                    "name": "foo",
                    "valueCount": 7,
                    "validCount": 3,
                    "min": 1.0,
                    "max": 6.0,
                    "mean": 3.333_333_333_333_333,
                    "stddev": 2.054_804_667_656_325_6,
                    "percentiles": [],
                },
                {
                    "name": "bar",
                    "valueCount": 7,
                    "validCount": 3,
                    "min": 1.0,
                    "max": 5.0,
                    "mean": 2.666_666_666_666_667,
                    "stddev": 1.699_673_171_197_595,
                    "percentiles": [],
                },
            ])
        );
    }

    #[tokio::test]
    async fn raster_percentile() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            tile_size_in_pixels,
        };

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_irregular(Some(TimeInterval::default())),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                TestDefault::test_default(),
                GridBoundingBox2D::new_min_max(-90, 89, -180, 179).unwrap(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let raster_source = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![RasterTile2D::new_with_tile_info(
                    TimeInterval::default(),
                    TileInformation {
                        global_geo_transform: TestDefault::test_default(),
                        global_tile_position: [0, 0].into(),
                        tile_size_in_pixels,
                    },
                    0,
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                        .unwrap()
                        .into(),
                    CacheHint::no_cache(),
                )],
                result_descriptor,
            },
        }
        .boxed();

        let statistics = Statistics {
            params: StatisticsParams {
                column_names: vec![],
                percentiles: vec![NotNan::new(0.25).unwrap(), NotNan::new(0.75).unwrap()],
            },
            sources: raster_source.into(),
        };

        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let statistics = statistics
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let processor = statistics.query_processor().unwrap().json_vega().unwrap();

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
        let result = table_rows(&result);

        assert_eq!(
            result.to_string(),
            json!([
                {
                    "name": "band",
                    "valueCount": 65_341, // 361*181: the query bounds include the pixels at the right and lower edge
                    "validCount": 6,
                    "min": 1.0,
                    "max": 6.0,
                    "mean": 3.5,
                    "stddev": 1.707_825_127_659_933,
                    "percentiles": [
                        {"percentile": 0.25, "value": 3.0},
                        {"percentile": 0.75, "value": 3.0},
                    ],
                }
            ])
            .to_string()
        );
    }

    #[tokio::test]
    async fn vector_percentiles() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            tile_size_in_pixels,
        };

        let vector_source = MockFeatureCollectionSource::multiple(vec![
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 7],
                &[
                    (
                        "foo",
                        FeatureData::NullableFloat(vec![
                            Some(1.0),
                            None,
                            Some(3.0),
                            None,
                            Some(f64::NAN),
                            Some(6.0),
                            Some(f64::NAN),
                        ]),
                    ),
                    (
                        "bar",
                        FeatureData::NullableFloat(vec![
                            Some(1.0),
                            Some(2.0),
                            None,
                            None,
                            Some(5.0),
                            Some(f64::NAN),
                            Some(f64::NAN),
                        ]),
                    ),
                ],
            )
            .unwrap(),
        ])
        .boxed();

        let statistics = Statistics {
            params: StatisticsParams {
                column_names: vec!["foo".to_string()],
                percentiles: vec![NotNan::new(0.25).unwrap(), NotNan::new(0.75).unwrap()],
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let statistics = statistics
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let processor = statistics.query_processor().unwrap().json_vega().unwrap();

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
        let result = table_rows(&result);

        assert_eq!(
            result.to_string(),
            json!([
                {
                    "name": "foo",
                    "valueCount": 7,
                    "validCount": 3,
                    "min": 1.0,
                    "max": 6.0,
                    "mean": 3.333_333_333_333_333,
                    "stddev": 2.054_804_667_656_325_6,
                    "percentiles": [
                        {"percentile": 0.25, "value": 1.0},
                        {"percentile": 0.75, "value": 6.0},
                    ],
                },
            ])
            .to_string()
        );
    }
}
