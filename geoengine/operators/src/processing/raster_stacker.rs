use crate::adapters::{
    PartialQueryRect, QueryWrapper, RasterStackerAdapter, RasterStackerSource,
    SimpleRasterStackerAdapter, SimpleRasterStackerError,
};
use crate::engine::{
    BoxRasterQueryProcessor, CanonicOperatorName, ExecutionContext, InitializedRasterOperator,
    InitializedSources, MultipleRasterSources, Operator, OperatorName, ProcessingGraphOperatorPath,
    QueryContext, QueryProcessor, RasterBandDescriptor, RasterOperator, RasterQueryProcessor,
    RasterResultDescriptor, TypedRasterOperator, TypedRasterQueryProcessor,
};
use crate::error::{
    InvalidNumberOfRasterStackerInputs, RasterInputsMustHaveSameSpatialReferenceAndDatatype,
};
use crate::optimization::OptimizationError;
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use geoengine_datatypes::primitives::{BandSelection, RasterQueryRectangle, SpatialResolution};
use geoengine_datatypes::raster::{
    DynamicRasterDataType, GridBoundingBox2D, Pixel, RasterTile2D, RenameBands,
};
use serde::{Deserialize, Serialize};
use snafu::ensure;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RasterStackerParams {
    pub rename_bands: RenameBands,
}

/// This `QueryProcessor` stacks all of it's inputs into a single raster time-series.
/// It does so by querying all of it's inputs outputting them by band, space and then time.
/// The tiles are automatically temporally aligned.
///
/// All inputs must have the same data type and spatial reference.
pub type RasterStacker = Operator<RasterStackerParams, MultipleRasterSources>;

impl OperatorName for RasterStacker {
    const TYPE_NAME: &'static str = "RasterStacker";
}

#[async_trait]
impl RasterOperator for RasterStacker {
    async fn _initialize(
        self: Box<Self>,
        path: ProcessingGraphOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let name = CanonicOperatorName::from(&self);

        ensure!(
            !self.sources.rasters.is_empty() && self.sources.rasters.len() <= 8,
            InvalidNumberOfRasterStackerInputs
        );

        let raster_sources = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?
            .rasters;

        let in_descriptors = raster_sources
            .iter()
            .map(InitializedRasterOperator::result_descriptor)
            .collect::<Vec<_>>();

        ensure!(
            in_descriptors.iter().all(|d| d.spatial_reference
                == in_descriptors[0].spatial_reference
                && d.data_type == in_descriptors[0].data_type),
            RasterInputsMustHaveSameSpatialReferenceAndDatatype {
                datatypes: in_descriptors
                    .iter()
                    .map(|d| d.data_type)
                    .collect::<Vec<_>>(),
                spatial_references: in_descriptors
                    .iter()
                    .map(|d| d.spatial_reference)
                    .collect::<Vec<_>>(),
            }
        );

        let first_spatial_grid = in_descriptors[0].spatial_grid;
        let result_spatial_grid = in_descriptors
            .iter()
            .skip(1)
            .map(|x| x.spatial_grid_descriptor())
            .try_fold(first_spatial_grid, |a, &b| {
                a.merge(&b)
                    .ok_or(crate::error::Error::CantMergeSpatialGridDescriptor { a, b })
            })?;

        let time = in_descriptors.iter().skip(1).map(|rd| rd.time).fold(
            in_descriptors
                .first()
                .expect("There must be at least one input")
                .time,
            |a, b| a.merge(b),
        );

        let data_type = in_descriptors[0].data_type;
        let spatial_reference = in_descriptors[0].spatial_reference;

        let bands_per_source = in_descriptors
            .iter()
            .map(|d| d.bands.count())
            .collect::<Vec<_>>();

        let band_names = self.params.rename_bands.apply(
            in_descriptors
                .iter()
                .map(|d| d.bands.iter().map(|b| b.name.clone()).collect())
                .collect(),
        )?;

        let output_band_descriptors = in_descriptors
            .into_iter()
            .flat_map(|d| d.bands.iter().cloned())
            .zip(band_names)
            .map(|(descriptor, name)| RasterBandDescriptor { name, ..descriptor })
            .collect::<Vec<_>>()
            .try_into()?;

        let result_descriptor = RasterResultDescriptor {
            data_type,
            spatial_reference,
            time,
            spatial_grid: result_spatial_grid,
            bands: output_band_descriptors,
        };

        Ok(Box::new(InitializedRasterStacker {
            name,
            path,
            result_descriptor,
            rename_bands: self.params.rename_bands.clone(),
            raster_sources,
            bands_per_source,
        }))
    }

    span_fn!(RasterStacker);
}

pub struct InitializedRasterStacker {
    name: CanonicOperatorName,
    path: ProcessingGraphOperatorPath,
    result_descriptor: RasterResultDescriptor,
    rename_bands: RenameBands,
    raster_sources: Vec<Box<dyn InitializedRasterOperator>>,
    bands_per_source: Vec<u32>,
}

impl InitializedRasterOperator for InitializedRasterStacker {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let typed_raster_processors = self
            .raster_sources
            .iter()
            .map(InitializedRasterOperator::query_processor)
            .collect::<Result<Vec<_>>>()?;

        // unpack all processors
        let datatype = typed_raster_processors[0].raster_data_type();

        let bands_per_source = self.bands_per_source.clone();

        // TODO: use a macro to unpack all the input processor to the same datatype?
        Ok(match datatype {
            geoengine_datatypes::raster::RasterDataType::U8 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_u8().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::U8(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::U16 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_u16().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::U16(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::U32 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_u32().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::U32(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::U64 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_u64().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::U64(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::I8 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_i8().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::I8(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::I16 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_i16().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::I16(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::I32 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_i32().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::I32(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::I64 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_i64().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::I64(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::F32 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_f32().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::F32(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::F64 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_f64().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::F64(Box::new(p))
            }
        })
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        RasterStacker::TYPE_NAME
    }

    fn path(&self) -> ProcessingGraphOperatorPath {
        self.path.clone()
    }

    fn optimize(
        &self,
        resolution: SpatialResolution,
    ) -> Result<TypedRasterOperator, OptimizationError> {
        Ok(RasterStacker {
            params: RasterStackerParams {
                rename_bands: self.rename_bands.clone(),
            },
            sources: MultipleRasterSources {
                rasters: self
                    .raster_sources
                    .iter()
                    .map(|s| s.optimize(resolution))
                    .collect::<Result<Vec<_>, _>>()?,
            },
        }
        .typed())
    }
}

pub(crate) struct RasterStackerProcessor<T> {
    sources: Vec<BoxRasterQueryProcessor<T>>,
    result_descriptor: RasterResultDescriptor,
    bands_per_source: Vec<u32>,
}

impl<T> RasterStackerProcessor<T> {
    pub fn new(
        sources: Vec<BoxRasterQueryProcessor<T>>,
        result_descriptor: RasterResultDescriptor,
        bands_per_source: Vec<u32>,
    ) -> Self {
        Self {
            sources,
            result_descriptor,
            bands_per_source,
        }
    }
}

#[async_trait]
impl<T> QueryProcessor for RasterStackerProcessor<T>
where
    T: Pixel,
{
    type Output = RasterTile2D<T>;
    type ResultDescription = RasterResultDescriptor;
    type Selection = BandSelection;
    type SpatialBounds = GridBoundingBox2D;

    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<T>>>> {
        // First try to create simple raster stacker for temporal aligned data
        let sdp = SimpleRasterStackerAdapter::<
            SimpleRasterStackerAdapter<BoxRasterQueryProcessor<T>>,
        >::stack_selected_regular_aligned_raster_bands(&query, ctx, &self.sources)
        .await;

        let x = match sdp {
            Ok(p) => Ok(Some(p)),
            Err(SimpleRasterStackerError::InputsNotTemporalAligned) => Ok(None),
            Err(e) => Err(crate::error::Error::SimpleRasterStacker { source: e }),
        }?;

        if let Some(sdp) = x {
            tracing::trace!("Using regular time aligned stacker processor");
            return Ok(Box::pin(sdp));
        }

        // if the simple stacker can not be used, try to use the more complex stacker

        tracing::trace!("Using non-regular time aligned stacker processor");

        let mut sources = vec![];
        let tiling_strat = self
            .result_descriptor
            .tiling_grid_definition(ctx.tiling_specification())
            .generate_data_tiling_strategy();

        for (idx, source) in self.sources.iter().enumerate() {
            // FIXME: find a better way to do the selection and avoid work done without benefit.
            let bands = BandSelection::first_n(self.bands_per_source[idx]);

            sources.push(RasterStackerSource {
                queryable: QueryWrapper { p: source, ctx },
                band_idxs: bands,
            });
        }

        #[cfg(debug_assertions)]
        {
            let num_input_bands = self.bands_per_source.iter().sum::<u32>() as usize;
            let num_query_bands = query.attributes().as_vec().len();

            let fact = num_input_bands as f32 / num_query_bands as f32;

            tracing::debug!(
                "StackerAdapter queries {num_input_bands} to produce {num_query_bands}. This is {fact}x the work required."
            );
        }

        let query_band_selection = query.attributes().clone();
        let partial_query = PartialQueryRect::from(query);
        let output =
            RasterStackerAdapter::new(sources, partial_query, tiling_strat).filter_map(move |o| {
                let pred = match o {
                    Ok(tile) if query_band_selection.contains(tile.band) => Some(Ok(tile)),
                    Ok(_) => None,
                    Err(e) => Some(Err(e)),
                };
                std::future::ready(pred)
            });

        Ok(Box::pin(output))
    }

    fn result_descriptor(&self) -> &Self::ResultDescription {
        &self.result_descriptor
    }
}

#[async_trait]
impl<T> RasterQueryProcessor for RasterStackerProcessor<T>
where
    T: Pixel,
{
    type RasterType = T;

    async fn _time_query<'a>(
        &'a self,
        query: geoengine_datatypes::primitives::TimeInterval,
        ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<futures::stream::BoxStream<'a, Result<geoengine_datatypes::primitives::TimeInterval>>>
    {
        let mut time_sources = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            let s = source.time_query(query, ctx).await?;
            time_sources.push(s);
        }
        // the merge splits intervals of one source at the bounds of another, also outside the query
        let output = crate::adapters::TimeIntervalStreamMerge::new(time_sources)
            .try_filter(move |time| futures::future::ready(time.intersects(&query)));
        Ok(Box::pin(output))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use futures::{StreamExt, TryStreamExt};
    use geoengine_datatypes::{
        primitives::{TimeInstance, TimeInterval, TimeStep},
        raster::{GridBoundingBox2D, RasterDataType},
        util::test::{AsciiRasterTiles, TestDefault, assert_raster_tiles_eq_ascii},
    };
    use indoc::indoc;

    use crate::{
        engine::{MockExecutionContext, SingleRasterSource, TimeDescriptor, TypedRasterOperator},
        mock::{MockRasterSource, MockRasterSourceParams},
        processing::{Expression, ExpressionParams},
        source::{GdalSource, GdalSourceParameters},
        util::{gdal::add_ndvi_dataset, test::AsciiRasterTilesExt},
    };

    use super::*;

    /// A mock source for `raster` whose result descriptor has the time descriptor `time`.
    fn mock_source(raster: &AsciiRasterTiles, time: TimeDescriptor) -> TypedRasterOperator {
        MockRasterSource {
            params: MockRasterSourceParams {
                data: raster.tiles::<u8>(),
                result_descriptor: RasterResultDescriptor {
                    time,
                    ..raster.result_descriptor::<u8>()
                },
            },
        }
        .typed()
    }

    #[tokio::test]
    async fn it_stacks() {
        it_stacks_impl(TimeDescriptor::new_irregular(None)).await;
    }

    #[tokio::test]
    async fn it_stacks_regular() {
        it_stacks_impl(TimeDescriptor::new_regular_with_epoch(
            Some(TimeInterval::new_unchecked(0, 5)),
            TimeStep::millis(5).unwrap(),
        ))
        .await;
    }

    async fn it_stacks_impl(time: TimeDescriptor) {
        let raster1 = AsciiRasterTiles::new(indoc! {"
            [0, 5)
            +-------+-------+
            |  0  1 |  4  5 |
            |  2  3 |  6  7 |
            +-------+-------+
            [5, 10)
            +-------+-------+
            |  8  9 | 12 13 |
            | 10 11 | 14 15 |
            +-------+-------+
            "})
        .tile_origin([-1, 0]);

        let raster2 = AsciiRasterTiles::new(indoc! {"
            [0, 5)
            +-------+-------+
            | 16 17 | 20 21 |
            | 18 19 | 22 23 |
            +-------+-------+
            [5, 10)
            +-------+-------+
            | 24 25 | 28 29 |
            | 26 27 | 30 31 |
            +-------+-------+
            "})
        .tile_origin([-1, 0]);

        let stacker = RasterStacker {
            params: RasterStackerParams {
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![mock_source(&raster1, time), mock_source(&raster2, time)],
            },
        }
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster1.tiling_specification());

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 10),
            [0, 1].try_into().unwrap(),
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let op = stacker
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 5)
                +-------+-------+
                |  0  1 |  4  5 |
                |  2  3 |  6  7 |
                +-------+-------+
                [0, 5) band 1
                +-------+-------+
                | 16 17 | 20 21 |
                | 18 19 | 22 23 |
                +-------+-------+
                [5, 10)
                +-------+-------+
                |  8  9 | 12 13 |
                | 10 11 | 14 15 |
                +-------+-------+
                [5, 10) band 1
                +-------+-------+
                | 24 25 | 28 29 |
                | 26 27 | 30 31 |
                +-------+-------+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn it_returns_only_times_of_the_query() {
        let static_raster = AsciiRasterTiles::new(indoc! {"
            [0, 10)
            +-----+
            | 0 1 |
            | 2 3 |
            +-----+
            "})
        .tile_origin([-1, 0]);

        let time_series = AsciiRasterTiles::new(indoc! {"
            [0, 5)
            +-----+
            | 0 1 |
            | 2 3 |
            +-----+
            [5, 10)
            +-----+
            | 0 1 |
            | 2 3 |
            +-----+
            "})
        .tile_origin([-1, 0]);

        let irregular = TimeDescriptor::new_irregular(None);
        // the merge splits [0, 10) at 5, also into [0, 5), which ends where the query starts
        let stacker = RasterStacker {
            params: RasterStackerParams {
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![
                    mock_source(&static_raster, irregular),
                    mock_source(&time_series, irregular),
                ],
            },
        }
        .typed();
        let exe_ctx =
            MockExecutionContext::new_with_tiling_spec(static_raster.tiling_specification());
        let qp = stacker
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let times: Vec<TimeInterval> = qp
            .time_query(
                TimeInterval::new_unchecked(5, 6),
                &exe_ctx.mock_query_context_test_default(),
            )
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        assert_eq!(times, vec![TimeInterval::new_unchecked(5, 10)]);
    }

    #[tokio::test]
    async fn it_stacks_stacks() {
        it_stacks_stacks_impl(TimeDescriptor::new_irregular(None)).await;
    }

    #[tokio::test]
    async fn it_stacks_stacks_regular() {
        it_stacks_stacks_impl(TimeDescriptor::new_regular_with_epoch(
            Some(TimeInterval::new_unchecked(0, 10)),
            TimeStep::millis(5).unwrap(),
        ))
        .await;
    }

    #[allow(clippy::too_many_lines)]
    async fn it_stacks_stacks_impl(time: TimeDescriptor) {
        let raster1 = AsciiRasterTiles::new(indoc! {"
            [0, 5)
            +-------+-------+
            |  0  1 |  4  5 |
            |  2  3 |  6  7 |
            +-------+-------+
            [0, 5) band 1
            +-------+-------+
            |  3  2 |  7  6 |
            |  1  0 |  5  4 |
            +-------+-------+
            [5, 10)
            +-------+-------+
            |  8  9 | 12 13 |
            | 10 11 | 14 15 |
            +-------+-------+
            [5, 10) band 1
            +-------+-------+
            | 11 10 | 15 14 |
            |  9  8 | 13 12 |
            +-------+-------+
            "})
        .tile_origin([-1, 0]);

        let raster2 = AsciiRasterTiles::new(indoc! {"
            [0, 5)
            +-------+-------+
            | 16 17 | 20 21 |
            | 18 19 | 22 23 |
            +-------+-------+
            [0, 5) band 1
            +-------+-------+
            | 19 18 | 32 22 |
            | 17 16 | 21 20 |
            +-------+-------+
            [5, 10)
            +-------+-------+
            | 24 25 | 28 29 |
            | 26 27 | 30 31 |
            +-------+-------+
            [5, 10) band 1
            +-------+-------+
            | 27 26 | 31 30 |
            | 25 24 | 39 28 |
            +-------+-------+
            "})
        .tile_origin([-1, 0]);

        let stacker = RasterStacker {
            params: RasterStackerParams {
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![mock_source(&raster1, time), mock_source(&raster2, time)],
            },
        }
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster1.tiling_specification());

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-1, 0], [-1, 2]).unwrap(),
            TimeInterval::new_unchecked(0, 10),
            [0, 1, 2, 3].try_into().unwrap(),
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let op = stacker
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 5)
                +-------+-------+
                |  0  1 |  4  5 |
                |  2  3 |  6  7 |
                +-------+-------+
                [0, 5) band 1
                +-------+-------+
                |  3  2 |  7  6 |
                |  1  0 |  5  4 |
                +-------+-------+
                [0, 5) band 2
                +-------+-------+
                | 16 17 | 20 21 |
                | 18 19 | 22 23 |
                +-------+-------+
                [0, 5) band 3
                +-------+-------+
                | 19 18 | 32 22 |
                | 17 16 | 21 20 |
                +-------+-------+
                [5, 10)
                +-------+-------+
                |  8  9 | 12 13 |
                | 10 11 | 14 15 |
                +-------+-------+
                [5, 10) band 1
                +-------+-------+
                | 11 10 | 15 14 |
                |  9  8 | 13 12 |
                +-------+-------+
                [5, 10) band 2
                +-------+-------+
                | 24 25 | 28 29 |
                | 26 27 | 30 31 |
                +-------+-------+
                [5, 10) band 3
                +-------+-------+
                | 27 26 | 31 30 |
                | 25 24 | 39 28 |
                +-------+-------+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn it_selects_band_from_stack() {
        it_selects_band_from_stack_impl(TimeDescriptor::new_irregular(None)).await;
    }

    #[tokio::test]
    async fn it_selects_band_from_stack_regular() {
        it_selects_band_from_stack_impl(TimeDescriptor::new_regular_with_epoch(
            Some(TimeInterval::new_unchecked(0, 10)),
            TimeStep::millis(5).unwrap(),
        ))
        .await;
    }

    async fn it_selects_band_from_stack_impl(time: TimeDescriptor) {
        let raster1 = AsciiRasterTiles::new(indoc! {"
            [0, 5)
            +-------+-------+
            |  0  1 |  4  5 |
            |  2  3 |  6  7 |
            +-------+-------+
            [5, 10)
            +-------+-------+
            |  8  9 | 12 13 |
            | 10 11 | 14 15 |
            +-------+-------+
            "})
        .tile_origin([-1, 0]);

        let raster2 = AsciiRasterTiles::new(indoc! {"
            [0, 5)
            +-------+-------+
            | 16 17 | 20 21 |
            | 18 19 | 22 23 |
            +-------+-------+
            [5, 10)
            +-------+-------+
            | 24 25 | 28 29 |
            | 26 27 | 30 31 |
            +-------+-------+
            "})
        .tile_origin([-1, 0]);

        let stacker = RasterStacker {
            params: RasterStackerParams {
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![mock_source(&raster1, time), mock_source(&raster2, time)],
            },
        }
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster1.tiling_specification());

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-1, 0], [-1, 2]).unwrap(),
            TimeInterval::new_unchecked(0, 10),
            1.into(),
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let op = stacker
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        // band 1 of the stack is band 0 of the second source
        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 5) band 1
                +-------+-------+
                | 16 17 | 20 21 |
                | 18 19 | 22 23 |
                +-------+-------+
                [5, 10) band 1
                +-------+-------+
                | 24 25 | 28 29 |
                | 26 27 | 30 31 |
                +-------+-------+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn it_answers_unsorted_band_selection_in_ascending_order() {
        it_answers_unsorted_band_selection_in_ascending_order_impl(TimeDescriptor::new_irregular(
            None,
        ))
        .await;
    }

    #[tokio::test]
    async fn it_answers_unsorted_band_selection_in_ascending_order_regular() {
        it_answers_unsorted_band_selection_in_ascending_order_impl(
            TimeDescriptor::new_regular_with_epoch(
                Some(TimeInterval::new_unchecked(0, 10)),
                TimeStep::millis(5).unwrap(),
            ),
        )
        .await;
    }

    /// Both stacker paths must answer `[1, 0]` like `[0, 1]`, with the band numbers at the right data.
    async fn it_answers_unsorted_band_selection_in_ascending_order_impl(time: TimeDescriptor) {
        let raster1 = AsciiRasterTiles::new(indoc! {"
            [0, 5)
            +-------+-------+
            |  0  1 |  4  5 |
            |  2  3 |  6  7 |
            +-------+-------+
            [5, 10)
            +-------+-------+
            |  8  9 | 12 13 |
            | 10 11 | 14 15 |
            +-------+-------+
            "})
        .tile_origin([-1, 0]);

        let raster2 = AsciiRasterTiles::new(indoc! {"
            [0, 5)
            +-------+-------+
            | 16 17 | 20 21 |
            | 18 19 | 22 23 |
            +-------+-------+
            [5, 10)
            +-------+-------+
            | 24 25 | 28 29 |
            | 26 27 | 30 31 |
            +-------+-------+
            "})
        .tile_origin([-1, 0]);

        let stacker = RasterStacker {
            params: RasterStackerParams {
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![mock_source(&raster1, time), mock_source(&raster2, time)],
            },
        }
        .typed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(raster1.tiling_specification());

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-1, 0], [-1, 2]).unwrap(),
            TimeInterval::new_unchecked(0, 10),
            vec![1, 0].try_into().unwrap(),
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let op = stacker
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_raster_tiles_eq_ascii(
            &result,
            &AsciiRasterTiles::new(indoc! {"
                [0, 5)
                +-------+-------+
                |  0  1 |  4  5 |
                |  2  3 |  6  7 |
                +-------+-------+
                [0, 5) band 1
                +-------+-------+
                | 16 17 | 20 21 |
                | 18 19 | 22 23 |
                +-------+-------+
                [5, 10)
                +-------+-------+
                |  8  9 | 12 13 |
                | 10 11 | 14 15 |
                +-------+-------+
                [5, 10) band 1
                +-------+-------+
                | 24 25 | 28 29 |
                | 26 27 | 30 31 |
                +-------+-------+
                "})
            .tile_origin([-1, 0]),
        );
    }

    #[tokio::test]
    async fn it_stacks_ndvi() {
        let mut exe_ctx = MockExecutionContext::test_default();

        let ndvi_id = add_ndvi_dataset(&mut exe_ctx);

        let expression = Expression {
            params: ExpressionParams {
                expression: "if A > 100 { A } else { 0 }".into(),
                output_type: RasterDataType::U8,
                output_band: None,
                map_no_data: false,
            },
            sources: SingleRasterSource {
                raster: GdalSource {
                    params: GdalSourceParameters {
                        data: ndvi_id.clone(),
                        overview_level: None,
                    },
                }
                .typed(),
            },
        }
        .typed();

        let operator = RasterStacker {
            params: RasterStackerParams {
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![
                    GdalSource {
                        params: GdalSourceParameters::new(ndvi_id),
                    }
                    .typed(),
                    expression,
                ],
            },
        }
        .typed();

        let operator = operator
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let processor = operator.query_processor().unwrap().get_u8().unwrap();

        let query_ctx = exe_ctx.mock_query_context_test_default();

        // query both bands
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap(),
            TimeInterval::new_unchecked(
                TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
                TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
            ),
            [0, 1].try_into().unwrap(),
        );

        let result = processor
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_ne!(result, [] as [_; 0]);

        // query only first band
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap(),
            TimeInterval::new_unchecked(
                TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
                TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
            ),
            [0].try_into().unwrap(),
        );

        let result = processor
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result_0 = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_ne!(result_0, [] as [_; 0]);
        assert!(result_0.iter().all(|t| t.band == 0));

        // query only second band
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap(),
            TimeInterval::new_unchecked(
                TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
                TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
            ),
            [1].try_into().unwrap(),
        );

        let result = processor
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result_1 = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_ne!(result_1, [] as [_; 0]);
        assert!(result_1.iter().all(|t| t.band == 1));

        assert_eq!(result_0.len(), result_1.len());
    }

    #[test]
    fn it_renames() {
        let names = vec![
            vec!["foo".to_string(), "bar".to_string()],
            vec!["foo".to_string(), "bla".to_string()],
            vec!["foo".to_string(), "baz".to_string()],
        ];

        assert_eq!(
            RenameBands::Default.apply(names.clone()).unwrap(),
            vec![
                "foo".to_string(),
                "bar".to_string(),
                "foo (1)".to_string(),
                "bla".to_string(),
                "foo (2)".to_string(),
                "baz".to_string()
            ]
        );

        assert_eq!(
            RenameBands::Suffix(vec![
                String::new(),
                " second".to_string(),
                " third".to_string()
            ])
            .apply(names.clone())
            .unwrap(),
            vec![
                "foo".to_string(),
                "bar".to_string(),
                "foo second".to_string(),
                "bla second".to_string(),
                "foo third".to_string(),
                "baz third".to_string()
            ]
        );

        assert_eq!(
            RenameBands::Rename(vec![
                "A".to_string(),
                "B".to_string(),
                "C".to_string(),
                "D".to_string(),
                "E".to_string(),
                "F".to_string()
            ])
            .apply(names.clone())
            .unwrap(),
            vec![
                "A".to_string(),
                "B".to_string(),
                "C".to_string(),
                "D".to_string(),
                "E".to_string(),
                "F".to_string()
            ]
        );
    }
}
