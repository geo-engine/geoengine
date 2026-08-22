use crate::adapters::{
    PartialQueryRect, QueryWrapper, RasterStackerAdapter, RasterStackerSource,
    SimpleRasterStackerAdapter, SimpleRasterStackerError,
};
use crate::engine::{
    BoxRasterQueryProcessor, CanonicOperatorName, ExecutionContext, InitializedRasterOperator,
    InitializedSources, MultipleRasterSources, Operator, OperatorName, QueryContext,
    QueryProcessor, RasterBandDescriptor, RasterOperator, RasterQueryProcessor,
    RasterResultDescriptor, SpatialGridDescriptor, TypedRasterQueryProcessor, WorkflowOperatorPath,
};
use crate::error::{
    InvalidNumberOfRasterStackerInputs, RasterInputsMustHaveSameSpatialReferenceAndDatatype,
};
use crate::optimization::OptimizationError;
use crate::processing::retile::ReTileProcessor;
use crate::util::Result;
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use geoengine_datatypes::primitives::{
    BandSelection, Coordinate2D, RasterQueryRectangle, SpatialResolution,
};
use geoengine_datatypes::raster::{
    DynamicRasterDataType, GridBoundingBox2D, Pixel, RasterTile2D, RenameBands, TilingSpecification,
};
use serde::{Deserialize, Serialize};
use snafu::ensure;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RasterStackerParams {
    pub rename_bands: RenameBands,
    #[serde(default)]
    pub output_origin: Option<Coordinate2D>,
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

#[typetag::serde]
#[async_trait]
#[allow(clippy::too_many_lines)]
impl RasterOperator for RasterStacker {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
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

        // stacking blits input cores onto one grid: halos would be lost
        in_descriptors
            .iter()
            .try_for_each(|d| d.ensure_no_tile_overlap(RasterStacker::TYPE_NAME))?;

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

        // All inputs must share the same resolution and be pixel-aligned to the output grid.
        // The output grid uses the first input's pixel size; its origin is the first input's
        // origin unless overridden via `output_origin`. Inputs that need a different resolution
        // or a non-aligned origin must be re-tiled/resampled before stacking.
        let first_spatial_grid = in_descriptors[0].spatial_grid_descriptor();
        let output_origin = self
            .params
            .output_origin
            .unwrap_or_else(|| first_spatial_grid.geo_transform().origin_coordinate);

        let mut result_grid = first_spatial_grid
            .spatial_grid
            .with_moved_origin_exact_grid(output_origin)
            .ok_or(crate::error::Error::RasterInputsNotReTileCompatible {
                origin: output_origin,
                index: 0,
            })?;

        for (idx, descriptor) in in_descriptors.iter().enumerate() {
            let aligned_grid = descriptor
                .spatial_grid_descriptor()
                .spatial_grid
                .with_moved_origin_exact_grid(output_origin)
                .ok_or(crate::error::Error::RasterInputsNotReTileCompatible {
                    origin: output_origin,
                    index: idx,
                })?;
            result_grid = result_grid.merge(&aligned_grid).ok_or(
                crate::error::Error::RasterInputsNotReTileCompatible {
                    origin: output_origin,
                    index: idx,
                },
            )?;
        }

        let output_tile_size = first_spatial_grid.tile_size;
        if output_tile_size.axis_size_y() == 0 || output_tile_size.axis_size_x() == 0 {
            return Err(crate::error::Error::InvalidTileSize {
                tile_size: output_tile_size,
            });
        }
        let result_spatial_grid = SpatialGridDescriptor::new_source(result_grid, output_tile_size);
        let output_tiling_spec = TilingSpecification::new(output_tile_size, output_origin);

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
            output_tiling_spec,
        }))
    }

    span_fn!(RasterStacker);
}

pub struct InitializedRasterStacker {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    result_descriptor: RasterResultDescriptor,
    rename_bands: RenameBands,
    raster_sources: Vec<Box<dyn InitializedRasterOperator>>,
    bands_per_source: Vec<u32>,
    output_tiling_spec: TilingSpecification,
}

impl InitializedRasterOperator for InitializedRasterStacker {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    #[allow(clippy::too_many_lines)]
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let typed_raster_processors = self
            .raster_sources
            .iter()
            .map(InitializedRasterOperator::query_processor)
            .collect::<Result<Vec<_>>>()?;

        // unpack all processors
        let datatype = typed_raster_processors[0].raster_data_type();

        let bands_per_source = self.bands_per_source.clone();
        let target_spatial_grid = *self.result_descriptor.spatial_grid_descriptor();
        let tiling_spec = self.output_tiling_spec;

        macro_rules! stacker_arm {
            ($getter:ident, $variant:ident) => {{
                let inputs = typed_raster_processors
                    .into_iter()
                    .zip(self.raster_sources.iter())
                    .map(|(p, source)| {
                        re_tile_source(
                            p.$getter().expect(
                                "all inputs should have the same datatype because it was checked in the initialization of the operator",
                            ),
                            source.as_ref(),
                            target_spatial_grid,
                            tiling_spec,
                        )
                    })
                    .collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::$variant(Box::new(p))
            }};
        }
        Ok(match datatype {
            geoengine_datatypes::raster::RasterDataType::U8 => stacker_arm!(get_u8, U8),
            geoengine_datatypes::raster::RasterDataType::U16 => stacker_arm!(get_u16, U16),
            geoengine_datatypes::raster::RasterDataType::U32 => stacker_arm!(get_u32, U32),
            geoengine_datatypes::raster::RasterDataType::U64 => stacker_arm!(get_u64, U64),
            geoengine_datatypes::raster::RasterDataType::I8 => stacker_arm!(get_i8, I8),
            geoengine_datatypes::raster::RasterDataType::I16 => stacker_arm!(get_i16, I16),
            geoengine_datatypes::raster::RasterDataType::I32 => stacker_arm!(get_i32, I32),
            geoengine_datatypes::raster::RasterDataType::I64 => stacker_arm!(get_i64, I64),
            geoengine_datatypes::raster::RasterDataType::F32 => stacker_arm!(get_f32, F32),
            geoengine_datatypes::raster::RasterDataType::F64 => stacker_arm!(get_f64, F64),
        })
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        RasterStacker::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }

    fn optimize(
        &self,
        resolution: SpatialResolution,
    ) -> Result<Box<dyn RasterOperator>, OptimizationError> {
        Ok(RasterStacker {
            params: RasterStackerParams {
                rename_bands: self.rename_bands.clone(),
                output_origin: Some(self.output_tiling_spec.tiling_origin_reference()),
            },
            sources: MultipleRasterSources {
                rasters: self
                    .raster_sources
                    .iter()
                    .map(|s| s.optimize(resolution))
                    .collect::<Result<Vec<_>, _>>()?,
            },
        }
        .boxed())
    }
}

/// Wraps a source processor so that its tiles are produced on the stacker's output grid.
///
/// Re-alignment is a blit-only `ReTile` (same pixel size, integer-pixel-aligned origins),
/// which was already validated in [`RasterStacker::_initialize`].
///
/// Sources that already produce tiles on the output grid are passed through unchanged.
fn re_tile_source<T: Pixel>(
    processor: BoxRasterQueryProcessor<T>,
    source: &dyn InitializedRasterOperator,
    target_spatial_grid: SpatialGridDescriptor,
    tiling_spec: TilingSpecification,
) -> BoxRasterQueryProcessor<T> {
    // ponytail: pass-through avoids unnecessary re-tiling of already-aligned inputs.
    if source.result_descriptor().spatial_grid_descriptor() == &target_spatial_grid {
        return processor;
    }

    let target_desc = RasterResultDescriptor {
        spatial_grid: target_spatial_grid,
        ..source.result_descriptor().clone()
    };

    ReTileProcessor::new(processor, target_desc, tiling_spec).boxed()
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
            .tiling_grid_definition()
            .tiling_strategy();

        for (idx, source) in self.sources.iter().enumerate() {
            // FIXME: find a better way to do the selection and avoid work done without benefit.
            let bands = BandSelection::first_n(self.bands_per_source[idx]);

            sources.push(RasterStackerSource {
                queryable: QueryWrapper { p: source, ctx },
                band_idxs: bands.as_vec(),
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
        let output = crate::adapters::TimeIntervalStreamMerge::new(time_sources);
        Ok(Box::pin(output))
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::raster::TileOverlap;
    use geoengine_datatypes::raster::TileSize;
    use std::str::FromStr;

    use futures::StreamExt;
    use geoengine_datatypes::{
        primitives::{CacheHint, Coordinate2D, TimeInstance, TimeInterval, TimeStep},
        raster::{
            GeoTransform, Grid, GridBoundingBox2D, GridShape, RasterDataType,
            TilesEqualIgnoringCacheHint,
        },
        spatial_reference::SpatialReference,
        util::test::{TestDefault, assert_eq_two_list_of_tiles},
    };

    use crate::{
        engine::{
            MockExecutionContext, RasterBandDescriptor, RasterBandDescriptors, SingleRasterSource,
            SpatialGridDescriptor, TimeDescriptor,
        },
        mock::{MockRasterSource, MockRasterSourceParams},
        processing::{Expression, ExpressionParams},
        source::{GdalSource, GdalSourceParameters},
        util::gdal::add_ndvi_dataset,
    };

    use super::*;

    #[tokio::test]
    async fn it_stacks() {
        it_stacks_impl(crate::engine::TimeDescriptor::new_irregular(None)).await;
    }

    #[tokio::test]

    async fn it_stacks_regular() {
        it_stacks_impl(crate::engine::TimeDescriptor::new_regular_with_epoch(
            Some(TimeInterval::new_unchecked(0, 5)),
            TimeStep::millis(5).unwrap(),
        ))
        .await;
    }

    #[allow(clippy::too_many_lines)]
    async fn it_stacks_impl(time_desc: crate::engine::TimeDescriptor) {
        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
        ];

        let data2: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![20, 21, 22, 23])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
        ];

        let result_descriptor1 = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: time_desc,
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
                TileSize::new(2, 2),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: result_descriptor1.clone(),
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor: result_descriptor1,
            },
        }
        .boxed();

        let stacker = RasterStacker {
            params: RasterStackerParams {
                output_origin: None,
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![mrs1, mrs2],
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size = TileSize(GridShape {
            shape_array: [2, 2],
        });

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 10),
            [0, 1].try_into().unwrap(),
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let op = stacker
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
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

        let expected: Vec<_> = data
            .into_iter()
            .zip(data2.into_iter().map(|mut tile| {
                tile.band = 1;
                tile
            }))
            .flat_map(|(a, b)| vec![a.clone(), b.clone()])
            .collect();

        assert!(expected.tiles_equal_ignoring_cache_hint(&result));
    }

    #[tokio::test]
    async fn it_stacks_stacks() {
        it_stacks_stacks_impl(crate::engine::TimeDescriptor::new_irregular(None)).await;
    }

    #[tokio::test]
    async fn it_stacks_stacks_regular() {
        it_stacks_stacks_impl(crate::engine::TimeDescriptor::new_regular_with_epoch(
            Some(TimeInterval::new_unchecked(0, 10)),
            TimeStep::millis(5).unwrap(),
        ))
        .await;
    }

    #[allow(clippy::too_many_lines)]
    async fn it_stacks_stacks_impl(time_desc: crate::engine::TimeDescriptor) {
        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![3, 2, 1, 0]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 6, 5, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![11, 10, 9, 8]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![15, 14, 13, 12])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
        ];

        let data2: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 18, 17, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![20, 21, 22, 23])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![32, 22, 21, 20])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![27, 26, 25, 24])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![31, 30, 39, 28])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
        ];

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: time_desc,
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
                TileSize::new(2, 2),
            ),
            bands: RasterBandDescriptors::new(vec![
                RasterBandDescriptor::new_unitless("band_0".into()),
                RasterBandDescriptor::new_unitless("band_1".into()),
            ])
            .unwrap(),
        };

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: result_descriptor.clone(),
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor,
            },
        }
        .boxed();

        let stacker = RasterStacker {
            params: RasterStackerParams {
                output_origin: None,
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![mrs1, mrs2],
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size = TileSize(GridShape {
            shape_array: [2, 2],
        });

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-1, 0], [-1, 2]).unwrap(),
            TimeInterval::new_unchecked(0, 10),
            [0, 1, 2, 3].try_into().unwrap(),
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let op = stacker
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
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

        let expected: Vec<_> = data
            .chunks(2)
            .zip(
                data2
                    .into_iter()
                    .map(|mut tile| {
                        tile.band += 2;
                        tile
                    })
                    .collect::<Vec<_>>()
                    .chunks(2),
            )
            .flat_map(|(chunk1, chunk2)| chunk1.iter().chain(chunk2.iter()))
            .cloned()
            .collect();

        assert!(expected.tiles_equal_ignoring_cache_hint(&result));
    }

    #[tokio::test]
    async fn it_selects_band_from_stack() {
        it_selects_band_from_stack_impl(crate::engine::TimeDescriptor::new_irregular(None)).await;
    }

    #[tokio::test]
    async fn it_selects_band_from_stack_regular() {
        it_selects_band_from_stack_impl(crate::engine::TimeDescriptor::new_regular_with_epoch(
            Some(TimeInterval::new_unchecked(0, 10)),
            TimeStep::millis(5).unwrap(),
        ))
        .await;
    }

    #[allow(clippy::too_many_lines)]
    async fn it_selects_band_from_stack_impl(time_desc: crate::engine::TimeDescriptor) {
        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
        ];

        let data2: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![20, 21, 22, 23])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
        ];

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: time_desc,
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
                TileSize::new(2, 2),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: result_descriptor.clone(),
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor,
            },
        }
        .boxed();

        let stacker = RasterStacker {
            params: RasterStackerParams {
                output_origin: None,
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![mrs1, mrs2],
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size = TileSize(GridShape {
            shape_array: [2, 2],
        });

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-1, 0], [-1, 2]).unwrap(),
            TimeInterval::new_unchecked(0, 10),
            1.into(),
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let op = stacker
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
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

        let expected_band1: Vec<_> = data2
            .iter()
            .map(|t| {
                let mut t_1 = t.clone();
                t_1.band = 1;
                t_1
            })
            .collect();

        assert_eq_two_list_of_tiles(&result, &expected_band1, false);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
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
                .boxed(),
            },
        }
        .boxed();

        let operator = RasterStacker {
            params: RasterStackerParams {
                output_origin: None,
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![
                    GdalSource {
                        params: GdalSourceParameters::new(ndvi_id),
                    }
                    .boxed(),
                    expression,
                ],
            },
        }
        .boxed();

        let operator = operator
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
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

        assert!(!result.is_empty());

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

        assert!(!result_0.is_empty());
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

        assert!(!result_1.is_empty());
        assert!(result_1.iter().all(|t| t.band == 1));

        assert_eq!(result_0.len(), result_1.len());
    }

    #[tokio::test]
    async fn output_origin_preserves_input_extent() {
        let input_grid = SpatialGridDescriptor::source_from_parts(
            GeoTransform::test_default(),
            GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            TileSize::new(2, 2),
        );
        let descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_irregular(None),
            spatial_grid: input_grid,
            bands: RasterBandDescriptors::new_single_band(),
        };
        let source = MockRasterSource {
            params: MockRasterSourceParams {
                data: Vec::<RasterTile2D<u8>>::new(),
                result_descriptor: descriptor,
            },
        }
        .boxed();
        let output_origin = Coordinate2D::new(2., -2.);
        let expected_grid = input_grid
            .spatial_grid
            .with_moved_origin_exact_grid(output_origin)
            .unwrap();
        let stacker = RasterStacker {
            params: RasterStackerParams {
                output_origin: Some(output_origin),
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![source],
            },
        }
        .boxed();
        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size = TileSize::new(2, 2);

        let initialized = stacker
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        assert_eq!(
            initialized
                .result_descriptor()
                .spatial_grid_descriptor()
                .spatial_grid,
            expected_grid
        );
    }

    #[tokio::test]
    async fn rejects_incompatible_input_grids() {
        let make_source = |geo_transform| {
            MockRasterSource {
                params: MockRasterSourceParams {
                    data: Vec::<RasterTile2D<u8>>::new(),
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        time: TimeDescriptor::new_irregular(None),
                        spatial_grid: SpatialGridDescriptor::source_from_parts(
                            geo_transform,
                            GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
                            TileSize::new(2, 2),
                        ),
                        bands: RasterBandDescriptors::new_single_band(),
                    },
                },
            }
            .boxed()
        };

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size = TileSize::new(2, 2);

        for second_transform in [
            GeoTransform::new(Coordinate2D::new(0.5, 0.), 1., -1.),
            GeoTransform::new(Coordinate2D::new(0., 0.), 2., -2.),
        ] {
            let stacker = RasterStacker {
                params: RasterStackerParams {
                    output_origin: None,
                    rename_bands: RenameBands::Default,
                },
                sources: MultipleRasterSources {
                    rasters: vec![
                        make_source(GeoTransform::test_default()),
                        make_source(second_transform),
                    ],
                },
            }
            .boxed();

            assert!(matches!(
                stacker
                    .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
                    .await,
                Err(crate::error::Error::RasterInputsNotReTileCompatible { .. })
            ));
        }
    }

    #[tokio::test]
    async fn it_retiles_sources_with_different_origin() {
        // Both sources use valid 2x2 tiles, but the second source uses a
        // pixel-aligned native origin. The stacker must re-tile it to the first
        // source's output grid.
        let data1: Vec<RasterTile2D<u8>> = vec![RasterTile2D {
            time: TimeInterval::new_unchecked(0, 10),
            tile_position: [-1, 0].into(),
            band: 0,
            global_geo_transform: TestDefault::test_default(),
            grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
            properties: Default::default(),
            cache_hint: CacheHint::default(),
            overlap: TileOverlap::zero(),
        }];

        let data2: Vec<RasterTile2D<u8>> = vec![RasterTile2D {
            time: TimeInterval::new_unchecked(0, 10),
            tile_position: [-1, -1].into(),
            band: 0,
            global_geo_transform: GeoTransform::new(Coordinate2D::new(2., 0.), 1., -1.),
            grid_array: Grid::new([2, 2].into(), vec![10, 11, 12, 13])
                .unwrap()
                .into(),
            properties: Default::default(),
            cache_hint: CacheHint::default(),
            overlap: TileOverlap::zero(),
        }];

        let descriptor1 = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: crate::engine::TimeDescriptor::new_regular_with_epoch(
                Some(TimeInterval::new_unchecked(0, 10)),
                TimeStep::millis(10).unwrap(),
            ),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [-1, 1]).unwrap(),
                TileSize::new(2, 2),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let descriptor2 = RasterResultDescriptor {
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new(Coordinate2D::new(2., 0.), 1., -1.),
                GridBoundingBox2D::new([-2, -2], [-1, -1]).unwrap(),
                TileSize::new(2, 2),
            ),
            ..descriptor1.clone()
        };

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data1.clone(),
                result_descriptor: descriptor1,
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor: descriptor2,
            },
        }
        .boxed();

        let stacker = RasterStacker {
            params: RasterStackerParams {
                output_origin: None,
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![mrs1, mrs2],
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size = TileSize(GridShape {
            shape_array: [2, 2],
        });

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-2, 0], [-1, 1]).unwrap(),
            TimeInterval::new_unchecked(0, 10),
            [0, 1].try_into().unwrap(),
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let op = stacker
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
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

        let mut expected = vec![data1[0].clone()];
        expected.push({
            let mut tile = data2[0].clone();
            tile.band = 1;
            tile.tile_position = [-1, 0].into();
            tile.global_geo_transform = TestDefault::test_default();
            tile
        });

        assert!(expected.tiles_equal_ignoring_cache_hint(&result));
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
