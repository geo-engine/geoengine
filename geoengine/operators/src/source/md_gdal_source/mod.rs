use crate::engine::SingleRasterSource;
use crate::engine::{
    CanonicOperatorName, InitializedRasterOperator, MetaData, OperatorData, OperatorName,
    QueryContext, QueryProcessor, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
    SourceOperator, TypedRasterQueryProcessor, WorkflowOperatorPath,
};
use crate::error::Error;
use crate::optimization::{OptimizableOperator, OptimizationError};
use crate::processing::{
    Downsampling, DownsamplingMethod, DownsamplingParams, DownsamplingResolution,
};
use crate::source::gdal_worker_process::{GdalReaderMode, ReaderState, TILE_READ_CONCURRENCY};
use crate::source::md_gdal_source::reader::{MdTileRequest, load_md_tile_from_files_async};
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use geoengine_datatypes::{
    dataset::NamedData,
    primitives::{BandSelection, CacheTtlSeconds, RasterQueryRectangle, TimeInterval},
    raster::{
        ChangeGridBounds, EmptyGrid, GridOrEmpty, Pixel, RasterDataType, RasterProperties,
        RasterTile2D, TileInformation, TilingSpecification,
    },
};
use num::FromPrimitive;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

mod error;
mod loading_info;
mod probe;
mod reader;

pub use error::MdGdalSourceError;
pub use loading_info::{GdalMdMetaData, MdDatasetFile, MdFileTimes, MdLoadingInfo, ZRole};
pub use probe::{
    MdArraySelection, ProbedGdalMdMetaData, presented_geo_transform, probe_md_loading_info,
    probe_md_variables_loading_info,
};

/// Parameters for the MD GDAL Source Operator.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MdGdalSourceParameters {
    pub data: NamedData,
}

/// The default bound on how many consecutive z slices one worker request may return.
///
/// This is a property of the *data*, not of a workflow, so it lives on the dataset meta data
/// (`max_z_batch_size`) rather than on these parameters; this is the fallback for datasets
/// that do not set it.
///
/// ponytail: 4 rather than more because a batch is one IPC message and one slice of a
/// 1440x600 float32 array is already 3.4 MB - a batch of 16 is a 55 MB message that takes
/// ~22 s cold over the network, while 4 is 14 MB / ~6 s. It is an upper bound: a query for a
/// single time step still reads exactly one slice. Raise it when reading more per round trip
/// measurably beats the per-request overhead.
pub const DEFAULT_MAX_Z_BATCH_SIZE: usize = 4;

impl MdGdalSourceParameters {
    #[must_use]
    pub fn new(data: NamedData) -> Self {
        Self { data }
    }
}

impl OperatorData for MdGdalSourceParameters {
    fn data_names_collect(&self, data_names: &mut Vec<NamedData>) {
        data_names.push(self.data.clone());
    }
}

type MdMetaData = Box<dyn MetaData<MdLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>;

/// A `RasterOperator` that reads n-dimensional GDAL arrays (netCDF/Zarr) and emits one 2D
/// raster tile per z-slice.
pub type MdGdalSource = SourceOperator<MdGdalSourceParameters>;

pub struct MdGdalSourceProcessor<T>
where
    T: Pixel,
{
    pub produced_result_descriptor: RasterResultDescriptor,
    pub tiling_specification: TilingSpecification,
    pub meta_data: MdMetaData,
    pub default_cache_ttl: CacheTtlSeconds,
    pub _phantom_data: PhantomData<T>,
}

impl<T> MdGdalSourceProcessor<T> where T: gdal::raster::GdalType + Pixel {}

#[async_trait]
impl<P> QueryProcessor for MdGdalSourceProcessor<P>
where
    P: Pixel + gdal::raster::GdalType + FromPrimitive,
{
    type Output = RasterTile2D<P>;
    type SpatialBounds = geoengine_datatypes::raster::GridBoundingBox2D;
    type Selection = BandSelection;
    type ResultDescription = RasterResultDescriptor;

    #[allow(clippy::too_many_lines)]
    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<BoxStream<Result<Self::Output>>> {
        tracing::debug!(
            "Querying MdGdalSourceProcessor<{:?}> with: {:?}.",
            P::TYPE,
            &query
        );

        let result_descriptor = self.result_descriptor();
        let produced_source = result_descriptor
            .spatial_grid
            .source_spatial_grid_definition()
            .expect("the source grid definition should be present in a source...");

        let produced_tiling_grid = result_descriptor
            .spatial_grid
            .tiling_grid_definition(self.tiling_specification);
        let tiling_strategy = produced_tiling_grid.generate_data_tiling_strategy();

        let reader_mode = GdalReaderMode::OriginalResolution(ReaderState {
            dataset_spatial_grid: produced_source,
        });

        let loading_info = self.meta_data.loading_info(query.clone()).await?;
        let z_role = loading_info.z_role();
        let gdal_worker = ctx.get_gdal_worker();
        let default_cache_ttl = self.default_cache_ttl;

        // resolve the requested z-slices: time steps for `ZRole::Variable`,
        // bands as z-indices for `ZRole::Band`
        let global_z = match z_role {
            ZRole::Band => query
                .attributes()
                .as_vec()
                .into_iter()
                .map(|band| band as usize)
                .collect(),
            ZRole::Variable => loading_info.z_indices_in_time(query.time_interval()),
        };

        // `ZRole::Variable`: each selected variable becomes its own output band and reads
        // only the files of that variable; the other roles have one implicit group covering
        // all files
        let band_groups =
            output_band_groups(z_role, result_descriptor.bands.count(), query.attributes())?;

        let spatial_tiles = tiling_strategy
            .tile_information_iterator_from_pixel_bounds(query.spatial_bounds())
            .collect::<Vec<_>>();

        tracing::debug!(
            "parsed loading_info with {} z-slices, {} spatial tiles",
            global_z.len(),
            spatial_tiles.len(),
        );

        // Build one request list per band, then interleave by z so the stream is
        // time-major: for each z step emit every band before moving to the next z step.
        // This matches the (time, space, band) order downstream consumers expect.
        let mut per_band: Vec<Vec<(usize, MdRequest)>> = Vec::with_capacity(band_groups.len());
        for sel_band in band_groups {
            let (batches, missing) =
                loading_info.z_batches(&global_z, sel_band, loading_info.max_z_batch_size());
            let mut reqs = Vec::with_capacity(batches.len() + missing.len());
            for batch in batches {
                reqs.push((
                    batch.local_z.start,
                    MdRequest::Batch(MdTileRequest {
                        file_idx: batch.file_idx,
                        local_z: batch.local_z,
                    }),
                ));
            }
            for gz in missing {
                reqs.push((
                    gz,
                    MdRequest::Gap(MdGapRequest {
                        global_z: gz,
                        output_band: sel_band.unwrap_or(0),
                    }),
                ));
            }
            per_band.push(reqs);
        }

        // merge-sort the per-band lists by first_z (stable, preserving band order within a z)
        let mut requests: Vec<MdRequest> = Vec::new();
        let mut cursors = vec![0usize; per_band.len()];
        loop {
            let mut best_band: Option<usize> = None;
            let mut best_z = usize::MAX;
            for (bi, reqs) in per_band.iter().enumerate() {
                if let Some((first_z, _)) = reqs.get(cursors[bi])
                    && *first_z < best_z
                {
                    best_z = *first_z;
                    best_band = Some(bi);
                }
            }
            match best_band {
                Some(bi) => {
                    requests.push(per_band[bi][cursors[bi]].1.clone());
                    cursors[bi] += 1;
                }
                None => break,
            }
        }

        let stream = stream::iter(itertools::iproduct!(requests, spatial_tiles.into_iter()))
            .map(move |(request, tile_info)| {
                let gdal_worker = gdal_worker.clone();
                let loading_info = loading_info.clone();
                async move {
                    match request {
                        MdRequest::Batch(batch) => load_md_tile_from_files_async::<P>(
                            &loading_info,
                            &batch,
                            reader_mode,
                            tile_info,
                            &gdal_worker,
                            default_cache_ttl,
                        )
                        .await
                        .map_err(Error::from),
                        MdRequest::Gap(gap) => Ok(vec![empty_tile(
                            &loading_info,
                            gap,
                            tile_info,
                            default_cache_ttl,
                        )]),
                    }
                }
            })
            .buffered(TILE_READ_CONCURRENCY)
            .flat_map(|res: Result<Vec<RasterTile2D<P>>>| match res {
                Ok(tiles) => stream::iter(tiles.into_iter().map(Ok)).boxed(),
                Err(err) => stream::once(futures::future::ready(Err(err))).boxed(),
            })
            .boxed();

        Ok(stream)
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.produced_result_descriptor
    }
}

/// One unit of work in the query stream: either a real read or a z slice that no file
/// covers and therefore becomes an empty tile.
#[derive(Debug, Clone)]
enum MdRequest {
    Batch(MdTileRequest),
    Gap(MdGapRequest),
}

/// A z slice that no file covers; it still needs a tile so that the temporal axis is
/// gap-free.
#[derive(Debug, Clone, Copy)]
struct MdGapRequest {
    global_z: usize,
    /// the Geo Engine output band this tile belongs to
    output_band: u32,
}

fn empty_tile<P: Pixel>(
    loading_info: &MdLoadingInfo,
    gap: MdGapRequest,
    tile_info: TileInformation,
    default_cache_ttl: CacheTtlSeconds,
) -> RasterTile2D<P> {
    let time_steps = loading_info.time_steps();
    let band = match loading_info.z_role() {
        ZRole::Band => gap.global_z as u32,
        ZRole::Variable => gap.output_band,
    };

    // `global_z` is validated against the band count before it reaches here, but a gap
    // index outside `time_steps` would panic; an empty tile with a degenerate interval is
    // recoverable, a panic in a query task is not
    let time = time_steps.get(gap.global_z).copied().unwrap_or_default();

    RasterTile2D::new_with_properties(
        time,
        tile_info.global_tile_position,
        band,
        tile_info.global_geo_transform,
        GridOrEmpty::from(EmptyGrid::new(tile_info.global_pixel_bounds())).unbounded(),
        RasterProperties::default(),
        loading_info.cache_hint(default_cache_ttl),
    )
}

/// The output-band groups to query: one per selected band for `ZRole::Variable` (each
/// variable reads only its own files), or a single `None` group (all files) otherwise.
fn output_band_groups(
    z_role: ZRole,
    band_count: u32,
    attributes: &BandSelection,
) -> Result<Vec<Option<u32>>> {
    match z_role {
        ZRole::Variable => {
            for &b in attributes.as_slice() {
                if b >= band_count {
                    return Err(Error::from(MdGdalSourceError::UnsupportedBandRequest {
                        message: format!("band {b} does not exist, there are {band_count} bands"),
                    }));
                }
            }
            Ok(attributes.as_vec().into_iter().map(Some).collect())
        }
        // `ZRole::Band` maps a band index straight to a z index, so an out-of-range band
        // would otherwise reach `empty_tile`/`z_batches` as a missing index and panic on
        // `time_steps[global_z]`.
        ZRole::Band => {
            for &b in attributes.as_slice() {
                if b >= band_count {
                    return Err(Error::from(MdGdalSourceError::UnsupportedBandRequest {
                        message: format!("band {b} does not exist, there are {band_count} bands"),
                    }));
                }
            }
            Ok(vec![None])
        }
    }
}

#[async_trait]
impl<P> RasterQueryProcessor for MdGdalSourceProcessor<P>
where
    P: Pixel + gdal::raster::GdalType + FromPrimitive,
{
    type RasterType = P;

    async fn _time_query<'a>(
        &'a self,
        query: TimeInterval,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<TimeInterval>>> {
        // The time axis is stored per file, so it can be answered from the meta data alone.
        // Sources whose axis is not separately answerable (`time_axis` returns `None`) fall
        // back to reading it off a loading info, which is what `GdalSource` does.
        let time_steps = match self.meta_data.time_axis(query).await? {
            Some(time_steps) => time_steps,
            None => {
                let q_bounds = self
                    .result_descriptor()
                    .tiling_grid_definition(ctx.tiling_specification())
                    .tiling_grid_bounds();
                self.meta_data
                    .loading_info(RasterQueryRectangle::new(
                        q_bounds,
                        query,
                        BandSelection::first(),
                    ))
                    .await?
                    .time_steps()
                    .to_vec()
            }
        };

        Ok(stream::iter(time_steps).map(Result::Ok).boxed())
    }
}

impl OperatorName for MdGdalSource {
    const TYPE_NAME: &'static str = "MdGdalSource";
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for MdGdalSource {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let data_id = context.resolve_named_data(&self.params.data).await?;
        let meta_data: MdMetaData = context.meta_data(&data_id).await?;

        tracing::debug!("Initializing MdGdalSource for {:?}.", &self.params.data);
        tracing::debug!("MdGdalSource path: {:?}", path);

        let meta_data_result_descriptor = meta_data.result_descriptor().await?;

        let op_name = CanonicOperatorName::from(&self);

        let op = InitializedMdGdalSourceOperator {
            name: op_name,
            path,
            data_name: self.params.data,
            meta_data,
            produced_result_descriptor: meta_data_result_descriptor,
            tiling_specification: context.tiling_specification(),
            default_cache_ttl: context.default_cache_ttl(),
        };

        Ok(op.boxed())
    }

    span_fn!(MdGdalSource);
}

/// An initialized source operator for an MD GDAL array.
pub struct InitializedMdGdalSourceOperator {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    data_name: NamedData,
    pub meta_data: MdMetaData,
    pub produced_result_descriptor: RasterResultDescriptor,
    pub tiling_specification: TilingSpecification,
    /// Fallback TTL for tiles of a dataset that does not carry its own.
    pub default_cache_ttl: CacheTtlSeconds,
}

impl InitializedRasterOperator for InitializedMdGdalSourceOperator {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.produced_result_descriptor
    }

    #[allow(clippy::too_many_lines)]
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        Ok(match self.result_descriptor().data_type {
            RasterDataType::U8 => TypedRasterQueryProcessor::U8(
                MdGdalSourceProcessor {
                    produced_result_descriptor: self.produced_result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    default_cache_ttl: self.default_cache_ttl,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::U16 => TypedRasterQueryProcessor::U16(
                MdGdalSourceProcessor {
                    produced_result_descriptor: self.produced_result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    default_cache_ttl: self.default_cache_ttl,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::U32 => TypedRasterQueryProcessor::U32(
                MdGdalSourceProcessor {
                    produced_result_descriptor: self.produced_result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    default_cache_ttl: self.default_cache_ttl,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::U64 => TypedRasterQueryProcessor::U64(
                MdGdalSourceProcessor {
                    produced_result_descriptor: self.produced_result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    default_cache_ttl: self.default_cache_ttl,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::I8 => TypedRasterQueryProcessor::I8(
                MdGdalSourceProcessor {
                    produced_result_descriptor: self.produced_result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    default_cache_ttl: self.default_cache_ttl,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::I16 => TypedRasterQueryProcessor::I16(
                MdGdalSourceProcessor {
                    produced_result_descriptor: self.produced_result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    default_cache_ttl: self.default_cache_ttl,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::I32 => TypedRasterQueryProcessor::I32(
                MdGdalSourceProcessor {
                    produced_result_descriptor: self.produced_result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    default_cache_ttl: self.default_cache_ttl,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::I64 => TypedRasterQueryProcessor::I64(
                MdGdalSourceProcessor {
                    produced_result_descriptor: self.produced_result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    default_cache_ttl: self.default_cache_ttl,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::F32 => TypedRasterQueryProcessor::F32(
                MdGdalSourceProcessor {
                    produced_result_descriptor: self.produced_result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    default_cache_ttl: self.default_cache_ttl,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::F64 => TypedRasterQueryProcessor::F64(
                MdGdalSourceProcessor {
                    produced_result_descriptor: self.produced_result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    default_cache_ttl: self.default_cache_ttl,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
        })
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        MdGdalSource::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }

    fn data(&self) -> Option<String> {
        Some(self.data_name.to_string())
    }

    fn optimize(
        &self,
        target_resolution: geoengine_datatypes::primitives::SpatialResolution,
    ) -> crate::util::Result<Box<dyn RasterOperator>, OptimizationError> {
        self.ensure_resolution_is_compatible_for_optimization(target_resolution)?;

        let source = MdGdalSource {
            params: MdGdalSourceParameters::new(self.data_name.clone()),
        }
        .boxed();

        // GDAL cannot build or read overviews for a multidimensional array, so there is
        // nothing to lower the resolution at the source. Downsample in front instead, the
        // same way `MockRasterSource` does.
        if target_resolution
            > self
                .produced_result_descriptor
                .spatial_grid
                .spatial_resolution()
        {
            return Ok(Downsampling {
                params: DownsamplingParams {
                    sampling_method: DownsamplingMethod::NearestNeighbor,
                    output_resolution: DownsamplingResolution::Resolution(target_resolution),
                    output_origin_reference: None,
                },
                sources: SingleRasterSource { raster: source },
            }
            .boxed());
        }

        Ok(source)
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp)] // fixture values are exact integers in f32
mod tests {
    use super::*;
    use crate::engine::{
        MockExecutionContext, MockQueryContext, SourceOperator, StaticMetaData,
        WorkflowOperatorPath,
    };
    use crate::test_data;
    use geoengine_datatypes::{
        dataset::{DataId, DatasetId, NamedData},
        primitives::{BandSelection, TimeInterval},
        raster::{GridBoundingBox2D, GridBounds, GridOrEmpty, RasterTile2D},
        util::Identifier,
        util::test::TestDefault,
    };
    use std::marker::PhantomData;
    use std::path::PathBuf;

    fn add_md_dataset(
        ctx: &mut MockExecutionContext,
        name: &str,
        paths: &[PathBuf],
        array_name: Option<&str>,
        max_z_batch_size: Option<usize>,
    ) -> NamedData {
        let id: DataId = DatasetId::new().into();
        let named = NamedData::with_system_name(name);
        let probed = probe_md_loading_info(paths, array_name, max_z_batch_size)
            .expect("probe should succeed");
        ctx.add_meta_data(
            id,
            named.clone(),
            Box::new(StaticMetaData {
                loading_info: probed.loading_info,
                result_descriptor: probed.result_descriptor,
                phantom: PhantomData::<RasterQueryRectangle>,
            }),
        );
        named
    }

    fn add_md_variables_dataset(
        ctx: &mut MockExecutionContext,
        name: &str,
        paths: &[PathBuf],
        selection: &MdArraySelection,
        max_z_batch_size: Option<usize>,
    ) -> NamedData {
        let id: DataId = DatasetId::new().into();
        let named = NamedData::with_system_name(name);
        let probed = probe_md_variables_loading_info(paths, selection, max_z_batch_size)
            .expect("probe should succeed");
        ctx.add_meta_data(
            id,
            named.clone(),
            Box::new(StaticMetaData {
                loading_info: probed.loading_info,
                result_descriptor: probed.result_descriptor,
                phantom: PhantomData::<RasterQueryRectangle>,
            }),
        );
        named
    }

    async fn query_md_source(
        exe_ctx: &MockExecutionContext,
        query_ctx: &MockQueryContext,
        name: NamedData,
        spatial_query: GridBoundingBox2D,
        time_interval: TimeInterval,
        attributes: BandSelection,
    ) -> Vec<Result<RasterTile2D<f32>>> {
        let op: MdGdalSource = SourceOperator {
            params: MdGdalSourceParameters::new(name.clone()),
        };
        let o = op
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), exe_ctx)
            .await
            .unwrap();

        o.query_processor()
            .unwrap()
            .get_f32()
            .unwrap()
            .raster_query(
                RasterQueryRectangle::new(spatial_query, time_interval, attributes),
                query_ctx,
            )
            .await
            .unwrap()
            .collect()
            .await
    }

    fn grid_value(tile: &RasterTile2D<f32>, row: usize, col: usize) -> f32 {
        let GridOrEmpty::Grid(grid) = &tile.grid_array else {
            panic!("tile is empty");
        };
        let width = grid.inner_grid.shape.x();
        grid.inner_grid.data[row * width + col]
    }

    const DAY: i64 = 86_400_000;
    const EPOCH_2000: i64 = 946_684_800_000;

    fn ts_grid_bounds() -> GridBoundingBox2D {
        // the 8x8 time_series grid spans global lattice cols 0..7, rows 0..7 (edges 0..240, 0..-8)
        GridBoundingBox2D::new_unchecked([0, 0], [7, 7])
    }

    #[tokio::test]
    async fn test_query_time_series_slices() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let name = add_md_dataset(
            &mut exe_ctx,
            "md_time_series",
            &[test_data!("md/time_series.nc").to_path_buf()],
            None,
            None,
        );

        // window over z-slices 2..6 (4 slices), batched one slice at a time
        let time = TimeInterval::new_unchecked(EPOCH_2000 + 2 * DAY, EPOCH_2000 + 6 * DAY);
        let tiles = query_md_source(
            &exe_ctx,
            &query_ctx,
            name,
            ts_grid_bounds(),
            time,
            BandSelection::first(),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

        assert_eq!(tiles.len(), 4);
        for (k, tile) in tiles.iter().enumerate() {
            let t = (2 + k) as i64;
            assert_eq!(
                tile.time,
                TimeInterval::new_unchecked(EPOCH_2000 + t * DAY, EPOCH_2000 + (t + 1) * DAY)
            );
            assert_eq!(tile.band, 0);
            for (y, x) in [(0, 0), (3, 5), (7, 7)] {
                assert_eq!(
                    grid_value(tile, y, x),
                    (t * 1000 + (y as i64) * 10) as f32 + x as f32,
                    "value at y={y}, x={x} of slice t={t}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_query_batched_z_equals_single_batched() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let time = TimeInterval::new_unchecked(EPOCH_2000 + 2 * DAY, EPOCH_2000 + 6 * DAY);

        // the batch bound is a dataset property, so comparing two bounds means two datasets
        let batched_name = add_md_dataset(
            &mut exe_ctx,
            "md_batched_8",
            &[test_data!("md/time_series.nc").to_path_buf()],
            None,
            Some(8),
        );
        let single_name = add_md_dataset(
            &mut exe_ctx,
            "md_batched_1",
            &[test_data!("md/time_series.nc").to_path_buf()],
            None,
            Some(1),
        );

        let batched = query_md_source(
            &exe_ctx,
            &query_ctx,
            batched_name,
            ts_grid_bounds(),
            time,
            BandSelection::first(),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let single = query_md_source(
            &exe_ctx,
            &query_ctx,
            single_name,
            ts_grid_bounds(),
            time,
            BandSelection::first(),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

        assert_eq!(batched.len(), 4);
        assert_eq!(batched.len(), single.len());
        for (a, b) in batched.iter().zip(&single) {
            assert_eq!(a.time, b.time);
            assert_eq!(
                a.tile_information().global_tile_position(),
                b.tile_information().global_tile_position()
            );
            for (y, x) in [(0, 0), (7, 7)] {
                assert_eq!(grid_value(a, y, x), grid_value(b, y, x));
            }
        }
    }

    #[tokio::test]
    async fn test_query_time_series_full() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let name = add_md_dataset(
            &mut exe_ctx,
            "md_time_series_full",
            &[test_data!("md/time_series.nc").to_path_buf()],
            None,
            None,
        );

        let time = TimeInterval::new_unchecked(EPOCH_2000, EPOCH_2000 + 8 * DAY);
        let tiles = query_md_source(
            &exe_ctx,
            &query_ctx,
            name,
            ts_grid_bounds(),
            time,
            BandSelection::first(),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

        assert_eq!(tiles.len(), 8);
        assert_eq!(tiles[0].time.start().inner(), EPOCH_2000);
        assert_eq!(tiles[7].time.start().inner(), EPOCH_2000 + 7 * DAY);
    }

    #[tokio::test]
    async fn test_query_split_files_crossing_boundary() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let name = add_md_dataset(
            &mut exe_ctx,
            "md_split",
            &[
                test_data!("md/time_series_split_b.nc").to_path_buf(),
                test_data!("md/time_series_split_a.nc").to_path_buf(),
            ],
            None,
            None,
        );

        // window [d3, d6): slice 3 (file a) and slices 4..6 (file b), crossing the file boundary
        let time = TimeInterval::new_unchecked(EPOCH_2000 + 3 * DAY, EPOCH_2000 + 6 * DAY);
        let tiles = query_md_source(
            &exe_ctx,
            &query_ctx,
            name,
            ts_grid_bounds(),
            time,
            BandSelection::first(),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

        assert_eq!(tiles.len(), 3);
        let times = tiles
            .iter()
            .map(|t| t.time.start().inner())
            .collect::<Vec<_>>();
        assert!(
            times.windows(2).all(|w| w[0] < w[1]),
            "tiles must be in time order"
        );
        for (k, tile) in tiles.iter().enumerate() {
            let t = 3 + k;
            assert_eq!(
                grid_value(tile, 0, 0),
                (t * 1000) as f32,
                "cross-file slice t={t}"
            );
        }
    }

    #[tokio::test]
    async fn test_query_zarr() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let name = add_md_dataset(
            &mut exe_ctx,
            "md_zarr",
            &[test_data!("md/time_series.zarr").to_path_buf()],
            None,
            None,
        );

        let time = TimeInterval::new_unchecked(EPOCH_2000 + 2 * DAY, EPOCH_2000 + 6 * DAY);
        let tiles = query_md_source(
            &exe_ctx,
            &query_ctx,
            name,
            ts_grid_bounds(),
            time,
            BandSelection::first(),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

        assert_eq!(tiles.len(), 4);
        for (k, tile) in tiles.iter().enumerate() {
            let t = 2 + k;
            assert_eq!(grid_value(tile, 7, 7), (t * 1000 + 7 * 10 + 7) as f32);
        }
    }

    #[tokio::test]
    async fn test_query_wrap_seam() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let name = add_md_dataset(
            &mut exe_ctx,
            "md_wrap",
            &[test_data!("md/wrap_0_360.nc").to_path_buf()],
            None,
            None,
        );

        // presented grid: world lon -180..180 (global cols -180..179), lat 45..35
        let spatial = GridBoundingBox2D::new_unchecked([-45, -180], [-36, 179]);
        let time = TimeInterval::new_unchecked(EPOCH_2000, EPOCH_2000 + DAY);
        let tiles = query_md_source(
            &exe_ctx,
            &query_ctx,
            name,
            spatial,
            time,
            BandSelection::first(),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

        // 512 px tiles anchored at world 0° yield one tile per hemisphere (y tile -1)
        assert_eq!(tiles.len(), 2);
        let mut xs: Vec<_> = tiles
            .iter()
            .map(|t| t.tile_information().global_tile_position().x())
            .collect();
        xs.sort_unstable();
        assert_eq!(xs, [-1, 0]);
        assert!(
            tiles
                .iter()
                .all(|t| t.tile_information().global_tile_position().y() == -1)
        );

        // value at world (row, col) = stored row (row + 45) * 10 + stored col (col mod 360)
        for (row, col) in [(-45_isize, -180_isize), (-45, -1), (-36, 0), (-36, 179)] {
            let tile = tiles
                .iter()
                .find(|t| {
                    let x = t.tile_information().global_tile_position().x();
                    (col < 0 && x == -1) || (col >= 0 && x == 0)
                })
                .unwrap();
            let local = tile.tile_information().global_pixel_bounds().min_index();
            let stored_col = col.rem_euclid(360);
            assert_eq!(
                grid_value(tile, (row - local.y()) as usize, (col - local.x()) as usize),
                ((row + 45) * 10 + stored_col) as f32,
                "world col {col}"
            );
        }
    }

    #[tokio::test]
    async fn test_query_cf_ascending_lat_flips() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let name = add_md_dataset(
            &mut exe_ctx,
            "md_cf_minutes",
            &[test_data!("md/cf_time_units_minutes.nc").to_path_buf()],
            None,
            None,
        );

        // the cf fixture stores latitudes ASCENDING (array row 0 = south); the tile must
        // be presented north-up: tile row 0 = lat -0.5 (array row 7), tile row 7 = array row 0
        let time = TimeInterval::new_unchecked(-2_208_988_800_000, -2_208_988_800_000 + 60_000);
        let tiles = query_md_source(
            &exe_ctx,
            &query_ctx,
            name,
            ts_grid_bounds(),
            time,
            BandSelection::first(),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

        assert_eq!(tiles.len(), 1);
        let tile = &tiles[0];
        assert_eq!(tile.time.start().inner(), -2_208_988_800_000);
        assert_eq!(grid_value(tile, 0, 0), 70.0);
        assert_eq!(grid_value(tile, 0, 7), 77.0);
        assert_eq!(grid_value(tile, 7, 0), 0.0);
        assert_eq!(grid_value(tile, 7, 7), 7.0);
    }

    #[tokio::test]
    async fn test_query_bands() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let name = add_md_dataset(
            &mut exe_ctx,
            "md_bands",
            &[test_data!("md/bands.nc").to_path_buf()],
            None,
            None,
        );

        // band 2 of a ZRole::Band dataset
        let time = TimeInterval::new_instant(0).unwrap();
        let tiles = query_md_source(
            &exe_ctx,
            &query_ctx,
            name,
            ts_grid_bounds(),
            time,
            BandSelection::new_single(2),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

        assert_eq!(tiles.len(), 1);
        let tile = &tiles[0];
        assert_eq!(tile.band, 2);
        assert_eq!(grid_value(tile, 0, 0), 200.0);
        assert_eq!(grid_value(tile, 7, 7), 200.0 + (7 * 10 + 7) as f32);
    }

    #[test]
    fn out_of_range_band_is_rejected_for_band_role() {
        // `ZRole::Band` maps a band index straight to a z index, so an index past the end
        // would reach `empty_tile` and panic on `time_steps[global_z]`
        assert!(
            output_band_groups(ZRole::Band, 4, &BandSelection::new_single(5)).is_err(),
            "band 5 of a 4-band dataset must be rejected"
        );
        assert!(output_band_groups(ZRole::Band, 4, &BandSelection::new_single(3)).is_ok());
    }

    #[tokio::test]
    async fn test_query_variables_bands() {
        // explicit variable order -> band order: 0 = temperature (x1), 1 = precipitation (x2)
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let name = add_md_variables_dataset(
            &mut exe_ctx,
            "md_variables",
            &[test_data!("md/variables.nc").to_path_buf()],
            &MdArraySelection {
                group: None,
                arrays: vec!["temperature".to_string(), "precipitation".to_string()],
            },
            None,
        );

        // slices t = 1, 2 (2 time steps) x bands [0, 1] x one spatial tile = 4 tiles
        let time = TimeInterval::new_unchecked(EPOCH_2000 + DAY, EPOCH_2000 + 3 * DAY);
        let tiles = query_md_source(
            &exe_ctx,
            &query_ctx,
            name,
            ts_grid_bounds(),
            time,
            BandSelection::first_n(2),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

        assert_eq!(tiles.len(), 4);
        let mut tiles = tiles;
        tiles.sort_by_key(|t| (t.band, t.time.start().inner()));
        for (i, tile) in tiles.iter().enumerate() {
            let b = (i / 2) as u32;
            let t = 1 + (i % 2) as i64;
            assert_eq!(tile.band, b);
            assert_eq!(
                tile.time,
                TimeInterval::new_unchecked(EPOCH_2000 + t * DAY, EPOCH_2000 + (t + 1) * DAY)
            );
            let multiplier = 1.0 + b as f32;
            assert_eq!(
                grid_value(tile, 3, 5),
                multiplier * (t * 100 + 35) as f32,
                "band {b}, slice {t}"
            );
            assert_eq!(
                grid_value(tile, 7, 7),
                multiplier * (t * 100 + 77) as f32,
                "band {b}, slice {t}"
            );
        }
    }

    #[tokio::test]
    async fn test_query_grouped_variables_bands() {
        // variables.nc with the data arrays inside the `analysis` subgroup:
        // auto-selected -> band 0 = cloud_area_fraction (x1), band 1 = precipitation (x2)
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let name = add_md_variables_dataset(
            &mut exe_ctx,
            "md_grouped",
            &[test_data!("md/grouped_variables.nc").to_path_buf()],
            &MdArraySelection {
                group: Some("analysis".to_string()),
                arrays: vec![],
            },
            None,
        );

        let time = TimeInterval::new_unchecked(EPOCH_2000 + DAY, EPOCH_2000 + 2 * DAY);
        let tiles = query_md_source(
            &exe_ctx,
            &query_ctx,
            name,
            ts_grid_bounds(),
            time,
            BandSelection::first_n(2),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

        // 1 time step x bands [0, 1] x one spatial tile = 2 tiles read from the subgroup
        assert_eq!(tiles.len(), 2);
        let mut tiles = tiles;
        tiles.sort_by_key(|t| t.band);
        for tile in &tiles {
            assert_eq!(
                tile.time,
                TimeInterval::new_unchecked(EPOCH_2000 + DAY, EPOCH_2000 + 2 * DAY)
            );
        }
        // auto-selected, name-sorted: band 0 = cloud_area_fraction (x3), band 1 = precipitation (x2)
        assert_eq!(tiles[0].band, 0);
        assert_eq!(tiles[1].band, 1);
        assert_eq!(grid_value(&tiles[0], 3, 5), 3.0 * (100 + 35) as f32);
        assert_eq!(grid_value(&tiles[0], 7, 7), 3.0 * (100 + 77) as f32);
        assert_eq!(grid_value(&tiles[1], 3, 5), 2.0 * (100 + 35) as f32);
        assert_eq!(grid_value(&tiles[1], 7, 7), 2.0 * (100 + 77) as f32);
    }

    #[test]
    fn params_serde_has_no_batch_size() {
        // the z batch bound is a dataset property, so it must not appear in the operator
        // parameters at all
        let params: MdGdalSourceParameters = serde_json::from_str(r#"{"data": "ns:ds"}"#).unwrap();
        assert_eq!(
            params,
            MdGdalSourceParameters::new(NamedData::with_namespaced_name("ns", "ds"))
        );
        assert!(serde_json::to_value(params).is_ok());
    }
}
