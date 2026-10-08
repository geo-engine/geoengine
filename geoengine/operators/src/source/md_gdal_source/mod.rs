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
    primitives::{
        AxisAlignedRectangle, BandSelection, CacheTtlSeconds, RasterQueryRectangle, TimeInterval,
    },
    raster::{
        ChangeGridBounds, EmptyGrid, GridOrEmpty, Pixel, RasterDataType, RasterProperties,
        RasterTile2D, SpatialGridDefinition, TileInformation, TilingSpecification,
    },
};
use num::FromPrimitive;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

mod error;
mod loading_info;
mod reader;

pub use error::MdGdalSourceError;
pub use loading_info::{
    GdalMdMetaData, MdDatasetFile, MdFileTimes, MdLoadingInfo, ZRole, presented_geo_transform,
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

        // `query.spatial_bounds` are pixels of the tiling lattice, which is anchored at the
        // World Mercator-style origin (0, 0), while the dataset metadata looks up files in the
        // source grid's own pixel space. Translate the query into that space first, otherwise a
        // source grid that does not start at (0, 0) would filter out every stored tile.
        let query_in_source_grid = query.select_spatial_bounds(
            produced_source
                .geo_transform()
                .bounding_box_2d_to_intersecting_grid_bounds(
                    &SpatialGridDefinition::new(
                        produced_tiling_grid.tiling_geo_transform(),
                        query.spatial_bounds(),
                    )
                    .spatial_partition()
                    .as_bbox(),
                ),
        );
        let loading_info = self.meta_data.loading_info(query_in_source_grid).await?;
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

        // A read batch may only span a *contiguous* run in the output order, which Geo Engine
        // fixes as `band` fastest, `space` (row by row) next and `time` slowest.
        //
        // `ZRole::Band` puts z on the band axis, so a batch is one contiguous run of bands at
        // one tile and pays off directly. `ZRole::Variable` puts z on the time axis - the
        // slowest one - so a batch would straddle whole time steps and the tiles could only be
        // put back in order by buffering every spatial tile of those steps. A spatial row can
        // be arbitrarily long, so that is not an option: a time axis gets one slice per read.
        let batch_size = effective_z_batch_size(z_role, loading_info.max_z_batch_size());

        let mut per_band: Vec<Vec<(usize, MdRequest)>> = Vec::with_capacity(band_groups.len());
        for sel_band in band_groups {
            let (batches, missing) = loading_info.z_batches(&global_z, sel_band, batch_size);
            let mut reqs = Vec::with_capacity(batches.len() + missing.len());
            for batch in batches {
                reqs.push((
                    batch.global_z.start,
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
            // batches and gaps are collected separately; the merge below needs one list
            // ascending in its key, and a hole in the middle of the axis would otherwise
            // put every gap after every batch
            reqs.sort_by_key(|(z, _)| *z);
            per_band.push(reqs);
        }

        // merge-sort the per-band lists by global z (stable, preserving band order within a z)
        let mut requests: Vec<(usize, MdRequest)> = Vec::new();
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
                    let (first_z, request) = per_band[bi][cursors[bi]].clone();
                    requests.push((first_z, request));
                    cursors[bi] += 1;
                }
                None => break,
            }
        }

        // Canonical output order: `band` fastest, `space` (row by row) next, `time` slowest.
        // `iproduct!(requests, spatial_tiles)` emitted (time, band, space) instead - band
        // before space - which is wrong for every multi-band query, and with a batch larger
        // than one it emitted (z-block, space) which is wrong even single-band.
        let mut work: Vec<(MdRequest, TileInformation)> = Vec::new();
        match z_role {
            // one time step: the order is (space, band), and a request is already a run of
            // consecutive bands of a single tile
            ZRole::Band => {
                for tile_info in &spatial_tiles {
                    for (_, request) in &requests {
                        work.push((request.clone(), *tile_info));
                    }
                }
            }
            // one z step per request, requests already ordered by (z, band)
            ZRole::Variable => {
                let mut start = 0;
                while start < requests.len() {
                    let z = requests[start].0;
                    let mut end = start;
                    while end < requests.len() && requests[end].0 == z {
                        end += 1;
                    }
                    for tile_info in &spatial_tiles {
                        for (_, request) in &requests[start..end] {
                            work.push((request.clone(), *tile_info));
                        }
                    }
                    start = end;
                }
            }
        }

        let stream = stream::iter(work)
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
            // Concurrency, not buffering: at most this many `(request, tile)` results are in
            // flight. A request is one z slice for a time axis, but a whole band run (up to
            // `batch_size` slices) for a band axis - so peak tile memory is
            // `TILE_READ_CONCURRENCY` slices for `ZRole::Variable` and
            // `TILE_READ_CONCURRENCY * batch_size` for `ZRole::Band`. Deliberately *not*
            // `batch x tiles`, which is what a reorder buffer would have needed and what
            // makes long spatial rows untenable.
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

/// How many consecutive z slices one read may ask for.
///
/// Only a contiguous run in the output order (`band` fastest, `space` next, `time` slowest)
/// may be read as one batch. `ZRole::Band` has z on the band axis, so it is; `ZRole::Variable`
/// has z on the time axis, so it is not - see the batching comment in `query_processor`.
const fn effective_z_batch_size(z_role: ZRole, max_z_batch_size: usize) -> usize {
    match z_role {
        ZRole::Band => max_z_batch_size,
        ZRole::Variable => 1,
    }
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
        MockExecutionContext, MockQueryContext, RasterBandDescriptor, RasterBandDescriptors,
        SourceOperator, SpatialGridDescriptor, StaticMetaData, WorkflowOperatorPath,
    };
    use crate::test_data;
    use geoengine_datatypes::{
        dataset::{DataId, DatasetId, NamedData},
        primitives::{BandSelection, Measurement, TimeInterval},
        raster::{GridBoundingBox2D, GridBounds, GridOrEmpty, RasterTile2D},
        util::Identifier,
        util::test::TestDefault,
    };
    use std::marker::PhantomData;

    /// One MD array file, declared the way an importer declares it.
    ///
    /// Nothing here is read from the file: the probe lives on a stacked branch, so a
    /// read-path test builds its metadata by hand - the same construction that production
    /// and `python/examples/md_gdal_source_dataset.ipynb` perform. Every constant is
    /// transcribed from the fixture manifest in `test_data/md/generate_md_fixtures.py`,
    /// which stays the single source of truth.
    #[derive(Clone)]
    struct MdFile {
        path: &'static str,
        array_name: &'static str,
        group: Option<&'static str>,
        band: u32,
        /// first z slice of this file in the dataset's concatenated z axis
        z_start: usize,
        slices: usize,
        /// fixed index into each dimension between z and (y, x)
        leading_prefix: Vec<i64>,
    }

    impl MdFile {
        fn of(path: &'static str, array_name: &'static str, z_start: usize, slices: usize) -> Self {
            Self {
                path,
                array_name,
                group: None,
                band: 0,
                z_start,
                slices,
                leading_prefix: Vec::new(),
            }
        }
    }

    /// A whole MD dataset, declared the way an importer notebook declares one.
    struct MdDataset {
        files: Vec<MdFile>,
        /// the *stored* grid, as it is in the file: `(origin x, origin y, x pixel, y pixel)`
        grid: (f64, f64, f64, f64),
        width: usize,
        height: usize,
        wrap: bool,
        z_role: ZRole,
        /// the global time axis as `origin + i * step`, or `None` for a band axis, which has
        /// synthetic `[k, k+1)` unit steps indexed by band instead
        time: Option<(i64, i64)>,
        bands: Vec<&'static str>,
        max_z_batch_size: Option<usize>,
    }

    impl MdDataset {
        /// The 8x8 `time_series` grid: edges lon 0..240 (30 degree pixels), lat 0..-8,
        /// daily slices from 2000-01-01. Shared by `time_series.nc`, its ZARR twin and its
        /// split halves.
        fn daily_series(path: &'static str, slices: usize) -> Self {
            Self {
                files: vec![MdFile::of(path, "temperature", 0, slices)],
                grid: (0.0, 0.0, 30.0, -1.0),
                width: 8,
                height: 8,
                wrap: false,
                z_role: ZRole::Variable,
                time: Some((EPOCH_2000, DAY)),
                bands: vec!["temperature"],
                max_z_batch_size: None,
            }
        }

        /// `(time, depth, y, x)`: 6 daily slices with `depth` held at index 0 by the prefix.
        fn four_dimensional() -> Self {
            let mut dataset = Self::daily_series("md/time_depth_4d.nc", 6);
            dataset.files[0].leading_prefix = vec![0];
            dataset
        }

        /// Two files of 4 slices each, concatenated to one 8-slice axis: file `a` covers
        /// global z 0..4, file `b` covers 4..8.
        fn split_series() -> Self {
            let mut dataset = Self::daily_series("md/time_series_split_a.nc", 4);
            dataset
                .files
                .push(MdFile::of("md/time_series_split_b.nc", "temperature", 4, 4));
            dataset
        }

        /// A 0..360 stored longitude grid, re-presented as -180..180.
        fn wrapped(
            path: &'static str,
            width: usize,
            height: usize,
            grid: (f64, f64, f64, f64),
            slices: usize,
        ) -> Self {
            Self {
                files: vec![MdFile::of(path, "temperature", 0, slices)],
                grid,
                width,
                height,
                wrap: true,
                z_role: ZRole::Variable,
                time: Some((EPOCH_2000, DAY)),
                bands: vec!["temperature"],
                max_z_batch_size: None,
            }
        }

        /// Several arrays as bands on the wrapped 0..360 grid, one band per array.
        fn wrapped_arrays(
            path: &'static str,
            width: usize,
            height: usize,
            grid: (f64, f64, f64, f64),
            slices: usize,
            arrays: &[(&'static str, u32)],
        ) -> Self {
            let mut dataset = Self::wrapped_arrays_flat(path, width, height, grid, slices, arrays);
            dataset.wrap = true;
            dataset
        }

        fn wrapped_arrays_flat(
            path: &'static str,
            width: usize,
            height: usize,
            grid: (f64, f64, f64, f64),
            slices: usize,
            arrays: &[(&'static str, u32)],
        ) -> Self {
            Self {
                files: arrays
                    .iter()
                    .map(|(array_name, band)| MdFile {
                        path,
                        array_name,
                        group: None,
                        band: *band,
                        z_start: 0,
                        slices,
                        leading_prefix: Vec::new(),
                    })
                    .collect(),
                grid,
                width,
                height,
                wrap: false,
                z_role: ZRole::Variable,
                time: Some((EPOCH_2000, DAY)),
                bands: arrays.iter().map(|(n, _)| *n).collect(),
                max_z_batch_size: None,
            }
        }

        /// Ascending latitudes (row 0 = south) with `minutes since 1900-01-01`, so the read
        /// path has to flip y and the times are minute steps from 1900.
        fn ascending_lat_minutes() -> Self {
            Self {
                files: vec![MdFile::of(
                    "md/cf_time_units_minutes.nc",
                    "temperature",
                    0,
                    8,
                )],
                grid: (0.0, -8.0, 30.0, 1.0),
                width: 8,
                height: 8,
                wrap: false,
                z_role: ZRole::Variable,
                time: Some((-2_208_988_800_000, 60_000)),
                bands: vec!["temperature"],
                max_z_batch_size: None,
            }
        }

        /// `(band, y, x)` with no CF time units on z: each z slice is an output band and the
        /// "time" axis is the synthetic `[k, k+1)` unit step.
        fn band_axis() -> Self {
            Self {
                files: vec![MdFile::of("md/bands.nc", "reflectance", 0, 4)],
                grid: (0.0, 0.0, 30.0, -1.0),
                width: 8,
                height: 8,
                wrap: false,
                z_role: ZRole::Band,
                time: None,
                bands: vec!["band 0", "band 1", "band 2", "band 3"],
                max_z_batch_size: None,
            }
        }

        /// One file holding several `(time, y, x)` arrays: one band per array, in the given
        /// order, all sharing the file's time axis. `group` addresses a subgroup.
        fn one_array_per_band(
            path: &'static str,
            group: Option<&'static str>,
            arrays: &[(&'static str, u32)],
        ) -> Self {
            Self {
                files: arrays
                    .iter()
                    .map(|(array_name, band)| MdFile {
                        path,
                        array_name,
                        group,
                        band: *band,
                        z_start: 0,
                        slices: 8,
                        leading_prefix: Vec::new(),
                    })
                    .collect(),
                grid: (0.0, 0.0, 30.0, -1.0),
                width: 8,
                height: 8,
                wrap: false,
                z_role: ZRole::Variable,
                time: Some((EPOCH_2000, DAY)),
                bands: vec!["temperature", "precipitation", "cloud_area_fraction"],
                max_z_batch_size: None,
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn add_md_dataset(ctx: &mut MockExecutionContext, name: &str, spec: MdDataset) -> NamedData {
        let MdDataset {
            files,
            grid,
            width,
            height,
            wrap,
            z_role,
            time,
            bands,
            max_z_batch_size,
        } = spec;

        // the global axis, from which each file's own slices are carved
        let time_steps: Vec<TimeInterval> = match time {
            Some((origin, step)) => (0..files.iter().map(|f| f.z_start + f.slices).max().unwrap())
                .map(|i| {
                    TimeInterval::new_unchecked(
                        origin + i as i64 * step,
                        origin + (i as i64 + 1) * step,
                    )
                })
                .collect(),
            // a band axis has no time: each slice is the unit interval `[k, k+1)`
            None => (0..files.iter().map(|f| f.z_start + f.slices).max().unwrap())
                .map(|i| TimeInterval::new(i as i64, i as i64 + 1).unwrap())
                .collect(),
        };

        let md_files = files
            .iter()
            .map(|f| MdDatasetFile {
                params: crate::source::gdal_worker_process::GdalDatasetParameters {
                    file_path: test_data!(f.path).to_path_buf(),
                    rasterband_channel: 1,
                    geo_transform: crate::source::gdal_worker_process::GdalDatasetGeoTransform {
                        origin_coordinate: (grid.0, grid.1).into(),
                        x_pixel_size: grid.2,
                        y_pixel_size: grid.3,
                    },
                    width,
                    height,
                    file_not_found_handling:
                        crate::source::gdal_worker_process::FileNotFoundHandling::NoData,
                    no_data_value: Some(-9999.0),
                    properties_mapping: None,
                    gdal_open_options: None,
                    gdal_config_options: None,
                    allow_alphaband_as_mask: false,
                    retry: None,
                },
                array_name: f.array_name.to_owned(),
                group: f.group.map(str::to_string),
                z_start: f.z_start,
                z_end: f.z_start + f.slices,
                local_offset: 0,
                times: MdFileTimes::from_intervals(
                    time_steps[f.z_start..f.z_start + f.slices].to_vec(),
                ),
                output_band: f.band,
                leading_prefix: f.leading_prefix.clone(),
            })
            .collect::<Vec<_>>();

        let presented = presented_geo_transform(
            crate::source::gdal_worker_process::GdalDatasetGeoTransform {
                origin_coordinate: (grid.0, grid.1).into(),
                x_pixel_size: grid.2,
                y_pixel_size: grid.3,
            },
            height,
            wrap,
        );
        let result_descriptor = RasterResultDescriptor::new(
            RasterDataType::F32,
            geoengine_datatypes::spatial_reference::SpatialReference::epsg_4326().into(),
            MdLoadingInfo::new(
                time_steps.clone(),
                md_files.clone(),
                None,
                z_role,
                wrap,
                max_z_batch_size,
            )
            .time_descriptor(),
            SpatialGridDescriptor::source_from_parts(
                presented,
                GridBoundingBox2D::new_unchecked([0, 0], [height as isize - 1, width as isize - 1]),
            ),
            RasterBandDescriptors::new(
                bands
                    .iter()
                    .map(|name| RasterBandDescriptor::new(name.to_string(), Measurement::Unitless))
                    .collect(),
            )
            .unwrap(),
        );

        let id: DataId = DatasetId::new().into();
        let named = NamedData::with_system_name(name);
        ctx.add_meta_data(
            id,
            named.clone(),
            Box::new(StaticMetaData {
                loading_info: MdLoadingInfo::new(
                    time_steps,
                    md_files,
                    None,
                    z_role,
                    wrap,
                    max_z_batch_size,
                ),
                result_descriptor,
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
            MdDataset::daily_series("md/time_series.nc", 8),
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

    /// The end-to-end check for 4D: `depth` is held fixed while `time` is sliced, and each
    /// slice must come back with the `depth` offset in its values. A read that dropped or
    /// mis-set the prefix would return another depth's data, not an error.
    #[tokio::test]
    async fn test_query_four_dimensional_array_holds_the_leading_prefix() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let name = add_md_dataset(&mut exe_ctx, "md_time_depth", MdDataset::four_dimensional());

        // two daily slices at depth 0
        let time = TimeInterval::new_unchecked(EPOCH_2000, EPOCH_2000 + 2 * DAY);
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

        assert_eq!(tiles.len(), 2);
        for (k, tile) in tiles.iter().enumerate() {
            let t = k as i64;
            assert_eq!(
                tile.time,
                TimeInterval::new_unchecked(EPOCH_2000 + t * DAY, EPOCH_2000 + (t + 1) * DAY)
            );
            for (y, x) in [(0, 0), (7, 3), (7, 7)] {
                // fixture value at (t, depth=0, y, x)
                let expected = (t * 10_000 + (y as i64) * 10 + x as i64) as f32;
                assert_eq!(
                    grid_value(tile, y, x),
                    expected,
                    "value at y={y}, x={x} of t={t} must be depth 0's slice"
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
        let mut batched_of_8 = MdDataset::daily_series("md/time_series.nc", 8);
        batched_of_8.max_z_batch_size = Some(8);
        let mut batched_of_1 = MdDataset::daily_series("md/time_series.nc", 8);
        batched_of_1.max_z_batch_size = Some(1);
        let batched_name = add_md_dataset(&mut exe_ctx, "md_batched_8", batched_of_8);
        let single_name = add_md_dataset(&mut exe_ctx, "md_batched_1", batched_of_1);

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
            MdDataset::daily_series("md/time_series.nc", 8),
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
        let name = add_md_dataset(&mut exe_ctx, "md_split", MdDataset::split_series());

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
            MdDataset::daily_series("md/time_series.zarr", 8),
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

    /// A query that reaches past a wrapped dataset's presented extent must return no tiles
    /// for the non-overlapping part, exactly as the non-wrap path already does.
    ///
    /// `wrapped_splitted_advises` returning an empty `advises` used to fall through to the
    /// `EmptyGrid` frames, which emitted a real (fully empty) tile. So a query extending below
    /// the southernmost stored latitude got a full rectangle of empty tiles back where
    /// `GdalSource` returns none. This fixture stores lat 1..0 only, so rows below -1 in the
    /// tiling frame are outside the data.
    #[tokio::test]
    async fn test_query_wrap_beyond_extent_returns_no_tiles() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let name = add_md_dataset(
            &mut exe_ctx,
            "md_wrap_outside",
            // edges lon 0..360 at 0.25 degree (1440 cols), lat 1..0
            MdDataset::wrapped(
                "md/wrap_0_360_multitile.nc",
                1440,
                4,
                (0.0, 1.0, 0.25, -0.25),
                2,
            ),
        );

        // The stored rows are -4..-1 in the tiling frame, i.e. inside tile row -1 (y -512..-1).
        // Rows -1000..-600 snap to tile row -2 (y -1024..-513) alone, which cannot reach the
        // data at all -- so every requested tile must be dropped.
        let spatial = GridBoundingBox2D::new_unchecked([-1000, -720], [-600, 719]);
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

        assert!(
            tiles.is_empty(),
            "a query entirely outside the wrapped extent must return no tiles, got {}",
            tiles.len()
        );
    }

    /// A wrapped dataset whose longitude axis is wider than one 512 px tile column.
    ///
    /// `wrap_0_360.nc` is 360 columns at 1 deg, so the whole world fits inside a single tile
    /// column and it cannot exercise the per-column stored<->presented mapping. At 0.25 deg
    /// the 1440 columns span three tile columns, which is the layout NEX-GDDP-CMIP6 has --
    /// and the one that turned out to matter in practice.
    ///
    /// Checks that every presented column maps back to the right stored column across all
    /// three tile columns, including the seam at world longitude 0.
    #[tokio::test]
    async fn test_query_wrap_across_multiple_tile_columns() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let name = add_md_dataset(
            &mut exe_ctx,
            "md_wrap_multitile",
            MdDataset::wrapped(
                "md/wrap_0_360_multitile.nc",
                1440,
                4,
                (0.0, 1.0, 0.25, -0.25),
                2,
            ),
        );

        // presented grid: world lon -180..180 (1440 columns at 0.25 deg), lat 0..-1 (4 rows)
        let spatial = GridBoundingBox2D::new_unchecked([-4, -720], [-1, 719]);
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

        // 1440 presented columns span the seam at world lon 0, so they need four 512 px tile
        // columns: -2 and -1 for lon -180..-0.25, 0 and 1 for lon 0..180
        let mut xs: Vec<_> = tiles
            .iter()
            .map(|t| t.tile_information().global_tile_position().x())
            .collect();
        xs.sort_unstable();
        xs.dedup();
        assert_eq!(xs, [-2, -1, 0, 1], "expected four tile columns");

        // value at presented (col) = t * 100_000 + row * 10 + (col mod 1440)
        // sample both hemispheres and the seam column itself
        for (row, col) in [
            (-4_isize, -720_isize),
            (-4, -1),
            (-4, 0),
            (-4, 1),
            (-4, 359),
            (-4, 360),
            (-4, 719),
        ] {
            let tile = tiles
                .iter()
                .find(|t| {
                    let b = t.tile_information().global_pixel_bounds();
                    b.y_min() <= row && row <= b.y_max() && b.x_min() <= col && col <= b.x_max()
                })
                .unwrap_or_else(|| panic!("no tile covers world ({row}, {col})"));
            let local = tile.tile_information().global_pixel_bounds().min_index();
            let stored_col = col.rem_euclid(1440);
            assert_eq!(
                grid_value(tile, (row - local.y()) as usize, (col - local.x()) as usize),
                stored_col as f32,
                "world col {col}"
            );
        }
    }

    /// A wrapped dataset that stores latitudes ASCENDING (south-up) and is queried with a
    /// PARTIAL cell window inside one tile, i.e. a read advise whose `read_window_bounds`
    /// no longer covers a full tile. NEX-GDDP-CMIP6 has exactly this shape: south-up 0..360
    /// stored, a query that touches one tile column and one cell row.
    #[tokio::test]
    async fn test_query_wrap_south_up_partial_window() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let name = add_md_dataset(
            &mut exe_ctx,
            "md_wrap_south_up_partial",
            // stored lon 0..360 at 0.25 deg (1440 cols), lat -1..0 ascending (stored row 0 = south)
            MdDataset::wrapped(
                "md/wrap_0_360_multitile.nc",
                1440,
                4,
                (0.0, -1.0, 0.25, 0.25),
                2,
            ),
        );

        // partial window: one row block (rows 0..3 = the full 4 stored row-equivalents) and a
        // cell column strip 100..257 entirely inside tile column 0
        let spatial = GridBoundingBox2D::new_unchecked([0, 100], [3, 257]);
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

        assert_eq!(
            tiles.len(),
            1,
            "expected a single tile for the partial window"
        );
        for (row, col) in [(0, 100), (1, 157), (3, 257), (2, 200)] {
            let local = tiles[0]
                .tile_information()
                .global_pixel_bounds()
                .min_index();
            // stored row r = 3 - row (stored row 0 is the south edge), value = t*100000 + r*10 + col
            let expected = ((3 - row) * 10 + col) as f32;
            assert_eq!(
                grid_value(
                    &tiles[0],
                    (row - local.y()) as usize,
                    (col - local.x()) as usize
                ),
                expected,
                "value at tiling cell ({row}, {col})"
            );
        }
    }

    #[tokio::test]
    async fn test_query_wrap_seam() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let name = add_md_dataset(
            &mut exe_ctx,
            "md_wrap",
            // edges lon 0..360 at 1 degree (360 cols), lat 45..35
            MdDataset::wrapped("md/wrap_0_360.nc", 360, 10, (0.0, 45.0, 1.0, -1.0), 4),
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
            MdDataset::ascending_lat_minutes(),
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
        let name = add_md_dataset(&mut exe_ctx, "md_bands", MdDataset::band_axis());

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
        let name = add_md_dataset(
            &mut exe_ctx,
            "md_variables",
            MdDataset::one_array_per_band(
                "md/variables.nc",
                None,
                &[("temperature", 0), ("precipitation", 1)],
            ),
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
        let name = add_md_dataset(
            &mut exe_ctx,
            "md_grouped",
            MdDataset::one_array_per_band(
                "md/grouped_variables.nc",
                Some("analysis"),
                &[("cloud_area_fraction", 0), ("precipitation", 1)],
            ),
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

    // --- the output order: band fastest, space next, time slowest ---

    /// `(time, space)` ordering. Three tile columns and two time steps: every tile of the
    /// first step must precede every tile of the second, whatever the batch size.
    #[tokio::test]
    async fn test_emits_time_before_space() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let mut spec = MdDataset::wrapped(
            "md/wrap_0_360_multitile.nc",
            1440,
            4,
            (0.0, 1.0, 0.25, -0.25),
            2,
        );
        // a batch larger than the query is exactly what used to emit all of a tile's steps
        // before the next tile
        spec.max_z_batch_size = Some(4);
        let name = add_md_dataset(&mut exe_ctx, "md_order_time_space", spec);

        let spatial = GridBoundingBox2D::new_unchecked([-4, -720], [-1, 719]);
        let time = TimeInterval::new_unchecked(EPOCH_2000, EPOCH_2000 + 2 * DAY);
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

        assert!(
            tiles.len() > 2,
            "the fixture must span more than one tile, got {}",
            tiles.len()
        );
        let keys: Vec<_> = tiles
            .iter()
            .map(|t| {
                let idx = t.tile_information().global_pixel_bounds().min_index();
                (t.time.start().inner(), idx.y(), idx.x())
            })
            .collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        assert_eq!(
            keys, sorted,
            "tiles must come out as (time, space): time slowest, spatial tile row by row"
        );
        assert_ne!(keys[0].0, keys[keys.len() - 1].0, "need two time steps");
    }

    /// `(time, band)` ordering with the band axis fastest. Two variables, two time steps:
    /// the two bands of a step must be adjacent, not the two steps of a band.
    #[tokio::test]
    async fn test_emits_band_fastest() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let name = add_md_dataset(
            &mut exe_ctx,
            "md_order_band",
            MdDataset::one_array_per_band(
                "md/variables.nc",
                None,
                &[("temperature", 0), ("precipitation", 1)],
            ),
        );

        let time = TimeInterval::new_unchecked(EPOCH_2000, EPOCH_2000 + 2 * DAY);
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

        let keys: Vec<_> = tiles
            .iter()
            .map(|t| (t.time.start().inner(), t.band))
            .collect();
        assert_eq!(
            keys,
            vec![
                (EPOCH_2000, 0),
                (EPOCH_2000, 1),
                (EPOCH_2000 + DAY, 0),
                (EPOCH_2000 + DAY, 1),
            ],
            "band is the fastest-changing axis, so both bands of a step are adjacent"
        );
    }

    /// `band` before `space` is the bug the fix exists for: with several bands *and* several
    /// tiles the two orderings differ, and only this shape can tell them apart.
    #[tokio::test]
    async fn test_emits_band_inside_space() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let mut spec = MdDataset::wrapped_arrays(
            "md/variables_multitile.nc",
            1440,
            4,
            (0.0, 1.0, 0.25, -0.25),
            2,
            &[("temperature", 0), ("precipitation", 1)],
        );
        spec.max_z_batch_size = Some(4);
        let name = add_md_dataset(&mut exe_ctx, "md_order_band_space", spec);

        // three tile columns, two time steps, two bands
        let spatial = GridBoundingBox2D::new_unchecked([-4, -720], [-1, 719]);
        let time = TimeInterval::new_unchecked(EPOCH_2000, EPOCH_2000 + 2 * DAY);
        let tiles = query_md_source(
            &exe_ctx,
            &query_ctx,
            name,
            spatial,
            time,
            BandSelection::first_n(2),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

        assert!(tiles.len() >= 6, "need several tiles, got {}", tiles.len());
        // group the emitted tiles into runs of identical (time, space); a correct stream has
        // one run per (time, space) holding every band, in band order
        let mut runs: Vec<((i64, isize, isize), Vec<u32>)> = Vec::new();
        for t in &tiles {
            let idx = t.tile_information().global_pixel_bounds().min_index();
            let key = (t.time.start().inner(), idx.y(), idx.x());
            match runs.last_mut() {
                Some((k, bands)) if *k == key => bands.push(t.band),
                _ => runs.push((key, vec![t.band])),
            }
        }
        for (key, bands) in &runs {
            let mut ascending = bands.clone();
            ascending.sort_unstable();
            assert_eq!(
                bands, &ascending,
                "the bands of one (time, tile) must be emitted together and in order,                  got {bands:?} at {key:?}"
            );
        }
        let keys: Vec<_> = runs.iter().map(|(k, _)| *k).collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        assert_eq!(keys, sorted, "(time, space) must be non-decreasing");
    }

    /// A time axis gets one slice per read; a band axis keeps the batch.
    #[test]
    fn test_z_batch_spans_only_the_fastest_axis() {
        assert_eq!(effective_z_batch_size(ZRole::Variable, 4), 1);
        assert_eq!(effective_z_batch_size(ZRole::Band, 4), 4);
        assert_eq!(effective_z_batch_size(ZRole::Band, 1), 1);
    }

    /// The merge used to key batches by their *file-local* z start, which restarts at every
    /// file boundary. Two bands whose files split at different z then interleave wrongly.
    /// Band 0 is split across two files at gz 4, band 1 is a single file: the key `0` of band
    /// 0's second file must not jump ahead of band 1's gz 3.
    #[tokio::test]
    async fn test_emits_band_fastest_across_mismatched_file_splits() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let mut spec = MdDataset::daily_series("md/time_series_split_a.nc", 4);
        spec.files
            .push(MdFile::of("md/time_series_split_b.nc", "temperature", 4, 4));
        spec.files.push(MdFile {
            path: "md/variables.nc",
            array_name: "precipitation",
            group: None,
            band: 1,
            z_start: 0,
            slices: 8,
            leading_prefix: Vec::new(),
        });
        spec.bands = vec!["temperature", "precipitation"];
        let name = add_md_dataset(&mut exe_ctx, "md_order_mismatched_splits", spec);

        let time = TimeInterval::new_unchecked(EPOCH_2000, EPOCH_2000 + 8 * DAY);
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

        let keys: Vec<_> = tiles
            .iter()
            .map(|t| (t.time.start().inner(), t.band))
            .collect();
        let expected: Vec<_> = (0..8)
            .flat_map(|t| [0u32, 1].map(|b| (EPOCH_2000 + t * DAY, b)))
            .collect();
        assert_eq!(
            keys, expected,
            "band fastest, then time; a file boundary in one band only must not reorder the stream"
        );
    }

    /// A z index no file covers is a gap-filled empty tile, and it belongs at its own time
    /// position - not appended after every real slice of the band.
    #[tokio::test]
    async fn test_emits_gap_at_its_time_position() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let mut spec = MdDataset::daily_series("md/time_series_split_a.nc", 2);
        // hole at gz 2: the two files cover 0..2 and 3..5
        spec.files
            .push(MdFile::of("md/time_series_split_b.nc", "temperature", 3, 2));
        let name = add_md_dataset(&mut exe_ctx, "md_order_gap", spec);

        let time = TimeInterval::new_unchecked(EPOCH_2000, EPOCH_2000 + 5 * DAY);
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

        let keys: Vec<_> = tiles
            .iter()
            .map(|t| (t.time.start().inner(), t.band))
            .collect();
        let expected: Vec<_> = (0..5).map(|t| (EPOCH_2000 + t * DAY, 0)).collect();
        assert_eq!(
            keys, expected,
            "the gap tile must sit at t=2, not after t=4"
        );
        assert!(
            tiles[2].grid_array.is_empty(),
            "the gap at gz 2 must be an empty tile"
        );
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
