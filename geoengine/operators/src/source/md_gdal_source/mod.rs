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
        ChangeGridBounds, EmptyGrid, GridOrEmpty, Pixel, RasterProperties, RasterTile2D,
        SpatialGridDefinition, TileInformation, TilingSpecification,
    },
};
use num::FromPrimitive;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;

mod db_types;
mod error;
mod loading_info;
mod presentation;
mod reader;

pub use error::MdGdalSourceError;
pub use loading_info::{
    GdalMdMetaData, MdDatasetFile, MdFileTimes, MdLoadingInfo, MdLoadingInfoQueryRectangle,
    MdVariable,
};
pub use presentation::MdPresentation;

/// Parameters for the MD GDAL Source Operator.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MdGdalSourceParameters {
    pub data: NamedData,
}

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

type MdMetaData =
    Box<dyn MetaData<MdLoadingInfo, RasterResultDescriptor, MdLoadingInfoQueryRectangle>>;

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
        let loading_info = self
            .meta_data
            .loading_info(MdLoadingInfoQueryRectangle {
                query_rectangle: query_in_source_grid,
                fetch_files: true,
            })
            .await?;
        let gdal_worker = ctx.get_gdal_worker();
        let default_cache_ttl = self.default_cache_ttl;

        let global_z = loading_info.z_indices_in_time(query.time_interval());

        // each selected variable becomes its own output band and reads only its own files
        let band_groups = output_band_groups(result_descriptor.bands.count(), query.attributes())?;

        let spatial_tiles = tiling_strategy
            .tile_information_iterator_from_pixel_bounds(query.spatial_bounds())
            .collect::<Vec<_>>();

        tracing::debug!(
            "parsed loading_info with {} z-slices, {} spatial tiles",
            global_z.len(),
            spatial_tiles.len(),
        );

        let mut per_band: Vec<Vec<(usize, MdRequest)>> = Vec::with_capacity(band_groups.len());
        for sel_band in band_groups {
            let (slices, missing) = loading_info.z_slices(&global_z, sel_band);
            let mut reqs = Vec::with_capacity(slices.len() + missing.len());
            for (gz, requests) in slices {
                reqs.push((gz, MdRequest::Read(requests)));
            }
            for gz in missing {
                reqs.push((
                    gz,
                    MdRequest::Gap(MdGapRequest {
                        global_z: gz,
                        output_band: sel_band,
                    }),
                ));
            }
            // reads and gaps are collected separately; the merge below needs one list
            // ascending in its key, and a hole in the middle of the axis would otherwise
            // put every gap after every read
            reqs.sort_by_key(|(z, _)| *z);
            per_band.push(reqs);
        }

        // merge the per-band lists by global z; a stable sort of the concatenation keeps band
        // order within a z, which is the fastest axis of the output
        let mut requests: Vec<(usize, MdRequest)> = per_band.into_iter().flatten().collect();
        requests.sort_by_key(|(z, _)| *z);

        // Canonical output order: `band` fastest, `space` (row by row) next, `time` slowest.
        // Groups are one z slice each, and within a group the tiles come before the requests,
        // so the order is (space, band) per time step.
        let groups: Vec<Vec<MdRequest>> = requests
            .chunk_by(|a, b| a.0 == b.0)
            .map(|group| group.iter().map(|(_, request)| request.clone()).collect())
            .collect();
        let loading_info = Arc::new(loading_info);

        // lazy: the (slice, tile) pairs are produced as the stream is polled, so a long time
        // range costs one group at a time, not one request per tile up front
        let stream = stream::iter(groups)
            .flat_map(move |group| {
                stream::iter(spatial_tiles.clone()).flat_map(move |tile_info| {
                    stream::iter(group.clone()).map(move |request| (request, tile_info))
                })
            })
            .map(move |(request, tile_info)| {
                let gdal_worker = gdal_worker.clone();
                let loading_info = Arc::clone(&loading_info);
                async move {
                    match request {
                        MdRequest::Read(reads) => load_md_tile_from_files_async::<P>(
                            &loading_info,
                            &reads,
                            reader_mode,
                            tile_info,
                            &gdal_worker,
                            default_cache_ttl,
                        )
                        .await
                        .map_err(Error::from),
                        MdRequest::Gap(gap) => Ok(Some(empty_tile(
                            &loading_info,
                            gap,
                            tile_info,
                            default_cache_ttl,
                        ))),
                    }
                }
            })
            // Concurrency, not buffering: at most this many tiles are in flight, and each one
            // is a single z slice. Deliberately *not* `slices x tiles`, which is what a reorder
            // buffer would have needed and what makes long spatial rows untenable.
            .buffered(TILE_READ_CONCURRENCY)
            .filter_map(|res: Result<Option<RasterTile2D<P>>>| {
                futures::future::ready(res.transpose())
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
    /// every file holding the z slice; they are mosaicked into one tile
    Read(Vec<MdTileRequest>),
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

    // a gap index outside `time_steps` would panic; an empty tile with a degenerate interval
    // is recoverable, a panic in a query task is not
    let time = time_steps
        .get(gap.global_z)
        .copied()
        .unwrap_or_else(TimeInterval::default);

    RasterTile2D::new_with_properties(
        time,
        tile_info.global_tile_position,
        gap.output_band,
        tile_info.global_geo_transform,
        GridOrEmpty::from(EmptyGrid::new(tile_info.global_pixel_bounds())).unbounded(),
        RasterProperties::default(),
        loading_info.cache_hint(default_cache_ttl),
    )
}

/// The output-band groups to query: one per selected band, each reading only its own files.
fn output_band_groups(band_count: u32, attributes: &BandSelection) -> Result<Vec<u32>> {
    // an out-of-range band would otherwise reach `empty_tile`/`z_slices` as a missing index
    // and panic on `time_steps[global_z]`
    for &b in attributes.as_slice() {
        if b >= band_count {
            return Err(Error::from(MdGdalSourceError::UnsupportedBandRequest {
                message: format!("band {b} does not exist, there are {band_count} bands"),
            }));
        }
    }
    Ok(attributes.as_vec())
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
        // only the time steps are needed, so no file is loaded
        let q_bounds = self
            .result_descriptor()
            .tiling_grid_definition(ctx.tiling_specification())
            .tiling_grid_bounds();
        let time_steps = self
            .meta_data
            .loading_info(MdLoadingInfoQueryRectangle {
                query_rectangle: RasterQueryRectangle::new(q_bounds, query, BandSelection::first()),
                fetch_files: false,
            })
            .await?
            .time_steps()
            .to_vec();

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

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        Ok(call_generic_raster_processor!(
            self.result_descriptor().data_type,
            MdGdalSourceProcessor {
                produced_result_descriptor: self.produced_result_descriptor.clone(),
                tiling_specification: self.tiling_specification,
                meta_data: self.meta_data.clone(),
                default_cache_ttl: self.default_cache_ttl,
                _phantom_data: PhantomData,
            }
            .boxed()
        ))
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
    use crate::source::MdAxisMapping;
    use crate::test_data;
    use futures::TryStreamExt;
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
    /// Nothing here is read from the file: MD metadata is always stated, so a read-path test
    /// builds it by hand - the same construction that
    /// `python/examples/md_gdal_source_dataset.ipynb` performs. Every constant is
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
        axis_mapping: Vec<MdAxisMapping>,
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
                axis_mapping: vec![MdAxisMapping::Time, MdAxisMapping::Y, MdAxisMapping::X],
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
        /// the global time axis as `origin + i * step`, or `None` for an array without a time
        /// axis, whose slices get synthetic `[k, k+1)` unit steps
        time: Option<(i64, i64)>,
        bands: Vec<&'static str>,
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
                time: Some((EPOCH_2000, DAY)),
                bands: vec!["temperature"],
            }
        }

        /// `(time, depth, y, x)`: 6 daily slices with depth fixed at index 0.
        fn four_dimensional() -> Self {
            let mut dataset = Self::daily_series("md/time_depth_4d.nc", 6);
            dataset.files[0].axis_mapping = vec![
                MdAxisMapping::Time,
                MdAxisMapping::Fixed { index: 0 },
                MdAxisMapping::Y,
                MdAxisMapping::X,
            ];
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
                time: Some((EPOCH_2000, DAY)),
                bands: vec!["temperature"],
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
                        axis_mapping: vec![MdAxisMapping::Time, MdAxisMapping::Y, MdAxisMapping::X],
                    })
                    .collect(),
                grid,
                width,
                height,
                wrap: true,
                time: Some((EPOCH_2000, DAY)),
                bands: arrays.iter().map(|(n, _)| *n).collect(),
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
                time: Some((-2_208_988_800_000, 60_000)),
                bands: vec!["temperature"],
            }
        }

        /// The `reflectance` array read as four variables, one per fixed index on its band
        /// dimension: each is a single-slice `(Fixed{k}, y, x)` array on its own output band,
        /// all sharing one synthetic unit time step.
        fn fixed_axis_variables() -> Self {
            Self {
                files: (0..4)
                    .map(|band| MdFile {
                        path: "md/bands.nc",
                        array_name: "reflectance",
                        group: None,
                        band,
                        z_start: 0,
                        slices: 1,
                        axis_mapping: vec![
                            MdAxisMapping::Fixed {
                                index: i64::from(band),
                            },
                            MdAxisMapping::Y,
                            MdAxisMapping::X,
                        ],
                    })
                    .collect(),
                grid: (0.0, 0.0, 30.0, -1.0),
                width: 8,
                height: 8,
                wrap: false,
                time: None,
                bands: vec!["band 0", "band 1", "band 2", "band 3"],
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
                        axis_mapping: vec![MdAxisMapping::Time, MdAxisMapping::Y, MdAxisMapping::X],
                    })
                    .collect(),
                grid: (0.0, 0.0, 30.0, -1.0),
                width: 8,
                height: 8,
                wrap: false,
                time: Some((EPOCH_2000, DAY)),
                bands: vec!["temperature", "precipitation", "cloud_area_fraction"],
            }
        }

        /// Two spatial pieces of one time step on the same 8x8 grid: the west file is valid
        /// for columns 0..4 and the east file for 4..8, each nodata elsewhere.
        fn mosaic_halves() -> Self {
            let half = |path| MdFile {
                path,
                array_name: "radiance",
                group: Some("data/band_hr/measured"),
                band: 0,
                z_start: 0,
                slices: 1,
                axis_mapping: vec![MdAxisMapping::Y, MdAxisMapping::X],
            };
            Self {
                files: vec![half("md/mosaic_west.nc"), half("md/mosaic_east.nc")],
                grid: (0.0, 0.0, 1.0, -1.0),
                width: 8,
                height: 8,
                wrap: false,
                time: Some((EPOCH_2000, DAY)),
                bands: vec!["radiance"],
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
            time,
            bands,
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
            // no time axis: each slice gets the unit interval `[k, k+1)`
            None => (0..files.iter().map(|f| f.z_start + f.slices).max().unwrap())
                .map(|i| TimeInterval::new(i as i64, i as i64 + 1).unwrap())
                .collect(),
        };

        // one variable per output band, taken from the first file of that band
        let mut variables: Vec<MdVariable> = Vec::new();
        for f in &files {
            if f.band as usize == variables.len() {
                variables.push(MdVariable {
                    name: bands[f.band as usize].to_owned(),
                    axis_mapping: f.axis_mapping.clone(),
                });
            }
        }

        let md_files = files
            .iter()
            .map(|f| MdDatasetFile {
                params: crate::source::gdal_worker_process::GdalDatasetParameters {
                    file_path: test_data!(f.path).to_path_buf(),
                    rasterband_channel: 1,
                    geo_transform: crate::source::gdal_worker_process::GdalDatasetGeoTransform {
                        origin_coordinate: (grid.0, grid.1).into(),
                        x_pixel_size: geoengine_datatypes::util::NonZeroFiniteF64::new_unchecked(
                            grid.2,
                        ),
                        y_pixel_size: geoengine_datatypes::util::NonZeroFiniteF64::new_unchecked(
                            grid.3,
                        ),
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
            })
            .collect::<Vec<_>>();

        let presented = MdPresentation::from_wrap(wrap).presented_geo_transform(
            crate::source::gdal_worker_process::GdalDatasetGeoTransform {
                origin_coordinate: (grid.0, grid.1).into(),
                x_pixel_size: geoengine_datatypes::util::NonZeroFiniteF64::new_unchecked(grid.2),
                y_pixel_size: geoengine_datatypes::util::NonZeroFiniteF64::new_unchecked(grid.3),
            },
            height,
        );
        let result_descriptor = RasterResultDescriptor::new(
            geoengine_datatypes::raster::RasterDataType::F32,
            geoengine_datatypes::spatial_reference::SpatialReference::epsg_4326().into(),
            MdLoadingInfo::new(
                time_steps.clone(),
                md_files.clone(),
                variables.clone(),
                None,
                wrap,
            )
            .unwrap()
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
                loading_info: MdLoadingInfo::new(time_steps, md_files, variables, None, wrap)
                    .unwrap(),
                result_descriptor,
                phantom: PhantomData::<MdLoadingInfoQueryRectangle>,
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
    ) -> Vec<RasterTile2D<f32>> {
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
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
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

        // window over z-slices 2..6 (4 slices), one read per slice
        let time = TimeInterval::new_unchecked(EPOCH_2000 + 2 * DAY, EPOCH_2000 + 6 * DAY);
        let tiles = query_md_source(
            &exe_ctx,
            &query_ctx,
            name,
            ts_grid_bounds(),
            time,
            BandSelection::first(),
        )
        .await;

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
    async fn test_query_four_dimensional_array_holds_the_mapped_fixed_axis() {
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
        .await;

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
        .await;

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
        .await;

        assert_eq!(tiles.len(), 4);
        for (k, tile) in tiles.iter().enumerate() {
            let t = 2 + k;
            assert_eq!(grid_value(tile, 7, 7), (t * 1000 + 7 * 10 + 7) as f32);
        }
    }

    /// A query past a wrapped dataset's presented extent still gets one empty tile per tile
    /// position, as `GdalSource` does: raster streams are gap-free, so downstream operators
    /// that zip several inputs stay aligned. This fixture stores lat 1..0 only, so rows below
    /// -1 in the tiling frame are outside the data.
    #[tokio::test]
    async fn test_query_wrap_beyond_extent_returns_empty_tiles() {
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
        // data at all -- so all four tile columns of that row come back empty.
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
        .await;

        assert_eq!(tiles.len(), 4, "one tile per tile position");
        assert!(
            tiles.iter().all(|tile| tile.grid_array.is_empty()),
            "a query entirely outside the wrapped extent must return only empty tiles"
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
        .await;

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
        .await;

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
        .await;

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
        .await;

        assert_eq!(tiles.len(), 1);
        let tile = &tiles[0];
        assert_eq!(tile.time.start().inner(), -2_208_988_800_000);
        assert_eq!(grid_value(tile, 0, 0), 70.0);
        assert_eq!(grid_value(tile, 0, 7), 77.0);
        assert_eq!(grid_value(tile, 7, 0), 0.0);
        assert_eq!(grid_value(tile, 7, 7), 7.0);
    }

    /// Packed netCDF (int16 plus CF `scale_factor`/`add_offset`) must hand the scale and
    /// offset on with the tile; without them the raw integers cannot be turned into values.
    #[tokio::test]
    async fn test_query_packed_array_carries_scale_and_offset() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let name = add_md_dataset(
            &mut exe_ctx,
            "md_packed",
            MdDataset {
                // `packed.nc` stores latitudes ascending, like `cf_time_units_minutes.nc`
                files: vec![MdFile {
                    axis_mapping: vec![MdAxisMapping::Y, MdAxisMapping::X],
                    ..MdFile::of("md/packed.nc", "Band1", 0, 1)
                }],
                grid: (0.0, -8.0, 30.0, 1.0),
                width: 8,
                height: 8,
                wrap: false,
                time: None,
                bands: vec!["ndvi"],
            },
        );

        let tiles = query_md_source(
            &exe_ctx,
            &query_ctx,
            name,
            ts_grid_bounds(),
            TimeInterval::new_instant(0).unwrap(),
            BandSelection::first(),
        )
        .await;

        assert_eq!(tiles.len(), 1);
        assert_eq!(tiles[0].properties.scale_option(), Some(0.5));
        assert_eq!(tiles[0].properties.offset_option(), Some(10.0));
    }

    #[tokio::test]
    async fn test_query_fixed_axis_variables() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let name = add_md_dataset(&mut exe_ctx, "md_bands", MdDataset::fixed_axis_variables());

        let time = TimeInterval::new_instant(0).unwrap();

        // one variable per fixed index on the band dimension, each on its own output band;
        // `grid_value` panics on an empty tile, so no gap tiles may appear here
        let tiles = query_md_source(
            &exe_ctx,
            &query_ctx,
            name.clone(),
            ts_grid_bounds(),
            time,
            BandSelection::new(vec![0, 1, 2, 3]).unwrap(),
        )
        .await;

        assert_eq!(tiles.len(), 4);
        for tile in &tiles {
            assert_eq!(grid_value(tile, 0, 0), tile.band as f32 * 100.0);
            assert_eq!(
                grid_value(tile, 7, 7),
                tile.band as f32 * 100.0 + (7 * 10 + 7) as f32
            );
        }

        // selecting only variable 2 reads exactly its fixed index
        let tiles = query_md_source(
            &exe_ctx,
            &query_ctx,
            name,
            ts_grid_bounds(),
            time,
            BandSelection::new_single(2),
        )
        .await;

        assert_eq!(tiles.len(), 1);
        let tile = &tiles[0];
        assert_eq!(tile.band, 2);
        assert_eq!(grid_value(tile, 0, 0), 200.0);
        assert_eq!(grid_value(tile, 7, 7), 200.0 + (7 * 10 + 7) as f32);
    }

    #[tokio::test]
    async fn test_query_mosaic_blends_spatial_pieces() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let name = add_md_dataset(&mut exe_ctx, "md_mosaic", MdDataset::mosaic_halves());

        let tiles = query_md_source(
            &exe_ctx,
            &query_ctx,
            name,
            ts_grid_bounds(),
            TimeInterval::new_instant(EPOCH_2000).unwrap(),
            BandSelection::first(),
        )
        .await;

        // value = row * 10 + col; both halves are present, so no nodata (-9999) is left
        assert_eq!(value_at(&tiles, 0, 0), 0.0);
        assert_eq!(value_at(&tiles, 3, 3), 33.0);
        assert_eq!(value_at(&tiles, 3, 4), 34.0);
        assert_eq!(value_at(&tiles, 7, 7), 77.0);
    }

    /// The 8x16 global grid (lat 48..-48, lon -180..180 presented) in global pixel
    /// indices, where the lattice origin is (0, 0): rows -4..3, columns -8..7. The window
    /// straddles the tile lattice, so it is split over four tiles.
    fn pressure_level_grid_bounds() -> GridBoundingBox2D {
        GridBoundingBox2D::new_unchecked([-4, -8], [3, 7])
    }

    /// The value at global pixel `(row, col)`, looked up in whichever tile covers it.
    fn value_at(tiles: &[RasterTile2D<f32>], row: isize, col: isize) -> f32 {
        tiles
            .iter()
            .find_map(|tile| {
                let GridOrEmpty::Grid(grid) = &tile.grid_array else {
                    return None;
                };
                let origin = tile.tile_information().global_pixel_bounds().min_index();
                let (r, c) = (row - origin.y(), col - origin.x());
                let shape = grid.inner_grid.shape;
                let covered =
                    (0..shape.y() as isize).contains(&r) && (0..shape.x() as isize).contains(&c);
                covered.then(|| grid_value(tile, r as usize, c as usize))
            })
            .expect("pixel is not covered by any tile")
    }

    #[tokio::test]
    async fn test_query_pressure_level_slices_fixed_level() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();

        // one variable per pressure level (1000, 850, 500 hPa); each is a `Fixed` slice of
        // `(time, level, y, x)`
        let levels = [0_usize, 1, 2];
        let mut dataset = MdDataset {
            files: levels
                .iter()
                .map(|_| MdFile::of("md/pressure_level_synthetic.nc", "temperature_3d", 0, 4))
                .collect(),
            grid: (0.0, 48.0, 22.5, -12.0),
            width: 16,
            height: 8,
            wrap: true,
            time: Some((EPOCH_2000, DAY)),
            bands: vec![
                "temperature_3d 1000",
                "temperature_3d 850",
                "temperature_3d 500",
            ],
        };
        for (file, level) in dataset.files.iter_mut().zip(levels) {
            file.band = level as u32;
            file.axis_mapping = vec![
                MdAxisMapping::Time,
                MdAxisMapping::Fixed {
                    index: level as i64,
                },
                MdAxisMapping::Y,
                MdAxisMapping::X,
            ];
        }
        let name = add_md_dataset(&mut exe_ctx, "md_pressure_level", dataset);

        // band 1 is the 850 hPa slice, time step 2
        let tiles = query_md_source(
            &exe_ctx,
            &query_ctx,
            name,
            pressure_level_grid_bounds(),
            TimeInterval::new_instant(EPOCH_2000 + 2 * DAY).unwrap(),
            BandSelection::new_single(1),
        )
        .await;

        assert_eq!(tiles.len(), 4);
        // value = t * 10_000 + p * 1000 + stored row * 10 + stored col; global (-4, -8) is
        // lat 48, lon -180, i.e. stored (0, 8)
        assert_eq!(value_at(&tiles, -4, -8), 21_008.0);
        assert_eq!(value_at(&tiles, 3, 7), 21_077.0);
    }

    #[tokio::test]
    async fn test_query_surface_without_level_axis() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let dataset = MdDataset {
            files: vec![MdFile::of(
                "md/pressure_level_synthetic.nc",
                "temperature_2d",
                0,
                4,
            )],
            grid: (0.0, 48.0, 22.5, -12.0),
            width: 16,
            height: 8,
            wrap: true,
            time: Some((EPOCH_2000, DAY)),
            bands: vec!["temperature_2d"],
        };
        let name = add_md_dataset(&mut exe_ctx, "md_surface", dataset);

        let tiles = query_md_source(
            &exe_ctx,
            &query_ctx,
            name,
            pressure_level_grid_bounds(),
            TimeInterval::new_instant(EPOCH_2000 + DAY).unwrap(),
            BandSelection::first(),
        )
        .await;

        assert_eq!(tiles.len(), 4);
        // value = t * 1000 + stored row * 10 + stored col
        assert_eq!(value_at(&tiles, -4, -8), 1_008.0);
        assert_eq!(value_at(&tiles, 3, 7), 1_077.0);
    }

    #[test]
    fn out_of_range_requested_band_is_rejected() {
        // an index past the band count would otherwise reach `empty_tile`/`z_slices` as a
        // missing index and panic on `time_steps[global_z]`
        assert!(
            output_band_groups(4, &BandSelection::new_single(5)).is_err(),
            "band 5 of a 4-band dataset must be rejected"
        );
        assert!(output_band_groups(4, &BandSelection::new_single(3)).is_ok());
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
        .await;

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
                "md/variables.nc",
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
        .await;

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

    /// `band` before `space` is the bug the fix exists for: with several bands *and* several
    /// tiles the two orderings differ, and only this shape can tell them apart.
    #[tokio::test]
    async fn test_emits_band_inside_space() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let spec = MdDataset::wrapped_arrays(
            "md/variables_multitile.nc",
            1440,
            4,
            (0.0, 1.0, 0.25, -0.25),
            2,
            &[("temperature", 0), ("precipitation", 1)],
        );
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
        .await;

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
        assert_ne!(
            keys[0].0,
            keys[keys.len() - 1].0,
            "the fixture must span two time steps"
        );
    }

    /// Band order must follow the global z, not each file's local z (which restarts at every
    /// file boundary). Band 0 is split across two files at gz 4, band 1 is a single file.
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
            axis_mapping: vec![MdAxisMapping::Time, MdAxisMapping::Y, MdAxisMapping::X],
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
        .await;

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
        .await;

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
    fn params_serde_round_trips() {
        let params: MdGdalSourceParameters = serde_json::from_str(r#"{"data": "ns:ds"}"#).unwrap();
        assert_eq!(
            params,
            MdGdalSourceParameters::new(NamedData::with_namespaced_name("ns", "ds"))
        );
        assert!(serde_json::to_value(params).is_ok());
    }
}
