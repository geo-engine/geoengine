use super::meta_data::{MultiBandGdalMetaData, StepFootprint};
use super::{GdalDatasetParameters, MultiBandGdalLoadingInfoQueryRectangle};
use crate::engine::{RasterResultDescriptor, StaticMetaData};
use crate::source::gdal_read::TilePlan;
use futures::{
    StreamExt, future,
    stream::{self, BoxStream},
};
use geoengine_datatypes::{
    primitives::{
        CacheHint, CacheTtlSeconds, SpatialPartition2D, SpatialPartitioned, TimeInterval,
    },
    raster::TileInformation,
};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, FromSql, ToSql, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GdalMultiBand {
    pub result_descriptor: RasterResultDescriptor,
    /// Dataset-level TTL fallback used when no tile-level TTL is provided.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_ttl: Option<CacheTtlSeconds>,
}

/// The loading info of a query: its parts in time order, which together have the gap-free time
/// steps of the queried time. A provider answers a long time range in several parts, so that the
/// files of all time steps are never held at once. It is a stream and not a cursor in the answer,
/// so that a caller cannot stop after the first part by mistake and the provider can keep its
/// state between the parts.
pub type MultiBandGdalLoadingInfo =
    BoxStream<'static, crate::util::Result<MultiBandGdalLoadingInfoPart>>;

/// One part of a [`MultiBandGdalLoadingInfo`]: some time steps in a row and their files.
#[derive(Debug, Clone)]
pub struct MultiBandGdalLoadingInfoPart {
    files: Vec<TileFile>,
    time_steps: Vec<TimeInterval>,
    /// Fallback TTL used when a tile does not provide its own TTL.
    cache_ttl: Option<CacheTtlSeconds>,
}

/// Static metadata answers with its one part, e.g. in tests and benches.
#[async_trait::async_trait]
impl MultiBandGdalMetaData
    for StaticMetaData<
        MultiBandGdalLoadingInfoPart,
        RasterResultDescriptor,
        MultiBandGdalLoadingInfoQueryRectangle,
    >
{
    async fn result_descriptor(&self) -> crate::util::Result<RasterResultDescriptor> {
        Ok(self.result_descriptor.clone())
    }

    async fn time_steps(
        &self,
        time: TimeInterval,
    ) -> crate::util::Result<BoxStream<'static, crate::util::Result<TimeInterval>>> {
        let steps = self.loading_info.time_steps.clone();
        Ok(stream::iter(
            steps
                .into_iter()
                .filter(move |step| step.intersects(&time))
                .map(Ok),
        )
        .boxed())
    }

    async fn loading_info(
        &self,
        _query: MultiBandGdalLoadingInfoQueryRectangle,
    ) -> crate::util::Result<MultiBandGdalLoadingInfo> {
        Ok(stream::once(future::ready(Ok(self.loading_info.clone()))).boxed())
    }

    fn box_clone(&self) -> Box<dyn MultiBandGdalMetaData> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TileFile {
    pub time: TimeInterval,
    pub spatial_partition: SpatialPartition2D,
    pub band: u32,
    pub z_index: i64,
    pub params: GdalDatasetParameters,
}

impl MultiBandGdalLoadingInfoPart {
    pub fn new(
        time_steps: Vec<TimeInterval>,
        files: Vec<TileFile>,
        cache_ttl: Option<CacheTtlSeconds>,
    ) -> Self {
        debug_assert!(!time_steps.is_empty(), "time_steps must not be empty");

        debug_assert!(
            time_steps.windows(2).all(|w| w[0] < w[1]),
            "time_steps must be sorted and must not contain duplicates"
        );

        debug_assert!(
            time_steps.windows(2).all(|w| w[0].end() == w[1].start()),
            "time_steps must be gap-free: each step must start where the previous one ends"
        );

        #[cfg(debug_assertions)]
        {
            let mut groups: std::collections::HashMap<(TimeInterval, u32), Vec<&TileFile>> =
                std::collections::HashMap::new();

            for file in &files {
                groups.entry((file.time, file.band)).or_default().push(file);
            }

            for ((time, band), group) in &groups {
                debug_assert!(
                    group.windows(2).all(|w| w[0].z_index <= w[1].z_index),
                    "Files for time {time:?} and band {band} are not sorted by z_index",
                );
            }
        }

        Self {
            files,
            time_steps,
            cache_ttl,
        }
    }

    /// Where each time step has data: the areas of its files of `bands`, none for a step without
    /// files. See [`MultiBandGdalMetaData::footprints`].
    pub fn footprints(&self, bands: &[u32]) -> Vec<(TimeInterval, StepFootprint)> {
        self.time_steps
            .iter()
            .map(|&time| {
                (
                    time,
                    StepFootprint::Areas(self.file_bounds(time, bands).collect()),
                )
            })
            .collect()
    }

    /// Return a gap-free list of time steps for the current loading info and query time.
    pub fn time_steps(&self) -> &[TimeInterval] {
        &self.time_steps
    }

    /// Return all files necessary to load a single tile, sorted by z-index.
    /// Might be empty if no files are needed.
    /// Spatial coverage alone does not establish occlusion: higher-z files may
    /// contain no-data. The reader checks actual pixel validity before skipping files.
    pub fn tile_files(
        &self,
        time: TimeInterval,
        tile: TileInformation,
        band: u32,
    ) -> Vec<GdalDatasetParameters> {
        let tile_partition = tile.spatial_partition();

        self.files
            .iter()
            .filter(|file| {
                time.intersects(&file.time)
                    && file.spatial_partition.intersects(&tile_partition)
                    && file.band == band
            })
            .map(|file| file.params.clone())
            .collect()
    }

    /// The areas of all files of a time step and bands, regardless of which tile needs them.
    pub fn file_bounds<'a>(
        &'a self,
        time: TimeInterval,
        bands: &'a [u32],
    ) -> impl Iterator<Item = SpatialPartition2D> + 'a {
        self.files
            .iter()
            .filter(move |file| time.intersects(&file.time) && bands.contains(&file.band))
            .map(|file| file.spatial_partition)
    }

    pub fn cache_hint(&self, default_ttl: CacheTtlSeconds) -> CacheHint {
        self.cache_ttl.unwrap_or(default_ttl).into()
    }

    /// The plan of one output tile, with its files highest z-index first.
    pub fn tile_plan(
        &self,
        time: TimeInterval,
        tile: TileInformation,
        band: u32,
        default_ttl: CacheTtlSeconds,
    ) -> TilePlan {
        let mut files = self.tile_files(time, tile, band);
        files.reverse();
        TilePlan {
            tile,
            time,
            band,
            files,
            cache_hint: self.cache_hint(default_ttl),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_tells_the_areas_of_the_files_of_each_step_and_none_for_a_step_without_files() {
        let area = SpatialPartition2D::new_unchecked((0., 1.).into(), (1., 0.).into());
        let file = |band| TileFile {
            time: TimeInterval::new_unchecked(0, 5),
            spatial_partition: area,
            band,
            z_index: 0,
            params: GdalDatasetParameters {
                file_path: "/foo/bar.tiff".into(),
                rasterband_channel: 1,
                geo_transform: geoengine_datatypes::util::test::TestDefault::test_default(),
                width: 1,
                height: 1,
                file_not_found_handling: crate::source::FileNotFoundHandling::NoData,
                no_data_value: None,
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: true,
                retry: None,
            },
        };
        let part = MultiBandGdalLoadingInfoPart::new(
            vec![
                TimeInterval::new_unchecked(0, 5),
                TimeInterval::new_unchecked(5, 10),
            ],
            vec![file(0), file(1)],
            None,
        );

        assert_eq!(
            part.footprints(&[1]),
            vec![
                (
                    TimeInterval::new_unchecked(0, 5),
                    StepFootprint::Areas(vec![area])
                ),
                (
                    TimeInterval::new_unchecked(5, 10),
                    StepFootprint::Areas(vec![])
                ),
            ]
        );
    }
}
