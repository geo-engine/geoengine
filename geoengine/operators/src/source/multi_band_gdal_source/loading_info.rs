use super::GdalDatasetParameters;
use crate::engine::{MetaData, RasterResultDescriptor, StaticMetaData};
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
    /// For each time step whether it has data, if the provider could tell without the files.
    /// The part then has no files. This holds for the whole part: either every time step has
    /// an entry, or the part has files. Each part of a long time range decides this for itself.
    availability: Option<Vec<bool>>,
}

/// Static metadata answers with its one part, e.g. in tests and benches.
#[async_trait::async_trait]
impl<Q> MetaData<MultiBandGdalLoadingInfo, RasterResultDescriptor, Q>
    for StaticMetaData<MultiBandGdalLoadingInfoPart, RasterResultDescriptor, Q>
where
    Q: std::fmt::Debug + Clone + Send + Sync + 'static,
{
    async fn loading_info(&self, _query: Q) -> crate::util::Result<MultiBandGdalLoadingInfo> {
        Ok(stream::once(future::ready(Ok(self.loading_info.clone()))).boxed())
    }

    async fn result_descriptor(&self) -> crate::util::Result<RasterResultDescriptor> {
        Ok(self.result_descriptor.clone())
    }

    fn box_clone(&self) -> Box<dyn MetaData<MultiBandGdalLoadingInfo, RasterResultDescriptor, Q>> {
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
            availability: None,
        }
    }

    /// Marks this part as telling for each time step whether it has data, without its files.
    ///
    /// # Errors
    /// If `has_data` does not have one entry per time step, because a missing entry would read as
    /// a step without data.
    pub fn with_availability(mut self, has_data: Vec<bool>) -> crate::util::Result<Self> {
        snafu::ensure!(
            has_data.len() == self.time_steps.len(),
            crate::error::LoadingInfoAvailabilityLength {
                entries: has_data.len(),
                time_steps: self.time_steps.len(),
            }
        );
        self.availability = Some(has_data);
        Ok(self)
    }

    /// Whether the time step with this index has data, if the provider told without files.
    pub fn has_data(&self, time_step_index: usize) -> Option<bool> {
        self.availability
            .as_ref()
            .and_then(|has_data| has_data.get(time_step_index).copied())
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
        self.cache_ttl(default_ttl).into()
    }

    pub fn cache_ttl(&self, default_ttl: CacheTtlSeconds) -> CacheTtlSeconds {
        self.cache_ttl.unwrap_or(default_ttl)
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
    fn it_rejects_an_availability_without_one_entry_per_time_step() {
        let info = || {
            MultiBandGdalLoadingInfoPart::new(
                vec![
                    TimeInterval::new_unchecked(0, 5),
                    TimeInterval::new_unchecked(5, 10),
                ],
                Vec::new(),
                None,
            )
        };

        assert!(info().with_availability(vec![true]).is_err());
        assert_eq!(
            info()
                .with_availability(vec![true, false])
                .unwrap()
                .has_data(1),
            Some(false)
        );
    }
}
