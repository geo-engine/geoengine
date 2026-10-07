use super::common;
use super::{
    StacClient, StacDataProvider, StacProviderDataset, StacProviderS3Config,
    cache::StacQueryCache,
    grid::{ProjectedStacGrid, StacGrid, StacGridCells},
};
use crate::error::Result;
use crate::util::format_stac_wgs84_bbox;
use crate::util::join_base_url_and_path;
use crate::util::retry::{RetryPolicy, retry_http};
use async_trait::async_trait;
use chrono::DateTime as ChronoDateTime;
use geoengine_datatypes::dataset::DataId;
use geoengine_datatypes::operations::reproject::ReprojectClipped;
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BoundingBox2D, CacheTtlSeconds, RasterQueryRectangle, SpatialPartition2D,
    TimeDimension, TimeInstance, TimeInterval, VectorQueryRectangle,
};
use geoengine_datatypes::raster::{GridBoundingBox2D, GridIdx2D};
use geoengine_datatypes::spatial_reference::{
    CoordinateProjection, DefaultCoordinateProjector, SpatialReference, SpatialReferenceAuthority,
};
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterBandDescriptors, RasterResultDescriptor, TimeDescriptor,
    VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{
    FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalLoadingInfo,
    GdalRetryOptions, MultiBandGdalLoadingInfo, MultiBandGdalLoadingInfoQueryRectangle,
    OgrSourceDataset, TileFile,
};
use ordered_float::OrderedFloat;
use stac::Item;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use tracing::debug;
use url::Url;

#[derive(Debug, Clone)]
struct StacMultiBandMetaData {
    api_url: String,
    collection_name: String,
    s3_config: Option<StacProviderS3Config>,
    time_dimension: TimeDimension,
    dataset: StacProviderDataset,
    page_limit: i64,
    client: StacClient,
    cache_ttl_secs: Option<CacheTtlSeconds>,
    /// Shared query-result cache from the provider.
    query_cache: Arc<StacQueryCache>,
    grid: StacGrid,
}

#[derive(Debug, Clone)]
enum StacQueryState {
    FirstPage {
        query_url: Url,
        query_params: Vec<(String, String)>,
    },
    NextPage {
        next_url: Url,
    },
    Finished,
}

async fn query_stac_item_collection(
    client: &StacClient,
    query_state: &StacQueryState,
) -> geoengine_operators::util::Result<(stac::ItemCollection, StacQueryState)> {
    let request_policy = RetryPolicy::new().stop_on_status(&[400, 404]);

    match query_state {
        StacQueryState::FirstPage {
            query_url,
            query_params,
        } => {
            debug!("STAC query first page with parameters: {:?}", query_params);

            let request_started = std::time::Instant::now();

            let item_collection: stac::ItemCollection = retry_http(
                || async {
                    let request = client.get(query_url.clone()).await?.query(query_params);
                    let response = request.send().await?;
                    response.error_for_status()?.json().await
                },
                &format!("Fetch STAC items from {query_url}"),
                &request_policy,
                |e| e.status().map(|s| s.as_u16()),
            )
            .await
            .map_err(|e| {
                geoengine_operators::error::Error::QueryingProcessorFailed {
                    source: Box::new(e),
                }
            })?;

            debug!(
                "STAC response received in {:?} s",
                request_started.elapsed().as_secs_f64()
            );

            let next_state = item_collection
                .links
                .iter()
                .find(|link| link.rel == "next")
                .and_then(|link| Url::parse(&link.href).ok())
                .map_or(StacQueryState::Finished, |next_url| {
                    StacQueryState::NextPage { next_url }
                });

            Ok((item_collection, next_state))
        }
        StacQueryState::NextPage { next_url } => {
            debug!("STAC query next page with url: {}", next_url);

            let request_started = std::time::Instant::now();

            let item_collection: stac::ItemCollection = retry_http(
                || async {
                    let request = client.get(next_url.clone()).await?;
                    let response = request.send().await?;
                    response.error_for_status()?.json().await
                },
                &format!("Fetch next STAC page from {next_url}"),
                &request_policy,
                |e| e.status().map(|s| s.as_u16()),
            )
            .await
            .map_err(|e| {
                geoengine_operators::error::Error::QueryingProcessorFailed {
                    source: Box::new(e),
                }
            })?;

            debug!(
                "STAC response received in {:?} s",
                request_started.elapsed().as_secs_f64()
            );

            let next_state = item_collection
                .links
                .iter()
                .find(|link| link.rel == "next")
                .and_then(|link| Url::parse(&link.href).ok())
                .map_or(StacQueryState::Finished, |next_url| {
                    StacQueryState::NextPage { next_url }
                });

            Ok((item_collection, next_state))
        }
        StacQueryState::Finished => {
            Err(geoengine_operators::error::Error::QueryingProcessorFailed {
                source: "no more STAC pages to query".into(),
            })
        }
    }
}

#[async_trait]
impl
    MetaData<
        MultiBandGdalLoadingInfo,
        RasterResultDescriptor,
        MultiBandGdalLoadingInfoQueryRectangle,
    > for StacMultiBandMetaData
{
    async fn loading_info(
        &self,
        query: MultiBandGdalLoadingInfoQueryRectangle,
    ) -> geoengine_operators::util::Result<MultiBandGdalLoadingInfo> {
        let time_interval =
            stac_query_time_interval(query.query_rectangle.time_interval(), self.time_dimension)?;

        if !query.fetch_tiles {
            // Regular layers expose every step, including steps without any assets.
            // Their time axis is defined by the provider, so no STAC search is needed.
            return Ok(MultiBandGdalLoadingInfo::new(
                RegularTimeStepIter::new(self.time_dimension, time_interval)?
                    .collect::<geoengine_operators::util::Result<Vec<_>>>()?,
                Vec::new(),
                self.cache_ttl_secs,
            ));
        }

        let spatial_bounds = query.query_rectangle.spatial_bounds();

        let base_url = Url::from_str(&self.api_url).map_err(|e| {
            geoengine_operators::error::Error::InvalidDataProviderConfig {
                reason: format!("invalid STAC API URL: {e}"),
            }
        })?;
        let items_url = join_base_url_and_path(
            &base_url,
            &format!("collections/{}/items", self.collection_name),
        )
        .map_err(
            |e| geoengine_operators::error::Error::InvalidDataProviderConfig {
                reason: format!("could not construct STAC items URL: {e}"),
            },
        )?;

        let (grid, cells) = self.grid_cells(spatial_bounds)?;
        if cells.clone().next().is_none() {
            // Preserve the existing empty result for queries outside the CRS area of use.
            let time_steps = RegularTimeStepIter::new(self.time_dimension, time_interval)?
                .collect::<geoengine_operators::util::Result<Vec<_>>>()?;
            return Ok(MultiBandGdalLoadingInfo::new(
                time_steps,
                Vec::new(),
                self.cache_ttl_secs,
            ));
        }

        let mut time_steps = Vec::new();
        let mut tile_files = Vec::new();
        for step in RegularTimeStepIter::new(self.time_dimension, time_interval)? {
            let step = step?;
            time_steps.push(step);

            for cell in cells.clone() {
                let Some(clipped) = grid.clip_cell_to_extent(cell.bbox) else {
                    continue;
                };
                let Some(bbox) = stac_query_bbox(clipped, self.dataset.projection)? else {
                    continue;
                };
                let metadata = self.clone();
                let url = items_url.clone();
                let output = self
                    .query_cache
                    .get_or_fetch(self.dataset.clone(), cell.index, step, move || async move {
                        metadata.search_bbox(url, bbox, step).await
                    })
                    .await
                    .map_err(|error| {
                        geoengine_operators::error::Error::QueryingProcessorFailed {
                            source: Box::new(error),
                        }
                    })?;
                tile_files.extend(output.iter().cloned());
            }
        }

        tile_files.retain(|file| {
            time_interval.intersects(&file.time)
                && file.spatial_partition.intersects(&spatial_bounds)
        });
        tile_files.sort_by(|left, right| {
            StacTileIdentity::from(left).cmp(&StacTileIdentity::from(right))
        });
        tile_files.dedup_by(|left, right| {
            StacTileIdentity::from(&*left) == StacTileIdentity::from(&*right)
        });

        Ok(MultiBandGdalLoadingInfo::new(
            time_steps,
            tile_files,
            self.cache_ttl_secs,
        ))
    }

    async fn result_descriptor(&self) -> geoengine_operators::util::Result<RasterResultDescriptor> {
        let bands = RasterBandDescriptors::new(
            self.dataset
                .bands
                .iter()
                .map(|b| b.band_descriptor.clone())
                .collect(),
        )?;

        Ok(RasterResultDescriptor {
            data_type: self.dataset.data_type,
            spatial_reference: self.dataset.projection.into(),
            time: TimeDescriptor {
                bounds: None,
                dimension: self.time_dimension,
            },
            spatial_grid: self.dataset.spatial_grid,
            bands,
        })
    }

    fn box_clone(
        &self,
    ) -> Box<
        dyn MetaData<
                MultiBandGdalLoadingInfo,
                RasterResultDescriptor,
                MultiBandGdalLoadingInfoQueryRectangle,
            >,
    > {
        Box::new(self.clone())
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for StacDataProvider
{
    async fn meta_data(
        &self,
        _id: &DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotImplemented)
    }
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for StacDataProvider
{
    async fn meta_data(
        &self,
        _id: &DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotImplemented)
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for StacDataProvider
{
    async fn meta_data(
        &self,
        _id: &DataId,
    ) -> Result<
        Box<
            dyn MetaData<
                    MockDatasetDataSourceLoadingInfo,
                    VectorResultDescriptor,
                    VectorQueryRectangle,
                >,
        >,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotImplemented)
    }
}

impl StacMultiBandMetaData {
    /// Projects the dataset area of use and selects all grid cells touching the query.
    fn grid_cells(
        &self,
        spatial_bounds: SpatialPartition2D,
    ) -> geoengine_operators::util::Result<(ProjectedStacGrid, StacGridCells)> {
        let projected_extent = self
            .dataset
            .projection
            .area_of_use_projected::<geoengine_datatypes::primitives::BoundingBox2D>()
            .map_err(
                |e| geoengine_operators::error::Error::InvalidDataProviderConfig {
                    reason: format!("could not get STAC CRS projected area of use: {e}"),
                },
            )?;

        let grid = self.grid.for_extent(projected_extent).map_err(|reason| {
            geoengine_operators::error::Error::InvalidDataProviderConfig {
                reason: reason.to_owned(),
            }
        })?;
        let cells = grid
            .cells_for_bbox(spatial_bounds.as_bbox())
            .map_err(
                |reason| geoengine_operators::error::Error::InvalidDataProviderConfig {
                    reason: reason.to_owned(),
                },
            )?;

        Ok((grid, cells))
    }

    /// Searches one complete projected grid cell for a single regular time step.
    ///
    /// Pagination must finish before this result enters the cache, so failed
    /// pages never publish partial tile files.
    async fn search_bbox(
        &self,
        items_url: Url,
        bbox: geoengine_datatypes::primitives::BoundingBox2D,
        time_interval: TimeInterval,
    ) -> geoengine_operators::util::Result<Vec<TileFile>> {
        let mut files = Vec::new();
        let mut state = StacQueryState::FirstPage {
            query_url: items_url,
            query_params: self.create_stac_query_params(bbox, time_interval)?,
        };

        while !matches!(state, StacQueryState::Finished) {
            let (collection, next_state) = query_stac_item_collection(&self.client, &state).await?;
            for item in collection.items {
                self.process_stac_item(&item, &mut files).map_err(|error| {
                    geoengine_operators::error::Error::LoadingInfo {
                        source: Box::new(error),
                    }
                })?;
            }
            state = next_state;
        }

        Ok(files)
    }

    fn create_stac_query_params(
        &self,
        bbox: geoengine_datatypes::primitives::BoundingBox2D,
        time_interval: TimeInterval,
    ) -> geoengine_operators::util::Result<Vec<(String, String)>> {
        let time_start = time_interval.start();
        let time_end = time_interval.end();

        let query_params = vec![
            ("bbox".to_owned(), format_stac_wgs84_bbox(bbox)),
            (
                "datetime".to_owned(),
                format!(
                    "{}/{}",
                    time_start
                        .as_date_time()
                        .ok_or_else(|| {
                            geoengine_operators::error::Error::InvalidDataProviderConfig {
                                reason: "query time start is not representable as a datetime"
                                    .to_owned(),
                            }
                        })?
                        .to_datetime_string_with_millis(),
                    time_end
                        .as_date_time()
                        .ok_or_else(|| {
                            geoengine_operators::error::Error::InvalidDataProviderConfig {
                                reason: "query time end is not representable as a datetime"
                                    .to_owned(),
                            }
                        })?
                        .to_datetime_string_with_millis(),
                ),
            ),
            ("limit".to_owned(), self.page_limit.to_string()),
            ("fields".to_owned(), common::STAC_ITEM_FIELDS.to_owned()),
        ];

        Ok(query_params)
    }

    fn process_stac_item(&self, item: &Item, files: &mut Vec<TileFile>) -> Result<()> {
        let Some((time, z_index)) = self.item_time_and_z_index(item)? else {
            return Ok(());
        };

        let item_epsg = common::epsg_code_from_item(item, common::StacExtensionMajorVersion::V2);

        for asset in item.assets.values() {
            self.process_stac_asset(asset, item_epsg, time, z_index, files)?;
        }

        Ok(())
    }

    fn item_time_and_z_index(&self, item: &Item) -> Result<Option<(TimeInterval, i64)>> {
        if item.version != stac::Version::v1_1_0 {
            tracing::warn!(
                "Skipping STAC item with unsupported version: {:?}",
                item.version
            );
            return Ok(None);
        }

        let Some(item_datetime) = item.properties.datetime else {
            tracing::warn!("Skipping STAC item without datetime: {}", item.id);
            return Ok(None);
        };

        let z_index = item
            .properties
            .updated
            .as_deref()
            .and_then(|updated| ChronoDateTime::parse_from_rfc3339(updated).ok())
            .map_or_else(
                || item_datetime.timestamp_millis(),
                |updated| updated.timestamp_millis(),
            );

        let item_time =
            TimeInstance::from_millis(item_datetime.timestamp_millis()).map_err(|e| {
                geoengine_operators::error::Error::InvalidDataProviderConfig {
                    reason: format!("could not convert STAC item datetime to Geo Engine time: {e}"),
                }
            })?;

        // Shared with the STAC harvester so both produce identical intervals.
        let time =
            common::snap_time_interval(item_time, &self.time_dimension).ok_or_else(|| {
                geoengine_operators::error::Error::InvalidDataProviderConfig {
                    reason: "could not snap STAC item datetime to configured time dimension"
                        .to_owned(),
                }
            })?;

        Ok(Some((time, z_index)))
    }

    fn process_stac_asset(
        &self,
        asset: &stac::Asset,
        item_epsg: Option<u32>,
        time: TimeInterval,
        z_index: i64,
        files: &mut Vec<TileFile>,
    ) -> Result<()> {
        if common::data_type_from_asset_v1_1_0_fallback(asset) != Some(self.dataset.data_type) {
            return Ok(());
        }

        let Some(asset_epsg) = common::epsg_code_from_fields(
            common::StacExtensionMajorVersion::V2,
            &asset.additional_fields,
        )
        .or(item_epsg) else {
            return Ok(());
        };

        if SpatialReference::new(SpatialReferenceAuthority::Epsg, asset_epsg)
            != self.dataset.projection
        {
            return Ok(());
        }

        let Some(geo_transform) = common::geo_transform_from_fields(&asset.additional_fields)
        else {
            tracing::warn!(
                "Skipping asset with href {} due to missing geo transform",
                asset.href
            );
            return Ok(());
        };

        let Some((height, width)) = common::proj_shape_from_fields(&asset.additional_fields) else {
            tracing::warn!(
                "Skipping asset with href {} due to missing projection shape",
                asset.href
            );
            return Ok(());
        };

        if (geo_transform.x_pixel_size().abs() - self.dataset.resolution.x).abs() > 1e-9
            || (geo_transform.y_pixel_size().abs() - self.dataset.resolution.y).abs() > 1e-9
        {
            return Ok(());
        }

        let Some(asset_title) = asset.title.as_deref() else {
            tracing::warn!(
                "Skipping asset with href {} due to missing title",
                asset.href
            );
            return Ok(());
        };

        let grid_bounds = stac_asset_grid_bounds(height, width)?;
        let spatial_partition = geo_transform.grid_to_spatial_bounds(&grid_bounds);

        let file_path = if asset.href.starts_with("http://")
            || asset.href.starts_with("https://")
            || asset.href.starts_with("s3://")
        {
            PathBuf::from(&asset.href)
        } else {
            return Err(
                geoengine_operators::error::Error::InvalidDataProviderConfig {
                    reason: format!("unsupported STAC asset href scheme in {:?}", asset.href),
                }
                .into(),
            );
        };

        let gdal_config_options =
            common::gdal_config_options_for_file_path(&file_path, self.s3_config.as_ref(), None);

        for (dataset_band_idx, dataset_band) in self.dataset.bands.iter().enumerate() {
            if dataset_band.asset_band.asset_title != asset_title {
                continue;
            }

            let Some(rasterband_channel) = common::rasterband_channel_for_dataset_band(
                asset,
                dataset_band.asset_band.band_name.as_deref(),
            ) else {
                continue;
            };

            files.push(TileFile {
                time,
                spatial_partition,
                band: dataset_band_idx as u32,
                z_index,
                params: GdalDatasetParameters {
                    file_path: file_path.clone(),
                    rasterband_channel,
                    geo_transform: GdalDatasetGeoTransform {
                        origin_coordinate: geo_transform.origin_coordinate(),
                        x_pixel_size: geo_transform.x_pixel_size(),
                        y_pixel_size: geo_transform.y_pixel_size(),
                    },
                    width,
                    height,
                    file_not_found_handling: FileNotFoundHandling::Error,
                    no_data_value: None,
                    properties_mapping: None,
                    gdal_open_options: None,
                    gdal_config_options: gdal_config_options.clone(),
                    allow_alphaband_as_mask: false,
                    retry: Some(GdalRetryOptions { max_retries: 99 }), // TODO: make configurable?
                },
            });
        }

        Ok(())
    }
}

/// Ordered borrowed key containing every field used to identify a returned tile.
/// Ordered float fields keep ordering and equality consistent for signed zero.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct StacTileIdentity<'a> {
    time_start: i64,
    time_end: i64,
    band: u32,
    z_index: i64,
    file_path: &'a Path,
    rasterband_channel: usize,
    width: usize,
    height: usize,
    geo_transform: [OrderedFloat<f64>; 4],
    spatial_bounds: [OrderedFloat<f64>; 4],
}

impl<'a> From<&'a TileFile> for StacTileIdentity<'a> {
    fn from(file: &'a TileFile) -> Self {
        let lower_left = file.spatial_partition.lower_left();
        let upper_right = file.spatial_partition.upper_right();
        let geo_transform = &file.params.geo_transform;

        Self {
            time_start: file.time.start().inner(),
            time_end: file.time.end().inner(),
            band: file.band,
            z_index: file.z_index,
            file_path: file.params.file_path.as_path(),
            rasterband_channel: file.params.rasterband_channel,
            width: file.params.width,
            height: file.params.height,
            geo_transform: [
                OrderedFloat(geo_transform.origin_coordinate.x),
                OrderedFloat(geo_transform.origin_coordinate.y),
                OrderedFloat(geo_transform.x_pixel_size),
                OrderedFloat(geo_transform.y_pixel_size),
            ],
            spatial_bounds: [
                OrderedFloat(lower_left.x),
                OrderedFloat(lower_left.y),
                OrderedFloat(upper_right.x),
                OrderedFloat(upper_right.y),
            ],
        }
    }
}

fn stac_asset_grid_bounds(
    height: usize,
    width: usize,
) -> geoengine_operators::util::Result<GridBoundingBox2D> {
    GridBoundingBox2D::new(
        GridIdx2D::new([0, 0]),
        GridIdx2D::new([(height as isize) - 1, (width as isize) - 1]),
    )
    .map_err(
        |e| geoengine_operators::error::Error::InvalidDataProviderConfig {
            reason: format!("could not create grid bounds from STAC asset projection shape: {e}"),
        },
    )
}

fn stac_query_bbox(
    bounds: geoengine_datatypes::primitives::BoundingBox2D,
    spatial_reference: SpatialReference,
) -> geoengine_operators::util::Result<Option<geoengine_datatypes::primitives::BoundingBox2D>> {
    let ll = bounds.lower_left();
    let ur = bounds.upper_right();
    if ![ll.x, ll.y, ur.x, ur.y]
        .iter()
        .all(|value| value.is_finite())
    {
        return Err(
            geoengine_operators::error::Error::InvalidDataProviderConfig {
                reason: "STAC query bounds must be finite".to_owned(),
            },
        );
    }
    if spatial_reference == SpatialReference::epsg_4326() {
        if ll.x > ur.x || ll.y > ur.y {
            return Ok(None);
        }

        let world = BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into());
        return Ok(bounds.intersection(&world));
    }
    let projector = DefaultCoordinateProjector::from_known_srs(
        spatial_reference,
        SpatialReference::epsg_4326(),
    )
    .map_err(
        |e| geoengine_operators::error::Error::InvalidDataProviderConfig {
            reason: format!("could not create coordinate projector for STAC bbox: {e}"),
        },
    )?;

    bounds.reproject_clipped(&projector).map_err(|e| {
        geoengine_operators::error::Error::InvalidDataProviderConfig {
            reason: format!("could not reproject query bounds to STAC coordinates: {e}"),
        }
    })
}

/// Lazily walks the regular steps contained by an interval and reports alignment or overflow errors.
///
/// Tile queries consume this iterator as they search cells, so long time ranges do
/// not allocate a second full list before the result itself is assembled.
struct RegularTimeStepIter {
    next_start: TimeInstance,
    interval_end: TimeInstance,
    step: geoengine_datatypes::primitives::TimeStep,
    finished: bool,
}

impl RegularTimeStepIter {
    fn new(
        time_dimension: TimeDimension,
        interval: TimeInterval,
    ) -> geoengine_operators::util::Result<Self> {
        let TimeDimension::Regular(regular) = time_dimension else {
            return Err(
                geoengine_operators::error::Error::InvalidDataProviderConfig {
                    reason: "irregular STAC time dimensions are not supported".to_owned(),
                },
            );
        };
        if regular.step.step == 0 {
            return Err(
                geoengine_operators::error::Error::InvalidDataProviderConfig {
                    reason: "STAC regular time step must be positive".to_owned(),
                },
            );
        }

        Ok(Self {
            next_start: interval.start(),
            interval_end: interval.end(),
            step: regular.step,
            finished: false,
        })
    }
}

impl Iterator for RegularTimeStepIter {
    type Item = geoengine_operators::util::Result<TimeInterval>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished || self.next_start >= self.interval_end {
            return None;
        }

        let start = self.next_start;
        let end = match (start + self.step).map_err(|error| {
            geoengine_operators::error::Error::InvalidDataProviderConfig {
                reason: format!("STAC regular time step overflow: {error}"),
            }
        }) {
            Ok(end) if end > start => end,
            Ok(_) => {
                self.finished = true;
                return Some(Err(
                    geoengine_operators::error::Error::InvalidDataProviderConfig {
                        reason: "STAC regular time step did not advance time".to_owned(),
                    },
                ));
            }
            Err(error) => {
                self.finished = true;
                return Some(Err(error));
            }
        };
        if end > self.interval_end {
            self.finished = true;
            return Some(Err(
                geoengine_operators::error::Error::InvalidDataProviderConfig {
                    reason: "STAC time interval does not end on a regular step boundary".to_owned(),
                },
            ));
        }

        let step = match TimeInterval::new(start, end) {
            Ok(step) => step,
            Err(error) => {
                self.finished = true;
                return Some(Err(
                    geoengine_operators::error::Error::InvalidDataProviderConfig {
                        reason: format!("invalid STAC time step interval: {error}"),
                    },
                ));
            }
        };
        self.next_start = end;
        Some(Ok(step))
    }
}

fn stac_query_time_interval(
    query_time_interval: TimeInterval,
    time_dimension: TimeDimension,
) -> geoengine_operators::util::Result<TimeInterval> {
    match time_dimension {
        TimeDimension::Regular(regular) => {
            if regular.step.step == 0 {
                return Err(
                    geoengine_operators::error::Error::InvalidDataProviderConfig {
                        reason: "STAC regular time step must be positive".to_owned(),
                    },
                );
            }
            let start = regular
                .snap_prev(query_time_interval.start())
                .map_err(
                    |e| geoengine_operators::error::Error::InvalidDataProviderConfig {
                        reason: format!(
                            "could not snap query start to previous regular time step: {e}"
                        ),
                    },
                )?;
            let end = regular.snap_next(query_time_interval.end()).map_err(|e| {
                geoengine_operators::error::Error::InvalidDataProviderConfig {
                    reason: format!("could not snap query end to next regular time step: {e}"),
                }
            })?;

            let end = if end <= start {
                (start + regular.step).map_err(|e| {
                    geoengine_operators::error::Error::InvalidDataProviderConfig {
                        reason: format!(
                            "could not extend empty snapped query interval by one regular step: {e}"
                        ),
                    }
                })?
            } else {
                end
            };

            TimeInterval::new(start, end).map_err(|e| {
                geoengine_operators::error::Error::InvalidDataProviderConfig {
                    reason: format!("could not construct snapped query time interval: {e}"),
                }
            })
        }
        TimeDimension::Irregular => Err(
            geoengine_operators::error::Error::InvalidDataProviderConfig {
                reason: "irregular STAC time dimensions are not supported".to_owned(),
            },
        ),
    }
}

#[async_trait]
impl
    MetaDataProvider<
        MultiBandGdalLoadingInfo,
        RasterResultDescriptor,
        MultiBandGdalLoadingInfoQueryRectangle,
    > for StacDataProvider
{
    async fn meta_data(
        &self,
        id: &DataId,
    ) -> geoengine_operators::util::Result<
        Box<
            dyn MetaData<
                    MultiBandGdalLoadingInfo,
                    RasterResultDescriptor,
                    MultiBandGdalLoadingInfoQueryRectangle,
                >,
        >,
    > {
        let dataset = self.dataset_from_data_id(id)?;

        Ok(Box::new(StacMultiBandMetaData {
            api_url: self.api_url.clone(),
            collection_name: self.collection_name.clone(),
            s3_config: self.s3_config.clone(),
            time_dimension: self.time_dimension,
            dataset: dataset.clone(),
            page_limit: self.page_limit,
            cache_ttl_secs: self.cache_ttl_secs,
            client: self.client.clone(),
            query_cache: self.query_cache.clone(),
            grid: self.grid,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{ApplicationContext, PostgresContext, SessionContext};
    use crate::layers::storage::LayerProviderDb;
    use crate::util::tests::admin_login;
    use geoengine_datatypes::dataset::{DataProviderId, ExternalDataId};
    use geoengine_datatypes::primitives::{
        BandSelection, DateTime, RegularTimeDimension, SpatialPartition2D, SpatialResolution,
        TimeGranularity, TimeStep,
    };
    use geoengine_datatypes::raster::{
        GeoTransform, GridBoundingBox2D, GridIdx2D, GridShape, TileInformation,
    };
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceAuthority};
    use geoengine_datatypes::util::Identifier;
    use geoengine_operators::engine::SpatialGridDescriptor;
    use geoengine_operators::engine::{
        MetaData, MetaDataProvider, RasterBandDescriptor, RasterResultDescriptor,
        WorkflowOperatorPath,
    };
    use geoengine_operators::source::{
        MultiBandGdalLoadingInfo, MultiBandGdalLoadingInfoQueryRectangle,
    };
    use httptest::{
        Expectation, Server, all_of,
        matchers::{contains, request, url_decoded},
        responders,
    };
    use std::time::Duration;
    use tokio_postgres::NoTls;

    fn make_stac_provider_def(
        provider_id: DataProviderId,
        api_url: String,
    ) -> crate::datasets::external::stac::StacDataProviderDefinition {
        crate::datasets::external::stac::StacDataProviderDefinition {
            name: "Sentinel 2 L2A from STAC".to_owned(),
            id: provider_id,
            description: String::new(),
            priority: Some(50),
            api_url,
            collection_name: "sentinel-2-l2a".to_owned(),
            s3_config: None,
            authentication: None,
            time_dimension: TimeDimension::Regular(RegularTimeDimension::new_with_epoch_origin(
                TimeStep {
                    granularity: TimeGranularity::Days,
                    step: 1,
                },
            )),
            datasets: vec![crate::datasets::external::stac::StacProviderDataset {
                name: "Sentinel-2 L2A EPSG:32632 U16 10m".to_owned(),
                description: String::new(),
                data_type: geoengine_datatypes::raster::RasterDataType::U16,
                resolution: SpatialResolution::new_unchecked(10.0, 10.0),
                projection: SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632),
                spatial_grid: SpatialGridDescriptor::source_from_parts(
                    GeoTransform::new((399_960.0, 5_700_000.0).into(), 10.0, -10.0),
                    GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([10979, 10979]))
                        .unwrap(),
                ),
                bands: vec![
                    crate::datasets::external::stac::StacProviderDatasetBand {
                        asset_band: crate::datasets::external::stac::StacAssetBand {
                            asset_title: "NIR 1 (band 8) - 10m".to_owned(),
                            band_name: Some("B08".to_owned()),
                        },
                        band_descriptor: RasterBandDescriptor::new_unitless("B08".to_owned()),
                    },
                    crate::datasets::external::stac::StacProviderDatasetBand {
                        asset_band: crate::datasets::external::stac::StacAssetBand {
                            asset_title: "Red (band 4) - 10m".to_owned(),
                            band_name: Some("B04".to_owned()),
                        },
                        band_descriptor: RasterBandDescriptor::new_unitless("B04".to_owned()),
                    },
                ],
            }],
            page_limit: 100,
            query_timeout_secs: 60,
            cache_ttl_secs: None,
            stac_grid: None,
        }
    }

    #[test]
    fn stac_bbox_rejects_nonfinite_bounds_before_clipping() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let invalid = SpatialPartition2D::new_unchecked((value, 1.).into(), (1., 0.).into());
            for projection in [
                SpatialReference::epsg_4326(),
                SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857),
            ] {
                assert!(stac_query_bbox(invalid.as_bbox(), projection).is_err());
            }
        }
    }

    #[test]
    fn stac_bbox_clips_queries_to_projection_domain() {
        let srs = SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632);
        let inside =
            SpatialPartition2D::new((500_000., 5_800_000.).into(), (510_000., 5_790_000.).into())
                .unwrap();
        assert!(stac_query_bbox(inside.as_bbox(), srs).unwrap().is_some());

        let outside = SpatialPartition2D::new(
            (-500_000., 5_800_000.).into(),
            (-490_000., 5_790_000.).into(),
        )
        .unwrap();
        assert!(stac_query_bbox(outside.as_bbox(), srs).unwrap().is_none());

        let overlapping = SpatialPartition2D::new(
            (-500_000., 5_800_000.).into(),
            (510_000., 5_790_000.).into(),
        )
        .unwrap();
        let bbox = stac_query_bbox(overlapping.as_bbox(), srs)
            .unwrap()
            .unwrap();
        assert!(bbox.lower_left().x < 9.);
        assert!(bbox.upper_right().x > 9.);
    }

    #[test]
    fn stac_bbox_invalid_projection_is_still_an_error() {
        let bounds = SpatialPartition2D::new((0., 1.).into(), (1., 0.).into()).unwrap();
        assert!(
            stac_query_bbox(
                bounds.as_bbox(),
                SpatialReference::new(SpatialReferenceAuthority::Epsg, 0),
            )
            .is_err()
        );
    }

    #[test]
    fn regular_time_steps_cover_calendar_steps_and_reject_zero_or_overflow() {
        let monthly =
            TimeDimension::Regular(RegularTimeDimension::new_with_epoch_origin(TimeStep {
                granularity: TimeGranularity::Months,
                step: 1,
            }));
        let start = DateTime::new_utc(2026, 1, 1, 0, 0, 0);
        let end = DateTime::new_utc(2026, 4, 1, 0, 0, 0);
        let steps = RegularTimeStepIter::new(monthly, TimeInterval::new(start, end).unwrap())
            .unwrap()
            .collect::<geoengine_operators::util::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(steps.len(), 3);
        assert_eq!(steps.first().unwrap().start(), TimeInstance::from(start));
        assert_eq!(steps.last().unwrap().end(), TimeInstance::from(end));

        let zero = TimeDimension::Regular(RegularTimeDimension::new_with_epoch_origin(TimeStep {
            granularity: TimeGranularity::Days,
            step: 0,
        }));
        assert!(RegularTimeStepIter::new(zero, TimeInterval::new(start, end).unwrap()).is_err());

        let daily = TimeDimension::Regular(RegularTimeDimension::new_with_epoch_origin(TimeStep {
            granularity: TimeGranularity::Days,
            step: 1,
        }));
        let min_end = TimeInstance::from_millis(TimeInstance::MIN.inner() + 1).unwrap();
        let sentinel_interval = TimeInterval::new(TimeInstance::MIN, min_end).unwrap();
        assert!(
            RegularTimeStepIter::new(daily, sentinel_interval)
                .unwrap()
                .next()
                .unwrap()
                .is_err()
        );

        let near_max = TimeInstance::from_millis(TimeInstance::MAX.inner() - 86_400_000).unwrap();
        let overflow_interval = TimeInterval::new(near_max, TimeInstance::MAX).unwrap();
        let monthly =
            TimeDimension::Regular(RegularTimeDimension::new_with_epoch_origin(TimeStep {
                granularity: TimeGranularity::Months,
                step: 1,
            }));
        assert!(
            RegularTimeStepIter::new(monthly, overflow_interval)
                .unwrap()
                .next()
                .unwrap()
                .is_err()
        );
    }

    fn identity_tile() -> TileFile {
        TileFile {
            time: TimeInterval::new_unchecked(0_i64, 10_i64),
            spatial_partition: SpatialPartition2D::new((0., 1.).into(), (1., 0.).into()).unwrap(),
            band: 0,
            z_index: 0,
            params: GdalDatasetParameters {
                file_path: PathBuf::from("/data/tile.tif"),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: (0., 1.).into(),
                    x_pixel_size: 1.,
                    y_pixel_size: -1.,
                },
                width: 10,
                height: 10,
                file_not_found_handling: FileNotFoundHandling::Error,
                no_data_value: None,
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: false,
                retry: None,
            },
        }
    }

    #[test]
    fn tile_identity_ord_matches_equality_and_retains_distinct_tiles() {
        let mut positive_zero = identity_tile();
        positive_zero.params.geo_transform.origin_coordinate.x = 0.;
        let mut negative_zero = identity_tile();
        negative_zero.params.geo_transform.origin_coordinate.x = -0.;
        assert_eq!(
            StacTileIdentity::from(&positive_zero),
            StacTileIdentity::from(&negative_zero)
        );
        assert_eq!(
            StacTileIdentity::from(&positive_zero).cmp(&StacTileIdentity::from(&negative_zero)),
            std::cmp::Ordering::Equal
        );

        let mut different_band = identity_tile();
        different_band.band = 1;
        let mut different_channel = identity_tile();
        different_channel.params.rasterband_channel = 2;
        let mut different_transform = identity_tile();
        different_transform.params.geo_transform.x_pixel_size = 2.;
        let mut different_bounds = identity_tile();
        different_bounds.spatial_partition =
            SpatialPartition2D::new((1., 1.).into(), (2., 0.).into()).unwrap();
        let variants = [
            different_band.clone(),
            different_channel,
            different_transform,
            different_bounds,
        ];
        assert!(variants.iter().all(|variant| {
            StacTileIdentity::from(&positive_zero) != StacTileIdentity::from(variant)
        }));

        let mut files = vec![positive_zero.clone(), negative_zero, different_band];
        files.sort_by(|left, right| {
            StacTileIdentity::from(left).cmp(&StacTileIdentity::from(right))
        });
        files.dedup_by(|left, right| {
            StacTileIdentity::from(&*left) == StacTileIdentity::from(&*right)
        });
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].band, 0);
        assert_eq!(files[1].band, 1);
    }

    #[test]
    fn tile_identity_orders_time_then_band_then_z_index() {
        let mut later_time = identity_tile();
        later_time.time = TimeInterval::new_unchecked(10_i64, 20_i64);
        let mut higher_band = identity_tile();
        higher_band.band = 1;
        let mut higher_z = identity_tile();
        higher_z.z_index = 1;

        let ordered = [later_time, higher_band, higher_z, identity_tile()];
        let mut sorted = ordered.clone();
        sorted.sort_by(|left, right| {
            StacTileIdentity::from(left).cmp(&StacTileIdentity::from(right))
        });
        assert_eq!(sorted[0].time.start().inner(), 0);
        assert_eq!(sorted[0].band, 0);
        assert_eq!(sorted[0].z_index, 0);
        assert_eq!(sorted[1].band, 0);
        assert_eq!(sorted[1].z_index, 1);
        assert_eq!(sorted[2].band, 1);
        assert_eq!(sorted[3].time.start().inner(), 10);
    }

    #[test]
    fn instant_query_snaps_to_its_containing_layer_step() {
        let dimension =
            TimeDimension::Regular(RegularTimeDimension::new_with_epoch_origin(TimeStep {
                granularity: TimeGranularity::Days,
                step: 1,
            }));
        let instant = DateTime::new_utc(2026, 1, 4, 12, 0, 0);
        let snapped =
            stac_query_time_interval(TimeInterval::new_unchecked(instant, instant), dimension)
                .unwrap();
        assert_eq!(
            snapped.start(),
            TimeInstance::from(DateTime::new_utc(2026, 1, 4, 0, 0, 0))
        );
        assert_eq!(
            snapped.end(),
            TimeInstance::from(DateTime::new_utc(2026, 1, 5, 0, 0, 0))
        );
    }

    #[tokio::test]
    async fn outside_projection_domain_returns_empty_tiles_without_stac_requests() {
        // No requests are expected: querying outside the domain must not contact STAC.
        let server = Server::run();
        let definition = make_stac_provider_def(DataProviderId::new(), server.url_str("/"));
        let meta = StacMultiBandMetaData {
            api_url: definition.api_url,
            collection_name: definition.collection_name,
            s3_config: None,
            time_dimension: definition.time_dimension,
            dataset: definition.datasets[0].clone(),
            page_limit: definition.page_limit,
            client: StacClient::new(reqwest::Client::new()),
            query_cache: Arc::new(StacQueryCache::new(1024 * 1024, Duration::from_mins(1))),
            grid: StacGrid::default(),
            cache_ttl_secs: definition.cache_ttl_secs,
        };
        let bounds = SpatialPartition2D::new(
            (-500_000., 5_800_000.).into(),
            (-490_000., 5_790_000.).into(),
        )
        .unwrap();
        let start = DateTime::new_utc(2026, 4, 1, 0, 0, 0);
        let middle = DateTime::new_utc(2026, 4, 2, 0, 0, 0);
        let end = DateTime::new_utc(2026, 4, 3, 0, 0, 0);
        let expected_steps = vec![
            TimeInterval::new(start, middle).unwrap(),
            TimeInterval::new(middle, end).unwrap(),
        ];
        let tile = TileInformation::new(
            GridIdx2D::new([0, 0]),
            GridShape::new([1000, 1000]),
            GeoTransform::new(bounds.upper_left(), 10., -10.),
        );

        // Exercise the initial query, its cached result, and an instant query.
        for (interval, expected) in [
            (
                TimeInterval::new(start, end).unwrap(),
                expected_steps.as_slice(),
            ),
            (
                TimeInterval::new(start, end).unwrap(),
                expected_steps.as_slice(),
            ),
            (
                TimeInterval::new_instant(start).unwrap(),
                &expected_steps[..1],
            ),
        ] {
            let info = meta
                .loading_info(MultiBandGdalLoadingInfoQueryRectangle::new(
                    bounds,
                    interval,
                    BandSelection::first(),
                    true,
                ))
                .await
                .unwrap();
            assert_eq!(info.time_steps(), expected);
            for step in info.time_steps() {
                assert!(info.tile_files(*step, tile, 0).is_empty());
            }
        }
    }

    fn stac_items_response() -> serde_json::Value {
        let json_str =
            include_str!("../../../../../test_data/stac_responses/items/code-de-marburg.json");
        serde_json::from_str(json_str).expect("code-de-marburg.json should be valid JSON")
    }

    fn token_response(
        access_token: &str,
        refresh_token: &str,
        expires_in: u64,
        refresh_expires_in: u64,
    ) -> serde_json::Value {
        serde_json::json!({
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_in": expires_in,
            "refresh_expires_in": refresh_expires_in,
            "token_type": "Bearer",
        })
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn authenticated_stac_requests_use_and_refresh_tokens() {
        let server = Server::run();

        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/token"),
                request::headers(contains((
                    "content-type",
                    "application/x-www-form-urlencoded"
                ))),
                request::body(url_decoded(all_of![
                    contains(("grant_type", "password")),
                    contains(("username", "test-user")),
                    contains(("password", "test-password")),
                    contains(("client_id", "my-client-id")),
                ])),
            ])
            .times(1)
            .respond_with(responders::json_encoded(token_response(
                "access-token-1",
                "refresh-token-1",
                1,
                1,
            ))),
        );

        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/token"),
                request::body(url_decoded(all_of![
                    contains(("grant_type", "refresh_token")),
                    contains(("refresh_token", "refresh-token-1")),
                    contains(("client_id", "my-client-id")),
                ])),
            ])
            .times(1)
            .respond_with(responders::json_encoded(token_response(
                "access-token-2",
                "refresh-token-2",
                1,
                1,
            ))),
        );

        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/token"),
                request::body(url_decoded(all_of![
                    contains(("grant_type", "refresh_token")),
                    contains(("refresh_token", "refresh-token-2")),
                    contains(("client_id", "my-client-id")),
                ])),
            ])
            .times(1)
            .respond_with(responders::json_encoded(token_response(
                "access-token-3",
                "refresh-token-3",
                3_600,
                7_200,
            ))),
        );

        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/initial-items"),
                request::headers(contains(("authorization", "Bearer access-token-1"))),
            ])
            .times(1)
            .respond_with(responders::json_encoded(stac_items_response())),
        );

        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/next-items"),
                request::headers(contains(("authorization", "Bearer access-token-1"))),
            ])
            .times(1)
            .respond_with(responders::json_encoded(stac_items_response())),
        );

        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/refreshed-items"),
                request::headers(contains(("authorization", "Bearer access-token-3"))),
            ])
            .times(1)
            .respond_with(responders::json_encoded(stac_items_response())),
        );

        let client = StacClient::new(reqwest::Client::new())
            .with_authentication(
                Some(
                    crate::datasets::external::stac::StacProviderAuthentication {
                        endpoint: server.url_str("/token"),
                        client_id: "my-client-id".to_owned(),
                        username: "test-user".to_owned(),
                        password: "test-password".to_owned(),
                    },
                ),
                &server.url_str("/"),
            )
            .await
            .expect("initial password grant should succeed");

        let initial_query = StacQueryState::FirstPage {
            query_url: Url::parse(&server.url_str("/initial-items")).unwrap(),
            query_params: vec![],
        };
        query_stac_item_collection(&client, &initial_query)
            .await
            .expect("initial authenticated STAC request should succeed");

        let next_page_query = StacQueryState::NextPage {
            next_url: Url::parse(&server.url_str("/next-items")).unwrap(),
        };
        query_stac_item_collection(&client, &next_page_query)
            .await
            .expect("authenticated STAC pagination request should succeed");

        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                if client.authentication.as_ref().unwrap().access_token().await == "access-token-3"
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("both access and refresh tokens should rotate before they expire");

        let refreshed_query = StacQueryState::FirstPage {
            query_url: Url::parse(&server.url_str("/refreshed-items")).unwrap(),
            query_params: vec![],
        };
        query_stac_item_collection(&client, &refreshed_query)
            .await
            .expect("refreshed authenticated STAC request should succeed");
    }

    /// Replicates the steps from `test_ndvi.http` without making real web requests:
    #[crate::ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn ndvi_stac_loading_info(app_ctx: PostgresContext<NoTls>) {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path(
                "GET",
                "/collections/sentinel-2-l2a/items",
            ))
            .times(1..=10)
            .respond_with(
                responders::status_code(200)
                    .append_header("Content-Type", "application/json")
                    .body(serde_json::to_string(&stac_items_response()).unwrap()),
            ),
        );

        let provider_id = DataProviderId::new();

        let admin_session = admin_login(&app_ctx).await;
        let admin_ctx = app_ctx.session_context(admin_session);
        admin_ctx
            .db()
            .add_layer_provider(
                make_stac_provider_def(
                    provider_id,
                    server.url_str("/").trim_end_matches('/').to_owned(),
                )
                .into(),
            )
            .await
            .unwrap();

        let provider = admin_ctx
            .db()
            .load_layer_provider(provider_id)
            .await
            .unwrap();

        let layer_id = geoengine_datatypes::dataset::LayerId("dataset/epsg32632_u16_10".to_owned());
        let data_id: DataId = ExternalDataId {
            provider_id,
            layer_id,
        }
        .into();

        let meta: Box<
            dyn MetaData<
                    MultiBandGdalLoadingInfo,
                    RasterResultDescriptor,
                    MultiBandGdalLoadingInfoQueryRectangle,
                >,
        > = MetaDataProvider::meta_data(provider.as_ref(), &data_id)
            .await
            .expect("meta_data should succeed");

        let spatial_bounds = SpatialPartition2D::new(
            (499_980.0, 5_800_020.0).into(),
            (510_000.0, 5_790_000.0).into(),
        )
        .unwrap();
        let time_interval =
            TimeInterval::new_instant(DateTime::new_utc(2026, 1, 3, 0, 0, 0)).unwrap();

        let query = MultiBandGdalLoadingInfoQueryRectangle::new(
            spatial_bounds,
            time_interval,
            BandSelection::new_unchecked(vec![0, 1]),
            true,
        );

        let loading_info = meta
            .loading_info(query)
            .await
            .expect("loading_info should succeed");

        let expected_time = TimeInterval::new(
            DateTime::new_utc(2026, 1, 3, 0, 0, 0),
            DateTime::new_utc(2026, 1, 4, 0, 0, 0),
        )
        .unwrap();

        let time_steps = loading_info.time_steps();
        assert!(
            time_steps.iter().any(|ts| ts == &expected_time),
            "loading_info should contain time step for 2026-01-03, got {time_steps:?}"
        );

        let tile_geo_transform = GeoTransform::new((499_980.0, 5_800_020.0).into(), 10.0, -10.0);
        let tile = TileInformation::new(
            GridIdx2D::new([0, 0]),
            GridShape::new([10980, 10980]),
            tile_geo_transform,
        );

        if let Some(time_step) = time_steps.first() {
            let b08_params = loading_info.tile_files(*time_step, tile, 0);
            let b04_params = loading_info.tile_files(*time_step, tile, 1);

            assert!(
                !b08_params.is_empty(),
                "Should have B08 (NIR) band files for {time_step}"
            );
            assert!(
                !b04_params.is_empty(),
                "Should have B04 (Red) band files for {time_step}"
            );

            for param in &b08_params {
                assert!(
                    param.file_path.to_string_lossy().starts_with("s3://"),
                    "B08 file should use S3 URL: {:?}",
                    param.file_path
                );
            }
            for param in &b04_params {
                assert!(
                    param.file_path.to_string_lossy().starts_with("s3://"),
                    "B04 file should use S3 URL: {:?}",
                    param.file_path
                );
            }
        }
    }

    #[crate::ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn ndvi_stac_workflow(app_ctx: PostgresContext<NoTls>) {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path(
                "GET",
                "/collections/sentinel-2-l2a/items",
            ))
            .times(0..=20)
            .respond_with(
                responders::status_code(200)
                    .append_header("Content-Type", "application/json")
                    .body(serde_json::to_string(&stac_items_response()).unwrap()),
            ),
        );

        let provider_id = DataProviderId::new();

        let admin_session = admin_login(&app_ctx).await;
        let admin_ctx = app_ctx.session_context(admin_session);

        let provider_def = crate::datasets::external::stac::StacDataProviderDefinition {
            name: "Sentinel 2 L2A from STAC".to_owned(),
            id: provider_id,
            description: "Test STAC provider for NDVI workflow".to_owned(),
            priority: Some(50),
            api_url: server.url_str("/"),
            collection_name: "sentinel-2-l2a".to_owned(),
            s3_config: None,
            authentication: None,
            time_dimension: TimeDimension::Regular(RegularTimeDimension::new_with_epoch_origin(
                TimeStep {
                    granularity: TimeGranularity::Days,
                    step: 1,
                },
            )),
            datasets: vec![
                crate::datasets::external::stac::StacProviderDataset {
                    name: "Sentinel-2 L2A EPSG:32632 U16 10m".to_owned(),
                    description: String::new(),
                    data_type: geoengine_datatypes::raster::RasterDataType::U16,
                    resolution: SpatialResolution::new_unchecked(10.0, 10.0),
                    projection: SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        GeoTransform::new((399_960.0, 5_700_000.0).into(), 10.0, -10.0),
                        GridBoundingBox2D::new(
                            GridIdx2D::new([0, 0]),
                            GridIdx2D::new([10979, 10979]),
                        )
                        .unwrap(),
                    ),
                    bands: vec![
                        crate::datasets::external::stac::StacProviderDatasetBand {
                            asset_band: crate::datasets::external::stac::StacAssetBand {
                                asset_title: "Blue (band 2) - 10m".to_owned(),
                                band_name: Some("B02".to_owned()),
                            },
                            band_descriptor: RasterBandDescriptor::new_unitless("B02".to_owned()),
                        },
                        crate::datasets::external::stac::StacProviderDatasetBand {
                            asset_band: crate::datasets::external::stac::StacAssetBand {
                                asset_title: "Green (band 3) - 10m".to_owned(),
                                band_name: Some("B03".to_owned()),
                            },
                            band_descriptor: RasterBandDescriptor::new_unitless("B03".to_owned()),
                        },
                        crate::datasets::external::stac::StacProviderDatasetBand {
                            asset_band: crate::datasets::external::stac::StacAssetBand {
                                asset_title: "Water vapour (WVP) - 10m".to_owned(),
                                band_name: Some("WVP".to_owned()),
                            },
                            band_descriptor: RasterBandDescriptor::new_unitless("WVP".to_owned()),
                        },
                        crate::datasets::external::stac::StacProviderDatasetBand {
                            asset_band: crate::datasets::external::stac::StacAssetBand {
                                asset_title: "NIR 1 (band 8) - 10m".to_owned(),
                                band_name: Some("B08".to_owned()),
                            },
                            band_descriptor: RasterBandDescriptor::new_unitless("B08".to_owned()),
                        },
                        crate::datasets::external::stac::StacProviderDatasetBand {
                            asset_band: crate::datasets::external::stac::StacAssetBand {
                                asset_title: "Red (band 4) - 10m".to_owned(),
                                band_name: Some("B04".to_owned()),
                            },
                            band_descriptor: RasterBandDescriptor::new_unitless("B04".to_owned()),
                        },
                    ],
                },
                crate::datasets::external::stac::StacProviderDataset {
                    name: "Sentinel-2 L2A EPSG:32632 U8 20m".to_owned(),
                    description: String::new(),
                    data_type: geoengine_datatypes::raster::RasterDataType::U8,
                    resolution: SpatialResolution::new_unchecked(20.0, 20.0),
                    projection: SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        GeoTransform::new((399_960.0, 5_700_000.0).into(), 20.0, -20.0),
                        GridBoundingBox2D::new(
                            GridIdx2D::new([0, 0]),
                            GridIdx2D::new([5489, 5489]),
                        )
                        .unwrap(),
                    ),
                    bands: vec![
                        crate::datasets::external::stac::StacProviderDatasetBand {
                            asset_band: crate::datasets::external::stac::StacAssetBand {
                                asset_title: "Aerosol optical thickness (AOT) - 20m".to_owned(),
                                band_name: Some("AOT".to_owned()),
                            },
                            band_descriptor: RasterBandDescriptor::new_unitless("AOT".to_owned()),
                        },
                        crate::datasets::external::stac::StacProviderDatasetBand {
                            asset_band: crate::datasets::external::stac::StacAssetBand {
                                asset_title: "Scene classification map (SCL) - 20m".to_owned(),
                                band_name: Some("SCL".to_owned()),
                            },
                            band_descriptor: RasterBandDescriptor::new_unitless("SCL".to_owned()),
                        },
                    ],
                },
            ],
            page_limit: 100,
            query_timeout_secs: 60,
            cache_ttl_secs: None,
            stac_grid: None,
        };

        admin_ctx
            .db()
            .add_layer_provider(provider_def.into())
            .await
            .unwrap();

        let ndvi_workflow_json =
            include_str!("../../../../../test_data/api_calls/stac_provider/ndvi-workflow.json");

        let workflow_json_with_provider = ndvi_workflow_json.replace(
            "_:b274275c-373d-4a3f-8b45-9b48e9614329",
            &format!("_:{provider_id}"),
        );

        let workflow: crate::workflows::workflow::Workflow =
            serde_json::from_str(&workflow_json_with_provider)
                .expect("workflow JSON should deserialize");

        let operator = workflow
            .operator
            .get_raster()
            .expect("workflow operator should be raster");

        let execution_ctx = admin_ctx.execution_context().expect("execution context");

        let initialized = operator
            .clone()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_ctx)
            .await
            .expect("operator should initialize");

        // Verify the operator initialized successfully - this tests that the workflow
        // can be created and initialized with the STAC provider data
        let _result_descriptor = initialized.result_descriptor();
        // If we get here, the operator initialized successfully
    }

    /// Test that a discover-generated mapping JSON can be used directly as a
    /// `StacDataProvider`, validating the mapping format works for both the
    /// harvester and the runtime provider.
    #[crate::ge_context::test]
    async fn mapping_from_discover_works_as_stacdataprovider(app_ctx: PostgresContext<NoTls>) {
        // Load the discover-generated mapping JSON via the API layer (camelCase)
        let api_def: crate::api::model::services::StacDataProviderDefinition =
            serde_json::from_str(include_str!(
                "../../../../../test_data/stac_responses/expected-mapping-code-de.json"
            ))
            .expect("valid discover mapping fixture");

        let mut provider_def: crate::datasets::external::stac::StacDataProviderDefinition =
            api_def.into();

        // Use a placeholder URL (no actual HTTP calls needed for meta_data registration)
        provider_def.api_url = "https://stac.test/v1".to_owned();
        provider_def.id = DataProviderId::new();

        let admin_session = admin_login(&app_ctx).await;
        let admin_ctx = app_ctx.session_context(admin_session);

        admin_ctx
            .db()
            .add_layer_provider(provider_def.clone().into())
            .await
            .unwrap();

        let provider = admin_ctx
            .db()
            .load_layer_provider(provider_def.id)
            .await
            .unwrap();

        // Verify each dataset from the discover-generated mapping can be
        // resolved via meta_data (no HTTP calls needed at this stage)
        for dataset in &provider_def.datasets {
            let epsg_code = dataset.projection.code();
            let data_type_str = format!("{:?}", dataset.data_type).to_lowercase();
            let resolution = dataset.resolution.x as u32;
            let stable_id = format!("epsg{epsg_code}_{data_type_str}_{resolution}");

            let layer_id = geoengine_datatypes::dataset::LayerId(format!("dataset/{stable_id}"));
            let data_id: DataId = ExternalDataId {
                provider_id: provider_def.id,
                layer_id,
            }
            .into();

            let meta_result: Result<
                Box<
                    dyn MetaData<
                            MultiBandGdalLoadingInfo,
                            RasterResultDescriptor,
                            MultiBandGdalLoadingInfoQueryRectangle,
                        >,
                >,
                geoengine_operators::error::Error,
            > = MetaDataProvider::meta_data(provider.as_ref(), &data_id).await;

            assert!(
                meta_result.is_ok(),
                "meta_data should succeed for dataset '{}' (stable_id: {})",
                dataset.name,
                stable_id
            );
        }
    }
}

#[cfg(test)]
mod grid_request_tests {
    use super::super::{
        StacAssetBand, StacDataProvider, StacDataProviderDefinition, StacGrid, StacProviderDataset,
        StacProviderDatasetBand, cache::StacQueryCache,
    };
    use super::stac_query_bbox;
    use geoengine_datatypes::{
        dataset::{DataId, DataProviderId, ExternalDataId, LayerId},
        primitives::{
            AxisAlignedRectangle, BandSelection, DateTime, RegularTimeDimension,
            SpatialPartition2D, SpatialResolution, TimeDimension, TimeGranularity, TimeInterval,
            TimeStep,
        },
        raster::{
            GeoTransform, GridBoundingBox2D, GridIdx2D, GridShape, RasterDataType, TileInformation,
        },
        spatial_reference::SpatialReference,
        util::Identifier,
    };
    use geoengine_operators::{
        engine::{MetaData, MetaDataProvider, RasterResultDescriptor, SpatialGridDescriptor},
        source::{MultiBandGdalLoadingInfo, MultiBandGdalLoadingInfoQueryRectangle},
    };
    use httptest::{
        Expectation, Server, all_of,
        matchers::{contains, request, url_decoded},
        responders,
    };
    use serde_json::{Value, json};
    use std::{
        path::PathBuf,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    type LoadingMetadata = Box<
        dyn MetaData<
                MultiBandGdalLoadingInfo,
                RasterResultDescriptor,
                MultiBandGdalLoadingInfoQueryRectangle,
            >,
    >;

    fn bounds(left: f64, right: f64) -> SpatialPartition2D {
        SpatialPartition2D::new((left, 1.).into(), (right, 0.).into()).unwrap()
    }

    fn interval(day: u8) -> TimeInterval {
        TimeInterval::new(
            DateTime::new_utc(2026, 1, day, 0, 0, 0),
            DateTime::new_utc(2026, 1, day + 1, 0, 0, 0),
        )
        .unwrap()
    }

    fn query(
        bounds: SpatialPartition2D,
        time: TimeInterval,
    ) -> MultiBandGdalLoadingInfoQueryRectangle {
        MultiBandGdalLoadingInfoQueryRectangle::new(bounds, time, BandSelection::first(), true)
    }

    fn item(id: &str, left: f64) -> Value {
        json!({
            "type": "Feature", "stac_version": "1.1.0", "stac_extensions": [], "id": id,
            "geometry": {"type": "Polygon", "coordinates": [[[left, 0.], [left + 1., 0.],
                [left + 1., 1.], [left, 1.], [left, 0.]]]},
            "bbox": [left, 0., left + 1., 1.],
            "properties": {"datetime": "2026-01-03T12:00:00Z"}, "links": [],
            "assets": {"data": {"href": format!("https://assets.example/{id}.tif"),
                "title": "data", "data_type": "uint16", "proj:code": "EPSG:4326",
                "proj:shape": [10, 10], "proj:transform": [0.1, 0., left, 0., -0.1, 1.]}}
        })
    }

    fn collection(features: &[Value], next: Option<String>) -> Value {
        let links = next.map_or_else(Vec::new, |href| vec![json!({"rel": "next", "href": href})]);
        json!({"type": "FeatureCollection", "features": features, "links": links})
    }

    fn provider(server: &Server, cache_bytes: usize, grid: StacGrid) -> StacDataProvider {
        let dataset = StacProviderDataset {
            name: "test".to_owned(),
            description: String::new(),
            data_type: RasterDataType::U16,
            resolution: SpatialResolution::new_unchecked(0.1, 0.1),
            projection: SpatialReference::epsg_4326(),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new((0., 1.).into(), 0.1, -0.1),
                GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([9, 19])).unwrap(),
            ),
            bands: vec![StacProviderDatasetBand::new_unitless(StacAssetBand {
                asset_title: "data".to_owned(),
                band_name: None,
            })],
        };
        let mut provider = StacDataProvider::from_definition(StacDataProviderDefinition {
            id: DataProviderId::new(),
            name: "test".to_owned(),
            description: String::new(),
            api_url: server.url_str("/"),
            collection_name: "test".to_owned(),
            s3_config: None,
            authentication: None,
            time_dimension: TimeDimension::Regular(RegularTimeDimension::new_with_epoch_origin(
                TimeStep {
                    granularity: TimeGranularity::Days,
                    step: 1,
                },
            )),
            datasets: vec![dataset],
            priority: None,
            page_limit: 100,
            query_timeout_secs: 5,
            cache_ttl_secs: None,
            stac_grid: Some(grid),
        })
        .unwrap();
        provider.query_cache = Arc::new(StacQueryCache::new(cache_bytes, Duration::from_mins(1)));
        provider
    }

    async fn metadata(provider: &StacDataProvider, index: usize) -> LoadingMetadata {
        provider
            .meta_data(&DataId::External(ExternalDataId {
                provider_id: provider.id,
                layer_id: LayerId(format!("dataset/{index}")),
            }))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn time_only_queries_use_provider_steps_without_stac_requests() {
        // No requests are expected, even when the query covers the entire world.
        let server = Server::run();
        let mut provider = provider(
            &server,
            0,
            StacGrid {
                target_number_of_cells: 64_800,
            },
        );
        let daily = TimeDimension::Regular(RegularTimeDimension::new(
            DateTime::new_utc(2026, 1, 1, 6, 0, 0).into(),
            TimeStep {
                granularity: TimeGranularity::Days,
                step: 2,
            },
        ));
        let at = |day, hour| DateTime::new_utc(2026, 1, day, hour, 0, 0);
        let daily_cases = [
            (
                TimeInterval::new(at(4, 8), at(7, 6)).unwrap(),
                vec![
                    TimeInterval::new(at(3, 6), at(5, 6)).unwrap(),
                    TimeInterval::new(at(5, 6), at(7, 6)).unwrap(),
                ],
            ),
            (
                TimeInterval::new(at(5, 6), at(5, 6)).unwrap(),
                vec![TimeInterval::new(at(5, 6), at(7, 6)).unwrap()],
            ),
            (
                TimeInterval::new(at(5, 8), at(5, 8)).unwrap(),
                vec![TimeInterval::new(at(5, 6), at(7, 6)).unwrap()],
            ),
        ];
        provider.time_dimension = daily;
        let world = SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap();
        for (time, expected) in daily_cases {
            let info = tokio::time::timeout(
                Duration::from_secs(2),
                metadata(&provider, 0).await.loading_info(
                    MultiBandGdalLoadingInfoQueryRectangle::new(
                        world,
                        time,
                        BandSelection::first(),
                        false,
                    ),
                ),
            )
            .await
            .expect("time-only queries must not perform STAC searches")
            .unwrap();
            assert_eq!(info.time_steps(), expected);
            assert!(info.tile_files(expected[0], tile(0.), 0).is_empty());
        }

        // Time-only queries also bypass URL parsing and reprojection entirely.
        provider.api_url = "invalid STAC URL".to_owned();
        provider.datasets[0].projection = SpatialReference::new(
            geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
            0,
        );
        provider.time_dimension =
            TimeDimension::Regular(RegularTimeDimension::new_with_epoch_origin(TimeStep {
                granularity: TimeGranularity::Months,
                step: 1,
            }));
        let february = DateTime::new_utc(2024, 2, 1, 0, 0, 0);
        let march = DateTime::new_utc(2024, 3, 1, 0, 0, 0);
        let april = DateTime::new_utc(2024, 4, 1, 0, 0, 0);
        let info = metadata(&provider, 0)
            .await
            .loading_info(MultiBandGdalLoadingInfoQueryRectangle::new(
                world,
                TimeInterval::new(DateTime::new_utc(2024, 2, 15, 12, 0, 0), april).unwrap(),
                BandSelection::first(),
                false,
            ))
            .await
            .unwrap();
        assert_eq!(
            info.time_steps(),
            &[
                TimeInterval::new(february, march).unwrap(),
                TimeInterval::new(march, april).unwrap(),
            ]
        );
    }

    #[tokio::test]
    async fn time_only_queries_reject_unsupported_time_dimensions_without_stac_requests() {
        let server = Server::run();
        let mut provider = provider(
            &server,
            0,
            StacGrid {
                target_number_of_cells: 64_800,
            },
        );
        for dimension in [
            TimeDimension::Irregular,
            TimeDimension::Regular(RegularTimeDimension::new_with_epoch_origin(TimeStep {
                granularity: TimeGranularity::Days,
                step: 0,
            })),
        ] {
            provider.time_dimension = dimension;
            let result = metadata(&provider, 0)
                .await
                .loading_info(MultiBandGdalLoadingInfoQueryRectangle::new(
                    bounds(0., 1.),
                    interval(3),
                    BandSelection::first(),
                    false,
                ))
                .await;
            assert!(matches!(
                result,
                Err(geoengine_operators::error::Error::InvalidDataProviderConfig { .. })
            ));
        }
    }

    fn tile(left: f64) -> TileInformation {
        TileInformation::new(
            GridIdx2D::new([0, 0]),
            GridShape::new([10, 10]),
            GeoTransform::new((left, 1.).into(), 0.1, -0.1),
        )
    }

    fn assert_file(info: &MultiBandGdalLoadingInfo, left: f64, expected_id: &str) {
        assert_eq!(info.time_steps(), &[interval(3)]);
        let files = info.tile_files(interval(3), tile(left), 0);
        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0].file_path,
            PathBuf::from(format!("https://assets.example/{expected_id}.tif"))
        );
    }

    type SearchStarts = Arc<Mutex<Vec<Instant>>>;

    fn expect_search(
        server: &Server,
        bbox: &str,
        features: Vec<Value>,
        delay: Duration,
        starts: SearchStarts,
    ) {
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/collections/test/items"),
                request::query(url_decoded(contains(("bbox", bbox.to_owned())))),
            ])
            .times(1)
            .respond_with(move || {
                starts.lock().unwrap().push(Instant::now());
                responders::delay_and_then(
                    delay,
                    responders::json_encoded(collection(&features, None)),
                )
            }),
        );
    }

    async fn wait_for_starts(starts: &SearchStarts, count: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if starts.lock().unwrap().len() >= count {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("new STAC cells should start without a collection timer");
    }

    #[tokio::test]
    async fn same_cell_shares_active_search_even_when_cache_cannot_admit_it() {
        for cache_bytes in [0, 1, 1024 * 1024] {
            let server = Server::run();
            let starts = Arc::new(Mutex::new(Vec::new()));
            expect_search(
                &server,
                "0.00000,0.00000,1.00000,1.00000",
                vec![item("shared", 0.)],
                Duration::from_millis(150),
                starts.clone(),
            );
            let provider = provider(
                &server,
                cache_bytes,
                StacGrid {
                    target_number_of_cells: 64_800,
                },
            );
            let meta = metadata(&provider, 0).await;
            let first = tokio::spawn(async move {
                meta.loading_info(query(bounds(0.1, 0.4), interval(3)))
                    .await
            });
            wait_for_starts(&starts, 1).await;
            let meta = metadata(&provider, 0).await;
            let (neighbor, other) = tokio::join!(
                meta.loading_info(query(bounds(0.6, 0.9), interval(3))),
                meta.loading_info(query(bounds(0.4, 0.6), interval(3))),
            );
            assert_file(&first.await.unwrap().unwrap(), 0., "shared");
            assert_file(&neighbor.unwrap(), 0., "shared");
            assert_file(&other.unwrap(), 0., "shared");
            if cache_bytes > 1 {
                assert_file(
                    &meta
                        .loading_info(query(bounds(0., 1.), interval(3)))
                        .await
                        .unwrap(),
                    0.,
                    "shared",
                );
            }
        }
    }

    fn item_at(id: &str, left: f64, bottom: f64) -> Value {
        let mut result = item(id, left);
        result["geometry"]["coordinates"] = json!([[
            [left, bottom],
            [left + 1., bottom],
            [left + 1., bottom + 1.],
            [left, bottom + 1.],
            [left, bottom]
        ]]);
        result["bbox"] = json!([left, bottom, left + 1., bottom + 1.]);
        result["assets"]["data"]["proj:transform"][5] = json!(bottom + 1.);
        result
    }

    #[tokio::test]
    async fn tile_spanning_four_cells_gets_all_assets_and_deduplicates_shared_assets() {
        use std::collections::HashSet;
        let server = Server::run();
        let starts = Arc::new(Mutex::new(Vec::new()));
        let mut shared = item("across", 0.);
        shared["geometry"]["coordinates"] =
            json!([[[0., 0.], [2., 0.], [2., 2.], [0., 2.], [0., 0.]]]);
        shared["bbox"] = json!([0., 0., 2., 2.]);
        shared["assets"]["data"]["proj:shape"] = json!([20, 20]);
        shared["assets"]["data"]["proj:transform"][5] = json!(2.);
        for (bbox, id, left, bottom) in [
            ("0.00000,0.00000,1.00000,1.00000", "southwest", 0., 0.),
            ("1.00000,0.00000,2.00000,1.00000", "southeast", 1., 0.),
            ("0.00000,1.00000,1.00000,2.00000", "northwest", 0., 1.),
            ("1.00000,1.00000,2.00000,2.00000", "northeast", 1., 1.),
        ] {
            expect_search(
                &server,
                bbox,
                vec![item_at(id, left, bottom), shared.clone()],
                Duration::ZERO,
                starts.clone(),
            );
        }
        let provider = provider(
            &server,
            1024 * 1024,
            StacGrid {
                target_number_of_cells: 64_800,
            },
        );
        let meta = metadata(&provider, 0).await;
        let area = SpatialPartition2D::new((0.5, 1.5).into(), (1.5, 0.5).into()).unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            meta.loading_info(query(area, interval(3))),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.time_steps(), &[interval(3)]);
        let tile = TileInformation::new(
            GridIdx2D::new([0, 0]),
            GridShape::new([10, 10]),
            GeoTransform::new((0.5, 1.5).into(), 0.1, -0.1),
        );
        let files = result.tile_files(interval(3), tile, 0);
        assert_eq!(files.len(), 5);
        assert_eq!(
            files
                .iter()
                .map(|file| &file.file_path)
                .collect::<HashSet<_>>()
                .len(),
            5
        );
        let warm = meta.loading_info(query(area, interval(3))).await.unwrap();
        assert_eq!(warm.tile_files(interval(3), tile, 0).len(), 5);
    }

    #[tokio::test]
    async fn different_cells_start_independently() {
        let server = Server::run();
        let starts = Arc::new(Mutex::new(Vec::new()));
        expect_search(
            &server,
            "0.00000,0.00000,1.00000,1.00000",
            vec![item("first", 0.)],
            Duration::from_millis(600),
            starts.clone(),
        );
        expect_search(
            &server,
            "2.00000,0.00000,3.00000,1.00000",
            vec![item("other", 2.)],
            Duration::ZERO,
            starts.clone(),
        );
        let provider = provider(
            &server,
            0,
            StacGrid {
                target_number_of_cells: 64_800,
            },
        );
        let meta = metadata(&provider, 0).await;
        let first = tokio::spawn(async move {
            meta.loading_info(query(bounds(0.1, 0.4), interval(3)))
                .await
        });
        wait_for_starts(&starts, 1).await;
        let meta = metadata(&provider, 0).await;
        let other = tokio::time::timeout(
            Duration::from_millis(400),
            meta.loading_info(query(bounds(2.1, 2.4), interval(3))),
        )
        .await
        .unwrap()
        .unwrap();
        assert_file(&other, 2., "other");
        assert_file(&first.await.unwrap().unwrap(), 0., "first");
        assert_eq!(starts.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn cancelled_caller_does_not_cancel_a_neighbors_cell_search() {
        let server = Server::run();
        let starts = Arc::new(Mutex::new(Vec::new()));
        expect_search(
            &server,
            "0.00000,0.00000,1.00000,1.00000",
            vec![item("neighbor", 0.)],
            Duration::from_millis(150),
            starts.clone(),
        );
        let provider = provider(
            &server,
            0,
            StacGrid {
                target_number_of_cells: 64_800,
            },
        );
        let meta = metadata(&provider, 0).await;
        let first = tokio::spawn(async move {
            meta.loading_info(query(bounds(0.1, 0.4), interval(3)))
                .await
        });
        wait_for_starts(&starts, 1).await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let neighbor = metadata(&provider, 0)
            .await
            .loading_info(query(bounds(0.6, 0.9), interval(3)))
            .await
            .unwrap();
        assert_file(&neighbor, 0., "neighbor");
    }

    #[tokio::test]
    async fn pagination_finishes_before_a_cell_is_cached() {
        let server = Server::run();
        let next = server.url_str("/next");
        server.expect(
            Expectation::matching(request::method_path("GET", "/collections/test/items"))
                .times(1)
                .respond_with(responders::json_encoded(collection(
                    &[item("a", 0.)],
                    Some(next),
                ))),
        );
        server.expect(
            Expectation::matching(request::method_path("GET", "/next"))
                .times(1)
                .respond_with(responders::json_encoded(collection(
                    &[item("b", 0.5)],
                    None,
                ))),
        );
        let provider = provider(
            &server,
            1024 * 1024,
            StacGrid {
                target_number_of_cells: 64_800,
            },
        );
        let meta = metadata(&provider, 0).await;
        let result = meta
            .loading_info(query(bounds(0.1, 0.9), interval(3)))
            .await
            .unwrap();
        assert_eq!(result.tile_files(interval(3), tile(0.), 0).len(), 2);
        let cached = meta
            .loading_info(query(bounds(0.6, 0.9), interval(3)))
            .await
            .unwrap();
        assert_eq!(cached.tile_files(interval(3), tile(0.), 0).len(), 2);
    }

    #[tokio::test]
    async fn provider_grid_counts_change_the_complete_search_cell() {
        let server = Server::run();
        let starts = Arc::new(Mutex::new(Vec::new()));
        expect_search(
            &server,
            "0.00000,0.00000,90.00000,90.00000",
            vec![item("coarse", 0.)],
            Duration::ZERO,
            starts,
        );
        let provider = provider(
            &server,
            0,
            StacGrid {
                target_number_of_cells: 8,
            },
        );
        let result = metadata(&provider, 0)
            .await
            .loading_info(query(bounds(0.1, 0.4), interval(3)))
            .await
            .unwrap();
        assert_file(&result, 0., "coarse");
    }

    #[tokio::test]
    async fn projected_native_tile_selects_complete_wgs84_grid_cells() {
        let server = Server::run();
        let starts = Arc::new(Mutex::new(Vec::new()));
        let mut asset = item("projected", 0.);
        asset["assets"]["data"]["proj:code"] = json!("EPSG:32632");
        asset["assets"]["data"]["proj:transform"] =
            json!([1000., 0., 500_000., 0., -1000., 5_800_000.]);
        let mut provider = provider(&server, 1024 * 1024, StacGrid::default());
        provider.datasets[0].projection = SpatialReference::new(
            geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
            32632,
        );
        provider.datasets[0].resolution = SpatialResolution::new_unchecked(1000., 1000.);
        provider.datasets[0].spatial_grid = SpatialGridDescriptor::source_from_parts(
            GeoTransform::new((500_000., 5_800_000.).into(), 1000., -1000.),
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([9, 19])).unwrap(),
        );
        let projection = provider.datasets[0].projection;
        let projected_extent = projection
            .area_of_use_projected::<geoengine_datatypes::primitives::BoundingBox2D>()
            .unwrap();
        let grid = StacGrid::default().for_extent(projected_extent).unwrap();
        let area =
            SpatialPartition2D::new((500_100., 5_799_900.).into(), (500_900., 5_799_100.).into())
                .unwrap();
        let cell = grid.cells_for_bbox(area.as_bbox()).unwrap().next().unwrap();
        let clipped = grid.clip_cell_to_extent(cell.bbox).unwrap();
        let expected_bbox = stac_query_bbox(clipped, projection).unwrap().unwrap();
        expect_search(
            &server,
            &crate::util::format_stac_wgs84_bbox(expected_bbox),
            vec![asset],
            Duration::ZERO,
            starts.clone(),
        );
        let first_area = area;
        let adjacent_area =
            SpatialPartition2D::new((500_900., 5_799_900.).into(), (501_700., 5_799_100.).into())
                .unwrap();
        assert_eq!(
            grid.cells_for_bbox(first_area.as_bbox())
                .unwrap()
                .next()
                .unwrap()
                .index,
            cell.index
        );
        assert_eq!(
            grid.cells_for_bbox(adjacent_area.as_bbox())
                .unwrap()
                .next()
                .unwrap()
                .index,
            cell.index
        );
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            metadata(&provider, 0)
                .await
                .loading_info(query(first_area, interval(3))),
        )
        .await
        .unwrap()
        .unwrap();
        let tile = TileInformation::new(
            GridIdx2D::new([0, 0]),
            GridShape::new([10, 10]),
            GeoTransform::new((500_000., 5_800_000.).into(), 1000., -1000.),
        );
        assert_eq!(result.tile_files(interval(3), tile, 0).len(), 1);
        metadata(&provider, 0)
            .await
            .loading_info(query(adjacent_area, interval(3)))
            .await
            .unwrap();
        assert_eq!(starts.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn layer_time_steps_share_keys_for_overlapping_non_aligned_queries() {
        let server = Server::run();
        let starts = Arc::new(Mutex::new(Vec::new()));
        for day in [3, 4] {
            let mut asset = item(&format!("day{day}"), 0.);
            asset["properties"]["datetime"] = json!(format!("2026-01-{day:02}T12:00:00Z"));
            let response_starts = starts.clone();
            let datetime = format!(
                "2026-01-{day:02}T00:00:00.000Z/2026-01-{:02}T00:00:00.000Z",
                day + 1
            );
            server.expect(
                Expectation::matching(all_of![
                    request::method_path("GET", "/collections/test/items"),
                    request::query(url_decoded(contains((
                        "bbox",
                        "0.00000,0.00000,1.00000,1.00000"
                    )))),
                    request::query(url_decoded(contains(("datetime", datetime)))),
                ])
                .times(1)
                .respond_with(move || {
                    response_starts.lock().unwrap().push(Instant::now());
                    responders::delay_and_then(
                        Duration::from_millis(150),
                        responders::json_encoded(collection(&[asset.clone()], None)),
                    )
                }),
            );
        }
        let provider = provider(
            &server,
            1024 * 1024,
            StacGrid {
                target_number_of_cells: 64_800,
            },
        );
        let meta = metadata(&provider, 0).await;
        let wide_time = TimeInterval::new(
            DateTime::new_utc(2026, 1, 3, 10, 0, 0),
            DateTime::new_utc(2026, 1, 5, 0, 0, 0),
        )
        .unwrap();
        let wide =
            tokio::spawn(
                async move { meta.loading_info(query(bounds(0.1, 0.4), wide_time)).await },
            );
        wait_for_starts(&starts, 1).await;
        let narrow_time = TimeInterval::new(
            DateTime::new_utc(2026, 1, 4, 3, 0, 0),
            DateTime::new_utc(2026, 1, 4, 5, 0, 0),
        )
        .unwrap();
        let narrow = metadata(&provider, 0)
            .await
            .loading_info(query(bounds(0.6, 0.9), narrow_time))
            .await
            .unwrap();
        assert_eq!(narrow.time_steps(), &[interval(4)]);
        let wide = wide.await.unwrap().unwrap();
        assert_eq!(wide.time_steps(), &[interval(3), interval(4)]);
        assert_eq!(wide.tile_files(interval(3), tile(0.), 0).len(), 1);
        assert_eq!(wide.tile_files(interval(4), tile(0.), 0).len(), 1);
        assert_eq!(starts.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn terminal_cell_failure_is_shared_and_a_later_caller_retries() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let server = Server::run();
        let starts = Arc::new(Mutex::new(Vec::new()));
        let response_starts = starts.clone();
        let attempts = Arc::new(AtomicUsize::new(0));
        server.expect(
            Expectation::matching(request::method_path("GET", "/collections/test/items"))
                .times(2)
                .respond_with(move || {
                    response_starts.lock().unwrap().push(Instant::now());
                    let first = attempts.fetch_add(1, Ordering::SeqCst) == 0;
                    responders::delay_and_then(
                        if first {
                            Duration::from_millis(150)
                        } else {
                            Duration::ZERO
                        },
                        responders::status_code(if first { 400 } else { 200 })
                            .append_header("Content-Type", "application/json")
                            .body(
                                serde_json::to_string(&collection(&[item("retry", 0.)], None))
                                    .unwrap(),
                            ),
                    )
                }),
        );
        let provider = provider(
            &server,
            1024 * 1024,
            StacGrid {
                target_number_of_cells: 64_800,
            },
        );
        let meta = metadata(&provider, 0).await;
        let first = tokio::spawn(async move {
            meta.loading_info(query(bounds(0.1, 0.4), interval(3)))
                .await
        });
        wait_for_starts(&starts, 1).await;
        let meta = metadata(&provider, 0).await;
        assert!(
            meta.loading_info(query(bounds(0.6, 0.9), interval(3)))
                .await
                .is_err()
        );
        assert!(first.await.unwrap().is_err());
        let retry = meta
            .loading_info(query(bounds(0.1, 0.4), interval(3)))
            .await
            .unwrap();
        assert_file(&retry, 0., "retry");
    }

    #[tokio::test]
    async fn rate_limited_cell_uses_existing_retries_shared_by_waiters() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let server = Server::run();
        let starts = Arc::new(Mutex::new(Vec::new()));
        let response_starts = starts.clone();
        let attempts = Arc::new(AtomicUsize::new(0));
        server.expect(
            Expectation::matching(request::method_path("GET", "/collections/test/items"))
                .times(2)
                .respond_with(move || {
                    response_starts.lock().unwrap().push(Instant::now());
                    let first = attempts.fetch_add(1, Ordering::SeqCst) == 0;
                    responders::status_code(if first { 429 } else { 200 })
                        .append_header("Content-Type", "application/json")
                        .body(
                            serde_json::to_string(&collection(&[item("retried", 0.)], None))
                                .unwrap(),
                        )
                }),
        );
        let provider = provider(
            &server,
            0,
            StacGrid {
                target_number_of_cells: 64_800,
            },
        );
        let meta = metadata(&provider, 0).await;
        let first = tokio::spawn(async move {
            meta.loading_info(query(bounds(0.1, 0.4), interval(3)))
                .await
        });
        wait_for_starts(&starts, 1).await;
        let neighbor = metadata(&provider, 0)
            .await
            .loading_info(query(bounds(0.6, 0.9), interval(3)))
            .await
            .unwrap();
        assert_file(&neighbor, 0., "retried");
        assert_file(&first.await.unwrap().unwrap(), 0., "retried");
    }

    #[tokio::test]
    async fn failed_next_page_does_not_publish_or_cache_partial_cell_results() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let server = Server::run();
        let next = server.url_str("/next");
        let attempts = Arc::new(AtomicUsize::new(0));
        server.expect(
            Expectation::matching(request::method_path("GET", "/collections/test/items"))
                .times(2)
                .respond_with(move || {
                    let first = attempts.fetch_add(1, Ordering::SeqCst) == 0;
                    responders::json_encoded(if first {
                        collection(&[item("partial", 0.)], Some(next.clone()))
                    } else {
                        collection(&[item("complete", 0.)], None)
                    })
                }),
        );
        server.expect(
            Expectation::matching(request::method_path("GET", "/next"))
                .times(1)
                .respond_with(responders::status_code(400)),
        );
        let provider = provider(
            &server,
            1024 * 1024,
            StacGrid {
                target_number_of_cells: 64_800,
            },
        );
        let meta = metadata(&provider, 0).await;
        assert!(
            meta.loading_info(query(bounds(0.1, 0.4), interval(3)))
                .await
                .is_err()
        );
        let retry = meta
            .loading_info(query(bounds(0.6, 0.9), interval(3)))
            .await
            .unwrap();
        assert_file(&retry, 0., "complete");
    }
}
