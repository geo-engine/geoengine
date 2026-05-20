use super::{StacDataProvider, StacProviderDataset, StacProviderS3Config};
use crate::util::join_base_url_and_path;
use async_trait::async_trait;
use chrono::DateTime as ChronoDateTime;
use geoengine_datatypes::dataset::DataId;
use geoengine_datatypes::operations::reproject::{
    CoordinateProjection, CoordinateProjector, ReprojectClipped,
};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, CacheHint, RasterQueryRectangle, TimeDimension, TimeInstance,
    TimeInterval, TryRegularTimeFillIterExt, VectorQueryRectangle,
};
use geoengine_datatypes::raster::{GeoTransform, GridBoundingBox2D, GridIdx2D, RasterDataType};
use geoengine_datatypes::spatial_reference::SpatialReference;
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
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use tracing::debug;
use url::Url;

const STAC_QUERY_TIMEOUT_SECS: u64 = 60;

#[derive(Debug, Clone)]
struct StacMultiBandMetaData {
    api_url: String,
    collection_name: String,
    s3_config: Option<StacProviderS3Config>,
    time_dimension: TimeDimension,
    dataset: StacProviderDataset,
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
    client: &reqwest::Client,
    query_state: &StacQueryState,
) -> geoengine_operators::util::Result<(stac::ItemCollection, StacQueryState)> {
    match query_state {
        StacQueryState::FirstPage {
            query_url,
            query_params,
        } => {
            debug!("STAC query first page with parameters: {:?}", query_params);

            let request_started = std::time::Instant::now();

            let item_collection: stac::ItemCollection = client
                .get(query_url.clone())
                .query(query_params)
                .send()
                .await
                .map_err(
                    |e| geoengine_operators::error::Error::QueryingProcessorFailed {
                        source: Box::new(e),
                    },
                )?
                .json()
                .await
                .map_err(
                    |e| geoengine_operators::error::Error::QueryingProcessorFailed {
                        source: Box::new(e),
                    },
                )?;

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

            let item_collection: stac::ItemCollection = client
                .get(next_url.clone())
                .send()
                .await
                .map_err(
                    |e| geoengine_operators::error::Error::QueryingProcessorFailed {
                        source: Box::new(e),
                    },
                )?
                .json()
                .await
                .map_err(
                    |e| geoengine_operators::error::Error::QueryingProcessorFailed {
                        source: Box::new(e),
                    },
                )?;

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
    #[allow(clippy::too_many_lines)]
    async fn loading_info(
        &self,
        query: MultiBandGdalLoadingInfoQueryRectangle,
    ) -> geoengine_operators::util::Result<MultiBandGdalLoadingInfo> {
        let base_url = Url::from_str(&self.api_url)
            .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?;
        let items_url = join_base_url_and_path(
            &base_url,
            &format!("collections/{}/items", self.collection_name),
        )
        .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?;

        let bbox = stac_query_bbox(
            query.query_rectangle.spatial_bounds(),
            self.dataset.projection,
        )?;
        let time_interval =
            stac_query_time_interval(query.query_rectangle.time_interval(), self.time_dimension)?;
        let time_start = time_interval.start();
        let time_end = time_interval.end();

        let query_params = vec![
			(
				"bbox".to_owned(),
				format!(
					"{},{},{},{}",
					bbox.lower_left().x,
					bbox.lower_left().y,
					bbox.upper_right().x,
					bbox.upper_right().y
				),
			),
			(
				"datetime".to_owned(),
				format!(
					"{}/{}",
					time_start
						.as_date_time()
						.ok_or(geoengine_operators::error::Error::InvalidDataProviderConfig)?
						.to_datetime_string_with_millis(),
					time_end
						.as_date_time()
						.ok_or(geoengine_operators::error::Error::InvalidDataProviderConfig)?
						.to_datetime_string_with_millis(),
				),
			),
			("limit".to_owned(), "100".to_owned()),
			(
				"fields".to_owned(),
				"stac_version,properties.datetime,properties.updated,assets.*.title,assets.*.href,assets.*.data_type,assets.*.bands,assets.*.proj:code,assets.*.proj:shape,assets.*.proj:transform".to_owned(),
			),
		];

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(STAC_QUERY_TIMEOUT_SECS))
            .build()
            .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?;
        let mut query_state = StacQueryState::FirstPage {
            query_url: items_url,
            query_params,
        };

        let mut files = Vec::new();
        let mut time_steps = Vec::new();

        while !matches!(query_state, StacQueryState::Finished) {
            let (item_collection, next_state) =
                query_stac_item_collection(&client, &query_state).await?;

            for item in item_collection.items {
                if item.version != stac::Version::v1_1_0 {
                    tracing::warn!(
                        "Skipping STAC item with unsupported version: {:?}",
                        item.version
                    );
                    continue;
                }

                let Some(item_datetime) = item.properties.datetime else {
                    tracing::warn!("Skipping STAC item without datetime: {}", item.id);
                    continue;
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

                let item_time = TimeInstance::from_millis(item_datetime.timestamp_millis())
                    .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?;

                let time = match self.time_dimension {
                    TimeDimension::Regular(regular) => {
                        let time_start = regular.snap_prev(item_time).map_err(|_e| {
                            geoengine_operators::error::Error::InvalidDataProviderConfig
                        })?;
                        let time_end = (time_start + regular.step).map_err(|_e| {
                            geoengine_operators::error::Error::InvalidDataProviderConfig
                        })?;

                        TimeInterval::new(time_start, time_end).map_err(|_e| {
                            geoengine_operators::error::Error::InvalidDataProviderConfig
                        })?
                    }
                    TimeDimension::Irregular => {
                        unreachable!("irregular time dimension rejected at provider initialization")
                    }
                };

                time_steps.push(time);

                if !query.fetch_tiles {
                    tracing::trace!(
                        "STAC query does not require fetching tiles, skipping item with id: {}",
                        item.id
                    );
                    continue;
                }

                for (_asset_key, asset) in item.assets {
                    if data_type_from_asset_v1_1_0(&asset) != Some(self.dataset.data_type) {
                        continue;
                    }

                    if !proj_code_matches_dataset(&asset.additional_fields, self.dataset.projection)
                    {
                        continue;
                    }

                    let Some(geo_transform) = geo_transform_from_fields(&asset.additional_fields)
                    else {
                        tracing::warn!(
                            "Skipping asset with href {} due to missing geo transform",
                            asset.href
                        );
                        continue;
                    };

                    let Some((height, width)) = proj_shape_from_fields(&asset.additional_fields)
                    else {
                        tracing::warn!(
                            "Skipping asset with href {} due to missing projection shape",
                            asset.href
                        );
                        continue;
                    };

                    if (geo_transform.x_pixel_size().abs() - self.dataset.resolution.x).abs() > 1e-9
                        || (geo_transform.y_pixel_size().abs() - self.dataset.resolution.y).abs()
                            > 1e-9
                    {
                        continue;
                    }

                    let Some(asset_title) = asset.title.as_deref() else {
                        tracing::warn!(
                            "Skipping asset with href {} due to missing title",
                            asset.href
                        );
                        continue;
                    };

                    let grid_bounds = GridBoundingBox2D::new(
                        GridIdx2D::new([0, 0]),
                        GridIdx2D::new([(width as isize) - 1, (height as isize) - 1]),
                    )
                    .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?;
                    let spatial_partition = geo_transform.grid_to_spatial_bounds(&grid_bounds);

                    let file_path = gdal_file_path(&asset.href)
                        .ok_or(geoengine_operators::error::Error::InvalidDataProviderConfig)?;

                    let gdal_config_options = self.gdal_config_options_for_file_path(&file_path);

                    for (dataset_band_idx, dataset_band) in self.dataset.bands.iter().enumerate() {
                        if dataset_band.asset_title != asset_title {
                            continue;
                        }

                        let rasterband_channel = if asset.bands.is_empty() {
                            if dataset_band.band_name.is_some() {
                                tracing::warn!(
                                    "STAC asset with href {} does not include bands, but dataset configuration requires a band name. Skipping asset.",
                                    asset.href
                                );
                                continue;
                            }

                            1
                        } else {
                            let Some(required_band_name) = dataset_band.band_name.as_deref() else {
                                tracing::warn!(
                                    "STAC asset with href {} includes bands, but dataset configuration does not specify a band name. Skipping asset.",
                                    asset.href
                                );
                                continue;
                            };

                            let Some(asset_band_idx) = asset.bands.iter().position(|asset_band| {
                                asset_band.name.as_deref() == Some(required_band_name)
                            }) else {
                                tracing::debug!(
                                    "Skipping asset with href {} due to missing required band {}",
                                    asset.href,
                                    required_band_name
                                );
                                continue;
                            };

                            asset_band_idx + 1
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
                                retry: Some(GdalRetryOptions { max_retries: 99 }),
                            },
                        });
                    }
                }
            }

            query_state = next_state;
        }

        time_steps.sort_by(|a, b| a.start().cmp(&b.start()).then(a.end().cmp(&b.end())));
        time_steps.dedup();

        let time_steps = match self.time_dimension {
            TimeDimension::Regular(regular) => time_steps
                .into_iter()
                .map(Ok::<_, geoengine_operators::error::Error>)
                .try_time_regular_range_fill(regular, time_interval)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?,
            TimeDimension::Irregular => {
                unreachable!("irregular time dimension rejected at provider initialization")
            }
        };

        files.sort_by(|a, b| {
            a.time
                .start()
                .cmp(&b.time.start())
                .then(a.time.end().cmp(&b.time.end()))
                .then(a.band.cmp(&b.band))
                .then(a.z_index.cmp(&b.z_index))
        });

        Ok(MultiBandGdalLoadingInfo::new(
            time_steps,
            files,
            CacheHint::default(),
        ))
    }

    async fn result_descriptor(&self) -> geoengine_operators::util::Result<RasterResultDescriptor> {
        Ok(RasterResultDescriptor {
            data_type: self.dataset.data_type,
            spatial_reference: self.dataset.projection.into(),
            time: TimeDescriptor {
                bounds: None,
                dimension: self.time_dimension,
            },
            spatial_grid: self.dataset.spatial_grid,
            bands: RasterBandDescriptors::new_multiple_bands(self.dataset.bands.len() as u32),
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
    fn gdal_config_options_for_file_path(&self, file_path: &Path) -> Option<Vec<(String, String)>> {
        let file_path_str = file_path.to_string_lossy();
        let is_vsi_s3 = file_path_str.starts_with("/vsis3/");
        let is_vsi_curl = file_path_str.starts_with("/vsicurl/");

        if !is_vsi_s3 && !is_vsi_curl {
            return None;
        }

        let mut options = vec![
            (
                "GDAL_DISABLE_READDIR_ON_OPEN".to_owned(),
                "EMPTY_DIR".to_owned(),
            ),
            (
                "CPL_VSIL_CURL_ALLOWED_EXTENSIONS".to_owned(),
                ".tif,.tiff,.jp2".to_owned(),
            ),
        ];

        if !is_vsi_s3 {
            return Some(options);
        }

        if let Some(config) = self.s3_config.as_ref() {
            options.push(("AWS_S3_ENDPOINT".to_owned(), config.endpoint.clone()));
            options.push(("AWS_VIRTUAL_HOSTING".to_owned(), "FALSE".to_owned())); // TODO: make configurable?

            if let Some(access_key) = &config.access_key {
                options.push(("AWS_ACCESS_KEY_ID".to_owned(), access_key.clone()));
            }

            if let Some(secret_key) = &config.secret_key {
                options.push(("AWS_SECRET_ACCESS_KEY".to_owned(), secret_key.clone()));
            }
        }

        Some(options)
    }
}

fn stac_query_bbox(
    spatial_bounds: geoengine_datatypes::primitives::SpatialPartition2D,
    spatial_reference: SpatialReference,
) -> geoengine_operators::util::Result<geoengine_datatypes::primitives::BoundingBox2D> {
    let projector =
        CoordinateProjector::from_known_srs(spatial_reference, SpatialReference::epsg_4326())
            .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?;

    spatial_bounds
        .as_bbox()
        .reproject_clipped(&projector)
        .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?
        .ok_or(geoengine_operators::error::Error::InvalidDataProviderConfig)
}

fn stac_query_time_interval(
    query_time_interval: TimeInterval,
    time_dimension: TimeDimension,
) -> geoengine_operators::util::Result<TimeInterval> {
    match time_dimension {
        TimeDimension::Regular(regular) => {
            let start = regular
                .snap_prev(query_time_interval.start())
                .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?;
            let end = regular
                .snap_next(query_time_interval.end())
                .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?;

            let end = if end <= start {
                (start + regular.step)
                    .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)?
            } else {
                end
            };

            TimeInterval::new(start, end)
                .map_err(|_e| geoengine_operators::error::Error::InvalidDataProviderConfig)
        }
        TimeDimension::Irregular => Ok(query_time_interval),
    }
}

fn gdal_file_path(href: &str) -> Option<PathBuf> {
    if href.starts_with("http") {
        return Some(PathBuf::from(format!("/vsicurl/{href}")));
    }

    href.strip_prefix("s3://")
        .map(|s3_path| PathBuf::from(format!("/vsis3/{s3_path}")))
}

fn proj_shape_from_fields(fields: &serde_json::Map<String, Value>) -> Option<(usize, usize)> {
    let proj_shape = fields.get("proj:shape")?.as_array()?;
    if proj_shape.len() != 2 {
        return None;
    }

    let height = proj_shape.first()?.as_u64()? as usize;
    let width = proj_shape.get(1)?.as_u64()? as usize;

    Some((height, width))
}

fn geo_transform_from_fields(fields: &serde_json::Map<String, Value>) -> Option<GeoTransform> {
    let proj_transform = fields.get("proj:transform")?;
    let proj_transform_array = proj_transform.as_array()?;
    if proj_transform_array.len() != 6 {
        return None;
    }

    let proj_transform_values = proj_transform_array
        .iter()
        .map(Value::as_f64)
        .collect::<Option<Vec<_>>>()?;

    let gdal_geotransform = [
        proj_transform_values[2],
        proj_transform_values[0],
        proj_transform_values[1],
        proj_transform_values[5],
        proj_transform_values[3],
        proj_transform_values[4],
    ];

    Some(gdal_geotransform.into())
}

fn data_type_from_asset_v1_1_0(asset: &stac::Asset) -> Option<RasterDataType> {
    asset
        .data_type
        .as_ref()
        .and_then(raster_data_type_from_stac_data_type)
}

fn raster_data_type_from_stac_data_type(
    data_type: &stac_extensions::raster::DataType,
) -> Option<RasterDataType> {
    match data_type {
        stac_extensions::raster::DataType::UInt8 => Some(RasterDataType::U8),
        stac_extensions::raster::DataType::UInt16 => Some(RasterDataType::U16),
        stac_extensions::raster::DataType::UInt32 => Some(RasterDataType::U32),
        stac_extensions::raster::DataType::Int16 => Some(RasterDataType::I16),
        stac_extensions::raster::DataType::Int32 => Some(RasterDataType::I32),
        stac_extensions::raster::DataType::Float32 => Some(RasterDataType::F32),
        stac_extensions::raster::DataType::Float64 => Some(RasterDataType::F64),
        _ => None,
    }
}

fn proj_code_matches_dataset(
    fields: &serde_json::Map<String, Value>,
    dataset_projection: SpatialReference,
) -> bool {
    let Some(code) = fields.get("proj:code") else {
        return false;
    };

    let Some(proj_code) = proj_code_as_srs_string(code) else {
        return false;
    };

    proj_code == dataset_projection.to_string()
}

fn proj_code_as_srs_string(value: &Value) -> Option<String> {
    if let Some(code_number) = value.as_u64() {
        return Some(format!("EPSG:{code_number}"));
    }

    let code_str = value.as_str()?.trim();
    if code_str.contains(':') {
        return Some(code_str.to_ascii_uppercase());
    }

    if let Ok(code_number) = code_str.parse::<u32>() {
        return Some(format!("EPSG:{code_number}"));
    }

    None
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
        MetaData, MetaDataProvider, RasterResultDescriptor, WorkflowOperatorPath,
    };
    use geoengine_operators::source::{
        MultiBandGdalLoadingInfo, MultiBandGdalLoadingInfoQueryRectangle,
    };
    use httptest::{Expectation, Server, matchers::request, responders};
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
                        asset_title: "NIR 1 (band 8) - 10m".to_owned(),
                        band_name: Some("B08".to_owned()),
                    },
                    crate::datasets::external::stac::StacProviderDatasetBand {
                        asset_title: "Red (band 4) - 10m".to_owned(),
                        band_name: Some("B04".to_owned()),
                    },
                ],
            }],
        }
    }

    fn stac_items_response() -> serde_json::Value {
        let json_str =
            include_str!("../../../../../test_data/stac_responses/items/code-de-marburg.json");
        serde_json::from_str(json_str).expect("code-de-marburg.json should be valid JSON")
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
                    server.url_str("").trim_end_matches('/').to_owned(),
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
                    param.file_path.to_string_lossy().contains("/vsis3/"),
                    "B08 file should use S3 path: {:?}",
                    param.file_path
                );
            }
            for param in &b04_params {
                assert!(
                    param.file_path.to_string_lossy().contains("/vsis3/"),
                    "B04 file should use S3 path: {:?}",
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
            api_url: server.url_str(""),
            collection_name: "sentinel-2-l2a".to_owned(),
            s3_config: None,
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
                            asset_title: "Blue (band 2) - 10m".to_owned(),
                            band_name: Some("B02".to_owned()),
                        },
                        crate::datasets::external::stac::StacProviderDatasetBand {
                            asset_title: "Green (band 3) - 10m".to_owned(),
                            band_name: Some("B03".to_owned()),
                        },
                        crate::datasets::external::stac::StacProviderDatasetBand {
                            asset_title: "Water vapour (WVP) - 10m".to_owned(),
                            band_name: Some("WVP".to_owned()),
                        },
                        crate::datasets::external::stac::StacProviderDatasetBand {
                            asset_title: "NIR 1 (band 8) - 10m".to_owned(),
                            band_name: Some("B08".to_owned()),
                        },
                        crate::datasets::external::stac::StacProviderDatasetBand {
                            asset_title: "Red (band 4) - 10m".to_owned(),
                            band_name: Some("B04".to_owned()),
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
                            asset_title: "Aerosol optical thickness (AOT) - 20m".to_owned(),
                            band_name: Some("AOT".to_owned()),
                        },
                        crate::datasets::external::stac::StacProviderDatasetBand {
                            asset_title: "Scene classification map (SCL) - 20m".to_owned(),
                            band_name: Some("SCL".to_owned()),
                        },
                    ],
                },
            ],
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
            .operator()
            .expect("workflow should have operator")
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
}
