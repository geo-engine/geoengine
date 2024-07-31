use std::path::PathBuf;

use crate::pro::datasets::external::copernicus_dataspace::stac::{
    load_stac_items, resolve_datetime_duplicates,
};
use gdal::{DatasetOptions, GdalOpenFlags};
use geoengine_datatypes::{
    primitives::{
        CacheTtlSeconds, RasterQueryRectangle, SpatialResolution, TimeInstance, TimeInterval,
    },
    spatial_reference::{SpatialReference, SpatialReferenceAuthority},
};
use geoengine_operators::{
    engine::{MetaData, RasterBandDescriptor, RasterResultDescriptor},
    source::{
        GdalDatasetParameters, GdalLoadingInfo, GdalLoadingInfoTemporalSlice,
        GdalLoadingInfoTemporalSliceIterator,
    },
    util::{
        gdal::{gdal_open_dataset_ex, gdal_parameters_from_dataset},
        TemporaryGdalThreadLocalConfigOptions,
    },
};

use async_trait::async_trait;
use snafu::{ResultExt, Snafu};
use url::Url;

use super::{
    ids::{Sentinel2Band, Sentinel2Product, UtmZone},
    stac::{CopernicusStacError, StacItemExt},
};

const CACHE_TTL_SECS: u32 = 60 * 60 * 24 * 30; // 30 days
const FALLBACK_CACHE_TTL_SECS: u32 = 60 * 60 * 24; // 1 day, time to cache tiles that have no known successor
const FALLBACK_TIME_SLICE_DURATION_MILLIS: i64 = 100; // validity of tiles that have no known successor

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum CopernicusSentinel2Error {
    CannotRetrieveStacItems {
        source: CopernicusStacError,
    },
    CannotParseStacUrl {
        source: url::ParseError,
    },
    CannotResolveDateTimeDuplicates {
        source: CopernicusStacError,
    },
    Stac {
        source: CopernicusStacError,
    },
    InvalidTimeInterval {
        source: geoengine_datatypes::error::Error,
    },
    CouldNotOpenDataset {
        source: geoengine_operators::error::Error,
    },
    CouldNotGenerateGdalParameters {
        source: geoengine_operators::error::Error,
    },
    Join {
        source: tokio::task::JoinError,
    },
    MissingAssetProductUrl {
        source: CopernicusStacError,
    },
}

#[derive(Debug, Clone)]
pub struct Sentinel2Metadata {
    pub stac_url: String, // TODO: use url crate
    pub s3_url: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub product: Sentinel2Product,
    pub zone: UtmZone,
    pub band: Sentinel2Band,
}

impl Sentinel2Metadata {
    async fn crate_loading_info(
        &self,
        query: RasterQueryRectangle,
    ) -> Result<GdalLoadingInfo, CopernicusSentinel2Error> {
        let mut stac_items = load_stac_items(
            Url::parse(&self.stac_url).context(CannotParseStacUrl)?,
            "SENTINEL-2",
            query,
            self.zone.spatial_reference(),
            self.product.product_type(),
        )
        .await
        .context(CannotRetrieveStacItems)?;

        resolve_datetime_duplicates(&mut stac_items).context(CannotResolveDateTimeDuplicates)?;

        // TODO: query right before and after query to determine temporal bounds
        //       and make sure that there is one stac item before and after the query time

        let mut parts = vec![];

        for (idx, item) in stac_items.iter().enumerate() {
            let time_start: TimeInstance = item.datetime().context(Stac)?.into();

            let next = stac_items.get(idx + 1);

            let (time_end, cache_ttl): (TimeInstance, u32) = if let Some(next) = next {
                (next.datetime().context(Stac)?.into(), CACHE_TTL_SECS)
            } else {
                // if no successor is known, set data to be valid forever but only cache this for a limited amount of time
                (
                    time_start + FALLBACK_TIME_SLICE_DURATION_MILLIS,
                    FALLBACK_CACHE_TTL_SECS,
                )
            };

            let part = GdalLoadingInfoTemporalSlice {
                time: TimeInterval::new(time_start, time_end).context(InvalidTimeInterval)?,
                params: Some(self.create_gdal_params(item).await?),
                cache_ttl: CacheTtlSeconds::new(cache_ttl),
            };

            parts.push(part);
        }

        // TODO: add time bounds
        Ok(GdalLoadingInfo::new_no_known_time_bounds(
            GdalLoadingInfoTemporalSliceIterator::Static {
                parts: parts.into_iter(),
            },
        ))
    }

    fn config_options(&self) -> Vec<(String, String)> {
        vec![
            ("AWS_ACCESS_KEY_ID".to_string(), self.s3_access_key.clone()),
            (
                "AWS_SECRET_ACCESS_KEY".to_string(),
                self.s3_secret_key.clone(),
            ),
            ("AWS_S3_ENDPOINT".to_string(), self.s3_url.clone()),
            (
                "GDAL_DISABLE_READDIR_ON_OPEN".to_string(),
                "EMPTY_DIR".to_string(),
            ),
            // limit the number of threads because we are setting the config on the thread level
            // and other threads used by the JP2OpenJPEG driver do not have the credentials etc.
            // TODO: set credentials in a more robust way like the file path
            ("GDAL_NUM_THREADS".to_string(), "1".to_string()),
            // Settings to avoid running into 429 rate limiting errors
            // TODO: we need a global limit for all requests to the same host
            (
                "CPL_VSIL_CURL_CHUNK_SIZE".to_string(),
                "2097152".to_string(), // request 2MB instead of 16KB chunks
            ),
            ("GDAL_HTTP_MAX_CONNECTIONS".to_string(), "1".to_string()),
            ("GDAL_HTTP_RETRY_CODES".to_string(), "429".to_string()),
            ("GDAL_HTTP_RETRY_DELAY".to_string(), "30".to_string()), // TODO: make configurable?
            // TODO: use our own retry mechanism in `load_tile_data_async` instead?
            ("GDAL_HTTP_MAX_RETRY".to_string(), "10".to_string()), //  TODO: make configurable?
            // Prevent opening non-existing aux files
            ("GDAL_PAM_ENABLED".to_string(), "NO".to_string()),
            // Debugging option
            // TODO: make configurable?
            // ("CPL_CURL_VERBOSE".to_string(), "YES".to_string()),
            // ("CPL_DEBUG".to_string(), "ON".to_string()),
        ]
    }

    // TODO: reuse most of the logic for all copernicus products
    async fn create_gdal_params(
        &self,
        item: &stac::Item,
    ) -> Result<GdalDatasetParameters, CopernicusSentinel2Error> {
        let asset_url = item
            .s3_assert_product_url()
            .context(MissingAssetProductUrl)?;

        let file_path = PathBuf::from(&format!(
            "{}:/vsis3/eodata{}/{}:{}m:EPSG_{}",
            self.product.driver_name(),
            asset_url,
            self.product.main_file_name(),
            self.band.resolution_meters(),
            self.zone.epsg_code()
        ));

        let config_options = self.config_options();

        let config_options_clone = config_options.clone();
        let file_path_clone = file_path.clone();
        let channel = self.band.channel_in_subdataset();

        let mut gdal_params = crate::util::spawn_blocking(move || {
            // set config options for the current thread and revert them on drop
            let _config_options_holder =
                TemporaryGdalThreadLocalConfigOptions::new(&config_options_clone);

            let dataset_options = DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_READONLY
                    | GdalOpenFlags::GDAL_OF_RASTER
                    | GdalOpenFlags::GDAL_OF_VERBOSE_ERROR,
                allowed_drivers: None,
                open_options: None,
                sibling_files: None,
            };

            let dataset = gdal_open_dataset_ex(&file_path_clone, dataset_options)
                .context(CouldNotOpenDataset)?;

            gdal_parameters_from_dataset(&dataset, channel, &file_path, None, None)
                .context(CouldNotGenerateGdalParameters)
        })
        .await
        .context(Join)??;

        // drop(config_options);
        gdal_params.gdal_config_options = Some(config_options);

        Ok(gdal_params)
    }
}

#[async_trait]
impl MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle> for Sentinel2Metadata {
    async fn loading_info(
        &self,
        query: RasterQueryRectangle,
    ) -> geoengine_operators::util::Result<GdalLoadingInfo> {
        self.crate_loading_info(query).await.map_err(|e| {
            geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            }
        })
    }

    async fn result_descriptor(&self) -> geoengine_operators::util::Result<RasterResultDescriptor> {
        Ok(RasterResultDescriptor {
            data_type: self.band.data_type(),
            spatial_reference: SpatialReference::new(
                SpatialReferenceAuthority::Epsg,
                self.zone.epsg_code(),
            )
            .into(),
            time: None, // TODO: specify time (2015, open end/current date)?
            bbox: None, // TODO: exclude parts that are never visited by the satellite?
            resolution: Some(SpatialResolution::new(
                self.band.resolution_meters() as f64,
                self.band.resolution_meters() as f64,
            )?),
            bands: vec![RasterBandDescriptor::new_unitless(format!("{}", self.band))].try_into()?, // TODO: add measurement unit
        })
    }

    fn box_clone(
        &self,
    ) -> Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{BandSelection, DateTime, SpatialPartition2D},
        test_data,
    };
    use httptest::{
        all_of,
        matchers::{contains, request, url_decoded},
        responders::status_code,
        Expectation,
    };

    use crate::pro::datasets::external::copernicus_dataspace::ids::UtmZoneDirection;

    use super::*;

    #[tokio::test]
    async fn it_creates_loading_info() {
        let mock_server = httptest::Server::run();

        let stac_body = tokio::fs::read(test_data!(
            "pro/copernicus_dataspace/stac_responses/stac_response_1.json"
        ))
        .await
        .unwrap();

        mock_server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path("/stac/collections/SENTINEL-2/items"),
                request::query(url_decoded(contains((
                    "bbox",
                    "8.751627919756874,50.79897568511778,8.765865426422579,50.80799775499464",
                )))),
                request::query(url_decoded(contains((
                    "datetime",
                    "2020-07-01T12:00:00+00:00/2020-07-03T12:00:00+00:00",
                )))),
                request::query(url_decoded(contains(("limit", "1000")))),
                request::query(url_decoded(contains(("page", "1")))),
                request::query(url_decoded(contains(("sortby", "+datetime"))))
            ])
            .respond_with(
                status_code(200)
                    .append_header("Content-Type", "application/json")
                    .body(stac_body),
            ),
        );

        // TODO: add responses for dataset

        let metadata = Sentinel2Metadata {
            stac_url: mock_server.url_str("/stac"),
            s3_url: mock_server.url_str("/s3"),
            s3_access_key: "ACCESS_KEY".to_string(),
            s3_secret_key: "SECRET_KEY".to_string(),
            product: Sentinel2Product::L2A,
            zone: UtmZone {
                zone: 32,
                direction: UtmZoneDirection::North,
            },
            band: Sentinel2Band::B04,
        };

        // time=2020-07-01T12%3A00%3A00.000Z/2020-07-03T12%3A00%3A00.000Z&EXCEPTIONS=application%2Fjson&WIDTH=256&HEIGHT=256&CRS=EPSG%3A32632&BBOX=482500%2C5627500%2C483500%2C5628500
        let loading_info = metadata
            .crate_loading_info(RasterQueryRectangle {
                spatial_bounds: SpatialPartition2D::new_unchecked(
                    (482_500., 5_627_500.).into(),
                    (483_500., 5_628_500.).into(),
                ),
                time_interval: TimeInterval::new_unchecked(
                    DateTime::parse_from_rfc3339("2020-07-01T12:00:00.000Z").unwrap(),
                    DateTime::parse_from_rfc3339("2020-07-03T12:00:00.000Z").unwrap(),
                ),
                spatial_resolution: SpatialResolution::new(10., 10.).unwrap(),
                attributes: BandSelection::first(),
            })
            .await
            .unwrap();

        dbg!(loading_info);
    }
}
