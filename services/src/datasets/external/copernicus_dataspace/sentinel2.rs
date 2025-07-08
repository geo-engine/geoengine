use std::path::PathBuf;

use crate::{
    datasets::external::copernicus_dataspace::stac::{
        load_stac_items, resolve_datetime_duplicates,
    },
    util::sentinel_2_utm_zones::UtmZone,
};
use gdal::{DatasetOptions, GdalOpenFlags};
use geoengine_datatypes::{
    primitives::{
        AxisAlignedRectangle, CacheTtlSeconds, ColumnSelection, DateTime, RasterQueryRectangle,
        TimeInstance, TimeInterval, VectorQueryRectangle,
    },
    raster::{GeoTransform, GridShape2D, SpatialGridDefinition, TilingSpecification},
    spatial_reference::{SpatialReference, SpatialReferenceAuthority},
};
use geoengine_operators::{
    engine::{MetaData, RasterBandDescriptor, RasterResultDescriptor, SpatialGridDescriptor},
    source::{
        GdalDatasetParameters, GdalLoadingInfo, GdalLoadingInfoTemporalSlice,
        GdalLoadingInfoTemporalSliceIterator,
    },
    util::{
        TemporaryGdalThreadLocalConfigOptions,
        gdal::{gdal_open_dataset_ex, gdal_parameters_from_dataset},
    },
};

use async_trait::async_trait;
use snafu::{ResultExt, Snafu};
use url::Url;

use super::{
    ids::{Sentinel2Band, Sentinel2ProductBand},
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
    pub s3_use_https: bool,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub product_band: Sentinel2ProductBand,
    pub zone: UtmZone,
    pub gdal_config: Vec<(String, String)>,
}

impl Sentinel2Metadata {
    async fn crate_loading_info(
        &self,
        query: VectorQueryRectangle, // TODO: here the name is misleading :(
    ) -> Result<GdalLoadingInfo, CopernicusSentinel2Error> {
        let mut stac_items = load_stac_items(
            Url::parse(&self.stac_url).context(CannotParseStacUrl)?,
            "SENTINEL-2",
            query,
            self.zone.spatial_reference(),
            self.product_band.product_type(),
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
        [
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
            (
                "AWS_HTTPS".to_string(),
                if self.s3_use_https {
                    "YES".to_string()
                } else {
                    "NO".to_string()
                },
            ),
            // disable virtual hosting i.e. the bucket name is not used as subdomain and the bucket is at the root of the specified s3 bucket url
            // this makes it easier to test with mock http server
            ("AWS_VIRTUAL_HOSTING".to_string(), "FALSE".to_string()),
            // limit the number of threads because we are setting the config on the thread level
            // and other threads used by the JP2OpenJPEG driver do not have the credentials etc.
            // TODO: set credentials in a more robust way like the file path
            ("GDAL_NUM_THREADS".to_string(), "1".to_string()),
            // Settings to avoid running into 429 rate limiting errors
            // TODO: we need a global limit for all requests to the same host
            ("GDAL_HTTP_MAX_CONNECTIONS".to_string(), "1".to_string()),
            ("GDAL_HTTP_RETRY_CODES".to_string(), "429".to_string()),
            ("GDAL_HTTP_RETRY_DELAY".to_string(), "5".to_string()), // TODO: make configurable?
            // TODO: use our own retry mechanism in `load_tile_data_async` instead?
            ("GDAL_HTTP_MAX_RETRY".to_string(), "2".to_string()), //  TODO: make configurable?
            // Prevent opening non-existing aux files
            ("GDAL_PAM_ENABLED".to_string(), "NO".to_string()),
            // Debugging option
            // TODO: make configurable?
            // ("CPL_CURL_VERBOSE".to_string(), "YES".to_string()),
            // ("CPL_DEBUG".to_string(), "ON".to_string()),
        ]
        .into_iter()
        .chain(self.gdal_config.iter().cloned())
        .collect()
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
            "{}:/vsis3{}/{}:{}m:EPSG_{}",
            self.product_band.driver_name(),
            asset_url,
            self.product_band.main_file_name(),
            self.product_band.resolution_meters(),
            self.zone.epsg_code()
        ));

        let config_options = self.config_options();

        let config_options_clone = config_options.clone();
        let file_path_clone = file_path.clone();
        let channel = self.product_band.channel_in_subdataset();

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
        let utm_extent = self.zone.native_extent();
        let px_size = self.product_band.resolution_meters() as f64;
        let geo_transform = GeoTransform::new(utm_extent.upper_left(), px_size, -px_size);
        let grid_bounds = geo_transform.spatial_to_grid_bounds(&utm_extent);
        let spatial_grid = SpatialGridDefinition::new(geo_transform, grid_bounds);

        // FIXME: get tiling_spec!
        let tiling_specification = TilingSpecification::new(GridShape2D::new_2d(512, 512));

        let spatial_bounds = SpatialGridDescriptor::new_source(spatial_grid)
            .tiling_grid_definition(tiling_specification)
            .tiling_geo_transform()
            .grid_to_spatial_bounds(&query.spatial_bounds.grid_bounds());

        let spatial_bounds_query = VectorQueryRectangle::with_bounds(
            spatial_bounds.as_bbox(),
            query.time_interval,
            ColumnSelection::all(),
        );

        self.crate_loading_info(spatial_bounds_query)
            .await
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })
    }

    async fn result_descriptor(&self) -> geoengine_operators::util::Result<RasterResultDescriptor> {
        let utm_extent = self.zone.native_extent();
        let px_size = self.product_band.resolution_meters() as f64;
        let geo_transform = GeoTransform::new(utm_extent.upper_left(), px_size, -px_size);
        let grid_bounds = geo_transform.spatial_to_grid_bounds(&utm_extent);
        let spatial_grid = SpatialGridDefinition::new(geo_transform, grid_bounds);

        let spatial_grid_desc = SpatialGridDescriptor::new_source(spatial_grid);

        Ok(RasterResultDescriptor {
            data_type: self.product_band.data_type(),
            spatial_reference: SpatialReference::new(
                SpatialReferenceAuthority::Epsg,
                self.zone.epsg_code(),
            )
            .into(),
            // Data is available from the time of the first image until now
            // first image:
            // $ s3cmd -c .s3cfg ls s3://eodata/Sentinel-2/MSI/L1C/2015/07/04/
            // DIR  s3://eodata/Sentinel-2/MSI/L1C/2015/07/04/S2A_MSIL1C_20150704T101006_N0204_R022_T32SKB_20150704T101337.SAFE/
            time: Some(TimeInterval::new_unchecked(
                DateTime::new_utc(2015, 7, 4, 10, 10, 6),
                DateTime::now(),
            )),
            spatial_grid: spatial_grid_desc,
            bands: vec![RasterBandDescriptor::new_unitless(
                self.product_band.band_name(),
            )]
            .try_into()?, // TODO: add measurement unit
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
    use super::*;
    use crate::{
        datasets::external::copernicus_dataspace::ids::L2ABand,
        util::sentinel_2_utm_zones::UtmZoneDirection,
    };
    use geoengine_datatypes::{
        primitives::{Coordinate2D, DateTime, SpatialPartition2D},
        test_data,
    };
    use geoengine_operators::source::{FileNotFoundHandling, GdalDatasetGeoTransform};
    use httptest::{
        Expectation, Server, all_of,
        matchers::{contains, request, url_decoded},
        responders::status_code,
    };
    use std::env;

    fn add_partial_responses(
        server: &Server,
        path: &'static str,
        chunks: &[(usize, usize)],
        total_length: usize,
        mut body: Vec<u8>,
    ) {
        for (start, end) in chunks {
            let length = end - start + 1;
            server.expect(
                Expectation::matching(all_of![
                    request::method("GET"),
                    request::path(path),
                    request::headers(contains((
                        "range".to_string(),
                        format!("bytes={start}-{end}")
                    )))
                ])
                .respond_with(
                    status_code(206)
                        .append_header("content-type", "binary/octet-stream")
                        .append_header("content-length", format!("{length}"))
                        .append_header(
                            "content-range",
                            format!("bytes {start}-{end}/{total_length}"),
                        )
                        .body(body.drain(..length).collect::<Vec<u8>>()),
                ),
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial_test::serial]
    #[allow(clippy::too_many_lines)]
    async fn it_creates_loading_info() {
        let mock_server = httptest::Server::run();

        // STAC request
        let stac_body = tokio::fs::read(test_data!(
            "copernicus_dataspace/stac_responses/stac_response_1.json"
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

        // top level dataset request
        let mtd_xml_body = tokio::fs::read(test_data!(
            "copernicus_dataspace/eodata/Sentinel-2/MSI/L2A_N0500/2020/07/03/S2A_MSIL2A_20200703T103031_N0500_R108_T32UMB_20230321T201840.SAFE/MTD_MSIL2A.xml"
        ))
        .await
        .unwrap();

        add_partial_responses(
            &mock_server,
            "/eodata/Sentinel-2/MSI/L2A_N0500/2020/07/03/S2A_MSIL2A_20200703T103031_N0500_R108_T32UMB_20230321T201840.SAFE/MTD_MSIL2A.xml",
            &[(0, 16383), (16384, 54842)],
            mtd_xml_body.len(),
            mtd_xml_body,
        );

        // granule request
        let granule_mtd_xml_body = tokio::fs::read(test_data!(
            "copernicus_dataspace/eodata/Sentinel-2/MSI/L2A_N0500/2020/07/03/S2A_MSIL2A_20200703T103031_N0500_R108_T32UMB_20230321T201840.SAFE/GRANULE/L2A_T32UMB_A026274_20200703T103027/MTD_TL.xml"
        ))
        .await
        .unwrap();

        add_partial_responses(
            &mock_server,
            "/eodata/Sentinel-2/MSI/L2A_N0500/2020/07/03/S2A_MSIL2A_20200703T103031_N0500_R108_T32UMB_20230321T201840.SAFE/GRANULE/L2A_T32UMB_A026274_20200703T103027/MTD_TL.xml",
            &[(0, 16383)],
            granule_mtd_xml_body.len(),
            granule_mtd_xml_body,
        );

        // JPG2

        // B04
        let b04_jp2_head_body = tokio::fs::read(test_data!(
                "copernicus_dataspace/eodata/Sentinel-2/MSI/L2A_N0500/2020/07/03/S2A_MSIL2A_20200703T103031_N0500_R108_T32UMB_20230321T201840.SAFE/GRANULE/L2A_T32UMB_A026274_20200703T103027/IMG_DATA/R10m/T32UMB_20200703T103031_B04_10m.jp2.head"
            ))
            .await.unwrap();

        add_partial_responses(
            &mock_server,
            "/eodata/Sentinel-2/MSI/L2A_N0500/2020/07/03/S2A_MSIL2A_20200703T103031_N0500_R108_T32UMB_20230321T201840.SAFE/GRANULE/L2A_T32UMB_A026274_20200703T103027/IMG_DATA/R10m/T32UMB_20200703T103031_B04_10m.jp2",
            &[(0, 16383)],
            129_257_917,
            b04_jp2_head_body,
        );

        // B03
        let b03_jp2_head_body = tokio::fs::read(test_data!(
                "copernicus_dataspace/eodata/Sentinel-2/MSI/L2A_N0500/2020/07/03/S2A_MSIL2A_20200703T103031_N0500_R108_T32UMB_20230321T201840.SAFE/GRANULE/L2A_T32UMB_A026274_20200703T103027/IMG_DATA/R10m/T32UMB_20200703T103031_B03_10m.jp2.head"
            ))
            .await
            .unwrap();

        add_partial_responses(
            &mock_server,
            "/eodata/Sentinel-2/MSI/L2A_N0500/2020/07/03/S2A_MSIL2A_20200703T103031_N0500_R108_T32UMB_20230321T201840.SAFE/GRANULE/L2A_T32UMB_A026274_20200703T103027/IMG_DATA/R10m/T32UMB_20200703T103031_B03_10m.jp2",
            &[(0, 16383)],
            130_541_105,
            b03_jp2_head_body,
        );

        // B02
        let b02_jp2_head_body = tokio::fs::read(test_data!(
                 "copernicus_dataspace/eodata/Sentinel-2/MSI/L2A_N0500/2020/07/03/S2A_MSIL2A_20200703T103031_N0500_R108_T32UMB_20230321T201840.SAFE/GRANULE/L2A_T32UMB_A026274_20200703T103027/IMG_DATA/R10m/T32UMB_20200703T103031_B02_10m.jp2.head"
             ))
             .await
             .unwrap();

        add_partial_responses(
            &mock_server,
            "/eodata/Sentinel-2/MSI/L2A_N0500/2020/07/03/S2A_MSIL2A_20200703T103031_N0500_R108_T32UMB_20230321T201840.SAFE/GRANULE/L2A_T32UMB_A026274_20200703T103027/IMG_DATA/R10m/T32UMB_20200703T103031_B02_10m.jp2",
            &[(0, 16383)],
            132_214_802,
            b02_jp2_head_body,
        );

        // B08
        let b08_jp2_head_body = tokio::fs::read(test_data!(
                "copernicus_dataspace/eodata/Sentinel-2/MSI/L2A_N0500/2020/07/03/S2A_MSIL2A_20200703T103031_N0500_R108_T32UMB_20230321T201840.SAFE/GRANULE/L2A_T32UMB_A026274_20200703T103027/IMG_DATA/R10m/T32UMB_20200703T103031_B08_10m.jp2.head"
            ))
            .await
            .unwrap();

        add_partial_responses(
            &mock_server,
            "/eodata/Sentinel-2/MSI/L2A_N0500/2020/07/03/S2A_MSIL2A_20200703T103031_N0500_R108_T32UMB_20230321T201840.SAFE/GRANULE/L2A_T32UMB_A026274_20200703T103027/IMG_DATA/R10m/T32UMB_20200703T103031_B08_10m.jp2",
            &[(0, 16383)],
            134_142_350,
            b08_jp2_head_body,
        );

        // TODO: add responses for dataset

        let metadata = Sentinel2Metadata {
            stac_url: mock_server.url_str("/stac"),
            s3_url: format!("{}", mock_server.addr()),
            s3_use_https: false,
            s3_access_key: "ACCESS_KEY".to_string(),
            s3_secret_key: "SECRET_KEY".to_string(),
            product_band: Sentinel2ProductBand::L2A(L2ABand::B04),
            zone: UtmZone {
                zone: 32,
                direction: UtmZoneDirection::North,
            },
            gdal_config: vec![],
        };

        // time=2020-07-01T12%3A00%3A00.000Z/2020-07-03T12%3A00%3A00.000Z&EXCEPTIONS=application%2Fjson&WIDTH=256&HEIGHT=256&CRS=EPSG%3A32632&BBOX=482500%2C5627500%2C483500%2C5628500
        let loading_info = metadata
            .crate_loading_info(VectorQueryRectangle::with_bounds(
                SpatialPartition2D::new_unchecked(
                    (482_500., 5_627_500.).into(),
                    (483_500., 5_628_500.).into(),
                )
                .as_bbox(),
                TimeInterval::new_unchecked(
                    DateTime::parse_from_rfc3339("2020-07-01T12:00:00.000Z").unwrap(),
                    DateTime::parse_from_rfc3339("2020-07-03T12:00:00.000Z").unwrap(),
                ),
                ColumnSelection::all(),
            ))
            .await
            .unwrap();

        let expected = GdalLoadingInfoTemporalSlice {
            time: TimeInterval::new_unchecked(1_593_772_231_024, 1_593_772_231_124),
            params: Some(
                GdalDatasetParameters {
                    file_path: "SENTINEL2_L2A:/vsis3/eodata/Sentinel-2/MSI/L2A_N0500/2020/07/03/S2A_MSIL2A_20200703T103031_N0500_R108_T32UMB_20230321T201840.SAFE/MTD_MSIL2A.xml:10m:EPSG_32632".into(),
                    rasterband_channel: 3,
                    geo_transform: GdalDatasetGeoTransform {
                        origin_coordinate: Coordinate2D {
                            x: 399_960.0,
                            y: 5_700_000.0,
                        },
                        x_pixel_size: 10.0,
                        y_pixel_size: -10.0,
                    },
                    width: 10980,
                    height: 10980,
                    file_not_found_handling: FileNotFoundHandling::Error,
                    no_data_value: None,
                    properties_mapping: None,
                    gdal_open_options: None,
                    gdal_config_options: Some(vec![
                            (
                                "AWS_ACCESS_KEY_ID".to_string(),
                                "ACCESS_KEY".to_string(),
                            ),
                            (
                                "AWS_SECRET_ACCESS_KEY".to_string(),
                                "SECRET_KEY".to_string(),
                            ),
                            (
                                "AWS_S3_ENDPOINT".to_string(),
                                format!("{}", mock_server.addr()),
                            ),
                            (
                                "GDAL_DISABLE_READDIR_ON_OPEN".to_string(),
                                "EMPTY_DIR".to_string(),
                            ),
                            (
                                "AWS_HTTPS".to_string(),
                                "NO".to_string(),
                            ),
                            (
                                "AWS_VIRTUAL_HOSTING".to_string(),
                                "FALSE".to_string(),
                            ),
                            (
                                "GDAL_NUM_THREADS".to_string(),
                                "1".to_string(),
                            ),
                            (
                                "GDAL_HTTP_MAX_CONNECTIONS".to_string(),
                                "1".to_string(),
                            ),
                            (
                                "GDAL_HTTP_RETRY_CODES".to_string(),
                                "429".to_string(),
                            ),
                            (
                                "GDAL_HTTP_RETRY_DELAY".to_string(),
                                "5".to_string(),
                            ),
                            (
                                "GDAL_HTTP_MAX_RETRY".to_string(),
                                "2".to_string(),
                            ),
                            (
                                "GDAL_PAM_ENABLED".to_string(),
                                "NO".to_string(),
                            ),
                        ],
                    ),
                    allow_alphaband_as_mask: true,
                    retry: None,
                },
            ),
            cache_ttl: CacheTtlSeconds::new(
                86400,
            ),
        };

        let mut iter = loading_info.info;
        assert_eq!(iter.next().unwrap().unwrap(), expected);
        assert!(iter.next().is_none());
    }
}
