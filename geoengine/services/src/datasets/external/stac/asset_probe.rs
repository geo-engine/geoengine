//! Reads the grid, data type, no-data value and CRS of a STAC asset from its file header.
//!
//! STAC catalogs often omit the projection and raster metadata of `NetCDF` assets
//! (no `proj:transform`, `proj:shape` or `data_type`, and `proj:code: null`), while GDAL reads
//! all of it from the file. The provider and the discovery use this probe to complete such
//! assets.
//!
//! The header is read by the `gdalsource-process` workers (`GdalRequest::Header`), like every
//! other GDAL read, and kept in the pool's header cache.

use std::path::PathBuf;
use std::sync::Arc;

use geoengine_datatypes::primitives::{CacheHint, CacheTtlSeconds, Coordinate2D};
use geoengine_datatypes::raster::{GeoTransform, RasterDataType};
use geoengine_datatypes::spatial_reference::{SpatialReferenceAuthority, SpatialReferenceOption};
use geoengine_datatypes::util::NonZeroFiniteF64;
use geoengine_operators::source::gdal_worker_process::{
    GdalPoolDispatcher, GdalPoolReader, GdalProcessPool, GdalProcessPoolError, RasterInfo,
};
use geoengine_operators::source::{
    FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters,
};
use snafu::{OptionExt, ResultExt, Snafu};

use super::StacProviderS3Config;
use super::common;

/// The grid and band metadata of the first band of an asset file.
#[derive(Debug, Clone, PartialEq)]
pub struct AssetProbe {
    /// The geo transform exactly as GDAL reports it, so tiles match what the GDAL source reads.
    pub geo_transform: GeoTransform,
    pub width: usize,
    pub height: usize,
    pub data_type: RasterDataType,
    pub no_data_value: Option<f64>,
    /// `None` if the file declares no CRS or a CRS without an identifiable EPSG code.
    pub epsg: Option<u32>,
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum AssetProbeError {
    #[snafu(display("Could not read the header of {}: {source}", path.display()))]
    Read {
        path: PathBuf,
        source: GdalProcessPoolError,
    },
    #[snafu(display("{} has no geo transform", path.display()))]
    NoGeoTransform { path: PathBuf },
    #[snafu(display("{} has an invalid geo transform: {source}", path.display()))]
    GeoTransform {
        path: PathBuf,
        source: geoengine_datatypes::error::Error,
    },
    #[snafu(display("{} has no raster band", path.display()))]
    NoBand { path: PathBuf },
}

impl AssetProbeError {
    /// Whether the header could not be read because the environment does not allow the
    /// `userfaultfd` syscall, which the GDAL netCDF driver needs for remote files.
    #[cfg(test)]
    pub fn is_userfaultfd_unavailable(&self) -> bool {
        use geoengine_operators::source::gdal_worker_process::process_common::{
            IpcProcessError, IpcProcessGdalErrorKind,
        };
        matches!(
            self,
            Self::Read {
                source: GdalProcessPoolError::IpcProcessError {
                    source: IpcProcessError::GdalError {
                        kind: IpcProcessGdalErrorKind::UserfaultfdUnavailable,
                        ..
                    }
                },
                ..
            }
        )
    }
}

/// Reads asset headers through the GDAL worker pool.
#[derive(Debug, Clone)]
pub struct AssetProber {
    pool: Arc<GdalProcessPool>,
}

impl AssetProber {
    pub fn new(pool: Arc<GdalProcessPool>) -> Self {
        Self { pool }
    }

    /// Reads the header of the asset at `file_path` (`http(s)://`, `s3://` or a local path) with
    /// the S3 credentials of the provider.
    ///
    /// The header stays in the pool's header cache for `cache_ttl`. Without a TTL, the file is
    /// read again on every call, like the search results that are not cached either.
    ///
    /// # Errors
    /// Returns an error if the file cannot be opened or its first band has no supported data type
    /// or no valid geo transform.
    pub async fn probe(
        &self,
        file_path: PathBuf,
        s3_config: Option<&StacProviderS3Config>,
        cache_ttl: Option<CacheTtlSeconds>,
    ) -> Result<AssetProbe, AssetProbeError> {
        let params = header_params(file_path.clone(), s3_config);
        let reader = GdalPoolReader::from(GdalPoolDispatcher::new(self.pool.clone()));
        // the placeholder grid is the same for every probe of the file, so the parameters
        // identify the file and its options
        let cache_key: Option<Arc<str>> = serde_json::to_string(&params).ok().map(Into::into);

        let remembered = cache_key
            .as_deref()
            .and_then(|key| reader.cached_header(key));
        let info = match remembered {
            Some(info) => info,
            None => {
                let info = reader
                    .read_raster_info(params)
                    .await
                    .context(ReadSnafu { path: &file_path })?;
                if let (Some(key), Some(ttl)) = (cache_key, cache_ttl) {
                    reader.cache_header(key, info.clone(), CacheHint::seconds(ttl.seconds()));
                }
                info
            }
        };

        asset_probe_from_raster_info(&info, file_path)
    }
}

/// Parameters that read only the header of band 1 of `file_path`. The worker does not use the
/// grid of a header read, so it is a placeholder.
fn header_params(
    file_path: PathBuf,
    s3_config: Option<&StacProviderS3Config>,
) -> GdalDatasetParameters {
    let unit = NonZeroFiniteF64::new_unchecked(1.);
    let gdal_config_options =
        common::gdal_config_options_for_file_path(&file_path, s3_config, None);
    GdalDatasetParameters {
        file_path,
        rasterband_channel: 1,
        geo_transform: GdalDatasetGeoTransform {
            origin_coordinate: Coordinate2D::new(0., 0.),
            x_pixel_size: unit,
            y_pixel_size: -unit,
        },
        width: 1,
        height: 1,
        file_not_found_handling: FileNotFoundHandling::Error,
        no_data_value: None,
        properties_mapping: None,
        gdal_open_options: None,
        gdal_config_options,
        allow_alphaband_as_mask: false,
        retry: None,
    }
}

fn asset_probe_from_raster_info(
    info: &RasterInfo,
    path: PathBuf,
) -> Result<AssetProbe, AssetProbeError> {
    let full_resolution = info.levels.first().context(NoBandSnafu { path: &path })?;
    let geo_transform = info
        .geo_transform
        .context(NoGeoTransformSnafu { path: &path })?;
    let geo_transform = GeoTransform::new(
        geo_transform.origin_coordinate,
        geo_transform.x_pixel_size.get(),
        geo_transform.y_pixel_size.get(),
    )
    .context(GeoTransformSnafu { path })?;

    Ok(AssetProbe {
        geo_transform,
        width: full_resolution.size_x,
        height: full_resolution.size_y,
        data_type: info.data_type,
        no_data_value: info.no_data_value,
        epsg: match info.spatial_reference {
            SpatialReferenceOption::SpatialReference(spatial_reference)
                if *spatial_reference.authority() == SpatialReferenceAuthority::Epsg =>
            {
                Some(spatial_reference.code())
            }
            _ => None,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::tests::gdal_process_pool_with_workers;
    use geoengine_operators::source::gdal_worker_process::process_common::{
        IpcProcessError, IpcProcessGdalErrorKind,
    };

    #[tokio::test]
    async fn it_probes_a_netcdf_asset() {
        let pool = gdal_process_pool_with_workers(1);
        let prober = AssetProber::new(pool.clone());
        let file_path = crate::test_data!("raster/netcdf_stac/s3_syn_v10_ndvi.nc").to_path_buf();

        let probe = prober
            .probe(file_path.clone(), None, Some(CacheTtlSeconds::new(60)))
            .await
            .unwrap();

        assert_eq!(probe.data_type, RasterDataType::U8);
        assert_eq!(probe.epsg, Some(4326));
        assert_eq!(probe.no_data_value, Some(255.));
        assert_eq!((probe.width, probe.height), (16, 8));
        // GDAL derives the pixel size from the coordinate vectors, rounded to 15 digits
        assert!((probe.geo_transform.x_pixel_size().get() - 0.008_928_571_428_571).abs() < 1e-15);
        assert!((probe.geo_transform.y_pixel_size().get() + 0.008_928_571_428_571).abs() < 1e-15);
        assert!((probe.geo_transform.origin_coordinate().x + 10.995_535_714_285_715).abs() < 1e-12);
        assert!((probe.geo_transform.origin_coordinate().y - 75.004_464_285_714_29).abs() < 1e-12);

        // the second probe comes from the pool's header cache
        let cached = prober
            .probe(file_path, None, Some(CacheTtlSeconds::new(60)))
            .await
            .unwrap();
        assert_eq!(cached, probe);
        assert_eq!(pool.request_stats().headers, 1);
    }

    #[tokio::test]
    async fn it_reads_the_header_again_without_a_cache_ttl() {
        let pool = gdal_process_pool_with_workers(1);
        let prober = AssetProber::new(pool.clone());
        let file_path = crate::test_data!("raster/netcdf_stac/s3_syn_v10_ndvi.nc").to_path_buf();

        prober.probe(file_path.clone(), None, None).await.unwrap();
        prober.probe(file_path, None, None).await.unwrap();

        assert_eq!(pool.request_stats().headers, 2);
    }

    #[test]
    fn it_recognizes_a_missing_userfaultfd() {
        let error = AssetProbeError::Read {
            path: PathBuf::from("https://example.com/NDVI.nc"),
            source: GdalProcessPoolError::IpcProcessError {
                source: IpcProcessError::GdalError {
                    kind: IpcProcessGdalErrorKind::UserfaultfdUnavailable,
                    details: "requires Linux userfaultfd".to_string(),
                },
            },
        };
        assert!(error.is_userfaultfd_unavailable());
        assert!(error.to_string().contains("seccomp"));
    }
}
