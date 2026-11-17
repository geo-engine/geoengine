//! Reads the grid, data type, no-data value and CRS of a STAC asset from its file header.
//!
//! STAC catalogs often omit the projection and raster metadata of `NetCDF` assets
//! (no `proj:transform`, `proj:shape` or `data_type`, and `proj:code: null`), while GDAL reads
//! all of it from the file. The provider and the discovery use this probe to complete such
//! assets.
//!
//! TODO: read the header through the `gdalsource-process` workers (`GdalRequest::Header`)
//! instead of opening the file in this process.

use std::ffi::CString;
use std::path::{Path, PathBuf};

use gdal::spatial_ref::SpatialRef;
use gdal::{DatasetOptions, GdalOpenFlags};
use geoengine_datatypes::raster::{GdalGeoTransform, GeoTransform, RasterDataType};
use geoengine_operators::util::TemporaryGdalThreadLocalConfigOptions;
use geoengine_operators::util::gdal::gdal_open_dataset_ex;
use snafu::{ResultExt, Snafu};

use super::StacProviderS3Config;
use super::common;

/// EPSG code of WGS 84, which geographic CRSs on the WGS 84 ellipsoid are identified as.
const EPSG_WGS84: u32 = 4326;
const WGS84_SEMI_MAJOR: f64 = 6_378_137.0;
const WGS84_SEMI_MINOR: f64 = 6_356_752.314_245_179;
/// Tolerance in meters for comparing ellipsoid axes.
const ELLIPSOID_AXIS_TOLERANCE: f64 = 1e-3;

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
    #[snafu(display("Could not set GDAL config options to probe {}: {source}", path.display()))]
    ConfigOptions {
        path: PathBuf,
        source: geoengine_operators::error::Error,
    },
    #[snafu(display("Could not open {} to probe its header: {source}", path.display()))]
    Open {
        path: PathBuf,
        source: geoengine_operators::error::Error,
    },
    #[snafu(display("Could not read the header of {}: {source}", path.display()))]
    Header {
        path: PathBuf,
        source: gdal::errors::GdalError,
    },
    #[snafu(display("{} has an unsupported data type: {source}", path.display()))]
    DataType {
        path: PathBuf,
        source: geoengine_datatypes::error::Error,
    },
    #[snafu(display("{} has an invalid geo transform: {source}", path.display()))]
    GeoTransform {
        path: PathBuf,
        source: geoengine_datatypes::error::Error,
    },
    #[snafu(display("Invalid GDAL option for {}: {source}", path.display()))]
    PathOption {
        path: PathBuf,
        source: std::ffi::NulError,
    },
    #[snafu(display("The header probe task panicked or was cancelled: {source}"))]
    Join { source: tokio::task::JoinError },
}

/// Reads the header of the asset at `file_path` (`http(s)://`, `s3://` or a local path) with the
/// S3 credentials of the provider.
///
/// # Errors
/// Returns an error if the file cannot be opened or its first band has no supported data type
/// or no valid geo transform.
pub async fn probe_asset(
    file_path: PathBuf,
    s3_config: Option<StacProviderS3Config>,
) -> Result<AssetProbe, AssetProbeError> {
    crate::util::spawn_blocking(move || probe_asset_blocking(&file_path, s3_config.as_ref()))
        .await
        .context(JoinSnafu)?
}

fn probe_asset_blocking(
    file_path: &Path,
    s3_config: Option<&StacProviderS3Config>,
) -> Result<AssetProbe, AssetProbeError> {
    let config_options =
        common::gdal_config_options_for_file_path(file_path, s3_config, None).unwrap_or_default();
    // set config options for the current thread and revert them on drop
    let _config_options = TemporaryGdalThreadLocalConfigOptions::new(&config_options)
        .context(ConfigOptionsSnafu { path: file_path })?;
    // GDAL reads remote NetCDF files from a thread of its own, which does not see thread-local
    // options
    set_s3_bucket_options(file_path, &config_options)?;

    let open_path = gdal_open_path(file_path);
    let dataset = gdal_open_dataset_ex(
        &open_path,
        DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_READONLY
                | GdalOpenFlags::GDAL_OF_RASTER
                | GdalOpenFlags::GDAL_OF_VERBOSE_ERROR,
            allowed_drivers: None,
            open_options: None,
            sibling_files: None,
        },
    )
    .context(OpenSnafu { path: file_path })?;

    let band = dataset
        .rasterband(1)
        .context(HeaderSnafu { path: file_path })?;
    let data_type = RasterDataType::from_gdal_data_type(band.band_type())
        .context(DataTypeSnafu { path: file_path })?;
    let gdal_geo_transform: GdalGeoTransform = dataset
        .geo_transform()
        .context(HeaderSnafu { path: file_path })?;
    let geo_transform: GeoTransform = gdal_geo_transform
        .try_into()
        .context(GeoTransformSnafu { path: file_path })?;
    let (width, height) = dataset.raster_size();

    Ok(AssetProbe {
        geo_transform,
        width,
        height,
        data_type,
        no_data_value: band.no_data_value(),
        // GDAL reports a file without a CRS as an error
        epsg: dataset.spatial_ref().ok().and_then(epsg_of_spatial_ref),
    })
}

/// The `/vsis3/` prefix of the bucket of an `s3://` path, e.g. `/vsis3/bucket/`.
fn s3_bucket_prefix(file_path: &Path) -> Option<String> {
    let bucket = file_path
        .to_str()?
        .strip_prefix("s3://")?
        .split('/')
        .next()
        .filter(|bucket| !bucket.is_empty())?;
    Some(format!("/vsis3/{bucket}/"))
}

/// Sets `options` for all reads below the bucket of an `s3://` path, in every thread.
///
/// The GDAL `netCDF` driver reads files on remote file systems through a `userfaultfd` handler
/// thread, which does not see the thread-local options of the opening thread, so the S3 endpoint
/// and credentials must be path-specific options. They stay set for the bucket; providers that
/// read the same bucket with different credentials overwrite each other's options.
fn set_s3_bucket_options(
    file_path: &Path,
    options: &[(String, String)],
) -> Result<(), AssetProbeError> {
    let Some(prefix) = s3_bucket_prefix(file_path) else {
        return Ok(());
    };
    let prefix = CString::new(prefix).context(PathOptionSnafu { path: file_path })?;
    for (key, value) in options {
        let key = CString::new(key.as_str()).context(PathOptionSnafu { path: file_path })?;
        let value = CString::new(value.as_str()).context(PathOptionSnafu { path: file_path })?;
        // SAFETY: all arguments are valid NUL-terminated strings that outlive the call, and GDAL
        // copies them into its own synchronized option store
        unsafe {
            gdal_sys::VSISetPathSpecificOption(prefix.as_ptr(), key.as_ptr(), value.as_ptr());
        }
    }
    Ok(())
}

/// The path GDAL opens for `file_path`, like `GdalDatasetParameters::file_path_for_open`.
fn gdal_open_path(file_path: &Path) -> PathBuf {
    let file_path_str = file_path.to_string_lossy();
    if let Some(s3_path) = file_path_str.strip_prefix("s3://") {
        PathBuf::from(format!("/vsis3/{s3_path}"))
    } else if file_path_str.starts_with("http://") || file_path_str.starts_with("https://") {
        PathBuf::from(format!("/vsicurl/{file_path_str}"))
    } else {
        file_path.to_path_buf()
    }
}

/// The EPSG code of a CRS: its declared authority code, else the code GDAL identifies, else
/// 4326 for a geographic CRS on the WGS 84 ellipsoid.
///
/// `NetCDF` files often declare a CF `latitude_longitude` grid mapping with the WGS 84 ellipsoid
/// but no datum name, which GDAL reports as an unnamed CRS without an authority.
fn epsg_of_spatial_ref(mut spatial_ref: SpatialRef) -> Option<u32> {
    if let Some(code) = epsg_authority_code(&spatial_ref) {
        return Some(code);
    }
    if spatial_ref.auto_identify_epsg().is_ok()
        && let Some(code) = epsg_authority_code(&spatial_ref)
    {
        return Some(code);
    }
    is_geographic_wgs84(&spatial_ref).then_some(EPSG_WGS84)
}

fn epsg_authority_code(spatial_ref: &SpatialRef) -> Option<u32> {
    let authority = spatial_ref.auth_name()?;
    if !authority.eq_ignore_ascii_case("EPSG") {
        return None;
    }
    u32::try_from(spatial_ref.auth_code().ok()?).ok()
}

fn is_geographic_wgs84(spatial_ref: &SpatialRef) -> bool {
    spatial_ref.is_geographic()
        && spatial_ref
            .semi_major()
            .is_ok_and(|axis| (axis - WGS84_SEMI_MAJOR).abs() < ELLIPSOID_AXIS_TOLERANCE)
        && spatial_ref
            .semi_minor()
            .is_ok_and(|axis| (axis - WGS84_SEMI_MINOR).abs() < ELLIPSOID_AXIS_TOLERANCE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_maps_remote_paths_to_gdal_virtual_file_systems() {
        assert_eq!(
            gdal_open_path(Path::new("s3://bucket/a/b.nc")),
            PathBuf::from("/vsis3/bucket/a/b.nc")
        );
        assert_eq!(
            gdal_open_path(Path::new("https://example.com/b.nc")),
            PathBuf::from("/vsicurl/https://example.com/b.nc")
        );
        assert_eq!(
            gdal_open_path(Path::new("/data/b.nc")),
            PathBuf::from("/data/b.nc")
        );
    }

    #[test]
    fn it_scopes_s3_options_to_the_bucket() {
        assert_eq!(
            s3_bucket_prefix(Path::new("s3://eodata/Sentinel-3/a/NDVI.nc")).as_deref(),
            Some("/vsis3/eodata/")
        );
        assert_eq!(
            s3_bucket_prefix(Path::new("https://example.com/NDVI.nc")),
            None
        );
        assert_eq!(s3_bucket_prefix(Path::new("s3:///NDVI.nc")), None);
    }

    #[test]
    fn it_identifies_an_unnamed_geographic_wgs84_crs_as_4326() {
        // a CF `latitude_longitude` grid mapping with the WGS 84 ellipsoid, as GDAL reads it
        // from Sentinel-3 SYN NetCDF files
        let spatial_ref = SpatialRef::from_proj4("+proj=longlat +a=6378137 +rf=298.257223563")
            .expect("PROJ string should be valid");
        assert_eq!(epsg_of_spatial_ref(spatial_ref), Some(EPSG_WGS84));

        let utm = SpatialRef::from_epsg(32632).expect("EPSG:32632 should exist");
        assert_eq!(epsg_of_spatial_ref(utm), Some(32632));

        let sphere = SpatialRef::from_proj4("+proj=longlat +R=6371000")
            .expect("PROJ string should be valid");
        assert_eq!(epsg_of_spatial_ref(sphere), None);
    }

    #[tokio::test]
    async fn it_probes_a_netcdf_asset() {
        let probe = probe_asset(
            crate::test_data!("raster/netcdf_stac/s3_syn_v10_ndvi.nc").to_path_buf(),
            None,
        )
        .await
        .unwrap();

        assert_eq!(probe.data_type, RasterDataType::U8);
        assert_eq!(probe.epsg, Some(EPSG_WGS84));
        assert_eq!(probe.no_data_value, Some(255.));
        assert_eq!((probe.width, probe.height), (16, 8));
        // GDAL derives the pixel size from the coordinate vectors, rounded to 15 digits
        assert!((probe.geo_transform.x_pixel_size().get() - 0.008_928_571_428_571).abs() < 1e-15);
        assert!((probe.geo_transform.y_pixel_size().get() + 0.008_928_571_428_571).abs() < 1e-15);
        assert!((probe.geo_transform.origin_coordinate().x + 10.995_535_714_285_715).abs() < 1e-12);
        assert!((probe.geo_transform.origin_coordinate().y - 75.004_464_285_714_29).abs() < 1e-12);
    }
}
