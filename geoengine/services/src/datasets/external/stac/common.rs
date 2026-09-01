//! Common utilities shared between the STAC provider and the STAC harvester CLI.
//!
//! This module contains functions for parsing STAC metadata (geometry, EPSG, data types,
//! band names), STAC API query helpers, and GDAL file path handling.
//!
//! All functions herein should be usable from both `loading_info.rs` (the STAC provider)
//! and `cli/stac_harvester.rs` (the new STAC harvester CLI).

#![allow(dead_code)]

use geoengine_datatypes::{
    primitives::{TimeDimension, TimeInstance, TimeInterval},
    raster::{GdalGeoTransform, GeoTransform, RasterDataType},
    spatial_reference::SpatialReference,
};
use serde::Deserialize;

use super::StacProviderS3Config;

/// STAC `fields` query parameter used to keep item responses small while including all
/// metadata needed by the provider (loading info) and the harvester (discovery/mapping).
///
/// Includes both the STAC 1.1.0 metadata (`assets.*.data_type`, `assets.*.bands`,
/// `assets.*.proj:code`) and the STAC 1.0.0 metadata (`assets.*.raster:bands`,
/// item-level projection fields, and `assets.*.proj:epsg`) so that items of either version
/// survive the field filter.
pub const STAC_ITEM_FIELDS: &str = "stac_version,properties.datetime,properties.updated,properties.proj:code,properties.proj:epsg,assets.*.title,assets.*.href,assets.*.data_type,assets.*.bands,assets.*.raster:bands,assets.*.proj:code,assets.*.proj:epsg,assets.*.proj:shape,assets.*.proj:transform";

// ---------------------------------------------------------------------------
// STAC extension version types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StacExtensionMajorVersion {
    V1,
    V2,
}

/// Extract a `GeoTransform` from `proj:transform` in asset/collection fields.
///
/// The STAC `proj:transform` is a 6-element array in GDAL convention:
/// `[pixel_width, rotation, origin_x, rotation, pixel_height, origin_y]`.
pub fn geo_transform_from_fields(
    fields: &serde_json::Map<String, serde_json::Value>,
) -> Option<GeoTransform> {
    let proj_transform = fields.get("proj:transform")?;
    let proj_transform_array = proj_transform.as_array()?;
    if proj_transform_array.len() != 6 {
        return None;
    }

    let values: Vec<f64> = proj_transform_array
        .iter()
        .filter_map(serde_json::Value::as_f64)
        .collect();
    if values.len() != 6 {
        return None;
    }

    // A `GeoTransform` requires non-zero pixel sizes. Some STAC catalogs encode
    // angular/QA assets with a zero pixel height, so skip those instead of panicking.
    if values[0] == 0.0 || values[4] == 0.0 {
        return None;
    }

    // GDAL geo-transform: [origin_x, pixel_width, rotation, origin_y, rotation, pixel_height]
    let gdal_geotransform: GdalGeoTransform = [
        values[2], // origin_x
        values[0], // pixel_width
        values[1], // rotation
        values[5], // origin_y
        values[3], // rotation
        values[4], // pixel_height (negative for north-up)
    ];
    Some(gdal_geotransform.into())
}

/// Extract `(height, width)` from `proj:shape` in asset fields.
pub fn proj_shape_from_fields(
    fields: &serde_json::Map<String, serde_json::Value>,
) -> Option<(usize, usize)> {
    let proj_shape = fields.get("proj:shape")?.as_array()?;
    if proj_shape.len() != 2 {
        return None;
    }
    let height = proj_shape.first()?.as_u64()? as usize;
    let width = proj_shape.get(1)?.as_u64()? as usize;

    Some((height, width))
}

// ---------------------------------------------------------------------------
// EPSG / projection helpers
// ---------------------------------------------------------------------------

/// Extract EPSG code from asset fields, respecting the STAC extension version's
/// field priority (`proj:epsg` vs `proj:code`).
pub fn epsg_code_from_fields(
    proj_extension_version: StacExtensionMajorVersion,
    fields: &serde_json::Map<String, serde_json::Value>,
) -> Option<u32> {
    let proj_epsg = fields.get("proj:epsg").and_then(|value| {
        value
            .as_u64()
            .map(|code| code as u32)
            .or_else(|| value.as_str().and_then(|code| code.parse::<u32>().ok()))
    });

    let proj_code = fields
        .get("proj:code")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_epsg_from_proj_code);

    match proj_extension_version {
        StacExtensionMajorVersion::V1 => proj_epsg.or(proj_code),
        StacExtensionMajorVersion::V2 => proj_code.or(proj_epsg),
    }
}

/// Extract EPSG code from a STAC item, with multi-layered fallback between
/// additional fields and serialized properties, and fallback extension version.
pub fn epsg_code_from_item(
    item: &stac::Item,
    proj_extension_version: StacExtensionMajorVersion,
) -> Option<u32> {
    let from_additional =
        epsg_code_from_fields(proj_extension_version, &item.properties.additional_fields);
    if from_additional.is_some() {
        return from_additional;
    }

    let properties = serde_json::to_value(item)
        .ok()
        .and_then(|value| value.get("properties").cloned())
        .and_then(|value| value.as_object().cloned())?;

    let from_properties = epsg_code_from_fields(proj_extension_version, &properties);
    if from_properties.is_some() {
        return from_properties;
    }

    let fallback_version = match proj_extension_version {
        StacExtensionMajorVersion::V1 => StacExtensionMajorVersion::V2,
        StacExtensionMajorVersion::V2 => StacExtensionMajorVersion::V1,
    };

    epsg_code_from_fields(fallback_version, &properties)
}

/// Parse an EPSG code from a `proj:code` string like `"EPSG:32632"` or
/// `"http://www.opengis.net/def/crs/EPSG/0/32632"`.
pub fn parse_epsg_from_proj_code(code: &str) -> Option<u32> {
    if let Some(code) = code.strip_prefix("EPSG:") {
        return code.parse::<u32>().ok();
    }

    // e.g. http://www.opengis.net/def/crs/EPSG/0/32632
    code.rsplit('/').next()?.parse::<u32>().ok()
}

/// Check if the asset's `proj:code` matches the dataset's projection.
pub fn proj_code_matches_dataset(
    fields: &serde_json::Map<String, serde_json::Value>,
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

/// Normalize a `proj:code` field value to an `"EPSG:nnnn"` string.
pub fn proj_code_as_srs_string(value: &serde_json::Value) -> Option<String> {
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

// ---------------------------------------------------------------------------
// Data type conversion helpers
// ---------------------------------------------------------------------------

/// Map a `stac_extensions::raster::DataType` to a Geo Engine `RasterDataType`.
pub fn raster_data_type_from_stac_data_type(
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

/// Map a STAC data type string (e.g. `"uint16"`, `"float32"`) to a `RasterDataType`.
pub fn raster_data_type_from_stac_data_type_str(data_type_str: &str) -> Option<RasterDataType> {
    match data_type_str.to_lowercase().as_str() {
        "uint8" => Some(RasterDataType::U8),
        "uint16" => Some(RasterDataType::U16),
        "uint32" => Some(RasterDataType::U32),
        "int16" => Some(RasterDataType::I16),
        "int32" => Some(RasterDataType::I32),
        "float32" => Some(RasterDataType::F32),
        "float64" => Some(RasterDataType::F64),
        _ => None,
    }
}

/// Extract data type from a STAC 1.1.0 asset (common metadata `data_type` field).
pub fn data_type_from_asset_v1_1_0(asset: &stac::Asset) -> Option<RasterDataType> {
    asset
        .data_type
        .as_ref()
        .and_then(raster_data_type_from_stac_data_type)
}

/// Extract data type from a STAC 1.1.0 asset with fallback to `additional_fields["data_type"]`.
///
/// Some STAC APIs write `data_type` as a raw string in additional fields instead of
/// populating the typed `stac::Asset::data_type` field. This function tries the typed
/// field first, then falls back to reading from `additional_fields`.
pub fn data_type_from_asset_v1_1_0_fallback(asset: &stac::Asset) -> Option<RasterDataType> {
    data_type_from_asset_v1_1_0(asset).or_else(|| {
        asset
            .additional_fields
            .get("data_type")
            .and_then(|v| v.as_str())
            .and_then(raster_data_type_from_stac_data_type_str)
    })
}

/// Extract data type from a STAC 1.0.0 asset, reading from `additional_fields["raster:bands"][0]["data_type"]`.
///
/// STAC 1.0.0 stores data types inside `raster:bands[]` arrays rather than directly on the asset.
/// This function reads the first band's `data_type` as a raw string.
pub fn data_type_from_asset_v1_0_0_fallback(asset: &stac::Asset) -> Option<RasterDataType> {
    asset
        .additional_fields
        .get("raster:bands")
        .and_then(|v| v.as_array())
        .and_then(|bands| bands.first())
        .and_then(|band| band.get("data_type"))
        .and_then(|v| v.as_str())
        .and_then(raster_data_type_from_stac_data_type_str)
}

// ---------------------------------------------------------------------------
// Band processing helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct EoBand {
    pub name: String,
    #[serde(default)]
    pub common_name: Option<String>,
}

/// Map a GDAL raster band channel index for a dataset band within an asset.
///
/// If the asset has no `bands` metadata, returns channel 1 (single-band asset).
/// If the asset has exactly one band, returns channel 1: a single-band asset has
/// only one GDAL raster band, and STAC servers commonly label it with a short
/// code (e.g. `B10`) while the configured band name may be the human-readable
/// asset title (e.g. `Thermal Infrared 10.9 (band 10) - 100m`), so a strict name
/// match would wrongly skip the asset.
/// If the asset has multiple bands, matches by `band_name` against asset band
/// names to select the channel.
/// Returns `None` if the required band is not found.
pub fn rasterband_channel_for_dataset_band(
    asset: &stac::Asset,
    required_band_name: Option<&str>,
) -> Option<usize> {
    if asset.bands.is_empty() {
        // No `bands` metadata: assume a single-band raster and map to channel 1.
        // STAC servers commonly omit `bands` for single-band products (e.g.
        // Sentinel-2 SCL/CLD/SNW, Landsat QA bands), even when the mapping
        // configures an explicit band name. Skipping here would silently lose
        // the band, so proceed with channel 1 (the single-band path below does
        // the same regardless of the requested name).
        if required_band_name.is_some() {
            tracing::debug!(
                "STAC asset with href {} does not include bands, but dataset configuration requires band name {:?}. Assuming single-band raster (channel 1).",
                asset.href,
                required_band_name
            );
        }
        return Some(1);
    }

    if asset.bands.len() == 1 {
        return Some(1);
    }

    let Some(required_band_name) = required_band_name else {
        tracing::warn!(
            "STAC asset with href {} includes {} bands, but dataset configuration does not specify a band name. Skipping asset.",
            asset.href,
            asset.bands.len()
        );
        return None;
    };

    let Some(asset_band_idx) = asset
        .bands
        .iter()
        .position(|asset_band| asset_band.name.as_deref() == Some(required_band_name))
    else {
        tracing::debug!(
            "Skipping asset with href {} due to missing required band {}",
            asset.href,
            required_band_name
        );
        return None;
    };

    Some(asset_band_idx + 1)
}

/// Parsed band information from a STAC 1.1.0 asset.
#[derive(Debug, Clone, PartialEq)]
pub struct AssetBandInfo {
    pub asset_title: String,
    pub band_names: Vec<String>,
}

/// Derive band names from a STAC 1.1.0 `Asset`, using the `bands` field.
///
/// For assets without `bands` metadata or with exactly one band, the single
/// band is named after the asset title. For multi-band assets the individual
/// STAC band names (e.g. `B04`) are returned, so the mapping can reference the
/// exact raster channel while keeping the real asset title.
pub fn band_names_from_asset_v1_1_0(
    asset: &stac::Asset,
    asset_key: Option<&str>,
) -> Result<AssetBandInfo, String> {
    let asset_title = asset
        .title
        .as_deref()
        .ok_or_else(|| "Missing title in asset metadata".to_string())?
        .to_string();

    let bands = &asset.bands;

    if bands.is_empty() || bands.len() == 1 {
        // Use asset_key if provided (e.g., "B01", "B02"), otherwise fall back to asset_title
        let band_name = asset_key.unwrap_or(&asset_title).to_string();
        return Ok(AssetBandInfo {
            asset_title: asset_title.clone(),
            band_names: vec![band_name],
        });
    }

    let mut names = Vec::new();
    for band in bands {
        let Some(band_name) = &band.name else {
            return Err("Band is missing name for multi-band asset".to_string());
        };
        names.push(band_name.clone());
    }

    Ok(AssetBandInfo {
        asset_title,
        band_names: names,
    })
}

/// Derive band names from a STAC 1.1.0 `ItemAsset`, using the `bands` additional field.
///
/// Prefers the `asset_key` when provided (e.g., "B01", "B02" from STAC collection `item_assets` keys).
/// Falls back to band names from the `bands` field, or the asset title.
pub fn band_names_from_item_asset_v1_1_0(
    asset: &stac::ItemAsset,
    asset_key: Option<&str>,
) -> Result<AssetBandInfo, String> {
    let asset_title = asset
        .title
        .as_deref()
        .ok_or_else(|| "Missing title in asset metadata".to_string())?
        .to_string();

    let band_names = asset
        .additional_fields
        .get("bands")
        .and_then(serde_json::Value::as_array);

    let Some(bands) = band_names else {
        // Use asset_key if provided, otherwise fall back to asset_title
        let band_name = asset_key.unwrap_or(&asset_title).to_string();
        return Ok(AssetBandInfo {
            asset_title: asset_title.clone(),
            band_names: vec![band_name],
        });
    };

    if bands.is_empty() || bands.len() == 1 {
        // Use asset_key if provided, otherwise fall back to asset_title
        let band_name = asset_key.unwrap_or(&asset_title).to_string();
        return Ok(AssetBandInfo {
            asset_title: asset_title.clone(),
            band_names: vec![band_name],
        });
    }

    // For multi-band assets, try to extract band names from the bands field
    let mut names = Vec::new();
    for band in bands {
        let band_name = band
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "Band is missing name for multi-band asset".to_string())?;
        names.push(band_name.to_string());
    }

    Ok(AssetBandInfo {
        asset_title,
        band_names: names,
    })
}

/// Normalize a label string: trim, lowercase, join whitespace-separated words with underscores.
pub fn normalize_label(value: &str) -> String {
    value
        .trim()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join("_")
}

/// Derive a fallback band label from an asset title.
///
/// Prefers concise acronym-like labels in parentheses, e.g. `"Scene classification map (SCL)"` → `"scl"`.
/// Falls back to a normalized version of the full title.
pub fn title_fallback_label(title: Option<&str>) -> String {
    if let Some(title) = title {
        // Prefer concise acronym-like labels in parentheses
        if let (Some(start), Some(end)) = (title.rfind('('), title.rfind(')'))
            && start < end
        {
            let short = title[start + 1..end].trim();
            if !short.is_empty()
                && short.len() <= 32
                && short
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            {
                return short.to_lowercase();
            }
        }

        let normalized = normalize_label(title);
        if !normalized.is_empty() {
            return normalized;
        }
    }

    "band".to_string()
}

/// Map red-edge variant names for Sentinel-2 bands B05/B06/B07 from metadata.
pub fn rededge_variant_from_metadata(eo_name: &str, title: Option<&str>) -> Option<&'static str> {
    let eo = eo_name.to_lowercase();
    let title_lower = title.map(str::to_lowercase).unwrap_or_default();

    if eo.contains("b05")
        || eo.contains("band_5")
        || title_lower.contains("band 5")
        || title_lower.contains("b05")
    {
        return Some("rededge1");
    }
    if eo.contains("b06")
        || eo.contains("band_6")
        || title_lower.contains("band 6")
        || title_lower.contains("b06")
    {
        return Some("rededge2");
    }
    if eo.contains("b07")
        || eo.contains("band_7")
        || title_lower.contains("band 7")
        || title_lower.contains("b07")
    {
        return Some("rededge3");
    }

    None
}

/// Derive a stable band name from STAC 1.0.0 metadata (EO band + asset title).
pub fn v1_0_0_band_name(
    title: Option<&str>,
    eo_band: Option<&EoBand>,
    band_count: usize,
) -> String {
    let eo_name = eo_band.and_then(|band| {
        let eo_name = band.name.to_lowercase();
        let common_name = band.common_name.as_ref().map(|name| name.to_lowercase());

        match common_name.as_deref() {
            // `rededge` is used for multiple Sentinel-2 bands (B05/B06/B07).
            // Keep stable, unique names to avoid band collisions.
            Some("rededge") => rededge_variant_from_metadata(&eo_name, title)
                .map(std::string::ToString::to_string)
                .or_else(|| Some(format!("rededge[{eo_name}]"))),
            Some(common_name) => Some(common_name.to_string()),
            None => Some(eo_name),
        }
    });

    if band_count > 1 {
        let asset_label = title_fallback_label(title);
        let eo_name = eo_name.unwrap_or_else(|| "band".to_string());
        return format!("{asset_label}[{eo_name}]");
    }

    if let Some(eo_name) = eo_name {
        return eo_name;
    }

    title_fallback_label(title)
}

// ---------------------------------------------------------------------------
// Media type helpers
// ---------------------------------------------------------------------------

/// Check if a media type is a Cloud-Optimized `GeoTIFF`.
pub fn is_cog_media_type(media_type: Option<&str>) -> bool {
    media_type == Some("image/tiff; application=geotiff; profile=cloud-optimized")
}

/// Check if a media type is JPEG 2000.
pub fn is_jp2_media_type(media_type: Option<&str>) -> bool {
    media_type == Some("image/jp2")
}

// ---------------------------------------------------------------------------
// Time helpers
// ---------------------------------------------------------------------------

/// Snap a timestamp to the previous step boundary of a regular time dimension and return
/// the interval `[start, start + step)`.
///
/// Returns `None` for irregular dimensions or if the arithmetic fails. Both the STAC
/// provider and the STAC harvester use this so that harvested tiles and provider-loaded
/// tiles produce identical time intervals for the same item.
pub fn snap_time_interval(
    time: TimeInstance,
    time_dimension: &TimeDimension,
) -> Option<TimeInterval> {
    match time_dimension {
        TimeDimension::Regular(regular) => {
            let start = regular.snap_prev(time).ok()?;
            let end = (start + regular.step).ok()?;
            TimeInterval::new(start, end).ok()
        }
        TimeDimension::Irregular => None,
    }
}

// ---------------------------------------------------------------------------
// GDAL config options
// ---------------------------------------------------------------------------

/// Build GDAL configuration options for S3-backed file paths.
///
/// Returns the common options plus S3-specific credentials when an S3 config is provided.
pub fn gdal_config_options_for_s3(
    s3_config: Option<&StacProviderS3Config>,
) -> Vec<(String, String)> {
    let mut options = Vec::new();

    if let Some(config) = s3_config {
        // For old GDAL versions, the S3 endpoint may not include the protocol
        options.push(("AWS_S3_ENDPOINT".to_owned(), config.endpoint.clone()));
        options.push(("AWS_VIRTUAL_HOSTING".to_owned(), "FALSE".to_owned()));

        if let Some(access_key) = &config.access_key {
            options.push(("AWS_ACCESS_KEY_ID".to_owned(), access_key.clone()));
        }

        if let Some(secret_key) = &config.secret_key {
            options.push(("AWS_SECRET_ACCESS_KEY".to_owned(), secret_key.clone()));
        }
    }

    options
}

/// Build GDAL configuration options for a remote (HTTP or S3) file path, including common CURL/S3 options.
///
/// When `retries` is set, adds GDAL HTTP retry options so transient failures reading
/// remote tiles are retried.
pub fn gdal_config_options_for_file_path(
    file_path: &std::path::Path,
    s3_config: Option<&StacProviderS3Config>,
    retries: Option<usize>,
) -> Option<Vec<(String, String)>> {
    let file_path_str = file_path.to_string_lossy();
    let is_s3 = file_path_str.starts_with("s3://");
    let is_http = file_path_str.starts_with("http://") || file_path_str.starts_with("https://");

    if !is_s3 && !is_http {
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

    if let Some(retries) = retries {
        options.push(("GDAL_HTTP_MAX_RETRY".to_owned(), retries.to_string()));
        options.push(("GDAL_HTTP_RETRY_DELAY".to_owned(), "5".to_owned()));
    }

    if is_s3 {
        options.extend(gdal_config_options_for_s3(s3_config));
    }

    Some(options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceAuthority};

    // -----------------------------------------------------------------------
    // geo_transform_from_fields
    // -----------------------------------------------------------------------

    #[test]
    fn test_geo_transform_from_fields_standard() {
        let mut fields = serde_json::Map::new();
        fields.insert(
            "proj:transform".to_string(),
            serde_json::json!([10.0, 0.0, 399_960.0, 0.0, -10.0, 5_700_000.0]),
        );

        let gt = geo_transform_from_fields(&fields).expect("should parse transform");
        assert!((gt.origin_coordinate.x - 399_960.0).abs() < 1e-9);
        assert!((gt.origin_coordinate.y - 5_700_000.0).abs() < 1e-9);
        assert!((gt.x_pixel_size() - 10.0).abs() < 1e-9);
        assert!((gt.y_pixel_size() - (-10.0)).abs() < 1e-9);
    }

    #[test]
    fn test_geo_transform_from_fields_missing() {
        let fields = serde_json::Map::new();
        assert!(geo_transform_from_fields(&fields).is_none());
    }

    #[test]
    fn test_geo_transform_from_fields_wrong_length() {
        let mut fields = serde_json::Map::new();
        fields.insert(
            "proj:transform".to_string(),
            serde_json::json!([1.0, 2.0, 3.0]),
        );
        assert!(geo_transform_from_fields(&fields).is_none());
    }

    #[test]
    fn test_geo_transform_from_fields_zero_pixel_size() {
        // Some STAC catalogs encode angular/QA assets with a zero pixel height;
        // these must be skipped rather than causing a panic.
        let mut fields = serde_json::Map::new();
        fields.insert(
            "proj:transform".to_string(),
            serde_json::json!([539_085.0, 30.0, 0.0, 5_846_715.0, 0.0, -30.0]),
        );
        assert!(geo_transform_from_fields(&fields).is_none());
    }

    // -----------------------------------------------------------------------
    // proj_shape_from_fields
    // -----------------------------------------------------------------------

    #[test]
    fn test_proj_shape_from_fields_standard() {
        let mut fields = serde_json::Map::new();
        fields.insert("proj:shape".to_string(), serde_json::json!([10980, 10980]));
        let (height, width) = proj_shape_from_fields(&fields).expect("should parse shape");
        assert_eq!(height, 10_980);
        assert_eq!(width, 10_980);
    }

    #[test]
    fn test_proj_shape_from_fields_missing() {
        let fields = serde_json::Map::new();
        assert!(proj_shape_from_fields(&fields).is_none());
    }

    // -----------------------------------------------------------------------
    // raster_data_type_from_stac_data_type
    // -----------------------------------------------------------------------

    #[test]
    fn test_raster_data_type_from_stac_data_type_all() {
        use stac_extensions::raster::DataType;
        assert_eq!(
            raster_data_type_from_stac_data_type(&DataType::UInt8),
            Some(RasterDataType::U8)
        );
        assert_eq!(
            raster_data_type_from_stac_data_type(&DataType::UInt16),
            Some(RasterDataType::U16)
        );
        assert_eq!(
            raster_data_type_from_stac_data_type(&DataType::UInt32),
            Some(RasterDataType::U32)
        );
        assert_eq!(
            raster_data_type_from_stac_data_type(&DataType::Int16),
            Some(RasterDataType::I16)
        );
        assert_eq!(
            raster_data_type_from_stac_data_type(&DataType::Int32),
            Some(RasterDataType::I32)
        );
        assert_eq!(
            raster_data_type_from_stac_data_type(&DataType::Float32),
            Some(RasterDataType::F32)
        );
        assert_eq!(
            raster_data_type_from_stac_data_type(&DataType::Float64),
            Some(RasterDataType::F64)
        );
    }

    #[test]
    fn test_raster_data_type_from_stac_data_type_unknown() {
        use stac_extensions::raster::DataType;
        assert_eq!(raster_data_type_from_stac_data_type(&DataType::Int8), None);
    }

    // -----------------------------------------------------------------------
    // raster_data_type_from_stac_data_type_str
    // -----------------------------------------------------------------------

    #[test]
    fn test_raster_data_type_from_stac_data_type_str_all() {
        assert_eq!(
            raster_data_type_from_stac_data_type_str("uint8"),
            Some(RasterDataType::U8)
        );
        assert_eq!(
            raster_data_type_from_stac_data_type_str("uint16"),
            Some(RasterDataType::U16)
        );
        assert_eq!(
            raster_data_type_from_stac_data_type_str("float32"),
            Some(RasterDataType::F32)
        );
        assert_eq!(
            raster_data_type_from_stac_data_type_str("UINT16"),
            Some(RasterDataType::U16)
        );
        assert_eq!(raster_data_type_from_stac_data_type_str("unknown"), None);
    }

    // -----------------------------------------------------------------------
    // parse_epsg_from_proj_code
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_epsg_from_proj_code_epsg_prefix() {
        assert_eq!(parse_epsg_from_proj_code("EPSG:32632"), Some(32632));
    }

    #[test]
    fn test_parse_epsg_from_proj_code_url() {
        assert_eq!(
            parse_epsg_from_proj_code("http://www.opengis.net/def/crs/EPSG/0/32632"),
            Some(32632)
        );
    }

    #[test]
    fn test_parse_epsg_from_proj_code_invalid() {
        assert!(parse_epsg_from_proj_code("invalid").is_none());
    }

    // -----------------------------------------------------------------------
    // proj_code_as_srs_string
    // -----------------------------------------------------------------------

    #[test]
    fn test_proj_code_as_srs_string_number() {
        assert_eq!(
            proj_code_as_srs_string(&serde_json::json!(32632)),
            Some("EPSG:32632".to_string())
        );
    }

    #[test]
    fn test_proj_code_as_srs_string_epsg_format() {
        assert_eq!(
            proj_code_as_srs_string(&serde_json::json!("EPSG:32632")),
            Some("EPSG:32632".to_string())
        );
    }

    #[test]
    fn test_proj_code_as_srs_string_lowercase() {
        assert_eq!(
            proj_code_as_srs_string(&serde_json::json!("epsg:32632")),
            Some("EPSG:32632".to_string())
        );
    }

    // -----------------------------------------------------------------------
    // proj_code_matches_dataset
    // -----------------------------------------------------------------------

    #[test]
    fn test_proj_code_matches_dataset_matching() {
        let mut fields = serde_json::Map::new();
        fields.insert("proj:code".to_string(), serde_json::json!("EPSG:32632"));
        let srs = SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632);
        assert!(proj_code_matches_dataset(&fields, srs));
    }

    #[test]
    fn test_proj_code_matches_dataset_not_matching() {
        let mut fields = serde_json::Map::new();
        fields.insert("proj:code".to_string(), serde_json::json!("EPSG:32633"));
        let srs = SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632);
        assert!(!proj_code_matches_dataset(&fields, srs));
    }

    #[test]
    fn test_proj_code_matches_dataset_missing() {
        let fields = serde_json::Map::new();
        let srs = SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632);
        assert!(!proj_code_matches_dataset(&fields, srs));
    }

    // -----------------------------------------------------------------------
    // epsg_code_from_fields
    // -----------------------------------------------------------------------

    #[test]
    fn test_epsg_code_from_fields_v1_epsg() {
        let mut fields = serde_json::Map::new();
        fields.insert("proj:epsg".to_string(), serde_json::json!(32632));
        assert_eq!(
            epsg_code_from_fields(StacExtensionMajorVersion::V1, &fields),
            Some(32632)
        );
    }

    #[test]
    fn test_epsg_code_from_fields_v1_code() {
        let mut fields = serde_json::Map::new();
        fields.insert("proj:code".to_string(), serde_json::json!("EPSG:32632"));
        // V1 prefers proj:epsg over proj:code
        assert_eq!(
            epsg_code_from_fields(StacExtensionMajorVersion::V1, &fields),
            Some(32632)
        );
    }

    #[test]
    fn test_epsg_code_from_fields_v2_code() {
        let mut fields = serde_json::Map::new();
        fields.insert("proj:code".to_string(), serde_json::json!("EPSG:32632"));
        // V2 prefers proj:code over proj:epsg
        assert_eq!(
            epsg_code_from_fields(StacExtensionMajorVersion::V2, &fields),
            Some(32632)
        );
    }

    // -----------------------------------------------------------------------
    // normalize_label / title_fallback_label
    // -----------------------------------------------------------------------

    #[test]
    fn test_normalize_label() {
        assert_eq!(
            normalize_label("Scene classification map"),
            "scene_classification_map"
        );
        assert_eq!(normalize_label("  Blue  band "), "blue_band");
    }

    #[test]
    fn test_title_fallback_label_parentheses() {
        assert_eq!(
            title_fallback_label(Some("Scene classification map (SCL)")),
            "scl"
        );
    }

    #[test]
    fn test_title_fallback_label_no_parentheses() {
        assert_eq!(
            title_fallback_label(Some("Blue (band 2) - 10m")),
            "blue_(band_2)_-_10m"
        );
    }

    #[test]
    fn test_title_fallback_label_none() {
        assert_eq!(title_fallback_label(None), "band");
    }

    // -----------------------------------------------------------------------
    // band_names_from_asset_v1_1_0
    // -----------------------------------------------------------------------

    #[test]
    fn test_band_names_from_asset_v1_1_0_no_bands() {
        let json = serde_json::json!({
            "href": "http://example.com/file.tif",
            "title": "My Band"
        });
        let asset: stac::Asset = serde_json::from_value(json).unwrap();
        let info = band_names_from_asset_v1_1_0(&asset, None).expect("should succeed");
        assert_eq!(info.asset_title, "My Band");
        assert_eq!(info.band_names, vec!["My Band"]);
    }

    #[test]
    fn test_band_names_from_asset_v1_1_0_single_band() {
        let json = serde_json::json!({
            "href": "http://example.com/file.tif",
            "title": "My Asset",
            "bands": [{"name": "B01"}]
        });
        let asset: stac::Asset = serde_json::from_value(json).unwrap();
        let info = band_names_from_asset_v1_1_0(&asset, None).expect("should succeed");
        assert_eq!(info.asset_title, "My Asset");
        assert_eq!(info.band_names, vec!["My Asset"]);
    }

    #[test]
    fn test_band_names_from_asset_v1_1_0_multi_band() {
        let json = serde_json::json!({
            "href": "http://example.com/file.tif",
            "title": "True color image",
            "bands": [{"name": "B04"}, {"name": "B03"}, {"name": "B02"}]
        });
        let asset: stac::Asset = serde_json::from_value(json).unwrap();
        let info = band_names_from_asset_v1_1_0(&asset, None).expect("should succeed");
        // The real asset title is preserved separately from the band names; the
        // band name is NOT encoded into the title (no `True color image [B02]`).
        assert_eq!(info.asset_title, "True color image");
        assert_eq!(info.band_names, vec!["B04", "B03", "B02"]);
    }

    // -----------------------------------------------------------------------
    // rasterband_channel_for_dataset_band
    // -----------------------------------------------------------------------

    #[test]
    fn test_rasterband_channel_no_bands_no_required() {
        let json = serde_json::json!({
            "href": "http://example.com/file.tif"
        });
        let asset: stac::Asset = serde_json::from_value(json).unwrap();
        assert_eq!(rasterband_channel_for_dataset_band(&asset, None), Some(1));
    }

    #[test]
    fn test_rasterband_channel_no_bands_with_required() {
        // An asset without `bands` metadata (e.g. Sentinel-2 SCL) still maps to
        // channel 1 even when the mapping configures an explicit band name —
        // skipping would silently lose the band.
        let json = serde_json::json!({
            "href": "http://example.com/file.tif"
        });
        let asset: stac::Asset = serde_json::from_value(json).unwrap();
        assert_eq!(
            rasterband_channel_for_dataset_band(
                &asset,
                Some("Scene classification map (SCL) - 20m")
            ),
            Some(1)
        );
    }

    #[test]
    fn test_rasterband_channel_single_band_with_required_name() {
        // A single-band asset (e.g. a Landsat thermal TIFF with
        // `bands: [{name: "B10"}]`) must still resolve to channel 1 even when
        // the configured band name is the human-readable asset title instead of
        // the STAC short code.
        let json = serde_json::json!({
            "href": "http://example.com/file.tif",
            "bands": [{"name": "B10"}]
        });
        let asset: stac::Asset = serde_json::from_value(json).unwrap();
        assert_eq!(
            rasterband_channel_for_dataset_band(
                &asset,
                Some("Thermal Infrared 10.9 (band 10) - 100m")
            ),
            Some(1)
        );
    }

    #[test]
    fn test_rasterband_channel_with_bands_matching() {
        let json = serde_json::json!({
            "href": "http://example.com/file.tif",
            "bands": [{"name": "B04"}, {"name": "B03"}]
        });
        let asset: stac::Asset = serde_json::from_value(json).unwrap();
        assert_eq!(
            rasterband_channel_for_dataset_band(&asset, Some("B04")),
            Some(1)
        );
        assert_eq!(
            rasterband_channel_for_dataset_band(&asset, Some("B03")),
            Some(2)
        );
    }

    #[test]
    fn test_rasterband_channel_with_bands_not_matching() {
        let json = serde_json::json!({
            "href": "http://example.com/file.tif",
            "bands": [{"name": "B04"}, {"name": "B03"}]
        });
        let asset: stac::Asset = serde_json::from_value(json).unwrap();
        assert_eq!(
            rasterband_channel_for_dataset_band(&asset, Some("B08")),
            None
        );
    }

    // -----------------------------------------------------------------------
    // gdal_config_options
    // -----------------------------------------------------------------------

    #[test]
    fn test_gdal_config_options_for_s3_empty() {
        let options = gdal_config_options_for_s3(None);
        assert!(options.is_empty());
    }

    #[test]
    fn test_gdal_config_options_for_s3_with_config() {
        let config = StacProviderS3Config {
            endpoint: "eodata.example.com".to_string(),
            access_key: Some("key".to_string()),
            secret_key: Some("secret".to_string()),
        };
        let options = gdal_config_options_for_s3(Some(&config));
        assert!(options.contains(&(
            "AWS_S3_ENDPOINT".to_string(),
            "eodata.example.com".to_string()
        )));
        assert!(options.contains(&("AWS_ACCESS_KEY_ID".to_string(), "key".to_string())));
        assert!(options.contains(&("AWS_SECRET_ACCESS_KEY".to_string(), "secret".to_string())));
        assert!(options.contains(&("AWS_VIRTUAL_HOSTING".to_string(), "FALSE".to_string())));
    }

    #[test]
    fn test_gdal_config_options_for_file_path_http_no_retries() {
        let options = gdal_config_options_for_file_path(
            std::path::Path::new("https://example.com/file.tif"),
            None,
            None,
        )
        .expect("http path should produce options");
        assert!(!options.iter().any(|(k, _)| k == "GDAL_HTTP_MAX_RETRY"));
        assert!(options.contains(&(
            "GDAL_DISABLE_READDIR_ON_OPEN".to_string(),
            "EMPTY_DIR".to_string()
        )));
    }

    #[test]
    fn test_gdal_config_options_for_file_path_with_retries() {
        let options = gdal_config_options_for_file_path(
            std::path::Path::new("https://example.com/file.tif"),
            None,
            Some(3),
        )
        .expect("http path should produce options");
        assert!(options.contains(&("GDAL_HTTP_MAX_RETRY".to_string(), "3".to_string())));
        assert!(options.contains(&("GDAL_HTTP_RETRY_DELAY".to_string(), "5".to_string())));
    }

    #[test]
    fn test_gdal_config_options_for_file_path_local_path_returns_none() {
        assert_eq!(
            gdal_config_options_for_file_path(std::path::Path::new("/data/file.tif"), None, None),
            None
        );
    }

    // -----------------------------------------------------------------------
    // data_type_from_asset_v1_1_0
    // -----------------------------------------------------------------------

    #[test]
    fn test_data_type_from_asset_v1_1_0_uint16() {
        let json = serde_json::json!({
            "href": "http://example.com/file.tif",
            "data_type": "uint16"
        });
        let asset: stac::Asset = serde_json::from_value(json).unwrap();
        assert_eq!(
            data_type_from_asset_v1_1_0(&asset),
            Some(RasterDataType::U16)
        );
    }

    #[test]
    fn test_data_type_from_asset_v1_1_0_missing() {
        let json = serde_json::json!({
            "href": "http://example.com/file.tif"
        });
        let asset: stac::Asset = serde_json::from_value(json).unwrap();
        assert_eq!(data_type_from_asset_v1_1_0(&asset), None);
    }
}
