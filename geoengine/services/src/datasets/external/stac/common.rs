//! Common utilities shared between the STAC provider and the STAC harvester CLI.
//!
//! This module contains functions for parsing STAC metadata (geometry, EPSG, data types,
//! band names), STAC API query helpers, and GDAL file path handling.
//!
//! All functions herein should be usable from both `loading_info.rs` (the STAC provider)
//! and `cli/stac_harvester.rs` (the new STAC harvester CLI).

#![allow(dead_code)]

use geoengine_datatypes::{
    raster::{GdalGeoTransform, GeoTransform, RasterDataType},
    spatial_reference::SpatialReference,
};
use serde::Deserialize;
use std::path::PathBuf;

use super::StacProviderS3Config;

// ---------------------------------------------------------------------------
// STAC extension version types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StacExtensionMajorVersion {
    V1,
    V2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StacExtensionVersions {
    pub projection: StacExtensionMajorVersion,
    pub raster: StacExtensionMajorVersion,
    pub eo: StacExtensionMajorVersion,
}

/// Parse STAC extension versions from a list of extension URIs.
///
/// Looks for extensions hosted under `https://stac-extensions.github.io/{name}/v{version}/schema.json`.
pub fn parse_stac_extension_versions(
    extensions: &[String],
) -> Result<StacExtensionVersions, String> {
    let mut projection = None;
    let mut raster = None;
    let mut eo = None;

    for name in ["projection", "raster", "eo"] {
        if let Some(ext_str) = extensions
            .iter()
            .find(|ext| ext.starts_with(&format!("https://stac-extensions.github.io/{name}")))
        {
            let version = stac_extension_version_from_str(ext_str, name)
                .map_err(|e| format!("Failed to parse version for {name} extension: {e}"))?;
            match name {
                "projection" => projection = Some(version),
                "raster" => raster = Some(version),
                "eo" => eo = Some(version),
                _ => unreachable!(),
            }
        }
    }

    Ok(StacExtensionVersions {
        projection: projection.ok_or_else(|| "Missing projection extension".to_string())?,
        raster: raster.ok_or_else(|| "Missing raster extension".to_string())?,
        eo: eo.ok_or_else(|| "Missing eo extension".to_string())?,
    })
}

/// Infer STAC extension versions from a collection's actual fields, falling back
/// when the extension URIs are missing (e.g. OpenGeoHub-style collections).
pub fn infer_collection_extension_versions(
    collection: &stac::Collection,
) -> Result<StacExtensionVersions, String> {
    match collection.version {
        stac::Version::v1_0_0 => {
            let has_projection = collection.item_assets.values().any(|asset| {
                asset.additional_fields.contains_key("proj:transform")
                    || asset.additional_fields.contains_key("proj:shape")
                    || asset.additional_fields.contains_key("proj:epsg")
            });
            let has_raster = collection
                .item_assets
                .values()
                .any(|asset| asset.additional_fields.contains_key("raster:bands"));
            let has_eo = collection
                .item_assets
                .values()
                .any(|asset| asset.additional_fields.contains_key("eo:bands"));

            Ok(StacExtensionVersions {
                projection: has_projection
                    .then_some(StacExtensionMajorVersion::V1)
                    .ok_or_else(|| "Missing projection extension".to_string())?,
                raster: has_raster
                    .then_some(StacExtensionMajorVersion::V1)
                    .ok_or_else(|| "Missing raster extension".to_string())?,
                eo: has_eo
                    .then_some(StacExtensionMajorVersion::V1)
                    .ok_or_else(|| "Missing eo extension".to_string())?,
            })
        }
        stac::Version::v1_1_0 => {
            // In STAC 1.1.0, projection extension is v2.0.0,
            // raster:bands/eo:bands are merged into common metadata `bands`,
            // and `data_type`/`nodata`/`gsd` are common metadata fields.
            //
            // Projection/raster/eo info is often only present at the item
            // level, not in collection-level item_assets. The collection
            // scan functions don't use the extension versions for data
            // extraction — they only need to know which scan function to
            // call. So we default all to V2 for any STAC 1.1.0 collection.
            Ok(StacExtensionVersions {
                projection: StacExtensionMajorVersion::V2,
                raster: StacExtensionMajorVersion::V2,
                eo: StacExtensionMajorVersion::V2,
            })
        }
        _ => Err(format!(
            "Cannot infer extension versions for STAC {}",
            collection.version
        )),
    }
}

impl TryFrom<&stac::Collection> for StacExtensionVersions {
    type Error = String;

    fn try_from(collection: &stac::Collection) -> Result<Self, Self::Error> {
        parse_stac_extension_versions(&collection.extensions)
            .or_else(|_| infer_collection_extension_versions(collection))
    }
}

impl TryFrom<&stac::Item> for StacExtensionVersions {
    type Error = String;

    fn try_from(item: &stac::Item) -> Result<Self, Self::Error> {
        parse_stac_extension_versions(&item.extensions).or_else(|_| {
            if matches!(
                item.version,
                stac::Version::v1_1_0 | stac::Version::Unknown(_)
            ) && !item.assets.is_empty()
            {
                return Ok(StacExtensionVersions {
                    projection: StacExtensionMajorVersion::V2,
                    raster: StacExtensionMajorVersion::V2,
                    eo: StacExtensionMajorVersion::V2,
                });
            }
            Err("Missing raster extension".to_string())
        })
    }
}

/// Parse the major version from a STAC extension URL like
/// `https://stac-extensions.github.io/projection/v1.0.0/schema.json`.
pub fn stac_extension_version_from_str(
    extension_str: &str,
    extension_name: &str,
) -> Result<StacExtensionMajorVersion, String> {
    let prefix = format!("https://stac-extensions.github.io/{extension_name}/v");
    let version_str = extension_str
        .strip_prefix(&prefix)
        .and_then(|rem| rem.strip_suffix("/schema.json"))
        .ok_or_else(|| format!("Unknown version for extension {extension_name}"))?;

    match version_str.split('.').next() {
        Some("1") => Ok(StacExtensionMajorVersion::V1),
        Some("2") => Ok(StacExtensionMajorVersion::V2),
        _ => Err(format!(
            "Unknown version '{version_str}' for extension {extension_name}"
        )),
    }
}

// ---------------------------------------------------------------------------
// Geometry / transform helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// File path helpers
// ---------------------------------------------------------------------------

/// Convert a STAC asset href (HTTP or S3 URL) to a GDAL VSI path.
///
/// - `http://...` → `/vsicurl/http://...`
/// - `s3://bucket/key` → `/vsis3/bucket/key`
pub fn gdal_file_path(href: &str) -> Option<PathBuf> {
    if href.starts_with("http") {
        return Some(PathBuf::from(format!("/vsicurl/{href}")));
    }

    href.strip_prefix("s3://")
        .map(|s3_path| PathBuf::from(format!("/vsis3/{s3_path}")))
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
/// If the asset has bands, matches by `band_name` against asset band names.
/// Returns `None` if the required band is not found.
pub fn rasterband_channel_for_dataset_band(
    asset: &stac::Asset,
    required_band_name: Option<&str>,
) -> Option<usize> {
    if asset.bands.is_empty() {
        // Single-band asset without raster:bands — use channel 1 regardless of
        // whether a band name is specified, since there's only one band.
        return Some(1);
    }

    let Some(required_band_name) = required_band_name else {
        tracing::warn!(
            "STAC asset with href {} includes bands, but dataset configuration does not specify a band name. Skipping asset.",
            asset.href
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

/// Derive band names from a STAC 1.1.0 `Asset`, using the `bands` field.
pub fn band_names_from_asset_v1_1_0(asset: &stac::Asset) -> Result<Vec<String>, String> {
    let asset_title = asset
        .title
        .as_deref()
        .ok_or_else(|| "Missing title in asset metadata".to_string())?;

    let bands = &asset.bands;

    if bands.is_empty() {
        return Ok(vec![asset_title.to_string()]);
    }

    if bands.len() == 1 {
        return Ok(vec![asset_title.to_string()]);
    }

    let mut names = Vec::new();
    for band in bands {
        let Some(band_name) = &band.name else {
            return Err("Band is missing name for multi-band asset".to_string());
        };
        names.push(format!("{asset_title} [{band_name}]"));
    }

    Ok(names)
}

/// Derive band names from a STAC 1.1.0 `ItemAsset`, using the `bands` additional field.
pub fn band_names_from_item_asset_v1_1_0(asset: &stac::ItemAsset) -> Result<Vec<String>, String> {
    let asset_title = asset
        .title
        .as_deref()
        .ok_or_else(|| "Missing title in asset metadata".to_string())?;

    let band_names = asset
        .additional_fields
        .get("bands")
        .and_then(serde_json::Value::as_array);

    let Some(bands) = band_names else {
        return Ok(vec![asset_title.to_string()]);
    };

    if bands.is_empty() {
        return Ok(vec![asset_title.to_string()]);
    }

    if bands.len() == 1 {
        return Ok(vec![asset_title.to_string()]);
    }

    let mut names = Vec::new();
    for band in bands {
        let band_name = band
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "Band is missing name for multi-band asset".to_string())?;
        names.push(format!("{asset_title} [{band_name}]"));
    }

    Ok(names)
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

/// Check if a media type is a Cloud-Optimized GeoTIFF.
pub fn is_cog_media_type(media_type: Option<&str>) -> bool {
    media_type == Some("image/tiff; application=geotiff; profile=cloud-optimized")
}

/// Check if a media type is JPEG 2000.
pub fn is_jp2_media_type(media_type: Option<&str>) -> bool {
    media_type == Some("image/jp2")
}

// ---------------------------------------------------------------------------
// GDAL config options
// ---------------------------------------------------------------------------

/// Build GDAL configuration options for `/vsis3/` paths.
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

/// Build GDAL configuration options for a VSI file path, including common CURL/S3 options.
pub fn gdal_config_options_for_file_path(
    file_path: &std::path::Path,
    s3_config: Option<&StacProviderS3Config>,
) -> Option<Vec<(String, String)>> {
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

    if is_vsi_s3 {
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
            serde_json::json!([10.0, 0.0, 399960.0, 0.0, -10.0, 5700000.0]),
        );

        let gt = geo_transform_from_fields(&fields).expect("should parse transform");
        assert_eq!(gt.origin_coordinate.x, 399_960.0);
        assert_eq!(gt.origin_coordinate.y, 5_700_000.0);
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
    // gdal_file_path
    // -----------------------------------------------------------------------

    #[test]
    fn test_gdal_file_path_http() {
        let path = gdal_file_path("https://example.com/file.tif").expect("should parse");
        assert_eq!(path, PathBuf::from("/vsicurl/https://example.com/file.tif"));
    }

    #[test]
    fn test_gdal_file_path_s3() {
        let path = gdal_file_path("s3://bucket/key/file.tif").expect("should parse");
        assert_eq!(path, PathBuf::from("/vsis3/bucket/key/file.tif"));
    }

    #[test]
    fn test_gdal_file_path_unsupported() {
        assert!(gdal_file_path("/local/path.tif").is_none());
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
    // extension version parsing
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_stac_extension_versions_v1() {
        let extensions = vec![
            "https://stac-extensions.github.io/projection/v1.0.0/schema.json".to_string(),
            "https://stac-extensions.github.io/raster/v1.1.0/schema.json".to_string(),
            "https://stac-extensions.github.io/eo/v1.0.0/schema.json".to_string(),
        ];
        let versions = parse_stac_extension_versions(&extensions).expect("should parse");
        assert_eq!(versions.projection, StacExtensionMajorVersion::V1);
        assert_eq!(versions.raster, StacExtensionMajorVersion::V1);
        assert_eq!(versions.eo, StacExtensionMajorVersion::V1);
    }

    #[test]
    fn test_parse_stac_extension_versions_v2() {
        let extensions = vec![
            "https://stac-extensions.github.io/projection/v2.0.0/schema.json".to_string(),
            "https://stac-extensions.github.io/raster/v2.0.0/schema.json".to_string(),
            "https://stac-extensions.github.io/eo/v2.0.0/schema.json".to_string(),
        ];
        let versions = parse_stac_extension_versions(&extensions).expect("should parse");
        assert_eq!(versions.projection, StacExtensionMajorVersion::V2);
        assert_eq!(versions.raster, StacExtensionMajorVersion::V2);
        assert_eq!(versions.eo, StacExtensionMajorVersion::V2);
    }

    #[test]
    fn test_parse_stac_extension_versions_missing() {
        let extensions = vec![];
        assert!(parse_stac_extension_versions(&extensions).is_err());
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
        assert_eq!(normalize_label("  Blue  band  "), "blue_band");
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
        let names = band_names_from_asset_v1_1_0(&asset).expect("should succeed");
        assert_eq!(names, vec!["My Band"]);
    }

    #[test]
    fn test_band_names_from_asset_v1_1_0_single_band() {
        let json = serde_json::json!({
            "href": "http://example.com/file.tif",
            "title": "My Asset",
            "bands": [{"name": "B01"}]
        });
        let asset: stac::Asset = serde_json::from_value(json).unwrap();
        let names = band_names_from_asset_v1_1_0(&asset).expect("should succeed");
        assert_eq!(names, vec!["My Asset"]);
    }

    #[test]
    fn test_band_names_from_asset_v1_1_0_multi_band() {
        let json = serde_json::json!({
            "href": "http://example.com/file.tif",
            "title": "Sentinel-2",
            "bands": [{"name": "B04"}, {"name": "B03"}, {"name": "B02"}]
        });
        let asset: stac::Asset = serde_json::from_value(json).unwrap();
        let names = band_names_from_asset_v1_1_0(&asset).expect("should succeed");
        assert_eq!(
            names,
            vec!["Sentinel-2 [B04]", "Sentinel-2 [B03]", "Sentinel-2 [B02]",]
        );
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
        let json = serde_json::json!({
            "href": "http://example.com/file.tif"
        });
        let asset: stac::Asset = serde_json::from_value(json).unwrap();
        assert_eq!(
            rasterband_channel_for_dataset_band(&asset, Some("B04")),
            None
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

    // -----------------------------------------------------------------------
    // STAC extension version from string
    // -----------------------------------------------------------------------

    #[test]
    fn test_stac_extension_version_from_str_v1() {
        let version = stac_extension_version_from_str(
            "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
            "projection",
        )
        .expect("should parse");
        assert_eq!(version, StacExtensionMajorVersion::V1);
    }

    #[test]
    fn test_stac_extension_version_from_str_v2() {
        let version = stac_extension_version_from_str(
            "https://stac-extensions.github.io/raster/v2.0.0/schema.json",
            "raster",
        )
        .expect("should parse");
        assert_eq!(version, StacExtensionMajorVersion::V2);
    }

    #[test]
    fn test_stac_extension_version_from_str_invalid() {
        assert!(stac_extension_version_from_str("invalid", "test").is_err());
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
