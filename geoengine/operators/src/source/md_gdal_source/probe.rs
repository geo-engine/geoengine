use std::path::{Path, PathBuf};

use chrono::NaiveDateTime;
use gdal::{
    Dataset, DatasetOptions, GdalOpenFlags,
    raster::{Group, MDArray},
};
use geoengine_datatypes::{
    primitives::{CacheHint, Measurement, TimeInstance, TimeInterval},
    raster::{GeoTransform, GridBoundingBox2D, RasterDataType},
    spatial_reference::SpatialReference,
};

use crate::engine::{
    RasterBandDescriptor, RasterBandDescriptors, RasterResultDescriptor, SpatialGridDescriptor,
    TimeDescriptor,
};
use crate::source::gdal_worker_process::{
    FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters,
};
use crate::source::md_gdal_source::{MdDatasetFile, MdGdalSourceError, MdLoadingInfo, ZRole};

/// The outcome of probing one or more MD arrays: ready-to-use metadata plus result descriptor.
#[derive(Debug)]
pub struct ProbedMdGdalMetaData {
    pub loading_info: MdLoadingInfo,
    pub result_descriptor: RasterResultDescriptor,
}

/// Selects which multidimensional arrays (CF data variables) of a file/group become
/// Geo Engine bands. Each selected variable maps to one band; within a variable the z
/// dimension is the (shared) time axis.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MdArraySelection {
    /// Path to a GDAL MD group below the root group, "/"-separated; `None` = root group.
    pub group: Option<String>,
    /// Explicit variable names; the order defines the band order. Empty = auto-select all
    /// qualifying data variables (sorted by name for determinism).
    pub arrays: Vec<String>,
}

/// Probes the MD arrays at `paths` and derives the `MdLoadingInfo` (z timeline, per-file
/// z ranges, exact wrap/role detection) and a `RasterResultDescriptor` for `MdGdalSource`.
///
/// All files must expose the same array: same name (if `array_name` is given), same XY grid,
/// same data type. Files are ordered by their detected z coordinate values. Overlapping z
/// ranges are rejected; gaps between files become gaps in the global time line.
///
/// `array_name` selects the array to probe; `None` requires exactly one array with at least
/// three dimensions in the files (the other arrays, e.g. coordinate variables, are ignored).
pub fn probe_md_loading_info(
    paths: &[PathBuf],
    array_name: Option<&str>,
) -> Result<ProbedMdGdalMetaData, MdGdalSourceError> {
    let array_name = match array_name {
        Some(name) => name.to_owned(),
        None => auto_array_name(&paths[0])?,
    };

    let mut probed_arrays: Vec<ProbedArray> = Vec::with_capacity(paths.len());
    let mut first: Option<ProbedArray> = None;
    for path in paths {
        let dataset = open_md_dataset(path)?;
        let root_group = dataset
            .root_group()
            .map_err(|e| MdGdalSourceError::ProbeError {
                message: format!("cannot open root group of {}: {e}", path.display()),
            })?;
        let md_array = root_group
            .open_md_array(&array_name, Default::default())
            .map_err(|e| MdGdalSourceError::ProbeError {
                message: format!(
                    "cannot open MD array '{array_name}' in {}: {e}",
                    path.display()
                ),
            })?;

        let probed = probed_array(&md_array, path, &array_name)?;

        if let Some(first) = &first {
            validate_same_grid(first, &probed)?;
        } else {
            first = Some(probed.clone());
        }
        probed_arrays.push(probed);
    }
    let first = first.ok_or(MdGdalSourceError::ProbeError {
        message: "no files to probe".to_owned(),
    })?;

    // order files by their detected z start, then concatenate the time line in that order
    probed_arrays.sort_by_key(|p| p.time_intervals[0].start().inner());

    let mut global_time_steps =
        Vec::with_capacity(probed_arrays.iter().map(|p| p.time_intervals.len()).sum());
    let mut files = Vec::with_capacity(probed_arrays.len());
    for p in probed_arrays {
        let z_start = global_time_steps.len();
        global_time_steps.extend(p.time_intervals.iter().copied());
        files.push(MdDatasetFile {
            params: p.dataset_parameters.clone(),
            array_name: array_name.clone(),
            z_start,
            z_end: z_start + p.z_coordinates.len(),
            time: global_time_steps[z_start],
            band: 0,
        });
    }

    let loading_info = MdLoadingInfo::new(
        global_time_steps,
        files,
        CacheHint::default(),
        first.z_role,
        first.wrap,
    );

    let result_descriptor = result_descriptor(&first, &loading_info);

    Ok(ProbedMdGdalMetaData {
        loading_info,
        result_descriptor,
    })
}

/// Just enough of one probed array to derive the metadata, prior to cross-file aggregation.
#[derive(Debug, Clone)]
struct ProbedArray {
    path: PathBuf,
    array_name: String,
    z_role: ZRole,
    wrap: bool,
    /// global z coordinates of the array (detected times or unit indices)
    z_coordinates: Vec<f64>,
    /// one `TimeInterval` per z slice
    time_intervals: Vec<TimeInterval>,
    /// pixel size and shape of the stored XY grid
    data_type: RasterDataType,
    x_size: usize,
    y_size: usize,
    /// world latitude of the top pixel edge of the grid (north-up presentation)
    top_edge_y: f64,
    /// absolute y pixel size (the descriptor always presents north-up)
    y_pixel_size_abs: f64,
    /// CF display name: `long_name` attr, else `standard_name`, else the variable name
    display_name: String,
    /// CF `unit` of the array (empty if unset)
    unit: String,
    dataset_parameters: GdalDatasetParameters,
}

fn open_md_dataset(path: &Path) -> Result<Dataset, MdGdalSourceError> {
    crate::util::gdal::gdal_open_dataset_ex(
        path,
        DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_RASTER | GdalOpenFlags::GDAL_OF_MULTIDIM_RASTER,
            ..Default::default()
        },
    )
    .map_err(|e| MdGdalSourceError::ProbeError {
        message: format!("cannot open {} as MD dataset: {e}", path.display()),
    })
}

fn probed_array(
    md_array: &MDArray<'_>,
    path: &Path,
    array_name: &str,
) -> Result<ProbedArray, MdGdalSourceError> {
    let dimensions = md_array
        .dimensions()
        .map_err(|e| MdGdalSourceError::ProbeError {
            message: format!(
                "cannot read dimensions of '{array_name}' in {}: {e}",
                path.display()
            ),
        })?;

    if dimensions.len() < 3 {
        return Err(MdGdalSourceError::ProbeError {
            message: format!(
                "MD array '{array_name}' needs at least 3 dimensions (z, y, x), got {}: {}",
                dimensions.len(),
                path.display()
            ),
        });
    }
    let leading = &dimensions[..dimensions.len() - 3];
    if leading.iter().any(|d| d.size() != 1) {
        return Err(MdGdalSourceError::ProbeError {
            message: format!(
                "MD array '{array_name}' has non-singleton leading dimensions (unsupported layout): {}",
                path.display()
            ),
        });
    }

    let z_dim = &dimensions[dimensions.len() - 3];
    let y_dim = &dimensions[dimensions.len() - 2];
    let x_dim = &dimensions[dimensions.len() - 1];
    let x_size = x_dim.size();
    let y_size = y_dim.size();

    let z_coordinates = read_coordinates(z_dim, array_name, path, "z")?;
    let x_coordinates = read_coordinates(x_dim, array_name, path, "x")?;
    let y_coordinates = read_coordinates(y_dim, array_name, path, "y")?;

    require_uniform(&x_coordinates, "x", array_name, path)?;
    require_uniform(&y_coordinates, "y", array_name, path)?;

    let data_type = {
        let gdal_type: gdal::raster::GdalDataType = md_array
            .datatype()
            .numeric_datatype()
            .try_into()
            .map_err(|e| MdGdalSourceError::ProbeError {
                message: format!("unsupported numeric data type of array '{array_name}': {e}"),
            })?;
        RasterDataType::from_gdal_data_type(gdal_type).map_err(|e| {
            MdGdalSourceError::ProbeError {
                message: format!("unsupported GDAL data type of array '{array_name}': {e}"),
            }
        })?
    };

    let z_units = units_of(z_dim);
    let x_units = units_of(x_dim);
    let geographic = is_geographic(md_array, &x_units);

    let xy = xy_geometry(
        array_name,
        path,
        geographic,
        &x_coordinates,
        &y_coordinates,
        x_size,
    )?;

    let (z_role, time_intervals) = z_role_and_intervals(&z_units, &z_coordinates)?;

    Ok(ProbedArray {
        path: path.to_path_buf(),
        array_name: array_name.to_owned(),
        z_role,
        wrap: xy.wrap,
        z_coordinates,
        time_intervals,
        data_type,
        x_size,
        y_size,
        top_edge_y: xy.top_edge_y,
        y_pixel_size_abs: xy.y_pixel_size_abs,
        display_name: string_attr(md_array, "long_name")
            .or_else(|| string_attr(md_array, "standard_name"))
            .unwrap_or_else(|| array_name.to_owned()),
        unit: md_array.unit(),
        dataset_parameters: GdalDatasetParameters {
            file_path: path.to_path_buf(),
            rasterband_channel: 1,
            geo_transform: xy.geo_transform,
            width: x_size,
            height: y_size,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: md_array.no_data_value_as_double(),
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: false,
            retry: None,
        },
    })
}

/// XY geometry derived from the x/y coordinate values (cell centers).
struct XyGeometry {
    geo_transform: GdalDatasetGeoTransform,
    /// world latitude of the north (top) pixel edge
    top_edge_y: f64,
    y_pixel_size_abs: f64,
    wrap: bool,
}

/// The geo transform describes the raw array index space (plan §B): the origin is the
/// *pixel edge* of array cell (0, 0) — coordinate variables are cell centers — and the
/// y pixel size keeps the sign of the row direction (CF lat ascending ⇒ positive).
fn xy_geometry(
    array_name: &str,
    path: &Path,
    geographic: bool,
    x_coordinates: &[f64],
    y_coordinates: &[f64],
    x_size: usize,
) -> Result<XyGeometry, MdGdalSourceError> {
    let delta_x = spacing(x_coordinates);
    if delta_x <= 0.0 {
        return Err(MdGdalSourceError::ProbeError {
            message: format!(
                "x coordinates of array '{array_name}' must be ascending, got spacing {delta_x}: {}",
                path.display()
            ),
        });
    }
    let wrap = geographic && is_0_360_wrap(x_coordinates, delta_x);
    if wrap && !x_size.is_multiple_of(2) {
        return Err(MdGdalSourceError::ProbeError {
            message: format!(
                "wrapped array '{array_name}' has odd width {x_size}, needs an even width: {}",
                path.display()
            ),
        });
    }

    let delta_y = spacing(y_coordinates);
    let y_pixel_size_abs = delta_y.abs();
    let origin_x = x_coordinates[0] - delta_x / 2.0;
    let (origin_y, y_pixel_size) = if delta_y < 0.0 {
        (y_coordinates[0] + y_pixel_size_abs / 2.0, -y_pixel_size_abs)
    } else {
        (y_coordinates[0] - y_pixel_size_abs / 2.0, y_pixel_size_abs)
    };
    // north-up top edge of the grid (world latitude of the first presented row)
    let top_edge_y = if delta_y < 0.0 {
        y_coordinates[0] + y_pixel_size_abs / 2.0
    } else {
        *y_coordinates
            .last()
            .expect("checked uniform, at least two values")
            + y_pixel_size_abs / 2.0
    };

    Ok(XyGeometry {
        geo_transform: GdalDatasetGeoTransform {
            origin_coordinate: (origin_x, origin_y).into(),
            x_pixel_size: delta_x,
            y_pixel_size,
        },
        top_edge_y,
        y_pixel_size_abs,
        wrap,
    })
}

enum ZRoleAndIntervals {
    Time(Vec<TimeInterval>),
    Band,
}

/// Map the z dimension to either time intervals (CF units) or band indices.
fn z_role_and_intervals(
    z_units: &str,
    z_coordinates: &[f64],
) -> Result<(ZRole, Vec<TimeInterval>), MdGdalSourceError> {
    let zr = match z_role_from_units(z_units) {
        Some(TimeUnitInfo { factor_ms, origin }) => ZRoleAndIntervals::Time(
            times_from_coordinates(z_coordinates, z_units_tag(z_units), factor_ms, origin)?,
        ),
        None => ZRoleAndIntervals::Band,
    };
    Ok(match zr {
        ZRoleAndIntervals::Time(intervals) => (ZRole::Time, intervals),
        ZRoleAndIntervals::Band => (
            ZRole::Band,
            z_coordinates
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    TimeInterval::new(i as i64, i as i64 + 1).expect("valid unit interval")
                })
                .collect(),
        ),
    })
}

struct TimeUnitInfo {
    factor_ms: f64,
    origin: NaiveDateTime,
}

fn z_role_from_units(units: &str) -> Option<TimeUnitInfo> {
    // CF convention: "<unit> since <origin>", e.g. "days since 2000-01-01 00:00:00"
    let (unit, origin_str) = units.split_once(" since ")?;
    let factor_ms = match unit {
        "seconds" => 1_000.0,
        "minutes" => 60_000.0,
        "hours" => 3_600_000.0,
        "days" => 86_400_000.0,
        _ => return None,
    };
    let origin = parse_cf_origin(origin_str)?;
    Some(TimeUnitInfo { factor_ms, origin })
}

fn parse_cf_origin(input: &str) -> Option<NaiveDateTime> {
    const FORMATS: &[&str] = &[
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ];
    let input = input.trim().trim_end_matches(['Z', 'z']);
    FORMATS
        .iter()
        .find_map(|fmt| NaiveDateTime::parse_from_str(input, fmt).ok())
}

fn times_from_coordinates(
    z_coordinates: &[f64],
    units_tag: &str,
    factor_ms: f64,
    origin: NaiveDateTime,
) -> Result<Vec<TimeInterval>, MdGdalSourceError> {
    let origin_ms = origin.and_utc().timestamp_millis() as f64;
    let stamps = z_coordinates
        .iter()
        .map(|&c| TimeInstance::from_millis((origin_ms + c * factor_ms) as i64))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| MdGdalSourceError::ProbeError {
            message: format!("invalid time coordinate for units `{units_tag}`: {e}"),
        })?;

    if stamps.len() > 1 && stamps.windows(2).any(|w| w[0] >= w[1]) {
        return Err(MdGdalSourceError::ProbeError {
            message: format!(
                "time coordinates for units `{units_tag}` must be strictly increasing"
            ),
        });
    }
    let delta_ms = if stamps.len() > 1 {
        (stamps[stamps.len() - 1].inner() - stamps[0].inner()) as f64 / (stamps.len() - 1) as f64
    } else {
        factor_ms
    };

    let mut intervals = Vec::with_capacity(stamps.len());
    // one interval per stamp: [t_j, t_{j+1}) and a final [t_n, t_n + spacing)
    for w in stamps.windows(2) {
        intervals.push(TimeInterval::new(w[0], w[1]).map_err(|e| {
            MdGdalSourceError::ProbeError {
                message: format!("invalid time interval for units `{units_tag}`: {e}"),
            }
        })?);
    }
    if let Some(last) = stamps.last() {
        let end = TimeInstance::from_millis(last.inner() + delta_ms as i64).map_err(|e| {
            MdGdalSourceError::ProbeError {
                message: format!("invalid time end for units `{units_tag}`: {e}"),
            }
        })?;
        intervals.push(TimeInterval::new(*last, end).map_err(|e| {
            MdGdalSourceError::ProbeError {
                message: format!("invalid time interval for units `{units_tag}`: {e}"),
            }
        })?);
    }
    Ok(intervals)
}

fn z_units_tag(units: &str) -> &str {
    units.split_once(" since ").map_or(units, |(u, _)| u)
}

fn read_coordinates(
    dim: &gdal::raster::Dimension<'_>,
    array_name: &str,
    path: &Path,
    axis: &str,
) -> Result<Vec<f64>, MdGdalSourceError> {
    let index_var = dim.indexing_variable();
    let n = index_var.num_elements() as usize;
    index_var
        .read_as::<f64>(vec![0], vec![n])
        .map_err(|e| MdGdalSourceError::ProbeError {
            message: format!(
                "cannot read {axis} coordinate values of array '{array_name}' in {}: {e}",
                path.display()
            ),
        })
}

fn units_of(dim: &gdal::raster::Dimension<'_>) -> String {
    dim.indexing_variable()
        .attribute("units")
        .ok()
        .map(|a| a.read_as_string())
        .unwrap_or_default()
}

/// Reads a string attribute (`long_name`, `standard_name`, ...) of an MD array.
fn string_attr(md_array: &MDArray<'_>, name: &str) -> Option<String> {
    md_array
        .attribute(name)
        .ok()
        .map(|a| a.read_as_string())
        .filter(|s| !s.is_empty())
}

fn is_geographic(md_array: &MDArray<'_>, x_units: &str) -> bool {
    // ponytail: `md_array.spatial_reference()` panics in gdal 0.19 when the array has no
    // sref (the netCDF driver never sets one for MD arrays), so only CF x units are checked.
    let _ = md_array;
    x_units.starts_with("degrees") || x_units.starts_with("degree")
}

/// True if the x coordinates describe a 0..360 longitude coverage with constant pixel size
/// whose extent equals 360° exactly (so `width/2` is a valid column shift).
fn is_0_360_wrap(x_coordinates: &[f64], delta_x: f64) -> bool {
    if delta_x <= 0.0 {
        return false;
    }
    // the stored grid covers [0°, 360°) in pixel edges: first center = Δ/2, last center = 360 − Δ/2
    let first_edge = x_coordinates[0] - delta_x / 2.0;
    let last_edge = x_coordinates[x_coordinates.len() - 1] + delta_x / 2.0;
    let tol = 1e-3 * delta_x;
    first_edge.abs() <= tol && (last_edge - 360.0).abs() <= tol
}

fn spacing(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 1.0;
    }
    (values[values.len() - 1] - values[0]) / (values.len() - 1) as f64
}

fn require_uniform(
    values: &[f64],
    axis: &str,
    array_name: &str,
    path: &Path,
) -> Result<(), MdGdalSourceError> {
    if values.len() < 2 {
        return Ok(());
    }
    let delta = spacing(values);
    if delta == 0.0 {
        return Err(MdGdalSourceError::ProbeError {
            message: format!(
                "{axis} coordinate values must not be constant: {}",
                path.display()
            ),
        });
    }
    let tol = 1e-4 * delta.abs();
    if values
        .windows(2)
        .any(|w| ((w[1] - w[0]) - delta).abs() > tol)
    {
        return Err(MdGdalSourceError::ProbeError {
            message: format!(
                "{axis} coordinate values of array '{array_name}' are not uniformly spaced: {}",
                path.display()
            ),
        });
    }
    Ok(())
}

fn validate_same_grid(a: &ProbedArray, b: &ProbedArray) -> Result<(), MdGdalSourceError> {
    if a.data_type != b.data_type
        || a.x_size != b.x_size
        || a.y_size != b.y_size
        || a.dataset_parameters.geo_transform != b.dataset_parameters.geo_transform
    {
        return Err(MdGdalSourceError::ProbeError {
            message: format!(
                "MD arrays '{a_array}' in {a_path} and '{b_name}' in {b_path} must share the same grid and data type",
                a_array = a.array_name,
                a_path = a.path.display(),
                b_name = b.array_name,
                b_path = b.path.display(),
            ),
        });
    }
    Ok(())
}

fn auto_array_name(path: &Path) -> Result<String, MdGdalSourceError> {
    let dataset = open_md_dataset(path)?;
    let root_group = dataset
        .root_group()
        .map_err(|e| MdGdalSourceError::ProbeError {
            message: format!("cannot open root group of {}: {e}", path.display()),
        })?;
    let names = root_group.array_names(Default::default());
    let candidates = names
        .into_iter()
        .filter(|name| {
            root_group
                .open_md_array(name, Default::default())
                .is_ok_and(|a| a.num_dimensions() >= 3)
        })
        .collect::<Vec<_>>();
    match candidates.len() {
        1 => Ok(candidates[0].clone()),
        0 => Err(MdGdalSourceError::ProbeError {
            message: format!(
                "no MD array with at least 3 dimensions found in {}",
                path.display()
            ),
        }),
        n => Err(MdGdalSourceError::ProbeError {
            message: format!(
                "{n} MD arrays with at least 3 dimensions found in {}; pass an explicit array name",
                path.display()
            ),
        }),
    }
}

/// Descends into the (possibly nested) GDAL MD group `path` ("/"-separated);
/// `None` or empty keeps the root group.
fn into_group<'d>(root: Group<'d>, path: Option<&str>) -> Result<Group<'d>, MdGdalSourceError> {
    let mut group = root;
    for segment in path
        .filter(|p| !p.is_empty())
        .into_iter()
        .flat_map(|p| p.split('/'))
    {
        group = group.open_group(segment, Default::default()).map_err(|e| {
            let p = path.unwrap_or_default();
            MdGdalSourceError::ProbeError {
                message: format!("cannot open MD group '{p}': {e}"),
            }
        })?;
    }
    Ok(group)
}

/// The variable names to probe, in band order: the explicit list, or all qualifying
/// data arrays in the group (>= 3 dims, z dimension with CF time units — which excludes
/// coordinate variables and band-like arrays), sorted by name for determinism.
fn resolve_variable_names(
    path: &Path,
    selection: &MdArraySelection,
) -> Result<Vec<String>, MdGdalSourceError> {
    if !selection.arrays.is_empty() {
        return Ok(selection.arrays.clone());
    }
    let dataset = open_md_dataset(path)?;
    let root_group = dataset
        .root_group()
        .map_err(|e| MdGdalSourceError::ProbeError {
            message: format!("cannot open root group of {}: {e}", path.display()),
        })?;
    let group = into_group(root_group, selection.group.as_deref())?;
    let mut candidates: Vec<String> = group
        .array_names(Default::default())
        .into_iter()
        .filter(|name| {
            group
                .open_md_array(name, Default::default())
                .is_ok_and(|a| {
                    a.dimensions().is_ok_and(|dims| {
                        dims.len() >= 3
                            && z_role_from_units(&units_of(&dims[dims.len() - 3])).is_some()
                    })
                })
        })
        .collect();
    candidates.sort();
    if candidates.is_empty() {
        return Err(MdGdalSourceError::ProbeError {
            message: format!(
                "no MD time-series data variables (>= 3 dims, CF time units on the leading dim) found in {}",
                path.display()
            ),
        });
    }
    Ok(candidates)
}

/// The band descriptor for one selected variable: name from CF attributes
/// (`long_name` → `standard_name` → variable name), unit as continuous measurement.
fn band_descriptor(probed: &ProbedArray) -> RasterBandDescriptor {
    let measurement = if probed.unit.is_empty() {
        Measurement::Unitless
    } else {
        Measurement::continuous(probed.display_name.clone(), Some(probed.unit.clone()))
    };
    RasterBandDescriptor::new(probed.display_name.clone(), measurement)
}

/// Probes MULTIPLE MD variables (CF data variables) and maps each selected variable to
/// one Geo Engine band (`ZRole::Variable`). All variables must share the same XY grid,
/// data type and time axis; files may additionally be split along time and are then
/// concatenated per variable. Every selected variable must have CF time units on its z
/// dimension (time-series layout only).
///
/// The `selection` chooses the group (`None` = root) and the variables (explicit list in
/// band order, or auto-select all qualifying data variables, name-sorted).
pub fn probe_md_variables_loading_info(
    paths: &[PathBuf],
    selection: &MdArraySelection,
) -> Result<ProbedMdGdalMetaData, MdGdalSourceError> {
    if paths.is_empty() {
        return Err(MdGdalSourceError::ProbeError {
            message: "no files to probe".to_owned(),
        });
    }

    let names = resolve_variable_names(&paths[0], selection)?;
    let mut variables: Vec<(String, Vec<ProbedArray>)> =
        names.into_iter().map(|n| (n, Vec::new())).collect();

    // open each file once, probe every selected variable in it
    for path in paths {
        let dataset = open_md_dataset(path)?;
        let root_group = dataset
            .root_group()
            .map_err(|e| MdGdalSourceError::ProbeError {
                message: format!("cannot open root group of {}: {e}", path.display()),
            })?;
        let group = into_group(root_group, selection.group.as_deref())?;

        for (name, probes) in &mut variables {
            let md_array = group.open_md_array(name, Default::default()).map_err(|e| {
                MdGdalSourceError::ProbeError {
                    message: format!("cannot open MD array '{name}' in {}: {e}", path.display()),
                }
            })?;
            let probed = probed_array(&md_array, path, name)?;
            if probed.z_role != ZRole::Time {
                return Err(MdGdalSourceError::ProbeError {
                    message: format!(
                        "variable '{name}' in {} has no CF time dimension; only time-series variables can map to bands",
                        path.display()
                    ),
                });
            }
            if let Some(first) = probes.first() {
                validate_same_grid(first, &probed)?;
            }
            probes.push(probed);
        }
    }

    let mut files = Vec::new();
    let mut shared_times: Option<Vec<TimeInterval>> = None;
    let mut reference: Option<ProbedArray> = None;
    let mut bands = Vec::with_capacity(variables.len());

    for (b, (name, mut probes)) in variables.into_iter().enumerate() {
        probes.sort_by_key(|p| p.time_intervals[0].start().inner());

        // cross-variable consistency: same grid, same wrap detection
        if let Some(ref reference) = reference {
            validate_same_grid(reference, &probes[0])?;
            if reference.wrap != probes[0].wrap {
                return Err(MdGdalSourceError::ProbeError {
                    message: format!("variable '{name}' must agree on the wrap-around detection"),
                });
            }
        } else {
            reference = Some(probes[0].clone());
        }

        let mut times = Vec::new();
        for p in &probes {
            let z_start = times.len();
            times.extend(p.time_intervals.iter().copied());
            files.push(MdDatasetFile {
                params: p.dataset_parameters.clone(),
                array_name: name.clone(),
                z_start,
                z_end: z_start + p.z_coordinates.len(),
                time: times[z_start],
                band: b as u32,
            });
        }

        if let Some(shared) = &shared_times {
            if shared != &times {
                return Err(MdGdalSourceError::ProbeError {
                    message: format!(
                        "variable '{name}' must share the same time axis as the other selected variables"
                    ),
                });
            }
        } else {
            shared_times = Some(times.clone());
        }

        bands.push(band_descriptor(&probes[0]));
    }

    let reference = reference.ok_or_else(|| MdGdalSourceError::ProbeError {
        message: "no variables selected".to_owned(),
    })?;
    let time_steps = shared_times.ok_or_else(|| MdGdalSourceError::ProbeError {
        message: "no variables selected".to_owned(),
    })?;

    let loading_info = MdLoadingInfo::new(
        time_steps,
        files,
        CacheHint::default(),
        ZRole::Variable,
        reference.wrap,
    );
    let bands = RasterBandDescriptors::new(bands).map_err(|e| MdGdalSourceError::ProbeError {
        message: format!("invalid band descriptors: {e}"),
    })?;
    let result_descriptor = result_descriptor_with_bands(&reference, bands);

    Ok(ProbedMdGdalMetaData {
        loading_info,
        result_descriptor,
    })
}

/// Builds the north-up `RasterResultDescriptor` for the probed grid with the given bands.
///
/// The descriptor always presents north-up (negative y pixel size); the stored array
/// may be south-up (ascending lat), which the read advise compensates via `flip_y`.
/// For a wrapped array the presented origin is shifted by -180 (longitude 0..360 stored,
/// -180..180 presented).
fn result_descriptor_with_bands(
    probed: &ProbedArray,
    bands: RasterBandDescriptors,
) -> RasterResultDescriptor {
    let origin_x = if probed.wrap {
        probed.dataset_parameters.geo_transform.origin_coordinate.x - 180.0
    } else {
        probed.dataset_parameters.geo_transform.origin_coordinate.x
    };
    let geo_transform = GeoTransform::new(
        (origin_x, probed.top_edge_y).into(),
        probed.dataset_parameters.geo_transform.x_pixel_size,
        -probed.y_pixel_size_abs,
    );
    let spatial_grid = SpatialGridDescriptor::source_from_parts(
        geo_transform,
        GridBoundingBox2D::new_unchecked(
            [0, 0],
            [probed.y_size as isize - 1, probed.x_size as isize - 1],
        ),
    );

    let time = TimeDescriptor::new_irregular(probed.time_intervals.first().copied());

    RasterResultDescriptor::new(
        probed.data_type,
        SpatialReference::epsg_4326().into(),
        time,
        spatial_grid,
        bands,
    )
}

/// The default band descriptors for a single-variable (`Time`/`Band`) MD array.
fn default_bands(probed: &ProbedArray) -> RasterBandDescriptors {
    match probed.z_role {
        ZRole::Time | ZRole::Variable => RasterBandDescriptors::new_single_band(),
        ZRole::Band => RasterBandDescriptors::new_multiple_bands(probed.z_coordinates.len() as u32),
    }
}

fn result_descriptor(
    probed: &ProbedArray,
    _loading_info: &MdLoadingInfo,
) -> RasterResultDescriptor {
    result_descriptor_with_bands(probed, default_bands(probed))
}

#[cfg(test)]
#[allow(clippy::float_cmp)] // exact values: half-pixel offsets are exactly representable f64
mod tests {
    use geoengine_datatypes::{primitives::Measurement, raster::RasterDataType, test_data};

    use super::MdArraySelection;
    use crate::source::{
        MdGdalSourceError, ZRole, probe_md_loading_info, probe_md_variables_loading_info,
    };

    fn expect(path: &str) -> super::ProbedMdGdalMetaData {
        probe_md_loading_info(&[test_data!(path).to_path_buf()], None).unwrap()
    }

    #[test]
    fn probe_time_series_nc() {
        let m = expect("md/time_series.nc");
        let li = &m.loading_info;
        assert_eq!(li.z_role(), ZRole::Time);
        assert!(!li.wrap());
        assert_eq!(li.time_steps().len(), 8);
        assert_eq!(li.files().len(), 1);
        assert_eq!(li.files()[0].z_start, 0);
        assert_eq!(li.files()[0].z_end, 8);
        assert_eq!(li.files()[0].params.width, 8);
        assert_eq!(li.files()[0].params.height, 8);
        assert_eq!(li.files()[0].params.geo_transform.origin_coordinate.x, 0.0);
        assert_eq!(li.files()[0].params.geo_transform.origin_coordinate.y, 0.0);
        assert_eq!(li.files()[0].params.geo_transform.x_pixel_size, 30.0);
        assert_eq!(li.files()[0].params.geo_transform.y_pixel_size, -1.0);
        // 2000-01-01 = 946_684_800_000 ms
        assert_eq!(li.time_steps()[0].start().inner(), 946_684_800_000);
        assert_eq!(
            li.time_steps()[1].start().inner(),
            946_684_800_000 + 86_400_000
        );
        assert_eq!(m.result_descriptor.data_type, RasterDataType::F32);
        assert_eq!(
            m.result_descriptor
                .spatial_grid_descriptor()
                .geo_transform()
                .origin_coordinate
                .x,
            0.0
        );
        assert_eq!(
            m.result_descriptor
                .spatial_grid_descriptor()
                .geo_transform()
                .origin_coordinate
                .y,
            0.0
        );
        assert_eq!(
            m.result_descriptor
                .spatial_grid_descriptor()
                .geo_transform()
                .y_pixel_size(),
            -1.0
        );
    }

    #[test]
    fn probe_time_series_zarr() {
        let m = expect("md/time_series.zarr");
        let li = &m.loading_info;
        assert_eq!(li.z_role(), ZRole::Time);
        assert_eq!(li.time_steps().len(), 8);
        assert_eq!(li.files().len(), 1);
        assert_eq!(li.files()[0].z_start, 0);
        assert_eq!(li.files()[0].z_end, 8);
    }

    #[test]
    fn probe_split_files() {
        // provide files in reverse order; probe must sort by time
        let m = probe_md_loading_info(
            &[
                test_data!("md/time_series_split_b.nc").to_path_buf(),
                test_data!("md/time_series_split_a.nc").to_path_buf(),
            ],
            None,
        )
        .unwrap();
        let li = &m.loading_info;
        assert_eq!(li.time_steps().len(), 8);
        assert_eq!(li.files().len(), 2);
        // first file is split_a (t 0..4)
        assert_eq!(li.files()[0].z_start, 0);
        assert_eq!(li.files()[0].z_end, 4);
        assert!(
            li.files()[0]
                .params
                .file_path
                .to_str()
                .unwrap()
                .ends_with("split_a.nc")
        );
        assert_eq!(li.files()[1].z_start, 4);
        assert_eq!(li.files()[1].z_end, 8);
        // continuous across files
        assert_eq!(
            li.time_steps()[3].start().inner(),
            946_684_800_000 + 3 * 86_400_000
        );
        assert_eq!(
            li.time_steps()[4].start().inner(),
            946_684_800_000 + 4 * 86_400_000
        );
    }

    #[test]
    fn probe_wrap_0_360() {
        let m = expect("md/wrap_0_360.nc");
        let li = &m.loading_info;
        assert!(li.wrap());
        assert_eq!(li.z_role(), ZRole::Time);
        assert_eq!(li.time_steps().len(), 4);
        assert_eq!(li.files().len(), 1);
        assert_eq!(li.files()[0].z_start, 0);
        assert_eq!(li.files()[0].z_end, 4);
        assert_eq!(li.files()[0].params.width, 360);
        assert_eq!(li.files()[0].params.height, 10);
        assert_eq!(li.files()[0].params.geo_transform.origin_coordinate.x, 0.0);
        assert_eq!(li.files()[0].params.geo_transform.origin_coordinate.y, 45.0);
        assert_eq!(li.files()[0].params.geo_transform.x_pixel_size, 1.0);
        assert_eq!(li.files()[0].params.geo_transform.y_pixel_size, -1.0);
        // descriptor origin shifted by −180, north-up presentation
        assert_eq!(
            m.result_descriptor
                .spatial_grid_descriptor()
                .geo_transform()
                .origin_coordinate
                .x,
            -180.0
        );
        assert_eq!(
            m.result_descriptor
                .spatial_grid_descriptor()
                .geo_transform()
                .origin_coordinate
                .y,
            45.0
        );
    }

    #[test]
    fn probe_bands() {
        let m = expect("md/bands.nc");
        let li = &m.loading_info;
        assert_eq!(li.z_role(), ZRole::Band);
        assert_eq!(li.time_steps().len(), 4);
        assert_eq!(li.files()[0].z_start, 0);
        assert_eq!(li.files()[0].z_end, 4);
        assert_eq!(m.result_descriptor.bands.len(), 4);
    }

    #[test]
    fn probe_cf_time_units_minutes() {
        let m = expect("md/cf_time_units_minutes.nc");
        let li = &m.loading_info;
        assert_eq!(li.z_role(), ZRole::Time);
        assert_eq!(li.time_steps().len(), 8);
        // 1900-01-01T00:00:00Z = -2_208_988_800_000 ms
        assert_eq!(li.time_steps()[0].start().inner(), -2_208_988_800_000);
        assert_eq!(
            li.time_steps()[1].start().inner(),
            -2_208_988_800_000 + 60_000
        );
        // ascending latitudes: params describe the array index space (row 0 = south) ...
        assert_eq!(li.files()[0].params.geo_transform.origin_coordinate.y, -8.0);
        assert_eq!(li.files()[0].params.geo_transform.y_pixel_size, 1.0);
        // ... while the descriptor presents north-up (top edge = 0°)
        let gt = m
            .result_descriptor
            .spatial_grid_descriptor()
            .geo_transform();
        assert_eq!(gt.origin_coordinate.y, 0.0);
        assert_eq!(gt.y_pixel_size(), -1.0);
    }

    #[test]
    fn probe_zarr() {
        let m = probe_md_loading_info(&[test_data!("md/time_series.zarr").to_path_buf()], None)
            .unwrap();
        let li = &m.loading_info;
        assert_eq!(li.z_role(), ZRole::Time);
        assert_eq!(li.time_steps().len(), 8);
    }

    #[test]
    fn probe_rejects_wrong_array_name() {
        let err = probe_md_loading_info(
            &[test_data!("md/time_series.nc").to_path_buf()],
            Some("nonexistent"),
        )
        .unwrap_err();
        match err {
            MdGdalSourceError::ProbeError { message } => {
                assert!(message.contains("cannot open MD array"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn probe_variables_auto_selects_all_in_time_series() {
        // no explicit names -> all 3 data variables, auto-selected and name-sorted:
        // band 0 = cloud_area_fraction (no attrs -> name/unitless)
        // band 1 = precipitation (long_name "precipitation", unit "mm")
        // band 2 = temperature    (long_name "air temperature", unit "K")
        let m = probe_md_variables_loading_info(
            &[test_data!("md/variables.nc").to_path_buf()],
            &MdArraySelection::default(),
        )
        .unwrap();
        let li = &m.loading_info;
        assert_eq!(li.z_role(), ZRole::Variable);
        assert_eq!(li.time_steps().len(), 8);
        // 3 variables, each covering the full time axis as a single file
        assert_eq!(li.files().len(), 3);
        assert_eq!(li.files()[0].array_name, "cloud_area_fraction");
        assert_eq!(li.files()[1].array_name, "precipitation");
        assert_eq!(li.files()[2].array_name, "temperature");
        for (b, f) in li.files().iter().enumerate() {
            assert_eq!(f.band, b as u32);
            assert_eq!((f.z_start, f.z_end), (0, 8));
        }
        let names: Vec<_> = m
            .result_descriptor
            .bands
            .bands()
            .iter()
            .map(|b| b.name.clone())
            .collect();
        assert_eq!(
            names,
            ["cloud_area_fraction", "precipitation", "air temperature"]
        );
    }

    #[test]
    fn probe_variables_explicit_order_and_measurements() {
        // explicit list defines band order; the value multipliers follow the source order
        let selection = MdArraySelection {
            group: None,
            arrays: vec!["temperature".to_string(), "precipitation".to_string()],
        };
        let m = probe_md_variables_loading_info(
            &[test_data!("md/variables.nc").to_path_buf()],
            &selection,
        )
        .unwrap();
        let li = &m.loading_info;
        assert_eq!(li.files().len(), 2);
        assert_eq!(li.files()[0].array_name, "temperature");
        assert_eq!(li.files()[0].band, 0);
        assert_eq!(li.files()[1].array_name, "precipitation");
        assert_eq!(li.files()[1].band, 1);
        let bands = m.result_descriptor.bands.bands();
        assert_eq!(bands[0].name, "air temperature");
        assert_eq!(bands[1].name, "precipitation");
        assert_eq!(
            bands[1].measurement,
            Measurement::continuous("precipitation".into(), Some("mm".into()))
        );
    }

    #[test]
    fn probe_variables_in_subgroup() {
        let selection = MdArraySelection {
            group: Some("analysis".to_string()),
            arrays: vec![],
        };
        let m = probe_md_variables_loading_info(
            &[test_data!("md/grouped_variables.nc").to_path_buf()],
            &selection,
        )
        .unwrap();
        assert_eq!(m.loading_info.files().len(), 3);
        assert_eq!(m.result_descriptor.bands.count(), 3);
    }

    #[test]
    fn probe_variables_rejects_group_without_arrays() {
        // grouped_variables.nc has no arrays at the root -> nothing to select
        let err = probe_md_variables_loading_info(
            &[test_data!("md/grouped_variables.nc").to_path_buf()],
            &MdArraySelection::default(),
        )
        .unwrap_err();
        match err {
            MdGdalSourceError::ProbeError { message } => {
                assert!(message.contains("no MD time-series data variables"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }
}
