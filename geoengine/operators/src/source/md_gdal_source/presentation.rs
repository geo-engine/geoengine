use geoengine_datatypes::{
    primitives::{AxisAlignedRectangle, SpatialPartition2D},
    raster::{GeoTransform, GridBoundingBox2D, GridBounds, GridSize, SpatialGridDefinition},
};

use super::error::MdGdalSourceError;
use crate::source::gdal_worker_process::{
    GdalDatasetGeoTransform, GdalDatasetParameters, GdalReaderMode,
    process_common::{GdalReadAdvise, GdalReadWindow},
};

/// How a stored MD grid is presented to queries.
///
/// Stored metadata always holds the raw file values (south-up y, 0..360° longitudes). Every
/// transform from raw to presented lives in this type, so the stored values never change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MdPresentation {
    /// the stored grid as it is
    Raw,
    /// the stored 0..360° longitudes are shown as −180..180°: presented longitude `p` shows
    /// stored longitude `p mod 360`
    Wrap180,
}

const WORLD_WIDTH: f64 = 360.0;
const EPS: f64 = 1e-6;

impl MdPresentation {
    #[must_use]
    pub fn from_wrap(wrap: bool) -> Self {
        if wrap { Self::Wrap180 } else { Self::Raw }
    }

    /// Rejects a raw grid that this presentation cannot show.
    ///
    /// # Errors
    /// Returns `UnpresentableGrid` if a wrapped grid is not 360° wide, is not on the
    /// pixel lattice of −180°, or has an odd width.
    pub fn validate(self, raw: &GdalDatasetParameters) -> Result<(), MdGdalSourceError> {
        if self == Self::Raw {
            return Ok(());
        }
        let gt = raw.geo_transform;
        let px = gt.x_pixel_size.abs();
        let unpresentable = |message: String| MdGdalSourceError::UnpresentableGrid { message };

        if !raw.width.is_multiple_of(2) {
            return Err(unpresentable(format!(
                "a wrapped grid needs an even width, found {}",
                raw.width
            )));
        }
        let span = raw.width as f64 * px;
        if (span - WORLD_WIDTH).abs() > EPS {
            return Err(unpresentable(format!(
                "a wrapped grid must span 360°, found {span}°"
            )));
        }
        let offset = (gt.origin_coordinate.x + 180.0) / px;
        if (offset - offset.round()).abs() > EPS {
            return Err(unpresentable(format!(
                "the origin longitude {} is not on the pixel lattice of −180°",
                gt.origin_coordinate.x
            )));
        }
        Ok(())
    }

    /// The transform the raw grid is presented as. The y axis is always north-up.
    #[must_use]
    pub fn presented_geo_transform(
        self,
        raw: GdalDatasetGeoTransform,
        height: usize,
    ) -> GeoTransform {
        let origin_x = match self {
            Self::Raw => raw.origin_coordinate.x,
            Self::Wrap180 => -180.0,
        };
        GeoTransform::new(
            (origin_x, north_edge(raw, height)).into(),
            raw.x_pixel_size,
            -raw.y_pixel_size.abs(),
        )
    }

    /// The grid the raw grid is presented as: the same pixels under the presented transform.
    #[must_use]
    pub fn presented_grid(self, raw: &SpatialGridDefinition) -> SpatialGridDefinition {
        let gt = raw.geo_transform();
        let raw_gt = GdalDatasetGeoTransform {
            origin_coordinate: gt.origin_coordinate(),
            x_pixel_size: gt.x_pixel_size(),
            y_pixel_size: gt.y_pixel_size(),
        };
        let height = raw.grid_bounds().axis_size_y();
        SpatialGridDefinition::new(
            self.presented_geo_transform(raw_gt, height),
            raw.grid_bounds(),
        )
    }

    /// The stored partitions that hold the presented partition `query`. A presented window
    /// that crosses the stored seam (origin + 360°) is split into two stored partitions.
    #[must_use]
    pub fn raw_partitions(
        self,
        raw: &SpatialGridDefinition,
        query: SpatialPartition2D,
    ) -> Vec<SpatialPartition2D> {
        let (ul, lr) = (query.upper_left(), query.lower_right());
        match self {
            Self::Raw => vec![query],
            Self::Wrap180 => {
                let origin = raw.geo_transform().origin_coordinate().x;
                let len = (lr.x - ul.x).min(WORLD_WIDTH);
                let start = origin + (ul.x - origin).rem_euclid(WORLD_WIDTH);
                let end = start + len;
                let runs = if end <= origin + WORLD_WIDTH {
                    vec![(start, end)]
                } else {
                    vec![(start, origin + WORLD_WIDTH), (origin, end - WORLD_WIDTH)]
                };
                runs.into_iter()
                    .map(|(x0, x1)| {
                        SpatialPartition2D::new_unchecked((x0, ul.y).into(), (x1, lr.y).into())
                    })
                    .collect()
            }
        }
    }

    /// The read windows that fill `tile` from the raw grid. Empty if the tile does not reach
    /// the presented extent.
    #[must_use]
    pub fn read_advises(
        self,
        raw: &GdalDatasetParameters,
        tile: &SpatialGridDefinition,
        reader_mode: GdalReaderMode,
    ) -> Vec<GdalReadAdvise> {
        match self {
            Self::Raw => reader_mode
                .tiling_to_dataset_read_advise(&raw.spatial_grid_definition(), tile)
                .into_iter()
                .collect(),
            Self::Wrap180 => wrap_read_advises(raw, tile),
        }
    }
}

/// The y coordinate of the north edge of the raw grid.
fn north_edge(raw: GdalDatasetGeoTransform, height: usize) -> f64 {
    if raw.y_pixel_size < 0.0 {
        raw.origin_coordinate.y
    } else {
        raw.origin_coordinate.y + height as f64 * raw.y_pixel_size.abs()
    }
}

/// Read windows for a wrapped grid. Presented columns are the tile lattice columns of
/// −180..180°; presented column `c` shows stored column `(c − col0) mod width`, where `col0`
/// is the lattice column of the stored origin. A run stops at the stored array edge, so a
/// tile that crosses world longitude 0° (or 180°) is split into two stored runs.
fn wrap_read_advises(
    raw: &GdalDatasetParameters,
    tile: &SpatialGridDefinition,
) -> Vec<GdalReadAdvise> {
    let gt = raw.geo_transform;
    let width = raw.width as isize;
    let height = raw.height as isize;
    let tile_origin = tile.geo_transform();
    let tile_bounds = tile.grid_bounds();

    let lattice_x =
        |lon: f64| ((lon - tile_origin.origin_coordinate.x) / gt.x_pixel_size).round() as isize;
    let col0 = lattice_x(gt.origin_coordinate.x);
    let presented_start = lattice_x(-180.0);
    let ady = gt.y_pixel_size.abs();
    let row0 =
        ((tile_origin.origin_coordinate.y - north_edge(gt, raw.height)) / ady).round() as isize;

    let t_min = tile_bounds.min_index();
    let t_max = tile_bounds.max_index();
    let q0 = t_min.x().max(presented_start);
    let q1 = t_max.x().min(presented_start + width - 1);
    let r0 = t_min.y().max(row0);
    let r1 = t_max.y().min(row0 + height - 1);
    if q0 > q1 || r0 > r1 {
        return vec![];
    }

    let size_y = (r1 - r0 + 1) as usize;
    // stored rows in raw array order; `flip_y` lets the pool turn them back north-up
    let (start_y, flip_y) = if gt.y_pixel_size < 0.0 {
        (r0 - row0, false)
    } else {
        (height - 1 - (r1 - row0), true)
    };

    let mut advises = Vec::new();
    let mut a = q0;
    while a <= q1 {
        let start_x = (a - col0).rem_euclid(width);
        let len = (width - start_x).min(q1 - a + 1);
        let b = a + len - 1;
        advises.push(GdalReadAdvise {
            gdal_read_widow: GdalReadWindow {
                start_x,
                start_y,
                size_x: len as usize,
                size_y,
            },
            read_window_bounds: GridBoundingBox2D::new_unchecked([r0, a], [r1, b]),
            bounds_of_target: tile_bounds,
            flip_y,
        });
        a = b + 1;
    }
    advises
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::gdal_worker_process::FileNotFoundHandling;

    fn params(
        origin: (f64, f64),
        pixel: (f64, f64),
        width: usize,
        height: usize,
    ) -> GdalDatasetParameters {
        GdalDatasetParameters {
            file_path: "/test.nc".into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: origin.into(),
                x_pixel_size: pixel.0,
                y_pixel_size: pixel.1,
            },
            width,
            height,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: None,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: false,
            retry: None,
        }
    }

    fn tile(pixel_size: f64, bounds: ([isize; 2], [isize; 2])) -> SpatialGridDefinition {
        SpatialGridDefinition::new(
            GeoTransform::new((0., 0.).into(), pixel_size, -pixel_size),
            GridBoundingBox2D::new_unchecked(bounds.0, bounds.1),
        )
    }

    /// the 0..360° fixture grid: stored lon 0..360, lat 45..35, presented −180..180
    fn wrap_params() -> GdalDatasetParameters {
        params((0., 45.), (1., -1.), 360, 10)
    }

    #[test]
    fn wrap_tile_west_of_seam() {
        // tile (−1, −1): covers world cols −512..−1 -> presented cols −180..−1 = stored 180..359
        let advises = wrap_read_advises(&wrap_params(), &tile(1.0, ([-512, -512], [-1, -1])));
        assert_eq!(advises.len(), 1);
        let w = advises[0].gdal_read_widow;
        assert_eq!((w.start_x, w.size_x), (180, 180));
        assert_eq!((w.start_y, w.size_y), (0, 10));
        assert_eq!(advises[0].read_window_bounds.min_index().x(), -180);
        assert_eq!(advises[0].read_window_bounds.max_index().x(), -1);
        assert!(!advises[0].flip_y);
    }

    #[test]
    fn wrap_tile_east_of_seam() {
        // tile (0, −1): covers world cols 0..511 -> presented cols 0..179 = stored 0..179
        let advises = wrap_read_advises(&wrap_params(), &tile(1.0, ([-512, 0], [-1, 511])));
        assert_eq!(advises.len(), 1);
        let w = advises[0].gdal_read_widow;
        assert_eq!((w.start_x, w.size_x), (0, 180));
        assert_eq!((w.start_y, w.size_y), (0, 10));
        assert_eq!(advises[0].read_window_bounds.min_index().x(), 0);
        assert_eq!(advises[0].read_window_bounds.max_index().x(), 179);
    }

    #[test]
    fn wrap_tile_straddling_seam_splits() {
        // a narrow tile crossing world lon 0° splits into two stored runs: lon −10..9 is stored
        // 350..369, lon 10 is stored 10
        let advises = wrap_read_advises(&wrap_params(), &tile(1.0, ([-45, -10], [-36, 10])));
        assert_eq!(advises.len(), 2);
        let (first, second) = (advises[0].gdal_read_widow, advises[1].gdal_read_widow);
        assert_eq!((first.start_x, first.size_x), (350, 10));
        assert_eq!((second.start_x, second.size_x), (0, 11));
        assert_eq!(advises[1].read_window_bounds.min_index().x(), 0);
        assert_eq!(advises[1].read_window_bounds.max_index().x(), 10);
    }

    #[test]
    fn wrap_tile_outside_presented_extent_is_empty() {
        let advises = wrap_read_advises(&wrap_params(), &tile(1.0, ([-45, 200], [-36, 300])));
        assert!(advises.is_empty());
    }

    #[test]
    fn wrap_ascending_y_reads_array_rows_and_flips() {
        // ascending latitudes (row 0 = south), 0..360 stored grid: edges lon 0..360, lat -5..5
        let p = params((0., -5.), (1., 1.), 360, 10);
        let tile_grid = SpatialGridDefinition::new(
            GeoTransform::new((0., 0.).into(), 1.0, -1.0),
            GridBoundingBox2D::new_unchecked([-512, 0], [-1, 511]),
        );
        let advises = wrap_read_advises(&p, &tile_grid);
        assert_eq!(advises.len(), 1);
        let w = advises[0].gdal_read_widow;
        // world cols 0..179 = stored 0..179; presented rows -5..-1 are array rows 5..9
        // (array row 0 = south) -> read in array order and flip back
        assert_eq!((w.start_x, w.size_x), (0, 180));
        assert_eq!((w.start_y, w.size_y), (5, 5));
        assert!(advises[0].flip_y);
        assert_eq!(advises[0].read_window_bounds.min_index().y(), -5);
        assert_eq!(advises[0].read_window_bounds.max_index().y(), -1);
    }

    #[test]
    fn wrap_non_zero_origin_shifts_the_seam() {
        // stored lon 10..370 (a whole grid column ahead of 0°): presented lon −180..−1 is
        // stored 170..349 and lon −10..9 straddles the stored edge at 370°
        let p = params((10., 45.), (1., -1.), 360, 10);
        let advises = wrap_read_advises(&p, &tile(1.0, ([-512, -180], [-1, -1])));
        assert_eq!(advises.len(), 1);
        assert_eq!(
            (
                advises[0].gdal_read_widow.start_x,
                advises[0].gdal_read_widow.size_x
            ),
            (170, 180)
        );

        let advises = wrap_read_advises(&p, &tile(1.0, ([-45, -10], [-36, 10])));
        assert_eq!(advises.len(), 2);
        assert_eq!(
            (
                advises[0].gdal_read_widow.start_x,
                advises[0].gdal_read_widow.size_x
            ),
            (340, 20)
        );
        assert_eq!(
            (
                advises[1].gdal_read_widow.start_x,
                advises[1].gdal_read_widow.size_x
            ),
            (0, 1)
        );
    }

    #[test]
    fn wrap_presents_the_grid_from_minus_180_whatever_the_stored_origin() {
        // south-up, lat −5..5: the presented north edge is 5°, the origin is at −180°
        let p = params((0., -5.), (1., 1.), 360, 10);
        let presented = MdPresentation::Wrap180.presented_geo_transform(p.geo_transform, p.height);
        assert_eq!(presented.origin_coordinate(), (-180., 5.).into());
        assert!((presented.x_pixel_size() - 1.).abs() < f64::EPSILON);
        assert!((presented.y_pixel_size() + 1.).abs() < f64::EPSILON);

        // a grid stored a whole column ahead is presented from the same −180, same bounds
        let raw = SpatialGridDefinition::new(
            GeoTransform::new((10., 45.).into(), 1., -1.),
            GridBoundingBox2D::new_unchecked([0, 0], [9, 359]),
        );
        let presented = MdPresentation::Wrap180.presented_grid(&raw);
        assert!((presented.geo_transform().origin_coordinate().x + 180.).abs() < f64::EPSILON);
        assert_eq!(presented.grid_bounds(), raw.grid_bounds());
    }

    #[test]
    fn wrap_splits_a_presented_window_at_the_stored_seam() {
        let raw = SpatialGridDefinition::new(
            GeoTransform::new((0., 45.).into(), 1., -1.),
            GridBoundingBox2D::new_unchecked([0, 0], [9, 359]),
        );
        let query = SpatialPartition2D::new_unchecked((-10., 45.).into(), (10., 35.).into());
        let parts = MdPresentation::Wrap180.raw_partitions(&raw, query);
        assert_eq!(parts.len(), 2);
        assert_eq!(
            (parts[0].upper_left().x, parts[0].lower_right().x),
            (350., 360.)
        );
        assert_eq!(
            (parts[1].upper_left().x, parts[1].lower_right().x),
            (0., 10.)
        );
        assert_eq!(MdPresentation::Raw.raw_partitions(&raw, query), vec![query]);
    }

    #[test]
    fn wrap_validation_rejects_what_it_cannot_show() {
        let validate = |p: &GdalDatasetParameters| MdPresentation::Wrap180.validate(p);
        assert!(validate(&wrap_params()).is_ok());
        assert!(validate(&params((0., 45.), (1., -1.), 361, 10)).is_err());
        assert!(validate(&params((0., 45.), (1., -1.), 180, 10)).is_err());
        assert!(validate(&params((0.5, 45.), (1., -1.), 360, 10)).is_err());
        // ERA5's 0.25° grid: origin −0.125 (0° is a pixel centre), half a pixel off the −180
        // lattice, so its 1440 columns have no whole-pixel wrap
        assert!(validate(&params((-0.125, 90.125), (0.25, -0.25), 1440, 721)).is_err());
        assert!(
            MdPresentation::Raw
                .validate(&params((0., 45.), (1., -1.), 361, 10))
                .is_ok()
        );
    }
}
