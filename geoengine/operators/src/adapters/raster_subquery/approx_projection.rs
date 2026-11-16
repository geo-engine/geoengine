//! An approximate coordinate projection for rows of output pixels, as GDAL's approximate
//! transformer (`GDALApproxTransform`, `gdalwarp -et`): it projects the start, the middle and the
//! end of a row exactly and interpolates linearly in between if the middle lies within
//! [`APPROX_ERROR_THRESHOLD_PX`] input pixels of the interpolation. Otherwise it splits the row
//! into halves and repeats. Rows of at most five coordinates are projected exactly.

use geoengine_datatypes::primitives::Coordinate2D;
use geoengine_datatypes::spatial_reference::CoordinateProjection;

/// The largest error of the approximate projection, in input pixels. GDAL's default for
/// `gdalwarp -et`, which makes the result differ from the exact projection only for output pixels
/// whose center lies within an eighth of an input pixel of an input pixel edge.
pub const APPROX_ERROR_THRESHOLD_PX: f64 = 0.125;

/// Rows of at most this many coordinates are projected exactly, as in GDAL
const MAX_EXACT_ROW_LEN: usize = 5;

/// Projects rows of coordinates approximately with `projector` into an input grid with
/// `in_pixel_size`.
pub struct ApproxProjection<'p, P> {
    projector: &'p P,
    /// the absolute pixel size of the input grid in x and y
    in_pixel_size: (f64, f64),
    max_error_px: f64,
}

impl<'p, P: CoordinateProjection> ApproxProjection<'p, P> {
    pub fn new(projector: &'p P, in_pixel_size: (f64, f64)) -> Self {
        Self {
            projector,
            in_pixel_size: (in_pixel_size.0.abs(), in_pixel_size.1.abs()),
            max_error_px: APPROX_ERROR_THRESHOLD_PX,
        }
    }

    /// Projects `row`, coordinates with the same y and increasing x, into `projected`, `None` for
    /// coordinates that cannot be projected.
    pub fn project_row(&self, row: &[Coordinate2D], projected: &mut [Option<Coordinate2D>]) {
        debug_assert_eq!(row.len(), projected.len());
        // the interpolation needs x to increase along the row
        if row.len() <= MAX_EXACT_ROW_LEN || row[row.len() - 1].x <= row[0].x {
            self.project_exactly(row, projected);
            return;
        }
        let ends = self
            .projector
            .project_coordinates([row[0], row[row.len() - 1]])
            .ok()
            .filter(|ends| ends.iter().all(is_finite));
        match ends.as_deref() {
            Some(&[start, end]) => self.project_segment(row, projected, start, end),
            _ => self.project_exactly(row, projected),
        }
    }

    /// Projects the segment `row` whose first and last coordinate project to `start` and `end`.
    fn project_segment(
        &self,
        row: &[Coordinate2D],
        projected: &mut [Option<Coordinate2D>],
        start: Coordinate2D,
        end: Coordinate2D,
    ) {
        let len = row.len();
        if len <= MAX_EXACT_ROW_LEN {
            self.project_exactly(&row[1..len - 1], &mut projected[1..len - 1]);
            projected[0] = Some(start);
            projected[len - 1] = Some(end);
            return;
        }

        let middle_index = (len - 1) / 2;
        let Some(middle) = self
            .projector
            .project_coordinate(row[middle_index])
            .ok()
            .filter(is_finite)
        else {
            self.project_exactly(row, projected);
            return;
        };

        let interpolate = |coordinate: Coordinate2D| {
            let fraction = (coordinate.x - row[0].x) / (row[len - 1].x - row[0].x);
            Coordinate2D::new(
                start.x + (end.x - start.x) * fraction,
                start.y + (end.y - start.y) * fraction,
            )
        };
        let interpolated_middle = interpolate(row[middle_index]);
        let error_px = (interpolated_middle.x - middle.x).abs() / self.in_pixel_size.0
            + (interpolated_middle.y - middle.y).abs() / self.in_pixel_size.1;

        if error_px <= self.max_error_px {
            for (projected, coordinate) in projected.iter_mut().zip(row) {
                *projected = Some(interpolate(*coordinate));
            }
            projected[0] = Some(start);
            projected[len - 1] = Some(end);
            return;
        }

        // the halves share the middle
        self.project_segment(
            &row[..=middle_index],
            &mut projected[..=middle_index],
            start,
            middle,
        );
        self.project_segment(
            &row[middle_index..],
            &mut projected[middle_index..],
            middle,
            end,
        );
    }

    fn project_exactly(&self, row: &[Coordinate2D], projected: &mut [Option<Coordinate2D>]) {
        if let Ok(coordinates) = self.projector.project_coordinates(row) {
            for (projected, coordinate) in projected.iter_mut().zip(coordinates) {
                *projected = Some(coordinate).filter(is_finite);
            }
            return;
        }
        for (projected, coordinate) in projected.iter_mut().zip(row) {
            *projected = self
                .projector
                .project_coordinate(*coordinate)
                .ok()
                .filter(is_finite);
        }
    }
}

fn is_finite(coordinate: &Coordinate2D) -> bool {
    coordinate.x.is_finite() && coordinate.y.is_finite()
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::spatial_reference::{DefaultCoordinateProjector, SpatialReference};

    /// The rows of a 512 × 512 tile of 1 km pixels in EPSG:3857 at about 70° N, projected into an
    /// EPSG:4326 grid of 0.1° pixels, deviate from the exact projection by at most the threshold
    #[test]
    fn it_stays_within_the_threshold() {
        let projector = DefaultCoordinateProjector::from_known_srs(
            SpatialReference::web_mercator(),
            SpatialReference::epsg_4326(),
        )
        .unwrap();
        let in_pixel_size = (0.1, 0.1);
        let approx = ApproxProjection::new(&projector, in_pixel_size);

        let mut max_error_px: f64 = 0.;
        let mut interpolated = 0;
        for row in 0..512 {
            let y = 11_000_000. - f64::from(row) * 1_000.;
            let coordinates: Vec<Coordinate2D> = (0..512)
                .map(|column| Coordinate2D::new(1_000_000. + f64::from(column) * 1_000. + 500., y))
                .collect();
            let mut projected = vec![None; coordinates.len()];
            approx.project_row(&coordinates, &mut projected);
            let exact = projector.project_coordinates(&coordinates).unwrap();
            for (approximate, exact) in projected.into_iter().zip(exact) {
                let approximate = approximate.unwrap();
                if approximate != exact {
                    interpolated += 1;
                }
                max_error_px = max_error_px.max(
                    (approximate.x - exact.x).abs() / in_pixel_size.0
                        + (approximate.y - exact.y).abs() / in_pixel_size.1,
                );
            }
        }

        assert!(
            max_error_px <= APPROX_ERROR_THRESHOLD_PX,
            "the error {max_error_px} exceeds the threshold"
        );
        assert!(interpolated > 0, "no coordinate was interpolated");
    }

    #[test]
    fn it_projects_short_rows_exactly() {
        let projector = DefaultCoordinateProjector::from_known_srs(
            SpatialReference::web_mercator(),
            SpatialReference::epsg_4326(),
        )
        .unwrap();
        let approx = ApproxProjection::new(&projector, (0.1, 0.1));
        let row: Vec<Coordinate2D> = (0..5)
            .map(|x| Coordinate2D::new(f64::from(x) * 100_000., 8_000_000.))
            .collect();
        let mut projected = vec![None; row.len()];

        approx.project_row(&row, &mut projected);

        let exact = projector.project_coordinates(&row).unwrap();
        assert_eq!(projected, exact.into_iter().map(Some).collect::<Vec<_>>());
    }
}
