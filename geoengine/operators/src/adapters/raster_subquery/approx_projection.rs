//! An approximate coordinate projection for rows of output pixels, as GDAL's approximate
//! transformer (`GDALApproxTransform`, `gdalwarp -et`): it projects the start, the middle and the
//! end of a row exactly and interpolates linearly in between if the middle lies within a threshold
//! of input pixels of the interpolation. Otherwise it splits the row into halves and repeats. Rows of at most five coordinates, and all
//! rows with a threshold of 0, are projected exactly.

use geoengine_datatypes::primitives::Coordinate2D;
use geoengine_datatypes::raster::GeoTransform;
use geoengine_datatypes::spatial_reference::CoordinateProjection;
use geoengine_datatypes::util::{NonNegativeFiniteF64, PositiveFiniteF64};

/// Rows of at most this many coordinates are projected exactly, as in GDAL
const MAX_EXACT_ROW_LEN: usize = 5;

/// Projects rows of coordinates approximately with `projector` into an input grid.
pub struct ApproxProjection<'p, P> {
    projector: &'p P,
    /// the absolute pixel size of the input grid in x and y
    in_pixel_size: (PositiveFiniteF64, PositiveFiniteF64),
    /// the largest error in input pixels, 0 to project exactly
    max_error_px: NonNegativeFiniteF64,
}

impl<'p, P: CoordinateProjection> ApproxProjection<'p, P> {
    /// Into the input grid of `in_geo_transform`, with an error of at most `max_error_px` input
    /// pixels, as `gdalwarp -et`; 0 projects every coordinate exactly
    pub fn new(
        projector: &'p P,
        in_geo_transform: &GeoTransform,
        max_error_px: NonNegativeFiniteF64,
    ) -> Self {
        Self {
            projector,
            in_pixel_size: (
                in_geo_transform.x_pixel_size().abs().into(),
                in_geo_transform.y_pixel_size().abs().into(),
            ),
            max_error_px,
        }
    }

    /// Projects `row`, coordinates with the same y and increasing x, into `projected`, `None` for
    /// coordinates that cannot be projected.
    pub fn project_row(&self, row: &[Coordinate2D], projected: &mut [Option<Coordinate2D>]) {
        debug_assert_eq!(row.len(), projected.len());
        // the interpolation needs x to increase along the row
        if self.max_error_px.get() == 0.
            || row.len() <= MAX_EXACT_ROW_LEN
            || row[row.len() - 1].x <= row[0].x
        {
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
        let error_px = (interpolated_middle.x - middle.x).abs() / self.in_pixel_size.0.get()
            + (interpolated_middle.y - middle.y).abs() / self.in_pixel_size.1.get();

        if error_px <= self.max_error_px.get() {
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

    /// GDAL's default for `gdalwarp -et`
    const THRESHOLD: NonNegativeFiniteF64 = NonNegativeFiniteF64::new_unchecked(0.125);

    /// An EPSG:4326 grid of 0.1° pixels
    fn grid() -> GeoTransform {
        GeoTransform::new_unchecked((-180., 90.).into(), 0.1, -0.1)
    }

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
        let approx = ApproxProjection::new(&projector, &grid(), THRESHOLD);

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
            max_error_px <= THRESHOLD.get(),
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
        let approx = ApproxProjection::new(&projector, &grid(), THRESHOLD);
        let row: Vec<Coordinate2D> = (0..5)
            .map(|x| Coordinate2D::new(f64::from(x) * 100_000., 8_000_000.))
            .collect();
        let mut projected = vec![None; row.len()];

        approx.project_row(&row, &mut projected);

        let exact = projector.project_coordinates(&row).unwrap();
        assert_eq!(projected, exact.into_iter().map(Some).collect::<Vec<_>>());
    }

    /// A threshold of 0 projects every coordinate exactly, as `gdalwarp -et 0`
    #[test]
    fn it_projects_exactly_with_a_threshold_of_zero() {
        let projector = DefaultCoordinateProjector::from_known_srs(
            SpatialReference::web_mercator(),
            SpatialReference::epsg_4326(),
        )
        .unwrap();
        let row: Vec<Coordinate2D> = (0..512)
            .map(|x| Coordinate2D::new(1_000_000. + f64::from(x) * 1_000., 11_000_000.))
            .collect();
        let exact = projector.project_coordinates(&row).unwrap();

        let project = |max_error_px: f64| {
            let mut projected = vec![None; row.len()];
            ApproxProjection::new(
                &projector,
                &grid(),
                NonNegativeFiniteF64::new(max_error_px).unwrap(),
            )
            .project_row(&row, &mut projected);
            projected
                .into_iter()
                .map(Option::unwrap)
                .collect::<Vec<_>>()
        };

        assert_eq!(project(0.), exact);
        // the same row is interpolated with the default threshold
        assert_ne!(project(THRESHOLD.get()), exact);
    }
}
