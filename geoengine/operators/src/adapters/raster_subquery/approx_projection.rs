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

    /// The largest error of [`Self::line_bounds`] in pixels of the grid: the threshold, or GDAL's
    /// default of an eighth of a pixel if coordinates are projected exactly, as a line cannot be
    /// split forever.
    pub fn line_bounds_error_px(&self) -> f64 {
        self.max_error_px.get().max(0.125)
    }

    /// The smallest and the largest coordinates of the projection of the line from `start` to
    /// `end`, at most [`Self::line_bounds_error_px`] pixels of the grid too small. `None` if a
    /// coordinate of the line cannot be projected.
    ///
    /// Like the rows, the line is split until the projection of the middle of a part lies within
    /// the threshold of the straight line between the projections of its ends. The projection of
    /// a line bends, so its extremes can lie between its ends; a fixed number of points along
    /// the line misses them by an unknown amount.
    pub fn line_bounds(
        &self,
        start: Coordinate2D,
        end: Coordinate2D,
    ) -> Option<(Coordinate2D, Coordinate2D)> {
        // a part of the line that is still bent after this many halvings is taken as it is
        const MAX_DEPTH: u32 = 16;

        let project = |coordinate: Coordinate2D| {
            self.projector
                .project_coordinate(coordinate)
                .ok()
                .filter(is_finite)
        };
        let middle_of = |a: Coordinate2D, b: Coordinate2D| {
            Coordinate2D::new(f64::midpoint(a.x, b.x), f64::midpoint(a.y, b.y))
        };

        let (projected_start, projected_end) = (project(start)?, project(end)?);
        let mut min = projected_start;
        let mut max = projected_start;
        let mut extend = |coordinate: Coordinate2D| {
            min = Coordinate2D::new(min.x.min(coordinate.x), min.y.min(coordinate.y));
            max = Coordinate2D::new(max.x.max(coordinate.x), max.y.max(coordinate.y));
        };
        extend(projected_end);

        let mut parts = vec![(start, end, projected_start, projected_end, 0)];
        while let Some((a, b, projected_a, projected_b, depth)) = parts.pop() {
            let middle = middle_of(a, b);
            let projected_middle = project(middle)?;
            extend(projected_middle);

            let straight = middle_of(projected_a, projected_b);
            let error_px = (straight.x - projected_middle.x).abs() / self.in_pixel_size.0.get()
                + (straight.y - projected_middle.y).abs() / self.in_pixel_size.1.get();
            // the first halving is always done: the middle alone can lie on the straight line
            // of a line that bends to both sides
            if (depth == 0 || error_px > self.line_bounds_error_px()) && depth < MAX_DEPTH {
                parts.push((a, middle, projected_a, projected_middle, depth + 1));
                parts.push((middle, b, projected_middle, projected_b, depth + 1));
            }
        }
        Some((min, max))
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

    /// The upper edge of a view of 500 km in EPSG:3857 bends in UTM zone 32: its lowest y lies
    /// between its ends, where seven points along the edge miss it by 48 m.
    #[test]
    fn it_bounds_a_line_that_bends() {
        let projector = DefaultCoordinateProjector::from_known_srs(
            SpatialReference::web_mercator(),
            SpatialReference::new(
                geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
                32632,
            ),
        )
        .unwrap();
        // a UTM grid of 10 m pixels
        let utm = GeoTransform::new_unchecked((0., 0.).into(), 10., -10.);
        let approx = ApproxProjection::new(&projector, &utm, THRESHOLD);

        for y in [6_400_000., 6_700_000.] {
            let (start, end) = (
                Coordinate2D::new(800_000., y),
                Coordinate2D::new(1_300_000., y),
            );
            let (min, max) = approx.line_bounds(start, end).unwrap();

            let dense: Vec<Coordinate2D> = (0..=5000)
                .map(|i| Coordinate2D::new(start.x + f64::from(i) * 100., y))
                .collect();
            let exact = projector.project_coordinates(&dense).unwrap();
            let tolerance = approx.line_bounds_error_px() * 10.;
            for coordinate in exact {
                assert!(
                    coordinate.x >= min.x - tolerance
                        && coordinate.x <= max.x + tolerance
                        && coordinate.y >= min.y - tolerance
                        && coordinate.y <= max.y + tolerance,
                    "{coordinate:?} is outside of {min:?} to {max:?}"
                );
            }
        }
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
