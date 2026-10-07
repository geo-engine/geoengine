//! Lazy square search cells in each dataset's CRS projected area of use.
use geoengine_datatypes::primitives::{AxisAlignedRectangle, BoundingBox2D, Coordinate2D};
use serde::{Deserialize, Serialize};

/// Provider-wide target count. Square cells and boundary coverage can require more cells.
#[derive(
    Clone,
    Copy,
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    postgres_types::ToSql,
    postgres_types::FromSql,
)]
#[postgres(name = "StacGrid")]
#[serde(rename_all = "camelCase")]
pub struct StacGrid {
    pub target_number_of_cells: i32,
}

impl Default for StacGrid {
    fn default() -> Self {
        Self {
            target_number_of_cells: 512,
        }
    }
}

impl StacGrid {
    pub(crate) fn validate(self) -> crate::error::Result<Self> {
        if self.target_number_of_cells < 1 {
            return Err(crate::error::Error::InvalidConfig {
                reason: "STAC target number of cells must be positive".to_owned(),
            });
        }
        Ok(self)
    }

    /// Derives square cells covering a dataset's projected area of use.
    ///
    /// The target count controls approximate cell area; the returned row and
    /// column counts can differ because the extent need not be square.
    pub(crate) fn for_extent(
        self,
        extent: BoundingBox2D,
    ) -> Result<ProjectedStacGrid, &'static str> {
        if self.target_number_of_cells < 1 {
            return Err("STAC target number of cells must be positive");
        }
        let lower = extent.lower_left();
        let upper = extent.upper_right();
        let width = extent.size_x();
        let height = extent.size_y();
        if ![lower.x, lower.y, upper.x, upper.y, width, height]
            .iter()
            .all(|v| v.is_finite())
            || width <= 0.
            || height <= 0.
        {
            return Err(
                "STAC CRS projected area of use must have finite positive width and height",
            );
        }
        // Separate square roots avoid overflowing width * height for large extents.
        let side = width.sqrt() * height.sqrt() / f64::from(self.target_number_of_cells).sqrt();
        if !side.is_finite() || side <= 0. {
            return Err("STAC derived cell side must be finite and positive");
        }
        let columns = axis_count(lower.x, upper.x, side)?;
        let rows = axis_count(lower.y, upper.y, side)?;
        Ok(ProjectedStacGrid {
            extent,
            side,
            columns,
            rows,
        })
    }
}

fn boundary(origin: f64, index: u32, side: f64) -> f64 {
    origin + f64::from(index) * side
}

/// Counts cells on one axis while correcting floating-point quotient rounding
/// against the actual cell boundaries.
fn axis_count(origin: f64, end: f64, side: f64) -> Result<u32, &'static str> {
    let count = ((end - origin) / side).ceil();
    if !count.is_finite() || count < 1. || count > f64::from(u32::MAX) {
        return Err("STAC derived grid dimensions are outside supported index range");
    }

    let mut count = count as u32;
    // Correct quotient rounding against the actual canonical boundaries. Never
    // create a terminal cell whose lower bound is already outside the extent.
    while count > 1 && boundary(origin, count - 1, side) >= end {
        count -= 1;
    }
    if boundary(origin, count, side) < end {
        count = count
            .checked_add(1)
            .ok_or("STAC derived grid dimension overflow")?;
    }
    let last = boundary(origin, count, side);
    let magnitude = origin.abs().max(last.abs()).max(f64::from(count) * side);
    if !last.is_finite() || side <= 2. * f64::EPSILON * magnitude {
        return Err("STAC derived cell boundaries exceed coordinate precision or range");
    }
    Ok(count)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct StacGridCellIndex {
    pub x: u32,
    pub y: u32,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct StacGridCell {
    pub index: StacGridCellIndex,
    /// Complete square in native CRS units; search footprints are clipped separately.
    pub bbox: BoundingBox2D,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProjectedStacGrid {
    extent: BoundingBox2D,
    side: f64,
    columns: u32,
    rows: u32,
}

#[derive(Clone)]
pub(crate) struct StacGridCells {
    grid: ProjectedStacGrid,
    next_x: u32,
    next_y: u32,
    start_x: u32,
    end_x: u32,
    end_y: u32,
    done: bool,
}

impl ProjectedStacGrid {
    /// Clips a full grid cell to the projected area of use for the HTTP search.
    pub(crate) fn clip_cell_to_extent(self, bbox: BoundingBox2D) -> Option<BoundingBox2D> {
        bbox.intersection(&self.extent)
    }

    /// Returns the lazily traversed cells touching a query rectangle.
    ///
    /// A point on an internal edge belongs to the cell beginning at that edge.
    /// For a positive-area query, an upper bound exactly on an edge ends at the
    /// preceding cell, excluding the cell beyond the edge.
    pub(crate) fn cells_for_bbox(self, bbox: BoundingBox2D) -> Result<StacGridCells, &'static str> {
        let lower = bbox.lower_left();
        let upper = bbox.upper_right();
        if ![lower.x, lower.y, upper.x, upper.y]
            .iter()
            .all(|v| v.is_finite())
            || lower.x > upper.x
            || lower.y > upper.y
        {
            return Err("STAC query bounds must be finite and ordered");
        }

        let mut cells = StacGridCells {
            grid: self,
            next_x: 0,
            next_y: 0,
            start_x: 0,
            end_x: 0,
            end_y: 0,
            done: true,
        };
        let Some(clipped) = bbox.intersection(&self.extent) else {
            return Ok(cells);
        };

        let lower = clipped.lower_left();
        let upper = clipped.upper_right();
        let origin = self.extent.lower_left();
        let start_x = self.index(lower.x, origin.x, self.columns);
        let start_y = self.index(lower.y, origin.y, self.rows);
        let end_x = self.upper_index(upper.x, lower.x, origin.x, self.columns);
        let end_y = self.upper_index(upper.y, lower.y, origin.y, self.rows);
        cells.next_x = start_x;
        cells.next_y = start_y;
        cells.start_x = start_x;
        cells.end_x = end_x;
        cells.end_y = end_y;
        cells.done = start_x > end_x || start_y > end_y;

        Ok(cells)
    }

    /// Finds the cell beginning at or immediately before a coordinate.
    fn index(self, value: f64, origin: f64, count: u32) -> u32 {
        // Compare canonical boundaries rather than a rounded floating quotient.
        // At most 32 comparisons even for the largest supported grid dimension.
        let mut lower = 0;
        let mut upper = count;
        while lower + 1 < upper {
            let middle = lower + (upper - lower) / 2;
            if boundary(origin, middle, self.side) <= value {
                lower = middle;
            } else {
                upper = middle;
            }
        }
        lower
    }

    /// Selects the cell containing an upper bound, including the preceding cell
    /// when a positive-width query ends exactly on a canonical boundary.
    #[allow(
        clippy::float_cmp,
        reason = "exact canonical boundary equality determines cell membership"
    )]
    fn upper_index(self, value: f64, lower: f64, origin: f64, count: u32) -> u32 {
        let mut index = self.index(value, origin, count);
        if value > lower && index > 0 && boundary(origin, index, self.side) == value {
            index -= 1;
        }
        index
    }
}

impl Iterator for StacGridCells {
    type Item = StacGridCell;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let x = self.next_x;
        let y = self.next_y;
        if x == self.end_x {
            self.next_x = self.start_x;
            if y == self.end_y {
                self.done = true;
            } else {
                self.next_y += 1;
            }
        } else {
            self.next_x += 1;
        }
        let origin = self.grid.extent.lower_left();
        let side = self.grid.side;
        let bbox = BoundingBox2D::new(
            Coordinate2D::new(boundary(origin.x, x, side), boundary(origin.y, y, side)),
            Coordinate2D::new(
                boundary(origin.x, x + 1, side),
                boundary(origin.y, y + 1, side),
            ),
        )
        .expect("derived grid boundaries are finite and ordered");
        Some(StacGridCell {
            index: StacGridCellIndex { x, y },
            bbox,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceAuthority};

    fn bbox(x0: f64, y0: f64, x1: f64, y1: f64) -> BoundingBox2D {
        BoundingBox2D::new((x0, y0).into(), (x1, y1).into()).unwrap()
    }

    fn assert_square(cell: StacGridCell) {
        let width = cell.bbox.size_x();
        let height = cell.bbox.size_y();
        assert!((width - height).abs() < width.max(height) * 1e-8);
    }

    #[test]
    fn default_geographic_grid_has_512_square_cells_and_safe_endpoints() {
        let extent = bbox(-180., -90., 180., 90.);
        let grid = StacGrid::default().for_extent(extent).unwrap();
        assert_eq!((grid.columns, grid.rows, grid.side), (32, 16, 11.25));
        assert_eq!(grid.cells_for_bbox(extent).unwrap().count(), 512);
        let last = grid
            .cells_for_bbox(bbox(180., 90., 180., 90.))
            .unwrap()
            .next()
            .unwrap();
        assert_eq!(last.index, StacGridCellIndex { x: 31, y: 15 });
        assert_eq!(last.bbox.upper_right(), extent.upper_right());
        assert_square(last);
    }

    #[test]
    fn exact_edges_points_and_multiple_rows_select_only_intersecting_cells() {
        let grid = StacGrid {
            target_number_of_cells: 8,
        }
        .for_extent(bbox(-4., -2., 4., 2.))
        .unwrap();
        let cells: Vec<_> = grid
            .cells_for_bbox(bbox(-2., -2., 0., 2.))
            .unwrap()
            .collect();
        assert_eq!(
            cells.iter().map(|c| c.index).collect::<Vec<_>>(),
            [
                StacGridCellIndex { x: 1, y: 0 },
                StacGridCellIndex { x: 1, y: 1 }
            ]
        );
        let point = grid
            .cells_for_bbox(bbox(-2., 0., -2., 0.))
            .unwrap()
            .next()
            .unwrap();
        assert_eq!(point.index, StacGridCellIndex { x: 1, y: 1 });
        assert_eq!(
            grid.cells_for_bbox(bbox(5., 3., 6., 4.)).unwrap().count(),
            0
        );
    }

    #[test]
    fn irrational_side_boundaries_are_identical_without_extra_neighbors() {
        let grid = StacGrid {
            target_number_of_cells: 11,
        }
        .for_extent(bbox(-2., -1., 5., 2.))
        .unwrap();
        let first = grid.cells_for_bbox(grid.extent).unwrap().next().unwrap();
        assert_eq!(grid.cells_for_bbox(first.bbox).unwrap().count(), 1);
        let corner = first.bbox.upper_right();
        let point = grid
            .cells_for_bbox(bbox(corner.x, corner.y, corner.x, corner.y))
            .unwrap()
            .next()
            .unwrap();
        assert_eq!(point.index, StacGridCellIndex { x: 1, y: 1 });
        assert_eq!(point.bbox.lower_left(), corner);
    }

    #[test]
    fn narrow_extent_keeps_square_cells_and_clips_only_search_footprints() {
        let extent = bbox(-0.5, 10., 0.5, 50.);
        let grid = StacGrid {
            target_number_of_cells: 4,
        }
        .for_extent(extent)
        .unwrap();
        assert_eq!((grid.columns, grid.rows), (1, 13));
        let cells: Vec<_> = grid.cells_for_bbox(extent).unwrap().collect();
        for cell in &cells {
            assert_square(*cell);
            let clipped = grid.clip_cell_to_extent(cell.bbox).unwrap();
            assert_eq!(clipped.size_x().to_bits(), 1_f64.to_bits());
            assert!(clipped.size_y() > 0.);
        }
        let last = cells.last().unwrap();
        assert!(last.bbox.upper_right().y > 50.);
        assert_eq!(
            grid.clip_cell_to_extent(last.bbox)
                .unwrap()
                .upper_right()
                .y
                .to_bits(),
            50_f64.to_bits()
        );
    }

    #[test]
    fn one_cell_target_can_require_extra_cells_for_full_extent_coverage() {
        let grid = StacGrid {
            target_number_of_cells: 1,
        }
        .for_extent(bbox(0., 0., 4., 9.))
        .unwrap();
        assert_eq!(grid.side.to_bits(), 6_f64.to_bits());
        assert_eq!(grid.cells_for_bbox(grid.extent).unwrap().count(), 2);
    }

    #[test]
    fn invalid_counts_extents_and_unrepresentable_cells_are_rejected() {
        for count in [0, -1] {
            let config = StacGrid {
                target_number_of_cells: count,
            };
            assert!(config.validate().is_err());
            assert!(config.for_extent(bbox(0., 0., 1., 1.)).is_err());
        }
        for extent in [
            bbox(0., 0., 0., 1.),
            bbox(0., 0., f64::INFINITY, 1.),
            bbox(1e16, 0., 1e16 + 2., 1.),
        ] {
            assert!(StacGrid::default().for_extent(extent).is_err());
        }
        let grid = StacGrid {
            target_number_of_cells: i32::MAX,
        }
        .for_extent(bbox(0., 0., 1., 1.))
        .unwrap();
        assert!(
            grid.cells_for_bbox(BoundingBox2D::new_unchecked(
                (0., 0.).into(),
                (f64::NAN, 1.).into()
            ))
            .is_err()
        );
        // Selecting a tiny part of a huge grid never allocates all its cells.
        assert_eq!(
            grid.cells_for_bbox(bbox(0., 0., 0., 0.)).unwrap().count(),
            1
        );
    }

    #[test]
    fn utm_grids_cover_the_crs_extent_with_square_native_cells() {
        for code in [32632, 32633] {
            let projection = SpatialReference::new(SpatialReferenceAuthority::Epsg, code);
            let extent = projection.area_of_use_projected::<BoundingBox2D>().unwrap();
            let grid = StacGrid::default().for_extent(extent).unwrap();
            assert!(grid.rows > grid.columns);
            assert!((80_000. ..150_000.).contains(&grid.side));
            let cells: Vec<_> = grid.cells_for_bbox(extent).unwrap().collect();
            for cell in cells {
                assert_square(cell);
                let footprint = grid.clip_cell_to_extent(cell.bbox).unwrap();
                assert!(footprint.size_x() > 0. && footprint.size_y() > 0.);
            }
        }
    }
}
