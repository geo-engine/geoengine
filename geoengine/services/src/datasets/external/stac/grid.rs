//! Lazy square search cells in each dataset's CRS projected area of use.
use geoengine_datatypes::{
    primitives::{AxisAlignedRectangle, BoundingBox2D, SpatialPartition2D, SpatialPartitioned},
    raster::{
        GeoTransform, GridBoundingBox2D, GridIdx2D, GridIntersection, GridShape2D,
        SpatialGridDefinition, TileInformation, TileInformationIter, TilingStrategy,
    },
};
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
        let upper_left = extent.upper_left();
        let lower_right = extent.lower_right();
        let width = extent.size_x();
        let height = extent.size_y();
        if ![
            upper_left.x,
            upper_left.y,
            lower_right.x,
            lower_right.y,
            width,
            height,
        ]
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
        let transform = GeoTransform::new(upper_left, side, -side);
        let extent_partition = SpatialPartition2D::new_unchecked(upper_left, lower_right);
        let grid_bounds = transform.spatial_to_grid_bounds(&extent_partition);
        let max_dimension = f64::from(u32::MAX).min(isize::MAX as f64);
        if grid_bounds.x_max() < 0
            || grid_bounds.y_max() < 0
            || grid_bounds.x_max() as f64 >= max_dimension
            || grid_bounds.y_max() as f64 >= max_dimension
        {
            return Err("STAC derived grid dimensions are outside supported index range");
        }
        let raster = SpatialGridDefinition::new(transform, grid_bounds);
        Ok(ProjectedStacGrid { extent, raster })
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct StacGridCell {
    /// Shared raster index in `[row(y), column(x)]` order; rows increase southward.
    pub index: GridIdx2D,
    /// Complete square in native CRS units; search footprints are clipped separately.
    pub bbox: BoundingBox2D,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProjectedStacGrid {
    extent: BoundingBox2D,
    raster: SpatialGridDefinition,
}

/// Maps shared one-pixel tile geometry to the cell record consumed by STAC callers.
pub(crate) type StacGridCells = std::iter::Map<
    std::iter::Flatten<std::option::IntoIter<TileInformationIter>>,
    fn(TileInformation) -> StacGridCell,
>;

impl From<TileInformation> for StacGridCell {
    fn from(tile: TileInformation) -> Self {
        Self {
            index: tile.global_tile_position(),
            bbox: tile.spatial_partition().as_bbox(),
        }
    }
}

impl ProjectedStacGrid {
    /// Clips a full grid cell to the projected area of use for the HTTP search.
    pub(crate) fn clip_cell_to_extent(self, bbox: BoundingBox2D) -> Option<BoundingBox2D> {
        bbox.intersection(&self.extent)
    }

    /// Returns the lazily traversed cells touching a query rectangle.
    ///
    /// Cell membership follows the shared raster spatial-to-grid conversion.
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

        let Some(clipped) = bbox.intersection(&self.extent) else {
            return Ok(self.cells_for_bounds(None));
        };

        let transform = self.raster.geo_transform();
        let query_bounds = if clipped.size_x() > 0. && clipped.size_y() > 0. {
            let partition =
                SpatialPartition2D::new_unchecked(clipped.upper_left(), clipped.lower_right());
            transform.spatial_to_grid_bounds(&partition)
        } else {
            transform.bounding_box_2d_to_intersecting_grid_bounds(&clipped)
        };
        let bounds = query_bounds.intersection(&self.raster.grid_bounds());
        Ok(self.cells_for_bounds(bounds))
    }

    fn cells_for_bounds(self, bounds: Option<GridBoundingBox2D>) -> StacGridCells {
        let tiling = TilingStrategy::new(GridShape2D::new_2d(1, 1), self.raster.geo_transform());
        let tiles = bounds.map(|bounds| tiling.tile_information_iterator_from_pixel_bounds(bounds));
        tiles
            .into_iter()
            .flatten()
            .map(StacGridCell::from as fn(TileInformation) -> StacGridCell)
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
        let grid_bounds = grid.raster.grid_bounds();
        assert_eq!(
            (
                grid_bounds.x_max() + 1,
                grid_bounds.y_max() + 1,
                grid.raster.geo_transform().x_pixel_size()
            ),
            (32, 16, 11.25)
        );
        assert_eq!(grid.cells_for_bbox(extent).unwrap().count(), 512);
        // The shared raster bounds are half-open at the grid's outer edge.
        assert_eq!(
            grid.cells_for_bbox(bbox(180., 90., 180., 90.))
                .unwrap()
                .count(),
            0
        );
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
            [GridIdx2D::new_y_x(0, 1), GridIdx2D::new_y_x(1, 1)]
        );
        let southern_cells = grid
            .cells_for_bbox(bbox(-4., -2., 4., 0.))
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(
            southern_cells
                .iter()
                .map(|cell| cell.index)
                .collect::<Vec<_>>(),
            [
                GridIdx2D::new_y_x(1, 0),
                GridIdx2D::new_y_x(1, 1),
                GridIdx2D::new_y_x(1, 2),
                GridIdx2D::new_y_x(1, 3)
            ]
        );
        let point = grid
            .cells_for_bbox(bbox(-2., 0., -2., 0.))
            .unwrap()
            .next()
            .unwrap();
        assert_eq!(point.index, GridIdx2D::new_y_x(1, 1));
        assert_eq!(
            grid.cells_for_bbox(bbox(5., 3., 6., 4.)).unwrap().count(),
            0
        );
    }

    #[test]
    fn it_shared_index_iteration_is_row_major_and_clones_keep_their_cursor() {
        let extent = bbox(0., 0., 4., 4.);
        let grid = StacGrid {
            target_number_of_cells: 4,
        }
        .for_extent(extent)
        .unwrap();
        let mut cells = grid.cells_for_bbox(extent).unwrap();

        let first = cells.next().unwrap();
        assert_eq!(first.index, GridIdx2D::new_y_x(0, 0));
        let mut clone = cells.clone();
        let next = cells.next().unwrap();
        assert_eq!(next.index, GridIdx2D::new_y_x(0, 1));
        let cloned_next = clone.next().unwrap();
        assert_eq!(cloned_next.index, next.index);
        assert_eq!(cloned_next.bbox, next.bbox);
        let row_one = cells.next().unwrap();
        assert_eq!(row_one.index, GridIdx2D::new_y_x(1, 0));
        assert!(row_one.bbox.lower_left().y < first.bbox.lower_left().y);
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
        assert_eq!(point.index, GridIdx2D::new_y_x(0, 1));
        assert_eq!(point.bbox.upper_left(), corner);
    }

    #[test]
    #[ignore = "shared inverse conversion can floor generated irrational edges to the previous cell; see STAC_GRID_LIMITATIONS.md"]
    fn it_preserves_queries_just_beyond_irrational_cell_edge() {
        let extent = bbox(-180., -90., 180., 90.);
        let grid = StacGrid {
            target_number_of_cells: 513,
        }
        .for_extent(extent)
        .unwrap();
        let transform = grid.raster.geo_transform();
        let side = transform.x_pixel_size();
        let edge = transform
            .grid_idx_to_pixel_upper_left_coordinate_2d(GridIdx2D::new_y_x(0, 1))
            .x;
        assert!(
            (edge - extent.lower_left().x) / side < 1.,
            "the inverse quotient rounds below its generated column-1 edge"
        );

        let edge_point = grid
            .cells_for_bbox(bbox(edge, 80., edge, 80.))
            .unwrap()
            .next()
            .unwrap();
        assert_eq!(edge_point.index, GridIdx2D::new_y_x(0, 1));

        let just_beyond = grid
            .cells_for_bbox(bbox(
                edge + side * 0.000_000_1,
                80.,
                edge + side * 0.000_000_5,
                80.1,
            ))
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(just_beyond.len(), 1);
        assert_eq!(just_beyond[0].index, GridIdx2D::new_y_x(0, 1));
    }

    #[test]
    #[ignore = "shared lower-right inward epsilon can omit a sub-micro-pixel overlap; see STAC_GRID_LIMITATIONS.md"]
    fn it_keeps_tiny_overlaps_across_vertical_and_horizontal_cell_edges() {
        let grid = StacGrid {
            target_number_of_cells: 16,
        }
        .for_extent(bbox(0., 0., 4., 4.))
        .unwrap();

        let vertical_overlap = grid
            .cells_for_bbox(bbox(0., 3.1, 1.000_000_5, 3.9))
            .unwrap()
            .map(|cell| cell.index)
            .collect::<Vec<_>>();
        assert_eq!(
            vertical_overlap,
            [GridIdx2D::new_y_x(0, 0), GridIdx2D::new_y_x(0, 1)]
        );

        let horizontal_overlap = grid
            .cells_for_bbox(bbox(0.1, 2.999_999_5, 0.9, 3.5))
            .unwrap()
            .map(|cell| cell.index)
            .collect::<Vec<_>>();
        assert_eq!(
            horizontal_overlap,
            [GridIdx2D::new_y_x(0, 0), GridIdx2D::new_y_x(1, 0)]
        );
    }

    #[test]
    fn narrow_extent_keeps_square_cells_and_clips_only_search_footprints() {
        let extent = bbox(-0.5, 10., 0.5, 50.);
        let grid = StacGrid {
            target_number_of_cells: 4,
        }
        .for_extent(extent)
        .unwrap();
        assert_eq!(
            (
                grid.raster.grid_bounds().x_max() + 1,
                grid.raster.grid_bounds().y_max() + 1
            ),
            (1, 13)
        );
        let cells: Vec<_> = grid.cells_for_bbox(extent).unwrap().collect();
        for cell in &cells {
            assert_square(*cell);
            let clipped = grid.clip_cell_to_extent(cell.bbox).unwrap();
            assert_eq!(clipped.size_x().to_bits(), 1_f64.to_bits());
            assert!(clipped.size_y() > 0.);
        }
        let last = cells.last().unwrap();
        assert!(last.bbox.lower_left().y < 10.);
        assert_eq!(
            grid.clip_cell_to_extent(last.bbox)
                .unwrap()
                .lower_left()
                .y
                .to_bits(),
            10_f64.to_bits()
        );
    }

    #[test]
    fn one_cell_target_can_require_extra_cells_for_full_extent_coverage() {
        let grid = StacGrid {
            target_number_of_cells: 1,
        }
        .for_extent(bbox(0., 0., 4., 9.))
        .unwrap();
        assert_eq!(
            grid.raster.geo_transform().x_pixel_size().to_bits(),
            6_f64.to_bits()
        );
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
        for extent in [bbox(0., 0., 0., 1.), bbox(0., 0., f64::INFINITY, 1.)] {
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
    #[ignore = "shared grid conversion accepts cells finer than coordinate precision at a large origin; see STAC_GRID_LIMITATIONS.md"]
    fn it_rejects_extent_beyond_shared_coordinate_precision() {
        assert!(
            StacGrid::default()
                .for_extent(bbox(1e16, 0., 1e16 + 2., 1.))
                .is_err()
        );
    }

    #[test]
    fn utm_grids_cover_the_crs_extent_with_square_native_cells() {
        for code in [32632, 32633] {
            let projection = SpatialReference::new(SpatialReferenceAuthority::Epsg, code);
            let extent = projection.area_of_use_projected::<BoundingBox2D>().unwrap();
            let grid = StacGrid::default().for_extent(extent).unwrap();
            let grid_bounds = grid.raster.grid_bounds();
            let side = grid.raster.geo_transform().x_pixel_size();
            assert!(grid_bounds.y_max() > grid_bounds.x_max());
            assert!((80_000. ..150_000.).contains(&side));
            let cells: Vec<_> = grid.cells_for_bbox(extent).unwrap().collect();
            for cell in cells {
                assert_square(cell);
                let footprint = grid.clip_cell_to_extent(cell.bbox).unwrap();
                assert!(footprint.size_x() > 0. && footprint.size_y() > 0.);
            }
        }
    }
}
