use super::{
    FromIndexFn, GeoTransform, GeoTransformAccess, Grid, GridBoundingBox2D, GridBoundingBoxExt,
    GridBounds, GridIdx, GridIdx2D, GridIntersection, TilingSpecification, TilingStrategy,
};
use crate::{
    operations::reproject::{
        suggest_pixel_size_from_diag_cross_projected, CoordinateProjection, Reproject,
        ReprojectClipped,
    },
    primitives::{
        AxisAlignedRectangle, Coordinate2D, SpatialPartition2D, SpatialPartitioned,
        SpatialResolution,
    },
    util::Result,
};
use float_cmp::approx_eq;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, ToSql, FromSql, PartialEq)]
pub struct SpatialGridDefinition {
    pub geo_transform: GeoTransform,
    pub grid_bounds: GridBoundingBox2D,
}

impl SpatialGridDefinition {
    pub fn new(geo_transform: GeoTransform, grid_bounds: GridBoundingBox2D) -> Self {
        Self {
            grid_bounds,
            geo_transform,
        }
    }

    pub fn new_generic<G: Into<GridBoundingBox2D>>(
        geo_transform: GeoTransform,
        grid_bounds: G,
    ) -> Self {
        let grid_bounds: GridBoundingBox2D = grid_bounds.into();
        Self::new(geo_transform, grid_bounds)
    }

    pub fn grid_bounds(&self) -> GridBoundingBox2D {
        self.grid_bounds
    }

    pub fn geo_transform(&self) -> GeoTransform {
        self.geo_transform
    }

    pub fn spatial_partition(&self) -> SpatialPartition2D {
        self.geo_transform.grid_to_spatial_bounds(&self.grid_bounds)
    }

    pub fn shift_bounds_relative_by_pixel_offset(&self, offset: GridIdx2D) -> Self {
        let grid_bounds = self.grid_bounds.shift_by_offset(offset);
        let geo_transform = self.geo_transform.shift_by_pixel_offset(-offset);
        Self::new(geo_transform, grid_bounds)
    }

    pub fn with_moved_origin_exact_grid(&self, new_origin: Coordinate2D) -> Option<Self> {
        if approx_eq!(
            Coordinate2D,
            self.geo_transform
                .distance_to_nearest_pixel_edge(new_origin),
            Coordinate2D::new(0., 0.)
        ) {
            Some(self.with_moved_origin_to_nearest_grid_edge(new_origin))
        } else {
            None
        }
    }

    /// This method moves the origin to the coordinate of the grid edge nearest to the supplied new origin reference
    pub fn with_moved_origin_to_nearest_grid_edge(
        &self,
        new_origin_referece: Coordinate2D,
    ) -> Self {
        let nearest_to_target = self
            .geo_transform
            .coordinate_to_grid_idx_2d(new_origin_referece);
        self.shift_bounds_relative_by_pixel_offset(-nearest_to_target)
    }

    /// This method moves the origin to the coordinate of the grid edge nearest to the supplied new origin reference
    pub fn with_moved_origin_to_nearest_grid_edge_with_distance(
        &self,
        new_origin_referece: Coordinate2D,
    ) -> (Self, Coordinate2D) {
        let distance = self
            .geo_transform
            .distance_to_nearest_pixel_edge(new_origin_referece);
        let new_self = self.with_moved_origin_to_nearest_grid_edge(new_origin_referece);
        (new_self, distance)
    }

    /// Creates a new spatial grid with the self shape and pixel size but new origin.
    pub fn with_replaced_origin(&self, new_origin: Coordinate2D) -> Self {
        Self {
            geo_transform: GeoTransform::new(
                new_origin,
                self.geo_transform.x_pixel_size(),
                self.geo_transform.y_pixel_size(),
            ),
            grid_bounds: self.grid_bounds,
        }
    }

    /// Merges two spatial grids
    /// If the second grid is not compatible with selfit returns None
    /// If the second grid has a different GeoTransform it is transformed to the GroTransform of self
    pub fn merge(&self, other: &Self) -> Option<Self> {
        if !self.is_compatible_grid_generic(other) {
            return None;
        };

        let other_shift =
            other.with_moved_origin_exact_grid(self.geo_transform.origin_coordinate)?;

        let merged_bounds = self.grid_bounds().extended(&other_shift.grid_bounds());

        Some(Self::new(self.geo_transform, merged_bounds))
    }

    pub fn is_compatible_grid_generic<G: GeoTransformAccess>(&self, g: &G) -> bool {
        self.geo_transform().is_compatible_grid(g.geo_transform())
    }

    /// Computes the intersection of self and other
    /// IF other is incompatible with self, None is returned.
    /// IF other has a different GeoTransform then self it is transformed to to the GeoTransform of self.
    pub fn intersection(&self, other: &SpatialGridDefinition) -> Option<SpatialGridDefinition> {
        if !self.is_compatible_grid_generic(other) {
            return None;
        };

        let other_shift =
            other.with_moved_origin_exact_grid(self.geo_transform.origin_coordinate)?;

        let intersection_bounds = self
            .grid_bounds()
            .intersection(&(other_shift.grid_bounds()))?;

        Some(Self::new(self.geo_transform, intersection_bounds))
    }

    /// Creates a new spatial grid that has the same origin as self.
    /// The pixel sizes are changed and the grid bounds are adapted to cover the same spatial area.
    /// Note: if the new resolution is not a multiple of the old resolution the new grid might cover a larger spatial area then self.
    pub fn with_changed_resolution(&self, new_res: SpatialResolution) -> Self {
        let geo_transform =
            GeoTransform::new(self.geo_transform.origin_coordinate, new_res.x, -new_res.y);
        let grid_bounds = geo_transform.spatial_to_grid_bounds(&self.spatial_partition());
        SpatialGridDefinition::new(geo_transform, grid_bounds)
    }

    pub fn generate_coord_grid_upper_left_edge(&self) -> Grid<GridBoundingBox2D, Coordinate2D> {
        let map_fn = |idx: GridIdx2D| {
            self.geo_transform
                .grid_idx_to_pixel_upper_left_coordinate_2d(idx)
        };

        Grid::from_index_fn(&self.grid_bounds, map_fn)
    }

    pub fn generate_coord_grid_pixel_center(&self) -> Grid<GridBoundingBox2D, Coordinate2D> {
        let map_fn = |idx: GridIdx2D| {
            self.geo_transform
                .grid_idx_to_pixel_center_coordinate_2d(idx)
        };

        Grid::from_index_fn(&self.grid_bounds, map_fn)
    }

    pub fn spatial_bounds_to_compatible_spatial_grid(
        &self,
        spatial_partition: SpatialPartition2D,
    ) -> Self {
        let grid_bounds = self
            .geo_transform
            .spatial_to_grid_bounds(&spatial_partition);
        Self::new(self.geo_transform, grid_bounds)
    }
}

impl SpatialPartitioned for SpatialGridDefinition {
    fn spatial_partition(&self) -> SpatialPartition2D {
        self.spatial_partition()
    }
}

impl GridBounds for SpatialGridDefinition {
    type IndexArray = [isize; 2];

    fn min_index(&self) -> GridIdx<Self::IndexArray> {
        self.grid_bounds.min_index()
    }

    fn max_index(&self) -> GridIdx<Self::IndexArray> {
        self.grid_bounds.max_index()
    }
}

impl GeoTransformAccess for SpatialGridDefinition {
    fn geo_transform(&self) -> GeoTransform {
        self.geo_transform()
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct TilingSpatialGridDefinition {
    // Don't make this public to avoid leaking inner
    element_grid_definition: SpatialGridDefinition,
    tiling_specification: TilingSpecification,
}

impl TilingSpatialGridDefinition {
    pub fn new(
        element_grid_definition: SpatialGridDefinition,
        tiling_specification: TilingSpecification,
    ) -> Self {
        Self {
            element_grid_definition,
            tiling_specification,
        }
    }

    pub fn tiling_spatial_grid_definition(&self) -> SpatialGridDefinition {
        // TODO: maybe do this in new and store it?
        self.element_grid_definition
            .with_moved_origin_to_nearest_grid_edge(
                self.tiling_specification.tiling_origin_reference(),
            )
    }

    pub fn tiling_geo_transform(&self) -> GeoTransform {
        self.tiling_spatial_grid_definition().geo_transform()
    }

    pub fn tiling_grid_bounds(&self) -> GridBoundingBox2D {
        self.tiling_spatial_grid_definition().grid_bounds()
    }

    pub fn is_compatible_grid_generic<G: GeoTransformAccess>(&self, g: &G) -> bool {
        // TODO: use tiling_spatial_grid_definition?
        self.element_grid_definition.is_compatible_grid_generic(g)
    }

    pub fn is_same_tiled_grid(&self, other: &TilingSpatialGridDefinition) -> bool {
        // TODO: re-implement when decided how to model struct
        let a = self.tiling_spatial_grid_definition();
        let b = other.tiling_spatial_grid_definition();
        approx_eq!(GeoTransform, a.geo_transform(), b.geo_transform())
    }

    /// Returns the data tiling strategy for the given tile size in pixels.
    pub fn generate_data_tiling_strategy(&self) -> TilingStrategy {
        TilingStrategy {
            geo_transform: self.tiling_geo_transform(),
            tile_size_in_pixels: self.tiling_specification.tile_size_in_pixels,
        }
    }

    pub fn with_other_bounds(&self, new_bounds: GridBoundingBox2D) -> Self {
        let new_grid = SpatialGridDefinition::new(self.tiling_geo_transform(), new_bounds);
        Self::new(new_grid, self.tiling_specification)
    }
}

impl SpatialPartitioned for TilingSpatialGridDefinition {
    fn spatial_partition(&self) -> SpatialPartition2D {
        // TODO: use tiling bounds and geotransform? must be equal!!!
        self.element_grid_definition.spatial_partition()
    }
}

impl<P: CoordinateProjection> Reproject<P> for SpatialGridDefinition {
    type Out = Self;

    fn reproject(&self, projector: &P) -> Result<Self::Out> {
        let spatial_bounds = self.spatial_partition();
        let projected_bounds = spatial_bounds.reproject(projector)?;
        let out_res: SpatialResolution = suggest_pixel_size_from_diag_cross_projected(
            spatial_bounds,
            projected_bounds,
            self.geo_transform().spatial_resolution(), // FIXME: sign should go through method
        )?;
        let out_geo_transform = GeoTransform::new(
            projected_bounds.upper_left(),
            out_res.x,
            -out_res.y, // FIXME: sign should go through method
        );
        let out_grid_bounds = out_geo_transform.spatial_to_grid_bounds(&projected_bounds);
        Ok(SpatialGridDefinition::new(
            out_geo_transform,
            out_grid_bounds,
        ))
    }
}

impl<P: CoordinateProjection> ReprojectClipped<P> for SpatialGridDefinition {
    type Out = Self;

    fn reproject_clipped(&self, projector: &P) -> Result<Option<Self::Out>> {
        let target_bounds: Option<SpatialPartition2D> = projector
            .target_srs()
            .area_of_use_intersection(&projector.source_srs())?;
        if target_bounds.is_none() {
            return Ok(None);
        };
        let target_bounds = target_bounds.expect("case checked above");
        let intersection_grid_bounds = target_bounds.intersection(&self.spatial_partition());
        if intersection_grid_bounds.is_none() {
            return Ok(None);
        };
        let intersection_grid_bounds = intersection_grid_bounds.expect("case checked above");
        let usable_grid_bounds = self
            .geo_transform()
            .spatial_to_grid_bounds(&intersection_grid_bounds);
        // TODO: maybe we need to crop a pixel at lr if the intersection is within the pixel at lr...
        let temp_grid = SpatialGridDefinition::new(self.geo_transform(), usable_grid_bounds);
        temp_grid.reproject(projector).map(Option::Some)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        primitives::AxisAlignedRectangle,
        spatial_reference::{SpatialReference, SpatialReferenceAuthority},
        test_data,
        util::gdal::gdal_open_dataset,
    };

    use super::*;

    #[test]
    fn shift_bounds_relative_by_pixel_offset() {
        let s = SpatialGridDefinition::new(
            GeoTransform::new_with_coordinate_x_y(0.0, 1.0, 0.0, -1.0),
            GridBoundingBox2D::new_min_max(-2, 0, 0, 2).unwrap(),
        );
        let shifted_s = s.shift_bounds_relative_by_pixel_offset(GridIdx2D::new([1, 1]));
        assert_eq!(
            shifted_s.geo_transform(),
            GeoTransform::new_with_coordinate_x_y(-1., 1.0, 1., -1.0)
        );
        assert_eq!(shifted_s.grid_bounds().min_index(), GridIdx2D::new([-1, 1]));
        assert_eq!(shifted_s.grid_bounds().max_index(), GridIdx2D::new([1, 3]));

        assert_eq!(s.spatial_partition(), shifted_s.spatial_partition());
    }

    #[test]
    fn merge() {
        let s = SpatialGridDefinition::new(
            GeoTransform::new_with_coordinate_x_y(0.0, 1.0, 0.0, -1.0),
            GridBoundingBox2D::new_min_max(-2, 0, 0, 2).unwrap(),
        );

        let s_2 = SpatialGridDefinition::new(
            GeoTransform::new_with_coordinate_x_y(1.0, 1.0, -1.0, -1.0),
            GridBoundingBox2D::new_min_max(-2, 0, 0, 2).unwrap(),
        );

        let merged = s.merge(&s_2).unwrap();

        assert_eq!(s.geo_transform, merged.geo_transform);
        assert_eq!(
            GridBoundingBox2D::new_min_max(-2, 1, 0, 3).unwrap(),
            merged.grid_bounds
        );

        let s_s2_spatial_partition = s.spatial_partition().extended(&s_2.spatial_partition());
        let merged_partition = merged.spatial_partition();

        assert!(approx_eq!(
            Coordinate2D,
            s_s2_spatial_partition.upper_left(),
            merged_partition.upper_left()
        ));
        assert!(approx_eq!(
            Coordinate2D,
            s_s2_spatial_partition.lower_right(),
            merged_partition.lower_right()
        ));
    }

    #[test]
    fn no_merge_origin() {
        let s = SpatialGridDefinition::new(
            GeoTransform::new_with_coordinate_x_y(0.0, 1.0, 0.0, -1.0),
            GridBoundingBox2D::new_min_max(-2, 0, 0, 2).unwrap(),
        );

        let s_2 = SpatialGridDefinition::new(
            GeoTransform::new_with_coordinate_x_y(1.1, 1.0, -1.1, -1.0),
            GridBoundingBox2D::new_min_max(-2, 0, 0, 2).unwrap(),
        );

        assert!(s.merge(&s_2).is_none());
    }

    #[test]
    fn no_merge_pixel_size() {
        let s = SpatialGridDefinition::new(
            GeoTransform::new_with_coordinate_x_y(0.0, 1.0, 0.0, -1.0),
            GridBoundingBox2D::new_min_max(-2, 0, 0, 2).unwrap(),
        );

        let s_2 = SpatialGridDefinition::new(
            GeoTransform::new_with_coordinate_x_y(1.0, 1.1, -1.0, -1.1),
            GridBoundingBox2D::new_min_max(-2, 0, 0, 2).unwrap(),
        );

        assert!(s.merge(&s_2).is_none());
    }

    #[test]
    fn source_resolution() {
        let epsg_4326 = SpatialReference::epsg_4326();
        let epsg_3857 = SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857);

        // use ndvi dataset that was reprojected using gdal as ground truth
        let dataset_4326 = gdal_open_dataset(test_data!(
            "raster/modis_ndvi/MOD13A2_M_NDVI_2014-04-01.TIFF"
        ))
        .unwrap();
        let geotransform_4326 = dataset_4326.geo_transform().unwrap();
        let res_4326 = SpatialResolution::new(geotransform_4326[1], -geotransform_4326[5]).unwrap();

        let dataset_3857 = gdal_open_dataset(test_data!(
            "raster/modis_ndvi/projected_3857/MOD13A2_M_NDVI_2014-04-01.TIFF"
        ))
        .unwrap();
        let geotransform_3857 = dataset_3857.geo_transform().unwrap();
        let res_3857 = SpatialResolution::new(geotransform_3857[1], -geotransform_3857[5]).unwrap();

        // ndvi was projected from 4326 to 3857. The calculated source_resolution for getting the raster in 3857 with `res_3857`
        // should thus roughly be like the original `res_4326`
        let result_res = suggest_pixel_size_from_diag_cross_projected::<SpatialPartition2D>(
            epsg_3857.area_of_use_projected().unwrap(),
            epsg_4326.area_of_use_projected().unwrap(),
            res_3857,
        )
        .unwrap();
        assert!(1. - (result_res.x / res_4326.x).abs() < 0.02);
        assert!(1. - (result_res.y / res_4326.y).abs() < 0.02);
    }
}
