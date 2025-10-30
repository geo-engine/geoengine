use crate::{
    error,
    primitives::{
        AxisAlignedRectangle, BoundingBox2D, Coordinate2D, Line, MultiLineString,
        MultiLineStringAccess, MultiLineStringRef, MultiPoint, MultiPointAccess, MultiPointRef,
        MultiPolygon, MultiPolygonAccess, MultiPolygonRef, SpatialBounded, SpatialResolution,
    },
    raster::{
        BoundedGrid, GeoTransform, GridBoundingBox, GridBounds, GridIdx, GridIdx2D, GridShape,
        GridSize, SamplePoints, SpatialGridDefinition,
    },
    spatial_reference::SpatialReference,
    util::Result,
};
use num_traits::Zero;
use proj::Proj;
use snafu::ensure;

pub trait CoordinateProjection {
    fn from_known_srs(from: SpatialReference, to: SpatialReference) -> Result<Self>
    where
        Self: Sized;
    /// project a single coord
    fn project_coordinate(&self, c: Coordinate2D) -> Result<Coordinate2D>;

    /// project a set of coords
    fn project_coordinates<A: AsRef<[Coordinate2D]>>(&self, coords: A)
    -> Result<Vec<Coordinate2D>>;

    fn source_srs(&self) -> SpatialReference;

    fn target_srs(&self) -> SpatialReference;
}

pub struct CoordinateProjector {
    pub from: SpatialReference,
    pub to: SpatialReference,
    p: Proj,
}

impl CoordinateProjection for CoordinateProjector {
    fn from_known_srs(from: SpatialReference, to: SpatialReference) -> Result<Self> {
        let p = Proj::new_known_crs(&from.proj_string()?, &to.proj_string()?, None)
            .map_err(|_| error::Error::NoCoordinateProjector { from, to })?;
        Ok(CoordinateProjector { from, to, p })
    }

    fn project_coordinate(&self, c: Coordinate2D) -> Result<Coordinate2D> {
        self.p.convert(c).map_err(Into::into)
    }

    fn project_coordinates<A: AsRef<[Coordinate2D]>>(
        &self,
        coords: A,
    ) -> Result<Vec<Coordinate2D>> {
        let c_ref = coords.as_ref();

        let mut cc = Vec::from(c_ref);
        self.p.convert_array(&mut cc)?;

        Ok(cc)
    }

    fn source_srs(&self) -> SpatialReference {
        self.from
    }

    fn target_srs(&self) -> SpatialReference {
        self.to
    }
}

impl Clone for CoordinateProjector {
    fn clone(&self) -> Self {
        CoordinateProjector {
            from: self.from,
            to: self.to,
            p: Proj::new_known_crs(&self.from.to_string(), &self.to.to_string(), None)
                .expect("the Proj object creation should work because it already worked in the creation of the `CoordinateProjector`"),
        }
    }
}

impl AsRef<CoordinateProjector> for CoordinateProjector {
    fn as_ref(&self) -> &CoordinateProjector {
        self
    }
}

pub trait Reproject<P: CoordinateProjection> {
    type Out;
    fn reproject(&self, projector: &P) -> Result<Self::Out>;
}

impl<P> Reproject<P> for Coordinate2D
where
    P: CoordinateProjection,
{
    type Out = Coordinate2D;

    fn reproject(&self, projector: &P) -> Result<Self::Out> {
        projector.project_coordinate(*self)
    }
}

impl<P> Reproject<P> for MultiPoint
where
    P: CoordinateProjection,
{
    type Out = MultiPoint;
    fn reproject(&self, projector: &P) -> Result<MultiPoint> {
        let ps: Result<Vec<Coordinate2D>> = projector.project_coordinates(self.points());
        ps.and_then(MultiPoint::new)
    }
}

impl<P> Reproject<P> for MultiPointRef<'_>
where
    P: CoordinateProjection,
{
    type Out = MultiPoint;
    fn reproject(&self, projector: &P) -> Result<MultiPoint> {
        let ps: Result<Vec<Coordinate2D>> = projector.project_coordinates(self.points());
        ps.and_then(MultiPoint::new)
    }
}

impl<P> Reproject<P> for Line
where
    P: CoordinateProjection,
{
    type Out = Line;

    fn reproject(&self, projector: &P) -> Result<Self::Out> {
        Ok(Line {
            start: self.start.reproject(projector)?,
            end: self.end.reproject(projector)?,
        })
    }
}

impl<P> Reproject<P> for MultiLineString
where
    P: CoordinateProjection,
{
    type Out = MultiLineString;
    fn reproject(&self, projector: &P) -> Result<MultiLineString> {
        let ls: Result<Vec<Vec<Coordinate2D>>> = self
            .lines()
            .iter()
            .map(|line| projector.project_coordinates(line))
            .collect();
        ls.and_then(MultiLineString::new)
    }
}

impl<P> Reproject<P> for MultiLineStringRef<'_>
where
    P: CoordinateProjection,
{
    type Out = MultiLineString;
    fn reproject(&self, projector: &P) -> Result<MultiLineString> {
        let ls: Result<Vec<Vec<Coordinate2D>>> = self
            .lines()
            .iter()
            .map(|line| projector.project_coordinates(line))
            .collect();
        ls.and_then(MultiLineString::new)
    }
}

impl<P> Reproject<P> for MultiPolygon
where
    P: CoordinateProjection,
{
    type Out = MultiPolygon;
    fn reproject(&self, projector: &P) -> Result<MultiPolygon> {
        let ls: Result<Vec<Vec<Vec<Coordinate2D>>>> = self
            .polygons()
            .iter()
            .map(|poly| {
                poly.iter()
                    .map(|ring| projector.project_coordinates(ring))
                    .collect()
            })
            .collect();
        ls.and_then(MultiPolygon::new)
    }
}

impl<P> Reproject<P> for MultiPolygonRef<'_>
where
    P: CoordinateProjection,
{
    type Out = MultiPolygon;
    fn reproject(&self, projector: &P) -> Result<MultiPolygon> {
        let ls: Result<Vec<Vec<Vec<Coordinate2D>>>> = self
            .polygons()
            .iter()
            .map(|poly| {
                poly.iter()
                    .map(|ring| projector.project_coordinates(ring))
                    .collect()
            })
            .collect();
        ls.and_then(MultiPolygon::new)
    }
}

impl<P, A> Reproject<P> for A
where
    P: CoordinateProjection,
    A: AxisAlignedRectangle,
{
    type Out = A;
    fn reproject(&self, projector: &P) -> Result<A> {
        const POINTS_PER_LINE: i32 = 7;
        let upper_line = Line::new(self.upper_left(), self.upper_right())
            .with_additional_equi_spaced_coords(POINTS_PER_LINE);
        let right_line = Line::new(self.upper_right(), self.lower_right())
            .with_additional_equi_spaced_coords(POINTS_PER_LINE);
        let lower_line = Line::new(self.lower_right(), self.lower_left())
            .with_additional_equi_spaced_coords(POINTS_PER_LINE);
        let left_line = Line::new(self.lower_left(), self.upper_left())
            .with_additional_equi_spaced_coords(POINTS_PER_LINE);

        let outline_coordinates: Vec<Coordinate2D> = upper_line
            .chain(right_line)
            .chain(lower_line)
            .chain(left_line)
            .collect();

        let proj_outline_coordinates = projector.project_coordinates(outline_coordinates)?;

        let bbox = MultiPoint::new_unchecked(proj_outline_coordinates).spatial_bounds();

        ensure!(
            bbox.size_x() > 0. && bbox.size_y() > 0.,
            error::OutputBboxEmpty { bbox }
        );

        A::from_min_max(bbox.lower_left(), bbox.upper_right())
    }
}

pub trait ReprojectClipped<P: CoordinateProjection> {
    type Out;
    /// Reproject and clip with respect to the area of use of the projection
    fn reproject_clipped(&self, projector: &P) -> Result<Option<Self::Out>>;
}

impl<P, A> ReprojectClipped<P> for A
where
    P: CoordinateProjection,
    A: AxisAlignedRectangle,
{
    type Out = A;
    fn reproject_clipped(&self, projector: &P) -> Result<Option<A>> {
        const POINTS_PER_LINE: i32 = 7;

        // clip bbox to the area of use of the target projection
        let area_of_use_projector = CoordinateProjector::from_known_srs(
            SpatialReference::epsg_4326(),
            projector.source_srs(),
        )?;
        let source_area_of_use = projector.source_srs().area_of_use::<A>()?;
        let target_area_of_use = projector.target_srs().area_of_use::<A>()?;
        let area_of_use = source_area_of_use.intersection(&target_area_of_use);
        let area_of_use_proj = area_of_use
            .map(|use_area| use_area.reproject(&area_of_use_projector))
            .transpose()?;

        let Some(area_of_use_proj) = area_of_use_proj else {
            return Ok(None);
        };

        let clipped_bbox = self.intersection(&area_of_use_proj);

        let Some(clipped_bbox) = clipped_bbox else {
            return Ok(None);
        };

        // project points on the bbox
        let upper_line = Line::new(clipped_bbox.upper_left(), clipped_bbox.upper_right())
            .with_additional_equi_spaced_coords(POINTS_PER_LINE);
        let right_line = Line::new(clipped_bbox.upper_right(), clipped_bbox.lower_right())
            .with_additional_equi_spaced_coords(POINTS_PER_LINE);
        let lower_line = Line::new(clipped_bbox.lower_right(), clipped_bbox.lower_left())
            .with_additional_equi_spaced_coords(POINTS_PER_LINE);
        let left_line = Line::new(clipped_bbox.lower_left(), clipped_bbox.upper_left())
            .with_additional_equi_spaced_coords(POINTS_PER_LINE);

        let outline_coordinates: Vec<Coordinate2D> = upper_line
            .chain(right_line)
            .chain(lower_line)
            .chain(left_line)
            .collect();

        let proj_outline_coordinates: Vec<Coordinate2D> =
            project_coordinates_fail_tolerant(&outline_coordinates, projector)
                .into_iter()
                .flatten()
                .collect();

        let out = MultiPoint::new(proj_outline_coordinates)?.spatial_bounds();

        ensure!(
            out.size_x() > 0. && out.size_y() > 0.,
            error::OutputBboxEmpty { bbox: out }
        );

        Some(A::from_min_max(out.lower_left(), out.upper_right())).transpose()
    }
}

#[inline]
fn euclidian_pixel_distance<B: AxisAlignedRectangle>(
    bbox: B,
    spatial_resolution: SpatialResolution,
) -> Result<f64> {
    ensure!(
        !(bbox.size_x().is_zero() || bbox.size_y().is_zero()),
        error::EmptySpatialBounds {
            lower_left_coordinate: bbox.lower_left(),
            upper_right_coordinate: bbox.upper_right(),
        }
    );

    // calculate the number of pixels per axis
    let x_pixels = (bbox.size_x() / spatial_resolution.x).abs();
    let y_pixels = (bbox.size_y() / spatial_resolution.y).abs();

    let diag_pixels: f64 = (x_pixels * x_pixels + y_pixels * y_pixels).sqrt();
    Ok(diag_pixels)
}

#[inline]
fn projected_diag_distance<P: CoordinateProjection>(
    edge_a: Coordinate2D,
    edge_b: Coordinate2D,
    projector: &P,
) -> Result<f64> {
    // reproject the upper left and lower right coordinates to the target srs.
    // NOTE: the edges of the projected bbox might differ.
    let proj_ul_coord = edge_a.reproject(projector)?;
    let proj_lr_coord = edge_b.reproject(projector)?;

    Ok(diag_distance(proj_ul_coord, proj_lr_coord))
}

#[inline]
fn diag_distance(ul_coord: Coordinate2D, lr_coord: Coordinate2D) -> f64 {
    // calculate the distance between upper left and lower right coordinate in srs units
    let proj_ul_lr_vector = ul_coord - lr_coord;
    (proj_ul_lr_vector.x * proj_ul_lr_vector.x + proj_ul_lr_vector.y * proj_ul_lr_vector.y).sqrt()
}

pub fn suggest_pixel_size_like_gdal_helper<B: AxisAlignedRectangle>(
    bbox: B,
    spatial_resolution: SpatialResolution,
    source_srs: SpatialReference,
    target: SpatialReference,
) -> Result<SpatialResolution> {
    let projector = CoordinateProjector::from_known_srs(source_srs, target)?;

    suggest_pixel_size_like_gdal(bbox, spatial_resolution, &projector)
}

/// This method calculates a suggested pixel size for the translation of a raster into a different projection.
/// The source raster is described using a `BoundingBox2D` and a pixel size as `SpatialResolution`.
/// A suggested pixel size is calculated using the approach used by GDAL:
/// The upper left and the lower right coordinates of the bounding box are projected in the target SRS.
/// Then, the distance between both points in the target SRS is devided by the distance in pixels of the source.
pub fn suggest_pixel_size_like_gdal<P: CoordinateProjection, B: AxisAlignedRectangle>(
    bbox: B,
    spatial_resolution: SpatialResolution,
    projector: &P,
) -> Result<SpatialResolution> {
    let diag_pixels = euclidian_pixel_distance(bbox, spatial_resolution)?;

    let proj_ul_lr_distance =
        projected_diag_distance(bbox.upper_left(), bbox.lower_right(), projector)?;

    // derive the pixel size by deviding srs unit distance by pixel distance in the source bbox
    let proj_ul_lr_pixel_size = proj_ul_lr_distance / diag_pixels;
    Ok(SpatialResolution::new_unchecked(
        proj_ul_lr_pixel_size,
        proj_ul_lr_pixel_size,
    ))
}

pub fn suggest_output_spatial_grid_like_gdal_helper(
    spatial_grid: &SpatialGridDefinition,
    source_srs: SpatialReference,
    target_srs: SpatialReference,
) -> Result<SpatialGridDefinition> {
    let projector = CoordinateProjector::from_known_srs(source_srs, target_srs)?;

    suggest_output_spatial_grid_like_gdal(spatial_grid, &projector)
}

pub fn reproject_spatial_grid_bounds<P: CoordinateProjection, A: AxisAlignedRectangle>(
    spatial_grid: &SpatialGridDefinition,
    projector: &P,
) -> Result<Option<A>> {
    const SAMPLE_POINT_STEPS: usize = 2;

    // First, try to reproject the bounds:
    let full_bounds: std::result::Result<BoundingBox2D, error::Error> = spatial_grid
        .spatial_partition()
        .as_bbox()
        .reproject(projector);

    if let Ok(projected_bounds) = full_bounds {
        let res = A::from_min_max(
            projected_bounds.lower_left(),
            projected_bounds.upper_right(),
        );
        return Some(res).transpose();
    }

    // Second, create a grid of coordinates project that and use the valid bounds.
    // To do this, we generate a `SpatialGridDefinition` ...
    let sample_bounds = SpatialGridDefinition::new(
        spatial_grid.geo_transform,
        GridBoundingBox::new_unchecked(
            spatial_grid.grid_bounds.min_index(),
            spatial_grid.grid_bounds.max_index() + GridIdx2D::new_y_x(1, 1),
        ),
    );
    // Then the the obvious way to generate sample points is to use all the pixels in the grid like this:
    // let coord_grid = spatial_grid.generate_coord_grid_upper_left_edge();
    // However, this creates a lot of redundant points == work.
    // The better way, also employed by GDAL is to use a "Haus vom Nikolaus" strategy which is done below:
    let mut coord_grid_sample = sample_bounds.sample_outline(SAMPLE_POINT_STEPS);
    coord_grid_sample.append(&mut sample_bounds.sample_diagonals(SAMPLE_POINT_STEPS));
    coord_grid_sample.append(&mut sample_bounds.sample_cross(SAMPLE_POINT_STEPS));
    // Then, we try to reproject the sample coordinates and gather all the valid coordinates.
    let proj_outline_coordinates: Vec<Coordinate2D> =
        project_coordinates_fail_tolerant(&coord_grid_sample, projector)
            .into_iter()
            .flatten()
            .collect();
    // TODO: we need a way to indicate that the operator might produce no data, e.g. if no points are valid after reprojection.
    if proj_outline_coordinates.is_empty() {
        return Ok(None);
    }
    // Then, the maximum bounding box is generated from the valid coordinates.
    let out = MultiPoint::new(proj_outline_coordinates)?.spatial_bounds();
    // Finally, the requested bound type is returned.
    Some(A::from_min_max(out.lower_left(), out.upper_right())).transpose()
}

pub fn suggest_output_spatial_grid_like_gdal<P: CoordinateProjection>(
    spatial_grid: &SpatialGridDefinition,
    projector: &P,
) -> Result<SpatialGridDefinition> {
    const ROUND_UP_SIZE: bool = false;

    let in_x_pixels = spatial_grid.grid_bounds().axis_size_x();
    let in_y_pixels = spatial_grid.grid_bounds().axis_size_y();

    let proj_bbox_option: Option<BoundingBox2D> =
        reproject_spatial_grid_bounds(spatial_grid, projector)?;

    let Some(proj_bbox) = proj_bbox_option else {
        return Err(error::Error::NoIntersectionWithTargetProjection {
            srs_in: projector.source_srs(),
            srs_out: projector.target_srs(),
            bounds: spatial_grid.spatial_partition().as_bbox(),
        });
    };

    let out_x_distance = proj_bbox.size_x();
    let out_y_distance = proj_bbox.size_y();

    let out_diagonal_dist =
        (out_x_distance * out_x_distance + out_y_distance * out_y_distance).sqrt();

    let pixel_size =
        out_diagonal_dist / ((in_x_pixels * in_x_pixels + in_y_pixels * in_y_pixels) as f64).sqrt();

    let x_pixels_with_frac = out_x_distance / pixel_size;
    let y_pixels_with_frac = out_y_distance / pixel_size;

    let (x_pixels, y_pixels) = if ROUND_UP_SIZE {
        const EPS_FROM_GDAL: f64 = 1e-5;
        (
            (x_pixels_with_frac - EPS_FROM_GDAL).ceil() as usize,
            (y_pixels_with_frac - EPS_FROM_GDAL).ceil() as usize,
        )
    } else {
        (
            (x_pixels_with_frac + 0.5) as usize,
            (y_pixels_with_frac + 0.5) as usize,
        )
    };

    // TODO: gdal does some magic to fit to the bounds which might change the pixel size again.
    // let x_pixel_size = out_x_distance / x_pixels as f64;
    // let y_pixel_size = out_y_distance / y_pixels as f64;
    let x_pixel_size = pixel_size;
    let y_pixel_size = pixel_size;

    let geo_transform = GeoTransform::new(proj_bbox.upper_left(), x_pixel_size, -y_pixel_size);
    let grid_bounds = GridShape::new_2d(y_pixels, x_pixels).bounding_box();
    let out_spatial_grid = SpatialGridDefinition::new(geo_transform, grid_bounds);

    // if the input grid is anchored at the upper left idx then we don't have to move the origin of the geo transform
    if spatial_grid.grid_bounds.min_index() == GridIdx([0, 0]) {
        return Ok(SpatialGridDefinition::new(geo_transform, grid_bounds));
    }

    let proj_origin = spatial_grid
        .geo_transform()
        .origin_coordinate()
        .reproject(projector)?;

    let out_spatial_grid_moved_origin =
        out_spatial_grid.with_moved_origin_to_nearest_grid_edge(proj_origin);

    Ok(out_spatial_grid_moved_origin.replace_origin(proj_origin))
}

pub fn suggest_pixel_size_from_diag_cross_helper<B: AxisAlignedRectangle>(
    bbox: B,
    spatial_resolution: SpatialResolution,
    source_srs: SpatialReference,
    target: SpatialReference,
) -> Result<SpatialResolution> {
    let projector = CoordinateProjector::from_known_srs(source_srs, target)?;

    suggest_pixel_size_from_diag_cross(bbox, spatial_resolution, &projector)
}

/// This approach uses the GDAL way to suggest the pixel size. However, we check both diagonals and take the smaller one.
/// This method fails if the bbox cannot be projected
pub fn suggest_pixel_size_from_diag_cross<P: CoordinateProjection, B: AxisAlignedRectangle>(
    bbox: B,
    spatial_resolution: SpatialResolution,
    projector: &P,
) -> Result<SpatialResolution> {
    let diag_pixels = euclidian_pixel_distance(bbox, spatial_resolution)?;

    let proj_ul_lr_distance =
        projected_diag_distance(bbox.upper_left(), bbox.lower_right(), projector);

    let proj_ll_ur_distance: std::prelude::v1::Result<f64, error::Error> =
        projected_diag_distance(bbox.lower_left(), bbox.upper_right(), projector);

    let min_dist_r = match (proj_ul_lr_distance, proj_ll_ur_distance) {
        (Ok(ul_lr), Ok(ll_ur)) => Ok(ul_lr.min(ll_ur)),
        (Ok(ul_lr), Err(_)) => Ok(ul_lr),
        (Err(_), Ok(ll_ur)) => Ok(ll_ur),
        (Err(e), Err(_)) => Err(e),
    };

    min_dist_r.map(|d| SpatialResolution::new_unchecked(d / diag_pixels, d / diag_pixels))
}

/// A version of `suggest_pixel_size_from_diag_cross` that takes a `partition` and a projected counterpart as input
/// The `spatial_resolution` corresponds to the `bbox`. The output of the function is the suggested resolution for
/// the `bbox_projected` rectangle.
pub fn suggest_pixel_size_from_diag_cross_projected<B: AxisAlignedRectangle>(
    bbox: B,
    bbox_projected: B,
    spatial_resolution: SpatialResolution,
) -> Result<SpatialResolution> {
    let diag_pixels = euclidian_pixel_distance(bbox, spatial_resolution)?;

    let proj_ul_lr_distance =
        diag_distance(bbox_projected.upper_left(), bbox_projected.lower_right());

    let proj_ll_ur_distance =
        diag_distance(bbox_projected.lower_left(), bbox_projected.upper_right());

    let min_dist_r = proj_ul_lr_distance.min(proj_ll_ur_distance);

    Ok(SpatialResolution::new_unchecked(
        min_dist_r / diag_pixels,
        min_dist_r / diag_pixels,
    ))
}

/// Tries to reproject all coordinates at once. If this fails, tries to reproject coordinate by coordinate.
/// It returns all coordinates in input order.
/// In case of success it returns `Some(Coordinate2D)` and `None` otherwise.
pub fn project_coordinates_fail_tolerant<P: CoordinateProjection>(
    i: &[Coordinate2D],
    p: &P,
) -> Vec<Option<Coordinate2D>> {
    if let Ok(projected_all) = p.project_coordinates(i) {
        return projected_all
            .into_iter()
            .map(Some)
            .collect::<Vec<Option<Coordinate2D>>>();
    }

    let individual_projected: Vec<Option<Coordinate2D>> = i
        .iter()
        .map(|&c| (c, c.reproject(p)))
        //.inspect(|(c, c_p)| {
        //    dbg!(c, c_p);
        //})
        .map(|(_, c_p)| c_p.ok())
        .collect();
    // For debuging use this to find oput how many coordinates could be transformed.
    //dbg!(
    //    individual_projected.iter().filter(|c| c.is_some()).count(),
    //    i.len()
    //);
    individual_projected
}

/// this method performs the transformation of a query rectangle in `target` projection
/// to a new query rectangle with coordinates in the `source` projection
pub fn reproject_spatial_query<S: AxisAlignedRectangle>(
    spatial_bounds: S,
    source: SpatialReference,
    target: SpatialReference,
    force_clipping: bool,
) -> Result<Option<S>> {
    let (Some(_s_bbox), Some(p_bbox)) =
        reproject_and_unify_bbox_internal(spatial_bounds, target, source, force_clipping)?
    else {
        return Ok(None);
    };

    Ok(Some(p_bbox))
}

/// Reproject a bounding box to the `target` projection and return the input and output bounding box
/// as a pair where both elements cover the same area of the original `source_bbox` in WGS84.
/// The pair is structured as `(source_bbox_clipped, target_bbox_clipped)`
pub fn reproject_and_unify_bbox<T: AxisAlignedRectangle>(
    source_bbox: T,
    source: SpatialReference,
    target: SpatialReference,
) -> Result<(Option<T>, Option<T>)> {
    reproject_and_unify_bbox_internal(source_bbox, source, target, true)
}

fn reproject_and_unify_bbox_internal<T: AxisAlignedRectangle>(
    source_bbox: T,
    source: SpatialReference,
    target: SpatialReference,
    force_clipping: bool,
) -> Result<(Option<T>, Option<T>)> {
    let proj_from_to = CoordinateProjector::from_known_srs(source, target)?;
    let proj_to_from = CoordinateProjector::from_known_srs(target, source)?;

    let target_bbox_clipped = source_bbox.reproject_clipped(&proj_from_to)?;

    if force_clipping {
        if let Some(target_b_clipped) = target_bbox_clipped {
            // If the non-clipped bbox reprojection failed, we use the clipped bbox
            let source_bbox_clipped = target_b_clipped.reproject(&proj_to_from)?;
            return Ok((Some(source_bbox_clipped), target_bbox_clipped));
        }
        return Ok((None, None));
    }

    let target_bbox_non_clipped = source_bbox.reproject(&proj_from_to).ok();

    if let (Some(target_b_clipped), Some(target_b_non_clipped)) =
        (target_bbox_clipped, target_bbox_non_clipped)
    {
        // If both bbox reprojections succeeded, we combine them
        let source_bbox_clipped = target_b_clipped.reproject(&proj_to_from)?;
        let source_bbox_non_clipped = target_b_non_clipped.reproject(&proj_to_from)?;
        let source_bbox_union = source_bbox_clipped.union(&source_bbox_non_clipped);
        let target_bbox_union = target_b_clipped.union(&target_b_non_clipped);
        Ok((Some(source_bbox_union), Some(target_bbox_union)))
    } else if let Some(target_b_clipped) = target_bbox_clipped {
        // If the non-clipped bbox reprojection failed, we use the clipped bbox
        let source_bbox_clipped = target_b_clipped.reproject(&proj_to_from)?;
        Ok((Some(source_bbox_clipped), target_bbox_clipped))
    } else if let Some(target_b_non_clipped) = target_bbox_non_clipped {
        // If the clipped bbox reprojection failed, we use the non-clipped bbox
        let source_bbox_non_clipped = target_b_non_clipped.reproject(&proj_to_from)?;
        Ok((Some(source_bbox_non_clipped), target_bbox_non_clipped))
    } else {
        // If both bbox reprojections failed, we return None
        Ok((None, None))
    }
}

/// Reproject the area of use of the `source` projection to the `target` projection and back. Return the back projected bounds and the area of use in the `target` projection.
pub fn reproject_and_unify_proj_bounds<T: AxisAlignedRectangle>(
    source: SpatialReference,
    target: SpatialReference,
) -> Result<(Option<T>, Option<T>)> {
    let proj_from_to = CoordinateProjector::from_known_srs(source, target)?;
    let proj_to_from = CoordinateProjector::from_known_srs(target, source)?;

    let target_bbox_clipped = source
        .area_of_use_projected::<T>()?
        .reproject_clipped(&proj_from_to)?; // TODO: can we intersect areas of use first?

    if let Some(target_b) = target_bbox_clipped {
        let source_bbox_clipped = target_b.reproject(&proj_to_from)?;
        Ok((Some(source_bbox_clipped), target_bbox_clipped))
    } else {
        Ok((None, None))
    }
}

#[cfg(test)]
mod tests {

    use crate::primitives::{BoundingBox2D, SpatialPartition2D};
    use crate::spatial_reference::SpatialReferenceAuthority;
    use crate::util::well_known_data::{
        COLOGNE_EPSG_900_913, COLOGNE_EPSG_4326, HAMBURG_EPSG_900_913, HAMBURG_EPSG_4326,
        MARBURG_EPSG_900_913, MARBURG_EPSG_4326,
    };
    use float_cmp::approx_eq;

    use super::*;

    #[test]
    fn new_proj() {
        let from = SpatialReference::epsg_4326();
        let to = SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);
        let p = CoordinateProjector::from_known_srs(from, to);
        assert!(p.is_ok());
    }

    #[test]
    fn new_proj_fail() {
        let from = SpatialReference::epsg_4326();
        let to = SpatialReference::new(SpatialReferenceAuthority::Epsg, 8_008_135);
        let p = CoordinateProjector::from_known_srs(from, to);
        assert!(p.is_err());
    }

    #[test]
    fn proj_coordinate_4326_900913() {
        let from = SpatialReference::epsg_4326();
        let to = SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);
        let p = CoordinateProjector::from_known_srs(from, to).unwrap();
        let rp = p.project_coordinate(MARBURG_EPSG_4326).unwrap();

        assert!(approx_eq!(f64, rp.x, MARBURG_EPSG_900_913.x));
        assert!(approx_eq!(f64, rp.y, MARBURG_EPSG_900_913.y));
    }

    #[test]
    fn reproject_coordinate_4326_900913() {
        let from = SpatialReference::epsg_4326();
        let to = SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);
        let p = CoordinateProjector::from_known_srs(from, to).unwrap();
        let rp = MARBURG_EPSG_4326.reproject(&p).unwrap();

        assert!(approx_eq!(f64, rp.x, MARBURG_EPSG_900_913.x));
        assert!(approx_eq!(f64, rp.y, MARBURG_EPSG_900_913.y));
    }

    #[test]
    fn reproject_line_4326_900913() {
        let from = SpatialReference::epsg_4326();
        let to = SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);
        let p = CoordinateProjector::from_known_srs(from, to).unwrap();

        let l = Line {
            start: MARBURG_EPSG_4326,
            end: COLOGNE_EPSG_4326,
        };
        let rl = l.reproject(&p).unwrap();

        assert!(approx_eq!(f64, rl.start.x, MARBURG_EPSG_900_913.x));
        assert!(approx_eq!(f64, rl.start.y, MARBURG_EPSG_900_913.y));
        assert!(approx_eq!(f64, rl.end.x, COLOGNE_EPSG_900_913.x));
        assert!(approx_eq!(f64, rl.end.y, COLOGNE_EPSG_900_913.y));
    }

    #[test]
    fn reproject_bounding_box_4326_900913() {
        let from = SpatialReference::epsg_4326();
        let to = SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);
        let p = CoordinateProjector::from_known_srs(from, to).unwrap();

        let bbox =
            BoundingBox2D::from_coord_ref_iter(&[MARBURG_EPSG_4326, COLOGNE_EPSG_4326]).unwrap();

        let rl = bbox.reproject(&p).unwrap();

        assert!(approx_eq!(f64, rl.lower_left().x, COLOGNE_EPSG_900_913.x));
        assert!(approx_eq!(f64, rl.lower_left().y, MARBURG_EPSG_900_913.y));
        assert!(approx_eq!(f64, rl.upper_right().x, MARBURG_EPSG_900_913.x));
        assert!(approx_eq!(f64, rl.upper_right().y, COLOGNE_EPSG_900_913.y));
    }

    #[test]
    fn reproject_multi_point_4326_900913() {
        let from = SpatialReference::epsg_4326();
        let to = SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);
        let p = CoordinateProjector::from_known_srs(from, to).unwrap();

        let cs = vec![MARBURG_EPSG_4326, COLOGNE_EPSG_4326];

        let mp = MultiPoint::new(cs).unwrap();
        let rp = mp.reproject(&p).unwrap();

        assert!(approx_eq!(f64, rp.points()[0].x, MARBURG_EPSG_900_913.x));
        assert!(approx_eq!(f64, rp.points()[0].y, MARBURG_EPSG_900_913.y));
        assert!(approx_eq!(f64, rp.points()[1].x, COLOGNE_EPSG_900_913.x));
        assert!(approx_eq!(f64, rp.points()[1].y, COLOGNE_EPSG_900_913.y));
    }

    #[test]
    fn reproject_multi_line_4326_900913() {
        let from = SpatialReference::epsg_4326();
        let to = SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);
        let p = CoordinateProjector::from_known_srs(from, to).unwrap();

        let cs = vec![vec![
            MARBURG_EPSG_4326,
            COLOGNE_EPSG_4326,
            HAMBURG_EPSG_4326,
        ]];

        let mp = MultiLineString::new(cs).unwrap();
        let rp = mp.reproject(&p).unwrap();

        assert!(approx_eq!(f64, rp.lines()[0][0].x, MARBURG_EPSG_900_913.x));
        assert!(approx_eq!(f64, rp.lines()[0][0].y, MARBURG_EPSG_900_913.y));
        assert!(approx_eq!(f64, rp.lines()[0][1].x, COLOGNE_EPSG_900_913.x));
        assert!(approx_eq!(f64, rp.lines()[0][1].y, COLOGNE_EPSG_900_913.y));
        assert!(approx_eq!(f64, rp.lines()[0][2].x, HAMBURG_EPSG_900_913.x));
        assert!(approx_eq!(f64, rp.lines()[0][2].y, HAMBURG_EPSG_900_913.y));
    }

    #[test]
    fn reproject_multi_polygon_4326_900913() {
        let from = SpatialReference::epsg_4326();
        let to = SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);
        let p = CoordinateProjector::from_known_srs(from, to).unwrap();

        let cs = vec![vec![vec![
            MARBURG_EPSG_4326,
            COLOGNE_EPSG_4326,
            HAMBURG_EPSG_4326,
            MARBURG_EPSG_4326,
        ]]];

        let mp = MultiPolygon::new(cs).unwrap();
        let rp = mp.reproject(&p).unwrap();

        assert!(approx_eq!(
            f64,
            rp.polygons()[0][0][0].x,
            MARBURG_EPSG_900_913.x
        ));
        assert!(approx_eq!(
            f64,
            rp.polygons()[0][0][0].y,
            MARBURG_EPSG_900_913.y
        ));
        assert!(approx_eq!(
            f64,
            rp.polygons()[0][0][1].x,
            COLOGNE_EPSG_900_913.x
        ));
        assert!(approx_eq!(
            f64,
            rp.polygons()[0][0][1].y,
            COLOGNE_EPSG_900_913.y
        ));
        assert!(approx_eq!(
            f64,
            rp.polygons()[0][0][2].x,
            HAMBURG_EPSG_900_913.x
        ));
        assert!(approx_eq!(
            f64,
            rp.polygons()[0][0][2].y,
            HAMBURG_EPSG_900_913.y
        ));
    }

    #[test]
    fn reproject_clipped_bbox_4326_3857() {
        let bbox = BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into());
        let p = CoordinateProjector::from_known_srs(
            SpatialReference::epsg_4326(),
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857),
        )
        .unwrap();

        let projected = bbox.reproject_clipped(&p).unwrap().unwrap();
        let expected = BoundingBox2D::new_unchecked(
            (-20_037_508.342_789_244, -20_048_966.104_014_6).into(),
            (20_037_508.342_789_244, 20_048_966.104_014_594).into(),
        );

        assert!(approx_eq!(
            BoundingBox2D,
            projected,
            expected,
            epsilon = 0.000_001
        ));
    }

    #[test]
    fn reproject_clipped_bbox_3857_900913() {
        let bbox = BoundingBox2D::new_unchecked(
            (-20_037_508.342_789_244, -20_048_966.104_014_6).into(),
            (20_037_508.342_789_244, 20_048_966.104_014_594).into(),
        );
        let p = CoordinateProjector::from_known_srs(
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857),
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913),
        )
        .unwrap();

        let projected = bbox.reproject_clipped(&p).unwrap().unwrap();
        let expected = BoundingBox2D::new_unchecked(
            (-20_037_508.342_789_244, -20_048_966.104_014_6).into(),
            (20_037_508.342_789_244, 20_048_966.104_014_594).into(),
        );

        assert!(approx_eq!(
            BoundingBox2D,
            projected,
            expected,
            epsilon = 0.000_001
        ));
    }

    #[test]
    fn suggest_pixel_size_gdal() {
        // This test uses the specs of the SRTM tile "srtm_38_03.tif"
        let ul_c = (5.0, 50.0).into();
        let lr_c = (10.0, 45.0).into();
        let x_pixels: f64 = 6000.;
        let y_pixels = 6000.;

        let bbox = BoundingBox2D::new_upper_left_lower_right(ul_c, lr_c).unwrap();

        let spatial_resolution =
            SpatialResolution::new_unchecked(bbox.size_x() / x_pixels, bbox.size_y() / y_pixels);

        assert!(approx_eq!(
            f64,
            spatial_resolution.x,
            0.000_833_333,
            epsilon = 0.000_000_1
        ));
        assert!(approx_eq!(
            f64,
            spatial_resolution.y,
            0.000_833_333,
            epsilon = 0.000_000_1
        ));

        let projector = CoordinateProjector::from_known_srs(
            SpatialReference::epsg_4326(),
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632), //EPSG4326 --> UTM 32 N
        )
        .unwrap();

        let sugg_pixel_size =
            suggest_pixel_size_like_gdal(bbox, spatial_resolution, &projector).unwrap();

        assert!(approx_eq!(f64, sugg_pixel_size.x, sugg_pixel_size.y));
        assert!(approx_eq!(
            f64,
            sugg_pixel_size.x,
            79.088_974_450_690_5, // this is the pixel size GDAL generates when reprojecting the SRTM tile.
            epsilon = 0.000_000_1
        ));
    }

    #[test]
    fn suggest_pixel_size_cross() {
        // This test uses the specs of the SRTM tile "srtm_38_03.tif"
        let ul_c = (5.0, 50.0).into();
        let lr_c = (10.0, 45.0).into();
        let x_pixels: f64 = 6000.;
        let y_pixels = 6000.;

        let bbox = BoundingBox2D::new_upper_left_lower_right(ul_c, lr_c).unwrap();

        let spatial_resolution =
            SpatialResolution::new_unchecked(bbox.size_x() / x_pixels, bbox.size_y() / y_pixels);

        assert!(approx_eq!(
            f64,
            spatial_resolution.x,
            0.000_833_333,
            epsilon = 0.000_000_1
        ));
        assert!(approx_eq!(
            f64,
            spatial_resolution.y,
            0.000_833_333,
            epsilon = 0.000_000_1
        ));

        let projector = CoordinateProjector::from_known_srs(
            SpatialReference::epsg_4326(),
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632), //EPSG4326 --> UTM 32 N
        )
        .unwrap();

        let sugg_pixel_size =
            suggest_pixel_size_from_diag_cross(bbox, spatial_resolution, &projector).unwrap();

        assert!(approx_eq!(f64, sugg_pixel_size.x, sugg_pixel_size.y));
        assert!(approx_eq!(
            f64,
            sugg_pixel_size.x,
            79.088_974_450_690_5, // this is the pixel size GDAL generates when reprojecting the SRTM tile.
            epsilon = 0.000_000_1
        ));
    }

    #[test]
    fn it_reprojects_and_unifies_bbox() {
        let bbox = SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());

        let (input, output) = reproject_and_unify_bbox_internal(
            bbox,
            SpatialReference::epsg_4326(),
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857),
            false,
        )
        .unwrap();

        assert_eq!(
            input.unwrap(),
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into())
        );

        assert_eq!(
            output.unwrap(),
            SpatialPartition2D::new_unchecked(
                (-20_037_508.342_789_244, 242_528_680.943_742_72).into(),
                (20_037_508.342_789_244, -242_528_680.943_742_72).into()
            )
        );

        let (input, output) = reproject_and_unify_bbox_internal(
            bbox,
            SpatialReference::epsg_4326(),
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857),
            true,
        )
        .unwrap();

        assert_eq!(
            input.unwrap(),
            SpatialPartition2D::new_unchecked((-180., 85.06).into(), (180., -85.06).into())
        );

        assert_eq!(
            output.unwrap(),
            SpatialPartition2D::new_unchecked(
                (-20_037_508.342_789_244, 20_048_966.104_014_594).into(),
                (20_037_508.342_789_244, -20_048_966.104_014_594).into()
            )
        );

        let (input, output) = reproject_and_unify_bbox_internal(
            bbox,
            SpatialReference::epsg_4326(),
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 25832),
            false,
        )
        .unwrap();

        assert_eq!(
            input.unwrap(),
            SpatialPartition2D::new_unchecked(
                (-179.534_226_944_867_13, 84.365_580_084_111_7).into(),
                (180., -73.092_416_467_242_37).into()
            )
        );

        assert_eq!(
            output.unwrap(),
            SpatialPartition2D::new_unchecked(
                (239_323.444_975_331_9, 19_995_929.885_877_542).into(),
                (1_505_646.899_515_872, -17_479_695.521_371_62).into()
            )
        );

        let (input, output) = reproject_and_unify_bbox_internal(
            bbox,
            SpatialReference::epsg_4326(),
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 25832),
            true,
        )
        .unwrap();

        assert_eq!(
            input.unwrap(),
            SpatialPartition2D::new_unchecked(
                (-13.447_614_202_942_217, 84.337_770_726_671_31).into(),
                (31.515_032_504_485_223, 38.721_273_539_513_95).into()
            )
        );

        assert_eq!(
            output.unwrap(),
            SpatialPartition2D::new_unchecked(
                (239_323.444_975_331_9, 9_365_801.909_364_037).into(),
                (761_545.650_790_771_2, 4_290_144.085_983_968).into()
            )
        );
    }
}
