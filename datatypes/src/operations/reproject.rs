use proj::Proj;

use crate::{
    error,
    primitives::{
        BoundingBox2D, Coordinate2D, Line, MultiLineString, MultiLineStringAccess,
        MultiLineStringRef, MultiPoint, MultiPointAccess, MultiPointRef, MultiPolygon,
        MultiPolygonAccess, MultiPolygonRef, SpatialBounded, SpatialResolution,
    },
    raster::{
        CoordinatePixelAccess, GridBoundingBox2D, GridBounds, GridIdx, GridIdx2D,
        GridIndexAccessMut, Pixel, RasterTile2D,
    },
    spatial_reference::SpatialReference,
    util::Result,
};

pub trait CoordinateProjection {
    fn from_known_srs(from: SpatialReference, to: SpatialReference) -> Result<Self>
    where
        Self: Sized;
    /// project a single coord
    fn project_coordinate(&self, c: Coordinate2D) -> Result<Coordinate2D>;

    /// project a set of coords
    fn project_coordinates<A: AsRef<[Coordinate2D]>>(&self, coords: A)
        -> Result<Vec<Coordinate2D>>;
}

pub struct CoordinateProjector {
    pub from: SpatialReference,
    pub to: SpatialReference,
    p: Proj,
}

impl CoordinateProjection for CoordinateProjector {
    fn from_known_srs(from: SpatialReference, to: SpatialReference) -> Result<Self> {
        let p = Proj::new_known_crs(&from.to_string(), &to.to_string(), None)
            .ok_or(error::Error::NoCoordinateProjector { from, to })?;
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
}

impl Clone for CoordinateProjector {
    fn clone(&self) -> Self {
        CoordinateProjector {
            from: self.from,
            to: self.to,
            p: Proj::new_known_crs(&self.from.to_string(), &self.to.to_string(), None)
                .expect("worked before"),
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

impl<'g, P> Reproject<P> for MultiPointRef<'g>
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

impl<'g, P> Reproject<P> for MultiLineStringRef<'g>
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

impl<'g, P> Reproject<P> for MultiPolygonRef<'g>
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

impl<P> Reproject<P> for BoundingBox2D
where
    P: CoordinateProjection,
{
    type Out = BoundingBox2D;
    fn reproject(&self, projector: &P) -> Result<BoundingBox2D> {
        const POINTS_PER_LINE: i32 = 7;
        let upper_line = Line::new(self.upper_left(), self.upper_right())
            .with_additional_equi_spaced_coords(POINTS_PER_LINE);
        let right_line = Line::new(self.upper_right(), self.lower_right())
            .with_additional_equi_spaced_coords(POINTS_PER_LINE);
        let lower_line = Line::new(self.lower_right(), self.lower_left())
            .with_additional_equi_spaced_coords(POINTS_PER_LINE);
        let left_line = Line::new(self.lower_left(), self.upper_left())
            .with_additional_equi_spaced_coords(POINTS_PER_LINE);

        let cs: Vec<Coordinate2D> = upper_line
            .chain(right_line)
            .chain(lower_line)
            .chain(left_line)
            .collect();

        MultiPoint::new_unchecked(cs)
            .reproject(projector)
            .map(|mp| mp.spatial_bounds())
    }
}

/// This method calculates a suggested pixel size for the translation of a raster into a different projection.
/// The source raster is described using a `BoundingBox2D` and a pixel size as `SpatialResolution`.
/// A suggested pixel size is calculated using the approach used by GDAL:
/// The upper left and the lower right coordinates of the bounding box are projected in the target SRS.
/// Then, the distance between both points in the target SRS is devided by the distance in pixels of the source.
pub fn suggest_pixel_size_like_gdal<P: CoordinateProjection>(
    bbox: BoundingBox2D,
    spatial_resolution: SpatialResolution,
    projector: &P,
) -> Result<SpatialResolution> {
    // calculate the number of pixels per axis
    let x_pixels = (bbox.size_x() / spatial_resolution.x).abs();
    let y_pixels = (bbox.size_y() / spatial_resolution.y).abs();
    // get the diagonal distance between bbox edges
    let diag_pixels: f64 = (x_pixels * x_pixels + y_pixels * y_pixels).sqrt();

    // reproject the upper left and lower right coordinates to the target srs.
    // NOTE: the edges of the projected bbox might differ.
    let proj_ul_coord = bbox.upper_left().reproject(projector)?;
    let proj_lr_coord = bbox.lower_right().reproject(projector)?;

    // calculate the distance between upper left and lower right coordinate in srs units
    let proj_ul_lr_vector = proj_ul_coord - proj_lr_coord;
    let proj_ul_lr_distance = (proj_ul_lr_vector.x * proj_ul_lr_vector.x
        + proj_ul_lr_vector.y * proj_ul_lr_vector.y)
        .sqrt();

    // derive the pixel size by deviding srs unit distance by pixel distance in the source bbox
    let proj_ul_lr_pixel_size = proj_ul_lr_distance / diag_pixels;
    Ok(SpatialResolution::new_unchecked(
        proj_ul_lr_pixel_size,
        proj_ul_lr_pixel_size,
    ))
}

/// This approach uses the GDAL way to suggest the pixel size. However, we check both diagonals and take the smaller one.
pub fn suggest_pixel_size_from_diag_cross<P: CoordinateProjection>(
    bbox: BoundingBox2D,
    spatial_resolution: SpatialResolution,
    projector: &P,
) -> Result<SpatialResolution> {
    // calculate the number of pixels per axis
    let x_pixels = (bbox.size_x() / spatial_resolution.x).abs();
    let y_pixels = (bbox.size_y() / spatial_resolution.y).abs();

    let diag_pixels: f64 = (x_pixels * x_pixels + y_pixels * y_pixels).sqrt();

    let proj_ul_coord = bbox.upper_left().reproject(projector)?;
    let proj_lr_coord = bbox.lower_right().reproject(projector)?;

    let proj_ul_lr_vector = proj_ul_coord - proj_lr_coord;
    let proj_ul_lr_distance = (proj_ul_lr_vector.x * proj_ul_lr_vector.x
        + proj_ul_lr_vector.y * proj_ul_lr_vector.y)
        .sqrt();

    let proj_ul_lr_pixel_size = proj_ul_lr_distance / diag_pixels;

    let proj_ll_coord = bbox.lower_left().reproject(projector)?;
    let proj_ur_coord = bbox.upper_right().reproject(projector)?;

    let proj_ll_ur_vector = proj_ll_coord - proj_ur_coord;
    let proj_ll_ur_distance = (proj_ll_ur_vector.x * proj_ll_ur_vector.x
        + proj_ll_ur_vector.y * proj_ll_ur_vector.y)
        .sqrt();

    let proj_ll_ur_pixel_size = proj_ll_ur_distance / diag_pixels;

    let min_pixel_size = proj_ll_ur_pixel_size.min(proj_ul_lr_pixel_size);

    Ok(SpatialResolution::new_unchecked(
        min_pixel_size,
        min_pixel_size,
    ))
}

pub fn insert_projected_pixels<T: Pixel, P: CoordinateProjection>(
    target: &mut RasterTile2D<T>,
    source: &RasterTile2D<T>,
    projector: &P,
    inverse_projector: &P,
) -> Result<()> {
    // transform the source bounds into the crs of the target to check if they intersect
    let bounds_in_target = source.spatial_bounds().reproject(inverse_projector)?;

    // intersect both tiles. Only if there is an intersection, we need to do something.
    if let Some(intersection) = target.spatial_bounds().intersection(&bounds_in_target) {
        // get the targets geo transform.
        let tgf = target.tile_geo_transform();
        // derive a no_data_value for pixels without a "partner".
        let no_data_value = target
            .grid_array
            .no_data_value
            .unwrap_or_else(|| T::from_(0.0));
        // calculate the pixel area in the target tile we can update with the source tile.
        let px_bounds = GridBoundingBox2D::new(
            tgf.coordinate_to_grid_idx_2d(intersection.upper_left()),
            tgf.coordinate_to_grid_idx_2d(intersection.lower_right()),
        )?;
        // get the pixels idxses
        let GridIdx([y_s, x_s]) = px_bounds.min_index();
        let GridIdx([y_e, x_e]) = px_bounds.max_index();

        // now for each line do the following:
        for y in y_s..y_e {
            // generate an iterator of all pixel idxs in the line. (this is still relative to the tile geo transform).

            // store the pixels and the pixel coordinates as individual Vec.
            let (t_l_idx, t_coords) = (x_s..x_e)
                .map(move |x| [y, x])
                .map(GridIdx::from)
                // map the idxs to coordinates
                .map(|local_px_idx| (local_px_idx, tgf.grid_idx_to_coordinate_2d(local_px_idx)))
                .unzip::<GridIdx2D, Coordinate2D, Vec<GridIdx2D>, Vec<Coordinate2D>>();

            // try to transform all pixel coords at once..
            if let Ok(s_scoords) = projector.project_coordinates(t_coords) {
                // if it works, get the pixel values from the source tile, nodata will stay the same value.
                let s_px_values = s_scoords
                    .iter()
                    // if we can not access a pixel, fill it with nodata. TODO: is this even possible?
                    .map(|&c| source.pixel_value_at_coord(c).ok().unwrap_or(no_data_value));

                // now zip pixel idx (target) and pixel values (source) and insert the values in target.
                t_l_idx
                    .iter()
                    .zip(s_px_values)
                    .try_for_each(|(&idx, v)| target.set_at_grid_index(idx, v))?;
            } else {
                dbg!("could not transform all coords, fallback to  single coords here?");
            }
        }
    } else {
        dbg!("there was no intersection");
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use crate::spatial_reference::SpatialReferenceAuthority;
    use crate::util::well_known_data::{
        COLOGNE_EPSG_4326, COLOGNE_EPSG_900_913, HAMBURG_EPSG_4326, HAMBURG_EPSG_900_913,
        MARBURG_EPSG_4326, MARBURG_EPSG_900_913,
    };
    use float_cmp::approx_eq;

    use super::*;

    #[test]
    fn new_proj() {
        let from = SpatialReference::epsg_4326();
        let to = SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);
        let p = CoordinateProjector::from_known_srs(from, to);
        assert!(p.is_ok())
    }

    #[test]
    fn new_proj_fail() {
        let from = SpatialReference::epsg_4326();
        let to = SpatialReference::new(SpatialReferenceAuthority::Epsg, 8_008_135);
        let p = CoordinateProjector::from_known_srs(from, to);
        assert!(p.is_err())
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
        ))
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
        ))
    }
}
