use proj::Proj;

use crate::{
    error,
    primitives::{
        BoundingBox2D, Coordinate2D, Line, MultiLineString, MultiLineStringAccess,
        MultiLineStringRef, MultiPoint, MultiPointAccess, MultiPointRef, MultiPolygon,
        MultiPolygonAccess, MultiPolygonRef, SpatialBounded,
    },
    spatial_reference::SpatialReference,
    util::Result,
};

pub trait CoordinateProjection {
    fn from_known_srs(from: SpatialReference, to: SpatialReference) -> Result<Self>
    where
        Self: Sized;
    fn project_coordinate(&self, c: Coordinate2D) -> Result<Coordinate2D>;

    // TODO: add for performance
    // fn project_coordinate_slice_inplace(&self, coords: &mut [Coordinate2D]) -> Result<&mut [Coordinate2D]>;
    fn project_coordinate_slice_copy<A: AsRef<[Coordinate2D]>>(
        &self,
        coords: A,
    ) -> Result<Vec<Coordinate2D>>;
}

pub struct CoordinateProjector {
    pub from: SpatialReference,
    pub to: SpatialReference,
    p: Proj,
}

// TODO: move Proj impl into a separate module?
impl CoordinateProjection for CoordinateProjector {
    fn from_known_srs(from: SpatialReference, to: SpatialReference) -> Result<Self> {
        let p = Proj::new_known_crs(&from.to_string(), &to.to_string(), None)
            .ok_or(error::Error::NoCoordinateProjector { from, to })?;
        Ok(CoordinateProjector { from, to, p })
    }

    fn project_coordinate(&self, c: Coordinate2D) -> Result<Coordinate2D> {
        self.p.convert(c).map_err(Into::into)
    }

    fn project_coordinate_slice_copy<A: AsRef<[Coordinate2D]>>(
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
        let ps: Result<Vec<Coordinate2D>> = projector.project_coordinate_slice_copy(self.points());
        ps.and_then(MultiPoint::new)
    }
}

impl<'g, P> Reproject<P> for MultiPointRef<'g>
where
    P: CoordinateProjection,
{
    type Out = MultiPoint;
    fn reproject(&self, projector: &P) -> Result<MultiPoint> {
        let ps: Result<Vec<Coordinate2D>> = projector.project_coordinate_slice_copy(self.points());
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
            .map(|line| projector.project_coordinate_slice_copy(line))
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
            .map(|line| projector.project_coordinate_slice_copy(line))
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
                    .map(|ring| projector.project_coordinate_slice_copy(ring))
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
                    .map(|ring| projector.project_coordinate_slice_copy(ring))
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
        dbg!(&mp, &rp);

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
}
