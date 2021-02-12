use proj::Proj;

use crate::{
    primitives::{
        BoundingBox2D, Coordinate2D, Line, MultiLineString, MultiLineStringAccess,
        MultiLineStringRef, MultiPoint, MultiPointAccess, MultiPolygon, MultiPolygonAccess,
        MultiPolygonRef, SpatialBounded,
    },
    spatial_reference::SpatialReference,
    util::Result,
};

pub trait CoordinateProjector {
    type Projector: CoordinateProjector;
    fn from_known_srs(from: SpatialReference, to: SpatialReference) -> Result<Self::Projector>;
    fn project_coordinate(&self, c: Coordinate2D) -> Result<Coordinate2D>;
}

impl CoordinateProjector for Proj {
    type Projector = Proj;

    fn from_known_srs(from: SpatialReference, to: SpatialReference) -> Result<Self> {
        dbg!(from, to);
        Ok(
            Proj::new_known_crs(&from.to_string(), &to.to_string(), None)
                .expect("handle ProjError"),
        )
    }

    fn project_coordinate(&self, c: Coordinate2D) -> Result<Coordinate2D> {
        Ok(self.convert((c.x, c.y))?.into())
    }
}

pub trait Reproject<P: CoordinateProjector> {
    type Out;
    fn reproject(&self, projector: &P) -> Result<Self::Out>;
}

impl<P, A> Reproject<P> for A
where
    A: MultiPointAccess,
    P: CoordinateProjector,
{
    type Out = MultiPoint;
    fn reproject(&self, projector: &P) -> Result<MultiPoint> {
        let ps: Result<Vec<Coordinate2D>> = self
            .points()
            .iter()
            .map(|&c| projector.project_coordinate(c))
            .collect();
        ps.and_then(MultiPoint::new)
    }
}

impl<P> Reproject<P> for MultiLineString
where
    P: CoordinateProjector,
{
    type Out = MultiLineString;
    fn reproject(&self, projector: &P) -> Result<MultiLineString> {
        let ls: Result<Vec<Vec<Coordinate2D>>> = self
            .lines()
            .iter()
            .map(|line| {
                line.iter()
                    .map(|&c| projector.project_coordinate(c))
                    .collect()
            })
            .collect();
        ls.and_then(MultiLineString::new)
    }
}

impl<'g, P> Reproject<P> for MultiLineStringRef<'g>
where
    P: CoordinateProjector,
{
    type Out = MultiLineString;
    fn reproject(&self, projector: &P) -> Result<MultiLineString> {
        let ls: Result<Vec<Vec<Coordinate2D>>> = self
            .lines()
            .iter()
            .map(|line| {
                line.iter()
                    .map(|&c| projector.project_coordinate(c))
                    .collect()
            })
            .collect();
        ls.and_then(MultiLineString::new)
    }
}

impl<P> Reproject<P> for MultiPolygon
where
    P: CoordinateProjector,
{
    type Out = MultiPolygon;
    fn reproject(&self, projector: &P) -> Result<MultiPolygon> {
        let ls: Result<Vec<Vec<Vec<Coordinate2D>>>> = self
            .polygons()
            .iter()
            .map(|poly| {
                poly.iter()
                    .map(|ring| {
                        ring.iter()
                            .map(|&c| projector.project_coordinate(c))
                            .collect()
                    })
                    .collect()
            })
            .collect();
        ls.and_then(MultiPolygon::new)
    }
}

impl<'g, P> Reproject<P> for MultiPolygonRef<'g>
where
    P: CoordinateProjector,
{
    type Out = MultiPolygon;
    fn reproject(&self, projector: &P) -> Result<MultiPolygon> {
        let ls: Result<Vec<Vec<Vec<Coordinate2D>>>> = self
            .polygons()
            .iter()
            .map(|poly| {
                poly.iter()
                    .map(|ring| {
                        ring.iter()
                            .map(|&c| projector.project_coordinate(c))
                            .collect()
                    })
                    .collect()
            })
            .collect();
        ls.and_then(MultiPolygon::new)
    }
}

impl<P> Reproject<P> for BoundingBox2D
where
    P: CoordinateProjector,
{
    type Out = BoundingBox2D;
    fn reproject(&self, projector: &P) -> Result<BoundingBox2D> {
        let points_per_line = 7; // TODO: static or something else ?
        let upper_line = Line::new(self.upper_left(), self.upper_right())
            .equi_spaced_coordinates(points_per_line);
        let right_line = Line::new(self.upper_right(), self.lower_right())
            .equi_spaced_coordinates(points_per_line);
        let lower_line = Line::new(self.lower_right(), self.lower_left())
            .equi_spaced_coordinates(points_per_line);
        let left_line = Line::new(self.lower_left(), self.upper_left())
            .equi_spaced_coordinates(points_per_line);

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
