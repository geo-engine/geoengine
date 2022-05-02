use crate::raster::{
    data_type::DefaultNoDataValue, EmptyGrid, Grid, GridOrEmpty, GridSize, NoDataValue,
    RasterTile2D,
};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};

pub trait MapElements<In, Out, F: Fn(In) -> Option<Out>> {
    type Output;
    fn map_elements(self, map_fn: F, out_no_data: Option<Out>) -> Self::Output;
}

pub trait MapElementsParallel<In, Out, F: Fn(In) -> Option<Out>> {
    type Output;
    fn map_elements_parallel(self, map_fn: F, out_no_data: Option<Out>) -> Self::Output;
}

impl<In, Out, F, G> MapElements<In, Out, F> for Grid<G, In>
where
    In: 'static + Copy + PartialEq,
    Out: 'static + Copy + DefaultNoDataValue,
    G: GridSize + Clone,
    F: Fn(In) -> Option<Out>,
{
    type Output = Grid<G, Out>;
    fn map_elements(self, map_fn: F, out_no_data: Option<Out>) -> Self::Output {
        let out_no_data = out_no_data.unwrap_or(Out::DEFAULT_NO_DATA_VALUE);
        let shape = self.shape.clone();
        let data = self
            .data
            .iter()
            .map(|x| {
                if self.is_no_data(*x) {
                    out_no_data
                } else {
                    map_fn(*x).unwrap_or(out_no_data)
                }
            })
            .collect();

        Grid {
            shape,
            data,
            no_data_value: Some(out_no_data),
        }
    }
}

impl<In, Out, F, G> MapElements<In, Out, F> for EmptyGrid<G, In>
where
    F: Fn(In) -> Option<Out>,
    Out: DefaultNoDataValue,
{
    type Output = EmptyGrid<G, Out>;

    fn map_elements(self, _map_fn: F, out_no_data: Option<Out>) -> Self::Output {
        let out_no_data = out_no_data.unwrap_or(Out::DEFAULT_NO_DATA_VALUE);
        EmptyGrid {
            shape: self.shape,
            no_data_value: out_no_data,
        }
    }
}

impl<In, Out, F, G> MapElements<In, Out, F> for GridOrEmpty<G, In>
where
    In: 'static + Copy + PartialEq,
    Out: 'static + Copy + DefaultNoDataValue,
    G: GridSize + Clone,
    F: Fn(In) -> Option<Out>,
{
    type Output = GridOrEmpty<G, Out>;

    fn map_elements(self, map_fn: F, out_no_data: Option<Out>) -> Self::Output {
        match self {
            GridOrEmpty::Grid(grid) => GridOrEmpty::Grid(grid.map_elements(map_fn, out_no_data)),
            GridOrEmpty::Empty(empty) => {
                GridOrEmpty::Empty(empty.map_elements(map_fn, out_no_data))
            }
        }
    }
}

impl<In, Out, F> MapElements<In, Out, F> for RasterTile2D<In>
where
    In: 'static + Copy + PartialEq,
    Out: 'static + Copy + DefaultNoDataValue,
    F: Fn(In) -> Option<Out>,
{
    type Output = RasterTile2D<Out>;

    fn map_elements(self, map_fn: F, out_no_data: Option<Out>) -> Self::Output {
        RasterTile2D {
            grid_array: self.grid_array.map_elements(map_fn, out_no_data),
            time: self.time,
            tile_position: self.tile_position,
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
        }
    }
}

impl<In, Out, F, G> MapElementsParallel<In, Out, F> for Grid<G, In>
where
    In: 'static + Copy + PartialEq + Send + Sync,
    Out: 'static + Copy + Send + Sync + DefaultNoDataValue,
    G: GridSize + Clone + Send + Sync,
    F: Fn(In) -> Option<Out> + Sync,
{
    type Output = Grid<G, Out>;
    fn map_elements_parallel(self, map_fn: F, out_no_data: Option<Out>) -> Self::Output {
        let out_no_data = out_no_data.unwrap_or(Out::DEFAULT_NO_DATA_VALUE);
        let shape = self.shape.clone();
        let data = self
            .data
            .par_iter()
            .with_min_len(self.shape.axis_size_x())
            .map(|x| {
                if self.is_no_data(*x) {
                    out_no_data
                } else {
                    map_fn(*x).unwrap_or(out_no_data)
                }
            })
            .collect();

        Grid {
            shape,
            data,
            no_data_value: Some(out_no_data),
        }
    }
}

impl<In, Out, F, G> MapElementsParallel<In, Out, F> for GridOrEmpty<G, In>
where
    In: 'static + Copy + PartialEq + Send + Sync,
    Out: 'static + Copy + Send + Sync + DefaultNoDataValue,
    G: GridSize + Clone + Send + Sync,
    F: Fn(In) -> Option<Out> + Sync,
{
    type Output = GridOrEmpty<G, Out>;

    fn map_elements_parallel(self, map_fn: F, out_no_data: Option<Out>) -> Self::Output {
        match self {
            GridOrEmpty::Grid(grid) => {
                GridOrEmpty::Grid(grid.map_elements_parallel(map_fn, out_no_data))
            }
            GridOrEmpty::Empty(empty) => {
                GridOrEmpty::Empty(empty.map_elements(map_fn, out_no_data))
            }
        }
    }
}

impl<In, Out, F> MapElementsParallel<In, Out, F> for RasterTile2D<In>
where
    In: 'static + Copy + PartialEq + Send + Sync,
    Out: 'static + Copy + Send + Sync + DefaultNoDataValue,

    F: Fn(In) -> Option<Out> + Sync,
{
    type Output = RasterTile2D<Out>;

    fn map_elements_parallel(self, map_fn: F, out_no_data: Option<Out>) -> Self::Output {
        RasterTile2D {
            grid_array: self.grid_array.map_elements_parallel(map_fn, out_no_data),
            time: self.time,
            tile_position: self.tile_position,
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        primitives::TimeInterval,
        raster::{EmptyGrid2D, GeoTransform, Grid2D},
        util::test::TestDefault,
    };

    use super::*;

    #[test]
    fn map_grid() {
        let dim = [2, 2];
        let data = vec![7, 7, 8, 255];
        let no_data = 255;

        let r1 = Grid2D::new(dim.into(), data, Some(no_data)).unwrap();
        let scaled_r1 = r1.map_elements(
            |p| {
                if p == 7 {
                    Some(p * 2 + 1)
                } else {
                    None
                }
            },
            Some(no_data),
        );

        let expected = [15, 15, 255, 255];
        assert_eq!(scaled_r1.data, expected);
    }

    #[test]
    fn map_grid_or_empty() {
        let dim = [2, 2];
        let data = vec![7, 7, 8, 255];
        let no_data = 255;

        let r1 = GridOrEmpty::Grid(Grid2D::new(dim.into(), data, Some(no_data)).unwrap());
        let scaled_r1 = r1.map_elements(
            |p| {
                if p == 7 {
                    Some(p * 2 + 1)
                } else {
                    None
                }
            },
            Some(no_data),
        );

        let expected = [15, 15, 255, 255];

        match scaled_r1 {
            GridOrEmpty::Grid(g) => {
                assert_eq!(g.data, expected);
            }
            GridOrEmpty::Empty(_) => assert!(false),
        }

        let r2 = GridOrEmpty::Empty(EmptyGrid2D::new(dim.into(), no_data));
        let scaled_r2 = r2.map_elements(|p| Some(p - 10), Some(8));

        match scaled_r2 {
            GridOrEmpty::Grid(_) => {
                assert!(false);
            }
            GridOrEmpty::Empty(e) => {
                assert_eq!(e.shape, dim.into());
                assert_eq!(e.no_data_value, 8);
            }
        }
    }

    #[test]
    fn map_raster_tile() {
        let dim = [2, 2];
        let data = vec![7, 7, 8, 255];
        let no_data = 255;
        let geo = GeoTransform::test_default();

        let r1 = GridOrEmpty::Grid(Grid2D::new(dim.into(), data, Some(no_data)).unwrap());
        let t1 = RasterTile2D::new(TimeInterval::default(), [0, 0].into(), geo, r1);

        let scaled_r1 = t1.map_elements(
            |p| {
                if p == 7 {
                    Some(p * 2 + 1)
                } else {
                    None
                }
            },
            Some(no_data),
        );
        let mat_scaled_r1 = scaled_r1.into_materialized_tile();

        let expected = [15, 15, 255, 255];

        assert_eq!(mat_scaled_r1.grid_array.data, expected);
    }

    #[test]
    fn map_grid_parallel() {
        let dim = [2, 2];
        let data = vec![7, 7, 8, 255];
        let no_data = 255;

        let r1 = Grid2D::new(dim.into(), data, Some(no_data)).unwrap();
        let scaled_r1 = r1.map_elements_parallel(
            |p| {
                if p == 7 {
                    Some(p * 2 + 1)
                } else {
                    None
                }
            },
            Some(no_data),
        );

        let expected = [15, 15, 255, 255];
        assert_eq!(scaled_r1.data, expected);
    }

    #[test]
    fn map_grid_or_empty_parallel() {
        let dim = [2, 2];
        let data = vec![7, 7, 8, 255];
        let no_data = 255;

        let r1 = GridOrEmpty::Grid(Grid2D::new(dim.into(), data, Some(no_data)).unwrap());
        let scaled_r1 = r1.map_elements_parallel(
            |p| {
                if p == 7 {
                    Some(p * 2 + 1)
                } else {
                    None
                }
            },
            Some(no_data),
        );

        let expected = [15, 15, 255, 255];

        match scaled_r1 {
            GridOrEmpty::Grid(g) => {
                assert_eq!(g.data, expected);
            }
            GridOrEmpty::Empty(_) => assert!(false),
        }

        let r2 = GridOrEmpty::Empty(EmptyGrid2D::new(dim.into(), no_data));
        let scaled_r2 = r2.map_elements_parallel(|p| Some(p - 10), Some(8));

        match scaled_r2 {
            GridOrEmpty::Grid(_) => {
                assert!(false);
            }
            GridOrEmpty::Empty(e) => {
                assert_eq!(e.shape, dim.into());
                assert_eq!(e.no_data_value, 8);
            }
        }
    }

    #[test]
    fn map_raster_tile_parallel() {
        let dim = [2, 2];
        let data = vec![7, 7, 8, 255];
        let no_data = 255;
        let geo = GeoTransform::test_default();

        let r1 = GridOrEmpty::Grid(Grid2D::new(dim.into(), data, Some(no_data)).unwrap());
        let t1 = RasterTile2D::new(TimeInterval::default(), [0, 0].into(), geo, r1);

        let scaled_r1 = t1.map_elements_parallel(
            |p| {
                if p == 7 {
                    Some(p * 2 + 1)
                } else {
                    None
                }
            },
            Some(no_data),
        );
        let mat_scaled_r1 = scaled_r1.into_materialized_tile();

        let expected = [15, 15, 255, 255];

        assert_eq!(mat_scaled_r1.grid_array.data, expected);
    }
}
