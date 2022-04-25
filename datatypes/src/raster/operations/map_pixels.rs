use crate::raster::{
    data_type::DefaultNoDataValue, EmptyGrid, Grid, GridOrEmpty, GridSize, NoDataValue,
    RasterTile2D,
};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};

pub trait MapPixels<In, Out, F: Fn(In) -> Option<Out>> {
    type Output;
    fn map_pixels(self, map_fn: F, out_no_data: Option<Out>) -> Self::Output;
}

pub trait MapPixelsParallel<In, Out, F: Fn(In) -> Option<Out>> {
    type Output;
    fn map_pixels_parallel(self, map_fn: F, out_no_data: Option<Out>) -> Self::Output;
}

impl<In, Out, F, G> MapPixels<In, Out, F> for Grid<G, In>
where
    In: 'static + Copy + PartialEq,
    Out: 'static + Copy + DefaultNoDataValue,
    G: GridSize + Clone,
    F: Fn(In) -> Option<Out>,
{
    type Output = Grid<G, Out>;
    fn map_pixels(self, map_fn: F, out_no_data: Option<Out>) -> Self::Output {
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

impl<In, Out, F, G> MapPixels<In, Out, F> for EmptyGrid<G, In>
where
    F: Fn(In) -> Option<Out>,
    Out: DefaultNoDataValue,
{
    type Output = EmptyGrid<G, Out>;

    fn map_pixels(self, _map_fn: F, out_no_data: Option<Out>) -> Self::Output {
        let out_no_data = out_no_data.unwrap_or(Out::DEFAULT_NO_DATA_VALUE);
        EmptyGrid {
            shape: self.shape,
            no_data_value: out_no_data,
        }
    }
}

impl<In, Out, F, G> MapPixels<In, Out, F> for GridOrEmpty<G, In>
where
    In: 'static + Copy + PartialEq,
    Out: 'static + Copy + DefaultNoDataValue,
    G: GridSize + Clone,
    F: Fn(In) -> Option<Out>,
{
    type Output = GridOrEmpty<G, Out>;

    fn map_pixels(self, map_fn: F, out_no_data: Option<Out>) -> Self::Output {
        match self {
            GridOrEmpty::Grid(grid) => GridOrEmpty::Grid(grid.map_pixels(map_fn, out_no_data)),
            GridOrEmpty::Empty(empty) => GridOrEmpty::Empty(empty.map_pixels(map_fn, out_no_data)),
        }
    }
}

impl<In, Out, F> MapPixels<In, Out, F> for RasterTile2D<In>
where
    In: 'static + Copy + PartialEq,
    Out: 'static + Copy + DefaultNoDataValue,
    F: Fn(In) -> Option<Out>,
{
    type Output = RasterTile2D<Out>;

    fn map_pixels(self, map_fn: F, out_no_data: Option<Out>) -> Self::Output {
        RasterTile2D {
            grid_array: self.grid_array.map_pixels(map_fn, out_no_data),
            time: self.time,
            tile_position: self.tile_position,
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
        }
    }
}

impl<In, Out, F, G> MapPixelsParallel<In, Out, F> for Grid<G, In>
where
    In: 'static + Copy + PartialEq + Send + Sync,
    Out: 'static + Copy + Send + Sync + DefaultNoDataValue,
    G: GridSize + Clone + Send + Sync,
    F: Fn(In) -> Option<Out> + Sync,
{
    type Output = Grid<G, Out>;
    fn map_pixels_parallel(self, map_fn: F, out_no_data: Option<Out>) -> Self::Output {
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

impl<In, Out, F, G> MapPixelsParallel<In, Out, F> for GridOrEmpty<G, In>
where
    In: 'static + Copy + PartialEq + Send + Sync,
    Out: 'static + Copy + Send + Sync + DefaultNoDataValue,
    G: GridSize + Clone + Send + Sync,
    F: Fn(In) -> Option<Out> + Sync,
{
    type Output = GridOrEmpty<G, Out>;

    fn map_pixels_parallel(self, map_fn: F, out_no_data: Option<Out>) -> Self::Output {
        match self {
            GridOrEmpty::Grid(grid) => {
                GridOrEmpty::Grid(grid.map_pixels_parallel(map_fn, out_no_data))
            }
            GridOrEmpty::Empty(empty) => GridOrEmpty::Empty(empty.map_pixels(map_fn, out_no_data)),
        }
    }
}

impl<In, Out, F> MapPixelsParallel<In, Out, F> for RasterTile2D<In>
where
    In: 'static + Copy + PartialEq + Send + Sync,
    Out: 'static + Copy + Send + Sync + DefaultNoDataValue,

    F: Fn(In) -> Option<Out> + Sync,
{
    type Output = RasterTile2D<Out>;

    fn map_pixels_parallel(self, map_fn: F, out_no_data: Option<Out>) -> Self::Output {
        RasterTile2D {
            grid_array: self.grid_array.map_pixels_parallel(map_fn, out_no_data),
            time: self.time,
            tile_position: self.tile_position,
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
        }
    }
}
