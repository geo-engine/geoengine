use crate::raster::{BaseTile, EmptyGrid, Grid, GridOrEmpty, GridSize};
use num_traits::AsPrimitive;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

pub trait ConvertDataType<Output> {
    fn convert_data_type(self) -> Output;
}

impl<In, Out, G> ConvertDataType<Grid<G, Out>> for Grid<G, In>
where
    In: AsPrimitive<Out> + Copy,
    Out: Copy + 'static,
{
    fn convert_data_type(self) -> Grid<G, Out> {
        Grid {
            shape: self.shape,
            data: self.data.iter().map(|&pixel| pixel.as_()).collect(),
            no_data_value: self.no_data_value.map(AsPrimitive::as_),
        }
    }
}

impl<In, Out, G> ConvertDataType<EmptyGrid<G, Out>> for EmptyGrid<G, In>
where
    In: AsPrimitive<Out> + Copy,
    Out: Copy + 'static,
{
    fn convert_data_type(self) -> EmptyGrid<G, Out> {
        EmptyGrid {
            shape: self.shape,
            no_data_value: self.no_data_value.as_(),
        }
    }
}

impl<In, Out, G> ConvertDataType<GridOrEmpty<G, Out>> for GridOrEmpty<G, In>
where
    In: AsPrimitive<Out> + Copy,
    Out: Copy + 'static,
{
    fn convert_data_type(self) -> GridOrEmpty<G, Out> {
        match self {
            GridOrEmpty::Grid(g) => GridOrEmpty::Grid(g.convert_data_type()),
            GridOrEmpty::Empty(n) => GridOrEmpty::Empty(n.convert_data_type()),
        }
    }
}

impl<GIn, GOut> ConvertDataType<BaseTile<GOut>> for BaseTile<GIn>
where
    GIn: ConvertDataType<GOut>,
{
    fn convert_data_type(self) -> BaseTile<GOut> {
        BaseTile {
            time: self.time,
            grid_array: self.grid_array.convert_data_type(),
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
            tile_position: self.tile_position,
        }
    }
}

pub trait ConvertDataTypeParallel<Output> {
    fn convert_data_type_parallel(self) -> Output;
}

impl<In, Out, G> ConvertDataTypeParallel<Grid<G, Out>> for Grid<G, In>
where
    G: GridSize,
    In: AsPrimitive<Out> + Copy + Send + Sync + 'static,
    Out: Copy + Send + Sync + 'static,
{
    fn convert_data_type_parallel(self) -> Grid<G, Out> {
        // let lowest_dim_size = self.shape.axis_size_x();

        Grid {
            shape: self.shape,
            data: self
                .data
                .into_par_iter()
                .with_min_len(512)
                .map(|pixel| pixel.as_())
                .collect(),
            no_data_value: self.no_data_value.map(AsPrimitive::as_),
        }
    }
}

impl<In, Out, G> ConvertDataTypeParallel<EmptyGrid<G, Out>> for EmptyGrid<G, In>
where
    In: AsPrimitive<Out> + Copy + Send + Sync + 'static,
    Out: Copy + Send + Sync + 'static,
{
    fn convert_data_type_parallel(self) -> EmptyGrid<G, Out> {
        self.convert_data_type()
    }
}

impl<In, Out, G> ConvertDataTypeParallel<GridOrEmpty<G, Out>> for GridOrEmpty<G, In>
where
    G: GridSize,
    In: AsPrimitive<Out> + Copy + Send + Sync + 'static,
    Out: Copy + Send + Sync + 'static,
{
    fn convert_data_type_parallel(self) -> GridOrEmpty<G, Out> {
        match self {
            GridOrEmpty::Grid(g) => GridOrEmpty::Grid(g.convert_data_type_parallel()),
            GridOrEmpty::Empty(n) => GridOrEmpty::Empty(n.convert_data_type_parallel()),
        }
    }
}

impl<GIn, GOut> ConvertDataTypeParallel<BaseTile<GOut>> for BaseTile<GIn>
where
    GIn: ConvertDataTypeParallel<GOut>,
{
    fn convert_data_type_parallel(self) -> BaseTile<GOut> {
        BaseTile {
            time: self.time,
            grid_array: self.grid_array.convert_data_type_parallel(),
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
            tile_position: self.tile_position,
        }
    }
}
