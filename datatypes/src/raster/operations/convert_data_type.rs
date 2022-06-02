use crate::raster::{
    data_type::DefaultNoDataValue, BaseTile, EmptyGrid, Grid, GridOrEmpty, GridSize, MapElements,
    MapElementsParallel,
};
use num_traits::AsPrimitive;

pub trait ConvertDataType<Output> {
    fn convert_data_type(self) -> Output;
}

impl<In, Out, G> ConvertDataType<Grid<G, Out>> for Grid<G, In>
where
    In: AsPrimitive<Out> + Copy + PartialEq,
    Out: Copy + 'static + DefaultNoDataValue,
    G: GridSize + Clone,
{
    fn convert_data_type(self) -> Grid<G, Out> {
        let no_data_value = self.no_data_value.map(AsPrimitive::as_);
        self.map_elements(|p| Some(p.as_()), no_data_value)
    }
}

impl<In, Out, G> ConvertDataType<EmptyGrid<G, Out>> for EmptyGrid<G, In>
where
    In: AsPrimitive<Out> + Copy + PartialEq,
    Out: Copy + 'static + DefaultNoDataValue,
    G: GridSize + Clone,
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
    In: AsPrimitive<Out> + Copy + PartialEq,
    Out: Copy + 'static + DefaultNoDataValue,
    G: GridSize + Clone,
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
    G: GridSize + Clone + Send + Sync,
    In: AsPrimitive<Out> + Copy + Send + Sync + 'static + PartialEq,
    Out: Copy + Send + Sync + 'static + DefaultNoDataValue,
{
    fn convert_data_type_parallel(self) -> Grid<G, Out> {
        let no_data_value = self.no_data_value.map(AsPrimitive::as_);
        self.map_elements_parallel(|p| Some(p.as_()), no_data_value)
    }
}

impl<In, Out, G> ConvertDataTypeParallel<GridOrEmpty<G, Out>> for GridOrEmpty<G, In>
where
    G: GridSize + Clone + Send + Sync,
    In: AsPrimitive<Out> + Copy + Send + Sync + 'static + PartialEq,
    Out: Copy + Send + Sync + 'static + DefaultNoDataValue,
{
    fn convert_data_type_parallel(self) -> GridOrEmpty<G, Out> {
        match self {
            GridOrEmpty::Grid(g) => GridOrEmpty::Grid(g.convert_data_type_parallel()),
            GridOrEmpty::Empty(n) => GridOrEmpty::Empty(n.convert_data_type()),
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

#[cfg(test)]
mod tests {
    use crate::{
        primitives::TimeInterval,
        raster::{EmptyGrid2D, GeoTransform, Grid2D, GridOrEmpty2D, RasterTile2D},
    };

    use super::*;

    #[test]
    #[allow(clippy::float_cmp)]
    fn convert_grid() {
        let g_u8: Grid2D<u8> = Grid2D::new_filled([32, 32].into(), 8, Some(7));
        let g_f64: Grid2D<f32> = g_u8.convert_data_type();
        assert!(g_f64.data.into_iter().all(|f| f == 8.));
        assert_eq!(g_f64.no_data_value, Some(7.));
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn convert_grid_parallel() {
        let g_u8: Grid2D<u8> = Grid2D::new_filled([32, 32].into(), 8, Some(7));
        let g_f64: Grid2D<f32> = g_u8.convert_data_type_parallel();
        assert!(g_f64.data.into_iter().all(|f| f == 8.));
        assert_eq!(g_f64.no_data_value, Some(7.));
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn convert_empty_grid() {
        let g_u8: EmptyGrid2D<u8> = EmptyGrid2D::new([32, 32].into(), 7);
        let g_f64: EmptyGrid2D<f32> = g_u8.convert_data_type();
        assert_eq!(g_f64.no_data_value, 7.);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn convert_grid_or_empty_grid() {
        let g_u8: GridOrEmpty2D<u8> = Grid2D::new_filled([32, 32].into(), 8, Some(7)).into();
        let g_f64: GridOrEmpty2D<f32> = g_u8.convert_data_type();
        if let GridOrEmpty2D::Grid(g) = g_f64 {
            assert!(g.data.into_iter().all(|f| f == 8.));
            assert_eq!(g.no_data_value, Some(7.));
        } else {
            panic!("Expected GridOrEmpty2D::Grid");
        }
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn convert_grid_or_empty_empty() {
        let g_u8: GridOrEmpty2D<u8> = EmptyGrid2D::new([32, 32].into(), 7).into();
        let g_f64: GridOrEmpty2D<f32> = g_u8.convert_data_type();
        if let GridOrEmpty2D::Empty(g) = g_f64 {
            assert_eq!(g.no_data_value, 7.);
        } else {
            panic!("Expected GridOrEmpty2D::Empty");
        }
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn convert_grid_or_empty_grid_parallel() {
        let g_u8: GridOrEmpty2D<u8> = Grid2D::new_filled([32, 32].into(), 8, Some(7)).into();
        let g_f64: GridOrEmpty2D<f32> = g_u8.convert_data_type_parallel();
        if let GridOrEmpty2D::Grid(g) = g_f64 {
            assert!(g.data.into_iter().all(|f| f == 8.));
            assert_eq!(g.no_data_value, Some(7.));
        } else {
            panic!("Expected GridOrEmpty2D::Grid");
        }
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn convert_grid_or_empty_empty_parallel() {
        let g_u8: GridOrEmpty2D<u8> = EmptyGrid2D::new([32, 32].into(), 7).into();
        let g_f64: GridOrEmpty2D<f32> = g_u8.convert_data_type_parallel();
        if let GridOrEmpty2D::Empty(g) = g_f64 {
            assert_eq!(g.no_data_value, 7.);
        } else {
            panic!("Expected GridOrEmpty2D::Empty");
        }
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn convert_raster_tile() {
        let g_u8: GridOrEmpty2D<u8> = Grid2D::new_filled([32, 32].into(), 8, Some(7)).into();
        let tile_u8 = RasterTile2D::new(
            TimeInterval::default(),
            [0, 0].into(),
            GeoTransform::new((0., 0.).into(), 1., -1.),
            g_u8,
        );
        let tile_f64: RasterTile2D<f32> = tile_u8.convert_data_type();

        assert_eq!(tile_f64.time, TimeInterval::default());
        assert_eq!(tile_f64.tile_position, [0, 0].into());
        assert_eq!(
            tile_f64.global_geo_transform,
            GeoTransform::new((0., 0.).into(), 1., -1.)
        );

        if let GridOrEmpty2D::Grid(g) = tile_f64.grid_array {
            assert!(g.data.into_iter().all(|f| f == 8.));
            assert_eq!(g.no_data_value, Some(7.));
        } else {
            panic!("Expected GridOrEmpty2D::Grid");
        }
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn convert_raster_tile_parallel() {
        let g_u8: GridOrEmpty2D<u8> = Grid2D::new_filled([32, 32].into(), 8, Some(7)).into();
        let tile_u8 = RasterTile2D::new(
            TimeInterval::default(),
            [0, 0].into(),
            GeoTransform::new((0., 0.).into(), 1., -1.),
            g_u8,
        );
        let tile_f64: RasterTile2D<f32> = tile_u8.convert_data_type_parallel();

        assert_eq!(tile_f64.time, TimeInterval::default());
        assert_eq!(tile_f64.tile_position, [0, 0].into());
        assert_eq!(
            tile_f64.global_geo_transform,
            GeoTransform::new((0., 0.).into(), 1., -1.)
        );

        if let GridOrEmpty2D::Grid(g) = tile_f64.grid_array {
            assert!(g.data.into_iter().all(|f| f == 8.));
            assert_eq!(g.no_data_value, Some(7.));
        } else {
            panic!("Expected GridOrEmpty2D::Grid");
        }
    }
}
