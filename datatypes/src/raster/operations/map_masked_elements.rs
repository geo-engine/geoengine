use rayon::iter::{IntoParallelIterator, IndexedParallelIterator, ParallelIterator};

use crate::raster::{MaskedGrid, GridSize, Grid, GridOrEmpty, RasterTile2D};


pub trait MapMaskedElements<In, Out, F: Fn(Option<In>) -> Option<Out>> {
    type Output;
    fn map_or_mask_elements(self, map_fn: F) -> Self::Output;
}

pub trait MapMaskedElementsParallel<In, Out, F: Fn(Option<In>) -> Option<Out>> {
    type Output;
    fn map_or_mask_elements_parallel(self, map_fn: F) -> Self::Output;
}


impl<In, Out, F, G> MapMaskedElements<In, Out, F> for MaskedGrid<G, In>
where
    In: 'static + Clone,
    Out: 'static + Clone + Default,
    G: GridSize + Clone + PartialEq,
    F: Fn(Option<In>) -> Option<Out>,
{
    type Output = MaskedGrid<G, Out>;

    fn map_or_mask_elements(self, map_fn: F) -> Self::Output {
        let MaskedGrid {
            inner_grid: data,
            mut validity_mask, // TODO: discuss if it is better to clone or mutate...
        } = self;

        let mut new_data = Grid::new_filled(data.shape.clone(), Out::default());

        let mut in_no_data_count = 0;
        let mut out_no_data_count = 0;
        new_data
            .data
            .iter_mut()
            .zip(validity_mask.data.iter_mut())
            .zip(data.data.into_iter())
            .for_each(|((o, m), i)| {
                let in_value = if *m {
                    Some(i)
                } else {
                    in_no_data_count += 1;
                    None
                };

                let new_out_value = map_fn(in_value);
                *m = new_out_value.is_some();

                if let Some(out_value) = new_out_value {
                    *o = out_value;
                } else {                    
                    out_no_data_count += 1;
                }
            });
        dbg!(in_no_data_count, out_no_data_count);

        MaskedGrid::new(new_data, validity_mask)
            .expect("Creation of grid with dimension failed before")
    }
}


impl<In, Out, F, G> MapMaskedElements<In, Out, F> for GridOrEmpty<G, In>
where
    In: 'static + Copy,
    Out: 'static + Copy + Default,
    G: GridSize + Clone + PartialEq,
    F: Fn(Option<In>) -> Option<Out>,
{
    type Output = GridOrEmpty<G, Out>;

    fn map_or_mask_elements(self, map_fn: F) -> Self::Output {
        match self {
            GridOrEmpty::Grid(grid) => GridOrEmpty::Grid(grid.map_or_mask_elements(map_fn)),
            GridOrEmpty::Empty(empty) => GridOrEmpty::Empty(empty.convert_dtype()),
        }
    }
}


impl<In, Out, F> MapMaskedElements<In, Out, F> for RasterTile2D<In>
where
    In: 'static + Copy + PartialEq,
    Out: 'static + Copy + Default,
    F: Fn(Option<In>) -> Option<Out>,
{
    type Output = RasterTile2D<Out>;

    fn map_or_mask_elements(self, map_fn: F) -> Self::Output {
        RasterTile2D {
            grid_array: self.grid_array.map_or_mask_elements(map_fn),
            time: self.time,
            tile_position: self.tile_position,
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
        }
    }
}


impl<In, Out, F, G> MapMaskedElementsParallel<In, Out, F> for Grid<G, In>
where
    In: 'static + Copy + PartialEq + Send + Sync,
    Out: 'static + Copy + Send + Sync + Default,
    G: GridSize + Clone + Send + Sync + PartialEq,
    F: Fn(Option<In>) -> Option<Out> + Sync + Send,
{
    type Output = MaskedGrid<G, Out>;
    fn map_or_mask_elements_parallel(self, map_fn: F) -> Self::Output {
        MaskedGrid::from(self).map_or_mask_elements_parallel(map_fn) // TODO: implement on & so that it does not clone the data
    }
}

impl<In, Out, F, G> MapMaskedElementsParallel<In, Out, F> for MaskedGrid<G, In>
where
    In: 'static + Copy + PartialEq + Send + Sync,
    Out: 'static + Copy + Send + Sync + Default,
    G: GridSize + Clone + Send + Sync + PartialEq,
    F: Fn(Option<In>) -> Option<Out> + Sync + Send,
{
    type Output = MaskedGrid<G, Out>;
    fn map_or_mask_elements_parallel(self, map_fn: F) -> Self::Output {
        let MaskedGrid {
            inner_grid: data,
            validity_mask,
        } = self;

        let shape = data.shape.clone();

        let (new_data, new_mask): (Vec<Out>, Vec<bool>) = data
            .data
            .into_par_iter()
            .with_min_len(data.shape.axis_size_x())
            .zip(
                validity_mask
                    .data
                    .into_par_iter()
                    .with_min_len(validity_mask.shape.axis_size_x()),
            )
            .map(|(i, m)| {
                let in_value = if m {
                    Some(i)
                } else {
                    None
                };

                if let Some(o) = map_fn(in_value) {
                    (o, m)
                } else {
                    (Out::default(), false)
                }
            })
            .collect();

        MaskedGrid::new(
            Grid::new(shape.clone(), new_data).expect("Grid creation failed before"),
            Grid::new(shape, new_mask).expect("Grid creation failed before"),
        )
        .expect("Grid creation failed before")
    }
}

impl<In, Out, F, G> MapMaskedElementsParallel<In, Out, F> for GridOrEmpty<G, In>
where
    In: 'static + Copy + PartialEq + Send + Sync,
    Out: 'static + Copy + Send + Sync + Default,
    G: GridSize + Clone + Send + Sync + PartialEq,
    F: Fn(Option<In>) -> Option<Out> + Sync + Send,
{
    type Output = GridOrEmpty<G, Out>;

    fn map_or_mask_elements_parallel(self, map_fn: F) -> Self::Output {
        match self {
            GridOrEmpty::Grid(grid) => {
                GridOrEmpty::Grid(grid.map_or_mask_elements_parallel(map_fn))
            }
            GridOrEmpty::Empty(empty) => GridOrEmpty::Empty(empty.convert_dtype()),
        }
    }
}

impl<In, Out, F> MapMaskedElementsParallel<In, Out, F> for RasterTile2D<In>
where
    In: 'static + Copy + PartialEq + Send + Sync,
    Out: 'static + Copy + Send + Sync + Default,

    F: Fn(Option<In>) -> Option<Out> + Sync + Send,
{
    type Output = RasterTile2D<Out>;

    fn map_or_mask_elements_parallel(self, map_fn: F) -> Self::Output {
        RasterTile2D {
            grid_array: self.grid_array.map_or_mask_elements_parallel(map_fn),
            time: self.time,
            tile_position: self.tile_position,
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{raster::{GeoTransform, Grid2D}, primitives::TimeInterval, util::test::TestDefault};

    use super::*;

    #[test]
    fn map_raster_tile() {
        let dim = [2, 2];
        let data = vec![7, 7, 8, 8];
        let geo = GeoTransform::test_default();

        let r1 = GridOrEmpty::Grid(MaskedGrid::from(Grid2D::new(dim.into(), data).unwrap()));
        let t1 = RasterTile2D::new(TimeInterval::default(), [0, 0].into(), geo, r1);

        let scaled_r1 = t1.map_or_mask_elements(|p| p.map(|p| if p == 7 { p * 2 + 1 } else {0} ));
        let mat_scaled_r1 = scaled_r1.into_materialized_tile();

        let expected = [15, 15, 0, 0];
        let expected_mask = [true, true, false, false];

        assert_eq!(mat_scaled_r1.grid_array.inner_grid.data, expected);
        assert_eq!(mat_scaled_r1.grid_array.validity_mask.data, expected_mask);
    }
}