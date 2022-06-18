use rayon::{slice::{ParallelSliceMut, ParallelSlice}, iter::{IndexedParallelIterator, ParallelIterator}};

use crate::raster::{GridIdx2D, GridOrEmpty2D, MaskedGrid2D, MaskedGrid, Grid, GridSize, GridOrEmpty, GridSpaceToLinearSpace, RasterTile2D};


pub trait MapIndexedElements<In, Out, GIdx, F: Fn(GIdx, Option<In>) -> Option<Out>> {
    type Output;
    fn map_index_elements(self, map_fn: F) -> Self::Output;
}

pub trait MapIndexedElementsParallel<In, Out, GIdx, F: Fn(GIdx, Option<In>) -> Option<Out>> {
    type Output;
    fn map_index_elements_parallel(self, map_fn: F) -> Self::Output;
}

impl<In, Out, F> MapIndexedElements<In, Out, GridIdx2D, F> for MaskedGrid2D<In>
where
    F: Fn(GridIdx2D, Option<In>) -> Option<Out>,
    In: Clone,
    Out: Default + Clone,
{
    type Output = MaskedGrid2D<Out>;

    fn map_index_elements(self, map_fn: F) -> Self::Output {
        let MaskedGrid {
            inner_grid: data,
            mut validity_mask,
        } = self;
        debug_assert!(data.data.len() == validity_mask.data.len());
        debug_assert!(data.shape == validity_mask.shape);

        let out_data: Vec<Out> = data
            .data
            .into_iter()
            .zip(validity_mask.data.iter_mut())
            .enumerate()
            .map(|(lin_idx, (i, m))| {
                let grid_idx = data.shape.grid_idx_unchecked(lin_idx);

                let in_masked_value = if *m { Some(i) } else { None };

                let out_value_option = map_fn(grid_idx, in_masked_value);

                *m = out_value_option.is_some();

                out_value_option.unwrap_or_default()
            })
            .collect();

        MaskedGrid::new(
            Grid::new(data.shape, out_data).expect("Grid createion failed before"),
            validity_mask,
        )
        .expect("Grid createion failed before")
    }
}

impl<In, Out, F> MapIndexedElements<In, Out, GridIdx2D, F> for GridOrEmpty2D<In>
where
    F: Fn(GridIdx2D, Option<In>) -> Option<Out>,
    In: Clone + 'static,
    Out: Default + Clone + 'static,
{
    type Output = GridOrEmpty2D<Out>;

    fn map_index_elements(self, map_fn: F) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => g.map_index_elements(map_fn).into(),
            GridOrEmpty::Empty(e) => e.convert_dtype::<Out>().into(),
        }
    }
}

impl<In, Out, F> MapIndexedElements<In, Out, GridIdx2D, F> for RasterTile2D<In>
where
    F: Fn(GridIdx2D, Option<In>) -> Option<Out>,
    In: Clone + 'static,
    Out: Default + Clone + 'static,
{
    type Output = RasterTile2D<Out>;

    fn map_index_elements(self, map_fn: F) -> Self::Output {
        RasterTile2D {
            grid_array: self.grid_array.map_index_elements(map_fn),
            time: self.time,
            tile_position: self.tile_position,
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
        }    
    }   
}

impl<In, Out, F> MapIndexedElementsParallel<In, Out, GridIdx2D, F> for MaskedGrid2D<In>
where
    F: Fn(GridIdx2D, Option<In>) -> Option<Out> + Send + Sync,
    In: Copy + Clone + Sync,
    Out: Default + Clone + Send,
{
    type Output = MaskedGrid2D<Out>;

    fn map_index_elements_parallel(self, map_fn: F) -> Self::Output {
        let MaskedGrid {
            inner_grid: data,
            mut validity_mask,
        } = self;
        debug_assert!(data.data.len() == validity_mask.data.len());
        debug_assert!(data.shape == validity_mask.shape);

        let x_axis_size = data.shape.axis_size_x();
        let y_axis_size = data.shape.axis_size_y();

        let mut out_data = vec![Out::default(); data.data.len()];

        let parallelism = rayon::current_num_threads();
        let rows_per_task = num::integer::div_ceil(y_axis_size, parallelism);

        let chunk_size = x_axis_size * rows_per_task;

        out_data
            .par_chunks_mut(chunk_size)
            .zip(validity_mask.data.par_chunks_mut(chunk_size))
            .zip(data.data.par_chunks(chunk_size))
            .enumerate()
            .for_each(|(y_f, ((out_rows_slice, mask_row_slice), in_raw_slice))| {
                let chunk_lin_start = y_f * chunk_size;

                out_rows_slice
                    .iter_mut()
                    .zip(mask_row_slice.iter_mut())
                    .zip(in_raw_slice)
                    .enumerate()
                    .for_each(|(elem_x_idx, ((out, mask), i))| {
                        let lin_idx = chunk_lin_start + elem_x_idx;
                        let grid_idx = data.shape.grid_idx_unchecked(lin_idx);

                        let in_masked_value = if *mask { Some(*i) } else { None };

                        let out_value_option = map_fn(grid_idx, in_masked_value);

                        *mask = out_value_option.is_some();

                        if let Some(out_value) = out_value_option {
                            *out = out_value;
                        }
                    });
            });

        MaskedGrid::new(
            Grid::new(data.shape, out_data).expect("Grid createion failed before"),
            validity_mask,
        )
        .expect("Grid createion failed before")
    }
}

impl<In, Out, F> MapIndexedElementsParallel<In, Out, GridIdx2D, F> for GridOrEmpty2D<In>
where
    F: Fn(GridIdx2D, Option<In>) -> Option<Out> + Send + Sync,
    In: Copy + Clone + Sync + 'static,
    Out: Default + Clone + Send + 'static,
{
    type Output = GridOrEmpty2D<Out>;

    fn map_index_elements_parallel(self, map_fn: F) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => g.map_index_elements_parallel(map_fn).into(),
            GridOrEmpty::Empty(e) => e.convert_dtype().into(),
        }
    }
}

impl<In, Out, F> MapIndexedElementsParallel<In, Out, GridIdx2D, F> for RasterTile2D<In>
where
    F: Fn(GridIdx2D, Option<In>) -> Option<Out> + Send + Sync,
    In: Copy + Clone + Sync + 'static,
    Out: Default + Clone + Send + 'static,
{
    type Output = RasterTile2D<Out>;

    fn map_index_elements_parallel(self, map_fn: F) -> Self::Output {
        RasterTile2D {
            grid_array: self.grid_array.map_index_elements_parallel(map_fn),
            time: self.time,
            tile_position: self.tile_position,
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
        }    
    }   
}
