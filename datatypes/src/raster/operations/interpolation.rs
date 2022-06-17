use crate::primitives::{AxisAlignedRectangle, SpatialPartitioned};
use crate::raster::{GridIndexAccess, GridSize, MaterializedRasterTile2D, Pixel, RasterTile2D};
use crate::util::Result;
use async_trait::async_trait;
use rayon::iter::{IndexedParallelIterator, ParallelIterator};
use rayon::slice::ParallelSliceMut;

#[async_trait]
pub trait InterpolationAlgorithm<P: Pixel>: Send + Sync + Clone + 'static {
    /// interpolate the given input tile into the output tile
    /// the output must be fully contained in the input tile and have an additional row and column in order
    /// to have all the required neighbor pixels.
    /// Also the output must have a finer resolution than the input
    fn interpolate(input: &RasterTile2D<P>, output: &mut MaterializedRasterTile2D<P>)
        -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct NearestNeighbor {}

#[async_trait]
impl<P> InterpolationAlgorithm<P> for NearestNeighbor
where
    P: Pixel,
{
    fn interpolate(
        input: &RasterTile2D<P>,
        output: &mut MaterializedRasterTile2D<P>,
    ) -> Result<()> {
        let info_in = input.tile_information();
        let in_upper_left = info_in.spatial_partition().upper_left();
        let in_x_size = info_in.global_geo_transform.x_pixel_size();
        let in_y_size = info_in.global_geo_transform.y_pixel_size();

        let info_out = output.tile_information();
        let out_upper_left = info_out.spatial_partition().upper_left();
        let out_x_size = info_out.global_geo_transform.x_pixel_size();
        let out_y_size = info_out.global_geo_transform.y_pixel_size();

        let parallelism = rayon::current_num_threads();
        let rows_per_task =
            num::integer::div_ceil(output.grid_array.shape.axis_size_y(), parallelism);

        let chunk_size = output.grid_array.shape.axis_size_x() * rows_per_task;

        output
            .grid_array
            .data
            .par_chunks_mut(chunk_size)
            .enumerate()
            .for_each(|(y_f, rows_slice)| {
                let y_start = y_f * rows_per_task;
                let y_end = y_start + rows_slice.len() / output.grid_array.shape.axis_size_x();

                (y_start..y_end)
                    .zip(rows_slice.chunks_mut(output.grid_array.shape.axis_size_x()))
                    .for_each(|(y, row)| {
                        let out_y_coord = out_upper_left.y + y as f64 * out_y_size;
                        let nearest_in_y_idx =
                            ((out_y_coord - in_upper_left.y) / in_y_size).round() as isize;

                        row.iter_mut().enumerate().for_each(|(x, pixel)| {
                            let out_x_coord = out_upper_left.x + x as f64 * out_x_size;
                            let nearest_in_x_idx =
                                ((out_x_coord - in_upper_left.x) / in_x_size).round() as isize;

                            let value = input
                                .get_at_grid_index_unchecked([nearest_in_y_idx, nearest_in_x_idx]);

                            *pixel = value;
                        });
                    });
            });

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Bilinear {}

impl Bilinear {
    #[inline]
    #[allow(clippy::too_many_arguments)]
    /// Interpolate values using the points coordinates (`a_x`, `a_y`) and values (`a_v`)
    /// a------c
    ///  |     |
    ///  |     |
    /// b------d
    pub fn bilinear_interpolation(
        x: f64,
        y: f64,
        a_x: f64,
        a_y: f64,
        a_v: f64,
        b_y: f64,
        b_v: f64,
        c_x: f64,
        c_v: f64,
        d_v: f64,
    ) -> f64 {
        (a_v * (c_x - x) * (b_y - y)
            + c_v * (x - a_x) * (b_y - y)
            + b_v * (c_x - x) * (y - a_y)
            + d_v * (x - a_x) * (y - a_y))
            / ((c_x - a_x) * (b_y - a_y))
    }
}

#[async_trait]
impl<P> InterpolationAlgorithm<P> for Bilinear
where
    P: Pixel,
{
    fn interpolate(
        input: &RasterTile2D<P>,
        output: &mut MaterializedRasterTile2D<P>,
    ) -> Result<()> {
        let info_in = input.tile_information();
        let in_upper_left = info_in.spatial_partition().upper_left();
        let in_x_size = info_in.global_geo_transform.x_pixel_size();
        let in_y_size = info_in.global_geo_transform.y_pixel_size();

        let info_out = output.tile_information();
        let out_upper_left = info_out.spatial_partition().upper_left();
        let out_x_size = info_out.global_geo_transform.x_pixel_size();
        let out_y_size = info_out.global_geo_transform.y_pixel_size();

        let parallelism = rayon::current_num_threads();
        let rows_per_task =
            num::integer::div_ceil(output.grid_array.shape.axis_size_y(), parallelism);

        let chunk_size = output.grid_array.shape.axis_size_x() * rows_per_task;

        output
            .grid_array
            .data
            .par_chunks_mut(chunk_size)
            .enumerate()
            .for_each(|(y_f, rows_slice)| {
                let y_start = y_f * rows_per_task;
                let y_end = y_start + rows_slice.len() / output.grid_array.shape.axis_size_x();

                (y_start..y_end)
                    .zip(rows_slice.chunks_mut(output.grid_array.shape.axis_size_x()))
                    .for_each(|(y, row)| {
                        let out_y = out_upper_left.y + y as f64 * out_y_size;
                        let in_y_idx = ((out_y - in_upper_left.y) / in_y_size).floor() as isize;

                        let a_y = in_upper_left.y + in_y_size * in_y_idx as f64;
                        let b_y = a_y + in_y_size;

                        row.iter_mut().enumerate().for_each(|(x, pixel)| {
                            let out_x = out_upper_left.x + x as f64 * out_x_size;
                            let in_x_idx = ((out_x - in_upper_left.x) / in_x_size).floor() as isize;

                            let a_x = in_upper_left.x + in_x_size * in_x_idx as f64;
                            let c_x = a_x + in_x_size;

                            let a_v: f64 = input
                                .get_at_grid_index_unchecked([in_y_idx, in_x_idx])
                                .as_();
                            let b_v: f64 = input
                                .get_at_grid_index_unchecked([in_y_idx + 1, in_x_idx])
                                .as_();
                            let c_v: f64 = input
                                .get_at_grid_index_unchecked([in_y_idx, in_x_idx + 1])
                                .as_();
                            let d_v: f64 = input
                                .get_at_grid_index_unchecked([in_y_idx + 1, in_x_idx + 1])
                                .as_();

                            let value = Self::bilinear_interpolation(
                                out_x, out_y, a_x, a_y, a_v, b_y, b_v, c_x, c_v, d_v,
                            );

                            *pixel = P::from_(value);
                        });
                    });
            });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rayon::ThreadPoolBuilder;

    use super::*;
    use crate::raster::{GeoTransform, Grid2D, GridOrEmpty, RasterTile2D, TileInformation};

    #[test]
    fn nearest_neightbor() {
        let input = RasterTile2D::new_with_tile_info(
            Default::default(),
            TileInformation {
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 3].into(),
                global_geo_transform: GeoTransform::new((0.0, 2.0).into(), 1.0, -1.0),
            },
            GridOrEmpty::Grid(
                Grid2D::new([3, 3].into(), vec![1, 2, 3, 4, 5, 6, 7, 8, 9], Some(42)).unwrap(),
            ),
        );

        let mut output = RasterTile2D::new_with_tile_info(
            Default::default(),
            TileInformation {
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [4, 4].into(),
                global_geo_transform: GeoTransform::new((0.0, 2.0).into(), 0.5, -0.5),
            },
            GridOrEmpty::Grid(Grid2D::new([4, 4].into(), vec![42; 16], Some(42)).unwrap()),
        )
        .into_materialized_tile();

        let pool = ThreadPoolBuilder::new().num_threads(0).build().unwrap();

        pool.install(|| NearestNeighbor::interpolate(&input, &mut output))
            .unwrap();

        assert_eq!(
            output.grid_array.data,
            vec![1, 2, 2, 3, 4, 5, 5, 6, 4, 5, 5, 6, 7, 8, 8, 9]
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn bilinear_fn() {
        let [(a_x, a_y, a_v), (_b_x, b_y, b_v), (c_x, _c_y, c_v), (_d_x, _d_y, d_v)] = [
            (54.5, 17.041_667, 31.993),
            (54.5, 17.083_333, 31.911),
            (54.458_333, 17.041_667, 31.945),
            (54.458_333, 17.083_333, 31.866),
        ];

        let (x, y) = (54.478_667_462_7, 17.047_072_136_9);

        assert_eq!(
            Bilinear::bilinear_interpolation(x, y, a_x, a_y, a_v, b_y, b_v, c_x, c_v, d_v),
            31.957_986_883_136_307
        );
    }

    #[test]
    fn bilinear() {
        let input = RasterTile2D::new_with_tile_info(
            Default::default(),
            TileInformation {
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 3].into(),
                global_geo_transform: GeoTransform::new((0.0, 2.0).into(), 1.0, -1.0),
            },
            GridOrEmpty::Grid(
                Grid2D::new(
                    [3, 3].into(),
                    vec![1., 2., 3., 4., 5., 6., 7., 8., 9.],
                    Some(42.),
                )
                .unwrap(),
            ),
        );

        let mut output = RasterTile2D::new_with_tile_info(
            Default::default(),
            TileInformation {
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [4, 4].into(),
                global_geo_transform: GeoTransform::new((0.0, 2.0).into(), 0.5, -0.5),
            },
            GridOrEmpty::Grid(Grid2D::new([4, 4].into(), vec![42.; 16], Some(42.)).unwrap()),
        )
        .into_materialized_tile();

        let pool = ThreadPoolBuilder::new().num_threads(0).build().unwrap();

        pool.install(|| Bilinear::interpolate(&input, &mut output))
            .unwrap();

        assert_eq!(
            output.grid_array.data,
            vec![1.0, 1.5, 2.0, 2.5, 2.5, 3.0, 3.5, 4.0, 4.0, 4.5, 5.0, 5.5, 5.5, 6.0, 6.5, 7.0]
        );
    }
}
