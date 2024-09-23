use super::from_index_fn::FromIndexFnParallel;
use crate::raster::{
    GeoTransform, GridBounds, GridIdx, GridIdx2D, GridIndexAccess, GridOrEmpty, GridShapeAccess,
    GridSize, GridSpaceToLinearSpace, Pixel,
};
use crate::util::Result;

pub trait InterpolationAlgorithm<D, P: Pixel>: Send + Sync + Clone + 'static {
    /// interpolate the given input tile into the output tile
    /// the output must be fully contained in the input tile and have an additional row and column in order
    /// to have all the required neighbor pixels.
    /// Also the output must have a finer resolution than the input
    fn interpolate(
        in_geo_transform: GeoTransform,
        input: &GridOrEmpty<D, P>,
        out_geo_transform: GeoTransform,
        out_bounds: D,
    ) -> Result<GridOrEmpty<D, P>>;
}

#[derive(Clone, Debug)]
pub struct NearestNeighbor {}

impl<D, P> InterpolationAlgorithm<D, P> for NearestNeighbor
where
    D: GridShapeAccess<ShapeArray = [usize; 2]>
        + Clone
        + GridSize<ShapeArray = [usize; 2]>
        + GridBounds<IndexArray = [isize; 2]>
        + PartialEq
        + Send
        + Sync
        + GridSpaceToLinearSpace<IndexArray = [isize; 2]>,
    P: Pixel,
    GridOrEmpty<D, P>:
        GridIndexAccess<Option<P>, GridIdx<<D as GridSpaceToLinearSpace>::IndexArray>>,
{
    fn interpolate(
        in_geo_transform: GeoTransform,
        input: &GridOrEmpty<D, P>,
        out_geo_transform: GeoTransform,
        out_bounds: D,
    ) -> Result<GridOrEmpty<D, P>> {
        if input.is_empty() {
            return Ok(GridOrEmpty::new_empty_shape(out_bounds));
        }

        let map_fn = |gidx: GridIdx2D| {
            let coordinate = out_geo_transform.grid_idx_to_pixel_center_coordinate_2d(gidx); // use center coordinate similar to ArgGIS
            let pixel_in_input = in_geo_transform.coordinate_to_grid_idx_2d(coordinate);

            input.get_at_grid_index_unchecked(pixel_in_input)
        };

        let out_data = GridOrEmpty::from_index_fn_parallel(&out_bounds, map_fn); // TODO: this will check for empty tiles. Change to MaskedGrid::from.. to avoid this.

        Ok(out_data)
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

impl<P, D> InterpolationAlgorithm<D, P> for Bilinear
where
    D: GridShapeAccess<ShapeArray = [usize; 2]>
        + Clone
        + GridSize<ShapeArray = [usize; 2]>
        + GridBounds<IndexArray = [isize; 2]>
        + PartialEq
        + Send
        + Sync
        + GridSpaceToLinearSpace<IndexArray = [isize; 2]>,
    P: Pixel,
    GridOrEmpty<D, P>:
        GridIndexAccess<Option<P>, GridIdx<<D as GridSpaceToLinearSpace>::IndexArray>>,
{
    fn interpolate(
        in_geo_transform: GeoTransform,
        input: &GridOrEmpty<D, P>,
        out_geo_transform: GeoTransform,
        out_bounds: D,
    ) -> Result<GridOrEmpty<D, P>> {
        if input.is_empty() {
            return Ok(GridOrEmpty::new_empty_shape(out_bounds));
        }

        let map_fn = |out_g_idx: GridIdx2D| {
            let out_coord = out_geo_transform.grid_idx_to_pixel_upper_left_coordinate_2d(out_g_idx);

            let in_g_idx = in_geo_transform.coordinate_to_grid_idx_2d(out_coord);

            let in_a_idx = in_g_idx;
            let in_b_idx = in_a_idx + [1, 0];
            let in_c_idx = in_a_idx + [0, 1];
            let in_d_idx = in_a_idx + [1, 1];

            let in_a_coord = in_geo_transform.grid_idx_to_pixel_upper_left_coordinate_2d(in_a_idx);
            let a_y = in_a_coord.y;
            let b_y = a_y + in_geo_transform.y_pixel_size();

            let a_x = in_a_coord.x;
            let c_x = a_x + in_geo_transform.x_pixel_size();

            let a_v = input.get_at_grid_index(in_a_idx).unwrap_or(None);

            let b_v = input.get_at_grid_index(in_b_idx).unwrap_or(None);

            let c_v = input.get_at_grid_index(in_c_idx).unwrap_or(None);

            let d_v = input.get_at_grid_index(in_d_idx).unwrap_or(None);

            let value = match (a_v, b_v, c_v, d_v) {
                (Some(a), Some(b), Some(c), Some(d)) => Some(Self::bilinear_interpolation(
                    out_coord.x,
                    out_coord.y,
                    a_x,
                    a_y,
                    a.as_(),
                    b_y,
                    b.as_(),
                    c_x,
                    c.as_(),
                    d.as_(),
                )),
                _ => None,
            };

            value.map(|v| P::from_(v))
        };

        let out_data = GridOrEmpty::from_index_fn_parallel(&out_bounds, map_fn);

        Ok(out_data)
    }
}

#[cfg(test)]
mod tests {
    use rayon::ThreadPoolBuilder;

    use super::*;
    use crate::{
        primitives::CacheHint,
        raster::{
            GeoTransform, GeoTransformAccess, Grid2D, GridOrEmpty, MaskedGrid, RasterTile2D,
            TileInformation,
        },
    };

    #[test]
    fn nearest_neightbor() {
        let input = RasterTile2D::new_with_tile_info(
            Default::default(),
            TileInformation {
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 3].into(),
                global_geo_transform: GeoTransform::new((0.0, 2.0).into(), 1.0, -1.0),
            },
            0,
            GridOrEmpty::Grid(MaskedGrid::from(
                Grid2D::new([3, 3].into(), vec![1, 2, 3, 4, 5, 6, 7, 8, 9]).unwrap(),
            )),
            CacheHint::default(),
        );

        let input_geo_transform = input.geo_transform();
        let input_grid = input.into_inner_positioned_grid();

        let output_info = TileInformation {
            global_tile_position: [0, 0].into(),
            tile_size_in_pixels: [3, 3].into(),
            global_geo_transform: GeoTransform::new((0.0, 2.0).into(), 0.5, -0.5),
        };

        let output_geo_transform = output_info.global_geo_transform;
        let output_bounds = output_info.global_pixel_bounds();

        let pool = ThreadPoolBuilder::new().num_threads(0).build().unwrap();

        let output = pool
            .install(|| {
                NearestNeighbor::interpolate(
                    input_geo_transform,
                    &input_grid,
                    output_geo_transform,
                    output_bounds,
                )
            })
            .unwrap();

        assert!(!output.is_empty());
        let output_data = output.as_masked_grid().unwrap();

        assert_eq!(
            output_data
                .masked_element_deref_iterator()
                .collect::<Vec<_>>(),
            vec![
                Some(1),
                Some(1),
                Some(2),
                Some(1),
                Some(1),
                Some(2),
                Some(4),
                Some(4),
                Some(5)
            ]
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
            0,
            GridOrEmpty::Grid(MaskedGrid::from(
                Grid2D::new([3, 3].into(), vec![1., 2., 3., 4., 5., 6., 7., 8., 9.]).unwrap(),
            )),
            CacheHint::default(),
        );

        let input_geo_transform = input.geo_transform();
        let input_grid = input.into_inner_positioned_grid();

        let output_info = TileInformation {
            global_tile_position: [0, 0].into(),
            tile_size_in_pixels: [4, 4].into(),
            global_geo_transform: GeoTransform::new((0.0, 2.0).into(), 0.5, -0.5),
        };

        let output_geo_transform = output_info.global_geo_transform;
        let output_bounds = output_info.global_pixel_bounds();

        let pool = ThreadPoolBuilder::new().num_threads(0).build().unwrap();

        let output = pool
            .install(|| {
                Bilinear::interpolate(
                    input_geo_transform,
                    &input_grid,
                    output_geo_transform,
                    output_bounds,
                )
            })
            .unwrap();

        assert!(!output.is_empty());
        let output_data = output.as_masked_grid().unwrap();

        assert_eq!(
            output_data
                .masked_element_deref_iterator()
                .collect::<Vec<_>>(),
            vec![
                Some(1.0),
                Some(1.5),
                Some(2.0),
                Some(2.5),
                Some(2.5),
                Some(3.0),
                Some(3.5),
                Some(4.0),
                Some(4.0),
                Some(4.5),
                Some(5.0),
                Some(5.5),
                Some(5.5),
                Some(6.0),
                Some(6.5),
                Some(7.0)
            ]
        );
    }
}
