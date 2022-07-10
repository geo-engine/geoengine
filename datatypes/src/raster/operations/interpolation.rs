use super::from_index_fn::FromIndexFnParallel;
use crate::primitives::{AxisAlignedRectangle, SpatialPartitioned};
use crate::raster::{
    EmptyGrid, GridIdx, GridIdx2D, GridIndexAccess, GridOrEmpty, Pixel, RasterTile2D,
    TileInformation,
};
use crate::util::Result;
use async_trait::async_trait;

#[async_trait]
pub trait InterpolationAlgorithm<P: Pixel>: Send + Sync + Clone + 'static {
    /// interpolate the given input tile into the output tile
    /// the output must be fully contained in the input tile and have an additional row and column in order
    /// to have all the required neighbor pixels.
    /// Also the output must have a finer resolution than the input
    fn interpolate(
        input: &RasterTile2D<P>,
        output_tile_info: &TileInformation,
    ) -> Result<RasterTile2D<P>>;
}

#[derive(Clone, Debug)]
pub struct NearestNeighbor {}

#[async_trait]
impl<P> InterpolationAlgorithm<P> for NearestNeighbor
where
    P: Pixel,
{
    fn interpolate(input: &RasterTile2D<P>, info_out: &TileInformation) -> Result<RasterTile2D<P>> {
        if input.is_empty() {
            return Ok(RasterTile2D::new_with_tile_info(
                input.time,
                *info_out,
                EmptyGrid::new(info_out.tile_size_in_pixels).into(),
            ));
        }

        let info_in = input.tile_information();
        let in_upper_left = info_in.spatial_partition().upper_left();
        let in_x_size = info_in.global_geo_transform.x_pixel_size();
        let in_y_size = info_in.global_geo_transform.y_pixel_size();

        let out_upper_left = info_out.spatial_partition().upper_left();
        let out_x_size = info_out.global_geo_transform.x_pixel_size();
        let out_y_size = info_out.global_geo_transform.y_pixel_size();

        let map_fn = |gidx: GridIdx2D| {
            let GridIdx([y, x]) = gidx;
            let out_y_coord = out_upper_left.y + y as f64 * out_y_size;
            let out_x_coord = out_upper_left.x + x as f64 * out_x_size;
            let nearest_in_y_idx = ((out_y_coord - in_upper_left.y) / in_y_size).round() as isize;
            let nearest_in_x_idx = ((out_x_coord - in_upper_left.x) / in_x_size).round() as isize;
            input.get_at_grid_index_unchecked([nearest_in_y_idx, nearest_in_x_idx])
        };

        let out_data = GridOrEmpty::from_index_fn_parallel(&info_out.tile_size_in_pixels, map_fn); // TODO: this will check for empty tiles. Change to MaskedGrid::from.. to avoid this.

        let out_tile = RasterTile2D::new(
            input.time,
            info_out.global_tile_position,
            info_out.global_geo_transform,
            out_data,
        );

        Ok(out_tile)
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
    fn interpolate(input: &RasterTile2D<P>, info_out: &TileInformation) -> Result<RasterTile2D<P>> {
        if input.is_empty() {
            return Ok(RasterTile2D::new_with_tile_info(
                input.time,
                *info_out,
                EmptyGrid::new(info_out.tile_size_in_pixels).into(),
            ));
        }

        let info_in = input.tile_information();
        let in_upper_left = info_in.spatial_partition().upper_left();
        let in_x_size = info_in.global_geo_transform.x_pixel_size();
        let in_y_size = info_in.global_geo_transform.y_pixel_size();

        let out_upper_left = info_out.spatial_partition().upper_left();
        let out_x_size = info_out.global_geo_transform.x_pixel_size();
        let out_y_size = info_out.global_geo_transform.y_pixel_size();

        let map_fn = |g_idx: GridIdx2D| {
            let GridIdx([y_idx, x_idx]) = g_idx;

            let out_y = out_upper_left.y + y_idx as f64 * out_y_size;
            let in_y_idx = ((out_y - in_upper_left.y) / in_y_size).floor() as isize;

            let a_y = in_upper_left.y + in_y_size * in_y_idx as f64;
            let b_y = a_y + in_y_size;

            let out_x = out_upper_left.x + x_idx as f64 * out_x_size;
            let in_x_idx = ((out_x - in_upper_left.x) / in_x_size).floor() as isize;

            let a_x = in_upper_left.x + in_x_size * in_x_idx as f64;
            let c_x = a_x + in_x_size;

            let a_v = input.get_at_grid_index_unchecked([in_y_idx, in_x_idx]);

            let b_v = input.get_at_grid_index_unchecked([in_y_idx + 1, in_x_idx]);

            let c_v = input.get_at_grid_index_unchecked([in_y_idx, in_x_idx + 1]);

            let d_v = input.get_at_grid_index_unchecked([in_y_idx + 1, in_x_idx + 1]);

            let value = match (a_v, b_v, c_v, d_v) {
                (Some(a), Some(b), Some(c), Some(d)) => Some(Self::bilinear_interpolation(
                    out_x,
                    out_y,
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

        let out_data = GridOrEmpty::from_index_fn_parallel(&info_out.tile_size_in_pixels, map_fn); // TODO: this will check for empty tiles. Change to MaskedGrid::from.. to avoid this.

        let out_tile = RasterTile2D::new(
            input.time,
            info_out.global_tile_position,
            info_out.global_geo_transform,
            out_data,
        );

        Ok(out_tile)
    }
}

#[cfg(test)]
mod tests {
    use rayon::ThreadPoolBuilder;

    use super::*;
    use crate::raster::{
        GeoTransform, Grid2D, GridOrEmpty, MaskedGrid, RasterTile2D, TileInformation,
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
            GridOrEmpty::Grid(MaskedGrid::from(
                Grid2D::new([3, 3].into(), vec![1, 2, 3, 4, 5, 6, 7, 8, 9]).unwrap(),
            )),
        );

        let output_info = TileInformation {
            global_tile_position: [0, 0].into(),
            tile_size_in_pixels: [4, 4].into(),
            global_geo_transform: GeoTransform::new((0.0, 2.0).into(), 0.5, -0.5),
        };

        let pool = ThreadPoolBuilder::new().num_threads(0).build().unwrap();

        let output = pool
            .install(|| NearestNeighbor::interpolate(&input, &output_info))
            .unwrap();

        assert!(!output.is_empty());
        let output_data = output.grid_array.as_masked_grid().unwrap();

        assert_eq!(
            output_data
                .masked_element_deref_iterator()
                .collect::<Vec<_>>(),
            vec![
                Some(1),
                Some(2),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
                Some(5),
                Some(6),
                Some(4),
                Some(5),
                Some(5),
                Some(6),
                Some(7),
                Some(8),
                Some(8),
                Some(9)
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
            GridOrEmpty::Grid(MaskedGrid::from(
                Grid2D::new([3, 3].into(), vec![1., 2., 3., 4., 5., 6., 7., 8., 9.]).unwrap(),
            )),
        );

        let output_info = TileInformation {
            global_tile_position: [0, 0].into(),
            tile_size_in_pixels: [4, 4].into(),
            global_geo_transform: GeoTransform::new((0.0, 2.0).into(), 0.5, -0.5),
        };

        let pool = ThreadPoolBuilder::new().num_threads(0).build().unwrap();

        let output = pool
            .install(|| Bilinear::interpolate(&input, &output_info))
            .unwrap();

        assert!(!output.is_empty());
        let output_data = output.grid_array.as_masked_grid().unwrap();

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
