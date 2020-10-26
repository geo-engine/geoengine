use crate::raster::{
    base_raster::Raster1D, BaseRaster, Dim1D, Dim2D, Dim3D, GridDimension, OffsetDim1D,
    OffsetDim2D, OffsetDim3D, OffsetDimension, Pixel, Raster2D, Raster3D, SignedGridIdx1D,
    SignedGridIdx2D, SignedGridIdx3D,
};
use crate::util::Result;

pub trait GridBlit<OD, C, T>
where
    OD: OffsetDimension,
    C: AsRef<[T]>,
    T: Pixel,
{
    fn grid_blit_from(
        &mut self,
        other: BaseRaster<OD::DimensionType, T, C>,
        start_index: OD::SignedIndexType,
    ) -> Result<()>;
}

impl<C, T> GridBlit<OffsetDim1D, C, T> for Raster1D<T>
where
    C: AsRef<[T]>,
    T: Pixel + Sized,
{
    fn grid_blit_from(
        &mut self,
        other: BaseRaster<Dim1D, T, C>,
        start_index: SignedGridIdx1D,
    ) -> Result<()> {
        let other_offset_dim = OffsetDim1D::new(other.grid_dimension, start_index);
        let offset_dim = OffsetDim1D::new(self.grid_dimension, [0]);
        if let Some(intersection_offset_dim) = offset_dim.intersection(&other_offset_dim) {
            let [overlap_offset] = intersection_offset_dim.offsets_as_index();
            let [overlap_size] = intersection_offset_dim.grid_dimension().size_as_index();

            let self_start_x =
                offset_dim.offset_index_to_linear_space_index_unchecked(&[overlap_offset]);

            let other_start_x =
                other_offset_dim.offset_index_to_linear_space_index_unchecked(&[overlap_offset]);

            self.data_container.as_mut_slice()[self_start_x..self_start_x + overlap_size]
                .copy_from_slice(
                    &other.data_container.as_ref()[other_start_x..other_start_x + overlap_size],
                );
        }
        Ok(())
    }
}

impl<C, T> GridBlit<OffsetDim2D, C, T> for Raster2D<T>
where
    C: AsRef<[T]>,
    T: Pixel + Sized,
{
    fn grid_blit_from(
        &mut self,
        other: BaseRaster<Dim2D, T, C>,
        start_index: SignedGridIdx2D,
    ) -> Result<()> {
        // a shifted grid for the other grid
        let other_offset_dim = OffsetDim2D::new(other.grid_dimension, start_index);
        // a shifted grid for the self grid
        let offset_dim = OffsetDim2D::new(self.grid_dimension, [0, 0]);
        // if there is an intersection ...
        if let Some(intersection_offset_dim) = offset_dim.intersection(&other_offset_dim) {
            let [overlap_y_offset, overlap_x_offset] = intersection_offset_dim.offsets_as_index();
            let [overlap_y_size, overlap_x_size] =
                intersection_offset_dim.grid_dimension().size_as_index();

            for y in overlap_y_offset..overlap_y_offset + overlap_y_size as isize {
                let other_start_x = other_offset_dim
                    .offset_index_to_linear_space_index_unchecked(&[y, overlap_x_offset]);

                let self_start_x =
                    offset_dim.offset_index_to_linear_space_index_unchecked(&[y, overlap_x_offset]);

                self.data_container.as_mut_slice()[self_start_x..self_start_x + overlap_x_size]
                    .copy_from_slice(
                        &other.data_container.as_ref()
                            [other_start_x..other_start_x + overlap_x_size],
                    );
            }
        }
        Ok(())
    }
}

impl<C, T> GridBlit<OffsetDim3D, C, T> for Raster3D<T>
where
    C: AsRef<[T]>,
    T: Pixel + Sized,
{
    fn grid_blit_from(
        &mut self,
        other: BaseRaster<Dim3D, T, C>,
        start_index: SignedGridIdx3D,
    ) -> Result<()> {
        let other_offset_dim = OffsetDim3D::new(other.grid_dimension, start_index);
        let offset_dim = OffsetDim3D::new(self.grid_dimension, [0, 0, 0]);
        if let Some(intersection_offset_dim) = offset_dim.intersection(&other_offset_dim) {
            let [overlap_z_offset, overlap_y_offset, overlap_x_offset] =
                intersection_offset_dim.offsets_as_index();
            let [overlap_z_size, overlap_y_size, overlap_x_size] =
                intersection_offset_dim.grid_dimension().size_as_index();

            for z in overlap_z_offset..overlap_z_offset + overlap_z_size as isize {
                for y in overlap_y_offset..overlap_y_offset + overlap_y_size as isize {
                    let self_start_x = offset_dim.offset_index_to_linear_space_index_unchecked(&[
                        z,
                        y,
                        overlap_x_offset,
                    ]);
                    let other_start_x = other_offset_dim
                        .offset_index_to_linear_space_index_unchecked(&[z, y, overlap_x_offset]);

                    self.data_container.as_mut_slice()[self_start_x..self_start_x + overlap_x_size]
                        .copy_from_slice(
                            &other.data_container.as_ref()
                                [other_start_x..other_start_x + overlap_x_size],
                        );
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::raster::{GridBlit, Raster2D};

    #[test]
    fn grid_blit_from_2d_0_0() {
        let dim = [4, 4];
        let data = vec![0; 16];

        let mut r1 = Raster2D::new(dim.into(), data, None).unwrap();

        let data = vec![7; 16];

        let r2 = Raster2D::new(dim.into(), data, None).unwrap();

        r1.grid_blit_from(r2, [0, 0]).unwrap();

        assert_eq!(r1.data_container, vec![7; 16]);
    }

    #[test]
    fn grid_blit_from_2d_2_2() {
        let dim = [4, 4];
        let data = vec![0; 16];

        let mut r1 = Raster2D::new(dim.into(), data, None).unwrap();

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

        let r2 = Raster2D::new(dim.into(), data, None).unwrap();

        r1.grid_blit_from(r2, [2, 2]).unwrap();

        assert_eq!(
            r1.data_container,
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 4, 5]
        );
    }

    #[test]
    fn grid_blit_from_2d_n2_n2() {
        let dim = [4, 4];
        let data = vec![0; 16];

        let mut r1 = Raster2D::new(dim.into(), data, None).unwrap();

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

        let r2 = Raster2D::new(dim.into(), data, None).unwrap();

        r1.grid_blit_from(r2, [-2, -2]).unwrap();

        assert_eq!(
            r1.data_container,
            vec![10, 11, 0, 0, 14, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
    }
}
