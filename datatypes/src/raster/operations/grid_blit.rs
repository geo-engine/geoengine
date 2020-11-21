use crate::{
    raster::{
        BoundedGrid, GridArray, GridArray1D, GridArray2D, GridArray3D, GridBoundingBox, GridBounds,
        GridIdx, GridIntersection, GridSize, GridSpaceToLinearSpace, Pixel,
    },
    util::Result,
};

pub trait GridBlit<OD, T>
where
    OD: GridSize + GridSpaceToLinearSpace,
    T: Pixel,
{
    fn grid_blit_from(&mut self, other: GridArray<OD, T>) -> Result<()>;
}

impl<OD, T> GridBlit<OD, T> for GridArray1D<T>
where
    OD: GridSize
        + GridBounds<IndexArray = [isize; 1]>
        + GridSpaceToLinearSpace<IndexArray = [isize; 1]>,
    T: Pixel + Sized,
{
    fn grid_blit_from(&mut self, other: GridArray<OD, T>) -> Result<()> {
        let other_offset_dim = other.bounding_box();
        let offset_dim = self.bounding_box();
        let intersection: Option<GridBoundingBox<[isize; 1]>> =
            offset_dim.intersection(&other_offset_dim);
        if let Some(intersection_offset_dim) = intersection {
            let overlap_start = intersection_offset_dim.min_index();
            let [overlap_size] = intersection_offset_dim.axis_size();

            let self_start_x = offset_dim.linear_space_index_unchecked(overlap_start);
            let other_start_x = other_offset_dim.linear_space_index_unchecked(overlap_start);

            self.data.as_mut_slice()[self_start_x..self_start_x + overlap_size]
                .copy_from_slice(&other.data[other_start_x..other_start_x + overlap_size]);
        }
        Ok(())
    }
}

impl<OD, T> GridBlit<OD, T> for GridArray2D<T>
where
    OD: GridSize
        + GridBounds<IndexArray = [isize; 2]>
        + GridSpaceToLinearSpace<IndexArray = [isize; 2]>, // TODO:
    T: Pixel + Sized,
{
    fn grid_blit_from(&mut self, other: GridArray<OD, T>) -> Result<()> {
        let other_offset_dim = other.bounding_box();
        let offset_dim = self.bounding_box();
        let intersection: Option<GridBoundingBox<[isize; 2]>> =
            offset_dim.intersection(&other_offset_dim);
        if let Some(intersection_offset_dim) = intersection {
            let GridIdx([overlap_y_start, overlap_x_start]) = intersection_offset_dim.min_index();
            let [overlap_y_size, overlap_x_size] = intersection_offset_dim.axis_size();

            for y in overlap_y_start..overlap_y_start + overlap_y_size as isize {
                let other_start_x =
                    other_offset_dim.linear_space_index_unchecked([y, overlap_x_start]);

                let self_start_x = offset_dim.linear_space_index_unchecked([y, overlap_x_start]);

                self.data.as_mut_slice()[self_start_x..self_start_x + overlap_x_size]
                    .copy_from_slice(&other.data[other_start_x..other_start_x + overlap_x_size]);
            }
        }
        Ok(())
    }
}

impl<OD, T> GridBlit<OD, T> for GridArray3D<T>
where
    OD: GridSize
        + GridBounds<IndexArray = [isize; 3]>
        + GridSpaceToLinearSpace<IndexArray = [isize; 3]>, // TODO:
    T: Pixel + Sized,
{
    fn grid_blit_from(&mut self, other: GridArray<OD, T>) -> Result<()> {
        let other_offset_dim = other.bounding_box();
        let offset_dim = self.bounding_box();
        let intersection: Option<GridBoundingBox<[isize; 3]>> =
            offset_dim.intersection(&other_offset_dim);

        if let Some(intersection_offset_dim) = intersection {
            let GridIdx([overlap_z_start, overlap_y_start, overlap_x_start]) =
                intersection_offset_dim.min_index();
            let [overlap_z_size, overlap_y_size, overlap_x_size] =
                intersection_offset_dim.axis_size();

            for z in overlap_z_start..overlap_z_start + overlap_z_size as isize {
                for y in overlap_y_start..overlap_y_start + overlap_y_size as isize {
                    let self_start_x =
                        offset_dim.linear_space_index_unchecked([z, y, overlap_x_start]);
                    let other_start_x =
                        other_offset_dim.linear_space_index_unchecked([z, y, overlap_x_start]);

                    self.data.as_mut_slice()[self_start_x..self_start_x + overlap_x_size]
                        .copy_from_slice(
                            &other.data[other_start_x..other_start_x + overlap_x_size],
                        );
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::raster::{GridArray, GridArray2D, GridBlit, GridBoundingBox, GridIdx};

    #[test]
    fn grid_blit_from_2d_0_0() {
        let dim = [4, 4];
        let data = vec![0; 16];

        let mut r1 = GridArray2D::new(dim.into(), data, None).unwrap();

        let data = vec![7; 16];

        let r2 = GridArray2D::new(dim.into(), data, None).unwrap();

        r1.grid_blit_from(r2).unwrap();

        assert_eq!(r1.data, vec![7; 16]);
    }

    #[test]
    fn grid_blit_from_2d_2_2() {
        let data = vec![0; 16];

        let mut r1 = GridArray2D::new([4, 4].into(), data, None).unwrap();

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

        let shifted_idx = GridIdx([2, 2]);
        let shifted_dim = GridBoundingBox::new(shifted_idx, shifted_idx + [3, 3]).unwrap();
        let r2 = GridArray::new(shifted_dim, data, None).unwrap();

        r1.grid_blit_from(r2).unwrap();

        assert_eq!(
            r1.data,
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 4, 5]
        );
    }

    #[test]
    fn grid_blit_from_2d_n2_n2() {
        let data = vec![0; 16];

        let mut r1 = GridArray2D::new([4, 4].into(), data, None).unwrap();

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

        let shifted_idx = GridIdx([-2, -2]);
        let shifted_dim = GridBoundingBox::new(shifted_idx, shifted_idx + [3, 3]).unwrap();
        let r2 = GridArray::new(shifted_dim, data, None).unwrap();

        r1.grid_blit_from(r2).unwrap();

        assert_eq!(
            r1.data,
            vec![10, 11, 0, 0, 14, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
    }
}
