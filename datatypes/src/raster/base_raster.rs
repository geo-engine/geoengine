use crate::raster::{
    Capacity, Dim, GenericRaster, GeoTransform, GridDimension, GridIndex, GridPixelAccess,
    GridPixelAccessMut, Raster,
};
use crate::util::Result;
use crate::{
    error,
    primitives::{BoundingBox2D, SpatialBounded, TemporalBounded, TimeInterval},
};
use snafu::ensure;
use std::convert::AsRef;
use std::fmt::Debug;

#[derive(Clone, Debug)]
pub struct BaseRaster<D, T, C> {
    grid_dimension: D,
    data_container: C,
    no_data_value: Option<T>,
    geo_transform: GeoTransform,
    temporal_bounds: TimeInterval,
}

impl<D, T, C> BaseRaster<D, T, C>
where
    D: GridDimension,
    C: Capacity,
{
    /// Generates a new raster
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::raster::{Raster, Dim, Raster2D};
    /// use geoengine_datatypes::primitives::TimeInterval;
    ///
    /// let mut raster2d = Raster2D::new(
    ///    [3, 2].into(),
    ///    vec![1,2,3,4,5,6],
    ///    None,
    ///    TimeInterval::default(),
    ///    [1.0, 1.0, 0.0, 1.0, 0.0, 1.0].into(),
    /// ).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// This constructor fails if the data container's capacity is different from the grid's dimension capacity
    ///
    pub fn new(
        grid_dimension: D,
        data_container: C,
        no_data_value: Option<T>,
        temporal_bounds: TimeInterval,
        geo_transform: GeoTransform,
    ) -> Result<Self> {
        ensure!(
            grid_dimension.capacity() == data_container.capacity(),
            error::DimnsionCapacityDoesNotMatchDataCapacity {
                dimension_cap: grid_dimension.capacity(),
                data_cap: data_container.capacity()
            }
        );

        Ok(Self {
            grid_dimension,
            data_container,
            no_data_value,
            temporal_bounds,
            geo_transform,
        })
    }
}

impl<D, T, C> TemporalBounded for BaseRaster<D, T, C> {
    fn temporal_bounds(&self) -> TimeInterval {
        self.temporal_bounds
    }
}

impl<D, C, T> SpatialBounded for BaseRaster<D, C, T>
where
    D: GridDimension,
{
    fn spatial_bounds(&self) -> BoundingBox2D {
        let top_left_coord = self.geo_transform.grid_2d_to_coordinate_2d((0, 0));
        let lower_right_coord = self.geo_transform.grid_2d_to_coordinate_2d((
            self.grid_dimension.size_of_y_axis(),
            self.grid_dimension.size_of_x_axis(),
        ));
        BoundingBox2D::new_upper_left_lower_right_unchecked(top_left_coord, lower_right_coord)
    }
}

impl<D, T, C> Raster<D, T, C> for BaseRaster<D, T, C>
where
    D: GridDimension,
    T: Copy,
    C: Capacity,
{
    fn dimension(&self) -> &D {
        &self.grid_dimension
    }
    fn no_data_value(&self) -> Option<T> {
        self.no_data_value
    }
    fn data_container(&self) -> &C {
        &self.data_container
    }
    fn geo_transform(&self) -> &GeoTransform {
        &self.geo_transform
    }
}

impl<D, T, C, I> GridPixelAccess<T, I> for BaseRaster<D, T, C>
where
    D: GridDimension,
    I: GridIndex<D>,
    C: AsRef<[T]> + Capacity,
    T: Copy,
{
    fn pixel_value_at_grid_index(&self, grid_index: &I) -> Result<T> {
        let index = grid_index.grid_index_to_1d_index(&self.grid_dimension)?;
        Ok(self.data_container.as_ref()[index])
    }
}

impl<D, T, C, I> GridPixelAccessMut<T, I> for BaseRaster<D, T, C>
where
    D: GridDimension,
    I: GridIndex<D>,
    C: AsMut<[T]> + Capacity,
    T: Copy,
{
    fn set_pixel_value_at_grid_index(&mut self, grid_index: &I, value: T) -> Result<()> {
        let index = grid_index.grid_index_to_1d_index(&self.grid_dimension)?;
        self.data_container.as_mut()[index] = value;
        Ok(())
    }
}

pub type Raster2D<T> = BaseRaster<Dim<[usize; 2]>, T, Vec<T>>;
pub type Raster3D<T> = BaseRaster<Dim<[usize; 3]>, T, Vec<T>>;

impl<T: Send + Debug> GenericRaster for Raster2D<T> {
    fn get(&self) {
        unimplemented!()
    }
}

pub trait Blit<T: Copy> {
    fn blit(&mut self, source: Raster2D<T>) -> Result<()>;
}

impl<T: Copy> Blit<T> for Raster2D<T> {
    /// Copy `source` raster pixels into this raster, fails if the rasters do not overlap
    #[allow(clippy::float_cmp)]
    fn blit(&mut self, source: Raster2D<T>) -> Result<()> {
        // TODO: same crs
        // TODO: allow approximately equal pixel sizes?
        // TODO: ensure pixels are aligned
        ensure!(
            (self.geo_transform.x_pixel_size == source.geo_transform.x_pixel_size)
                && (self.geo_transform.y_pixel_size == source.geo_transform.y_pixel_size),
            error::Blit {
                details: "Incompatible pixel size"
            }
        );

        let intersection = self
            .spatial_bounds()
            .intersection(&source.spatial_bounds())
            .ok_or(error::Error::Blit {
                details: "No overlapping region".into(),
            })?;

        println!(
            "self: {:?}\ntile: {:?}\nintersection: {:?}",
            self.spatial_bounds(),
            source.spatial_bounds(),
            intersection
        );

        let (start_y, start_x) = self
            .geo_transform
            .coordinate_2d_to_grid_2d(&intersection.upper_left());
        let (stop_y, stop_x) = self
            .geo_transform
            .coordinate_2d_to_grid_2d(&intersection.lower_right());

        let (start_source_y, start_source_x) = source
            .geo_transform
            .coordinate_2d_to_grid_2d(&intersection.upper_left());

        println!("start: {:?}", (start_y, start_x));
        println!("stop: {:?}", (stop_y, stop_x));
        println!("start_source: {:?}", (start_source_y, start_source_x));

        // TODO: check if dimension of self and source fit?
        let width = stop_x - start_x;

        for y in 0..stop_y - start_y {
            let index =
                (start_y + y, start_x).grid_index_to_1d_index_unchecked(&self.grid_dimension);
            let index_source = (start_source_y + y, start_source_x)
                .grid_index_to_1d_index_unchecked(&source.grid_dimension);

            self.data_container.as_mut_slice()[index..index + width].copy_from_slice(
                &source.data_container.as_slice()[index_source..index_source + width],
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Dim, GridPixelAccess, GridPixelAccessMut, Raster2D, TimeInterval};
    use crate::raster::base_raster::Blit;
    use crate::raster::GeoTransform;

    #[test]
    fn simple_raster_2d() {
        let dim = [3, 2];
        let data = vec![1, 2, 3, 4, 5, 6];
        let geo_transform = [1.0, 1.0, 0.0, 1.0, 0.0, 1.0];
        let temporal_bounds: TimeInterval = TimeInterval::default();
        Raster2D::new(
            dim.into(),
            data,
            None,
            temporal_bounds,
            geo_transform.into(),
        )
        .unwrap();
    }

    #[test]
    fn simple_raster_2d_at_tuple() {
        let tuple_index = (1, 1);

        let dim = [3, 2];
        let data = vec![1, 2, 3, 4, 5, 6];
        let geo_transform = [1.0, 1.0, 0.0, 1.0, 0.0, 1.0];
        let temporal_bounds: TimeInterval = TimeInterval::default();
        let raster2d = Raster2D::new(
            dim.into(),
            data,
            None,
            temporal_bounds,
            geo_transform.into(),
        )
        .unwrap();
        assert_eq!(raster2d.pixel_value_at_grid_index(&tuple_index).unwrap(), 4);
    }

    #[test]
    fn simple_raster_2d_at_arr() {
        let dim_index = Dim::from([1, 1]);

        let dim = [3, 2];
        let data = vec![1, 2, 3, 4, 5, 6];
        let geo_transform = [1.0, 1.0, 0.0, 1.0, 0.0, 1.0];
        let temporal_bounds: TimeInterval = TimeInterval::default();
        let raster2d = Raster2D::new(
            dim.into(),
            data,
            None,
            temporal_bounds,
            geo_transform.into(),
        )
        .unwrap();
        let value = raster2d.pixel_value_at_grid_index(&dim_index).unwrap();
        assert!(value == 4);
    }

    #[test]
    fn simple_raster_2d_set_at_tuple() {
        let tuple_index = (1, 1);

        let dim = [3, 2];
        let data = vec![1, 2, 3, 4, 5, 6];
        let geo_transform = [1.0, 1.0, 0.0, 1.0, 0.0, 1.0];
        let temporal_bounds: TimeInterval = TimeInterval::default();
        let mut raster2d = Raster2D::new(
            dim.into(),
            data,
            None,
            temporal_bounds,
            geo_transform.into(),
        )
        .unwrap();

        raster2d
            .set_pixel_value_at_grid_index(&tuple_index, 9)
            .unwrap();
        let value = raster2d.pixel_value_at_grid_index(&tuple_index).unwrap();
        assert!(value == 9);
        assert_eq!(raster2d.data_container, [1, 2, 3, 9, 5, 6]);
    }

    #[test]
    fn test_blit() {
        let dim = [4, 4];
        let data = vec![0; 16];
        let geo_transform = GeoTransform::new((0.0, 10.0).into(), 10.0 / 4.0, -10.0 / 4.0);
        let temporal_bounds: TimeInterval = TimeInterval::default();

        let mut r1 = Raster2D::new(dim.into(), data, None, temporal_bounds, geo_transform).unwrap();

        let data = vec![7; 16];
        let geo_transform = GeoTransform::new((5.0, 15.0).into(), 10.0 / 4.0, -10.0 / 4.0);

        let r2 = Raster2D::new(dim.into(), data, None, temporal_bounds, geo_transform).unwrap();

        r1.blit(r2).unwrap();

        assert_eq!(
            r1.data_container,
            vec![0, 0, 7, 7, 0, 0, 7, 7, 0, 0, 0, 0, 0, 0, 0, 0]
        );
    }
}
