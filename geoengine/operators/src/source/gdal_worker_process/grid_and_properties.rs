use geoengine_datatypes::raster::{
    ChangeGridBounds, GridBoundingBox2D, GridOrEmpty, GridShape2D, RasterProperties,
};

pub struct GridAndProperties<T, D = GridShape2D> {
    pub grid: GridOrEmpty<D, T>,
    pub properties: RasterProperties,
}

impl<T> From<GridAndProperties<T, GridBoundingBox2D>> for GridAndProperties<T, GridShape2D>
where
    T: Copy,
{
    fn from(value: GridAndProperties<T, GridBoundingBox2D>) -> Self {
        GridAndProperties {
            grid: value.grid.unbounded(),
            properties: value.properties,
        }
    }
}
