use super::{
    AxisAlignedRectangle, BoundingBox2D, SpatialBounded, SpatialPartition2D, TimeInterval,
};
use crate::raster::{GeoTransform, GridBoundingBox2D};
use crate::{
    error::{DuplicateBandInQueryBandSelection, QueryBandSelectionMustNotBeEmpty},
    util::Result,
};
use serde::{Deserialize, Serialize};
use snafu::ensure;

/// A spatio-temporal rectangle with a specified resolution and the selected bands
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRectangle<SpatialBounds, AttributeSelection: QueryAttributeSelection> {
    pub spatial_bounds: SpatialBounds,
    pub time_interval: TimeInterval,
    pub attributes: AttributeSelection,
}

impl<SpatialBounds: Copy, A: QueryAttributeSelection> QueryRectangle<SpatialBounds, A> {
    pub fn spatial_query(&self) -> SpatialBounds {
        self.spatial_bounds
    }

    pub fn temporal_query(&self) -> TimeInterval {
        self.time_interval
    }

    pub fn spatial_query_mut(&mut self) -> &mut SpatialBounds {
        &mut self.spatial_bounds
    }

    pub fn temporal_query_mut(&mut self) -> &mut TimeInterval {
        &mut self.time_interval
    }

    pub fn attributes(&self) -> &A {
        &self.attributes
    }

    pub fn attributes_mut(&mut self) -> &mut A {
        &mut self.attributes
    }
}

pub type RasterSpatialQueryRectangle = SpatialGridQueryRectangle;
pub type VectorSpatialQueryRectangle = BoundingBox2D;
pub type PlotSpatialQueryRectangle = BoundingBox2D;

pub type VectorQueryRectangle = QueryRectangle<VectorSpatialQueryRectangle, ColumnSelection>;
pub type RasterQueryRectangle = QueryRectangle<RasterSpatialQueryRectangle, BandSelection>;
pub type PlotQueryRectangle = QueryRectangle<PlotSpatialQueryRectangle, PlotSeriesSelection>;

// Implementation for VectorQueryRectangle and PlotQueryRectangle
impl<S, A> QueryRectangle<S, A>
where
    S: SpatialBounded + Copy,
    A: QueryAttributeSelection,
{
    pub fn new(spatial_bounds: S, time_interval: TimeInterval, attributes: A) -> Self {
        Self {
            spatial_bounds,
            time_interval,
            attributes,
        }
    }

    /// Creates a new `QueryRectangle` from a `BoundingBox2D`, and a `TimeInterval`
    pub fn with_bounds(spatial_bounds: S, time_interval: TimeInterval, attributes: A) -> Self {
        Self {
            spatial_bounds,
            time_interval,
            attributes,
        }
    }

    /// Creates a new `QueryRectangle` with bounds and time from a `RasterQueryRectangle` and supplied attributes.
    ///
    /// # Panics
    /// If the `geo_transform` can't transform the raster bounds into a valid `SpatialPartition`
    pub fn from_raster_query_and_geo_transform_replace_attributes(
        raster_query: &RasterQueryRectangle,
        geo_transform: GeoTransform,
        attributes: A,
    ) -> QueryRectangle<S, A>
    where
        S: AxisAlignedRectangle,
    {
        let bounds =
            geo_transform.grid_to_spatial_bounds(&raster_query.spatial_bounds.grid_bounds());
        let bounding_box = S::from_min_max(bounds.lower_left(), bounds.upper_right())
            .expect("Bounds are already valid");

        QueryRectangle::with_bounds(bounding_box, raster_query.time_interval, attributes)
    }

    pub fn spatial_bounds(&self) -> S {
        self.spatial_bounds
    }
}

impl RasterQueryRectangle {
    /// Creates a new `QueryRectangle` that describes the requested grid.
    /// The spatial query is derived from a vector query rectangle and a `GeoTransform`.
    /// The temporal query is defined by a `TimeInterval`.
    /// NOTE: If the distance between the upper left of the spatial partition and the origin coordinate is not at a multiple of the spatial resolution, the grid bounds will be shifted.
    pub fn with_spatial_query_and_geo_transform<S: SpatialBounded, A: QueryAttributeSelection>(
        vector_query: &QueryRectangle<S, A>,
        geo_transform: GeoTransform,
        attributes: BandSelection,
    ) -> Self {
        Self::new(
            SpatialGridQueryRectangle::with_bounding_box_and_geo_transform(
                vector_query.spatial_bounds.spatial_bounds(),
                geo_transform,
            ),
            vector_query.time_interval,
            attributes,
        )
    }

    pub fn new_with_grid_bounds(
        grid_bounds: GridBoundingBox2D,
        time_interval: TimeInterval,
        attributes: BandSelection,
    ) -> Self {
        Self::new(
            SpatialGridQueryRectangle::new(grid_bounds),
            time_interval,
            attributes,
        )
    }

    /// Creates a new `QueryRectangle` with a spatial grid query defined by a `SpatialGridQueryRectangle` and a temporal query defined by a `TimeInterval`.
    pub fn new(
        spatial_bounds: SpatialGridQueryRectangle,
        time_interval: TimeInterval,
        attributes: BandSelection,
    ) -> Self {
        Self {
            spatial_bounds,
            time_interval,
            attributes,
        }
    }

    pub fn from_qrect_and_geo_transform<
        S: SpatialBounded + AxisAlignedRectangle,
        A: QueryAttributeSelection,
    >(
        query: &QueryRectangle<S, A>,
        bands: BandSelection,
        geo_transform: GeoTransform,
    ) -> Self {
        Self::new(
            SpatialGridQueryRectangle::with_bounding_box_and_geo_transform(
                query.spatial_bounds().as_bbox(),
                geo_transform,
            ),
            query.time_interval,
            bands,
        )
    }

    #[must_use]
    pub fn select_bands(&self, bands: BandSelection) -> Self {
        Self {
            spatial_bounds: self.spatial_bounds,
            time_interval: self.time_interval,
            attributes: bands,
        }
    }
}

pub trait QueryAttributeSelection: Clone + Send + Sync {}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BandSelection(Vec<u32>);

impl BandSelection {
    pub fn new(bands: Vec<u32>) -> Result<Self> {
        fn has_no_duplicates<T: std::hash::Hash + std::cmp::Eq>(vec: &[T]) -> bool {
            let set: std::collections::HashSet<_> = vec.iter().collect();
            set.len() == vec.len()
        }

        ensure!(has_no_duplicates(&bands), DuplicateBandInQueryBandSelection);
        ensure!(!bands.is_empty(), QueryBandSelectionMustNotBeEmpty);

        Ok(Self(bands))
    }

    pub fn new_unchecked(bands: Vec<u32>) -> Self {
        Self(bands)
    }

    pub fn first() -> Self {
        Self(vec![0])
    }

    pub fn first_n(n: u32) -> Self {
        Self((0..n).collect())
    }

    pub fn new_single(band: u32) -> Self {
        Self(vec![band])
    }

    pub fn count(&self) -> u32 {
        self.0.len() as u32
    }

    pub fn as_slice(&self) -> &[u32] {
        &self.0
    }

    pub fn as_vec(&self) -> Vec<u32> {
        self.0.clone()
    }

    pub fn is_single(&self) -> bool {
        self.count() == 1
    }
}

impl From<u32> for BandSelection {
    fn from(value: u32) -> Self {
        Self(vec![value])
    }
}

impl TryFrom<Vec<u32>> for BandSelection {
    type Error = crate::error::Error;

    fn try_from(value: Vec<u32>) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<const N: usize> TryFrom<[u32; N]> for BandSelection {
    type Error = crate::error::Error;

    fn try_from(value: [u32; N]) -> Result<Self, Self::Error> {
        Self::new(value.to_vec())
    }
}

impl QueryAttributeSelection for BandSelection {}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnSelection {}

impl ColumnSelection {
    pub fn all() -> Self {
        Self {}
    }
}

impl QueryAttributeSelection for ColumnSelection {}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlotSeriesSelection {}

impl PlotSeriesSelection {
    pub fn all() -> Self {
        Self {}
    }
}

impl QueryAttributeSelection for PlotSeriesSelection {}

/*
impl From<QueryRectangle<BoundingBox2D>> for QueryRectangle<SpatialPartition2D> {
    fn from(value: QueryRectangle<BoundingBox2D>) -> Self {
impl From<QueryRectangle<BoundingBox2D, ColumnSelection>>
    for QueryRectangle<BoundingBox2D, PlotSeriesSelection>
{
    fn from(value: QueryRectangle<BoundingBox2D, ColumnSelection>) -> Self {
        Self {
            spatial_bounds: value.spatial_bounds,
            time_interval: value.time_interval,
            spatial_resolution: value.spatial_resolution,
            attributes: value.attributes.into(),
        }
    }
}
*/

/// A spatio-temporal grid query with a geotransform and a size in pixels.
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpatialGridQueryRectangle {
    grid_bounds: GridBoundingBox2D,
}

impl SpatialGridQueryRectangle {
    /// Creates a new `SpatialGridQueryRectangle` from a geo transform and a grid bounds.
    pub fn new(grid_bounds: GridBoundingBox2D) -> Self {
        Self { grid_bounds }
    }

    pub fn grid_bounds(&self) -> GridBoundingBox2D {
        self.grid_bounds
    }

    /// Creates a new `SpatialGridQueryRectangle` from a spatial partition and a geo transform.
    pub fn with_partition_and_geo_transform(
        spatial_partition: SpatialPartition2D,
        geo_transform: GeoTransform,
    ) -> Self {
        let grid_bounds = geo_transform.spatial_to_grid_bounds(&spatial_partition);

        Self::new(grid_bounds)
    }

    /// Creates a new `SpatialGridQueryRectangle` from a spatial bounding box and a geo transform.
    pub fn with_bounding_box_and_geo_transform(
        spatial_bounds: BoundingBox2D,
        geo_transform: GeoTransform,
    ) -> Self {
        let grid_bounds =
            geo_transform.bounding_box_2d_to_intersecting_grid_bounds(&spatial_bounds);

        Self::new(grid_bounds)
    }

    pub fn with_vector_query_geo_transform(
        vector_spatial_query: VectorSpatialQueryRectangle,
        geo_transform: GeoTransform,
    ) -> Self {
        let pixel_bounds = geo_transform
            .bounding_box_2d_to_intersecting_grid_bounds(&vector_spatial_query.spatial_bounds());

        Self::new(pixel_bounds)
    }
}
