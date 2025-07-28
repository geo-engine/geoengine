use super::{AxisAlignedRectangle, BoundingBox2D, TimeInterval};
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
    spatial_bounds: SpatialBounds,
    time_interval: TimeInterval,
    attributes: AttributeSelection,
}

impl<SpatialBounds: Copy, A: QueryAttributeSelection> QueryRectangle<SpatialBounds, A> {
    pub fn time_interval(&self) -> TimeInterval {
        self.time_interval
    }

    pub fn spatial_bounds(&self) -> SpatialBounds {
        self.spatial_bounds
    }

    pub fn spatial_bounds_mut(&mut self) -> &mut SpatialBounds {
        &mut self.spatial_bounds
    }

    pub fn time_interval_mut(&mut self) -> &mut TimeInterval {
        &mut self.time_interval
    }

    pub fn attributes(&self) -> &A {
        &self.attributes
    }

    pub fn attributes_mut(&mut self) -> &mut A {
        &mut self.attributes
    }
}

pub type VectorQueryRectangle = QueryRectangle<BoundingBox2D, ColumnSelection>;
pub type RasterQueryRectangle = QueryRectangle<GridBoundingBox2D, BandSelection>;
pub type PlotQueryRectangle = QueryRectangle<BoundingBox2D, PlotSeriesSelection>;

// Implementation for VectorQueryRectangle and PlotQueryRectangle
impl<A> QueryRectangle<BoundingBox2D, A>
where
    A: QueryAttributeSelection,
{
    pub fn new(spatial_bounds: BoundingBox2D, time_interval: TimeInterval, attributes: A) -> Self {
        Self {
            spatial_bounds,
            time_interval,
            attributes,
        }
    }

    /// Creates a new `QueryRectangle` from a `BoundingBox2D`, and a `TimeInterval`
    pub fn with_bounds(
        spatial_bounds: BoundingBox2D,
        time_interval: TimeInterval,
        attributes: A,
    ) -> Self {
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
    ) -> QueryRectangle<BoundingBox2D, A> {
        let bounds = geo_transform.grid_to_spatial_bounds(&raster_query.grid_bounds());
        let bounding_box = BoundingBox2D::from_min_max(bounds.lower_left(), bounds.upper_right())
            .expect("Bounds are already valid");

        QueryRectangle::with_bounds(bounding_box, raster_query.time_interval, attributes)
    }
}

impl RasterQueryRectangle {
    pub fn new_with_grid_bounds(
        grid_bounds: GridBoundingBox2D,
        time_interval: TimeInterval,
        attributes: BandSelection,
    ) -> Self {
        Self::new(grid_bounds, time_interval, attributes)
    }

    /// Creates a new `QueryRectangle` with a spatial grid query defined by a `GridBoundingBox2D` and a temporal query defined by a `TimeInterval`.
    pub fn new(
        grid_bounds: GridBoundingBox2D,
        time_interval: TimeInterval,
        attributes: BandSelection,
    ) -> Self {
        Self {
            spatial_bounds: grid_bounds,
            time_interval,
            attributes,
        }
    }

    /// Creates a new `QueryRectangle` that describes the requested grid.
    /// The spatial query is derived from a vector query rectangle and a `GeoTransform`.
    /// The temporal query is defined by a `TimeInterval`.
    /// NOTE: If the distance between the upper left of the spatial partition and the origin coordinate is not at a multiple of the spatial resolution, the grid bounds will be shifted.
    pub fn from_bounds_and_geo_transform<A: QueryAttributeSelection>(
        query: &QueryRectangle<BoundingBox2D, A>,
        bands: BandSelection,
        geo_transform: GeoTransform,
    ) -> Self {
        let grid_bounds =
            geo_transform.bounding_box_2d_to_intersecting_grid_bounds(&query.spatial_bounds());
        Self::new(grid_bounds, query.time_interval, bands)
    }

    #[must_use]
    pub fn select_bands(&self, bands: BandSelection) -> Self {
        Self {
            spatial_bounds: self.spatial_bounds,
            time_interval: self.time_interval,
            attributes: bands,
        }
    }

    pub fn grid_bounds(&self) -> GridBoundingBox2D {
        self.spatial_bounds
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
