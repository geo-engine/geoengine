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
pub struct QueryRectangle<SpatialBounds, AttributeSelection> {
    spatial_bounds: SpatialBounds,
    time_interval: TimeInterval,
    attributes: AttributeSelection,
}

impl<SpatialBounds: Copy, A: Clone> QueryRectangle<SpatialBounds, A> {
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

    /// Creates a new `QueryRectangle` from a `BoundingBox2D`, and a `TimeInterval`
    pub fn new(spatial_bounds: SpatialBounds, time_interval: TimeInterval, attributes: A) -> Self {
        Self {
            spatial_bounds,
            time_interval,
            attributes,
        }
    }

    /// Create a clone of `self` with another `TimeInterval`.
    #[must_use]
    pub fn select_time_interval(&self, time_interval: TimeInterval) -> Self {
        Self {
            spatial_bounds: self.spatial_bounds(),
            time_interval,
            attributes: self.attributes().clone(),
        }
    }

    /// Create a clone of `self` with other `SpatialBounds`.
    #[must_use]
    pub fn select_spatial_bounds(&self, spatial_bounds: SpatialBounds) -> Self {
        Self {
            spatial_bounds,
            time_interval: self.time_interval(),
            attributes: self.attributes().clone(),
        }
    }

    /// Create a copy of `self` with other `QueryAttributeSelection`.
    /// This method also allow to change the type of `QueryAttributeSelection`.
    #[must_use]
    pub fn select_attributes<B: QueryAttributeSelection>(
        &self,
        attributes: B,
    ) -> QueryRectangle<SpatialBounds, B> {
        QueryRectangle {
            spatial_bounds: self.spatial_bounds,
            time_interval: self.time_interval,
            attributes,
        }
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
    /// Creates a new `QueryRectangle` with bounds and time from a `RasterQueryRectangle` and supplied attributes.
    ///
    /// # Panics
    /// If the `geo_transform` can't transform the raster bounds into a valid `SpatialPartition`
    pub fn from_raster_query_and_geo_transform_replace_attributes(
        raster_query: &RasterQueryRectangle,
        geo_transform: GeoTransform,
        attributes: A,
    ) -> QueryRectangle<BoundingBox2D, A> {
        let bounds = geo_transform.grid_to_spatial_bounds(&raster_query.spatial_bounds());
        let bounding_box = BoundingBox2D::from_min_max(bounds.lower_left(), bounds.upper_right())
            .expect("Bounds are already valid");

        QueryRectangle::new(bounding_box, raster_query.time_interval, attributes)
    }
}

impl RasterQueryRectangle {
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BandSelectionIter {
    band_selection: BandSelection,
    next_index: usize,
}

impl BandSelectionIter {
    pub fn new(band_selection: BandSelection) -> Self {
        Self {
            band_selection,
            next_index: 0,
        }
    }

    pub fn reset(&mut self) {
        self.next_index = 0;
    }
}

impl Iterator for BandSelectionIter {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_index >= self.band_selection.0.len() {
            return None;
        }

        let item = self.band_selection.0[self.next_index];
        self.next_index += 1;
        Some(item)
    }
}

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
