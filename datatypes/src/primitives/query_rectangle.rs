use super::{
    AxisAlignedRectangle, BoundingBox2D, SpatialPartition2D, SpatialPartitioned, SpatialResolution,
    TimeInterval,
};
use serde::{Deserialize, Serialize};

/// A spatio-temporal rectangle with a specified resolution and the selected bands
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRectangle<
    SpatialBounds: AxisAlignedRectangle,
    AttributeSelection: QueryAttributeSelection,
> {
    pub spatial_bounds: SpatialBounds,
    pub time_interval: TimeInterval,
    pub spatial_resolution: SpatialResolution,
    #[serde(default)] // TODO: remove once all clients send this
    pub attributes: AttributeSelection,
}

pub trait QueryAttributeSelection: Clone + Send + Sync + Default /* TOOD: remove */ {}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// TODO: custom deserializer that checks for duplicates(?)
pub struct BandSelection(Vec<usize>);

impl Default for BandSelection {
    fn default() -> Self {
        Self(vec![0]) // TODO: default should maybe be all bands? But that would require knowledge of all AVAILABLE bands
    }
}

impl BandSelection {
    pub fn new(mut bands: Vec<usize>) -> Self {
        fn has_no_duplicates<T: std::hash::Hash + std::cmp::Eq>(vec: &[T]) -> bool {
            let set: std::collections::HashSet<_> = vec.iter().collect();
            set.len() == vec.len()
        }
        // TODO: `ensure` instead of `debug_assert`?
        debug_assert!(has_no_duplicates(&bands), "Input bands have duplicates");
        debug_assert!(!bands.is_empty(), "Input bands empty");

        bands.sort_unstable(); // TODO: should order of bands matter? Or: introduce a restacking operation?
        Self(bands)
    }

    pub fn new_single(band: usize) -> Self {
        Self(vec![band])
    }

    pub fn count(&self) -> usize {
        self.0.len()
    }

    pub fn as_slice(&self) -> &[usize] {
        &self.0
    }

    pub fn as_vec(&self) -> Vec<usize> {
        self.0.clone()
    }
}

impl From<usize> for BandSelection {
    fn from(value: usize) -> Self {
        Self(vec![value])
    }
}

impl From<Vec<usize>> for BandSelection {
    fn from(value: Vec<usize>) -> Self {
        Self::new(value)
    }
}

impl<const N: usize> From<[usize; N]> for BandSelection {
    fn from(value: [usize; N]) -> Self {
        Self::new(value.to_vec())
    }
}

impl QueryAttributeSelection for BandSelection {}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct ColumnSelection {}

impl QueryAttributeSelection for ColumnSelection {}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct PlotSeriesSelection {}

impl QueryAttributeSelection for PlotSeriesSelection {}

pub type VectorQueryRectangle = QueryRectangle<BoundingBox2D, ColumnSelection>;
pub type RasterQueryRectangle = QueryRectangle<SpatialPartition2D, BandSelection>;
pub type PlotQueryRectangle = QueryRectangle<BoundingBox2D, PlotSeriesSelection>;

impl SpatialPartitioned for QueryRectangle<BoundingBox2D, ColumnSelection> {
    fn spatial_partition(&self) -> SpatialPartition2D {
        SpatialPartition2D::with_bbox_and_resolution(self.spatial_bounds, self.spatial_resolution)
    }
}

impl SpatialPartitioned for QueryRectangle<BoundingBox2D, PlotSeriesSelection> {
    fn spatial_partition(&self) -> SpatialPartition2D {
        SpatialPartition2D::with_bbox_and_resolution(self.spatial_bounds, self.spatial_resolution)
    }
}

impl SpatialPartitioned for QueryRectangle<SpatialPartition2D, BandSelection> {
    fn spatial_partition(&self) -> SpatialPartition2D {
        self.spatial_bounds
    }
}

impl From<QueryRectangle<BoundingBox2D, ColumnSelection>>
    for QueryRectangle<SpatialPartition2D, BandSelection>
{
    fn from(value: QueryRectangle<BoundingBox2D, ColumnSelection>) -> Self {
        Self {
            spatial_bounds: value.spatial_partition(),
            time_interval: value.time_interval,
            spatial_resolution: value.spatial_resolution,
            attributes: Default::default(), // TODO: how to do this automatically? maybe we have to remove this From implementation
        }
    }
}

impl From<QueryRectangle<BoundingBox2D, PlotSeriesSelection>>
    for QueryRectangle<SpatialPartition2D, BandSelection>
{
    fn from(value: QueryRectangle<BoundingBox2D, PlotSeriesSelection>) -> Self {
        Self {
            spatial_bounds: value.spatial_partition(),
            time_interval: value.time_interval,
            spatial_resolution: value.spatial_resolution,
            attributes: Default::default(), // TODO: how to do this automatically? maybe we have to remove this From implementation
        }
    }
}

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

impl From<QueryRectangle<BoundingBox2D, PlotSeriesSelection>>
    for QueryRectangle<BoundingBox2D, ColumnSelection>
{
    fn from(value: QueryRectangle<BoundingBox2D, PlotSeriesSelection>) -> Self {
        Self {
            spatial_bounds: value.spatial_bounds,
            time_interval: value.time_interval,
            spatial_resolution: value.spatial_resolution,
            attributes: value.attributes.into(),
        }
    }
}

impl From<ColumnSelection> for PlotSeriesSelection {
    fn from(_: ColumnSelection) -> Self {
        Self {}
    }
}

impl From<PlotSeriesSelection> for ColumnSelection {
    fn from(_: PlotSeriesSelection) -> Self {
        Self {}
    }
}
