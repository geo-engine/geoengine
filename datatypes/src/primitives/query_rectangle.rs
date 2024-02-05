use crate::raster::{GeoTransform, GridBoundingBox2D, GridBounds};

use super::{
    AxisAlignedRectangle, BoundingBox2D, Coordinate2D, SpatialBounded, SpatialPartition2D,
    SpatialPartitioned, SpatialResolution, TimeInterval,
};
use crate::{
    error::{DuplicateBandInQueryBandSelection, QueryBandSelectionMustNotBeEmpty},
    util::Result,
};
use serde::{Deserialize, Serialize};
use snafu::ensure;

/// A spatio-temporal rectangle with a specified resolution and the selected bands
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRectangle<SpatialQuery, AttributeSelection: QueryAttributeSelection> {
    pub spatial_query: SpatialQuery,
    pub time_interval: TimeInterval,
    pub attributes: AttributeSelection,
}

impl<SpatialQuery: Copy, A: QueryAttributeSelection> QueryRectangle<SpatialQuery, A> {
    pub fn spatial_query(&self) -> SpatialQuery {
        self.spatial_query
    }

    pub fn temporal_query(&self) -> TimeInterval {
        self.time_interval
    }

    pub fn spatial_query_mut(&mut self) -> &mut SpatialQuery {
        &mut self.spatial_query
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

pub type VectorQueryRectangle =
    QueryRectangle<SpatialQueryRectangle<BoundingBox2D>, ColumnSelection>;
pub type RasterQueryRectangle = QueryRectangle<SpatialGridQueryRectangle, BandSelection>;
pub type PlotQueryRectangle =
    QueryRectangle<SpatialQueryRectangle<BoundingBox2D>, PlotSeriesSelection>;

// Implementation for VectorQueryRectangle and PlotQueryRectangle
impl<S, A> QueryRectangle<SpatialQueryRectangle<S>, A>
where
    S: SpatialBounded,
    A: QueryAttributeSelection,
{
    pub fn new(
        spatial_bounds: SpatialQueryRectangle<S>,
        time_interval: TimeInterval,
        attributes: A,
    ) -> Self {
        Self {
            spatial_query: spatial_bounds,
            time_interval,
            attributes,
        }
    }

    /// Creates a new `QueryRectangle` from a `BoundingBox2D`, a `TimeInterval`, and a `SpatialResolution`.
    pub fn with_bounds_and_resolution(
        spatial_bounds: S,
        time_interval: TimeInterval,
        spatial_resolution: SpatialResolution,
        attributes: A,
    ) -> Self {
        Self {
            spatial_query: SpatialQueryRectangle {
                spatial_bounds,
                spatial_resolution,
            },
            time_interval,
            attributes,
        }
    }
}

impl RasterQueryRectangle {
    /// Creates a new `QueryRectangle` that describes the requested grid.
    /// The spatial query is defined by a `SpatialGridQueryRectangle`, which is derived from a `SpatialPartition2D`, a `SpatialResolution` and a origin `Coordinate2D`.
    /// The temporal query is defined by a `TimeInterval`.
    /// NOTE: If the distance between the upper left of the spatial partition and the origin coordinate is not at a multiple of the spatial resolution, the grid bounds will be shifted.
    pub fn with_partition_and_resolution_and_origin(
        spatial_partition: SpatialPartition2D,
        spatial_resolution: SpatialResolution,
        origin_coordinate: Coordinate2D,
        time_interval: TimeInterval,
        attributes: BandSelection,
    ) -> Self {
        Self::new(
            SpatialGridQueryRectangle::with_partition_and_resolution_and_origin(
                spatial_partition,
                spatial_resolution,
                origin_coordinate,
            ),
            time_interval,
            attributes,
        )
    }

    /// Creates a new `QueryRectangle` that describes the requested grid.
    /// The spatial query is derived from a vector query rectangle and a grid origin.
    /// The temporal query is defined by a `TimeInterval`.
    /// NOTE: If the distance between the upper left of the spatial partition and the origin coordinate is not at a multiple of the spatial resolution, the grid bounds will be shifted.
    pub fn with_vector_query_and_grid_origin(
        vector_query: VectorQueryRectangle,
        grid_origin: Coordinate2D,
        attributes: BandSelection,
    ) -> Self {
        Self::new(
            SpatialGridQueryRectangle::with_vector_query_and_grid_origin(
                vector_query.spatial_query(),
                grid_origin,
            ),
            vector_query.time_interval,
            attributes,
        )
    }

    pub fn with_spatial_query_and_grid_origin(
        spatial_query: SpatialQueryRectangle<BoundingBox2D>,
        grid_origin: Coordinate2D,
        time_interval: TimeInterval,
        attributes: BandSelection,
    ) -> Self {
        Self::new(
            SpatialGridQueryRectangle::with_vector_query_and_grid_origin(
                spatial_query,
                grid_origin,
            ),
            time_interval,
            attributes,
        )
    }

    /// Creates a new `QueryRectangle` that describes the requested grid.
    /// The spatial query is defined by a `SpatialGridQueryRectangle`, which is derived from a `SpatialPartition2D` and a `SpatialResolution`.
    /// The temporal query is defined by a `TimeInterval`.
    pub fn with_partition_and_resolution(
        spatial_partition: SpatialPartition2D,
        spatial_resolution: SpatialResolution,
        time_interval: TimeInterval,
        attributes: BandSelection,
    ) -> Self {
        Self::new(
            SpatialGridQueryRectangle::_with_partition_and_resolution(
                spatial_partition,
                spatial_resolution,
            ),
            time_interval,
            attributes,
        )
    }

    pub fn with_grid_bounds_and_resolution(
        grid_bounds: GridBoundingBox2D,
        geo_transform: GeoTransform,
        time_interval: TimeInterval,
        attributes: BandSelection,
    ) -> Self {
        Self::new(
            SpatialGridQueryRectangle::new(geo_transform, grid_bounds),
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
            spatial_query: spatial_bounds,
            time_interval,
            attributes,
        }
    }

    pub fn from_qrect_and_bands<A: QueryAttributeSelection>(
        query: &QueryRectangle<SpatialQueryRectangle<BoundingBox2D>, A>,
        bands: BandSelection,
    ) -> Self {
        Self::new(
            SpatialGridQueryRectangle::_with_partition_and_resolution(
                query.spatial_query.spatial_partition(),
                query.spatial_query.spatial_resolution,
            ), // TODO: can we always do this into?
            query.temporal_query(),
            bands,
        )
    }
}

pub type RasterSpatialQueryRectangle = SpatialGridQueryRectangle;
pub type VectorSpatialQueryRectangle = SpatialQueryRectangle<BoundingBox2D>;
pub type PlotSpatialQueryRectangle = SpatialQueryRectangle<BoundingBox2D>;

/// A spatial rectangle with a specified resolution
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpatialQueryRectangle<SpatialBounds> {
    pub spatial_bounds: SpatialBounds,
    pub spatial_resolution: SpatialResolution,
}

impl SpatialBounded for SpatialQueryRectangle<BoundingBox2D> {
    fn spatial_bounds(&self) -> BoundingBox2D {
        self.spatial_bounds
    }
}

impl SpatialPartitioned for SpatialQueryRectangle<BoundingBox2D> {
    fn spatial_partition(&self) -> SpatialPartition2D {
        SpatialPartition2D::with_bbox_and_resolution(self.spatial_bounds, self.spatial_resolution)
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

impl SpatialPartitioned for VectorQueryRectangle {
    fn spatial_partition(&self) -> SpatialPartition2D {
        self.spatial_query.spatial_partition()
    }
}

impl SpatialPartitioned for SpatialQueryRectangle<SpatialPartition2D> {
    fn spatial_partition(&self) -> SpatialPartition2D {
        self.spatial_bounds
    }
}

impl SpatialPartitioned for PlotQueryRectangle {
    fn spatial_partition(&self) -> SpatialPartition2D {
        self.spatial_query.spatial_partition()
    }
}

impl SpatialPartitioned for RasterQueryRectangle {
    fn spatial_partition(&self) -> SpatialPartition2D {
        self.spatial_query.spatial_partition()
    }
}

impl From<RasterQueryRectangle> for VectorQueryRectangle {
    fn from(value: RasterQueryRectangle) -> Self {
        Self::with_bounds_and_resolution(
            value.spatial_query().spatial_bounds(),
            value.time_interval,
            value.spatial_query().geo_transform.spatial_resolution(),
            ColumnSelection::all(), // TODO: this will propably stop working once the selection can do more then "all"
        )
    }
}

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
    pub geo_transform: GeoTransform,
    pub grid_bounds: GridBoundingBox2D,
}

impl SpatialPartitioned for SpatialGridQueryRectangle {
    fn spatial_partition(&self) -> SpatialPartition2D {
        self.geo_transform.grid_to_spatial_bounds(&self.grid_bounds)
    }
}

impl SpatialBounded for SpatialGridQueryRectangle {
    fn spatial_bounds(&self) -> BoundingBox2D {
        self.spatial_partition().as_bbox()
    }
}

impl SpatialGridQueryRectangle {
    pub fn new(geo_transform: GeoTransform, grid_bounds: GridBoundingBox2D) -> Self {
        Self {
            geo_transform,
            grid_bounds,
        }
    }

    pub fn spatial_resolution(&self) -> SpatialResolution {
        self.geo_transform.spatial_resolution()
    }

    pub fn origin_coordinate(&self) -> Coordinate2D {
        self.geo_transform.origin_coordinate()
    }

    /// Creates a new `SpatialGridQueryRectangle` from a spatial partition and a spatial resolution.
    /// The origin of the grid is set to the upper left corner of the spatial partition.
    /// TODO: we may need to replace this with a version that supports positive and negative (pixel) resolutions.
    fn _with_partition_and_resolution(
        spatial_partition: SpatialPartition2D,
        spatial_resolution: SpatialResolution,
    ) -> Self {
        // we need to create a geo transform here. There might be a better way to do this.
        debug_assert!(spatial_resolution.x > 0.0);
        debug_assert!(spatial_resolution.y > 0.0);

        let geo_transform = GeoTransform::new(
            spatial_partition.upper_left(),
            spatial_resolution.x,
            spatial_resolution.y.abs() * -1.0,
        );

        // Once we have the geo transform, we can calculate the grid bounds.
        let grid_bounds = geo_transform.spatial_to_grid_bounds(&spatial_partition);

        Self {
            geo_transform,
            grid_bounds,
        }
    }

    /// Creates a new `SpatialGridQueryRectangle` from a spatial partition and a spatial resolution.
    /// The origin of the grid is set to the provided origin coordinate.
    /// NOTE: If the distance between the upper left of the spatial partition and the origin coordinate is not at a multiple of the spatial resolution, the grid bounds will be shifted.
    pub fn with_partition_and_resolution_and_origin(
        spatial_partition: SpatialPartition2D,
        spatial_resolution: SpatialResolution,
        origin_coordinate: Coordinate2D,
    ) -> Self {
        let SpatialGridQueryRectangle {
            geo_transform,
            grid_bounds,
        } = Self::_with_partition_and_resolution(spatial_partition, spatial_resolution);

        let offset = geo_transform.coordinate_to_grid_idx_2d(origin_coordinate);

        let shifted_grid_bounds = GridBoundingBox2D::new(
            grid_bounds.min_index() - offset,
            grid_bounds.max_index() - offset,
        )
        .expect(
            "shifting the grid bounds must not fail since the offset is identical for min and max",
        );

        let shifted_geo_transform = GeoTransform::new(
            origin_coordinate,
            geo_transform.x_pixel_size(),
            geo_transform.y_pixel_size(),
        );

        Self {
            geo_transform: shifted_geo_transform,
            grid_bounds: shifted_grid_bounds,
        }
    }

    pub fn with_vector_query_and_grid_origin(
        vector_spatial_query: VectorSpatialQueryRectangle,
        origin_coordinate: Coordinate2D,
    ) -> Self {
        Self::with_partition_and_resolution_and_origin(
            vector_spatial_query.spatial_partition(),
            vector_spatial_query.spatial_resolution,
            origin_coordinate,
        )
    }
}
