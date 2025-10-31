use crate::error::{
    Error, RasterBandNameMustNotBeEmpty, RasterBandNameTooLong, RasterBandNamesMustBeUnique,
};
use crate::util::Result;
use geoengine_datatypes::operations::reproject::{CoordinateProjection, ReprojectClipped};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BandSelection, BoundingBox2D, ColumnSelection, Coordinate2D,
    FeatureDataType, Measurement, PlotSeriesSelection, QueryAttributeSelection, QueryRectangle,
    RegularTimeDimension, SpatialPartition2D, SpatialResolution, TimeDimension, TimeInstance,
    TimeInterval, TimeStep,
};
use geoengine_datatypes::raster::{
    GeoTransform, GeoTransformAccess, Grid, GridBoundingBox2D, GridShape2D, GridShapeAccess,
    SpatialGridDefinition, TilingSpatialGridDefinition, TilingSpecification,
};
use geoengine_datatypes::util::ByteSize;
use geoengine_datatypes::{
    collections::VectorDataType, raster::RasterDataType, spatial_reference::SpatialReferenceOption,
};
use postgres_types::{FromSql, IsNull, ToSql, Type};
use serde::{Deserialize, Deserializer, Serialize};
use snafu::ensure;
use std::collections::{HashMap, HashSet};
use std::ops::Index;

/// A descriptor that contains information about the query result, for instance, the data type
/// and spatial reference.
pub trait ResultDescriptor: Clone + Serialize {
    type DataType;
    type QueryRectangleSpatialBounds;
    type QueryRectangleAttributeSelection: QueryAttributeSelection;

    // Check the `query` against the `ResultDescriptor` and return `true` if the query is valid
    // and `false` if, e.g., invalid attributes are specified
    fn validate_query(
        &self,
        query: &QueryRectangle<
            Self::QueryRectangleSpatialBounds,
            Self::QueryRectangleAttributeSelection,
        >,
    ) -> Result<()>;

    /// Return the type-specific result data type
    fn data_type(&self) -> Self::DataType;

    /// Return the spatial reference of the result
    fn spatial_reference(&self) -> SpatialReferenceOption;

    /// Map one descriptor to another one
    #[must_use]
    fn map<F>(&self, f: F) -> Self
    where
        F: Fn(&Self) -> Self,
    {
        f(self)
    }

    /// Map one descriptor to another one by modifying only the spatial reference
    #[must_use]
    fn map_data_type<F>(&self, f: F) -> Self
    where
        F: Fn(&Self::DataType) -> Self::DataType;

    /// Map one descriptor to another one by modifying only the data type
    #[must_use]
    fn map_spatial_reference<F>(&self, f: F) -> Self
    where
        F: Fn(&SpatialReferenceOption) -> SpatialReferenceOption;

    /// Map one descriptor to another one by modifying only the time
    #[must_use]
    fn map_time<F>(&self, f: F) -> Self
    where
        F: Fn(&Option<TimeInterval>) -> Option<TimeInterval>;
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum SpatialGridDescriptorState {
    /// The spatial grid represents a native dataset
    Source,
    /// The spatial grid was created by merging two non equal spatial grids
    Merged,
}

impl SpatialGridDescriptorState {
    #[must_use]
    pub fn merge(self, other: Self) -> Self {
        match (self, other) {
            (SpatialGridDescriptorState::Source, SpatialGridDescriptorState::Source) => {
                SpatialGridDescriptorState::Source
            }
            _ => SpatialGridDescriptorState::Merged,
        }
    }

    pub fn is_source(self) -> bool {
        self == SpatialGridDescriptorState::Source
    }

    pub fn is_derived(self) -> bool {
        !self.is_source()
    }
}

/// A `ResultDescriptor` for raster queries
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SpatialGridDescriptor {
    spatial_grid: SpatialGridDefinition,
    state: SpatialGridDescriptorState,
}

impl SpatialGridDescriptor {
    pub fn new_source(spatial_grid_def: SpatialGridDefinition) -> Self {
        Self {
            spatial_grid: spatial_grid_def,
            state: SpatialGridDescriptorState::Source,
        }
    }

    pub fn source_from_parts(geo_transform: GeoTransform, grid_bounds: GridBoundingBox2D) -> Self {
        Self::new_source(SpatialGridDefinition::new(geo_transform, grid_bounds))
    }

    #[must_use]
    pub fn as_derived(self) -> Self {
        Self {
            state: SpatialGridDescriptorState::Merged,
            ..self
        }
    }

    pub fn merge(&self, other: &SpatialGridDescriptor) -> Option<Self> {
        // TODO: merge directly to tiling origin?
        let merged_grid = self.spatial_grid.merge(&other.spatial_grid)?;
        let state = if self.spatial_grid.grid_bounds == merged_grid.grid_bounds
            && other.spatial_grid.grid_bounds == merged_grid.grid_bounds
        {
            self.state.merge(other.state)
        } else {
            SpatialGridDescriptorState::Merged
        };

        Some(Self {
            spatial_grid: merged_grid,
            state,
        })
    }

    #[must_use]
    pub fn map<F: Fn(&SpatialGridDefinition) -> SpatialGridDefinition>(&self, map_fn: F) -> Self {
        Self {
            spatial_grid: map_fn(&self.spatial_grid),
            ..*self
        }
    }

    pub fn try_map<F: Fn(&SpatialGridDefinition) -> Result<SpatialGridDefinition>>(
        &self,
        map_fn: F,
    ) -> Result<Self> {
        Ok(Self {
            spatial_grid: map_fn(&self.spatial_grid)?,
            ..*self
        })
    }

    pub fn is_compatible_grid_generic<G: GeoTransformAccess>(&self, g: &G) -> bool {
        self.spatial_grid.is_compatible_grid_generic(g)
    }

    pub fn is_compatible_grid(&self, other: &Self) -> bool {
        self.is_compatible_grid_generic(&other.spatial_grid)
    }

    pub fn tiling_grid_definition(
        &self,
        tiling_specification: TilingSpecification,
    ) -> TilingSpatialGridDefinition {
        // TODO: we could also store the tiling_origin_reference and then use that directly?
        TilingSpatialGridDefinition::new(self.spatial_grid, tiling_specification)
    }

    pub fn is_source(&self) -> bool {
        self.state == SpatialGridDescriptorState::Source
    }

    pub fn source_spatial_grid_definition(&self) -> Option<SpatialGridDefinition> {
        match self.state {
            SpatialGridDescriptorState::Source => Some(self.spatial_grid),
            SpatialGridDescriptorState::Merged => None,
        }
    }

    pub fn derived_spatial_grid_definition(&self) -> Option<SpatialGridDefinition> {
        match self.state {
            SpatialGridDescriptorState::Merged => Some(self.spatial_grid),
            SpatialGridDescriptorState::Source => None,
        }
    }

    pub fn spatial_partition(&self) -> SpatialPartition2D {
        self.spatial_grid.spatial_partition()
    }

    pub fn geo_transform(&self) -> GeoTransform {
        self.spatial_grid.geo_transform
    }

    pub fn spatial_resolution(&self) -> SpatialResolution {
        self.spatial_grid.geo_transform.spatial_resolution()
    }

    pub fn grid_shape(&self) -> GridShape2D {
        self.spatial_grid.grid_bounds().grid_shape()
    }

    #[must_use]
    pub fn with_changed_resolution(&self, new_res: SpatialResolution) -> Self {
        self.map(|x| x.with_changed_resolution(new_res))
    }

    #[must_use]
    pub fn replace_origin(&self, new_origin: Coordinate2D) -> Self {
        self.map(|x| x.replace_origin(new_origin))
    }

    #[must_use]
    pub fn with_moved_origin_to_nearest_grid_edge(
        &self,
        new_origin_referece: Coordinate2D,
    ) -> Self {
        self.map(|x| x.with_moved_origin_to_nearest_grid_edge(new_origin_referece))
    }

    pub fn reproject_clipped<P: CoordinateProjection>(
        &self,
        projector: &P,
    ) -> Result<Option<Self>> {
        let projected = self.spatial_grid.reproject_clipped(projector)?;
        match projected {
            Some(p) => Ok(Some(Self {
                spatial_grid: p,
                state: SpatialGridDescriptorState::Merged,
            })),
            None => Ok(None),
        }
    }

    pub fn generate_coord_grid_pixel_center(&self) -> Grid<GridBoundingBox2D, Coordinate2D> {
        self.spatial_grid.generate_coord_grid_pixel_center()
    }

    #[must_use]
    pub fn spatial_bounds_to_compatible_spatial_grid(
        &self,
        spatial_partition: SpatialPartition2D,
    ) -> Self {
        self.map(|x| x.spatial_bounds_to_compatible_spatial_grid(spatial_partition))
    }

    pub fn intersection_with_tiling_grid(
        &self,
        tiling_grid: &TilingSpatialGridDefinition,
    ) -> Option<Self> {
        let tiling_spatial_grid = tiling_grid.tiling_spatial_grid_definition();
        let intersection = self.spatial_grid.intersection(&tiling_spatial_grid)?;

        let descriptor = if self.spatial_grid.grid_bounds == intersection.grid_bounds {
            self.state
        } else {
            SpatialGridDescriptorState::Merged
        };

        Some(Self {
            spatial_grid: intersection,
            state: descriptor,
        })
    }

    pub fn as_parts(&self) -> (SpatialGridDescriptorState, SpatialGridDefinition) {
        let SpatialGridDescriptor {
            spatial_grid,
            state,
        } = *self;
        (state, spatial_grid)
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, ToSql, FromSql, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TimeDescriptor {
    pub bounds: Option<TimeInterval>,
    pub dimension: TimeDimension,
}

impl TimeDescriptor {
    /// Create a new `TimeDescriptor`
    pub fn new(bounds: Option<TimeInterval>, dimension: TimeDimension) -> Self {
        Self { bounds, dimension }
    }

    /// Create a new `TimeDescriptor` with an irregular time dimension
    pub fn new_irregular(bounds: Option<TimeInterval>) -> Self {
        Self {
            bounds,
            dimension: TimeDimension::Irregular,
        }
    }

    /// Create a new `TimeDescriptor` with a regular time dimension
    pub fn new_regular(bounds: Option<TimeInterval>, origin: TimeInstance, step: TimeStep) -> Self {
        Self {
            bounds,
            dimension: TimeDimension::Regular(RegularTimeDimension { origin, step }),
        }
    }

    /// Create a new `TimeDescriptor` with a regular time dimension and an epoch origin
    pub fn new_regular_with_epoch(bounds: Option<TimeInterval>, step: TimeStep) -> Self {
        Self {
            bounds,
            dimension: TimeDimension::Regular(RegularTimeDimension::new_with_epoch_origin(step)),
        }
    }

    /// Create a new `TimeDescriptor` with a regular time dimension and an origin at the start of the bounds
    pub fn new_regular_with_origin_at_start(bounds: TimeInterval, step: TimeStep) -> Self {
        Self {
            bounds: Some(bounds),
            dimension: TimeDimension::Regular(RegularTimeDimension {
                origin: bounds.start(),
                step,
            }),
        }
    }

    #[must_use]
    pub fn merge(&self, other: Self) -> Self {
        let bounds = match (self.bounds, other.bounds) {
            (Some(a), Some(b)) => Some(a.extend(&b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        let dimension = match (&self.dimension, other.dimension) {
            (TimeDimension::Irregular, _) | (_, TimeDimension::Irregular) => {
                TimeDimension::Irregular
            }
            (TimeDimension::Regular(a), TimeDimension::Regular(b)) => match a.merge(b) {
                Some(merged) => TimeDimension::Regular(merged),
                None => TimeDimension::Irregular,
            },
        };

        Self { bounds, dimension }
    }

    /// Is the time dimension regular?
    pub fn is_regular(&self) -> bool {
        self.dimension.is_regular()
    }

    fn map_time_bounds<F>(&self, f: F) -> Self
    where
        F: Fn(&Option<TimeInterval>) -> Option<TimeInterval>,
    {
        Self {
            bounds: f(&self.bounds),
            ..*self
        }
    }
}

/// A `ResultDescriptor` for raster queries
#[derive(Debug, Clone, Serialize, Deserialize, ToSql, FromSql, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RasterResultDescriptor {
    pub data_type: RasterDataType,
    pub spatial_reference: SpatialReferenceOption,
    pub time: TimeDescriptor,
    pub spatial_grid: SpatialGridDescriptor,
    pub bands: RasterBandDescriptors,
}

impl ResultDescriptor for RasterResultDescriptor {
    type DataType = RasterDataType;
    type QueryRectangleSpatialBounds = GridBoundingBox2D;
    type QueryRectangleAttributeSelection = BandSelection;

    fn data_type(&self) -> Self::DataType {
        self.data_type
    }

    fn spatial_reference(&self) -> SpatialReferenceOption {
        self.spatial_reference
    }

    fn map_data_type<F>(&self, f: F) -> Self
    where
        F: Fn(&Self::DataType) -> Self::DataType,
    {
        Self {
            data_type: f(&self.data_type),
            bands: self.bands.clone(),
            ..*self
        }
    }

    fn map_spatial_reference<F>(&self, f: F) -> Self
    where
        F: Fn(&SpatialReferenceOption) -> SpatialReferenceOption,
    {
        Self {
            spatial_reference: f(&self.spatial_reference),
            bands: self.bands.clone(),
            ..*self
        }
    }

    fn map_time<F>(&self, f: F) -> Self
    where
        F: Fn(&Option<TimeInterval>) -> Option<TimeInterval>,
    {
        Self {
            time: self.time.map_time_bounds(f), // FIXME: remove this extra function and use TimeDescriptor::map_time_bounds
            bands: self.bands.clone(),
            ..*self
        }
    }

    fn validate_query(
        &self,
        query: &QueryRectangle<
            Self::QueryRectangleSpatialBounds,
            Self::QueryRectangleAttributeSelection,
        >,
    ) -> Result<()> {
        for band in query.attributes().as_slice() {
            if *band as usize >= self.bands.len() {
                return Err(Error::BandDoesNotExist { band_idx: *band });
            }
        }

        Ok(())
    }
}

impl RasterResultDescriptor {
    /// create a new `RasterResultDescriptor`
    pub fn new(
        data_type: RasterDataType,
        spatial_reference: SpatialReferenceOption,
        time: TimeDescriptor,
        spatial_grid: SpatialGridDescriptor,
        bands: RasterBandDescriptors,
    ) -> Self {
        Self {
            data_type,
            spatial_reference,
            time,
            spatial_grid,
            bands,
        }
    }

    pub fn spatial_grid_descriptor(&self) -> &SpatialGridDescriptor {
        &self.spatial_grid
    }

    /// Returns tiling grid definition of the data.
    pub fn tiling_grid_definition(
        &self,
        tiling_specification: TilingSpecification,
    ) -> TilingSpatialGridDefinition {
        self.spatial_grid
            .tiling_grid_definition(tiling_specification)
    }

    pub fn spatial_bounds(&self) -> SpatialPartition2D {
        self.spatial_grid.spatial_partition()
    }

    pub fn with_datatype_and_num_bands(
        data_type: RasterDataType,
        num_bands: u32,
        pixel_bounds: GridBoundingBox2D,
        geo_transform: GeoTransform,
        time: TimeDescriptor,
    ) -> Self {
        Self {
            data_type,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            time,
            spatial_grid: SpatialGridDescriptor::new_source(SpatialGridDefinition::new(
                geo_transform,
                pixel_bounds,
            )),
            bands: RasterBandDescriptors::new_multiple_bands(num_bands),
        }
    }

    pub fn replace_resolution(&mut self, resolution: SpatialResolution) {
        self.spatial_grid = self.spatial_grid.with_changed_resolution(resolution);
    }
}

/// A `ResultDescriptor` for vector queries
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorResultDescriptor {
    pub data_type: VectorDataType,
    pub spatial_reference: SpatialReferenceOption,
    pub columns: HashMap<String, VectorColumnInfo>,
    pub time: Option<TimeInterval>,
    pub bbox: Option<BoundingBox2D>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorColumnInfo {
    pub data_type: FeatureDataType,
    pub measurement: Measurement,
}

impl VectorResultDescriptor {
    /// Create a new `VectorResultDescriptor` by only modifying the columns
    #[must_use]
    pub fn map_columns<F>(&self, f: F) -> Self
    where
        F: Fn(&HashMap<String, VectorColumnInfo>) -> HashMap<String, VectorColumnInfo>,
    {
        Self {
            data_type: self.data_type,
            spatial_reference: self.spatial_reference,
            columns: f(&self.columns),
            ..*self
        }
    }

    pub fn column_data_type(&self, column: &str) -> Option<FeatureDataType> {
        self.columns.get(column).map(|c| c.data_type)
    }

    pub fn column_measurement(&self, column: &str) -> Option<&Measurement> {
        self.columns.get(column).map(|c| &c.measurement)
    }
}

impl ResultDescriptor for VectorResultDescriptor {
    type DataType = VectorDataType;
    type QueryRectangleSpatialBounds = BoundingBox2D;
    type QueryRectangleAttributeSelection = ColumnSelection;

    fn data_type(&self) -> Self::DataType {
        self.data_type
    }

    fn spatial_reference(&self) -> SpatialReferenceOption {
        self.spatial_reference
    }

    fn map_data_type<F>(&self, f: F) -> Self
    where
        F: Fn(&Self::DataType) -> Self::DataType,
    {
        Self {
            data_type: f(&self.data_type),
            spatial_reference: self.spatial_reference,
            columns: self.columns.clone(),
            ..*self
        }
    }

    fn map_spatial_reference<F>(&self, f: F) -> Self
    where
        F: Fn(&SpatialReferenceOption) -> SpatialReferenceOption,
    {
        Self {
            data_type: self.data_type,
            spatial_reference: f(&self.spatial_reference),
            columns: self.columns.clone(),
            ..*self
        }
    }

    fn map_time<F>(&self, f: F) -> Self
    where
        F: Fn(&Option<TimeInterval>) -> Option<TimeInterval>,
    {
        Self {
            time: f(&self.time),
            columns: self.columns.clone(),
            ..*self
        }
    }

    fn validate_query(
        &self,
        _query: &QueryRectangle<
            Self::QueryRectangleSpatialBounds,
            Self::QueryRectangleAttributeSelection,
        >,
    ) -> Result<()> {
        Ok(())
    }
}

/// A `ResultDescriptor` for plot queries
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize, ToSql, FromSql)]
#[serde(rename_all = "camelCase")]
pub struct PlotResultDescriptor {
    pub spatial_reference: SpatialReferenceOption,
    pub time: Option<TimeInterval>,
    pub bbox: Option<BoundingBox2D>,
}

impl ResultDescriptor for PlotResultDescriptor {
    type DataType = (); // TODO: maybe distinguish between image, interactive plot, etc.
    type QueryRectangleSpatialBounds = BoundingBox2D;
    type QueryRectangleAttributeSelection = PlotSeriesSelection;

    fn data_type(&self) -> Self::DataType {}

    fn spatial_reference(&self) -> SpatialReferenceOption {
        self.spatial_reference
    }

    fn map_data_type<F>(&self, _f: F) -> Self
    where
        F: Fn(&Self::DataType) -> Self::DataType,
    {
        *self
    }

    fn map_spatial_reference<F>(&self, _f: F) -> Self
    where
        F: Fn(&SpatialReferenceOption) -> SpatialReferenceOption,
    {
        *self
    }

    fn map_time<F>(&self, f: F) -> Self
    where
        F: Fn(&Option<TimeInterval>) -> Option<TimeInterval>,
    {
        Self {
            time: f(&self.time),
            ..*self
        }
    }

    fn validate_query(
        &self,
        _query: &QueryRectangle<
            Self::QueryRectangleSpatialBounds,
            Self::QueryRectangleAttributeSelection,
        >,
    ) -> Result<()> {
        Ok(())
    }
}

// implementing `From` is possible here because we don't need any additional information, while we would need
// a measurement and a no data value to convert it into a `RasterResultDescriptor`
impl From<VectorResultDescriptor> for PlotResultDescriptor {
    fn from(descriptor: VectorResultDescriptor) -> Self {
        Self {
            spatial_reference: descriptor.spatial_reference,
            time: descriptor.time,
            bbox: descriptor.bbox,
        }
    }
}

// implementing `From` is possible here because we don't need any additional information, while we would need
// to know the `columns` to convert it into a `VectorResultDescriptor`
impl From<RasterResultDescriptor> for PlotResultDescriptor {
    fn from(descriptor: RasterResultDescriptor) -> Self {
        Self {
            spatial_reference: descriptor.spatial_reference,
            time: descriptor.time.bounds,
            // converting `SpatialPartition2D` to `BoundingBox2D` is ok here, because is makes the covered area only larger
            bbox: Some(descriptor.spatial_bounds().as_bbox()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum TypedResultDescriptor {
    Plot(PlotResultDescriptor),
    Raster(RasterResultDescriptor),
    Vector(VectorResultDescriptor),
}

impl From<PlotResultDescriptor> for TypedResultDescriptor {
    fn from(value: PlotResultDescriptor) -> Self {
        Self::Plot(value)
    }
}

impl From<RasterResultDescriptor> for TypedResultDescriptor {
    fn from(value: RasterResultDescriptor) -> Self {
        Self::Raster(value)
    }
}

impl From<VectorResultDescriptor> for TypedResultDescriptor {
    fn from(value: VectorResultDescriptor) -> Self {
        Self::Vector(value)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RasterBandDescriptors(Vec<RasterBandDescriptor>);

impl RasterBandDescriptors {
    pub fn new(bands: Vec<RasterBandDescriptor>) -> Result<Self> {
        let mut names = HashSet::new();
        for value in &bands {
            ensure!(!value.name.is_empty(), RasterBandNameMustNotBeEmpty);
            ensure!(value.name.byte_size() <= 256, RasterBandNameTooLong);
            ensure!(
                names.insert(&value.name),
                RasterBandNamesMustBeUnique {
                    duplicate_key: value.name.clone()
                }
            );
        }

        Ok(Self(bands))
    }

    /// Convenience method to crate a single band result descriptor with no specific name and a unitless measurement for single band rasters
    pub fn new_single_band() -> Self {
        Self(vec![RasterBandDescriptor {
            name: "band".into(),
            measurement: Measurement::Unitless,
        }])
    }

    /// Convenience method to crate multipe band result descriptors with no specific name and a unitless measurement
    pub fn new_multiple_bands(num_bands: u32) -> Self {
        Self(
            (0..num_bands)
                .map(RasterBandDescriptor::new_unitless_with_idx)
                .collect(),
        )
    }

    pub fn bands(&self) -> &[RasterBandDescriptor] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn count(&self) -> u32 {
        self.0.len() as u32
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &RasterBandDescriptor> {
        self.0.iter()
    }

    pub fn into_vec(self) -> Vec<RasterBandDescriptor> {
        self.0
    }

    // Merge the bands of two descriptors into a new one, fails if there are duplicate names
    pub fn merge(&self, other: &Self) -> Result<Self> {
        let mut bands = self.0.clone();
        bands.extend(other.0.clone());
        Self::new(bands)
    }

    pub fn is_single(&self) -> bool {
        self.len() == 1
    }
}

impl TryFrom<Vec<RasterBandDescriptor>> for RasterBandDescriptors {
    type Error = Error;

    fn try_from(value: Vec<RasterBandDescriptor>) -> Result<Self, Self::Error> {
        RasterBandDescriptors::new(value)
    }
}

impl From<&RasterBandDescriptors> for BandSelection {
    fn from(value: &RasterBandDescriptors) -> Self {
        Self::new_unchecked((0..value.len() as u32).collect())
    }
}

impl Index<usize> for RasterBandDescriptors {
    type Output = RasterBandDescriptor;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl<'de> Deserialize<'de> for RasterBandDescriptors {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec = Vec::deserialize(deserializer)?;
        RasterBandDescriptors::new(vec).map_err(serde::de::Error::custom)
    }
}

impl<'a> FromSql<'a> for RasterBandDescriptors {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let vec = Vec::<RasterBandDescriptor>::from_sql(ty, raw)?;
        Ok(RasterBandDescriptors(vec))
    }

    fn accepts(ty: &Type) -> bool {
        <Vec<RasterBandDescriptor> as FromSql>::accepts(ty)
    }
}

impl ToSql for RasterBandDescriptors {
    fn to_sql(
        &self,
        ty: &Type,
        w: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        ToSql::to_sql(&self.0, ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <Vec<RasterBandDescriptor> as FromSql>::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        w: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        ToSql::to_sql_checked(&self.0, ty, w)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSql, FromSql)]
pub struct RasterBandDescriptor {
    pub name: String,
    pub measurement: Measurement,
}

impl RasterBandDescriptor {
    pub fn new(name: String, measurement: Measurement) -> Self {
        Self { name, measurement }
    }

    pub fn new_unitless(name: String) -> Self {
        Self {
            name,
            measurement: Measurement::Unitless,
        }
    }

    pub fn new_unitless_with_idx(idx: u32) -> Self {
        Self {
            name: format!("band {idx}"),
            measurement: Measurement::Unitless,
        }
    }
}

mod db_types {
    use super::*;
    use crate::error::Error;
    use geoengine_datatypes::delegate_from_to_sql;
    use postgres_types::{FromSql, ToSql};

    #[derive(Debug, FromSql, ToSql)]
    #[postgres(name = "VectorColumnInfo")]
    pub struct VectorColumnInfoDbType {
        pub column: String,
        pub data_type: FeatureDataType,
        pub measurement: Measurement,
    }

    #[derive(Debug, FromSql, ToSql)]
    #[postgres(name = "VectorResultDescriptor")]
    pub struct VectorResultDescriptorDbType {
        pub data_type: VectorDataType,
        pub spatial_reference: SpatialReferenceOption,
        pub columns: Vec<VectorColumnInfoDbType>,
        pub time: Option<TimeInterval>,
        pub bbox: Option<BoundingBox2D>,
    }

    impl From<&VectorResultDescriptor> for VectorResultDescriptorDbType {
        fn from(result_descriptor: &VectorResultDescriptor) -> Self {
            Self {
                data_type: result_descriptor.data_type,
                spatial_reference: result_descriptor.spatial_reference,
                columns: result_descriptor
                    .columns
                    .iter()
                    .map(|(column, info)| VectorColumnInfoDbType {
                        column: column.clone(),
                        data_type: info.data_type,
                        measurement: info.measurement.clone(),
                    })
                    .collect(),
                time: result_descriptor.time,
                bbox: result_descriptor.bbox,
            }
        }
    }

    impl TryFrom<VectorResultDescriptorDbType> for VectorResultDescriptor {
        type Error = Error;

        fn try_from(result_descriptor: VectorResultDescriptorDbType) -> Result<Self, Self::Error> {
            Ok(Self {
                data_type: result_descriptor.data_type,
                spatial_reference: result_descriptor.spatial_reference,
                columns: result_descriptor
                    .columns
                    .into_iter()
                    .map(|info| {
                        (
                            info.column,
                            VectorColumnInfo {
                                data_type: info.data_type,
                                measurement: info.measurement,
                            },
                        )
                    })
                    .collect(),
                time: result_descriptor.time,
                bbox: result_descriptor.bbox,
            })
        }
    }

    #[derive(Debug, ToSql, FromSql)]
    #[postgres(name = "ResultDescriptor")]
    pub struct TypedResultDescriptorDbType {
        raster: Option<RasterResultDescriptor>,
        vector: Option<VectorResultDescriptor>,
        plot: Option<PlotResultDescriptor>,
    }

    impl From<&TypedResultDescriptor> for TypedResultDescriptorDbType {
        fn from(result_descriptor: &TypedResultDescriptor) -> Self {
            match result_descriptor {
                TypedResultDescriptor::Raster(raster) => Self {
                    raster: Some(raster.clone()),
                    vector: None,
                    plot: None,
                },
                TypedResultDescriptor::Vector(vector) => Self {
                    raster: None,
                    vector: Some(vector.clone()),
                    plot: None,
                },
                TypedResultDescriptor::Plot(plot) => Self {
                    raster: None,
                    vector: None,
                    plot: Some(*plot),
                },
            }
        }
    }

    impl TryFrom<TypedResultDescriptorDbType> for TypedResultDescriptor {
        type Error = Error;

        fn try_from(result_descriptor: TypedResultDescriptorDbType) -> Result<Self, Self::Error> {
            match result_descriptor {
                TypedResultDescriptorDbType {
                    raster: Some(raster),
                    vector: None,
                    plot: None,
                } => Ok(Self::Raster(raster)),
                TypedResultDescriptorDbType {
                    raster: None,
                    vector: Some(vector),
                    plot: None,
                } => Ok(Self::Vector(vector)),
                TypedResultDescriptorDbType {
                    raster: None,
                    vector: None,
                    plot: Some(plot),
                } => Ok(Self::Plot(plot)),
                _ => {
                    Err(geoengine_datatypes::error::Error::UnexpectedInvalidDbTypeConversion.into())
                }
            }
        }
    }

    #[derive(Debug, ToSql, FromSql)]
    #[postgres(name = "SpatialGridDescriptorState")]
    pub enum SpatialGridDescriptorStateDbType {
        /// The spatial grid represents the original data
        Source,
        /// The spatial grid was created by merging two non equal spatial grids
        Merged,
    }

    impl From<&SpatialGridDescriptorState> for SpatialGridDescriptorStateDbType {
        fn from(value: &SpatialGridDescriptorState) -> Self {
            match value {
                SpatialGridDescriptorState::Source => SpatialGridDescriptorStateDbType::Source,
                SpatialGridDescriptorState::Merged => SpatialGridDescriptorStateDbType::Merged,
            }
        }
    }

    impl From<SpatialGridDescriptorStateDbType> for SpatialGridDescriptorState {
        fn from(value: SpatialGridDescriptorStateDbType) -> Self {
            match value {
                SpatialGridDescriptorStateDbType::Source => SpatialGridDescriptorState::Source,
                SpatialGridDescriptorStateDbType::Merged => SpatialGridDescriptorState::Merged,
            }
        }
    }

    #[derive(Debug, ToSql, FromSql)]
    #[postgres(name = "SpatialGridDescriptor")]
    pub struct SpatialGridDescriptorDbType {
        state: SpatialGridDescriptorStateDbType,
        spatial_grid: SpatialGridDefinition,
    }

    impl From<&SpatialGridDescriptor> for SpatialGridDescriptorDbType {
        fn from(value: &SpatialGridDescriptor) -> Self {
            Self {
                spatial_grid: value.spatial_grid,
                state: SpatialGridDescriptorStateDbType::from(&value.state),
            }
        }
    }

    impl From<SpatialGridDescriptorDbType> for SpatialGridDescriptor {
        fn from(value: SpatialGridDescriptorDbType) -> Self {
            Self {
                spatial_grid: value.spatial_grid,
                state: SpatialGridDescriptorState::from(value.state),
            }
        }
    }

    delegate_from_to_sql!(SpatialGridDescriptor, SpatialGridDescriptorDbType);
    delegate_from_to_sql!(VectorResultDescriptor, VectorResultDescriptorDbType);
    delegate_from_to_sql!(TypedResultDescriptor, TypedResultDescriptorDbType);
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use serde_json::json;

    #[test]
    fn map_vector_descriptor() {
        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let columns = {
            let mut columns = HashMap::with_capacity(1);
            columns.insert(
                "foo".to_string(),
                VectorColumnInfo {
                    data_type: FeatureDataType::Float,
                    measurement: Measurement::continuous("bar".into(), None),
                },
            );
            columns
        };

        let descriptor = descriptor
            .map_data_type(|_d| VectorDataType::MultiPoint)
            .map_spatial_reference(|_sref| SpatialReference::epsg_4326().into())
            .map_columns(|_cols| columns.clone());

        assert_eq!(
            descriptor,
            VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns,
                time: None,
                bbox: None,
            }
        );
    }

    #[test]
    fn it_checks_duplicate_bands() {
        assert!(
            RasterBandDescriptors::new(vec![
                RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
                RasterBandDescriptor::new("bar".into(), Measurement::Unitless),
            ])
            .is_ok()
        );

        assert!(
            RasterBandDescriptors::new(vec![
                RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
                RasterBandDescriptor::new("bar".into(), Measurement::Unitless),
                RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
            ])
            .is_err()
        );
    }

    #[test]
    fn it_merges_band_descriptors() {
        assert_eq!(
            RasterBandDescriptors::new(vec![
                RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
                RasterBandDescriptor::new("bar".into(), Measurement::Unitless),
            ])
            .unwrap()
            .merge(
                &RasterBandDescriptors::new(vec![
                    RasterBandDescriptor::new("baz".into(), Measurement::Unitless),
                    RasterBandDescriptor::new("bla".into(), Measurement::Unitless),
                ])
                .unwrap()
            )
            .unwrap(),
            RasterBandDescriptors::new(vec![
                RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
                RasterBandDescriptor::new("bar".into(), Measurement::Unitless),
                RasterBandDescriptor::new("baz".into(), Measurement::Unitless),
                RasterBandDescriptor::new("bla".into(), Measurement::Unitless),
            ])
            .unwrap()
        );
    }

    #[test]
    fn it_checks_duplicates_while_deserializing_band_descriptors() {
        assert_eq!(
            serde_json::from_value::<RasterBandDescriptors>(json!([{
                "name": "foo",
                "measurement": {
                    "type": "unitless"
                }
            },{
                "name": "bar",
                "measurement": {
                    "type": "unitless"
                }
            }]))
            .unwrap(),
            RasterBandDescriptors::new(vec![
                RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
                RasterBandDescriptor::new("bar".into(), Measurement::Unitless),
            ])
            .unwrap()
        );

        assert!(
            serde_json::from_value::<RasterBandDescriptors>(json!([{
                "name": "foo",
                "measurement": {
                    "type": "unitless"
                }
            },{
                "name": "foo",
                "measurement": {
                    "type": "unitless"
                }
            }]))
            .is_err()
        );
    }
}
