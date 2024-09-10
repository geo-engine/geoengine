use crate::error::{
    Error, RasterBandNameMustNotBeEmpty, RasterBandNameTooLong, RasterBandNamesMustBeUnique,
};
use crate::util::Result;
use geoengine_datatypes::operations::reproject::{CoordinateProjection, ReprojectClipped};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BandSelection, BoundingBox2D, ColumnSelection, Coordinate2D,
    FeatureDataType, Measurement, PlotSeriesSelection, QueryAttributeSelection, QueryRectangle,
    SpatialGridQueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval,
    VectorSpatialQueryRectangle,
};
use geoengine_datatypes::raster::{
    GeoTransform, GeoTransformAccess, Grid, GridBoundingBox2D, SpatialGridDefinition,
    TilingSpatialGridDefinition, TilingSpecification,
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

/// A `ResultDescriptor` for raster queries
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum SpatialGridDescriptor {
    Source(SpatialGridDefinition),
    Derived(SpatialGridDefinition),
}

impl SpatialGridDescriptor {
    pub fn new_source(spatial_grid_def: SpatialGridDefinition) -> Self {
        Self::Source(spatial_grid_def)
    }

    pub fn source_from_parts(geo_transform: GeoTransform, grid_bounds: GridBoundingBox2D) -> Self {
        Self::new_source(SpatialGridDefinition::new(geo_transform, grid_bounds))
    }

    pub fn as_derived(self) -> Self {
        match self {
            Self::Source(s) => Self::Derived(s),
            Self::Derived(d) => Self::Derived(d),
        }
    }

    pub fn merge(&self, other: &SpatialGridDescriptor) -> Option<Self> {
        // TODO: merge directly to tiling origin?
        match (self, other) {
            (SpatialGridDescriptor::Source(s), SpatialGridDescriptor::Source(o)) => {
                let m = s.merge(o)?;
                if m.grid_bounds == s.grid_bounds && m.grid_bounds == o.grid_bounds {
                    Some(SpatialGridDescriptor::Source(m))
                } else {
                    Some(SpatialGridDescriptor::Derived(m))
                }
            }
            (SpatialGridDescriptor::Source(s), SpatialGridDescriptor::Derived(o)) => {
                Some(SpatialGridDescriptor::Derived(s.merge(o)?))
            }
            (SpatialGridDescriptor::Derived(s), SpatialGridDescriptor::Source(o)) => {
                Some(SpatialGridDescriptor::Derived(s.merge(o)?))
            }
            (SpatialGridDescriptor::Derived(s), SpatialGridDescriptor::Derived(o)) => {
                Some(SpatialGridDescriptor::Derived(s.merge(o)?))
            }
        }
    }

    pub fn map<F: Fn(&SpatialGridDefinition) -> SpatialGridDefinition>(&self, map_fn: F) -> Self {
        match self {
            SpatialGridDescriptor::Source(s) => SpatialGridDescriptor::Source(map_fn(s)),
            SpatialGridDescriptor::Derived(m) => SpatialGridDescriptor::Derived(map_fn(m)),
        }
    }

    pub fn try_map<F: Fn(&SpatialGridDefinition) -> Result<SpatialGridDefinition>>(
        &self,
        map_fn: F,
    ) -> Result<Self> {
        match self {
            SpatialGridDescriptor::Source(s) => Ok(SpatialGridDescriptor::Source(map_fn(s)?)),
            SpatialGridDescriptor::Derived(m) => Ok(SpatialGridDescriptor::Derived(map_fn(m)?)),
        }
    }

    pub fn is_compatible_grid_generic<G: GeoTransformAccess>(&self, g: &G) -> bool {
        match self {
            SpatialGridDescriptor::Source(s) => s.is_compatible_grid_generic(g),
            SpatialGridDescriptor::Derived(m) => m.is_compatible_grid_generic(g),
        }
    }

    pub fn is_compatible_grid(&self, other: &Self) -> bool {
        let b = match other {
            SpatialGridDescriptor::Source(s) => s,
            SpatialGridDescriptor::Derived(m) => m,
        };

        self.is_compatible_grid_generic(b)
    }

    pub fn tiling_grid_definition(
        &self,
        tiling_specification: TilingSpecification,
    ) -> TilingSpatialGridDefinition {
        // TODO: we could also store the tiling_origin_reference and then use that directly?
        let grid_def = match self {
            SpatialGridDescriptor::Source(s) => s,
            SpatialGridDescriptor::Derived(m) => m,
        };
        TilingSpatialGridDefinition::new(*grid_def, tiling_specification)
    }

    pub fn is_source(&self) -> bool {
        match self {
            SpatialGridDescriptor::Source(_) => true,
            SpatialGridDescriptor::Derived(_) => false,
        }
    }

    pub fn source_spatial_grid_definition(&self) -> Option<SpatialGridDefinition> {
        match self {
            SpatialGridDescriptor::Source(s) => Some(*s),
            SpatialGridDescriptor::Derived(_) => None,
        }
    }

    pub fn derived_spatial_grid_definition(&self) -> Option<SpatialGridDefinition> {
        match self {
            SpatialGridDescriptor::Source(_) => None,
            SpatialGridDescriptor::Derived(m) => Some(*m),
        }
    }

    pub fn spatial_partition(&self) -> SpatialPartition2D {
        let grid_def = match self {
            SpatialGridDescriptor::Source(s) => s,
            SpatialGridDescriptor::Derived(m) => m,
        };
        grid_def.spatial_partition()
    }

    pub fn spatial_resolution(&self) -> SpatialResolution {
        match self {
            SpatialGridDescriptor::Source(s) => s.geo_transform().spatial_resolution(),
            SpatialGridDescriptor::Derived(m) => m.geo_transform().spatial_resolution(),
        }
    }

    pub fn with_changed_resolution(&self, new_res: SpatialResolution) -> Self {
        self.map(|x| x.with_changed_resolution(new_res))
    }

    pub fn replace_origin(&self, new_origin: Coordinate2D) -> Self {
        self.map(|x| x.replace_origin(new_origin))
    }

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
        match self {
            SpatialGridDescriptor::Source(s) => Ok(s
                .reproject_clipped(projector)?
                .map(SpatialGridDescriptor::Derived)),
            SpatialGridDescriptor::Derived(m) => Ok(m
                .reproject_clipped(projector)?
                .map(SpatialGridDescriptor::Derived)),
        }
    }

    pub fn generate_coord_grid_pixel_center(&self) -> Grid<GridBoundingBox2D, Coordinate2D> {
        match self {
            SpatialGridDescriptor::Source(s) => s.generate_coord_grid_pixel_center(),
            SpatialGridDescriptor::Derived(m) => m.generate_coord_grid_pixel_center(),
        }
    }

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
        let intersect = match self {
            SpatialGridDescriptor::Source(s) => s.intersection(&tiling_spatial_grid),
            SpatialGridDescriptor::Derived(d) => d.intersection(&tiling_spatial_grid),
        };
        intersect.map(SpatialGridDescriptor::Derived)
    }
}

/// A `ResultDescriptor` for raster queries
#[derive(Debug, Clone, Serialize, Deserialize, ToSql, FromSql, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RasterResultDescriptor {
    pub data_type: RasterDataType,
    pub spatial_reference: SpatialReferenceOption,
    pub time: Option<TimeInterval>,
    pub spatial_grid: SpatialGridDescriptor,
    pub bands: RasterBandDescriptors,
}

impl ResultDescriptor for RasterResultDescriptor {
    type DataType = RasterDataType;
    type QueryRectangleSpatialBounds = SpatialGridQueryRectangle;
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
            time: f(&self.time),
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
        for band in query.attributes.as_slice() {
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
        time: Option<TimeInterval>,
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
        num_bands: usize,
        pixel_bounds: GridBoundingBox2D,
        geo_transform: GeoTransform,
    ) -> Self {
        Self {
            data_type,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            time: None,
            spatial_grid: SpatialGridDescriptor::new_source(SpatialGridDefinition::new(
                geo_transform,
                pixel_bounds,
            )),
            bands: RasterBandDescriptors::new(
                (0..num_bands)
                    .map(|n| RasterBandDescriptor::new(format!("{n}"), Measurement::Unitless))
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        }
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
    type QueryRectangleSpatialBounds = VectorSpatialQueryRectangle;
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
            time: descriptor.time,
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
    pub enum SpatialGridDescriptorState {
        /// The spatial grid represents the original data
        Source,
        /// The spatial grid was created by merging two non equal spatial grids
        Merged,
    }

    #[derive(Debug, ToSql, FromSql)]
    #[postgres(name = "SpatialGridDescriptor")]
    pub struct SpatialGridDescriptorDbType {
        state: SpatialGridDescriptorState,
        spatial_grid: SpatialGridDefinition,
    }

    impl From<&SpatialGridDescriptor> for SpatialGridDescriptorDbType {
        fn from(value: &SpatialGridDescriptor) -> Self {
            match value {
                SpatialGridDescriptor::Source(s) => Self {
                    spatial_grid: *s,
                    state: SpatialGridDescriptorState::Source,
                },
                SpatialGridDescriptor::Derived(m) => Self {
                    spatial_grid: *m,
                    state: SpatialGridDescriptorState::Merged,
                },
            }
        }
    }

    impl From<SpatialGridDescriptorDbType> for SpatialGridDescriptor {
        fn from(value: SpatialGridDescriptorDbType) -> SpatialGridDescriptor {
            match value.state {
                SpatialGridDescriptorState::Source => {
                    SpatialGridDescriptor::Source(value.spatial_grid)
                }
                SpatialGridDescriptorState::Merged => {
                    SpatialGridDescriptor::Derived(value.spatial_grid)
                }
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

    /* FIXME: bring back?
        #[test]
        fn raster_tiling_origin() {
            let descriptor = RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                time: None,
                geo_transform_x: GeoTransform::new(Coordinate2D::new(-10., 10.), 0.3, -0.3),
                pixel_bounds_x: GridShape2D::new([36, 30]).bounding_box(),
                bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                    "foo".into(),
                    Measurement::Unitless,
                )])
                .unwrap(),
            };

            let to = descriptor.tiling_origin();

            assert_approx_eq!(f64, to.x, -0.09999, epsilon = 0.00001); // we are only interested in a number thats smaller then the pixel size
            assert_approx_eq!(f64, to.y, 0.09999, epsilon = 0.00001);
        }

        #[test]
        fn raster_tiling_equals() {
            let descriptor = RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                time: None,
                geo_transform_x: GeoTransform::new(Coordinate2D::new(-15., 15.), 0.5, -0.5),
                pixel_bounds_x: GridShape2D::new([50, 50]).bounding_box(),
                bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                    "foo".into(),
                    Measurement::Unitless,
                )])
                .unwrap(),
            };

            let descriptor2 = RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                time: None,
                geo_transform_x: GeoTransform::new(Coordinate2D::new(-10., 10.), 0.5, -0.5),
                pixel_bounds_x: GridShape2D::new([9, 11]).bounding_box(),
                bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                    "foo".into(),
                    Measurement::Unitless,
                )])
                .unwrap(),
            };

            assert!(descriptor.spatial_tiling_equals(&descriptor2));
        }
    */
    #[test]
    fn it_checks_duplicate_bands() {
        assert!(RasterBandDescriptors::new(vec![
            RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
            RasterBandDescriptor::new("bar".into(), Measurement::Unitless),
        ])
        .is_ok());

        assert!(RasterBandDescriptors::new(vec![
            RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
            RasterBandDescriptor::new("bar".into(), Measurement::Unitless),
            RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
        ])
        .is_err());
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

        assert!(serde_json::from_value::<RasterBandDescriptors>(json!([{
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
        .is_err());
    }
}
