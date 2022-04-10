use snafu::{prelude::*, AsErrorSource};
use std::convert::Infallible;

use crate::{
    collections::FeatureCollectionError,
    primitives::{BoundingBox2D, TimeInstance},
    spatial_reference::SpatialReference,
};
use crate::{
    primitives::{Coordinate2D, PrimitivesError, TimeInterval},
    raster::RasterDataType,
};

pub trait ErrorSource: std::error::Error + Send + Sync + 'static + AsErrorSource {
    // fn boxed(self) -> Box<Self>;
}

// pub trait SizedErrorSource: ErrorSource + Sized {}

impl<T> ErrorSource for T where T: std::error::Error + Send + Sync + 'static {}

// impl<T> SizedErrorSource for T where T: ErrorSource + Sized {}

// impl ErrorSource for dyn std::error::Error + Send + Sync + 'static {}

// impl<T> ErrorSource for Box<T> where T: std::error::Error + Send + Sync + 'static {}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum Error {
    #[snafu(display("Arrow internal error: {:?}", source))]
    ArrowInternal {
        source: arrow::error::ArrowError,
    },

    #[snafu(display("ProjInternal error: {:?}", source))]
    ProjInternal {
        source: proj::ProjError,
    },
    #[snafu(display("No CoordinateProjector available for: {:?} --> {:?}", from, to))]
    NoCoordinateProjector {
        from: SpatialReference,
        to: SpatialReference,
    },
    #[snafu(display(
        "Could not resolve a Proj string for this SpatialReference: {}",
        spatial_ref
    ))]
    ProjStringUnresolvable {
        spatial_ref: SpatialReference,
    },

    #[snafu(display("Field is reserved or already in use: {}", name))]
    ColumnNameConflict {
        name: String,
    },

    #[snafu(display("Start `{}` must be before end `{}`", start, end))]
    TimeIntervalEndBeforeStart {
        start: i64,
        end: i64,
    },

    #[snafu(display(
        "{} cannot be unioned with {} since the intervals are neither intersecting nor contiguous",
        i1,
        i2
    ))]
    TimeIntervalUnmatchedIntervals {
        i1: TimeInterval,
        i2: TimeInterval,
    },

    #[snafu(display(
        "{} must be larger than {} and {} must be smaller than {}",
        start.inner(),
        min.inner(),
        end.inner(),
        max.inner()
    ))]
    TimeIntervalOutOfBounds {
        start: TimeInstance,
        end: TimeInstance,
        min: TimeInstance,
        max: TimeInstance,
    },

    #[snafu(display(
        "{:?} is not a valid index in the bounds {:?}, {:?} ",
        index,
        min_index,
        max_index,
    ))]
    GridIndexOutOfBounds {
        index: Vec<isize>,
        min_index: Vec<isize>,
        max_index: Vec<isize>,
    },

    #[snafu(display("Invalid GridIndex ({:?}), reason: \"{}\".", grid_index, description))]
    InvalidGridIndex {
        grid_index: Vec<usize>,
        description: &'static str,
    },

    #[snafu(display("Invalid raster operation. Reason: \"{}\".", description))]
    InvalidRasterOperation {
        description: &'static str,
    },

    #[snafu(display(
        "Dimension capacity  ≠ data capacity ({} ≠ {})",
        dimension_cap,
        data_cap
    ))]
    DimensionCapacityDoesNotMatchDataCapacity {
        dimension_cap: usize,
        data_cap: usize,
    },

    #[snafu(display(
        "The conditions ll.x <= ur.x && ll.y <= ur.y are not met by ll:{} ur:{}",
        lower_left_coordinate,
        upper_right_coordinate
    ))]
    InvalidBoundingBox {
        lower_left_coordinate: Coordinate2D,
        upper_right_coordinate: Coordinate2D,
    },

    #[snafu(display(
        "Mask length ≠ collection length ({} ≠ {})",
        mask_length,
        collection_length
    ))]
    MaskLengthDoesNotMatchCollectionLength {
        mask_length: usize,
        collection_length: usize,
    },

    FeatureCollection {
        source: FeatureCollectionError,
    },

    #[snafu(display("FeatureData exception: {}", details))]
    FeatureData {
        details: String,
    },

    #[snafu(display("FeatureCollectionBuilder exception: {}", details))]
    FeatureCollectionBuilder {
        details: String,
    },

    #[snafu(display("Plot exception: {}", details))]
    Plot {
        details: String,
    },

    #[snafu(display("Colorizer exception: {}", details))]
    Colorizer {
        details: String,
    },

    Primitives {
        source: PrimitivesError,
    },

    #[snafu(display("Blit exception: {}", details))]
    Blit {
        details: String,
    },
    #[snafu(display("NonMatchingRasterTypes: a=\"{:?}\", b=\"{:?}\"", a, b))]
    NonMatchingRasterTypes {
        a: RasterDataType,
        b: RasterDataType,
    },
    #[snafu(display(
        "Invalid Grid bounds: Each eleemnt in {:?} must be <= the corresponding element in {:?}.",
        min,
        max
    ))]
    InvalidGridBounds {
        min: Vec<isize>,
        max: Vec<isize>,
    },
    #[snafu(display("InvalidSpatialReferenceString: {}", spatial_reference_string))]
    InvalidSpatialReferenceString {
        spatial_reference_string: String,
    },

    #[snafu(display("ParseU32: {}", source))]
    ParseU32 {
        source: <u32 as std::str::FromStr>::Err,
    },
    InvalidTypedGridConversion,
    InvalidTypedValueConversion,

    InvalidUuid,

    #[snafu(display("NoDateTimeValid: {:?}", time_instance))]
    NoDateTimeValid {
        time_instance: TimeInstance,
    },

    DateTimeOutOfBounds {
        year: i32,
        month: u32,
        day: u32,
    },

    #[snafu(display(
        "The supplied spatial bounds are empty: {} {}",
        lower_left_coordinate,
        upper_right_coordinate
    ))]
    EmptySpatialBounds {
        lower_left_coordinate: Coordinate2D,
        upper_right_coordinate: Coordinate2D,
    },

    #[snafu(display("GdalError: {}", source))]
    Gdal {
        source: gdal::errors::GdalError,
    },

    NoMatchingVectorDataTypeForOgrGeometryType,

    NoMatchingFeatureDataTypeForOgrFieldType,

    InvalidProjDefinition {
        proj_definition: String,
    },
    NoAreaOfUseDefined {
        proj_string: String,
    },
    SpatialBoundsDoNotIntersect {
        bounds_a: BoundingBox2D,
        bounds_b: BoundingBox2D,
    },

    OutputBboxEmpty {
        bbox: BoundingBox2D,
    },

    WrongMetadataType,

    #[snafu(display(
        "The conditions ul.x < lr.x && ul.y < lr.y are not met by ul:{} lr:{}",
        upper_left_coordinate,
        lower_right_coordinate
    ))]
    InvalidSpatialPartition {
        upper_left_coordinate: Coordinate2D,
        lower_right_coordinate: Coordinate2D,
    },

    #[snafu(display("MissingRasterProperty Error: {}", property))]
    MissingRasterProperty {
        property: String,
    },
}

impl From<arrow::error::ArrowError> for Error {
    fn from(source: arrow::error::ArrowError) -> Self {
        Error::ArrowInternal { source }
    }
}

impl From<proj::ProjError> for Error {
    fn from(source: proj::ProjError) -> Self {
        Error::ProjInternal { source }
    }
}

impl From<Infallible> for Error {
    fn from(_: Infallible) -> Self {
        unreachable!("This function cannot be called on a non-failing type")
    }
}

impl From<gdal::errors::GdalError> for Error {
    fn from(gdal_error: gdal::errors::GdalError) -> Self {
        Self::Gdal { source: gdal_error }
    }
}
