use super::query::{QueryContext, QueryRectangle};
use super::{PlotQueryRectangle, RasterQueryRectangle, VectorQueryRectangle};
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use geoengine_datatypes::collections::{
    DataCollection, MultiLineStringCollection, MultiPolygonCollection,
};
use geoengine_datatypes::plots::{PlotData, PlotOutputFormat};
use geoengine_datatypes::primitives::{AxisAlignedRectangle, BoundingBox2D, SpatialPartition2D};
use geoengine_datatypes::raster::Pixel;
use geoengine_datatypes::{collections::MultiPointCollection, raster::RasterTile2D};

/// An instantiation of an operator that produces a stream of results for a query
#[async_trait]
pub trait QueryProcessor: Send + Sync {
    type Output;
    type SpatialBounds: AxisAlignedRectangle + Send + Sync;
    async fn query<'a>(
        &'a self,
        query: QueryRectangle<Self::SpatialBounds>,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>>;
}

/// An instantiation of a raster operator that produces a stream of raster results for a query
#[async_trait]
pub trait RasterQueryProcessor: Sync + Send {
    type RasterType: Pixel;

    async fn raster_query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<Self::RasterType>>>>;

    fn boxed(self) -> Box<dyn RasterQueryProcessor<RasterType = Self::RasterType>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

#[async_trait]
impl<S, T> RasterQueryProcessor for S
where
    S: QueryProcessor<Output = RasterTile2D<T>> + Sync + Send,
    T: Pixel,
{
    type RasterType = T;
    async fn raster_query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<Self::RasterType>>>> {
        self.raster_query(query, ctx).await
    }
}

/// An instantiation of a vector operator that produces a stream of vector results for a query
#[async_trait]
pub trait VectorQueryProcessor: Sync + Send {
    type VectorType;
    async fn vector_query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::VectorType>>>;

    fn boxed(self) -> Box<dyn VectorQueryProcessor<VectorType = Self::VectorType>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

#[async_trait]
impl<S, VD> VectorQueryProcessor for S
where
    S: QueryProcessor<Output = VD> + Sync + Send,
{
    type VectorType = VD;

    async fn vector_query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::VectorType>>> {
        self.vector_query(query, ctx).await
    }
}

/// An instantiation of a plot operator that produces a stream of vector results for a query
#[async_trait]
pub trait PlotQueryProcessor: Sync + Send {
    type OutputFormat;

    fn plot_type(&self) -> &'static str;

    async fn plot_query<'a>(
        &'a self,
        query: PlotQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<Self::OutputFormat>;

    fn boxed(self) -> Box<dyn PlotQueryProcessor<OutputFormat = Self::OutputFormat>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

#[async_trait]
impl<T, S> QueryProcessor for Box<dyn QueryProcessor<Output = T, SpatialBounds = S>>
where
    S: AxisAlignedRectangle + Send + Sync,
{
    type Output = T;
    type SpatialBounds = S;

    async fn query<'a>(
        &'a self,
        query: QueryRectangle<S>,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        self.as_ref().query(query, ctx).await
    }
}

#[async_trait]
impl<T> QueryProcessor for Box<dyn RasterQueryProcessor<RasterType = T>>
where
    T: Pixel,
{
    type Output = RasterTile2D<T>;
    type SpatialBounds = SpatialPartition2D;

    async fn query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        self.as_ref().raster_query(query, ctx).await
    }
}

#[async_trait]
impl<V> QueryProcessor for Box<dyn VectorQueryProcessor<VectorType = V>>
where
    V: 'static,
{
    type Output = V;
    type SpatialBounds = BoundingBox2D;

    async fn query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        self.as_ref().vector_query(query, ctx).await
    }
}

/// An enum to differentiate between outputs of raster processors
pub enum TypedRasterQueryProcessor {
    U8(Box<dyn RasterQueryProcessor<RasterType = u8>>),
    U16(Box<dyn RasterQueryProcessor<RasterType = u16>>),
    U32(Box<dyn RasterQueryProcessor<RasterType = u32>>),
    U64(Box<dyn RasterQueryProcessor<RasterType = u64>>),
    I8(Box<dyn RasterQueryProcessor<RasterType = i8>>),
    I16(Box<dyn RasterQueryProcessor<RasterType = i16>>),
    I32(Box<dyn RasterQueryProcessor<RasterType = i32>>),
    I64(Box<dyn RasterQueryProcessor<RasterType = i64>>),
    F32(Box<dyn RasterQueryProcessor<RasterType = f32>>),
    F64(Box<dyn RasterQueryProcessor<RasterType = f64>>),
}

impl TypedRasterQueryProcessor {
    pub fn get_u8(self) -> Option<Box<dyn RasterQueryProcessor<RasterType = u8>>> {
        match self {
            Self::U8(r) => Some(r),
            _ => None,
        }
    }
    pub fn get_u16(self) -> Option<Box<dyn RasterQueryProcessor<RasterType = u16>>> {
        match self {
            Self::U16(r) => Some(r),
            _ => None,
        }
    }
    pub fn get_u32(self) -> Option<Box<dyn RasterQueryProcessor<RasterType = u32>>> {
        match self {
            Self::U32(r) => Some(r),
            _ => None,
        }
    }
    pub fn get_u64(self) -> Option<Box<dyn RasterQueryProcessor<RasterType = u64>>> {
        match self {
            Self::U64(r) => Some(r),
            _ => None,
        }
    }
    pub fn get_i8(self) -> Option<Box<dyn RasterQueryProcessor<RasterType = i8>>> {
        match self {
            Self::I8(r) => Some(r),
            _ => None,
        }
    }
    pub fn get_i16(self) -> Option<Box<dyn RasterQueryProcessor<RasterType = i16>>> {
        match self {
            Self::I16(r) => Some(r),
            _ => None,
        }
    }
    pub fn get_i32(self) -> Option<Box<dyn RasterQueryProcessor<RasterType = i32>>> {
        match self {
            Self::I32(r) => Some(r),
            _ => None,
        }
    }
    pub fn get_i64(self) -> Option<Box<dyn RasterQueryProcessor<RasterType = i64>>> {
        match self {
            Self::I64(r) => Some(r),
            _ => None,
        }
    }
    pub fn get_f32(self) -> Option<Box<dyn RasterQueryProcessor<RasterType = f32>>> {
        match self {
            Self::F32(r) => Some(r),
            _ => None,
        }
    }
    pub fn get_f64(self) -> Option<Box<dyn RasterQueryProcessor<RasterType = f64>>> {
        match self {
            Self::F64(r) => Some(r),
            _ => None,
        }
    }
}

impl From<Box<dyn RasterQueryProcessor<RasterType = u8>>> for TypedRasterQueryProcessor {
    fn from(value: Box<dyn RasterQueryProcessor<RasterType = u8>>) -> Self {
        TypedRasterQueryProcessor::U8(value)
    }
}

impl From<Box<dyn RasterQueryProcessor<RasterType = i8>>> for TypedRasterQueryProcessor {
    fn from(value: Box<dyn RasterQueryProcessor<RasterType = i8>>) -> Self {
        TypedRasterQueryProcessor::I8(value)
    }
}

impl From<Box<dyn RasterQueryProcessor<RasterType = u16>>> for TypedRasterQueryProcessor {
    fn from(value: Box<dyn RasterQueryProcessor<RasterType = u16>>) -> Self {
        TypedRasterQueryProcessor::U16(value)
    }
}

impl From<Box<dyn RasterQueryProcessor<RasterType = i16>>> for TypedRasterQueryProcessor {
    fn from(value: Box<dyn RasterQueryProcessor<RasterType = i16>>) -> Self {
        TypedRasterQueryProcessor::I16(value)
    }
}

impl From<Box<dyn RasterQueryProcessor<RasterType = u32>>> for TypedRasterQueryProcessor {
    fn from(value: Box<dyn RasterQueryProcessor<RasterType = u32>>) -> Self {
        TypedRasterQueryProcessor::U32(value)
    }
}

impl From<Box<dyn RasterQueryProcessor<RasterType = i32>>> for TypedRasterQueryProcessor {
    fn from(value: Box<dyn RasterQueryProcessor<RasterType = i32>>) -> Self {
        TypedRasterQueryProcessor::I32(value)
    }
}

impl From<Box<dyn RasterQueryProcessor<RasterType = u64>>> for TypedRasterQueryProcessor {
    fn from(value: Box<dyn RasterQueryProcessor<RasterType = u64>>) -> Self {
        TypedRasterQueryProcessor::U64(value)
    }
}
impl From<Box<dyn RasterQueryProcessor<RasterType = i64>>> for TypedRasterQueryProcessor {
    fn from(value: Box<dyn RasterQueryProcessor<RasterType = i64>>) -> Self {
        TypedRasterQueryProcessor::I64(value)
    }
}
impl From<Box<dyn RasterQueryProcessor<RasterType = f32>>> for TypedRasterQueryProcessor {
    fn from(value: Box<dyn RasterQueryProcessor<RasterType = f32>>) -> Self {
        TypedRasterQueryProcessor::F32(value)
    }
}
impl From<Box<dyn RasterQueryProcessor<RasterType = f64>>> for TypedRasterQueryProcessor {
    fn from(value: Box<dyn RasterQueryProcessor<RasterType = f64>>) -> Self {
        TypedRasterQueryProcessor::F64(value)
    }
}

/// An enum that contains all possible query processor variants
#[allow(clippy::pub_enum_variant_names)]
pub enum TypedVectorQueryProcessor {
    Data(Box<dyn VectorQueryProcessor<VectorType = DataCollection>>),
    MultiPoint(Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>),
    MultiLineString(Box<dyn VectorQueryProcessor<VectorType = MultiLineStringCollection>>),
    MultiPolygon(Box<dyn VectorQueryProcessor<VectorType = MultiPolygonCollection>>),
}

impl TypedVectorQueryProcessor {
    pub fn data(self) -> Option<Box<dyn VectorQueryProcessor<VectorType = DataCollection>>> {
        if let TypedVectorQueryProcessor::Data(p) = self {
            Some(p)
        } else {
            None
        }
    }

    pub fn multi_point(
        self,
    ) -> Option<Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>> {
        if let TypedVectorQueryProcessor::MultiPoint(p) = self {
            Some(p)
        } else {
            None
        }
    }

    pub fn multi_line_string(
        self,
    ) -> Option<Box<dyn VectorQueryProcessor<VectorType = MultiLineStringCollection>>> {
        if let TypedVectorQueryProcessor::MultiLineString(p) = self {
            Some(p)
        } else {
            None
        }
    }

    pub fn multi_polygon(
        self,
    ) -> Option<Box<dyn VectorQueryProcessor<VectorType = MultiPolygonCollection>>> {
        if let TypedVectorQueryProcessor::MultiPolygon(p) = self {
            Some(p)
        } else {
            None
        }
    }
}

/// An enum that contains all possible query processor variants
pub enum TypedPlotQueryProcessor {
    JsonPlain(Box<dyn PlotQueryProcessor<OutputFormat = serde_json::Value>>),
    JsonVega(Box<dyn PlotQueryProcessor<OutputFormat = PlotData>>),
    ImagePng(Box<dyn PlotQueryProcessor<OutputFormat = Vec<u8>>>),
}

impl From<&TypedPlotQueryProcessor> for PlotOutputFormat {
    fn from(typed_processor: &TypedPlotQueryProcessor) -> Self {
        match typed_processor {
            TypedPlotQueryProcessor::JsonPlain(_) => PlotOutputFormat::JsonPlain,
            TypedPlotQueryProcessor::JsonVega(_) => PlotOutputFormat::JsonVega,
            TypedPlotQueryProcessor::ImagePng(_) => PlotOutputFormat::ImagePng,
        }
    }
}

impl TypedPlotQueryProcessor {
    pub fn plot_type(&self) -> &'static str {
        match self {
            TypedPlotQueryProcessor::JsonPlain(p) => p.plot_type(),
            TypedPlotQueryProcessor::JsonVega(p) => p.plot_type(),
            TypedPlotQueryProcessor::ImagePng(p) => p.plot_type(),
        }
    }

    pub fn json_plain(
        self,
    ) -> Option<Box<dyn PlotQueryProcessor<OutputFormat = serde_json::Value>>> {
        if let TypedPlotQueryProcessor::JsonPlain(p) = self {
            Some(p)
        } else {
            None
        }
    }

    pub fn json_vega(self) -> Option<Box<dyn PlotQueryProcessor<OutputFormat = PlotData>>> {
        if let TypedPlotQueryProcessor::JsonVega(p) = self {
            Some(p)
        } else {
            None
        }
    }

    pub fn image_png(self) -> Option<Box<dyn PlotQueryProcessor<OutputFormat = Vec<u8>>>> {
        if let TypedPlotQueryProcessor::ImagePng(p) = self {
            Some(p)
        } else {
            None
        }
    }
}

/// Maps a `TypedVectorQueryProcessor` to another `TypedVectorQueryProcessor` by calling a function on its variant.
/// Call via `map_typed_vector_query_processor!(input, processor => function)`.
#[macro_export]
macro_rules! map_typed_vector_query_processor {
    ($input:expr, $processor:ident => $function_call:expr) => {
        map_typed_vector_query_processor!(
            @variants $input, $processor => $function_call,
            Data, MultiPoint, MultiLineString, MultiPolygon
        )
    };

    (@variants $input:expr, $processor:ident => $function_call:expr, $($variant:tt),+) => {
        match $input {
            $(
                $crate::engine::TypedVectorQueryProcessor::$variant($processor) => {
                    $crate::engine::TypedVectorQueryProcessor::$variant($function_call)
                }
            )+
        }
    };
}
