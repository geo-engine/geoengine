use super::query::{QueryContext, QueryRectangle};
use crate::util::Result;
use futures::stream::BoxStream;
use geoengine_datatypes::collections::{
    DataCollection, MultiLineStringCollection, MultiPolygonCollection,
};
use geoengine_datatypes::raster::Pixel;
use geoengine_datatypes::{collections::MultiPointCollection, raster::RasterTile2D};

/// An instantiation of an operator that produces a stream of results for a query
pub trait QueryProcessor {
    type Output;
    fn query(&self, query: QueryRectangle, ctx: QueryContext) -> BoxStream<Result<Self::Output>>;
}

/// An instantiation of a raster operator that produces a stream of raster results for a query
pub trait RasterQueryProcessor: Sync + Send {
    type RasterType: Pixel;

    fn raster_query(
        &self,
        query: QueryRectangle,
        ctx: QueryContext,
    ) -> BoxStream<Result<RasterTile2D<Self::RasterType>>>;

    fn boxed(self) -> Box<dyn RasterQueryProcessor<RasterType = Self::RasterType>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

impl<S, T> RasterQueryProcessor for S
where
    S: QueryProcessor<Output = RasterTile2D<T>> + Sync + Send,
    T: Pixel,
{
    type RasterType = T;
    fn raster_query(
        &self,
        query: QueryRectangle,
        ctx: QueryContext,
    ) -> BoxStream<Result<RasterTile2D<Self::RasterType>>> {
        self.query(query, ctx)
    }
}

/// An instantiation of a vector operator that produces a stream of vector results for a query
pub trait VectorQueryProcessor: Sync + Send {
    type VectorType;
    fn vector_query(
        &self,
        query: QueryRectangle,
        ctx: QueryContext,
    ) -> BoxStream<Result<Self::VectorType>>;

    fn boxed(self) -> Box<dyn VectorQueryProcessor<VectorType = Self::VectorType>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

impl<S, VD> VectorQueryProcessor for S
where
    S: QueryProcessor<Output = VD> + Sync + Send,
{
    type VectorType = VD;

    fn vector_query(
        &self,
        query: QueryRectangle,
        ctx: QueryContext,
    ) -> BoxStream<Result<Self::VectorType>> {
        self.query(query, ctx)
    }
}

impl<T> QueryProcessor for Box<dyn QueryProcessor<Output = T>> {
    type Output = T;
    fn query(&self, query: QueryRectangle, ctx: QueryContext) -> BoxStream<Result<Self::Output>> {
        self.as_ref().query(query, ctx)
    }
}

impl<T> QueryProcessor for Box<dyn RasterQueryProcessor<RasterType = T>>
where
    T: Pixel,
{
    type Output = RasterTile2D<T>;

    fn query(&self, query: QueryRectangle, ctx: QueryContext) -> BoxStream<Result<Self::Output>> {
        self.as_ref().raster_query(query, ctx)
    }
}

impl<V> QueryProcessor for Box<dyn VectorQueryProcessor<VectorType = V>>
where
    V: 'static,
{
    type Output = V;
    fn query(&self, query: QueryRectangle, ctx: QueryContext) -> BoxStream<Result<Self::Output>> {
        self.as_ref().vector_query(query, ctx)
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

/// An enum that contains all possible query processor variants
#[allow(clippy::pub_enum_variant_names)]
pub enum TypedVectorQueryProcessor {
    Data(Box<dyn VectorQueryProcessor<VectorType = DataCollection>>),
    MultiPoint(Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>),
    MultiLineString(Box<dyn VectorQueryProcessor<VectorType = MultiLineStringCollection>>),
    MultiPolygon(Box<dyn VectorQueryProcessor<VectorType = MultiPolygonCollection>>),
}
