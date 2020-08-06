use super::query::{QueryContext, QueryRectangle};
use crate::util::Result;
use futures::stream::BoxStream;
use geoengine_datatypes::{collections::MultiPointCollection, raster::RasterTile2D};

/// The Query...
#[derive(Debug, Clone, Copy)]
pub struct Query;

pub trait QueryProcessor {
    type Output;
    fn query(&self, query: QueryRectangle, ctx: QueryContext) -> BoxStream<Result<Self::Output>>;
}

pub trait RasterQueryProcessor: Sync {
    type RasterType;
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
    S: QueryProcessor<Output = RasterTile2D<T>> + Sync,
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

pub trait VectorQueryProcessor: Sync {
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
    S: QueryProcessor<Output = VD> + Sync,
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
    T: 'static,
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

pub enum TypedVectorQueryProcessor {
    MultiPoint(Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>),
}

/*
#[cfg(test)]
mod tests {
    use crate::{
        GdalSource, MyVectorSource, Point, Query, RasterOperatorExt, Source, VectorOperatorExt,
        VectorSource,
    };
    use std::marker::PhantomData;

    #[test]
    fn complex() {
        // a gdal source
        let gdal_source: GdalSource<u16> = GdalSource {
            dataset: "meh".to_owned(),
            data: PhantomData,
        };

        // concrete raster!
        let r = gdal_source.query(Query);
        println!("{:?}", r);

        let raster_plus_one = gdal_source.plus_one();
        let r = raster_plus_one.query(Query);
        println!("{:?}", r);

        let other_gdal_source: GdalSource<u8> = GdalSource {
            dataset: "meh".to_owned(),
            data: PhantomData,
        };

        let raster_plusone_plus_other = raster_plus_one.plus_raster(other_gdal_source);
        let r = raster_plusone_plus_other.query(Query);
        println!("{:?}", r);

        // a vector source
        let vector_source: MyVectorSource<Point> = MyVectorSource {
            dataset: "vec".to_owned(),
            data: PhantomData,
        };

        // concrete vector!
        let v = vector_source.query(Query);
        println!("{:?}", v);

        // take the vector_source, add a noop, combine the result with the raster_source wrapped in a noop
        let vector_noop_raster_noop_combine = vector_source
            .noop()
            .add_raster_values(RasterOperatorExt::noop(raster_plusone_plus_other));
        // add more noops
        let vector_noop_raster_noop_combine_noop_noop =
            vector_noop_raster_noop_combine.noop().noop();
        // will produce the concrete vector type! (all known at compile time)
        println!(
            "{:?}",
            vector_noop_raster_noop_combine_noop_noop.vector_query(Query)
        );
    }
}
*/
