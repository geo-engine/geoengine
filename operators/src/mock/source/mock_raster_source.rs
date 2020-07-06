use crate::engine::{QueryProcessor, QueryContext, QueryRectangle};
use geoengine_datatypes::primitives::TimeInterval;
use futures::StreamExt;
use crate::util::Result;
use futures::stream::BoxStream;
use geoengine_datatypes::raster::{Raster2D, GenericRaster};

pub struct MockRasterSourceImpl {
    pub data: Vec<f64>,
    pub dim: [usize; 2],
    pub geo_transform: [f64; 6]
}

impl QueryProcessor<dyn GenericRaster> for MockRasterSourceImpl {
    fn query(&self, _query: QueryRectangle, ctx: QueryContext) -> BoxStream<Result<Box<dyn GenericRaster>>> {
        let _size = f64::sqrt(ctx.chunk_byte_size as f64 / 8.0);

        // TODO: return tiles of the input raster
        let temporal_bounds: TimeInterval = TimeInterval::default();
        let raster = Raster2D::new(
            self.dim.into(),
            self.data.clone(),
            None,
            temporal_bounds,
            self.geo_transform.into(),
        ).unwrap();

        let v: Vec<Result<Box<dyn GenericRaster>>> = vec![Ok(Box::new(raster))];
        futures::stream::iter(v.into_iter()).boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D};

    #[tokio::test]
    async fn test() {
        let r = MockRasterSourceImpl {
            data: vec![1., 2., 3., 4.],
            dim: [2, 2],
            geo_transform: [1.0, 1.0, 0.0, 1.0, 0.0, 1.0]
        };

        let query = QueryRectangle {
            bbox: BoundingBox2D::new_unchecked(Coordinate2D::new(1., 2.), Coordinate2D::new(1., 2.)),
            time_interval: TimeInterval::new_unchecked(0, 1),
        };
        let ctx = QueryContext {
            chunk_byte_size: 10 * 8 * 2
        };

        r.query(query, ctx).for_each(|x| {
            println!("{:?}", x);
            futures::future::ready(())
        }).await;
    }
}
