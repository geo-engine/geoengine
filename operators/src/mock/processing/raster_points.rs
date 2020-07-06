use geoengine_datatypes::collections::MultiPointCollection;
use crate::engine::{QueryProcessor, QueryRectangle, QueryContext};
use geoengine_datatypes::raster::GenericRaster;
use futures::stream::BoxStream;
use crate::util::Result;
use futures::StreamExt;

/// Attach raster value at given coords to points
pub struct MockRasterPointsImpl {
    pub points: Vec<Box<dyn QueryProcessor<MultiPointCollection>>>,
    pub rasters: Vec<Box<dyn QueryProcessor<dyn GenericRaster>>>,
    pub coords: [usize; 2],
}

impl QueryProcessor<MultiPointCollection> for MockRasterPointsImpl {
    fn query(&self, query: QueryRectangle, ctx: QueryContext) -> BoxStream<Result<Box<MultiPointCollection>>> {
        // TODO perform join

        self.points[0].query(query, ctx)
            .then(async move |p| {
                // TODO: use points bbox
                let mut rs = self.rasters[0].query(query, ctx);
                while let Some(_r) = rs.next().await {
                    // TODO: extract raster info
                }
                p
            })
            .boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::mock::source::mock_point_source::MockPointSourceImpl;
    use geoengine_datatypes::primitives::{Coordinate2D, BoundingBox2D, TimeInterval};
    use crate::mock::source::mock_raster_source::MockRasterSourceImpl;

    #[tokio::test]
    #[allow(clippy::cast_lossless)]
    async fn test() {
        let mut coordinates = Vec::new();
        for i in 0..100 {
            coordinates.push(Coordinate2D::new(i as f64, (i + 1) as f64));
        }

        let p = MockPointSourceImpl {
            points: coordinates
        };

        let r = MockRasterSourceImpl {
            data: vec![1., 2., 3., 4.],
            dim: [2, 2],
            geo_transform: [1.0, 1.0, 0.0, 1.0, 0.0, 1.0]
        };

        let o = MockRasterPointsImpl {
            points: vec![Box::new(p)],
            rasters: vec![Box::new(r)],
            coords: [0, 0]
        };

        let query = QueryRectangle {
            bbox: BoundingBox2D::new_unchecked(Coordinate2D::new(1., 2.), Coordinate2D::new(1., 2.)),
            time_interval: TimeInterval::new_unchecked(0, 1),
        };
        let ctx = QueryContext {
            chunk_byte_size: 10 * 8 * 2
        };

        o.query(query, ctx).for_each(|x| {
            println!("{:?}", x);
            futures::future::ready(())
        }).await;
    }
}
