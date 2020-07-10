use std::{thread, time};

use futures::stream::BoxStream;
use futures::StreamExt;

use geoengine_datatypes::collections::MultiPointCollection;

use crate::engine::{QueryContext, QueryProcessor, QueryRectangle};
use crate::util::Result;

pub struct MockDelayImpl {
    pub points: Vec<Box<dyn QueryProcessor<MultiPointCollection>>>,
    pub seconds: u64
}

impl QueryProcessor<MultiPointCollection> for MockDelayImpl {
    fn query(&self, query: QueryRectangle, ctx: QueryContext) -> BoxStream<Result<Box<MultiPointCollection>>> {
        let seconds = self.seconds;

        self.points[0].query(query, ctx).then(async move |x| {
            let _ = tokio::spawn(async move {
                thread::sleep(time::Duration::from_secs(seconds));
            }).await;
            x
        }).boxed()
    }
}


#[cfg(test)]
mod test {
    use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D, TimeInterval};

    use crate::mock::source::mock_point_source::MockPointSourceImpl;

    use super::*;

    #[tokio::test]
    async fn test() {
        let mut coordinates = Vec::new();
        for _ in 0..100 {
            coordinates.push(Coordinate2D::new(0., 1.));
        }

        let p = MockPointSourceImpl {
            points: coordinates
        };

        let o = MockDelayImpl {
            points: vec![Box::new(p)],
            seconds: 2,
        };

        let query = QueryRectangle {
            bbox: BoundingBox2D::new_unchecked(Coordinate2D::new(1., 2.), Coordinate2D::new(1., 2.)),
            time_interval: TimeInterval::new_unchecked(0, 1),
        };

        let ctx = QueryContext {
            chunk_byte_size: 10 * 8 * 2
        };

        let now = time::Instant::now();
        o.query(query, ctx).for_each(|_x| {
            println!("{}", now.elapsed().as_secs());
            futures::future::ready(())
        }).await;

        // TODO: 2nd query and select!
    }
}
