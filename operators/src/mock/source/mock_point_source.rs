use crate::engine::{QueryProcessor, QueryContext, QueryRectangle};
use geoengine_datatypes::primitives::{TimeInterval, Coordinate2D};
use futures::{Stream, StreamExt};
use geoengine_datatypes::collections::MultiPointCollection;
use futures::task::{Context, Poll};
use std::pin::Pin;
use crate::util::Result;
use std::collections::HashMap;
use crate::error::Error;
use futures::stream::BoxStream;
use std::iter::FromIterator;

pub struct MockPointSourceImpl {
    pub points: Vec<Coordinate2D>,
}

impl QueryProcessor<MultiPointCollection> for MockPointSourceImpl {
    fn query(&self, _query: QueryRectangle, ctx: QueryContext) -> BoxStream<Result<Box<MultiPointCollection>>> {
        MockPointSourceResultStream {
            points: self.points.clone(),
            chunk_size: ctx.chunk_byte_size / std::mem::size_of::<Coordinate2D>(),
            index: 0,
        }.boxed()
    }
}

pub struct MockPointSourceResultStream {
    points: Vec<Coordinate2D>,
    chunk_size: usize,
    index: usize,
}

impl Stream for MockPointSourceResultStream {
    type Item = Result<Box<MultiPointCollection>>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let coordinates: Vec<Coordinate2D> = Vec::from_iter(self.points.as_slice().chunks(self.chunk_size).skip(self.index).take(1).flatten().cloned());
        self.index += 1;

        if coordinates.is_empty() {
            return Poll::Ready(None);
        }

        let pc = MultiPointCollection::from_data(
            coordinates.iter().map(|x| vec![*x]).collect(),
            vec![
                TimeInterval::new_unchecked(0, 1); coordinates.len()
            ],
            HashMap::new(),
        ).map(Box::new);

        Poll::Ready(Some(pc.map_err(Error::from)))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use geoengine_datatypes::primitives::BoundingBox2D;
    use crate::engine;

    #[tokio::test]
    async fn test() {
        let mut coordinates = Vec::new();
        for _ in 0..1000 {
            coordinates.push(Coordinate2D::new(0., 1.));
        }

        let p = MockPointSourceImpl {
            points: coordinates
        };

        let query = QueryRectangle {
            bbox: BoundingBox2D::new_unchecked(Coordinate2D::new(1., 2.), Coordinate2D::new(1., 2.)),
            time_interval: TimeInterval::new_unchecked(0, 1),
        };
        let ctx = QueryContext {
            chunk_byte_size: 10 * 8 * 2
        };

        engine::QueryProcessor::query(&p, query, ctx).for_each(|x| {
            println!("{:?}", x);
            futures::future::ready(())
        }).await;
    }
}
