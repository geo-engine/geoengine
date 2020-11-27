use crate::util::Result;
use futures::ready;
use futures::stream::FusedStream;
use futures::Stream;
use geoengine_datatypes::collections::FeatureCollection;
use geoengine_datatypes::primitives::Geometry;
use geoengine_datatypes::util::arrow::ArrowTyped;
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Merges a stream of `FeatureCollection` so that they are at least `chunk_byte_size` large.
/// TODO: This merger outputs an empty stream if all collections are empty
///     Do we need an empty collection with column info as output instead?
///     Do we put the columns to the stream's `VectorQueryContext` instead?
#[pin_project(project = FeatureCollectionChunkMergerProjection)]
pub struct FeatureCollectionChunkMerger<St, G>
where
    St: Stream<Item = Result<FeatureCollection<G>>> + FusedStream,
    G: Geometry + ArrowTyped,
{
    #[pin]
    stream: St,
    accum: Option<FeatureCollection<G>>,
    chunk_size_bytes: usize,
}

impl<St, G> FeatureCollectionChunkMerger<St, G>
where
    St: Stream<Item = Result<FeatureCollection<G>>> + FusedStream,
    G: Geometry + ArrowTyped,
{
    pub fn new(stream: St, chunk_size_bytes: usize) -> Self {
        Self {
            stream,
            accum: None,
            chunk_size_bytes,
        }
    }

    fn merge_and_proceed(
        accum: &mut Option<FeatureCollection<G>>,
        chunk_size_bytes: usize,
        new_collection: St::Item,
    ) -> Option<Poll<Option<St::Item>>> {
        if new_collection.is_err() {
            // TODO: maybe first output existing chunk and then the error?
            return Some(Poll::Ready(Some(new_collection)));
        }

        let new_collection = new_collection.expect("checked");

        let merged_collection = if let Some(old_collection) = accum.take() {
            // TODO: execute on separate thread?
            old_collection.append(&new_collection)
        } else {
            Ok(new_collection)
        };

        match merged_collection {
            Ok(collection)
                if !collection.is_empty() && collection.byte_size() >= chunk_size_bytes =>
            {
                Some(Poll::Ready(Some(Ok(collection))))
            }
            Ok(collection) => {
                *accum = Some(collection);
                None
            }
            Err(error) => Some(Poll::Ready(Some(Err(error.into())))),
        }
    }

    fn output_remaining_chunk(accum: &mut Option<FeatureCollection<G>>) -> Poll<Option<St::Item>> {
        match accum.take() {
            Some(last_chunk) if !last_chunk.is_empty() => Poll::Ready(Some(Ok(last_chunk))),
            _ => Poll::Ready(None),
        }
    }
}

impl<St, G> Stream for FeatureCollectionChunkMerger<St, G>
where
    St: Stream<Item = Result<FeatureCollection<G>>> + FusedStream,
    G: Geometry + ArrowTyped,
{
    type Item = St::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<St::Item>> {
        let FeatureCollectionChunkMergerProjection {
            mut stream,
            accum,
            chunk_size_bytes,
        } = self.as_mut().project();

        let mut output: Option<Poll<Option<St::Item>>> = None;

        while output.is_none() {
            if stream.is_terminated() {
                return Self::output_remaining_chunk(accum);
            }

            let next = ready!(stream.as_mut().poll_next(cx));

            output = if let Some(collection) = next {
                Self::merge_and_proceed(accum, *chunk_size_bytes, collection)
            } else {
                Some(Self::output_remaining_chunk(accum))
            }
        }

        output.expect("checked")
    }
}

impl<St, G> FusedStream for FeatureCollectionChunkMerger<St, G>
where
    St: Stream<Item = Result<FeatureCollection<G>>> + FusedStream,
    G: Geometry + ArrowTyped,
{
    fn is_terminated(&self) -> bool {
        self.stream.is_terminated() && self.accum.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::{
        MockExecutionContextCreator, QueryContext, QueryProcessor, QueryRectangle,
        TypedVectorQueryProcessor, VectorOperator,
    };
    use crate::error::Error;
    use crate::mock::{MockFeatureCollectionSource, MockPointSource, MockPointSourceParams};
    use futures::{StreamExt, TryStreamExt};
    use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D, MultiPoint, TimeInterval};
    use geoengine_datatypes::{
        collections::{DataCollection, MultiPointCollection},
        primitives::SpatialResolution,
    };

    #[tokio::test]
    async fn simple() {
        let coordinates: Vec<Coordinate2D> = (0..10)
            .map(f64::from)
            .map(|v| Coordinate2D::new(v, v))
            .collect();

        let source = MockPointSource {
            params: MockPointSourceParams {
                points: coordinates.clone(),
            },
        };

        let source = source
            .boxed()
            .initialize(&MockExecutionContextCreator::default().context())
            .unwrap();

        let processor =
            if let TypedVectorQueryProcessor::MultiPoint(p) = source.query_processor().unwrap() {
                p
            } else {
                unreachable!();
            };

        let qrect = QueryRectangle {
            bbox: BoundingBox2D::new((0.0, 0.0).into(), (10.0, 10.0).into()).unwrap(),
            time_interval: Default::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let cx = QueryContext {
            chunk_byte_size: std::mem::size_of::<Coordinate2D>() * 2,
        };

        let number_of_source_chunks = processor
            .query(qrect, cx)
            .fold(0_usize, async move |i, _| i + 1)
            .await;
        assert_eq!(number_of_source_chunks, 5);

        let stream = processor.query(qrect, cx);

        let chunk_byte_size = MultiPointCollection::from_data(
            MultiPoint::many(coordinates[0..5].to_vec()).unwrap(),
            vec![TimeInterval::default(); 5],
            Default::default(),
        )
        .unwrap()
        .byte_size();

        let stream = FeatureCollectionChunkMerger::new(stream.fuse(), chunk_byte_size);

        let collections: Vec<MultiPointCollection> = stream
            .collect::<Vec<Result<MultiPointCollection>>>()
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect();

        assert_eq!(collections.len(), 2);

        assert_eq!(
            collections[0],
            MultiPointCollection::from_data(
                MultiPoint::many(coordinates[0..6].to_vec()).unwrap(),
                vec![TimeInterval::default(); 6],
                Default::default()
            )
            .unwrap()
        );

        assert_eq!(
            collections[1],
            MultiPointCollection::from_data(
                MultiPoint::many(coordinates[6..10].to_vec()).unwrap(),
                vec![TimeInterval::default(); 4],
                Default::default()
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn empty() {
        let source = MockFeatureCollectionSource {
            params: MockFeatureCollectionSourceParams {
                collection: DataCollection::empty(),
            },
        }
        .boxed()
        .initialize(&MockExecutionContextCreator::default().context())
        .unwrap();
        let source = MockFeatureCollectionSource::single(DataCollection::empty())
            .boxed()
            .initialize(&ExecutionContext::mock_empty())
            .unwrap();

        let processor =
            if let TypedVectorQueryProcessor::Data(p) = source.query_processor().unwrap() {
                p
            } else {
                unreachable!();
            };

        let qrect = QueryRectangle {
            bbox: BoundingBox2D::new((0.0, 0.0).into(), (0.0, 0.0).into()).unwrap(),
            time_interval: Default::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let cx = QueryContext { chunk_byte_size: 0 };

        let collections = FeatureCollectionChunkMerger::new(processor.query(qrect, cx).fuse(), 0)
            .collect::<Vec<Result<DataCollection>>>()
            .await;

        assert_eq!(collections.len(), 0);
    }

    #[tokio::test]
    async fn intermediate_errors() {
        let source = futures::stream::iter(vec![
            MultiPointCollection::from_data(
                MultiPoint::many(vec![(0.0, 0.1)]).unwrap(),
                vec![TimeInterval::new(0, 1).unwrap()],
                Default::default(),
            ),
            MultiPointCollection::from_data(
                vec![], // should fail
                vec![TimeInterval::new(0, 1).unwrap()],
                Default::default(),
            ),
            MultiPointCollection::from_data(
                MultiPoint::many(vec![(1.0, 1.1)]).unwrap(),
                vec![TimeInterval::new(0, 1).unwrap()],
                Default::default(),
            ),
        ])
        .map_err(Error::from);

        let merged_collections = FeatureCollectionChunkMerger::new(source.fuse(), 0)
            .collect::<Vec<Result<MultiPointCollection>>>()
            .await;

        assert_eq!(merged_collections.len(), 3);
        assert_eq!(
            merged_collections[0].as_ref().unwrap(),
            &MultiPointCollection::from_data(
                MultiPoint::many(vec![(0.0, 0.1)]).unwrap(),
                vec![TimeInterval::new(0, 1).unwrap()],
                Default::default(),
            )
            .unwrap()
        );
        assert!(merged_collections[1].is_err());
        assert_eq!(
            merged_collections[2].as_ref().unwrap(),
            &MultiPointCollection::from_data(
                MultiPoint::many(vec![(1.0, 1.1)]).unwrap(),
                vec![TimeInterval::new(0, 1).unwrap()],
                Default::default(),
            )
            .unwrap()
        );
    }

    #[tokio::test(max_threads = 1)]
    async fn interleaving_pendings() {
        let mut stream_history: Vec<Poll<Option<Result<MultiPointCollection>>>> = vec![
            Poll::Pending,
            Poll::Ready(Some(
                MultiPointCollection::from_data(
                    MultiPoint::many(vec![(0.0, 0.1)]).unwrap(),
                    vec![TimeInterval::new(0, 1).unwrap()],
                    Default::default(),
                )
                .map_err(Error::from),
            )),
            Poll::Pending,
            Poll::Ready(Some(
                MultiPointCollection::from_data(
                    MultiPoint::many(vec![(1.0, 1.1)]).unwrap(),
                    vec![TimeInterval::new(0, 1).unwrap()],
                    Default::default(),
                )
                .map_err(Error::from),
            )),
        ];
        stream_history.reverse();

        let stream = futures::stream::poll_fn(move |cx| {
            let item = stream_history.pop().unwrap_or(Poll::Ready(None));

            if let Poll::Pending = item {
                cx.waker().wake_by_ref();
            }

            item
        });

        let merged_collections = FeatureCollectionChunkMerger::new(stream.fuse(), usize::MAX)
            .collect::<Vec<Result<MultiPointCollection>>>()
            .await;

        assert_eq!(merged_collections.len(), 1);
        assert_eq!(
            merged_collections[0].as_ref().unwrap(),
            &MultiPointCollection::from_data(
                MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1)]).unwrap(),
                vec![TimeInterval::new(0, 1).unwrap(); 2],
                Default::default(),
            )
            .unwrap()
        );
    }
}
