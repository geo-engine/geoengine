use crate::error::{AtLeastOneStreamRequired, Error};
use crate::util::Result;
use futures::future::join_all;
use futures::stream::{BoxStream, Stream};
use futures::{Future, ready};
use geoengine_datatypes::primitives::{BandSelection, RasterQueryRectangle, TimeInterval};
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use pin_project::pin_project;
use snafu::ensure;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Stacks the bands of the input raster streams to create a single raster stream with all the combined bands.
///
/// NOTE: ALL THE INPUT STREAMS MUST HAVE THE SAME TIME INTERVALS.
///
/// If the streams are not guaranteed to be temporally aligned, use the `RasterStackerAdapter` instead.
#[pin_project(project = SimpleRasterStackerAdapterProjection)]
pub struct SimpleRasterStackerAdapter<S> {
    #[pin]
    streams: Vec<S>,
    batch_size_per_stream: Vec<usize>,
    current_stream: usize,
    current_stream_item: usize,
    current_time: Option<TimeInterval>,
    finished: bool,
}

pub struct SimpleRasterStackerSource<S> {
    pub stream: S,
    pub num_bands: usize,
}

impl<S> From<(S, usize)> for SimpleRasterStackerSource<S> {
    fn from(value: (S, usize)) -> Self {
        debug_assert!(value.1 > 0, "At least one band required");
        Self {
            stream: value.0,
            num_bands: value.1,
        }
    }
}

impl<S> SimpleRasterStackerAdapter<S> {
    pub fn new(streams: Vec<SimpleRasterStackerSource<S>>) -> Result<Self> {
        ensure!(!streams.is_empty(), AtLeastOneStreamRequired);

        Ok(SimpleRasterStackerAdapter {
            batch_size_per_stream: streams.iter().map(|s| s.num_bands).collect(),
            streams: streams.into_iter().map(|s| s.stream).collect(),
            current_stream: 0,
            current_stream_item: 0,
            current_time: None,
            finished: false,
        })
    }
}

impl<S, T> Stream for SimpleRasterStackerAdapter<S>
where
    S: Stream<Item = Result<RasterTile2D<T>>> + Unpin,
    T: Send + Sync,
{
    type Item = Result<RasterTile2D<T>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished || self.streams.is_empty() {
            return Poll::Ready(None);
        }

        let SimpleRasterStackerAdapterProjection {
            mut streams,
            batch_size_per_stream,
            current_stream,
            current_stream_item,
            current_time,
            finished: _,
        } = self.as_mut().project();

        let stream = &mut streams[*current_stream];

        let item = ready!(Pin::new(stream).poll_next(cx));

        let Some(mut item) = item else {
            // if one input stream ends, end the output stream
            return Poll::Ready(None);
        };

        if let Ok(tile) = item.as_mut() {
            // compute output band number from its place among all bands of all inputs
            let band = batch_size_per_stream
                .iter()
                .take(*current_stream)
                .sum::<usize>()
                + *current_stream_item;
            tile.band = band as u32;

            // TODO: replace time check with temporal alignment
            if let Some(time) = current_time {
                if band == 0 {
                    // save the first bands time
                    *current_time = Some(tile.time);
                } else {
                    // all other bands must have the same time as the first band
                    if tile.time != *time {
                        return Poll::Ready(Some(Err(
                            Error::InputStreamsMustBeTemporallyAligned {
                                stream_index: *current_stream,
                                expected: *time,
                                found: tile.time,
                            },
                        )));
                    }
                }
            } else {
                // first tile ever, set time
                *current_time = Some(tile.time);
            }
        }

        // next item in stream, or go to next stream
        *current_stream_item += 1;
        if *current_stream_item >= batch_size_per_stream[*current_stream] {
            *current_stream_item = 0;
            *current_stream = (*current_stream + 1) % streams.len();
        }

        Poll::Ready(Some(item))
    }
}

/// A helper method that computes a function on multiple bands (that are already aligned) individually and then stacks the result into a multi-band raster.
pub async fn stack_individual_aligned_raster_bands<'a, F, Fut, P>(
    query: &RasterQueryRectangle,
    ctx: &'a dyn crate::engine::QueryContext,
    create_single_bands_stream_fn: F,
) -> Result<BoxStream<'a, Result<RasterTile2D<P>>>>
where
    F: Fn(RasterQueryRectangle, &'a dyn crate::engine::QueryContext) -> Fut,
    Fut: Future<Output = Result<BoxStream<'a, Result<RasterTile2D<P>>>>>,
    P: Pixel,
{
    if query.attributes.count() == 1 {
        // special case of single band query requires no tile stacking
        return create_single_bands_stream_fn(query.clone(), ctx).await;
    }

    // compute the aggreation for each band separately and stack the streams to get a multi band raster tile stream
    let band_streams = join_all(query.attributes.as_slice().iter().map(|band| {
        let query = query.select_bands(BandSelection::new_single(*band));

        async {
            Ok(SimpleRasterStackerSource {
                stream: create_single_bands_stream_fn(query, ctx).await?,
                num_bands: 1,
            })
        }
    }))
    .await
    .into_iter()
    .collect::<Result<Vec<_>>>()?;

    Ok(Box::pin(SimpleRasterStackerAdapter::new(band_streams)?))
}

#[cfg(test)]
mod tests {
    use futures::{StreamExt, stream};
    use geoengine_datatypes::{
        primitives::{CacheHint, TimeInterval},
        raster::{Grid, TilesEqualIgnoringCacheHint},
        util::test::TestDefault,
    };

    use super::*;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_stacks() {
        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let data2: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![20, 21, 22, 23])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let stream = stream::iter(data.clone().into_iter().map(Result::Ok)).boxed();
        let stream2 = stream::iter(data2.clone().into_iter().map(Result::Ok)).boxed();

        let stacker =
            SimpleRasterStackerAdapter::new(vec![(stream, 1).into(), (stream2, 1).into()]).unwrap();

        let result = stacker.collect::<Vec<_>>().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        let expected: Vec<_> = data
            .into_iter()
            .zip(data2.into_iter().map(|mut tile| {
                tile.band = 1;
                tile
            }))
            .flat_map(|(a, b)| vec![a.clone(), b.clone()])
            .collect();

        assert!(expected.tiles_equal_ignoring_cache_hint(&result));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_stacks_stacks() {
        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![3, 2, 1, 0]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 6, 5, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![11, 10, 9, 8]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![15, 14, 13, 12])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let data2: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 18, 17, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![20, 21, 22, 23])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![32, 22, 21, 20])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![27, 26, 25, 24])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![31, 30, 39, 28])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let stream = stream::iter(data.clone().into_iter().map(Result::Ok)).boxed();
        let stream2 = stream::iter(data2.clone().into_iter().map(Result::Ok)).boxed();

        let stacker =
            SimpleRasterStackerAdapter::new(vec![(stream, 2).into(), (stream2, 2).into()]).unwrap();

        let result = stacker.collect::<Vec<_>>().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        let expected: Vec<_> = data
            .chunks(2)
            .zip(
                data2
                    .into_iter()
                    .map(|mut tile| {
                        tile.band += 2;
                        tile
                    })
                    .collect::<Vec<_>>()
                    .chunks(2),
            )
            .flat_map(|(chunk1, chunk2)| chunk1.iter().chain(chunk2.iter()))
            .cloned()
            .collect();

        assert!(expected.tiles_equal_ignoring_cache_hint(&result));
    }

    #[tokio::test]
    async fn it_checks_temporal_alignment() {
        let data: Vec<RasterTile2D<u8>> = vec![RasterTile2D {
            time: TimeInterval::new_unchecked(0, 5),
            tile_position: [-1, 0].into(),
            band: 0,
            global_geo_transform: TestDefault::test_default(),
            grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
            properties: Default::default(),
            cache_hint: CacheHint::default(),
        }];

        let data2: Vec<RasterTile2D<u8>> = vec![RasterTile2D {
            time: TimeInterval::new_unchecked(1, 6),
            tile_position: [-1, 0].into(),
            band: 0,
            global_geo_transform: TestDefault::test_default(),
            grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
                .unwrap()
                .into(),
            properties: Default::default(),
            cache_hint: CacheHint::default(),
        }];

        let stream = stream::iter(data.clone().into_iter().map(Result::Ok)).boxed();
        let stream2 = stream::iter(data2.clone().into_iter().map(Result::Ok)).boxed();

        let stacker =
            SimpleRasterStackerAdapter::new(vec![(stream, 1).into(), (stream2, 1).into()]).unwrap();

        let result = stacker.collect::<Vec<_>>().await;

        assert!(result[0].is_ok());
        assert!(matches!(
            result[1],
            Err(Error::InputStreamsMustBeTemporallyAligned { .. })
        ));
    }
}
