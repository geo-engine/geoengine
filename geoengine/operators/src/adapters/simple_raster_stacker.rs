use crate::engine::RasterQueryProcessor;
use crate::error::Error;
use crate::util::Result;
use futures::future::join_all;
use futures::stream::{BoxStream, Stream};
use futures::{TryFutureExt, ready};
use geoengine_datatypes::primitives::{BandSelection, RasterQueryRectangle, TimeInterval};
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use pin_project::pin_project;
use snafu::{Snafu, ensure};
use std::pin::Pin;
use std::task::{Context, Poll};
use strum::IntoStaticStr;

#[derive(Debug, Snafu, IntoStaticStr)]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum SimpleRasterStackerError {
    #[snafu(display(
        "BandSelection count exceeds band input count:  {} > {}",
        selection_bands,
        source_bands
    ))]
    BandInputAndSelectionMissmatch {
        source_bands: usize,
        selection_bands: usize,
    },
    AtLeastOneStreamRequired,

    InputsNotTemporalAligned,

    CreateSourceStreams {
        source: Box<Error>,
    },

    CreateSingleBandStream {
        source: Box<Error>,
    },

    UnAlignedElements {
        t1: TimeInterval,
        t2: TimeInterval,
    },
}

/// Stacks the bands of the input raster streams to create a single raster stream with all the combined bands.
///
/// NOTE: ALL THE INPUT STREAMS MUST HAVE THE SAME TIME INTERVALS.
///
/// If the streams are not guaranteed to be temporally aligned, use the `RasterStackerAdapter` instead.
#[pin_project(project = SimpleRasterStackerAdapterProjection)]
pub struct SimpleRasterStackerAdapter<S> {
    #[pin]
    streams: Vec<S>,
    current_idx: usize,
    current_time: Option<TimeInterval>,
    finished: bool,
    band_tracking: BandTracker,
}

pub struct BandTracker {
    source_band_idxs: Vec<BandSelection>,
    band_selection: BandSelection,
}

impl BandTracker {
    pub fn new(
        source_bands: Vec<BandSelection>,
        out_bands: BandSelection,
    ) -> Result<Self, SimpleRasterStackerError> {
        ensure!(!source_bands.is_empty(), AtLeastOneStreamRequired);
        ensure!(out_bands.count() > 0, AtLeastOneStreamRequired);

        let bt = Self {
            source_band_idxs: source_bands,
            band_selection: out_bands,
        };

        ensure!(
            bt.num_total_source_bands() == bt.band_selection.as_slice().len(),
            BandInputAndSelectionMissmatch {
                selection_bands: bt.band_selection.as_slice().len(),
                source_bands: bt.num_total_source_bands()
            }
        );

        Ok(bt)
    }

    pub fn num_total_source_bands(&self) -> usize {
        self.source_band_idxs.iter().map(|s| s.as_vec().len()).sum()
    }

    pub fn num_sources(&self) -> usize {
        self.source_band_idxs.len()
    }

    pub fn num_source_bands(&self, source: usize) -> usize {
        self.source_band_idxs[source].as_slice().len()
    }

    pub fn band_idx_to_source_band_idx(&self, band_idx: usize) -> (usize, usize) {
        debug_assert!(
            band_idx < self.num_total_source_bands(),
            "Band must be in range of all source bands"
        );

        let mut starts_at = 0;
        let mut source_idx = 0;
        for (i, num_bands) in self
            .source_band_idxs
            .iter()
            .map(|s| s.as_slice().len())
            .enumerate()
        {
            let next_starts_at = starts_at + num_bands;
            if (starts_at..next_starts_at).contains(&band_idx) {
                source_idx = i;
                break;
            }
            starts_at = next_starts_at;
        }

        let source_band_idx = band_idx - starts_at;

        debug_assert!(
            source_idx < self.num_sources(),
            "Source {source_idx} must be in range {0}",
            self.num_sources()
        );
        debug_assert!(
            source_band_idx < self.num_source_bands(source_idx),
            "Band {source_band_idx} must be in range of source bands {0}",
            self.num_source_bands(source_idx)
        );

        (source_idx, source_band_idx)
    }

    pub fn source_in_and_out_band(&self, band_idx: usize) -> (usize, u32, u32) {
        let (source, in_band_idx) = self.band_idx_to_source_band_idx(band_idx);
        let in_band = self.source_band_idxs[source].as_slice()[in_band_idx];
        let out_band = self.band_selection.as_slice()[band_idx];

        (source, in_band, out_band)
    }
}

pub struct SimpleRasterStackerSource<S> {
    pub stream: S,
    pub band_idxs: BandSelection,
}

impl<S> SimpleRasterStackerSource<S> {
    pub fn num_bands(&self) -> usize {
        self.band_idxs.as_slice().len()
    }
}

impl<S, I: TryInto<BandSelection>> TryFrom<(S, I)> for SimpleRasterStackerSource<S> {
    type Error = I::Error;

    fn try_from(value: (S, I)) -> Result<Self, Self::Error> {
        value.1.try_into().map(|bs| SimpleRasterStackerSource {
            stream: value.0,
            band_idxs: bs,
        })
    }
}

impl<S> SimpleRasterStackerAdapter<S> {
    pub fn new(
        streams: Vec<SimpleRasterStackerSource<S>>,
        out_bands: BandSelection,
    ) -> Result<Self, SimpleRasterStackerError> {
        ensure!(!streams.is_empty(), AtLeastOneStreamRequired);

        let (streams, bandsel): (Vec<S>, Vec<BandSelection>) = streams
            .into_iter()
            .map(|s| (s.stream, s.band_idxs))
            .collect();

        let bt = BandTracker::new(bandsel, out_bands)?;

        Ok(SimpleRasterStackerAdapter {
            streams,
            current_idx: 0,
            current_time: None,
            finished: false,
            band_tracking: bt,
        })
    }

    /// A helper method that computes a function on multiple bands (that are already aligned) individually and then stacks the result into a multi-band raster.
    pub async fn stack_individual_aligned_raster_bands<'a, F, Fut, P>(
        query: &RasterQueryRectangle,
        ctx: &'a dyn crate::engine::QueryContext,
        create_single_bands_stream_fn: F,
    ) -> Result<SimpleRasterStackerAdapter<S>, SimpleRasterStackerError>
    where
        S: Stream<Item = Result<RasterTile2D<P>>> + Unpin,
        F: Fn(RasterQueryRectangle, &'a dyn crate::engine::QueryContext) -> Fut,
        Fut: Future<Output = Result<S>>,
        P: Pixel,
    {
        // compute the aggreation for each band separately and stack the streams to get a multi band raster tile stream
        let band_streams = join_all(query.attributes().as_slice().iter().map(|band| {
            let band_idx = BandSelection::new_single(*band);
            let query = query.select_attributes(band_idx.clone());

            async {
                create_single_bands_stream_fn(query, ctx)
                    .await
                    .map_err(|e| SimpleRasterStackerError::CreateSingleBandStream {
                        source: Box::new(e),
                    })
                    .map(|s| SimpleRasterStackerSource {
                        stream: s,
                        band_idxs: band_idx,
                    })
            }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

        SimpleRasterStackerAdapter::new(band_streams, query.attributes().clone())
    }

    /// This method queries multiple sources for the requested bands and stacks the results into a multi band raster tile stream.
    /// The order of the bands is determined by the order of the sources. The query attributes are used to select the relevant bands from the sources.
    pub async fn stack_selected_regular_aligned_raster_bands<'a, P, R>(
        query: &RasterQueryRectangle,
        ctx: &'a dyn crate::engine::QueryContext,
        sources: &'a [R],
    ) -> Result<
        SimpleRasterStackerAdapter<BoxStream<'a, Result<RasterTile2D<P>>>>,
        SimpleRasterStackerError,
    >
    where
        R: RasterQueryProcessor<RasterType = P> + 'a,
    {
        // check that inputs are temp aligned
        let _regular_time = sources
            .iter()
            .map(|r| r.raster_result_descriptor().time.dimension.unwrap_regular())
            .reduce(|a, f| match (a, f) {
                (Some(at), Some(ft)) if at.compatible_with(ft) => Some(at),
                _ => None,
            })
            .ok_or(SimpleRasterStackerError::AtLeastOneStreamRequired)?
            .ok_or(SimpleRasterStackerError::InputsNotTemporalAligned)?;

        let bands_per_source = sources
            .iter()
            .map(|rq| rq.result_descriptor().bands.count())
            .collect::<Vec<_>>();

        let total_bands: u32 = bands_per_source.iter().sum();

        let temp_bt = BandTracker::new(
            bands_per_source
                .iter()
                .map(|&num_bands| BandSelection::first_n(num_bands))
                .collect(),
            BandSelection::first_n(total_bands),
        )?;

        // Build source bands mapping
        let mut source_bands: Vec<Vec<u32>> = vec![Vec::new(); sources.len()];
        for out_band in query.attributes().as_slice() {
            let (source_idx, in_band, check_out_band) =
                temp_bt.source_in_and_out_band(*out_band as usize);

            debug_assert_eq!(check_out_band, *out_band, "Band tracking mismatch");

            source_bands[source_idx].push(in_band);
        }

        // Query relevant sources
        let (futures, band_lists): (Vec<_>, Vec<_>) = (0..sources.len())
            .filter_map(|source_idx| {
                if source_bands[source_idx].is_empty() {
                    return None;
                }
                let bands = source_bands[source_idx].clone();
                let band_selection = BandSelection::new_unchecked(bands.clone());
                Some((
                    sources[source_idx]
                        .raster_query(query.select_attributes(band_selection), ctx)
                        .map_err(|e| SimpleRasterStackerError::CreateSourceStreams {
                            source: Box::new(e),
                        }),
                    bands,
                ))
            })
            .unzip();

        // Await and construct sources
        let stacker_sources = join_all(futures)
            .await
            .into_iter()
            .zip(band_lists)
            .map(|(stream_result, bands)| {
                stream_result.map(|stream| SimpleRasterStackerSource {
                    stream,
                    band_idxs: BandSelection::new_unchecked(bands),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        SimpleRasterStackerAdapter::new(stacker_sources, query.attributes().clone())
    }
}

impl<S, T> Stream for SimpleRasterStackerAdapter<S>
where
    S: Stream<Item = Result<RasterTile2D<T>>> + Unpin,
    T: Send + Sync + Pixel,
{
    type Item = Result<RasterTile2D<T>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished || self.streams.is_empty() {
            return Poll::Ready(None);
        }

        let SimpleRasterStackerAdapterProjection {
            mut streams,
            current_idx,
            current_time,
            finished,
            band_tracking,
        } = self.as_mut().project();

        let (stream_idx, in_band, out_band) = band_tracking.source_in_and_out_band(*current_idx);
        let stream = &mut streams[stream_idx];

        let item = ready!(Pin::new(stream).poll_next(cx));

        let Some(mut item) = item else {
            // if one input stream ends, end the output stream
            *finished = true;
            return Poll::Ready(None);
        };

        if let Ok(tile) = item.as_mut() {
            debug_assert!(
                tile.band == in_band,
                "Expected stream {stream_idx} band {in_band} got {0} to produce out band {out_band}",
                tile.band
            );

            tile.band = out_band;

            if let Some(time) = current_time {
                if *current_idx == 0 {
                    debug_assert!(
                        *time == tile.time || time.end() == tile.time.start(),
                        "Time hole discovered! Time of last tile: {time}, time of current tile: {0}. Stream index: {stream_idx}, In band: {in_band}, Out band: {out_band}",
                        tile.time
                    );

                    // save the first bands time
                    *current_time = Some(tile.time);
                } else {
                    // all other bands must have the same time as the first band
                    if tile.time != *time {
                        return Poll::Ready(Some(Err(
                            Error::InputStreamsMustBeTemporallyAligned {
                                stream_index: stream_idx,
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
        *current_idx += 1;
        if *current_idx >= band_tracking.num_total_source_bands() {
            *current_idx = 0;
        }

        Poll::Ready(Some(item))
    }
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

    #[test]
    fn band_tracker_works() {
        let bt = BandTracker::new(
            vec![BandSelection::first_n(2), BandSelection::first_n(3)],
            BandSelection::first_n(5),
        )
        .unwrap();

        assert_eq!(bt.num_total_source_bands(), 5);
        assert_eq!(bt.num_sources(), 2);
        assert_eq!(bt.num_source_bands(0), 2);
        assert_eq!(bt.num_source_bands(1), 3);

        assert_eq!(bt.band_idx_to_source_band_idx(0), (0, 0));
        assert_eq!(bt.band_idx_to_source_band_idx(1), (0, 1));
        assert_eq!(bt.band_idx_to_source_band_idx(2), (1, 0));
        assert_eq!(bt.band_idx_to_source_band_idx(3), (1, 1));
        assert_eq!(bt.band_idx_to_source_band_idx(4), (1, 2));

        assert_eq!(bt.source_in_and_out_band(0), (0, 0, 0));
        assert_eq!(bt.source_in_and_out_band(1), (0, 1, 1));
        assert_eq!(bt.source_in_and_out_band(2), (1, 0, 2));
        assert_eq!(bt.source_in_and_out_band(3), (1, 1, 3));
        assert_eq!(bt.source_in_and_out_band(4), (1, 2, 4));
    }

    #[test]
    fn band_tracker_with_gaps_works() {
        let bt = BandTracker::new(
            vec![BandSelection::first_n(2), BandSelection::first_n(3)],
            BandSelection::new(vec![0, 2, 4, 6, 8]).unwrap(),
        )
        .unwrap();

        assert_eq!(bt.num_total_source_bands(), 5);
        assert_eq!(bt.num_sources(), 2);
        assert_eq!(bt.num_source_bands(0), 2);
        assert_eq!(bt.num_source_bands(1), 3);

        assert_eq!(bt.band_idx_to_source_band_idx(0), (0, 0));
        assert_eq!(bt.band_idx_to_source_band_idx(1), (0, 1));
        assert_eq!(bt.band_idx_to_source_band_idx(2), (1, 0));
        assert_eq!(bt.band_idx_to_source_band_idx(3), (1, 1));
        assert_eq!(bt.band_idx_to_source_band_idx(4), (1, 2));

        assert_eq!(bt.source_in_and_out_band(0), (0, 0, 0));
        assert_eq!(bt.source_in_and_out_band(1), (0, 1, 2));
        assert_eq!(bt.source_in_and_out_band(2), (1, 0, 4));
        assert_eq!(bt.source_in_and_out_band(3), (1, 1, 6));
        assert_eq!(bt.source_in_and_out_band(4), (1, 2, 8));
    }

    #[test]
    fn band_tracker_with_input_gaps_works() {
        let bt = BandTracker::new(
            vec![
                BandSelection::new(vec![0, 2]).unwrap(),
                BandSelection::new(vec![1, 3, 5]).unwrap(),
            ],
            BandSelection::first_n(5),
        )
        .unwrap();

        assert_eq!(bt.num_total_source_bands(), 5);
        assert_eq!(bt.num_sources(), 2);
        assert_eq!(bt.num_source_bands(0), 2);
        assert_eq!(bt.num_source_bands(1), 3);

        assert_eq!(bt.band_idx_to_source_band_idx(0), (0, 0));
        assert_eq!(bt.band_idx_to_source_band_idx(1), (0, 1));
        assert_eq!(bt.band_idx_to_source_band_idx(2), (1, 0));
        assert_eq!(bt.band_idx_to_source_band_idx(3), (1, 1));
        assert_eq!(bt.band_idx_to_source_band_idx(4), (1, 2));

        assert_eq!(bt.source_in_and_out_band(0), (0, 0, 0));
        assert_eq!(bt.source_in_and_out_band(1), (0, 2, 1));
        assert_eq!(bt.source_in_and_out_band(2), (1, 1, 2));
        assert_eq!(bt.source_in_and_out_band(3), (1, 3, 3));
        assert_eq!(bt.source_in_and_out_band(4), (1, 5, 4));
    }

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

        let stacker = SimpleRasterStackerAdapter::new(
            vec![
                (stream, 0).try_into().unwrap(),
                (stream2, 0).try_into().unwrap(),
            ],
            BandSelection::first_n(2),
        )
        .unwrap();

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

        let stacker = SimpleRasterStackerAdapter::new(
            vec![
                (stream, [0, 1]).try_into().unwrap(),
                (stream2, [0, 1]).try_into().unwrap(),
            ],
            BandSelection::first_n(4),
        )
        .unwrap();

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

        let stacker = SimpleRasterStackerAdapter::new(
            vec![
                (stream, 0).try_into().unwrap(),
                (stream2, 0).try_into().unwrap(),
            ],
            BandSelection::first_n(2),
        )
        .unwrap();

        let result = stacker.collect::<Vec<_>>().await;

        assert!(result[0].is_ok());
        assert!(matches!(
            result[1],
            Err(Error::InputStreamsMustBeTemporallyAligned { .. })
        ));
    }
}
