use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::util::Result;
use futures::{ready, Stream};
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use pin_project::pin_project;

/// This adapter extracts the specified bands from a raster stream and adjusts the band number of the output tiles to 0..n.
#[pin_project(project = BandExtractorProjection)]
pub struct BandExtractor<S, T>
where
    S: Stream<Item = Result<RasterTile2D<T>>>,
    T: Pixel,
{
    #[pin]
    stream: S,
    selected_bands: Vec<u32>,
    current_input_band_idx: u32,
    current_output_band_idx: u32,
    num_bands_in_source: u32,
    finished: bool,
}

impl<S, T> BandExtractor<S, T>
where
    S: Stream<Item = Result<RasterTile2D<T>>>,
    T: Pixel,
{
    pub fn new(stream: S, selected_bands: Vec<u32>, num_bands_in_source: u32) -> Self {
        debug_assert!(!selected_bands.is_empty());
        debug_assert!(num_bands_in_source > selected_bands.len() as u32);
        //check that selected bands are ascending
        debug_assert!(selected_bands.windows(2).all(|w| w[0] < w[1]));
        debug_assert!(selected_bands.iter().all(|&b| b < num_bands_in_source));

        Self {
            stream,
            selected_bands,
            current_input_band_idx: 0,
            current_output_band_idx: 0,
            num_bands_in_source,
            finished: false,
        }
    }

    fn next_input_band_idx(
        current_input_band_idx: u32,
        current_output_band_idx: u32,
        num_bands_in_source: u32,
    ) -> u32 {
        let input_band_idx = current_input_band_idx + 1;
        if input_band_idx >= num_bands_in_source {
            debug_assert_eq!(
                current_output_band_idx, 0,
                "all input bands consumed, but not all output bands produced"
            );
            0
        } else {
            input_band_idx
        }
    }
}

impl<S, T> Stream for BandExtractor<S, T>
where
    S: Stream<Item = Result<RasterTile2D<T>>>,
    T: Pixel,
{
    type Item = Result<RasterTile2D<T>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        let BandExtractorProjection {
            mut stream,
            selected_bands,
            current_input_band_idx,
            current_output_band_idx,
            num_bands_in_source,
            finished,
        } = self.as_mut().project();

        // loop until we find a tile that has a band that we want to output
        loop {
            let tile = ready!(stream.as_mut().poll_next(cx));
            let mut tile = match tile {
                Some(Ok(tile)) => tile,
                Some(Err(e)) => {
                    *finished = true;
                    return Poll::Ready(Some(Err(e)));
                }
                None => {
                    *finished = true;
                    return Poll::Ready(None);
                }
            };

            debug_assert_eq!(tile.band, *current_input_band_idx);

            let selected_band = selected_bands[*current_output_band_idx as usize];

            if tile.band == selected_band {
                tile.band = *current_output_band_idx;

                *current_output_band_idx += 1;
                if *current_output_band_idx >= selected_bands.len() as u32 {
                    // all output bands produced, next tile starts with output band 0
                    *current_output_band_idx = 0;
                }

                *current_input_band_idx = Self::next_input_band_idx(
                    *current_input_band_idx,
                    *current_output_band_idx,
                    *num_bands_in_source,
                );

                return Poll::Ready(Some(Ok(tile)));
            }

            *current_input_band_idx = Self::next_input_band_idx(
                *current_input_band_idx,
                *current_output_band_idx,
                *num_bands_in_source,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use geoengine_datatypes::{
        primitives::{CacheHint, TimeInterval},
        raster::{Grid, TilesEqualIgnoringCacheHint},
        util::test::TestDefault,
    };

    use super::*;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_extracts_bands() {
        let tiles = vec![
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
                grid_array: Grid::new([2, 2].into(), vec![2, 3, 4, 5]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let stream = futures::stream::iter(tiles.into_iter().map(Ok));

        let extractor = BandExtractor::new(stream, vec![1, 2], 3);

        let result = extractor
            .map(std::result::Result::unwrap)
            .collect::<Vec<_>>()
            .await;

        let expected = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![2, 3, 4, 5]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
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
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        assert!(result.tiles_equal_ignoring_cache_hint(&expected));
    }
}
