use std::iter::Enumerate;

use geoengine_datatypes::collections::{FeatureCollection, GeometryRandomAccess};
use geoengine_datatypes::primitives::{Geometry, MultiPoint, MultiPointAccess, MultiPolygon};
use geoengine_datatypes::raster::{GridContains, GridShapeAccess};
use geoengine_datatypes::{
    primitives::TimeInterval,
    raster::{GridIdx2D, Pixel, RasterTile2D},
};

use crate::processing::PointInPolygonTester;

/// A `FeatureTimeSpan` combines a `TimeInterval` with a set of features it spans over.
/// Thus, it is used in combination with a `FeatureCollection`.
///
/// Both, `feature_index_start` and `feature_index_end` are inclusive.
///
#[derive(Debug, Clone, PartialEq)]
pub struct FeatureTimeSpan {
    pub feature_index_start: usize,
    pub feature_index_end: usize,
    pub time_interval: TimeInterval,
}

/// An iterator over `FeatureTimeSpan`s of `TimeInterval`s of a sorted `FeatureCollection`.
///
/// The `TimeInterval`s must be sorted ascending in order for the iterator to work.
///
pub struct FeatureTimeSpanIter<'c> {
    time_intervals: Enumerate<std::slice::Iter<'c, TimeInterval>>,
    current_time_span: Option<FeatureTimeSpan>,
}

impl<'c> FeatureTimeSpanIter<'c> {
    pub fn new(time_intervals: &'c [TimeInterval]) -> Self {
        Self {
            time_intervals: time_intervals.iter().enumerate(),
            current_time_span: None,
        }
    }
}

impl<'c> Iterator for FeatureTimeSpanIter<'c> {
    type Item = FeatureTimeSpan;

    fn next(&mut self) -> Option<Self::Item> {
        for (idx, time_interval) in &mut self.time_intervals {
            match self.current_time_span.take() {
                None => {
                    // nothing there yet? store it as the beginning

                    self.current_time_span = Some(FeatureTimeSpan {
                        feature_index_start: idx,
                        feature_index_end: idx,
                        time_interval: *time_interval,
                    });
                }
                Some(mut time_span) => {
                    if let Ok(combined_time_interval) =
                        time_span.time_interval.union(&time_interval)
                    {
                        // merge time intervals if possible

                        time_span.time_interval = combined_time_interval;
                        time_span.feature_index_end = idx;

                        self.current_time_span = Some(time_span);
                    } else {
                        // store current time interval for next span

                        self.current_time_span = Some(FeatureTimeSpan {
                            feature_index_start: idx,
                            feature_index_end: idx,
                            time_interval: *time_interval,
                        });

                        return Some(time_span);
                    }
                }
            }
        }

        // output last time span or `None`
        self.current_time_span.take()
    }
}

/// To calculate the pixels covered by a feature's geometries, a calculator is first initialized with a
/// collection of the given type `G` and can then be queried for each feature by the feature's index
pub trait CoveredPixels<G: Geometry>: Send + Sync {
    /// initialize the calculator with the given `collection`, potentially doing some  (expensive) precalculations
    fn initialize(collection: FeatureCollection<G>) -> Self;

    /// return the pixels of the given `raster` that are covered by the geometries of the feature at the
    /// `feature_index`
    fn covered_pixels<P: Pixel>(
        &self,
        feature_index: usize,
        raster: &RasterTile2D<P>,
    ) -> Vec<GridIdx2D>;

    fn collection_ref(&self) -> &FeatureCollection<G>;

    fn collection(self) -> FeatureCollection<G>;
}

pub struct MultiPointCoveredPixels {
    collection: FeatureCollection<MultiPoint>,
}

impl<'a> CoveredPixels<MultiPoint> for MultiPointCoveredPixels {
    fn initialize(collection: FeatureCollection<MultiPoint>) -> Self {
        Self { collection }
    }

    fn covered_pixels<P: Pixel>(
        &self,
        feature_index: usize,
        raster: &RasterTile2D<P>,
    ) -> Vec<GridIdx2D> {
        let geo_transform = raster.tile_information().tile_geo_transform();
        self.collection
            .geometry_at(feature_index)
            .unwrap()
            .points()
            .iter()
            .map(|c| geo_transform.coordinate_to_grid_idx_2d(*c))
            .filter(|idx| raster.grid_shape().contains(idx))
            .collect()
    }

    fn collection_ref(&self) -> &FeatureCollection<MultiPoint> {
        &self.collection
    }

    fn collection(self) -> FeatureCollection<MultiPoint> {
        self.collection
    }
}

pub struct MultiPolygonCoveredPixels {
    tester: PointInPolygonTester,
}

impl CoveredPixels<MultiPolygon> for MultiPolygonCoveredPixels {
    fn initialize(collection: FeatureCollection<MultiPolygon>) -> Self {
        Self {
            tester: PointInPolygonTester::new(collection), // TODO: parallelize
        }
    }

    fn covered_pixels<P: Pixel>(
        &self,
        feature_index: usize,
        raster: &RasterTile2D<P>,
    ) -> Vec<GridIdx2D> {
        let geo_transform = raster.tile_information().tile_geo_transform();

        let [height, width] = raster.grid_shape_array();

        let mut pixels = vec![];
        for row in 0..height {
            for col in 0..width {
                let idx = [row as isize, col as isize].into();
                let coordinate = geo_transform.grid_idx_to_upper_left_coordinate_2d(idx);

                if self
                    .tester
                    .is_coordinate_in_multi_polygon(coordinate, feature_index)
                {
                    pixels.push(dbg!(idx))
                }
            }
        }

        pixels
    }

    fn collection_ref(&self) -> &FeatureCollection<MultiPolygon> {
        &self.tester.polygons_ref()
    }

    fn collection(self) -> FeatureCollection<MultiPolygon> {
        self.tester.polygons()
    }
}

/// Creates a new calculator for for pixels covered by a given `feature_collection`'s geometries.
pub trait PixelCoverCreator<G: Geometry> {
    type C: CoveredPixels<G>;

    fn create_covered_pixels(self) -> Self::C;
}

impl PixelCoverCreator<MultiPoint> for FeatureCollection<MultiPoint> {
    type C = MultiPointCoveredPixels;

    fn create_covered_pixels(self) -> Self::C {
        MultiPointCoveredPixels::initialize(self)
    }
}

impl PixelCoverCreator<MultiPolygon> for FeatureCollection<MultiPolygon> {
    type C = MultiPolygonCoveredPixels;

    fn create_covered_pixels(self) -> Self::C {
        MultiPolygonCoveredPixels::initialize(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_spans() {
        let time_spans = FeatureTimeSpanIter::new(&[
            TimeInterval::new_unchecked(0, 4),
            TimeInterval::new_unchecked(2, 6),
            TimeInterval::new_unchecked(7, 9),
            TimeInterval::new_unchecked(9, 11),
            TimeInterval::new_unchecked(13, 14),
        ])
        .collect::<Vec<_>>();

        assert_eq!(
            time_spans,
            vec![
                FeatureTimeSpan {
                    feature_index_start: 0,
                    feature_index_end: 1,
                    time_interval: TimeInterval::new_unchecked(0, 6)
                },
                FeatureTimeSpan {
                    feature_index_start: 2,
                    feature_index_end: 3,
                    time_interval: TimeInterval::new_unchecked(7, 11)
                },
                FeatureTimeSpan {
                    feature_index_start: 4,
                    feature_index_end: 4,
                    time_interval: TimeInterval::new_unchecked(13, 14)
                }
            ]
        );
    }
}
