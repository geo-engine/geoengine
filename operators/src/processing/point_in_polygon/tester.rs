use geoengine_datatypes::{
    collections::{FeatureCollectionInfos, GeometryCollection, MultiPolygonCollection},
    primitives::{Coordinate2D, TimeInterval},
};

/// Creates a context to check points against polygons
///
/// The algorithm is taken from <http://alienryderflex.com/polygon/>
///
pub struct PointInPolygonTester<'a> {
    feature_offsets: &'a [i32],
    polygon_offsets: &'a [i32],
    ring_offsets: &'a [i32],
    coordinates: &'a [Coordinate2D],
    time_intervals: &'a [TimeInterval],
    constants: Vec<f64>,
    multiples: Vec<f64>,
}

impl<'a> PointInPolygonTester<'a> {
    pub fn new(polygons: &'a MultiPolygonCollection) -> Self {
        let feature_offsets = polygons.feature_offsets();
        let polygon_offsets = polygons.polygon_offsets();
        let ring_offsets = polygons.ring_offsets();
        let coordinates = polygons.coordinates();
        let time_intervals = polygons.time_intervals();

        let (constants, multiples) = Self::precalculate_polygons(ring_offsets, coordinates);

        Self {
            feature_offsets,
            polygon_offsets,
            ring_offsets,
            coordinates,
            time_intervals,
            constants,
            multiples,
        }
    }

    fn precalculate_polygons(
        ring_offsets: &'a [i32],
        coordinates: &'a [Coordinate2D],
    ) -> (Vec<f64>, Vec<f64>) {
        let num_coords = coordinates.len();
        let mut constants = vec![0.; num_coords];
        let mut multiples = vec![0.; num_coords];

        for (ring_start_index, ring_end_index) in
            two_tuple_windows(ring_offsets.iter().map(|&c| c as usize))
        {
            Self::precalculate_ring(
                ring_start_index,
                ring_end_index,
                coordinates,
                &mut constants,
                &mut multiples,
            );
        }

        (constants, multiples)
    }

    fn precalculate_ring(
        ring_start_index: usize,
        ring_end_index: usize,
        polygon_coordinates: &[Coordinate2D],
        constants: &mut Vec<f64>,
        multiples: &mut Vec<f64>,
    ) {
        let number_of_corners = ring_end_index - ring_start_index - 1;
        let mut j = number_of_corners - 1;

        for i in 0..number_of_corners {
            let c_i = polygon_coordinates[ring_start_index + i];
            let c_j = polygon_coordinates[ring_start_index + j];

            let helper_array_index = ring_start_index + i;

            if float_cmp::approx_eq!(f64, c_j.y, c_i.y) {
                constants[helper_array_index] = c_i.x;
                multiples[helper_array_index] = 0.0;
            } else {
                constants[helper_array_index] =
                    c_i.x - (c_i.y * c_j.x) / (c_j.y - c_i.y) + (c_i.y * c_i.x) / (c_j.y - c_i.y);
                multiples[helper_array_index] = (c_j.x - c_i.x) / (c_j.y - c_i.y);
            }

            j = i;
        }
    }

    fn ring_contains_coordinate(
        &self,
        coordinate: &Coordinate2D,
        ring_index_start: usize,
        ring_index_stop: usize,
    ) -> bool {
        let number_of_corners = ring_index_stop - ring_index_start - 1;
        let mut j = number_of_corners - 1;
        let mut odd_nodes = false;

        let polygon_coordinates = self.coordinates;

        for i in 0..number_of_corners {
            let c_i = polygon_coordinates[ring_index_start + i];
            let c_j = polygon_coordinates[ring_index_start + j];

            if (c_i.y < coordinate.y && c_j.y >= coordinate.y)
                || (c_j.y < coordinate.y && c_i.y >= coordinate.y)
            {
                let coordinate_index = ring_index_start + i;

                odd_nodes ^= coordinate.y * self.multiples[coordinate_index]
                    + self.constants[coordinate_index]
                    < coordinate.x;
            }

            j = i;
        }

        odd_nodes
    }

    pub fn multi_polygon_contains_coordinate(
        &self,
        coordinate: Coordinate2D,
        feature_index: usize,
    ) -> bool {
        let polygon_offsets = self.polygon_offsets;
        let ring_offsets = self.ring_offsets;

        self.check_multipolygons_contain_coordinate(
            &coordinate,
            polygon_offsets,
            ring_offsets,
            feature_index,
            feature_index + 1,
        )
    }

    fn check_multipolygons_contain_coordinate(
        &self,
        coordinate: &Coordinate2D,
        polygon_offsets: &[i32],
        ring_offsets: &[i32],
        multi_polygon_start_index: usize,
        multi_polygon_end_index: usize,
    ) -> bool {
        for (polygon_start_index, polygon_end_index) in two_tuple_windows(
            polygon_offsets[multi_polygon_start_index..=multi_polygon_end_index]
                .iter()
                .map(|&c| c as usize),
        ) {
            let mut is_coordinate_in_polygon = true;

            for (ring_number, (ring_start_index, ring_end_index)) in two_tuple_windows(
                ring_offsets[polygon_start_index..=polygon_end_index]
                    .iter()
                    .map(|&c| c as usize),
            )
            .enumerate()
            {
                let is_coordinate_in_ring =
                    self.ring_contains_coordinate(coordinate, ring_start_index, ring_end_index);

                if (ring_number == 0 && !is_coordinate_in_ring)
                    || (ring_number > 0 && is_coordinate_in_ring)
                {
                    // coordinate is either "not in outer ring" or "in inner ring"
                    is_coordinate_in_polygon = false;
                    break;
                }
            }

            if is_coordinate_in_polygon {
                return true;
            }
        }

        false
    }

    fn multi_polygon_contains_coordinate_iter<'p>(
        &'p self,
        coordinate: &'p Coordinate2D,
        time_interval: &'p TimeInterval,
    ) -> impl Iterator<Item = bool> + 'p {
        let polygon_offsets = self.polygon_offsets;
        let ring_offsets = self.ring_offsets;

        let time_intervals = self.time_intervals;

        two_tuple_windows(self.feature_offsets.iter().map(|&c| c as usize))
            .zip(time_intervals)
            .map(
                move |(
                    (multi_polygon_start_index, multi_polygon_end_index),
                    multi_polygon_time_interval,
                )| {
                    if !multi_polygon_time_interval.intersects(time_interval) {
                        return false;
                    }

                    self.check_multipolygons_contain_coordinate(
                        coordinate,
                        polygon_offsets,
                        ring_offsets,
                        multi_polygon_start_index,
                        multi_polygon_end_index,
                    )
                },
            )
    }

    /// Is the coordinate contained in any polygon of the collection?
    ///
    /// The function returns `true` if the `Coordinate2D` is inside the multi polygon, or
    /// `false` if it is not. If the point is exactly on the edge of the polygon,
    /// then the function may return `true` or `false`.
    ///
    /// TODO: check boundary conditions separately
    ///
    pub fn any_polygon_contains_coordinate(
        &self,
        coordinate: &Coordinate2D,
        time_interval: &TimeInterval,
    ) -> bool {
        self.multi_polygon_contains_coordinate_iter(coordinate, time_interval)
            .any(std::convert::identity)
    }

    pub fn multi_polygons_containing_coordinate(
        &self,
        coordinate: &Coordinate2D,
        time_interval: &TimeInterval,
    ) -> Vec<bool> {
        self.multi_polygon_contains_coordinate_iter(coordinate, time_interval)
            .collect()
    }
}

/// Loop through an iterator by yielding the current and previous tuple. Starts with the
/// (first, second) item, so the iterator must have more than one item to create an output.
fn two_tuple_windows<I, T>(mut iter: I) -> impl Iterator<Item = (T, T)>
where
    I: Iterator<Item = T>,
    T: Copy,
{
    let mut last = iter.next();

    iter.map(move |item| {
        let output = (last.unwrap(), item);
        last = Some(item);
        output
    })
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::primitives::MultiPolygon;

    use super::*;

    #[test]
    fn point_in_polygon_tester() {
        let collection = MultiPolygonCollection::from_data(
            vec![MultiPolygon::new(vec![
                vec![vec![
                    Coordinate2D::new(20., 20.),
                    Coordinate2D::new(30., 20.),
                    Coordinate2D::new(30., 30.),
                    Coordinate2D::new(20., 30.),
                    Coordinate2D::new(20., 20.),
                ]],
                vec![
                    vec![
                        Coordinate2D::new(0., 0.),
                        Coordinate2D::new(10., 0.),
                        Coordinate2D::new(10., 10.),
                        Coordinate2D::new(0., 10.),
                        Coordinate2D::new(0., 0.),
                    ],
                    vec![
                        Coordinate2D::new(1., 5.),
                        Coordinate2D::new(3., 3.),
                        Coordinate2D::new(5., 3.),
                        Coordinate2D::new(6., 5.),
                        Coordinate2D::new(7., 1.5),
                        Coordinate2D::new(4., 0.),
                        Coordinate2D::new(2., 1.),
                        Coordinate2D::new(1., 3.),
                        Coordinate2D::new(1., 5.),
                    ],
                ],
            ])
            .unwrap()],
            vec![Default::default(); 1],
            Default::default(),
        )
        .unwrap();

        let tester = PointInPolygonTester::new(&collection);

        assert!(!tester.ring_contains_coordinate(&Coordinate2D::new(4., 5.), 0, 5));
        assert!(tester.ring_contains_coordinate(&Coordinate2D::new(4., 5.), 5, 10));
        assert!(!tester.ring_contains_coordinate(&Coordinate2D::new(4., 5.), 10, 19));

        assert!(!tester.ring_contains_coordinate(&Coordinate2D::new(4., 2.), 0, 5));
        assert!(tester.ring_contains_coordinate(&Coordinate2D::new(4., 2.), 5, 10));
        assert!(tester.ring_contains_coordinate(&Coordinate2D::new(4., 2.), 10, 19));

        assert!(
            tester.any_polygon_contains_coordinate(&Coordinate2D::new(4., 5.), &Default::default())
        );
        assert!(!tester
            .any_polygon_contains_coordinate(&Coordinate2D::new(4., 2.), &Default::default()),);

        assert_eq!(
            tester.multi_polygons_containing_coordinate(
                &Coordinate2D::new(4., 5.),
                &Default::default()
            ),
            vec![true]
        );
        assert_eq!(
            tester.multi_polygons_containing_coordinate(
                &Coordinate2D::new(4., 2.),
                &Default::default()
            ),
            vec![false]
        );
    }
}
