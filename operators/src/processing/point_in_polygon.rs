use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use snafu::ensure;

use geoengine_datatypes::collections::{
    MultiPointCollection, MultiPolygonCollection, VectorDataType,
};
use geoengine_datatypes::primitives::Coordinate2D;

use crate::engine::{
    ExecutionContext, InitializedOperator, InitializedOperatorImpl, InitializedVectorOperator,
    Operator, QueryContext, QueryProcessor, QueryRectangle, TypedVectorQueryProcessor,
    VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
};
use crate::error;
use crate::util::Result;
use arrow::array::BooleanArray;
use itertools::Itertools;

pub type PointInPolygonFilter = Operator<()>;

#[typetag::serde]
impl VectorOperator for PointInPolygonFilter {
    fn initialize(
        self: Box<Self>,
        context: &ExecutionContext,
    ) -> Result<Box<InitializedVectorOperator>> {
        ensure!(
            self.vector_sources.len() == 2,
            error::InvalidNumberOfVectorInputs {
                expected: 2..3,
                found: self.vector_sources.len()
            }
        );
        ensure!(
            self.raster_sources.is_empty(),
            error::InvalidNumberOfRasterInputs {
                expected: 0..1,
                found: self.raster_sources.len()
            }
        );

        let vector_sources = self
            .vector_sources
            .into_iter()
            .map(|o| o.initialize(context))
            .collect::<Result<Vec<Box<InitializedVectorOperator>>>>()?;

        ensure!(
            vector_sources[0].result_descriptor().data_type == VectorDataType::MultiPoint,
            error::InvalidType {
                expected: VectorDataType::MultiPoint.to_string(),
                found: vector_sources[0].result_descriptor().data_type.to_string(),
            }
        );
        ensure!(
            vector_sources[1].result_descriptor().data_type == VectorDataType::MultiPolygon,
            error::InvalidType {
                expected: VectorDataType::MultiPolygon.to_string(),
                found: vector_sources[1].result_descriptor().data_type.to_string(),
            }
        );

        Ok(InitializedPointInPolygonFilter::new(
            (),
            vector_sources[0].result_descriptor(),
            vec![],
            vector_sources,
            (),
        )
        .boxed())
    }
}

pub type InitializedPointInPolygonFilter = InitializedOperatorImpl<(), VectorResultDescriptor, ()>;

impl InitializedOperator<VectorResultDescriptor, TypedVectorQueryProcessor>
    for InitializedPointInPolygonFilter
{
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        let point_processor = if let TypedVectorQueryProcessor::MultiPoint(point_processor) =
            self.vector_sources[0].query_processor()?
        {
            point_processor
        } else {
            unreachable!("checked in `PointInPolygonFilter` constructor");
        };

        let polygon_processor = if let TypedVectorQueryProcessor::MultiPolygon(polygon_processor) =
            self.vector_sources[1].query_processor()?
        {
            polygon_processor
        } else {
            unreachable!("checked in `PointInPolygonFilter` constructor");
        };

        Ok(TypedVectorQueryProcessor::MultiPoint(
            PointInPolygonFilterProcessor::new(point_processor, polygon_processor).boxed(),
        ))
    }
}

pub struct PointInPolygonFilterProcessor {
    points: Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
    polygons: Box<dyn VectorQueryProcessor<VectorType = MultiPolygonCollection>>,
}

impl PointInPolygonFilterProcessor {
    pub fn new(
        points: Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
        polygons: Box<dyn VectorQueryProcessor<VectorType = MultiPolygonCollection>>,
    ) -> Self {
        Self { points, polygons }
    }

    fn filter_points(
        points: &MultiPointCollection,
        polygons: &MultiPolygonCollection,
        initial_filter: &BooleanArray,
    ) -> Result<BooleanArray> {
        let mut filter = Vec::with_capacity(points.len());

        let tester = PointInPolygonTester::new(polygons);

        let coordinates = points.coordinates();
        for (&coordinates_start_index, &coordinates_end_index) in
            points.multipoint_offsets().iter().tuple_windows()
        {
            let (coordinates_start_index, coordinates_end_index) = (
                coordinates_start_index as usize,
                coordinates_end_index as usize,
            );

            let is_multi_point_in_polygon_collection = coordinates
                [coordinates_start_index..coordinates_end_index]
                .iter()
                .any(|coordinate| tester.is_coordinate_in_collection(coordinate));

            filter.push(is_multi_point_in_polygon_collection);
        }

        arrow::compute::or(initial_filter, &filter.into()).map_err(Into::into)
    }
}

impl VectorQueryProcessor for PointInPolygonFilterProcessor {
    type VectorType = MultiPointCollection;

    fn vector_query(
        &self,
        query: QueryRectangle,
        ctx: QueryContext,
    ) -> BoxStream<'_, Result<Self::VectorType>> {
        // TODO: multi-threading

        self.points
            .query(query, ctx)
            .and_then(move |points| async move {
                let initial_filter = BooleanArray::from(vec![false; points.len()]);

                let filter = self
                    .polygons
                    .query(query, ctx)
                    .fold(Ok(initial_filter), |filter, polygons| async {
                        let polygons = polygons?;

                        if polygons.is_empty() {
                            return filter;
                        }

                        Self::filter_points(&points, &polygons, &filter?)
                    })
                    .await?;

                points.filter(filter).map_err(Into::into)
            })
            .boxed()
    }
}

/// Creates a context to check points against polygons
///
/// The algorithm is taken from <http://alienryderflex.com/polygon/>
///
struct PointInPolygonTester<'p> {
    polygons: &'p MultiPolygonCollection,
    constants: Vec<f64>,
    multiples: Vec<f64>,
}

impl<'p> PointInPolygonTester<'p> {
    pub fn new(polygons: &'p MultiPolygonCollection) -> Self {
        let number_of_coordinates = polygons.coordinates().len();

        let mut tester = Self {
            polygons,
            constants: vec![0.; number_of_coordinates],
            multiples: vec![0.; number_of_coordinates],
        };

        tester.precalculate_polygons();

        tester
    }

    fn precalculate_polygons(&mut self) {
        for (&ring_start_index, &ring_end_index) in
            self.polygons.ring_offsets().iter().tuple_windows()
        {
            self.precalculate_ring(ring_start_index as usize, ring_end_index as usize);
        }
    }

    fn precalculate_ring(&mut self, ring_start_index: usize, ring_end_index: usize) {
        let number_of_corners = ring_end_index - ring_start_index - 1;
        let mut j = number_of_corners - 1;

        let polygon_coordinates = self.polygons.coordinates();

        for i in 0..number_of_corners {
            let c_i = polygon_coordinates[ring_start_index + i];
            let c_j = polygon_coordinates[ring_start_index + j];

            let helper_array_index = ring_start_index + i;

            if float_cmp::approx_eq!(f64, c_j.y, c_i.y) {
                self.constants[helper_array_index] = c_i.x;
                self.multiples[helper_array_index] = 0.0;
            } else {
                self.constants[helper_array_index] =
                    c_i.x - (c_i.y * c_j.x) / (c_j.y - c_i.y) + (c_i.y * c_i.x) / (c_j.y - c_i.y);
                self.multiples[helper_array_index] = (c_j.x - c_i.x) / (c_j.y - c_i.y);
            }

            j = i;
        }
    }

    fn is_coordinate_in_ring(
        &self,
        coordinate: &Coordinate2D,
        ring_index_start: usize,
        ring_index_stop: usize,
    ) -> bool {
        let number_of_corners = ring_index_stop - ring_index_start - 1;
        let mut j = number_of_corners - 1;
        let mut odd_nodes = false;

        let polygon_coordinates = self.polygons.coordinates();

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

    /// Is the coordinate contained in any polygon of the collection?
    ///
    /// The function returns `true` if the `Coordinate2D` is inside the multi polygon, or
    /// `false` if it is not. If the point is exactly on the edge of the polygon,
    /// then the function may return `true` or `false`.
    ///
    /// TODO: check boundary conditions separately
    ///
    // TODO: check time validity
    pub fn is_coordinate_in_collection(&self, coordinate: &Coordinate2D) -> bool {
        let ring_offsets = self.polygons.ring_offsets();

        for (&polygon_start_index, &polygon_end_index) in
            self.polygons.polygon_offsets().iter().tuple_windows()
        {
            let polygon_start_index = polygon_start_index as usize;
            let polygon_end_index = polygon_end_index as usize;

            let mut is_coordinate_in_polygon = true;

            for (ring_number, (&ring_start_index, &ring_end_index)) in ring_offsets
                [polygon_start_index..=polygon_end_index]
                .iter()
                .tuple_windows()
                .enumerate()
            {
                let is_coordinate_in_ring = self.is_coordinate_in_ring(
                    coordinate,
                    ring_start_index as usize,
                    ring_end_index as usize,
                );

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

    // TODO: check time validity
    #[allow(dead_code)]
    pub fn multi_polygons_containing_coordinate(&self, coordinate: &Coordinate2D) -> Vec<bool> {
        let mut result = Vec::with_capacity(self.polygons.len());

        let polygon_offsets = self.polygons.polygon_offsets();
        let ring_offsets = self.polygons.ring_offsets();

        for (&multi_polygon_start_index, &multi_polygon_end_index) in
            self.polygons.multi_polygon_offsets().iter().tuple_windows()
        {
            let multi_polygon_start_index = multi_polygon_start_index as usize;
            let multi_polygon_end_index = multi_polygon_end_index as usize;

            let mut is_coordinate_in_multi_polygon = false;

            for (&polygon_start_index, &polygon_end_index) in polygon_offsets
                [multi_polygon_start_index..=multi_polygon_end_index]
                .iter()
                .tuple_windows()
            {
                let polygon_start_index = polygon_start_index as usize;
                let polygon_end_index = polygon_end_index as usize;

                let mut is_coordinate_in_polygon = true;

                for (ring_number, (&ring_start_index, &ring_end_index)) in ring_offsets
                    [polygon_start_index..=polygon_end_index]
                    .iter()
                    .tuple_windows()
                    .enumerate()
                {
                    let is_coordinate_in_ring = self.is_coordinate_in_ring(
                        coordinate,
                        ring_start_index as usize,
                        ring_end_index as usize,
                    );

                    if (ring_number == 0 && !is_coordinate_in_ring)
                        || (ring_number > 0 && is_coordinate_in_ring)
                    {
                        // coordinate is either "not in outer ring" or "in inner ring"
                        is_coordinate_in_polygon = false;
                        break;
                    }
                }

                if is_coordinate_in_polygon {
                    is_coordinate_in_multi_polygon = true;
                    break;
                }
            }

            result.push(is_coordinate_in_multi_polygon);
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::primitives::{
        BoundingBox2D, MultiPoint, MultiPolygon, SpatialResolution, TimeInterval,
    };

    use crate::mock::{MockFeatureCollectionSource, MockFeatureCollectionSourceParams};

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

        assert!(!tester.is_coordinate_in_ring(&Coordinate2D::new(4., 5.), 0, 5));
        assert!(tester.is_coordinate_in_ring(&Coordinate2D::new(4., 5.), 5, 10));
        assert!(!tester.is_coordinate_in_ring(&Coordinate2D::new(4., 5.), 10, 19));

        assert!(!tester.is_coordinate_in_ring(&Coordinate2D::new(4., 2.), 0, 5));
        assert!(tester.is_coordinate_in_ring(&Coordinate2D::new(4., 2.), 5, 10));
        assert!(tester.is_coordinate_in_ring(&Coordinate2D::new(4., 2.), 10, 19));

        assert!(tester.is_coordinate_in_collection(&Coordinate2D::new(4., 5.)));
        assert!(!tester.is_coordinate_in_collection(&Coordinate2D::new(4., 2.)));

        assert_eq!(
            tester.multi_polygons_containing_coordinate(&Coordinate2D::new(4., 5.)),
            vec![true]
        );
        assert_eq!(
            tester.multi_polygons_containing_coordinate(&Coordinate2D::new(4., 2.)),
            vec![false]
        );
    }

    #[test]
    fn point_in_polygon_boundary_conditions() {
        let collection = MultiPolygonCollection::from_data(
            vec![MultiPolygon::new(vec![vec![vec![
                (0.0, 0.0).into(),
                (10.0, 0.0).into(),
                (10.0, 10.0).into(),
                (0.0, 10.0).into(),
                (0.0, 0.0).into(),
            ]]])
            .unwrap()],
            vec![Default::default(); 1],
            Default::default(),
        )
        .unwrap();

        let tester = PointInPolygonTester::new(&collection);

        // the algorithm is not stable for boundary cases directly on the edges

        assert!(tester.is_coordinate_in_collection(&Coordinate2D::new(0.000_001, 0.000_001)));
        assert!(tester.is_coordinate_in_collection(&Coordinate2D::new(0.000_001, 0.1)));
        assert!(tester.is_coordinate_in_collection(&Coordinate2D::new(0.1, 0.000_001)));

        assert!(tester.is_coordinate_in_collection(&Coordinate2D::new(9.9, 9.9)));
        assert!(tester.is_coordinate_in_collection(&Coordinate2D::new(10.0, 9.9)));
        assert!(tester.is_coordinate_in_collection(&Coordinate2D::new(9.9, 10.0)));

        assert!(!tester.is_coordinate_in_collection(&Coordinate2D::new(-0.1, -0.1)));
        assert!(!tester.is_coordinate_in_collection(&Coordinate2D::new(0.0, -0.1)));
        assert!(!tester.is_coordinate_in_collection(&Coordinate2D::new(-0.1, 0.0)));

        assert!(!tester.is_coordinate_in_collection(&Coordinate2D::new(10.1, 10.1)));
        assert!(!tester.is_coordinate_in_collection(&Coordinate2D::new(10.1, 9.9)));
        assert!(!tester.is_coordinate_in_collection(&Coordinate2D::new(9.9, 10.1)));
    }

    #[tokio::test]
    async fn all() -> Result<()> {
        let points = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.001, 0.1), (1.0, 1.1), (2.0, 3.1)]).unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 3],
            Default::default(),
        )?;

        let point_source = MockFeatureCollectionSource {
            params: MockFeatureCollectionSourceParams {
                collection: points.clone(),
            },
        }
        .boxed();

        let polygon_source = MockFeatureCollectionSource {
            params: MockFeatureCollectionSourceParams {
                collection: MultiPolygonCollection::from_data(
                    vec![MultiPolygon::new(vec![vec![vec![
                        (0.0, 0.0).into(),
                        (10.0, 0.0).into(),
                        (10.0, 10.0).into(),
                        (0.0, 10.0).into(),
                        (0.0, 0.0).into(),
                    ]]])?],
                    vec![TimeInterval::new_unchecked(0, 1); 1],
                    Default::default(),
                )?,
            },
        }
        .boxed();

        let operator = PointInPolygonFilter {
            vector_sources: vec![point_source, polygon_source],
            raster_sources: vec![],
            params: (),
        }
        .boxed()
        .initialize(&ExecutionContext::mock_empty())?;

        let query_processor = operator.query_processor()?.multi_point().unwrap();

        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = QueryContext {
            chunk_byte_size: usize::MAX,
        };

        let query = query_processor.query(query_rectangle, ctx);

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        assert_eq!(result[0], points);

        Ok(())
    }

    #[tokio::test]
    async fn none() -> Result<()> {
        let points = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1), (2.0, 3.1)]).unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 3],
            Default::default(),
        )?;

        let point_source = MockFeatureCollectionSource {
            params: MockFeatureCollectionSourceParams {
                collection: points.clone(),
            },
        }
        .boxed();

        let polygon_source = MockFeatureCollectionSource {
            params: MockFeatureCollectionSourceParams {
                collection: MultiPolygonCollection::from_data(vec![], vec![], Default::default())?,
            },
        }
        .boxed();

        let operator = PointInPolygonFilter {
            vector_sources: vec![point_source, polygon_source],
            raster_sources: vec![],
            params: (),
        }
        .boxed()
        .initialize(&ExecutionContext::mock_empty())?;

        let query_processor = operator.query_processor()?.multi_point().unwrap();

        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = QueryContext {
            chunk_byte_size: usize::MAX,
        };

        let query = query_processor.query(query_rectangle, ctx);

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        assert_eq!(result[0], MultiPointCollection::empty());

        Ok(())
    }
}
