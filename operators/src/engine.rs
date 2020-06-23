use geoengine_datatypes::primitives::{TimeInterval, BoundingBox2D};
use crate::util::Result;
use futures::stream::BoxStream;
use crate::Operator;
use geoengine_datatypes::collections::MultiPointCollection;
use geoengine_datatypes::raster::{Raster, GenericRaster};
use crate::mock::source::mock_point_source::MockPointSourceImpl;
use crate::error::Error;
use crate::mock::processing::delay::MockDelayImpl;
use crate::mock::source::mock_raster_source::MockRasterSourceImpl;
use crate::mock::processing::raster_points::MockRasterPointsImpl;

//pub(crate) const NO_SOURCES: &[usize] = &[];

// TODO: opterators returning T don' necessarily have sources of type T
pub trait OperatorImpl<T>: QueryProcessor<T> + HasSources<T> {}

// pub enum QueryResult {
//     Points(MultiPointCollection),
//     Lines(MultiLineCollection),
//     Polygons(MultiPolygonCollection),
//     // TODO: raster
// }

pub trait QueryResult {}

impl QueryResult for MultiPointCollection {}
// impl QueryResult for Raster<>


#[derive(Copy, Clone)]
pub struct Query {
    pub bbox: BoundingBox2D,
    pub time_interval: TimeInterval,
}

#[derive(Copy, Clone)]
pub struct QueryContext {
    // TODO: resolution, profiler, user session, ...
    // TODO: determine chunk size globally or dynamically from workload? Or global Engine Manager instance that gives that info
    pub chunk_byte_size: usize
}

pub trait QueryProcessor<T: ?Sized>: Sync {
    fn query(&self, query: Query, ctx: QueryContext) -> BoxStream<Result<Box<T>>>;
}

pub trait HasSources<T> {
    fn sources(self) -> Vec<Box<dyn QueryProcessor<T>>>;
}


// TODO: single factory method for creating all kind of query processors to avoid redundant code
fn point_processor(operator: &Operator) -> Result<Box<dyn QueryProcessor<MultiPointCollection>>> {
    match operator {
        // Operator::MockPointSource { params, sources } => Box::new(params.into()),
        Operator::MockPointSource { params, sources: _ } => {
            Ok(Box::new(MockPointSourceImpl {
                points: params.points.clone()
            }))
        },
        Operator::MockDelay { params, sources } => {
            Ok(Box::new(MockDelayImpl {
                // TODO: generic method (macro?) for creating source operators
                points: create_sources(&sources.points, point_processor)?,
                seconds: params.seconds
            }))
        },
        Operator::MockRasterPoints { params, sources } => {
            Ok(Box::new(MockRasterPointsImpl {
                points: create_sources(&sources.points, point_processor)?,
                rasters: vec![],//create_sources(&sources.rasters, raster_processor)?,
                coords: params.coords
            }))
        },
        _ => Err(Error::QueryProcessor)
    }
}

fn create_sources<T>(sources: &Vec<Operator>, x: fn(&Operator) -> Result<Box<dyn QueryProcessor<T>>, Error>) -> Result<Vec<Box<dyn QueryProcessor<T>>>> {
    sources.iter().map(|p| Ok(x(p)?)).collect::<Result<Vec<Box<dyn QueryProcessor<T>>>>>()
}

fn raster_processor<T>(operator: &Operator) -> Result<Box<dyn QueryProcessor<dyn GenericRaster>>> {
    match operator {
        Operator::MockRasterSource { params, sources: _ } => {
            Ok(Box::new(MockRasterSourceImpl {
                data: params.data.clone(),
                dim: params.dim,
                geo_transform: params.geo_transform
            }))
        },
        _ => Err(Error::QueryProcessor)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use geoengine_datatypes::primitives::Coordinate2D;
    use futures::StreamExt;

    #[tokio::test]
    async fn test() {
        let json_string = r#"{
            "type": "mock_delay",
            "params": {
                "seconds": 5
            },
            "sources": {
                "points": [{
                    "type": "mock_point_source",
                    "params": {
                        "points": [
                            [1, 2],
                            [3, 4]
                        ]
                    },
                    "sources": []
                }]
            }
        }"#;

        let operator: Operator = serde_json::from_str(json_string).unwrap();

        let op_impl = point_processor(&operator).unwrap();

        let query = Query {
            bbox: BoundingBox2D::new_unchecked(Coordinate2D::new(1., 2.), Coordinate2D::new(1., 2.)),
            time_interval: TimeInterval::new_unchecked(0, 1),
        };
        let ctx = QueryContext {
            chunk_byte_size: 10 * 8 * 2
        };

        op_impl.query(query, ctx).for_each(|x| {
            println!("{:?}", x);
            futures::future::ready(())
        }).await;
    }
}
