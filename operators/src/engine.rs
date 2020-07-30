use futures::stream::BoxStream;

use geoengine_datatypes::collections::MultiPointCollection;
use geoengine_datatypes::primitives::{BoundingBox2D, TimeInterval};

use crate::error::Error;
use crate::mock::processing::delay::MockDelayImpl;
use crate::mock::processing::raster_points::MockRasterPointsImpl;
use crate::mock::source::mock_point_source::MockPointSourceImpl;
use crate::mock::source::mock_raster_source::MockRasterSourceImpl;
use crate::source::gdal_source::{JsonDatasetInformationProvider, RasterTile2D};
use crate::source::GdalSource;
use crate::util::Result;
use crate::Operator;

#[derive(Copy, Clone)]
pub struct QueryRectangle {
    pub bbox: BoundingBox2D,
    pub time_interval: TimeInterval,
}

#[derive(Copy, Clone)]
pub struct QueryContext {
    // TODO: resolution, profiler, user session, ...
    // TODO: determine chunk size globally or dynamically from workload? Or global Engine Manager instance that gives that info
    pub chunk_byte_size: usize,
}

pub trait QueryProcessor<T: Send>: Send + Sync {
    fn query(&self, query: QueryRectangle, ctx: QueryContext) -> BoxStream<Result<T>>;
}

pub enum QueryProcessorType {
    PointProcessor(Box<dyn QueryProcessor<MultiPointCollection>>),
    RasterProcessor(Box<dyn QueryProcessor<RasterTile2D>>),
}

impl QueryProcessorType {
    // TODO: TryInto?
    pub fn point_processor(self) -> Result<Box<dyn QueryProcessor<MultiPointCollection>>> {
        if let QueryProcessorType::PointProcessor(p) = self {
            Ok(p)
        } else {
            Err(crate::error::Error::QueryProcessor)
        }
    }

    pub fn raster_processor(self) -> Result<Box<dyn QueryProcessor<RasterTile2D>>> {
        if let QueryProcessorType::RasterProcessor(p) = self {
            Ok(p)
        } else {
            Err(crate::error::Error::QueryProcessor)
        }
    }
}

fn create_sources<T: ?Sized, F: Copy>(
    sources: &[Operator],
    type_extractor: F,
) -> Result<Vec<Box<dyn QueryProcessor<T>>>>
where
    F: FnOnce(QueryProcessorType) -> Result<Box<dyn QueryProcessor<T>>>,
{
    sources
        .iter()
        .map(|o| processor(o).and_then(type_extractor))
        .collect()
}

pub fn processor(operator: &Operator) -> Result<QueryProcessorType> {
    // TODO: handle operators that work on multiple types
    Ok(match operator {
        Operator::MockRasterSource { params, sources: _ } => {
            QueryProcessorType::RasterProcessor(Box::new(MockRasterSourceImpl {
                data: params.data.clone(),
                dim: params.dim,
                geo_transform: params.geo_transform,
            }))
        }
        Operator::MockPointSource { params, sources: _ } => {
            QueryProcessorType::PointProcessor(Box::new(MockPointSourceImpl {
                points: params.points.clone(),
            }))
        }
        Operator::MockDelay { params, sources } => {
            QueryProcessorType::PointProcessor(Box::new(MockDelayImpl {
                points: create_sources(&sources.points, QueryProcessorType::point_processor)?,
                seconds: params.seconds,
            }))
        }
        Operator::MockRasterPoints { params, sources } => {
            QueryProcessorType::PointProcessor(Box::new(MockRasterPointsImpl {
                points: create_sources(&sources.points, QueryProcessorType::point_processor)?,
                rasters: create_sources(&sources.rasters, QueryProcessorType::raster_processor)?,
                coords: params.coords,
            }))
        }
        Operator::GdalSource { params, sources: _ } => {
            QueryProcessorType::RasterProcessor(Box::new(GdalSource::<
                JsonDatasetInformationProvider,
            >::from_params(
                params.clone()
            )?))
        }
        // Operator::CsvSource { params, .. } => {
        //     QueryProcessorType::PointProcessor(Box::new(CsvSourceProcessor { params: params.clone() }))
        // }
        _ => return Err(Error::QueryProcessor),
    })
}

#[cfg(test)]
mod test {
    use futures::StreamExt;

    use geoengine_datatypes::primitives::Coordinate2D;

    use super::*;

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

        let op_impl = processor(&operator);

        if let Ok(QueryProcessorType::PointProcessor(op_impl)) = op_impl {
            let query = QueryRectangle {
                bbox: BoundingBox2D::new_unchecked(
                    Coordinate2D::new(1., 2.),
                    Coordinate2D::new(1., 2.),
                ),
                time_interval: TimeInterval::new_unchecked(0, 1),
            };
            let ctx = QueryContext {
                chunk_byte_size: 10 * 8 * 2,
            };

            op_impl
                .query(query, ctx)
                .for_each(|pc| {
                    println!("{:?}", pc);
                    futures::future::ready(())
                })
                .await;
        }
    }
}
