use crate::engine::{
    Operator, QueryProcessor, RasterOperator, RasterQueryProcessor, TypedRasterQueryProcessor,
    VectorOperator,
};
use crate::util::Result;
use futures::StreamExt;
use geoengine_datatypes::raster::Pixel;
use geoengine_datatypes::{
    projection::ProjectionOption,
    raster::{RasterClassification, RasterDataType, RasterTile2D},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct RasterClassificationParams {
    bounds_class: Vec<(f64, f64, u32)>,
    no_data_class: u32,
}

pub struct RasterClassificationProcessor<R> {
    raster_source: R,
    params: RasterClassificationParams,
}

impl<R> RasterClassificationProcessor<R> {
    pub fn new(raster_source: R, params: RasterClassificationParams) -> Self {
        Self {
            raster_source,
            params,
        }
    }
}

impl<R, T> QueryProcessor for RasterClassificationProcessor<R>
where
    R: RasterQueryProcessor<RasterType = T> + Sync,
    T: Pixel,
{
    type Output = RasterTile2D<u32>;
    fn query(
        &self,
        query: crate::engine::QueryRectangle,
        ctx: crate::engine::QueryContext,
    ) -> futures::stream::BoxStream<crate::util::Result<Self::Output>> {
        self.raster_source
            .raster_query(query, ctx)
            .map(move |raster_tile_result| {
                raster_tile_result.map(|tile| {
                    let class_result = tile
                        .data
                        .classification(&self.params.bounds_class, self.params.no_data_class)
                        .unwrap(); // TODO move to future? handle error?

                    RasterTile2D::new(tile.time, tile.tile, class_result)
                })
            })
            .boxed()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RasterClassificationOperator {
    pub params: RasterClassificationParams,
    pub raster_sources: Vec<Box<dyn RasterOperator>>,
}

impl Operator for RasterClassificationOperator {
    fn raster_sources(&self) -> &[Box<dyn RasterOperator>] {
        self.raster_sources.as_slice() // no sources!
    }
    fn vector_sources(&self) -> &[Box<dyn VectorOperator>] {
        &[]
    }
    fn projection(&self) -> ProjectionOption {
        self.raster_sources[0].projection()
    }
}

#[typetag::serde]
impl RasterOperator for RasterClassificationOperator {
    fn raster_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source = self.raster_sources.get(0).unwrap().raster_processor()?; // TODO: resolve error
        let processor = match source {
            TypedRasterQueryProcessor::U8(p) => {
                RasterClassificationProcessor::new(p, self.params.clone()).boxed()
            }
            TypedRasterQueryProcessor::U16(p) => {
                RasterClassificationProcessor::new(p, self.params.clone()).boxed()
            }
            TypedRasterQueryProcessor::U32(p) => {
                RasterClassificationProcessor::new(p, self.params.clone()).boxed()
            }
            TypedRasterQueryProcessor::U64(p) => {
                RasterClassificationProcessor::new(p, self.params.clone()).boxed()
            }
            TypedRasterQueryProcessor::I8(p) => {
                RasterClassificationProcessor::new(p, self.params.clone()).boxed()
            }
            TypedRasterQueryProcessor::I16(p) => {
                RasterClassificationProcessor::new(p, self.params.clone()).boxed()
            }
            TypedRasterQueryProcessor::I32(p) => {
                RasterClassificationProcessor::new(p, self.params.clone()).boxed()
            }
            TypedRasterQueryProcessor::I64(p) => {
                RasterClassificationProcessor::new(p, self.params.clone()).boxed()
            }
            TypedRasterQueryProcessor::F32(p) => {
                RasterClassificationProcessor::new(p, self.params.clone()).boxed()
            }
            TypedRasterQueryProcessor::F64(p) => {
                RasterClassificationProcessor::new(p, self.params.clone()).boxed()
            }
        };
        Ok(TypedRasterQueryProcessor::U32(processor))
    }
    fn result_type(&self) -> RasterDataType {
        RasterDataType::U32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        engine::{QueryContext, QueryRectangle},
        mock::MockRasterSource,
    };
    use futures::executor::block_on_stream;
    use geoengine_datatypes::{
        primitives::{BoundingBox2D, Coordinate2D, TimeInterval},
        raster::{Raster2D, TileInformation},
    };

    #[test]
    fn classify_raster() {
        let dim = [3, 2];
        let data = vec![1, 2, 3, 4, 5, 6];
        let geo_transform = [1.0, 1.0, 0.0, 1.0, 0.0, 1.0];
        let temporal_bounds: TimeInterval = TimeInterval::default();
        let raster2d = Raster2D::new(
            dim.into(),
            data,
            None,
            temporal_bounds,
            geo_transform.into(),
        )
        .unwrap();

        let raster_tile = RasterTile2D {
            time: TimeInterval::default(),
            tile: TileInformation {
                geo_transform: Default::default(),
                global_pixel_position: [0, 0].into(),
                global_size_in_tiles: [1, 2].into(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            data: raster2d,
        };

        let mrs = MockRasterSource {
            data: vec![raster_tile],
            raster_type: RasterDataType::F32,
        }
        .boxed();

        let classification_def = vec![(1.0, 2.0, 2), (4.0, 5.0, 3)];

        let classification_operator = RasterClassificationOperator {
            params: RasterClassificationParams {
                bounds_class: classification_def,
                no_data_class: 0,
            },
            raster_sources: vec![mrs],
        }
        .boxed();

        let processor = classification_operator.raster_processor().unwrap();

        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new((0., 0.).into(), (1., 1.).into()).unwrap(),
            time_interval: TimeInterval::default(),
        };
        let ctx = QueryContext {
            chunk_byte_size: 69 * std::mem::size_of::<Coordinate2D>(),
        };

        let typed_processor = processor.get_u32().unwrap();

        let stream = typed_processor.query(query_rectangle, ctx);

        let blocking_stream = block_on_stream(stream);

        let result_collection: Vec<RasterTile2D<u32>> =
            blocking_stream.map(Result::unwrap).collect();

        let classfied_raster_tile = &result_collection[0];

        assert_eq!(
            classfied_raster_tile.data.data_container,
            vec![2, 2, 0, 3, 3, 0,]
        )
    }
}
