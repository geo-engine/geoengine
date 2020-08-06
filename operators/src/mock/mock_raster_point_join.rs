use crate::engine::{
    Operator, QueryProcessor, RasterOperator, RasterQueryProcessor, TypedVectorQueryProcessor,
    VectorOperator, VectorQueryProcessor,
};
use futures::StreamExt;
use geoengine_datatypes::{
    collections::{FeatureCollection, MultiPointCollection},
    primitives::FeatureData,
    raster::{GridPixelAccess, Raster2D},
};
use serde::{Deserialize, Serialize};

pub struct MockRasterPointJoinProcessor<R, V> {
    raster_source: R,
    point_source: V,
    feature_name: String,
}

impl<R, V> MockRasterPointJoinProcessor<R, V> {
    pub fn new(raster_source: R, point_source: V, params: MockRasterPointJoinParams) -> Self {
        Self {
            raster_source,
            point_source,
            feature_name: params.feature_name,
        }
    }
}

impl<R, V, T> QueryProcessor for MockRasterPointJoinProcessor<R, V>
where
    R: RasterQueryProcessor<RasterType = T> + Sync,
    T: std::fmt::Debug + Into<f64> + Sync + Copy,
    V: VectorQueryProcessor<VectorType = MultiPointCollection> + Sync,
{
    type Output = MultiPointCollection;
    fn query(
        &self,
        query: crate::engine::QueryRectangle,
        ctx: crate::engine::QueryContext,
    ) -> futures::stream::BoxStream<crate::util::Result<Self::Output>> {
        let point_stream = self.point_source.vector_query(query, ctx);
        point_stream
            .then(async move |collection| {
                let collection = collection?;
                let mut raster_stream = self.raster_source.raster_query(query, ctx);
                let raster_future = raster_stream.next().await;
                let raster: Raster2D<T> =
                    raster_future.ok_or(crate::error::Error::QueryProcessor)??;
                let pixel = raster.pixel_value_at_grid_index(&(0, 0))?;
                let collection = collection.add_column(
                    &self.feature_name,
                    FeatureData::Number(vec![pixel.into(); collection.len()]),
                )?;
                Ok(collection)
            })
            .boxed()
    }
}

impl MockRasterPointJoinOperator {
    fn create_binary<T1>(
        source_a: Box<dyn RasterQueryProcessor<RasterType = T1>>,
        source_b: Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
        params: MockRasterPointJoinParams,
    ) -> MockRasterPointJoinProcessor<
        Box<dyn RasterQueryProcessor<RasterType = T1>>,
        Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
    >
    where
        T1: Copy + Sync + 'static,
    {
        MockRasterPointJoinProcessor::new(source_a, source_b, params)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MockRasterPointJoinParams {
    pub feature_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MockRasterPointJoinOperator {
    raster_sources: Vec<Box<dyn RasterOperator>>,
    point_sources: Vec<Box<dyn VectorOperator>>,
    params: MockRasterPointJoinParams,
}

impl Operator for MockRasterPointJoinOperator {
    fn raster_sources(&self) -> &[Box<dyn RasterOperator>] {
        &self.raster_sources
    }
    fn vector_sources(&self) -> &[Box<dyn VectorOperator>] {
        &self.point_sources
    }
}

#[typetag::serde]
impl VectorOperator for MockRasterPointJoinOperator {
    fn vector_query_processor(&self) -> crate::engine::TypedVectorQueryProcessor {
        let raster_source = self.raster_sources[0].create_raster_op();
        let point_source = match self.point_sources[0].vector_query_processor() {
            TypedVectorQueryProcessor::MultiPoint(v) => v,
            _ => panic!(),
        };
        TypedVectorQueryProcessor::MultiPoint(match raster_source {
            crate::engine::TypedRasterQueryProcessor::U8(r) => Box::new(Self::create_binary::<u8>(
                r,
                point_source,
                self.params.clone(),
            )),
            _ => panic!(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        engine::{QueryContext, QueryRectangle},
        mock::{MockPointSource, MockRasterSource},
    };
    use futures::executor::block_on_stream;
    use geoengine_datatypes::{
        primitives::{BoundingBox2D, Coordinate2D, FeatureDataRef, TimeInterval},
        raster::RasterDataType,
    };

    #[test]
    fn serde() {
        let points = vec![Coordinate2D::new(1., 2.); 3];
        let mps = MockPointSource { points }.boxed();

        let raster = Raster2D::new(
            [3, 2].into(),
            vec![1, 2, 3, 4, 5, 6],
            None,
            Default::default(),
            Default::default(),
        )
        .unwrap();

        let mrs = MockRasterSource {
            data: vec![raster],
            raster_type: RasterDataType::U8,
        }
        .boxed();

        let params = MockRasterPointJoinParams {
            feature_name: "raster_values".to_string(),
        };
        let op = MockRasterPointJoinOperator {
            params,
            raster_sources: vec![mrs],
            point_sources: vec![mps],
        }
        .boxed();

        let serialized = serde_json::to_string(&op).unwrap();
        dbg!(&serialized);
        let expected = "{\"type\":\"MockRasterPointJoinOperator\",\"raster_sources\":[{\"type\":\"MockRasterSource\",\"data\":[{\"grid_dimension\":{\"dimension_size\":[3,2]},\"data_container\":[1,2,3,4,5,6],\"no_data_value\":null,\"geo_transform\":{\"upper_left_coordinate\":{\"x\":0.0,\"y\":0.0},\"x_pixel_size\":1.0,\"y_pixel_size\":-1.0},\"temporal_bounds\":{\"start\":-9223372036854775808,\"end\":9223372036854775807}}],\"raster_type\":\"U8\"}],\"point_sources\":[{\"type\":\"MockPointSource\",\"points\":[{\"x\":1.0,\"y\":2.0},{\"x\":1.0,\"y\":2.0},{\"x\":1.0,\"y\":2.0}]}],\"params\":{\"feature_name\":\"raster_values\"}}";
        assert_eq!(serialized, expected);
        let _: Box<dyn VectorOperator> = serde_json::from_str(&serialized).unwrap();
    }
    #[test]
    fn execute() {
        let points = vec![Coordinate2D::new(1., 2.); 3];
        let mps = MockPointSource { points }.boxed();

        let raster = Raster2D::new(
            [3, 2].into(),
            vec![1, 2, 3, 4, 5, 6],
            None,
            Default::default(),
            Default::default(),
        )
        .unwrap();

        let mrs = MockRasterSource {
            data: vec![raster],
            raster_type: RasterDataType::U8,
        }
        .boxed();
        let new_column_name = "raster_values".to_string();
        let params = MockRasterPointJoinParams {
            feature_name: new_column_name.clone(),
        };
        let op = MockRasterPointJoinOperator {
            params,
            raster_sources: vec![mrs],
            point_sources: vec![mps],
        }
        .boxed();

        let point_processor = match op.vector_query_processor() {
            TypedVectorQueryProcessor::MultiPoint(processor) => processor,
            _ => panic!(),
        };

        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new((0., 0.).into(), (4., 4.).into()).unwrap(),
            time_interval: TimeInterval::default(),
        };
        let ctx = QueryContext {
            chunk_byte_size: 2 * std::mem::size_of::<Coordinate2D>(),
        };
        let stream = point_processor.vector_query(query_rectangle, ctx);

        let blocking_stream = block_on_stream(stream);
        let collections: Vec<MultiPointCollection> = blocking_stream.map(Result::unwrap).collect();
        assert_eq!(collections.len(), 2);
        assert_eq!(collections[0].len(), 2);
        assert_eq!(collections[1].len(), 1);

        let column = collections[0].data(&new_column_name).unwrap();
        let numbers = if let FeatureDataRef::Number(numbers) = column {
            numbers
        } else {
            panic!()
        };

        assert_eq!(numbers.as_ref(), &[1.0, 1.0]);

        let column = collections[1].data(&new_column_name).unwrap();
        let numbers = if let FeatureDataRef::Number(numbers) = column {
            numbers
        } else {
            panic!()
        };

        assert_eq!(numbers.as_ref(), &[1.0]);
    }
}
