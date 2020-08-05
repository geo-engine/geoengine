use crate::engine_old::{QueryContext, QueryProcessor, QueryRectangle};
use crate::source::gdal_source::{RasterTile2D, TileInformation};
use crate::util::Result;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::primitives::TimeInterval;
use geoengine_datatypes::raster::{Raster2D, TypedRaster2D};

pub struct MockRasterSourceImpl {
    pub data: Vec<f64>,
    pub dim: [usize; 2],
    pub geo_transform: [f64; 6],
}

impl QueryProcessor<RasterTile2D> for MockRasterSourceImpl {
    fn query(&self, _query: QueryRectangle, ctx: QueryContext) -> BoxStream<Result<RasterTile2D>> {
        let _size = f64::sqrt(ctx.chunk_byte_size as f64 / 8.0);

        // TODO: return tiles of the input raster
        let temporal_bounds: TimeInterval = TimeInterval::default();
        let raster = Raster2D::new(
            self.dim.into(),
            self.data.clone(),
            None,
            temporal_bounds,
            self.geo_transform.into(),
        )
        .unwrap();

        let tile = RasterTile2D::new(
            temporal_bounds,
            TileInformation::new(
                (1, 1).into(),
                (0, 0).into(),
                (0, 0).into(),
                self.dim.into(),
                self.geo_transform.into(),
            ),
            TypedRaster2D::F64(raster),
        );

        let v: Vec<Result<RasterTile2D>> = vec![Ok(tile)];
        futures::stream::iter(v.into_iter()).boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D};

    #[tokio::test]
    async fn test() {
        let r = MockRasterSourceImpl {
            data: vec![1., 2., 3., 4.],
            dim: [2, 2],
            geo_transform: [1.0, 1.0, 0.0, 1.0, 0.0, 1.0],
        };

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

        r.query(query, ctx)
            .for_each(|x| {
                println!("{:?}", x);
                futures::future::ready(())
            })
            .await;
    }
}
