use crate::adapters::FeatureCollectionStreamExt;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use geoengine_datatypes::primitives::BoundingBox2D;
use std::sync::Arc;

use geoengine_datatypes::{
    collections::FeatureCollectionModifications,
    primitives::{FeatureData, TimeInterval},
    raster::Pixel,
};
use geoengine_datatypes::{collections::MultiPointCollection, raster::RasterTile2D};

use crate::engine::{
    QueryContext, QueryProcessor, RasterQueryProcessor, TypedRasterQueryProcessor,
    VectorQueryProcessor, VectorQueryRectangle,
};
use crate::util::Result;
use crate::{adapters::RasterStreamExt, error::Error};
use async_trait::async_trait;
use geoengine_datatypes::collections::FeatureCollectionInfos;
use geoengine_datatypes::collections::GeometryCollection;
use geoengine_datatypes::raster::{CoordinatePixelAccess, NoDataValue, RasterDataType};
use num_traits::AsPrimitive;

pub struct RasterPointJoinProcessor {
    points: Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
    raster_processors: Vec<TypedRasterQueryProcessor>,
    column_names: Vec<String>,
}

impl RasterPointJoinProcessor {
    pub fn new(
        points: Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
        raster_processors: Vec<TypedRasterQueryProcessor>,
        column_names: Vec<String>,
    ) -> Self {
        Self {
            points,
            raster_processors,
            column_names,
        }
    }

    async fn process_collections<'a>(
        points: BoxStream<'a, Result<MultiPointCollection>>,
        raster_processor: &'a TypedRasterQueryProcessor,
        new_column_name: &'a str,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> BoxStream<'a, Result<MultiPointCollection>> {
        let stream = points.and_then(async move |points| {
            Self::process_collection_chunk(points, raster_processor, new_column_name, query, ctx)
                .await
        });

        stream
            .try_flatten()
            .merge_chunks(ctx.chunk_byte_size())
            .boxed()
    }

    async fn process_collection_chunk<'a>(
        points: MultiPointCollection,
        raster_processor: &'a TypedRasterQueryProcessor,
        new_column_name: &'a str,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<MultiPointCollection>>> {
        call_on_generic_raster_processor!(raster_processor, raster_processor => {
            Self::process_typed_collection_chunk(points, raster_processor, new_column_name, query, ctx).await
        })
    }

    async fn process_typed_collection_chunk<'a, P: Pixel>(
        points: MultiPointCollection,
        raster_processor: &'a dyn RasterQueryProcessor<RasterType = P>,
        new_column_name: &'a str,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<MultiPointCollection>>> {
        // make qrect smaller wrt. points
        let query = VectorQueryRectangle {
            spatial_bounds: points
                .bbox()
                .and_then(|bbox| bbox.intersection(&query.spatial_bounds))
                .unwrap_or(query.spatial_bounds),
            time_interval: points
                .time_bounds()
                .and_then(|time| time.intersect(&query.time_interval))
                .unwrap_or(query.time_interval),
            spatial_resolution: query.spatial_resolution,
        };

        let raster_query = raster_processor.raster_query(query.into(), ctx).await?;

        let points = Arc::new(points);

        let collection_stream = raster_query
            .time_multi_fold(
                move || Ok(PointRasterJoiner::new()),
                move |accum, raster| {
                    let points = points.clone();
                    async move {
                        let accum = accum?;
                        let raster = raster?;
                        accum.extract_raster_values(&points, &raster)
                    }
                },
            )
            .map(move |accum| accum?.into_colletion(new_column_name));

        Ok(collection_stream.boxed())
    }
}

struct PointRasterJoiner<P: Pixel> {
    values: Vec<Option<P>>,
    points: Option<MultiPointCollection>,
}

impl<P: Pixel> PointRasterJoiner<P> {
    fn new() -> Self {
        // TODO: is it possible to do the initialization here?

        Self {
            values: vec![],
            points: None,
        }
    }

    fn initialize(
        &mut self,
        points: &MultiPointCollection,
        raster_time: &TimeInterval,
    ) -> Result<()> {
        // TODO: could be paralellized

        let (indexes, time_intervals): (Vec<_>, Vec<_>) = points
            .time_intervals()
            .iter()
            .enumerate()
            .filter_map(|(i, time)| {
                time.intersect(raster_time)
                    .map(|time_intersection| (i, time_intersection))
            })
            .unzip();

        let mut valid = vec![false; points.len()];
        for i in indexes {
            valid[i] = true;
        }

        let points = points.filter(valid)?;
        let points = points.replace_time(&time_intervals)?;

        self.values = vec![None; points.coordinates().len()];
        self.points = Some(points);

        Ok(())
    }

    fn extract_raster_values(
        mut self,
        points: &MultiPointCollection,
        raster: &RasterTile2D<P>,
    ) -> Result<Self> {
        let points = loop {
            if let Some(points) = &self.points {
                break points;
            }

            self.initialize(points, &raster.time)?;
        };

        // TODO: avoid iterating over coordinates that already have a value

        for (&coordinate, value_option) in points.coordinates().iter().zip(self.values.iter_mut()) {
            if value_option.is_some() {
                continue; // already has a value
            }

            let value = match raster.pixel_value_at_coord(coordinate) {
                Ok(value) => value,
                Err(_) => continue, // not found in this raster tile
            };

            if raster.is_no_data(value) {
                continue; // value is NODATA, so we leave it being `None`
            }

            *value_option = Some(value);
        }

        Ok(self)
    }

    fn into_colletion(self, new_column_name: &str) -> Result<MultiPointCollection> {
        let points = match &self.points {
            Some(points) => points,
            None => return Err(Error::EmptyInput), // TODO: maybe output empty dataset or just nulls
        };

        // TODO: directly save values in vector of correct type
        // TODO: handle classified values
        let feature_data = match P::TYPE {
            RasterDataType::U8
            | RasterDataType::U16
            | RasterDataType::U32
            | RasterDataType::U64
            | RasterDataType::I8
            | RasterDataType::I16
            | RasterDataType::I32
            | RasterDataType::I64 => FeatureData::NullableInt(self.typed_values(points)),
            RasterDataType::F32 | RasterDataType::F64 => {
                FeatureData::NullableFloat(self.typed_values(points))
            }
        };

        Ok(points.add_column(new_column_name, feature_data)?)
    }

    fn typed_values<To: Pixel>(&self, points: &MultiPointCollection) -> Vec<Option<To>>
    where
        P: AsPrimitive<To>,
    {
        points
            .feature_offsets()
            .windows(2)
            .map(|window| {
                let (begin, end) = match *window {
                    [a, b] => (a as usize, b as usize),
                    _ => return None,
                };

                let slice = &self.values[begin..end];

                // TODO: implement aggregation methods

                match slice.first() {
                    Some(Some(value)) => Some(value.as_()),
                    _ => None,
                }
            })
            .collect()
    }
}

#[async_trait]
impl QueryProcessor for RasterPointJoinProcessor {
    type Output = MultiPointCollection;
    type SpatialBounds = BoundingBox2D;

    async fn query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let mut stream = self.points.query(query, ctx).await?;

        for (raster_processor, new_column_name) in
            self.raster_processors.iter().zip(&self.column_names)
        {
            stream =
                Self::process_collections(stream, raster_processor, new_column_name, query, ctx)
                    .await;
        }

        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::{MockExecutionContext, RasterOperator, VectorOperator};
    use crate::engine::{MockQueryContext, VectorQueryRectangle};
    use crate::mock::MockFeatureCollectionSource;
    use crate::source::{GdalSource, GdalSourceParameters};
    use crate::util::gdal::add_ndvi_dataset;
    use chrono::NaiveDate;
    use geoengine_datatypes::primitives::BoundingBox2D;
    use geoengine_datatypes::primitives::SpatialResolution;
    use geoengine_datatypes::primitives::{MultiPoint, TimeInterval};

    #[tokio::test]
    async fn both_instant() {
        let time_instant =
            TimeInterval::new_instant(NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0)).unwrap();

        let points = MockFeatureCollectionSource::single(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![
                    (-13.95, 20.05),
                    (-14.05, 20.05),
                    (-13.95, 19.95),
                    (-14.05, 19.95),
                ])
                .unwrap(),
                vec![time_instant; 4],
                Default::default(),
            )
            .unwrap(),
        )
        .boxed();

        let mut execution_context = MockExecutionContext::default();

        let raster_source = GdalSource {
            params: GdalSourceParameters {
                dataset: add_ndvi_dataset(&mut execution_context),
            },
        }
        .boxed();

        let points = points
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let rasters = raster_source
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap();

        let processor =
            RasterPointJoinProcessor::new(points, vec![rasters], vec!["ndvi".to_owned()]);

        let mut result = processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: time_instant,
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                },
                &MockQueryContext::new(usize::MAX),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let result = result.remove(0);

        assert_eq!(
            result,
            MultiPointCollection::from_slices(
                &MultiPoint::many(vec![
                    (-13.95, 20.05),
                    (-14.05, 20.05),
                    (-13.95, 19.95),
                    (-14.05, 19.95),
                ])
                .unwrap(),
                &[time_instant; 4],
                // these values are taken from loading the tiff in QGIS
                &[("ndvi", FeatureData::Int(vec![54, 55, 51, 55]))],
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn points_instant() {
        let points = MockFeatureCollectionSource::single(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![
                    (-13.95, 20.05),
                    (-14.05, 20.05),
                    (-13.95, 19.95),
                    (-14.05, 19.95),
                ])
                .unwrap(),
                vec![
                    TimeInterval::new_instant(NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0))
                        .unwrap();
                    4
                ],
                Default::default(),
            )
            .unwrap(),
        )
        .boxed();

        let mut execution_context = MockExecutionContext::default();

        let raster_source = GdalSource {
            params: GdalSourceParameters {
                dataset: add_ndvi_dataset(&mut execution_context),
            },
        }
        .boxed();

        let points = points
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let rasters = raster_source
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap();

        let processor =
            RasterPointJoinProcessor::new(points, vec![rasters], vec!["ndvi".to_owned()]);

        let mut result = processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::new(
                        NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
                        NaiveDate::from_ymd(2014, 3, 1).and_hms(0, 0, 0),
                    )
                    .unwrap(),
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                },
                &MockQueryContext::new(usize::MAX),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let result = result.remove(0);

        assert_eq!(
            result,
            MultiPointCollection::from_slices(
                &MultiPoint::many(vec![
                    (-13.95, 20.05),
                    (-14.05, 20.05),
                    (-13.95, 19.95),
                    (-14.05, 19.95),
                ])
                .unwrap(),
                &[TimeInterval::new_instant(NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0))
                    .unwrap(); 4],
                // these values are taken from loading the tiff in QGIS
                &[("ndvi", FeatureData::Int(vec![54, 55, 51, 55]))],
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn raster_instant() {
        let points = MockFeatureCollectionSource::single(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![
                    (-13.95, 20.05),
                    (-14.05, 20.05),
                    (-13.95, 19.95),
                    (-14.05, 19.95),
                ])
                .unwrap(),
                vec![
                    TimeInterval::new(
                        NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
                        NaiveDate::from_ymd(2014, 3, 1).and_hms(0, 0, 0),
                    )
                    .unwrap();
                    4
                ],
                Default::default(),
            )
            .unwrap(),
        )
        .boxed();

        let mut execution_context = MockExecutionContext::default();

        let raster_source = GdalSource {
            params: GdalSourceParameters {
                dataset: add_ndvi_dataset(&mut execution_context),
            },
        }
        .boxed();

        let points = points
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let rasters = raster_source
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap();

        let processor =
            RasterPointJoinProcessor::new(points, vec![rasters], vec!["ndvi".to_owned()]);

        let mut result = processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::new_instant(
                        NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
                    )
                    .unwrap(),
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                },
                &MockQueryContext::new(usize::MAX),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let result = result.remove(0);

        assert_eq!(
            result,
            MultiPointCollection::from_slices(
                &MultiPoint::many(vec![
                    (-13.95, 20.05),
                    (-14.05, 20.05),
                    (-13.95, 19.95),
                    (-14.05, 19.95),
                ])
                .unwrap(),
                &[TimeInterval::new(
                    NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
                    NaiveDate::from_ymd(2014, 2, 1).and_hms(0, 0, 0),
                )
                .unwrap(); 4],
                // these values are taken from loading the tiff in QGIS
                &[("ndvi", FeatureData::Int(vec![54, 55, 51, 55]))],
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn both_ranges() {
        let points = MockFeatureCollectionSource::single(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![
                    (-13.95, 20.05),
                    (-14.05, 20.05),
                    (-13.95, 19.95),
                    (-14.05, 19.95),
                ])
                .unwrap(),
                vec![
                    TimeInterval::new(
                        NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
                        NaiveDate::from_ymd(2014, 3, 1).and_hms(0, 0, 0),
                    )
                    .unwrap();
                    4
                ],
                Default::default(),
            )
            .unwrap(),
        )
        .boxed();

        let mut execution_context = MockExecutionContext::default();

        let raster_source = GdalSource {
            params: GdalSourceParameters {
                dataset: add_ndvi_dataset(&mut execution_context),
            },
        }
        .boxed();

        let points = points
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let rasters = raster_source
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap();

        let processor =
            RasterPointJoinProcessor::new(points, vec![rasters], vec!["ndvi".to_owned()]);

        let mut result = processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::new(
                        NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
                        NaiveDate::from_ymd(2014, 3, 1).and_hms(0, 0, 0),
                    )
                    .unwrap(),
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                },
                &MockQueryContext::new(usize::MAX),
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        let result = result.remove(0);

        let t1 = TimeInterval::new(
            NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
            NaiveDate::from_ymd(2014, 2, 1).and_hms(0, 0, 0),
        )
        .unwrap();
        let t2 = TimeInterval::new(
            NaiveDate::from_ymd(2014, 2, 1).and_hms(0, 0, 0),
            NaiveDate::from_ymd(2014, 3, 1).and_hms(0, 0, 0),
        )
        .unwrap();
        assert_eq!(
            result,
            MultiPointCollection::from_slices(
                &MultiPoint::many(vec![
                    (-13.95, 20.05),
                    (-14.05, 20.05),
                    (-13.95, 19.95),
                    (-14.05, 19.95),
                    (-13.95, 20.05),
                    (-14.05, 20.05),
                    (-13.95, 19.95),
                    (-14.05, 19.95),
                ])
                .unwrap(),
                &[t1, t1, t1, t1, t2, t2, t2, t2],
                // these values are taken from loading the tiff in QGIS
                &[(
                    "ndvi",
                    FeatureData::Int(vec![54, 55, 51, 55, 52, 55, 50, 53])
                )],
            )
            .unwrap()
        );
    }
}
