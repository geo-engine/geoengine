use std::sync::Arc;

use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};

use geoengine_datatypes::{
    collections::FeatureCollectionModifications,
    primitives::{FeatureData, TimeInterval},
    raster::Pixel,
};
use geoengine_datatypes::{collections::MultiPointCollection, raster::RasterTile2D};

use crate::engine::{
    QueryContext, QueryProcessor, QueryRectangle, RasterQueryProcessor, TypedRasterQueryProcessor,
    VectorQueryProcessor,
};
use crate::util::Result;
use crate::{
    adapters::{FeatureCollectionStreamExt, RasterStreamExt},
    error::Error,
};
use geoengine_datatypes::collections::FeatureCollectionInfos;
use geoengine_datatypes::collections::GeometryCollection;
use geoengine_datatypes::raster::Raster;
use geoengine_datatypes::raster::{CoordinatePixelAccess, RasterDataType};
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

    // fn extract_raster_values<P: Pixel>(
    //     points: &MultiPointCollection,
    //     raster: &RasterTile2D<P>,
    //     new_column_name: &str,
    // ) -> Result<MultiPointCollection> {
    //     let mut valid: Vec<bool> = vec![false; points.len()];
    //     let mut values = Vec::with_capacity(points.len());
    //     let mut time_intervals = Vec::with_capacity(points.len());

    //     for row in points {
    //         let time_interval = match row.time_interval.intersect(&raster.time) {
    //             Some(t) => t,
    //             None => continue,
    //         };

    //         let mut value_option: Option<P> = None;
    //         for &coordinate in row.geometry.points() {
    //             let value = match raster.pixel_value_at_coord(coordinate) {
    //                 Ok(value) => value,
    //                 Err(_) => continue, // TODO: is it okay to ignore null values?
    //             };

    //             if raster.is_no_data(value) {
    //                 continue; // TODO: is it okay to ignore null values?
    //             }

    //             value_option = Some(value);

    //             // TODO: aggregate multiple extracted values for one multi point before inserting it
    //             break;
    //         }

    //         if let Some(value) = value_option {
    //             valid[row.index()] = true;
    //             time_intervals.push(time_interval);
    //             values.push(value);
    //         }
    //     }

    //     // TODO: directly save values in vector of correct type
    //     // TODO: handle classified values
    //     let feature_data = match P::TYPE {
    //         RasterDataType::U8
    //         | RasterDataType::U16
    //         | RasterDataType::U32
    //         | RasterDataType::U64
    //         | RasterDataType::I8
    //         | RasterDataType::I16
    //         | RasterDataType::I32
    //         | RasterDataType::I64 => {
    //             FeatureData::Decimal(values.into_iter().map(AsPrimitive::as_).collect())
    //         }
    //         RasterDataType::F32 | RasterDataType::F64 => {
    //             FeatureData::Number(values.into_iter().map(AsPrimitive::as_).collect())
    //         }
    //     };

    //     let points = points.filter(valid)?;

    //     let points = points.replace_time(&time_intervals)?;
    //     let points = points.add_column(new_column_name, feature_data)?;

    //     Ok(points)
    // }

    fn process_collections<'a>(
        points: BoxStream<'a, Result<MultiPointCollection>>,
        raster_processor: &'a TypedRasterQueryProcessor,
        new_column_name: &'a str,
        query: QueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> BoxStream<'a, Result<MultiPointCollection>> {
        let stream = points.map_ok(move |points| {
            Self::process_collection_chunk(points, raster_processor, new_column_name, query, ctx)
        });

        stream
            .try_flatten()
            .merge_chunks(ctx.chunk_byte_size())
            .boxed()
    }

    fn process_collection_chunk<'a>(
        points: MultiPointCollection,
        raster_processor: &'a TypedRasterQueryProcessor,
        new_column_name: &'a str,
        query: QueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> BoxStream<'a, Result<MultiPointCollection>> {
        call_on_generic_raster_processor!(raster_processor, raster_processor => {
            Self::process_typed_collection_chunk(points, raster_processor, new_column_name, query, ctx)
        })
    }

    fn process_typed_collection_chunk<'a, P: Pixel>(
        points: MultiPointCollection,
        raster_processor: &'a dyn RasterQueryProcessor<RasterType = P>,
        new_column_name: &'a str,
        query: QueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> BoxStream<'a, Result<MultiPointCollection>> {
        // TODO: intersection bbox of collection with qrect

        let raster_query = match raster_processor.raster_query(query, ctx) {
            Ok(q) => q,
            Err(e) => return futures::stream::once(async { Err(e) }).boxed(),
        };

        let points = Arc::new(points);
        let accum_points = points.clone();

        let collection_stream = raster_query
            .time_multi_fold(
                move || Ok(PointRasterJoiner::new(&accum_points)),
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

        collection_stream.boxed()
    }
}

struct PointRasterJoiner<P: Pixel> {
    values: Vec<Option<P>>,
    points: Option<MultiPointCollection>,
}

impl<P: Pixel> PointRasterJoiner<P> {
    fn new(_points: &MultiPointCollection) -> Self {
        // TODO: do initialization here

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

        // TODO: avoid looking up coordinates again and again if they already have a value

        for (&coordinate, value_option) in points.coordinates().iter().zip(self.values.iter_mut()) {
            if value_option.is_some() {
                continue; // already has a value
            }

            let value = match raster.pixel_value_at_coord(coordinate) {
                Ok(value) => value,
                Err(_) => continue, // not found in this raster tile
            };

            if raster.is_no_data(value) {
                continue; // TODO: is it okay to treat no data the same as not found?
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
            | RasterDataType::I64 => FeatureData::NullableDecimal(self.typed_values(points)),
            RasterDataType::F32 | RasterDataType::F64 => {
                FeatureData::NullableNumber(self.typed_values(points))
            }
        };

        // match self.points {
        //     Some(points) => Ok(points.add_column(new_column_name, feature_data)?),
        //     None => Err(Error::EmptyInput), // TODO: maybe output empty dataset or just nulls
        // }

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

impl VectorQueryProcessor for RasterPointJoinProcessor {
    type VectorType = MultiPointCollection;

    fn vector_query<'a>(
        &'a self,
        query: QueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::VectorType>>> {
        let mut stream = self.points.query(query, ctx)?;

        for (raster_processor, new_column_name) in
            self.raster_processors.iter().zip(&self.column_names)
        {
            stream =
                Self::process_collections(stream, raster_processor, new_column_name, query, ctx);
        }

        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::{MockExecutionContext, VectorOperator};
    use crate::mock::MockFeatureCollectionSource;
    use crate::util::gdal::add_ndvi_dataset;
    use chrono::NaiveDate;
    use geoengine_datatypes::primitives::{MultiPoint, TimeInterval};

    #[test]
    fn test_name() {
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
                        NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
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

        let _ndvi_id = add_ndvi_dataset(&mut execution_context);

        let points = points
            .initialize(&execution_context)
            .unwrap()
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let _processor = RasterPointJoinProcessor::new(points, vec![], vec!["ndvi".to_owned()]);
    }
}
