use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedRasterOperator, InitializedSources,
    MultipleRasterSources, Operator, OperatorName, QueryContext, RasterOperator,
    RasterQueryProcessor, RasterResultDescriptor, TypedRasterQueryProcessor, WorkflowOperatorPath,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::future::join_all;
use futures::ready;
use futures::stream::BoxStream;
use geoengine_datatypes::primitives::{
    partitions_extent, time_interval_extent, RasterQueryRectangle, SpatialResolution,
};
use geoengine_datatypes::raster::{DynamicRasterDataType, Pixel, RasterTile2D};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};

// TODO: IF this operator shall perform spatio-temporal alignment automatically: specify the alignment strategy here
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RasterStackerParams {}

/// This `QueryProcessor` stacks all of it's inputs into a single raster time-series.
/// It does so by querying all of it's inputs outputting them by band, space and then time.
///
/// All inputs must have the same data type and spatial reference.
// TODO: spatio-temporal alignment(?) or do that beforehand?
//     if we explicitly align beforehand using custom operators we have the problem that we need to hardcode the alignment params(?) and if the dataset changes the workflow no longer works
//      we have no way of aligning indepentently of each other before putting them into the `RasterStacker`` as we cannot access other operators in the workflow
pub type RasterStacker = Operator<RasterStackerParams, MultipleRasterSources>;

impl OperatorName for RasterStacker {
    const TYPE_NAME: &'static str = "RasterStacker";
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for RasterStacker {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let name = CanonicOperatorName::from(&self);

        // TODO: ensure at least two inputs

        // TODO: verify all inputs have same data type and spatial reference

        // TODO: add sparse fill adapter on top of sources? Or do we guarantee gap-free already?

        // TODO: inject operators ontop of the sources to align them spatio-temporally

        let raster_sources = self
            .sources
            .initialize_sources(path, context)
            .await?
            .rasters;

        let in_descriptors = raster_sources
            .iter()
            .map(InitializedRasterOperator::result_descriptor)
            .collect::<Vec<_>>();

        let time = time_interval_extent(in_descriptors.iter().map(|d| d.time));
        let bbox = partitions_extent(in_descriptors.iter().map(|d| d.bbox));

        let resolution = in_descriptors
            .iter()
            .map(|d| d.resolution)
            .reduce(|a, b| match (a, b) {
                (Some(a), Some(b)) => {
                    Some(SpatialResolution::new_unchecked(a.x.min(b.x), a.y.min(b.y)))
                }
                _ => None,
            })
            .flatten();

        let bands_per_source = in_descriptors
            .iter()
            .map(|d| d.bands as usize)
            .collect::<Vec<_>>();

        let bands: usize = bands_per_source.iter().sum();

        let result_descriptor = RasterResultDescriptor {
            data_type: in_descriptors[0].data_type,
            spatial_reference: in_descriptors[0].spatial_reference,
            measurement: in_descriptors[0].measurement.clone(),
            time,
            bbox,
            resolution,
            bands: bands as i32,
        };

        Ok(Box::new(InitializedRasterStacker {
            name,
            result_descriptor,
            raster_sources,
            bands_per_source,
        }))
    }

    span_fn!(RasterStacker);
}

pub struct InitializedRasterStacker {
    name: CanonicOperatorName,
    result_descriptor: RasterResultDescriptor,
    raster_sources: Vec<Box<dyn InitializedRasterOperator>>,
    bands_per_source: Vec<usize>,
}

impl InitializedRasterOperator for InitializedRasterStacker {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let typed_raster_processors = self
            .raster_sources
            .iter()
            .map(InitializedRasterOperator::query_processor)
            .collect::<Result<Vec<_>>>()?;

        // unpack all processors
        let datatype = typed_raster_processors[0].raster_data_type();

        let bands_per_source = self.bands_per_source.clone();

        // TODO: use a macro to unpack all the input processor to the same datatype?
        Ok(match datatype {
            geoengine_datatypes::raster::RasterDataType::U8 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_u8().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(inputs, bands_per_source);
                TypedRasterQueryProcessor::U8(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::U16 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_u16().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(inputs, bands_per_source);
                TypedRasterQueryProcessor::U16(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::U32 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_u32().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(inputs, bands_per_source);
                TypedRasterQueryProcessor::U32(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::U64 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_u64().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(inputs, bands_per_source);
                TypedRasterQueryProcessor::U64(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::I8 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_i8().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(inputs, bands_per_source);
                TypedRasterQueryProcessor::I8(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::I16 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_i16().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(inputs, bands_per_source);
                TypedRasterQueryProcessor::I16(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::I32 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_i32().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(inputs, bands_per_source);
                TypedRasterQueryProcessor::I32(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::I64 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_i64().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(inputs, bands_per_source);
                TypedRasterQueryProcessor::I64(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::F32 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_f32().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(inputs, bands_per_source);
                TypedRasterQueryProcessor::F32(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::F64 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_f64().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(inputs, bands_per_source);
                TypedRasterQueryProcessor::F64(Box::new(p))
            }
        })
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }
}

pub(crate) struct RasterStackerProcessor<T> {
    sources: Vec<Box<dyn RasterQueryProcessor<RasterType = T>>>,
    bands_per_source: Vec<usize>,
}

impl<T> RasterStackerProcessor<T> {
    pub fn new(
        sources: Vec<Box<dyn RasterQueryProcessor<RasterType = T>>>,
        bands_per_source: Vec<usize>,
    ) -> Self {
        Self {
            sources,
            bands_per_source,
        }
    }
}

#[async_trait]
impl<T> RasterQueryProcessor for RasterStackerProcessor<T>
where
    T: Pixel,
{
    type RasterType = T;
    async fn raster_query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<T>>>> {
        let source_stream_futures = self
            .sources
            .iter()
            .map(|s| async { s.raster_query(query, ctx).await });

        let source_streams = join_all(source_stream_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let output = RoundRobin::new(source_streams, self.bands_per_source.clone());

        Ok(Box::pin(output))
    }
}

use futures::stream::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Return items from the input streams in a round-robin fashion.
/// For each input stream it returns items according to the batch size.
/// Then it continues with the next stream and wraps around at the end until all streams are finished.
#[pin_project(project = RoundRobinProjection)]
pub struct RoundRobin<S> {
    #[pin]
    streams: Vec<S>,
    batch_size_per_stream: Vec<usize>,
    current_stream: usize,
    current_stream_item: usize,
    finished: bool,
}

impl<S> RoundRobin<S> {
    pub fn new(streams: Vec<S>, batch_size_per_stream: Vec<usize>) -> Self {
        RoundRobin {
            streams,
            batch_size_per_stream,
            current_stream: 0,
            current_stream_item: 0,
            finished: false,
        }
    }
}

impl<S, T> Stream for RoundRobin<S>
where
    S: Stream<Item = T> + Unpin,
    T: Send + Sync,
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished || self.streams.is_empty() {
            return Poll::Ready(None);
        }

        let RoundRobinProjection {
            mut streams,
            batch_size_per_stream,
            current_stream,
            current_stream_item,
            finished: _,
        } = self.as_mut().project();

        let stream = &mut streams[*current_stream];

        let item = ready!(Pin::new(stream).poll_next(cx));

        let Some(item) = item else {
            // if one input stream ends, end the output stream
            return Poll::Ready(None);
        };

        // next item in stream, or go to next stream
        *current_stream_item += 1;
        if *current_stream_item >= batch_size_per_stream[*current_stream] {
            *current_stream_item = 0;
            *current_stream = (*current_stream + 1) % streams.len();
        }

        Poll::Ready(Some(item))
    }
}
