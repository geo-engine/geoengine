use async_trait::async_trait;
use crate::{pro::{supervised_learner::Classifier, datatypes::{PythonTrainingSession, InitializedPythonTrainingSession, split_time_intervals, perform_query_on_training_data, build_arrays}}, engine::{RasterQueryProcessor, QueryContext}, error::Error};
use geoengine_datatypes::{raster::Pixel, primitives::{TimeGranularity, TimeStep, QueryRectangle, SpatialPartition2D}};
use eval_metrics::classification::{MultiConfusionMatrix};
use rand::prelude::*;
use core::pin::Pin;
use futures::{Stream, StreamExt};

pub struct ImageSegmentator<T: Sync + Send + Pixel, C: QueryContext> {
    //The input channels, which have to be of the same datatype
    processors: Vec<Box<dyn RasterQueryProcessor<RasterType=T>>>,
    //The ground truth whcih is of type u8
    processor_truth: Box<dyn RasterQueryProcessor<RasterType = u8>>,
    //pts: InitializedPythonTrainingSession<'a, T>,
    step: usize,
    query_ctx: C,
    batch_size: usize,
    no_data_value: u8,
    tile_size: [usize;2],
    default_value: T,
    shuffle: bool,
}

impl<T: Sync + Send + Pixel + numpy::Element, C: QueryContext> ImageSegmentator<T, C> {
    pub fn new(
        processors: Vec<Box<dyn RasterQueryProcessor<RasterType=T>>>,
        processor_truth: Box<dyn RasterQueryProcessor<RasterType = u8>>,
        query_rect: QueryRectangle<SpatialPartition2D>,
        time_step: TimeStep,
        query_ctx: C,
        batch_size: usize,
        batches_per_query: usize,
        no_data_value: u8,
        tile_size: [usize;2],
        default_value: T,
        shuffle: bool,
    ) -> Self {
        let mut step: usize = 0;

        match time_step.granularity {
            TimeGranularity::Millis => {
            step = 1 * time_step.step as usize * batch_size * batches_per_query;
            },
            TimeGranularity::Seconds => {
                step = 1000 * time_step.step as usize * batch_size * batches_per_query;
            },
            TimeGranularity::Minutes => {
                step = 60_000 * time_step.step as usize * batch_size * batches_per_query;
            },
            TimeGranularity::Hours => {
                step = 3_600_000 * time_step.step as usize * batch_size * batches_per_query;
            },
            TimeGranularity::Days => {
                step = 86_400_000 * time_step.step as usize * batch_size * batches_per_query;
            },
            TimeGranularity::Months => {
                unimplemented!();
            },
            TimeGranularity::Years => {
            step = 31_536_000_000 * time_step.step as usize * batch_size * batches_per_query;
            }
        }

        //Load a Training Session with datatype T
        let mut pts: PythonTrainingSession<T> = PythonTrainingSession::new();
    
        //Initiate the Session
        let mut init_pts = pts.init();
        //Load the right module to be used by the session
        init_pts.load_module(include_str!("tf_v2.py"));
    
        //Create a model
        init_pts.initiate_model("test_model_1", 5, batch_size as u8);
        
        ImageSegmentator {
            processors: processors,
            processor_truth: processor_truth,
            //pts: init_pts,
            step: step,
            query_ctx:  query_ctx,
            batch_size: batch_size,
            no_data_value: no_data_value,
            tile_size: tile_size,
            default_value: default_value,
            shuffle: shuffle,
        }
    }
}

#[async_trait]
impl<T: Sync + Send + Pixel, C: QueryContext + Send> Classifier for ImageSegmentator<T, C> {
    
    type ReturnType = ();

    async fn train(
        &mut self, 
        query: QueryRectangle<SpatialPartition2D>, 
        query_ctx: &dyn QueryContext) -> Result<(), Error>{

            let mut rng = StdRng::from_entropy();
            let mut queries: Vec<QueryRectangle<SpatialPartition2D>> = Vec::new();
            if self.shuffle {
                queries.append(&mut split_time_intervals(query, self.step as i64).unwrap());
            } else {
                queries.push(query);
            }

            let mut final_buff_proc: Vec<Vec<Vec<T>>> = Vec::new();
            let mut final_buff_truth: Vec<Vec<u8>> = Vec::new();

            while !queries.is_empty() {

                let queries_left = queries.len();
                let mut the_chosen_one: QueryRectangle<SpatialPartition2D> = queries.remove(0);

                if self.shuffle && queries_left > 1{
                    the_chosen_one = queries.remove(rng.gen_range(0..queries_left-1));
                } else {
                    the_chosen_one = queries.remove(0);
                }
                
                let mut data_stream = perform_query_on_training_data(
                    &mut self.processors, 
                    &mut self.processor_truth,
                     the_chosen_one, 
                     &self.query_ctx).await.unwrap();

                while let Some((proc, truth)) = data_stream.next().await.unwrap() {
                    final_buff_proc.push(proc);
                    final_buff_truth.push(truth);

                    if final_buff_proc.len() >= self.batch_size {

                    }
                }
            }
            Ok(())
    }

    async fn validate(&self, 
    query: QueryRectangle<SpatialPartition2D>,
    query_ctx: &dyn QueryContext,) -> Result<MultiConfusionMatrix, Error> {
        unimplemented!();
        Ok(MultiConfusionMatrix::with_counts(Vec::new()).unwrap())
    }

    async fn predictor(
        &self,
        query: QueryRectangle<SpatialPartition2D>,
        query_ctx: &dyn QueryContext
    ) -> () {
        unimplemented!();
    }

}