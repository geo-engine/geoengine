use futures::{Stream, StreamExt, TryStreamExt};
use futures::task::{Context, Poll};
use futures::stream::BoxStream;
use futures::pin_mut;
use pin_project::pin_project;
use rand::prelude::*;
//use std::task::{Poll, Context};
use core::pin::Pin;
use std::time::{Instant};
use pyo3::{Python, GILGuard, types::{PyModule, PyUnicode}};
use ndarray::{ArrayBase, OwnedRepr, Dim};
use std::marker::PhantomData;
use numpy::PyArray;
use crate::engine::{RasterQueryProcessor, QueryContext};
use geoengine_datatypes::primitives::{TimeStep, QueryRectangle, SpatialPartition2D, TimeInstance, TimeInterval, TimeGranularity};
use geoengine_datatypes::raster::{BaseTile, GridOrEmpty, GridShape, Pixel};
use crate::error::Error;


///Python session, which keeps a GIL guard. Creates an intiated training session.
pub struct PythonTrainingSession<T> {
    gil_guard: GILGuard,
    phantom_data: PhantomData<T>,
}
///Initialized session can be used to perform all training related tasks. Can load any python file as module. File MUST have methods for intializing model(initUnet), loading model(load), training(train), prediction(pred) and saving(save).
pub struct InitializedPythonTrainingSession<'a, T> {
    py: Python<'a>,
    py_mod: Option<&'a PyModule>,
    phantom_data: PhantomData<T>,
}

impl<'a, T> PythonTrainingSession<T> where T: numpy::Element {
    pub fn new() -> Self {
        pyo3::prepare_freethreaded_python();
        PythonTrainingSession {
            gil_guard: pyo3::Python::acquire_gil(),
            phantom_data: PhantomData,
        }
    }

    pub fn init(&'a mut self) -> InitializedPythonTrainingSession<'a, T> {

        InitializedPythonTrainingSession {
            py: self.gil_guard.python(),
            py_mod: None,
            phantom_data: PhantomData,
        }

    }


}

impl<'a, T> InitializedPythonTrainingSession<'a, T> where T: numpy::Element {

    pub fn load_module(&mut self, module_name: &'static str) -> () {
        self.py_mod = Some(PyModule::from_code(self.py, module_name,"filename.py", "modulename").unwrap());
    }

    //Load an existing model.
    pub fn load_model(&self, name: &'static str) -> () {
        if(self.py_mod.is_some()) {
            let model_name = PyUnicode::new(self.py, name);
            self.py_mod.unwrap().call("load", (model_name,), None).unwrap();
        } else {
            println!("Initiate session first");
            
        }
    }
    //Initiates new model under the given name
    pub fn initiate_model(&self, name: &'static str, number_of_classes: u8, batch_size: u8) -> () {
        if(self.py_mod.is_some()) {
            let model_name = PyUnicode::new(self.py, name);
            self.py_mod.unwrap().call("initUnet2", (number_of_classes,model_name,batch_size), None).unwrap();
        } else {
            println!("Initiate session first");
            
        }
    }

    //Performs one training step
    pub fn training_step(&self, regressors: ArrayBase<OwnedRepr<T>, Dim<[usize; 4]>>, ground_truth: ArrayBase<OwnedRepr<u8>, Dim<[usize; 4]>>) -> () {

        let pool = unsafe {self.py.new_pool()};
        let pool_py = pool.python();

        let py_img = PyArray::from_owned_array(pool_py, regressors);
        let py_truth = PyArray::from_owned_array(pool_py, ground_truth);

        let _result = self.py_mod.unwrap().call("fit", (py_img, py_truth), None).unwrap();
    }

    pub fn save(&self, name: &'static str) -> () {
        if(self.py_mod.is_some()) {
            let model_name = PyUnicode::new(self.py, name);
            self.py_mod.unwrap().call("save", (model_name,), None).unwrap();
        } else {
            println!("Initiate session first");
            
        }
    }

}

///Can be used to query batches for training
pub struct TrainingDataGenerator<T, C: QueryContext> {
    processors: Vec<Box<dyn RasterQueryProcessor<RasterType=T>>>,
    ground_truth: Box<dyn RasterQueryProcessor<RasterType=u8>>,
    query_ctx: C,
    shuffle: bool,
    queries: Vec<QueryRectangle<SpatialPartition2D>>,
    current_query_stream: Option<Pin<Box<dyn Stream<Item = Option<(Vec<Vec<T,>,>, Vec<u8,>)>>,>>>,
    batch_size: usize,
    tile_size: [usize;2],
    no_data_value: u8,
    default_value: T,
    rng: ThreadRng,
}

impl<T: Pixel, C: QueryContext> TrainingDataGenerator<T, C> {
    pub fn new(processors: Vec<Box<dyn RasterQueryProcessor<RasterType=T>>>,
               ground_truth: Box<dyn RasterQueryProcessor<RasterType=u8>>, 
               query_rect: QueryRectangle<SpatialPartition2D>,
               query_ctx: C,
               shuffle: bool,
               time_step: TimeStep,
               batch_size: usize,
               batches_per_query: usize,
               tile_size: [usize;2],
               no_data_value: u8,
               default_value: T,) -> Self {
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
                   let mut queries: Vec<QueryRectangle<SpatialPartition2D>> = Vec::new();
                   if shuffle {
                    queries.append(&mut split_time_intervals(query_rect, step as i64).unwrap());
                   } else {
                       queries.push(query_rect);
                   }

                   TrainingDataGenerator{
                       processors: processors,
                       ground_truth: ground_truth,
                       query_ctx: query_ctx,
                       shuffle: shuffle,
                       queries: queries,
                       current_query_stream: None,
                       batch_size: batch_size,
                       tile_size: tile_size,
                       no_data_value: no_data_value,
                       default_value: default_value,
                       rng: rand::thread_rng(),
                   }

               }

    pub async fn next_batch(&'static mut self) -> Option<((Vec<Vec<Vec<T>>>, Vec<Vec<u8>>))> {
        let mut batch_processors: Vec<Vec<Vec<T>>> = Vec::new();
        let mut batch_truth: Vec<Vec<u8>> = Vec::new();
        //Nothing has been queried yet
        if self.current_query_stream.is_none() {
            let random_index = get_random_index(&self.queries, &mut self.rng);
            let the_chosen_one = self.queries.remove(random_index);
            self.current_query_stream = Some(perform_query_on_training_data(&self.processors, &self.ground_truth, the_chosen_one, &self.query_ctx).await.unwrap());
        }

        if self.current_query_stream.is_some() {
            
            while let Some(Some((proc, truth))) = self.current_query_stream.as_mut().unwrap().next().await {
                batch_processors.push(proc);
                batch_truth.push(truth);

                if batch_processors.len() == self.batch_size {
                    assert!(batch_truth.len() == self.batch_size);
                    return Some((batch_processors, batch_truth));
                }
            }
            //Batch is not full yet
            if !self.queries.is_empty() {
                let random_index = get_random_index(&self.queries, &mut self.rng);
                let the_chosen_one = self.queries.remove(random_index);
                self.current_query_stream = Some(perform_query_on_training_data(&self.processors, &self.ground_truth, the_chosen_one, &self.query_ctx).await.unwrap());
            } else {
                //Batch could not be filled
                return None;
            }
        }

        None
    }
}
#[derive(PartialEq, Clone)]
pub enum RasterResult<T>{
    Error,
    Empty,
    None,
    Some(Vec<T>),
}

/// Splits time intervals into smaller intervalls of length step. Step has to be in milliseconds and >= 1000!
fn split_time_intervals(query_rect: QueryRectangle<SpatialPartition2D>, step: i64 ) -> Result<Vec<QueryRectangle<SpatialPartition2D>>, Error> {

    assert!(step >= 1000);

    let mut start = query_rect.time_interval.start();
    let mut inter_end = query_rect.time_interval.start();
    let end = query_rect.time_interval.end();

    let mut queries: Vec<QueryRectangle<SpatialPartition2D>> = Vec::new();

    let mut start_plus_step = start.as_utc_date_time().unwrap().timestamp() * 1000 + step;
    let end_time = end.as_utc_date_time().unwrap().timestamp() * 1000;

    while start_plus_step <= end_time {

        let end_time_new = inter_end.as_utc_date_time().unwrap().timestamp() * 1000 + step;
        inter_end = TimeInstance::from_millis(end_time_new)?;
        
        
        let new_rect = QueryRectangle{
            spatial_bounds: query_rect.spatial_bounds,
            time_interval: TimeInterval::new(start, inter_end)?,
            spatial_resolution: query_rect.spatial_resolution
        };
        queries.push(new_rect);
        
        let start_time_new = start.as_utc_date_time().unwrap().timestamp() * 1000 + step;
        start = TimeInstance::from_millis(start_time_new)?;

        start_plus_step = start.as_utc_date_time().unwrap().timestamp() * 1000 + step;
        
    }
    Ok(queries)
}

///Returns a random index from the vector. Needs o be provided with a RandomThread.
fn get_random_index<T>(queries: &Vec<T>, rng: &mut ThreadRng) -> usize {
    let queries_left = queries.len();
    
    if queries_left > 1 {
        return rng.gen_range(0..queries_left-1);
    } else {
        return 0;
    }
}

//Queries everything that is available and returns it in a stream
async fn perform_query_on_training_data<'a, T>(processors: &'a Vec<Box<dyn                           RasterQueryProcessor<RasterType=T>>>,
ground_truth: &'a Box<dyn RasterQueryProcessor<RasterType = u8>>, query_rect: QueryRectangle<SpatialPartition2D>, query_ctx: &'a dyn QueryContext) -> Result<Pin<Box<dyn Stream<Item = Option<(Vec<Vec<T>>, Vec<u8>)>> + 'a>>, Error>
//For reasons beyond my understanding this is needed...
//TODO There might be a better way to solve this.
where dyn crate::engine::query_processor::RasterQueryProcessor<RasterType = T>: crate::engine::query_processor::RasterQueryProcessor, T: Clone + Sized {
        
    let mut bffr: Vec<Pin<Box<dyn Stream<Item = Result<BaseTile<GridOrEmpty<GridShape<[usize; 2]>, T>>, Error>> + Send, >>> = Vec::new();   

    let nop = processors.len();
    

    for element in processors.iter() {
        bffr.push(element.raster_query(query_rect, query_ctx).await?);
    }


    let mut truth_stream = ground_truth.raster_query(query_rect, query_ctx).await?;

    let mut zip = GeoZip::new(bffr);
    
    let zipped = zip.zip(truth_stream);        
    
    let output = zipped.map(|data| extract_data(data, 11));
    
    Ok(Box::pin(output))
}

fn extract_data<T>(input: (Vec<Result<BaseTile<GridOrEmpty<GridShape<[usize; 2]>, T>>, Error>>, Result<BaseTile<GridOrEmpty<GridShape<[usize; 2]>, u8>>, Error>), nop: usize) -> Option<(Vec<Vec<T>>, Vec<u8>)> where T: Clone{
    
    let mut buffer_proc: Vec<RasterResult<T>> = Vec::with_capacity(nop);
    let truth_int: RasterResult<u8>;

    let mut final_buff_proc: Vec<Vec<Vec<T>>> = Vec::new(); 
    let mut final_buff_truth: Vec<Vec<u8>> = Vec::new();
    
    let (proc, truth) = input;

    for processor in proc {
        match processor {
            Ok(processor) => {
                match processor.grid_array {
                    GridOrEmpty::Grid(processor) => {
                        buffer_proc.push(RasterResult::Some(processor.data));
                    },
                    _ => {
                        buffer_proc.push(RasterResult::Empty);
                    }
                }
            },
            _ => {
                buffer_proc.push(RasterResult::Error);
            }
        }
    }
    match truth {
        Ok(truth) => {
            match truth.grid_array {
                GridOrEmpty::Grid(truth) => {
                    truth_int = RasterResult::Some(truth.data);
                },
                _ => {
                    truth_int = RasterResult::Empty;
                }
            }
        },
        _ => {
            truth_int = RasterResult::Error;
        }
    }
    if buffer_proc.iter().all(|x| matches!(x, &RasterResult::Some(_))) && buffer_proc.len() == nop && matches!(truth_int, RasterResult::Some(_)) {
        final_buff_proc.push(buffer_proc.into_iter().map(|x| {
            if let RasterResult::Some(x) = x {
                
                
                return x;
            } else {
                unreachable!();
            }
        }).collect());
        if let RasterResult::Some(x) = truth_int{
            final_buff_truth.push(x);
        } else {
            unreachable!();
        }            
    }

    match (final_buff_proc.get(0), final_buff_truth.get(0)) {
        (Some(x), Some(y)) => {
            return (Some((x.to_vec(), y.to_vec())))
        },
        _ => {
            return None
        }
    }
}



#[pin_project(project = ZipProjection)]
pub struct GeoZip<St>
where
    St: Stream,
{
    #[pin]
    streams: Vec<St>,
    values: Vec<Option<St::Item>>,
    times: Vec<u64>,
    state: ZipState,
}

enum ZipState {
    Idle,
    Busy,
    Finished,
}

impl<St> GeoZip<St>
where
    // can we really say Unpin, Send and static?
    St: Stream + std::marker::Unpin,
{
    pub fn new(streams: Vec<St>) -> Self {
        assert!(!streams.is_empty());

        Self {
            values: Vec::with_capacity(streams.len()),
            times: Vec::with_capacity(streams.len()),
            streams,
            state: ZipState::Idle,
        }
    }

    fn check_streams(self: Pin<&mut Self>, cx: &mut Context<'_>) {
        let mut this = self.project();

        if this.values.is_empty() {
            this.values.resize_with(this.streams.len(), ||None);
        }

        if this.times.is_empty() {
            this.times.resize_with(this.streams.len(), || 0);
        }

        *this.state = ZipState::Busy;

        for (i, stream) in this.streams.iter_mut().enumerate() {
            //eprintln!("check work {}", i); // TODO: REMOVE

            if this.values[i].is_some() {
                // already emitted value, do not poll!
                continue;
            }

            match Pin::new(stream).poll_next(cx) {
                Poll::Ready(Some(value)) => {
                    this.values[i] = Some(value);
                }
                Poll::Ready(None) => {
                    // for (i, element) in this.times.iter().enumerate() {
                    //     println!("Processor{}: {:?}",i, element);
                        
                    // }
                    // first stream is done, so the whole `Zip` is done
                    *this.state = ZipState::Finished;
                    return;
                }
                Poll::Pending => {
                    //this.times[i] = this.times[i] + 1;
                },
            }
        }
    }

    fn return_values(self: Pin<&mut Self>) -> Option<Vec<St::Item>> {
        if self.values.iter().any(Option::is_none) {
            return None;
        }

        //eprintln!("ready to return"); // TODO: REMOVE

        let values = self
            .project()
            .values
            .drain(..)
            .map(Option::unwrap)
            .collect();

        Some(values)
    }
}

impl<St> Stream for GeoZip<St>
where
    St: Stream + std::marker::Unpin,
{
    type Item = Vec<St::Item>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Vec<St::Item>>> {
        //eprintln!("poll next"); // TODO: REMOVE
        let time_instace = Instant::now();

        if matches!(self.state, ZipState::Finished) {
            return Poll::Ready(None);
        }

        self.as_mut().check_streams(cx);

        if matches!(self.state, ZipState::Finished) {
            //println!("Time in poll: {:?}", time_instace.elapsed());
            
            return Poll::Ready(None);
        }

        if let Some(values) = self.return_values() {
            //println!("Time in poll: {:?}", time_instace.elapsed());
            Poll::Ready(Some(values))
        } else {
            //println!("Time in poll: {:?}", time_instace.elapsed());
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*; 

    #[tokio::test]
    async fn python_training_session_test() {
        let mut pts: PythonTrainingSession<i32> = PythonTrainingSession::new();
        let mut init_pts = pts.init();
        init_pts.load_module(include_str!("tf_v2.py"));

    }
}


#[tokio::test]
async fn main() {
//     let st1 = stream! {
//         for i in 1..=3 {
//             tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
//             yield i;
//         }
//     };

//     let st2 = stream! {
//         for i in 1..=3 {
//             tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
//             yield i * 10;
//         }
//     };

//     let st1: BoxStream<'static, u32> = Box::pin(st1);
//     let st2: BoxStream<'static, u32> = Box::pin(st2);

//     let mut st_all = Zip::new(vec![st1, st2]);

//     eprintln!();
//     eprintln!();
//     eprintln!();

//     let start = std::time::Instant::now();

//     while let Some(value) = st_all.next().await {
//         println!("{:?}", value);
//     }

//     eprint!(
//         "Elapsed = {} (should be ~3000)",
//         start.elapsed().as_millis()
//     );

//     let s = stream! {
//         for i in 1..=3 {
//             tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
//             yield i;
//         }
//     };
//     pin_mut!(s);

//     let start = std::time::Instant::now();

//     while let Some(value) = s.next().await {
//         println!("{:?}", value);
//     }

//     eprint!(
//         "Elapsed = {} (should be ~3000)",
//         start.elapsed().as_millis()
//     );
}