use futures::{Stream, StreamExt, TryStreamExt};
use futures::task::{Context, Poll};
use futures::stream::BoxStream;
use futures::pin_mut;
use pin_project::pin_project;
//use std::task::{Poll, Context};
use core::pin::Pin;
use std::time::{Instant};
use pyo3::{Python, GILGuard, types::{PyModule, PyUnicode}};
use ndarray::{ArrayBase, OwnedRepr, Dim};
use std::marker::PhantomData;
use numpy::PyArray;


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

#[derive(PartialEq, Clone)]
pub enum RasterResult<T>{
    Error,
    Empty,
    None,
    Some(Vec<T>),
}
#[pin_project(project = ZipProjection)]
pub struct Zip<St>
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

impl<St> Zip<St>
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

impl<St> Stream for Zip<St>
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