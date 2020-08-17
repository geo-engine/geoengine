use super::{
    ExecutionContext, InitializedOperator, InitializedRasterOperator, InitializedVectorOperator,
    Operator, RasterOperator, VectorOperator,
};
use crate::util::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OperatorImpl<P> {
    pub params: P,
    pub raster_sources: Vec<Box<dyn RasterOperator>>,
    pub vector_sources: Vec<Box<dyn VectorOperator>>,
}

impl<P> Operator for OperatorImpl<P> where P: std::fmt::Debug + Send + Sync + Clone + 'static {}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct SourceOperatorImpl<P> {
    pub params: P,
}

impl<P> Operator for SourceOperatorImpl<P> where P: std::fmt::Debug + Send + Sync + Clone + 'static {}

#[derive(Clone)]
pub struct InitilaizedOperatorImpl<Parameters, ResultDescriptor, State> {
    pub params: Parameters,
    pub raster_sources: Vec<Box<dyn InitializedRasterOperator>>,
    pub vector_sources: Vec<Box<dyn InitializedVectorOperator>>,
    pub context: ExecutionContext,
    pub result_descriptor: ResultDescriptor,
    pub state: State,
}

impl<P, R, S> InitilaizedOperatorImpl<P, R, S> {
    pub fn new(
        params: P,
        context: ExecutionContext,
        result_descriptor: R,
        raster_sources: Vec<Box<dyn InitializedRasterOperator>>,
        vector_sources: Vec<Box<dyn InitializedVectorOperator>>,
        state: S,
    ) -> Self {
        Self {
            params,
            raster_sources,
            vector_sources,
            context,
            result_descriptor,
            state,
        }
    }

    pub fn create<RF, SF>(
        params: P,
        context: ExecutionContext,
        state_fn: SF,
        result_descriptor_fn: RF,
        raster_sources_not_init: Vec<Box<dyn RasterOperator>>,
        vector_sources_not_init: Vec<Box<dyn VectorOperator>>,
    ) -> Result<Self>
    where
        RF: Fn(
            &P,
            &ExecutionContext,
            &S,
            &[Box<dyn InitializedRasterOperator>],
            &[Box<dyn InitializedVectorOperator>],
        ) -> Result<R>,
        SF: Fn(
            &P,
            &ExecutionContext,
            &[Box<dyn InitializedRasterOperator>],
            &[Box<dyn InitializedVectorOperator>],
        ) -> Result<S>,
    {
        let raster_sources = raster_sources_not_init
            .into_iter()
            .map(|o| o.initialized_operator(context))
            .collect::<Result<Vec<Box<dyn InitializedRasterOperator>>>>()?;
        let vector_sources = vector_sources_not_init
            .into_iter()
            .map(|o| o.into_initialized_operator(context))
            .collect::<Result<Vec<Box<dyn InitializedVectorOperator>>>>()?;
        let state = state_fn(
            &params,
            &context,
            raster_sources.as_slice(),
            vector_sources.as_slice(),
        )?;

        let result_descriptor = result_descriptor_fn(
            &params,
            &context,
            &state,
            raster_sources.as_slice(),
            vector_sources.as_slice(),
        )?;

        Ok(Self::new(
            params,
            context,
            result_descriptor,
            raster_sources,
            vector_sources,
            state,
        ))
    }
}

impl<P, R, S> InitializedOperator for InitilaizedOperatorImpl<P, R, S>
where
    P: std::fmt::Debug + Clone + 'static,
    R: std::fmt::Debug + Clone + 'static,
    S: std::clone::Clone + 'static,
{
    fn execution_context(&self) -> &ExecutionContext {
        &self.context
    }
    fn raster_sources(&self) -> &[Box<dyn InitializedRasterOperator>] {
        self.raster_sources.as_slice()
    }
    fn vector_sources(&self) -> &[Box<dyn InitializedVectorOperator>] {
        self.vector_sources.as_slice()
    }
    fn raster_sources_mut(&mut self) -> &mut [Box<dyn InitializedRasterOperator>] {
        self.raster_sources.as_mut_slice()
    }
    fn vector_sources_mut(&mut self) -> &mut [Box<dyn InitializedVectorOperator>] {
        self.vector_sources.as_mut_slice()
    }
}
