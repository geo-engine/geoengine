use super::{
    ExecutionContext, InitializedOperator, InitializedRasterOperator, InitializedVectorOperator,
    Operator, RasterOperator, ResultDescriptor, VectorOperator,
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

pub struct InitializedOperatorImpl<P, R, S> {
    pub params: P,
    pub raster_sources: Vec<Box<InitializedRasterOperator>>,
    pub vector_sources: Vec<Box<InitializedVectorOperator>>,
    pub context: ExecutionContext,
    pub result_descriptor: R,
    pub state: S,
}

impl<P, R, S> InitializedOperatorImpl<P, R, S> {
    pub fn new(
        params: P,
        context: ExecutionContext,
        result_descriptor: R,
        raster_sources: Vec<Box<InitializedRasterOperator>>,
        vector_sources: Vec<Box<InitializedVectorOperator>>,
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
        uninitialized_raster_sources: Vec<Box<dyn RasterOperator>>,
        uninitialized_vector_sources: Vec<Box<dyn VectorOperator>>,
    ) -> Result<Self>
    where
        RF: Fn(
            &P,
            &ExecutionContext,
            &S,
            &[Box<InitializedRasterOperator>],
            &[Box<InitializedVectorOperator>],
        ) -> Result<R>,
        SF: Fn(
            &P,
            &ExecutionContext,
            &[Box<InitializedRasterOperator>],
            &[Box<InitializedVectorOperator>],
        ) -> Result<S>,
    {
        let raster_sources = uninitialized_raster_sources
            .into_iter()
            .map(|o| o.initialize(context))
            .collect::<Result<Vec<Box<InitializedRasterOperator>>>>()?;
        let vector_sources = uninitialized_vector_sources
            .into_iter()
            .map(|o| o.initialize(context))
            .collect::<Result<Vec<Box<InitializedVectorOperator>>>>()?;
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

impl<P, R, S> InitializedOperator for InitializedOperatorImpl<P, R, S>
where
    P: std::fmt::Debug + Clone + 'static,
    R: std::fmt::Debug + Clone + 'static + ResultDescriptor,
    S: std::clone::Clone + 'static,
{
    type Descriptor = R;

    fn execution_context(&self) -> &ExecutionContext {
        &self.context
    }
    fn raster_sources(&self) -> &[Box<InitializedRasterOperator>] {
        self.raster_sources.as_slice()
    }
    fn vector_sources(&self) -> &[Box<InitializedVectorOperator>] {
        self.vector_sources.as_slice()
    }
    fn raster_sources_mut(&mut self) -> &mut [Box<InitializedRasterOperator>] {
        self.raster_sources.as_mut_slice()
    }
    fn vector_sources_mut(&mut self) -> &mut [Box<InitializedVectorOperator>] {
        self.vector_sources.as_mut_slice()
    }
    fn result_descriptor(&self) -> Self::Descriptor {
        self.result_descriptor
    }
}
