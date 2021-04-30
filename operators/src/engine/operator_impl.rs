use super::{
    ExecutionContext, InitializedOperatorBase, InitializedRasterOperator,
    InitializedVectorOperator, RasterOperator, ResultDescriptor, VectorOperator,
};
use crate::util::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Operator<P> {
    pub params: P,
    pub raster_sources: Vec<Box<dyn RasterOperator>>,
    pub vector_sources: Vec<Box<dyn VectorOperator>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct SourceOperator<P> {
    pub params: P,
}

pub struct InitializedOperatorImpl<R, S> {
    pub raster_sources: Vec<Box<InitializedRasterOperator>>,
    pub vector_sources: Vec<Box<InitializedVectorOperator>>,
    pub result_descriptor: R,
    pub state: S,
}

impl<R, S> InitializedOperatorImpl<R, S> {
    pub fn new(
        result_descriptor: R,
        raster_sources: Vec<Box<InitializedRasterOperator>>,
        vector_sources: Vec<Box<InitializedVectorOperator>>,
        state: S,
    ) -> Self {
        Self {
            raster_sources,
            vector_sources,
            result_descriptor,
            state,
        }
    }

    pub fn create<P, RF, SF>(
        params: &P,
        context: &dyn ExecutionContext,
        state_fn: SF,
        result_descriptor_fn: RF,
        uninitialized_raster_sources: Vec<Box<dyn RasterOperator>>,
        uninitialized_vector_sources: Vec<Box<dyn VectorOperator>>,
    ) -> Result<Self>
    where
        RF: Fn(
            &P,
            &dyn ExecutionContext,
            &S,
            &[Box<InitializedRasterOperator>],
            &[Box<InitializedVectorOperator>],
        ) -> Result<R>,
        SF: Fn(
            &P,
            &dyn ExecutionContext,
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
            params,
            context,
            raster_sources.as_slice(),
            vector_sources.as_slice(),
        )?;

        let result_descriptor = result_descriptor_fn(
            params,
            context,
            &state,
            raster_sources.as_slice(),
            vector_sources.as_slice(),
        )?;

        Ok(Self::new(
            result_descriptor,
            raster_sources,
            vector_sources,
            state,
        ))
    }
}

impl<R, S> InitializedOperatorBase for InitializedOperatorImpl<R, S>
where
    R: ResultDescriptor,
    S: Clone,
{
    type Descriptor = R;

    fn result_descriptor(&self) -> &Self::Descriptor {
        &self.result_descriptor
    }
}
