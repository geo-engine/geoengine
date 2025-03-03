use async_trait::async_trait;

use crate::{
    error::Error,
    util::{
        Result,
        input::{MultiRasterOrVectorOperator, RasterOrVectorOperator},
    },
};

use super::{
    ExecutionContext, InitializedRasterOperator, InitializedVectorOperator,
    MultipleRasterOrSingleVectorSource, MultipleRasterSources, MultipleVectorSources,
    SingleRasterOrVectorSource, SingleRasterSource, SingleVectorMultipleRasterSources,
    SingleVectorSource, WorkflowOperatorPath,
};

#[async_trait]
pub trait InitializedSources<Initialized, E = Error> {
    /// Initialize the source operator(s) with a path
    async fn initialize_sources(
        self,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Initialized, E>;
}

pub struct InitializedSingleRasterSource<R: InitializedRasterOperator> {
    pub raster: R,
    pub path: WorkflowOperatorPath,
}

pub struct InitializedSingleVectorSource<V: InitializedVectorOperator> {
    pub vector: V,
    pub path: WorkflowOperatorPath,
}

pub struct InitializedMultipleRasterSources<R: InitializedRasterOperator> {
    pub path: WorkflowOperatorPath,
    pub rasters: Vec<R>,
}

pub struct InitializedMultipleVectorSources<V: InitializedVectorOperator> {
    pub path: WorkflowOperatorPath,
    pub vectors: Vec<V>,
}

pub struct InitializedSingleVectorMultipleRasterSources<
    V: InitializedVectorOperator,
    R: InitializedRasterOperator,
> {
    pub path: WorkflowOperatorPath,
    pub vector: V,
    pub rasters: Vec<R>,
}

pub enum InitializedSingleRasterOrVectorOperator<
    R: InitializedRasterOperator,
    V: InitializedVectorOperator,
> {
    Raster(R),
    Vector(V),
}

impl<R: InitializedRasterOperator, V: InitializedVectorOperator>
    InitializedSingleRasterOrVectorOperator<R, V>
{
    pub fn is_raster(&self) -> bool {
        match self {
            InitializedSingleRasterOrVectorOperator::Raster(_) => true,
            InitializedSingleRasterOrVectorOperator::Vector(_) => false,
        }
    }

    pub fn is_vector(&self) -> bool {
        match self {
            InitializedSingleRasterOrVectorOperator::Raster(_) => false,
            InitializedSingleRasterOrVectorOperator::Vector(_) => true,
        }
    }
}

pub struct InitializedSingleRasterOrVectorSource<
    R: InitializedRasterOperator,
    V: InitializedVectorOperator,
> {
    pub path: WorkflowOperatorPath,
    pub source: InitializedSingleRasterOrVectorOperator<R, V>,
}

pub enum InitializedMultiRasterOrVectorOperator<
    R: InitializedRasterOperator,
    V: InitializedVectorOperator,
> {
    Raster(Vec<R>),
    Vector(V),
}

impl<R: InitializedRasterOperator, V: InitializedVectorOperator>
    InitializedMultiRasterOrVectorOperator<R, V>
{
    pub fn is_raster(&self) -> bool {
        match self {
            InitializedMultiRasterOrVectorOperator::Raster(_) => true,
            InitializedMultiRasterOrVectorOperator::Vector(_) => false,
        }
    }

    pub fn is_vector(&self) -> bool {
        match self {
            InitializedMultiRasterOrVectorOperator::Raster(_) => false,
            InitializedMultiRasterOrVectorOperator::Vector(_) => true,
        }
    }
}

pub struct InitializedMultiRasterOrVectorSource<
    R: InitializedRasterOperator,
    V: InitializedVectorOperator,
> {
    pub path: WorkflowOperatorPath,
    pub source: InitializedMultiRasterOrVectorOperator<R, V>,
}

#[async_trait]
impl InitializedSources<InitializedSingleRasterSource<Box<dyn InitializedRasterOperator>>>
    for SingleRasterSource
{
    async fn initialize_sources(
        self,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<InitializedSingleRasterSource<Box<dyn InitializedRasterOperator>>> {
        let op_path = path.clone_and_append(0);

        let op_initialized = self.raster.initialize(op_path, context).await?;
        Ok(InitializedSingleRasterSource {
            raster: op_initialized,
            path,
        })
    }
}

#[async_trait]
impl InitializedSources<InitializedSingleVectorSource<Box<dyn InitializedVectorOperator>>>
    for SingleVectorSource
{
    async fn initialize_sources(
        self,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<InitializedSingleVectorSource<Box<dyn InitializedVectorOperator>>> {
        let op_path = path.clone_and_append(0);

        let op_initialized = self.vector.initialize(op_path, context).await?;
        Ok(InitializedSingleVectorSource {
            vector: op_initialized,
            path,
        })
    }
}

#[async_trait]
impl InitializedSources<InitializedMultipleRasterSources<Box<dyn InitializedRasterOperator>>>
    for MultipleRasterSources
{
    async fn initialize_sources(
        self,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<InitializedMultipleRasterSources<Box<dyn InitializedRasterOperator>>> {
        let rasters = futures::future::try_join_all(
            self.rasters
                .into_iter()
                .enumerate()
                .map(|(i, op)| op.initialize(path.clone_and_append(i as u8), context)),
        )
        .await?;

        Ok(InitializedMultipleRasterSources { path, rasters })
    }
}

#[async_trait]
impl InitializedSources<InitializedMultipleVectorSources<Box<dyn InitializedVectorOperator>>>
    for MultipleVectorSources
{
    async fn initialize_sources(
        self,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<InitializedMultipleVectorSources<Box<dyn InitializedVectorOperator>>> {
        let vectors = futures::future::try_join_all(
            self.vectors
                .into_iter()
                .enumerate()
                .map(|(i, op)| op.initialize(path.clone_and_append(i as u8), context)),
        )
        .await?;

        Ok(InitializedMultipleVectorSources { path, vectors })
    }
}

#[async_trait]
impl
    InitializedSources<
        InitializedSingleVectorMultipleRasterSources<
            Box<dyn InitializedVectorOperator>,
            Box<dyn InitializedRasterOperator>,
        >,
    > for SingleVectorMultipleRasterSources
{
    async fn initialize_sources(
        self,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<
        InitializedSingleVectorMultipleRasterSources<
            Box<dyn InitializedVectorOperator>,
            Box<dyn InitializedRasterOperator>,
        >,
    > {
        let op_path = path.clone_and_append(0);
        let vector = self.vector.initialize(op_path, context).await?;

        let rasters = futures::future::try_join_all(
            self.rasters
                .into_iter()
                .enumerate()
                .map(|(i, op)| op.initialize(path.clone_and_append(i as u8 + 1), context)),
        )
        .await?;

        Ok(InitializedSingleVectorMultipleRasterSources {
            path,
            vector,
            rasters,
        })
    }
}

#[async_trait]
impl
    InitializedSources<
        InitializedSingleRasterOrVectorSource<
            Box<dyn InitializedRasterOperator>,
            Box<dyn InitializedVectorOperator>,
        >,
    > for SingleRasterOrVectorSource
{
    async fn initialize_sources(
        self,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<
        InitializedSingleRasterOrVectorSource<
            Box<dyn InitializedRasterOperator>,
            Box<dyn InitializedVectorOperator>,
        >,
    > {
        let op_path = path.clone_and_append(0);

        let source = match self.source {
            RasterOrVectorOperator::Raster(raster) => {
                let raster = raster.initialize(op_path, context).await?;
                InitializedSingleRasterOrVectorOperator::Raster(raster)
            }
            RasterOrVectorOperator::Vector(vector) => {
                let vector = vector.initialize(op_path, context).await?;
                InitializedSingleRasterOrVectorOperator::Vector(vector)
            }
        };

        Ok(InitializedSingleRasterOrVectorSource { path, source })
    }
}

#[async_trait]
impl
    InitializedSources<
        InitializedMultiRasterOrVectorSource<
            Box<dyn InitializedRasterOperator>,
            Box<dyn InitializedVectorOperator>,
        >,
    > for MultipleRasterOrSingleVectorSource
{
    async fn initialize_sources(
        self,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<
        InitializedMultiRasterOrVectorSource<
            Box<dyn InitializedRasterOperator>,
            Box<dyn InitializedVectorOperator>,
        >,
    > {
        let source = match self.source {
            MultiRasterOrVectorOperator::Vector(vector) => {
                let op_path = path.clone_and_append(0);

                let op_initialized = vector.initialize(op_path, context).await?;
                InitializedMultiRasterOrVectorOperator::Vector(op_initialized)
            }
            MultiRasterOrVectorOperator::Raster(r) => {
                let rasters = futures::future::try_join_all(
                    r.into_iter()
                        .enumerate()
                        .map(|(i, op)| op.initialize(path.clone_and_append(i as u8 + 1), context)),
                )
                .await?;

                InitializedMultiRasterOrVectorOperator::Raster(rasters)
            }
        };

        Ok(InitializedMultiRasterOrVectorSource { path, source })
    }
}
